package ltx

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"os"

	"github.com/pierrec/lz4/v4"
)

// DefaultSpillThreshold is the number of page index entries an Encoder keeps
// in memory before spilling the index to disk, when a spill directory is set.
// 1M entries is ~24 MiB in memory; a 4 KiB-page database crosses it at 4 GiB.
const DefaultSpillThreshold = 1 << 20

// Encoder implements an encoder for an LTX file.
type Encoder struct {
	w     io.Writer // main writer
	state string

	header  Header
	trailer Trailer
	hash    hash.Hash64
	index   pageIndex // pages in write order (ascending pgno)
	n       int64     // bytes written

	// Optional on-disk spill for the page index of very large files. Once
	// the in-memory index reaches spillThreshold entries and spillDir is set,
	// entries are appended to a temp file as their final varint records and
	// copied into the output on Close, so encoder memory stops scaling with
	// page count (litestream #1477 / ltx #96).
	spillDir       string
	spillThreshold int
	spill          *indexSpill

	// LZ4 block compression
	compressor  lz4.Compressor
	compressBuf []byte

	// Track how many of each write has occurred to move state.
	prevPgno     uint32
	pagesWritten uint32
}

// NewEncoder returns a new instance of Encoder.
func NewEncoder(w io.Writer) (*Encoder, error) {
	return &Encoder{
		w:              w,
		state:          stateHeader,
		spillThreshold: DefaultSpillThreshold,
	}, nil
}

// SetSpillDir enables spilling the page index to a temp file created in dir
// once it reaches the spill threshold. The directory must be writable for
// the lifetime of the encoder; litestream passes its per-database meta
// directory since hardened images have no /tmp. Empty (the default) keeps
// the whole index in memory.
func (enc *Encoder) SetSpillDir(dir string) { enc.spillDir = dir }

// SetSpillThreshold sets the number of in-memory page index entries that
// triggers a spill. Values <= 0 restore DefaultSpillThreshold.
func (enc *Encoder) SetSpillThreshold(n int) {
	if n <= 0 {
		n = DefaultSpillThreshold
	}
	enc.spillThreshold = n
}

// Spilled reports whether the page index has been spilled to disk.
func (enc *Encoder) Spilled() bool { return enc.spill != nil }

// Cleanup removes the spill file, if any, and aborts an encoder that has
// not completed a successful Close, so a later Close cannot emit a file
// whose index was discarded. It is idempotent, safe after Close, and must be
// called by callers that abandon an encoder without closing it (for example
// when a snapshot upload is cancelled) so the spill file is not left behind.
// On removal failure the spill is kept so Cleanup can be retried; the error
// is returned.
func (enc *Encoder) Cleanup() error {
	if enc.state != stateClosed {
		enc.state = stateAborted
	}
	if enc.spill == nil {
		return nil
	}
	if err := enc.spill.remove(); err != nil {
		return err
	}
	enc.spill = nil
	return nil
}

// abort marks the encoder unusable after a failed write.
func (enc *Encoder) abort(err error) error {
	enc.state = stateAborted
	return err
}

// N returns the number of bytes written.
func (enc *Encoder) N() int64 { return enc.n }

// Header returns a copy of the header.
func (enc *Encoder) Header() Header { return enc.header }

// Trailer returns a copy of the trailer. File checksum available after Close().
func (enc *Encoder) Trailer() Trailer { return enc.trailer }

// PostApplyPos returns the replication position after underlying the LTX file is applied.
// Only valid after successful Close().
func (enc *Encoder) PostApplyPos() Pos {
	return Pos{
		TXID:              enc.header.MaxTXID,
		PostApplyChecksum: enc.trailer.PostApplyChecksum,
	}
}

// SetPostApplyChecksum sets the post-apply checksum of the database.
// Must call before Close().
func (enc *Encoder) SetPostApplyChecksum(chksum Checksum) {
	enc.trailer.PostApplyChecksum = chksum
}

// Close flushes the checksum to the header.
func (enc *Encoder) Close() (err error) {
	if enc.state == stateClosed {
		// The file is complete; the only thing that can still be pending
		// is a spill removal that failed last time, so retry it rather
		// than report success over a leaked temp file.
		if enc.spill != nil {
			return enc.Cleanup()
		}
		return nil // no-op
	} else if enc.state == stateAborted {
		return ErrEncoderAborted
	} else if enc.state != statePage {
		return fmt.Errorf("cannot close, expected %s", enc.state)
	}

	// Closing is all-or-nothing: on success the spill file is removed, and
	// on any failure the encoder is aborted (a retry would emit a second
	// end-of-pages marker and an empty index) and the spill is still removed.
	defer func() {
		if err != nil {
			enc.state = stateAborted
		}
		if cerr := enc.Cleanup(); cerr != nil && err == nil {
			err = fmt.Errorf("cleanup spill: %w", cerr)
		}
	}()

	// Marshal empty page header to mark end of page block.
	b0, err := (&PageHeader{}).MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal empty page header: %w", err)
	} else if _, err := enc.write(b0); err != nil {
		return fmt.Errorf("write empty page header: %w", err)
	}

	// Write index to file.
	if err := enc.encodePageIndex(); err != nil {
		return fmt.Errorf("write page index: %w", err)
	}

	// Marshal trailer to bytes.
	b1, err := enc.trailer.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal trailer: %w", err)
	}
	enc.writeToHash(b1[:TrailerChecksumOffset])
	enc.trailer.FileChecksum = ChecksumFlag | Checksum(enc.hash.Sum64())

	// Validate trailer now that we have the file checksum.
	if err := enc.trailer.Validate(enc.header); err != nil {
		return fmt.Errorf("validate trailer: %w", err)
	}

	// If we are encoding a deletion LTX file then ensure that we have an empty checksum.
	if enc.header.Commit == 0 && enc.trailer.PostApplyChecksum != ChecksumFlag {
		return fmt.Errorf("post-apply checksum must be empty for zero-length database")
	}

	// Remarshal with correct checksum.
	b1, err = enc.trailer.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal trailer: %w", err)
	} else if n, err := enc.w.Write(b1); err != nil {
		return fmt.Errorf("write trailer: %w", err)
	} else if n < len(b1) {
		return fmt.Errorf("write trailer: %w", io.ErrShortWrite)
	}
	enc.n += ChecksumSize

	enc.state = stateClosed

	return nil
}

func (enc *Encoder) encodePageIndex() error {
	offset := enc.n

	// EncodePage enforces strictly ascending page numbers, so the index is
	// already in sorted order.
	//
	// Write each element as a varint-encoded tuple, either from the spill
	// file (which already holds every record in that format) or from memory.
	buf := make([]byte, 0, 3*binary.MaxVarintLen64)
	if enc.spill != nil {
		if err := enc.spill.copyTo(enc); err != nil {
			return fmt.Errorf("copy spilled page index: %w", err)
		}
	} else {
		for _, chunk := range enc.index.chunks {
			for _, elem := range chunk {
				if _, err := enc.write(appendPageIndexRecord(buf[:0], elem)); err != nil {
					return fmt.Errorf("write page index element: %w", err)
				}
			}
		}
	}

	// Write end marker.
	buf = binary.AppendUvarint(buf[:0], uint64(0))
	if _, err := enc.write(buf); err != nil {
		return fmt.Errorf("write page index pgno: %w", err)
	}

	// Write size of page index.
	buf = binary.BigEndian.AppendUint64(buf[:0], uint64(enc.n-offset))
	if _, err := enc.write(buf); err != nil {
		return fmt.Errorf("write page index size: %w", err)
	}

	return nil
}

// EncodeHeader writes hdr to the file's header block.
func (enc *Encoder) EncodeHeader(hdr Header) error {
	if enc.state == stateClosed {
		return ErrEncoderClosed
	} else if enc.state == stateAborted {
		return ErrEncoderAborted
	} else if enc.state != stateHeader {
		return fmt.Errorf("cannot encode header frame, expected %s", enc.state)
	} else if err := hdr.Validate(); err != nil {
		return err
	}

	enc.header = hdr

	// Initialize hash.
	enc.hash = crc64.New(crc64.MakeTable(crc64.ISO))

	// Write header to underlying writer. A partial header cannot be
	// retried, so a write failure aborts the encoder.
	b, err := enc.header.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal header: %w", err)
	} else if _, err := enc.write(b); err != nil {
		return enc.abort(fmt.Errorf("write header: %w", err))
	}

	// Move writer state to write page headers.
	enc.state = statePage // file must have at least one page

	return nil
}

// EncodePage writes hdr & data to the file's page block.
func (enc *Encoder) EncodePage(hdr PageHeader, data []byte) (err error) {
	if enc.state == stateClosed {
		return ErrEncoderClosed
	} else if enc.state == stateAborted {
		return ErrEncoderAborted
	} else if enc.state != statePage {
		return fmt.Errorf("cannot encode page header, expected %s", enc.state)
	} else if hdr.Pgno > enc.header.Commit {
		return fmt.Errorf("page number %d out-of-bounds for commit size %d", hdr.Pgno, enc.header.Commit)
	} else if err := hdr.Validate(); err != nil {
		return err
	} else if uint32(len(data)) != enc.header.PageSize {
		return fmt.Errorf("invalid page buffer size: %d, expecting %d", len(data), enc.header.PageSize)
	}

	lockPgno := LockPgno(enc.header.PageSize)
	if hdr.Pgno == lockPgno {
		return fmt.Errorf("cannot encode lock page: pgno=%d", hdr.Pgno)
	}

	// Snapshots must start with page 1 and include all pages up to the commit size.
	// Non-snapshot files can include any pages but they must be in order.
	if enc.header.IsSnapshot() {
		if enc.prevPgno == 0 && hdr.Pgno != 1 {
			return fmt.Errorf("snapshot transaction file must start with page number 1")
		}

		if enc.prevPgno == lockPgno-1 {
			if hdr.Pgno != enc.prevPgno+2 { // skip lock page
				return fmt.Errorf("nonsequential page numbers in snapshot transaction (skip lock page): %d,%d", enc.prevPgno, hdr.Pgno)
			}
		} else if enc.prevPgno != 0 && hdr.Pgno != enc.prevPgno+1 {
			return fmt.Errorf("nonsequential page numbers in snapshot transaction: %d,%d", enc.prevPgno, hdr.Pgno)
		}
	} else {
		if enc.prevPgno >= hdr.Pgno {
			return fmt.Errorf("out-of-order page numbers: %d,%d", enc.prevPgno, hdr.Pgno)
		}
	}

	offset := enc.n

	// Allocate compression buffer if needed.
	if enc.compressBuf == nil {
		enc.compressBuf = make([]byte, lz4.CompressBlockBound(int(enc.header.PageSize)))
	}

	// Compress data using LZ4 block compression.
	n, err := enc.compressor.CompressBlock(data, enc.compressBuf)
	if err != nil {
		return fmt.Errorf("compress page data: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("lz4 block compression failed")
	}

	// Set flag indicating size field follows the page header (block format).
	hdr.Flags |= PageHeaderFlagSize

	writeData := enc.compressBuf[:n]

	// Write page header. Once any bytes of a frame have been written the
	// file cannot be completed, so write failures abort the encoder.
	b, err := hdr.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	} else if _, err := enc.write(b); err != nil {
		return enc.abort(fmt.Errorf("write page header: %w", err))
	}

	// Write data size (4 bytes, big-endian).
	sizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuf, uint32(len(writeData)))
	if _, err := enc.write(sizeBuf); err != nil {
		return enc.abort(fmt.Errorf("write data size: %w", err))
	}

	// Write page data (compressed or uncompressed).
	if n, err := enc.w.Write(writeData); err != nil {
		return enc.abort(fmt.Errorf("write page data: %w", err))
	} else if n < len(writeData) {
		return enc.abort(fmt.Errorf("write page data: %w", io.ErrShortWrite))
	}
	_, _ = enc.hash.Write(data) // hash the uncompressed data
	enc.n += int64(len(writeData))

	enc.pagesWritten++
	enc.prevPgno = hdr.Pgno
	entry := pageIndexEntry{
		pgno:   hdr.Pgno,
		offset: offset,
		size:   enc.n - offset,
	}
	// The page bytes are already in the output, so an index failure here
	// leaves a file that cannot be completed: abort the encoder.
	if enc.spill != nil {
		if err := enc.spill.append(entry); err != nil {
			return enc.abort(fmt.Errorf("spill page index: %w", err))
		}
		return nil
	}
	enc.index.append(entry)
	if enc.spillDir != "" && enc.index.n >= enc.spillThreshold {
		if err := enc.startSpill(); err != nil {
			return enc.abort(fmt.Errorf("spill page index: %w", err))
		}
	}

	return nil
}

// startSpill moves the in-memory page index to a temp file in spillDir and
// releases the in-memory chunks.
func (enc *Encoder) startSpill() error {
	spill, err := newIndexSpill(enc.spillDir)
	if err != nil {
		return err
	}
	// Reference the spill before migrating so a failure here (which aborts
	// the encoder) still leaves Cleanup able to remove, or retry removing,
	// the temp file.
	enc.spill = spill
	for _, chunk := range enc.index.chunks {
		for _, elem := range chunk {
			if err := spill.append(elem); err != nil {
				return err
			}
		}
	}
	enc.index = pageIndex{}
	return nil
}

// appendPageIndexRecord appends elem's on-disk varint tuple to b.
func appendPageIndexRecord(b []byte, elem pageIndexEntry) []byte {
	b = binary.AppendUvarint(b, uint64(elem.pgno))
	b = binary.AppendUvarint(b, uint64(elem.offset))
	b = binary.AppendUvarint(b, uint64(elem.size))
	return b
}

// indexSpill is a buffered temp file holding page index records in their
// final on-disk format.
type indexSpill struct {
	f    *os.File
	w    *bufio.Writer
	buf  []byte
	path string // retained after a failed removal so it can be retried
}

func newIndexSpill(dir string) (*indexSpill, error) {
	f, err := os.CreateTemp(dir, ".ltx-page-index-*.tmp")
	if err != nil {
		return nil, err
	}
	return &indexSpill{f: f, w: bufio.NewWriterSize(f, 256<<10), buf: make([]byte, 0, 3*binary.MaxVarintLen64)}, nil
}

func (s *indexSpill) append(elem pageIndexEntry) error {
	s.buf = appendPageIndexRecord(s.buf[:0], elem)
	_, err := s.w.Write(s.buf)
	return err
}

// copyTo flushes the spill file and streams its records through enc.write so
// they are hashed and counted exactly as in-memory records are.
func (s *indexSpill) copyTo(enc *Encoder) error {
	if err := s.w.Flush(); err != nil {
		return err
	}
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	buf := make([]byte, 256<<10)
	for {
		n, err := s.f.Read(buf)
		if n > 0 {
			if _, werr := enc.write(buf[:n]); werr != nil {
				return werr
			}
		}
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
	}
}

// remove closes and deletes the spill file. The handle is closed before the
// file is removed so the sequence is valid on Windows too. Safe to call more
// than once; a failed removal keeps the path so it can be retried.
func (s *indexSpill) remove() error {
	if s.f != nil {
		s.path = s.f.Name()
		err := s.f.Close()
		s.f = nil
		if err != nil {
			return err
		}
	}
	if s.path == "" {
		return nil
	}
	if err := os.Remove(s.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	s.path = ""
	return nil
}

// write to the uncompressed writer & add to the checksum.
func (enc *Encoder) write(b []byte) (int, error) {
	n, err := enc.w.Write(b)
	if err == nil && n < len(b) {
		err = io.ErrShortWrite
	}
	enc.writeToHash(b[:n])
	return n, err
}

func (enc *Encoder) writeToHash(b []byte) {
	_, _ = enc.hash.Write(b)
	enc.n += int64(len(b))
}

// pageIndexEntry is the encoder's in-memory page index element: a compact
// 24-byte struct rather than a map entry. Large snapshots retain one element
// per page, and the map representation cost roughly 4x as much memory per
// page plus rehash spikes while growing (litestream issue #1477).
type pageIndexEntry struct {
	pgno   uint32
	offset int64
	size   int64
}

const (
	// pageIndexMinChunk is the capacity of the first index chunk (6 KiB), so
	// the many small LTX files written on every sync stay cheap.
	pageIndexMinChunk = 1 << 8
	// pageIndexMaxChunk caps chunk capacity (1.5 MiB per chunk).
	pageIndexMaxChunk = 1 << 16
)

// pageIndex is an append-only sequence of pageIndexEntry stored in chunks
// whose capacity doubles from pageIndexMinChunk up to pageIndexMaxChunk.
// Chunking keeps memory proportional to the pages actually encoded: there is
// no upfront allocation sized from the (caller-supplied) header commit count
// and no copy of the whole index when it grows.
type pageIndex struct {
	chunks [][]pageIndexEntry
	n      int // total entries
}

func (idx *pageIndex) append(e pageIndexEntry) {
	idx.n++
	n := len(idx.chunks)
	if n == 0 || len(idx.chunks[n-1]) == cap(idx.chunks[n-1]) {
		size := pageIndexMinChunk
		if n > 0 {
			size = min(2*cap(idx.chunks[n-1]), pageIndexMaxChunk)
		}
		idx.chunks = append(idx.chunks, make([]pageIndexEntry, 0, size))
	}
	last := &idx.chunks[len(idx.chunks)-1]
	*last = append(*last, e)
}

type PageIndexElem struct {
	Level   int
	MinTXID TXID
	MaxTXID TXID

	Offset int64
	Size   int64
}
