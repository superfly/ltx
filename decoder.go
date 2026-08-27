package ltx

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"math"

	"github.com/pierrec/lz4/v4"
)

// lz4FrameFooterSize is the size of the LZ4 frame footer:
// EndMark (4 bytes) + Content Checksum (4 bytes).
// Used when decoding old format files without compressed size prefix.
const lz4FrameFooterSize = 8

// Decoder represents a decoder of an LTX file.
type Decoder struct {
	r  io.Reader        // main reader
	lr io.LimitedReader // limited reader for lz4 (reused)
	zr *lz4.Reader      // lz4 reader

	header    Header
	trailer   Trailer
	pageIndex map[uint32]PageIndexElem
	state     string

	// retainPageIndex controls whether Close materializes the page index map.
	// Callers that only stream pages (e.g. compaction inputs) turn it off so a
	// database-sized map is never built.
	retainPageIndex bool

	chksum Checksum
	hash   hash.Hash64
	pageN  int   // pages read
	n      int64 // bytes read

	// pageSeq is a running hash of the decoded page numbers in order; Close
	// compares it with the same hash over the page index so the index must
	// name exactly the decoded pages, without retaining them.
	pageSeq hash.Hash64
}

// NewDecoder returns a new instance of Decoder.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{
		r:               r,
		zr:              lz4.NewReader(r),
		state:           stateHeader,
		hash:            crc64.New(crc64.MakeTable(crc64.ISO)),
		pageSeq:         crc64.New(crc64.MakeTable(crc64.ISO)),
		retainPageIndex: true,
	}
}

// SetRetainPageIndex controls whether Close builds the in-memory page index
// returned by PageIndex. It defaults to true. When false, Close still reads,
// validates, and checksums the index but discards the entries, so memory no
// longer scales with the number of pages in the file; PageIndex returns nil.
// Must be called before Close.
func (dec *Decoder) SetRetainPageIndex(retain bool) { dec.retainPageIndex = retain }

// N returns the number of bytes read.
func (dec *Decoder) N() int64 { return dec.n }

// PageN returns the number of pages read.
func (dec *Decoder) PageN() int { return dec.pageN }

// Header returns a copy of the header.
func (dec *Decoder) Header() Header { return dec.header }

// Trailer returns a copy of the trailer. File checksum available after Close().
func (dec *Decoder) Trailer() Trailer { return dec.trailer }

// PostApplyPos returns the replication position after underlying the LTX file is applied.
// Only valid after successful Close().
func (dec *Decoder) PostApplyPos() Pos {
	return Pos{
		TXID:              dec.header.MaxTXID,
		PostApplyChecksum: dec.trailer.PostApplyChecksum,
	}
}

// PageIndex returns a mapping of page numbers to byte offsets and sizes of those pages.
// This returns the raw reference and not a copy.
func (dec *Decoder) PageIndex() map[uint32]PageIndexElem {
	return dec.pageIndex
}

// Close verifies the reader is at the end of the file and that the checksum matches.
func (dec *Decoder) Close() error {
	if dec.state == stateClosed {
		return nil // no-op
	} else if dec.state != stateClose {
		return fmt.Errorf("cannot close, expected %s", dec.state)
	}

	// Stream the page index straight from the reader, hashing bytes as they
	// are consumed, instead of slurping the tail of the file into memory.
	// The index is only materialized when retention is requested.
	br := bufio.NewReader(dec.r)
	var index map[uint32]PageIndexElem
	if dec.retainPageIndex {
		index = make(map[uint32]PageIndexElem)
	}
	// Index entries must describe one frame per decoded page, in ascending
	// page order, with non-overlapping frames after the header. (Offsets are
	// file positions of compressed frames, which the decoder does not track,
	// so exact page-block bounds are not checked here.)
	v := pageIndexValidator{commit: dec.header.Commit, nextOffset: HeaderSize, seq: crc64.New(crc64.MakeTable(crc64.ISO))}
	if err := dec.streamPageIndex(br, func(pgno uint32, offset, size int64) error {
		if err := v.check(pgno, offset, size); err != nil {
			return err
		}
		if index != nil {
			index[pgno] = PageIndexElem{
				MinTXID: dec.header.MinTXID,
				MaxTXID: dec.header.MaxTXID,
				Offset:  offset,
				Size:    size,
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("read page index: %w", err)
	}
	if v.n != dec.pageN {
		return fmt.Errorf("page index has %d entries but %d pages were decoded", v.n, dec.pageN)
	}
	if v.seq.Sum64() != dec.pageSeq.Sum64() {
		return errors.New("page index does not match the decoded page numbers")
	}
	dec.pageIndex = index

	// Read trailer. Everything except the trailing file checksum is hashed.
	b := make([]byte, TrailerSize)
	if _, err := io.ReadFull(br, b); err != nil {
		return fmt.Errorf("read trailer: %w", err)
	}
	dec.writeToHash(b[:TrailerChecksumOffset])
	if err := dec.trailer.UnmarshalBinary(b); err != nil {
		return fmt.Errorf("unmarshal trailer: %w", err)
	}
	if _, err := br.ReadByte(); err == nil {
		return errors.New("unexpected data after trailer")
	} else if !errors.Is(err, io.EOF) {
		return fmt.Errorf("read after trailer: %w", err)
	}

	// TODO: Ensure last read page is equal to the commit for snapshot LTX files

	// Compare file checksum with checksum in trailer.
	if chksum := ChecksumFlag | Checksum(dec.hash.Sum64()); chksum != dec.trailer.FileChecksum {
		return ErrChecksumMismatch
	}

	// Verify post-apply checksum for snapshot files if checksums are being tracked.
	if dec.header.IsSnapshot() && !dec.header.NoChecksum() {
		if dec.trailer.PostApplyChecksum != dec.chksum {
			return fmt.Errorf("post-apply checksum in trailer (%s) does not match calculated checksum (%s)", dec.trailer.PostApplyChecksum, dec.chksum)
		}
	}

	// Update state to mark as closed.
	dec.state = stateClosed

	return nil
}

// DecodeHeader reads the LTX file header frame and stores it internally.
// Call Header() to retrieve the header after this is successfully called.
func (dec *Decoder) DecodeHeader() error {
	b := make([]byte, HeaderSize)
	if _, err := io.ReadFull(dec.r, b); err != nil {
		return err
	} else if err := dec.header.UnmarshalBinary(b); err != nil {
		return fmt.Errorf("unmarshal header: %w", err)
	}

	dec.writeToHash(b)
	dec.state = statePage

	if err := dec.header.Validate(); err != nil {
		return err
	}

	// Initialize checksum if checksum tracking is enabled.
	if !dec.header.NoChecksum() {
		dec.chksum = ChecksumFlag
	}

	return nil
}

// DecodePage reads the next page header into hdr and associated page data.
func (dec *Decoder) DecodePage(hdr *PageHeader, data []byte) error {
	if dec.state == stateClosed {
		return ErrDecoderClosed
	} else if dec.state == stateClose {
		return io.EOF
	} else if dec.state != statePage {
		return fmt.Errorf("cannot read page header, expected %s", dec.state)
	} else if uint32(len(data)) != dec.header.PageSize {
		return fmt.Errorf("invalid page buffer size: %d, expecting %d", len(data), dec.header.PageSize)
	}

	// Read and unmarshal page header.
	b := make([]byte, PageHeaderSize)
	if _, err := io.ReadFull(dec.r, b); err != nil {
		return err
	} else if err := hdr.UnmarshalBinary(b); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	dec.writeToHash(b)

	// An empty page header indicates the end of the page block.
	if hdr.IsZero() {
		dec.state = stateClose
		return io.EOF
	}

	if err := hdr.Validate(); err != nil {
		return err
	}

	// Read page data using format-specific approach.
	if hdr.Flags&PageHeaderFlagSize != 0 {
		// New block format: read size prefix, then LZ4 block data.
		sizeBuf := make([]byte, 4)
		if _, err := io.ReadFull(dec.r, sizeBuf); err != nil {
			return fmt.Errorf("read data size: %w", err)
		}
		dec.writeToHash(sizeBuf)
		dataSize := binary.BigEndian.Uint32(sizeBuf)

		compressed := make([]byte, dataSize)
		if _, err := io.ReadFull(dec.r, compressed); err != nil {
			return fmt.Errorf("read compressed data: %w", err)
		}
		if _, err := lz4.UncompressBlock(compressed, data); err != nil {
			return fmt.Errorf("decompress block: %w", err)
		}
	} else {
		// Old format: use LimitedReader workaround for lz4 frame concatenation.
		// The lz4 library peeks ahead after EOF to check for concatenated frames,
		// so we limit reads to prevent it from reading into the next page header.
		dec.lr.R = dec.r
		dec.lr.N = math.MaxInt64
		dec.zr.Reset(&dec.lr)

		if _, err := io.ReadFull(dec.zr, data); err != nil {
			return err
		}

		// Limit remaining reads to the LZ4 frame footer size before checking EOF.
		dec.lr.N = lz4FrameFooterSize
		if err := dec.readLZ4Trailer(); err != nil {
			return fmt.Errorf("read lz4 trailer: %w", err)
		}
	}

	dec.writeToHash(data)
	dec.pageN++
	hashPgno(dec.pageSeq, hdr.Pgno)

	// Calculate checksum while decoding snapshots if tracking checksums.
	if dec.header.IsSnapshot() && !dec.header.NoChecksum() {
		if hdr.Pgno != LockPgno(dec.header.PageSize) {
			dec.chksum = ChecksumFlag | (dec.chksum ^ ChecksumPage(hdr.Pgno, data))
		}
	}

	return nil
}

// Verify reads the entire file. Header & trailer can be accessed via methods
// after the file is successfully verified. All other data is discarded.
func (dec *Decoder) Verify() error {
	if err := dec.DecodeHeader(); err != nil {
		return fmt.Errorf("decode header: %w", err)
	}

	var pageHeader PageHeader
	data := make([]byte, dec.header.PageSize)
	for i := 0; ; i++ {
		if err := dec.DecodePage(&pageHeader, data); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("decode page %d: %w", i, err)
		}
	}

	if err := dec.Close(); err != nil {
		return fmt.Errorf("close reader: %w", err)
	}
	return nil
}

// DecodeDatabaseTo decodes the LTX file as a SQLite database to w.
// The LTX file MUST be a snapshot file.
func (dec *Decoder) DecodeDatabaseTo(w io.Writer) error {
	if err := dec.DecodeHeader(); err != nil {
		return fmt.Errorf("decode header: %w", err)
	}

	hdr := dec.Header()
	lockPgno := hdr.LockPgno()
	if !dec.header.IsSnapshot() {
		return fmt.Errorf("cannot decode non-snapshot LTX file to SQLite database")
	}

	var pageHeader PageHeader
	data := make([]byte, dec.header.PageSize)
	for pgno := uint32(1); pgno <= hdr.Commit; pgno++ {
		if pgno == lockPgno {
			// Write empty page for lock page.
			for i := range data {
				data[i] = 0
			}
		} else {
			// Otherwise read the page from the LTX decoder.
			if err := dec.DecodePage(&pageHeader, data); err != nil {
				return fmt.Errorf("decode page %d: %w", pgno, err)
			} else if pageHeader.Pgno != pgno {
				return fmt.Errorf("unexpected pgno while decoding page: read %d, expected %d", pageHeader.Pgno, pgno)
			}
		}

		if _, err := w.Write(data); err != nil {
			return fmt.Errorf("write page %d: %w", pgno, err)
		}
	}

	// Issue one more final read and expect to see an EOF. This is required so
	// that the decoder can successfully close and validate.
	if err := dec.DecodePage(&pageHeader, data); err == nil {
		return fmt.Errorf("unexpected page %d after commit %d", pageHeader.Pgno, hdr.Commit)
	} else if err != io.EOF {
		return fmt.Errorf("unexpected error decoding after end of database: %w", err)
	}

	if err := dec.Close(); err != nil {
		return fmt.Errorf("close decoder: %w", err)
	}
	return nil
}

func (dec *Decoder) writeToHash(b []byte) {
	_, _ = dec.hash.Write(b)
	dec.n += int64(len(b))
}

// readLZ4Trailer reads the LZ4 trailer frame to ensure we hit EOF.
func (dec *Decoder) readLZ4Trailer() error {
	if _, err := io.ReadFull(dec.zr, make([]byte, 1)); err != io.EOF {
		return fmt.Errorf("expected lz4 end frame")
	}
	return nil
}

// DecodeHeader decodes the header from r. Returns the header & read bytes.
func DecodeHeader(r io.Reader) (hdr Header, data []byte, err error) {
	data = make([]byte, HeaderSize)
	n, err := io.ReadFull(r, data)
	if err != nil {
		return hdr, data[:n], err
	} else if err := hdr.UnmarshalBinary(data); err != nil {
		return hdr, data[:n], err
	}
	return hdr, data, nil
}

// DecodePageData decodes the page header & data from a single frame.
func DecodePageData(b []byte) (hdr PageHeader, data []byte, err error) {
	if err := hdr.UnmarshalBinary(b); err != nil {
		return hdr, data, fmt.Errorf("unmarshal: %w", err)
	}
	if hdr.IsZero() {
		return hdr, data, nil
	}

	if hdr.Flags&PageHeaderFlagSize != 0 {
		// New block format: read size and decompress.
		if len(b) < PageHeaderSize+4 {
			return hdr, nil, fmt.Errorf("buffer too small for size prefix")
		}
		dataSize := binary.BigEndian.Uint32(b[PageHeaderSize:])
		offset := PageHeaderSize + 4

		if len(b) < offset+int(dataSize) {
			return hdr, nil, fmt.Errorf("buffer too small for data: need %d, have %d", offset+int(dataSize), len(b))
		}

		// LZ4 block compressed data.
		compressed := b[offset : offset+int(dataSize)]
		// Estimate uncompressed size - pages are typically 512-65536 bytes.
		// We'll use a reasonable upper bound and resize if needed.
		data = make([]byte, 65536)
		n, err := lz4.UncompressBlock(compressed, data)
		if err != nil {
			return hdr, nil, fmt.Errorf("decompress block: %w", err)
		}
		data = data[:n]
	} else {
		// Old frame format: use LZ4 reader.
		r := bytes.NewReader(b[PageHeaderSize:])
		zr := lz4.NewReader(r)
		data, err = io.ReadAll(zr)
	}

	return hdr, data, err
}

// streamPageIndex reads the page index section (records, end marker, and
// size field) from br, hashing every byte consumed and validating that page
// numbers ascend and that the size field matches the bytes read. fn is called
// for each record in file order.
func (dec *Decoder) streamPageIndex(br *bufio.Reader, fn func(pgno uint32, offset, size int64) error) error {
	return parsePageIndex(br, dec.writeToHash, fn)
}

// pageIndexValidator checks that index entries are structurally plausible
// without retaining them: page numbers within the commit size, and frames
// that start after the header, do not overlap, and have a positive size.
type pageIndexValidator struct {
	commit     uint32
	nextOffset int64 // earliest offset the next frame may start at
	n          int
	seq        hash.Hash64 // running hash of page numbers, compared with Decoder.pageSeq
}

func (v *pageIndexValidator) check(pgno uint32, offset, size int64) error {
	if pgno > v.commit {
		return fmt.Errorf("page index pgno %d exceeds commit %d", pgno, v.commit)
	}
	if offset < v.nextOffset {
		return fmt.Errorf("page index pgno %d offset %d overlaps previous frame ending at %d", pgno, offset, v.nextOffset)
	}
	if size <= PageHeaderSize || offset > math.MaxInt64-size {
		return fmt.Errorf("page index pgno %d has invalid frame size %d", pgno, size)
	}
	v.nextOffset = offset + size
	v.n++
	hashPgno(v.seq, pgno)
	return nil
}

// hashPgno feeds pgno into a page-sequence hash.
func hashPgno(h hash.Hash64, pgno uint32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], pgno)
	_, _ = h.Write(b[:])
}

// parsePageIndex reads page index records from br until the end marker,
// then the size field, validating that page numbers ascend and that the size
// field equals the bytes consumed. observe, if non-nil, receives every byte
// consumed (for checksumming); fn receives every record.
func parsePageIndex(br io.ByteReader, observe func([]byte), fn func(pgno uint32, offset, size int64) error) error {
	var scratch [3 * binary.MaxVarintLen64]byte
	var consumed int64
	var prevPgno uint32
	for {
		buf := scratch[:0]
		pgno, err := readUvarintInto(br, &buf)
		if err != nil {
			return fmt.Errorf("read page index pgno: %w", err)
		}
		if pgno == 0 {
			if observe != nil {
				observe(buf)
			}
			consumed += int64(len(buf))
			break // end marker
		}
		if pgno > math.MaxUint32 {
			return fmt.Errorf("page index pgno %d out of range", pgno)
		}
		if uint32(pgno) <= prevPgno {
			return fmt.Errorf("page index out of order: %d after %d", pgno, prevPgno)
		}
		offset, err := readUvarintInto(br, &buf)
		if err != nil {
			return fmt.Errorf("read page index offset: %w", err)
		}
		size, err := readUvarintInto(br, &buf)
		if err != nil {
			return fmt.Errorf("read page index size: %w", err)
		}
		if offset > math.MaxInt64 || size > math.MaxInt64 {
			return fmt.Errorf("page index pgno %d offset/size out of range", pgno)
		}
		if observe != nil {
			observe(buf)
		}
		consumed += int64(len(buf))
		prevPgno = uint32(pgno)
		if err := fn(uint32(pgno), int64(offset), int64(size)); err != nil {
			return err
		}
	}

	var sizeBuf [8]byte
	for i := range sizeBuf {
		b, err := br.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return fmt.Errorf("read page index size: %w", err)
		}
		sizeBuf[i] = b
	}
	if observe != nil {
		observe(sizeBuf[:])
	}
	if indexSize := binary.BigEndian.Uint64(sizeBuf[:]); indexSize != uint64(consumed) {
		return fmt.Errorf("page index size mismatch: field=%d read=%d", indexSize, consumed)
	}
	return nil
}

// readUvarintInto reads a uvarint from br, appending the consumed bytes to *buf.
func readUvarintInto(br io.ByteReader, buf *[]byte) (uint64, error) {
	var x uint64
	var s uint
	for i := 0; i < binary.MaxVarintLen64; i++ {
		b, err := br.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) && i > 0 {
				err = io.ErrUnexpectedEOF
			}
			return 0, err
		}
		*buf = append(*buf, b)
		if b < 0x80 {
			if i == binary.MaxVarintLen64-1 && b > 1 {
				return 0, errors.New("uvarint overflows 64 bits")
			}
			return x | uint64(b)<<s, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, errors.New("uvarint overflows 64 bits")
}

// DecodePageIndex decodes the page index from r. It validates that page
// numbers ascend, that offsets and sizes are in range, and that the trailing
// size field matches the bytes read; it cannot check the index against the
// page block, so callers reading only the tail of a file must treat the
// result as untrusted range metadata.
func DecodePageIndex(r io.ByteReader, level int, minTXID, maxTXID TXID) (map[uint32]PageIndexElem, error) {
	pageIndex := make(map[uint32]PageIndexElem)
	if err := parsePageIndex(r, nil, func(pgno uint32, offset, size int64) error {
		pageIndex[pgno] = PageIndexElem{
			Level:   level,
			MinTXID: minTXID,
			MaxTXID: maxTXID,
			Offset:  offset,
			Size:    size,
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return pageIndex, nil
}
