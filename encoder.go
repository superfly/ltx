package ltx

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"slices"

	"github.com/pierrec/lz4/v4"
)

// Encoder implements an encoder for an LTX file.
type Encoder struct {
	w     io.Writer // main writer
	state string

	header  Header
	trailer Trailer
	hash    hash.Hash64
	index   map[uint32]PageIndexElem // page number to offset
	n       int64                    // bytes written

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
		w:     w,
		state: stateHeader,
		index: make(map[uint32]PageIndexElem),
	}, nil
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
func (enc *Encoder) Close() error {
	if enc.state == stateClosed {
		return nil // no-op
	} else if enc.state != statePage {
		return fmt.Errorf("cannot close, expected %s", enc.state)
	}

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
	} else if _, err := enc.w.Write(b1); err != nil {
		return fmt.Errorf("write trailer: %w", err)
	}
	enc.n += ChecksumSize

	enc.state = stateClosed

	return nil
}

func (enc *Encoder) encodePageIndex() error {
	offset := enc.n

	// Write elements in sorted page number order.
	pgnos := make([]uint32, 0, len(enc.index))
	for pgno := range enc.index {
		pgnos = append(pgnos, pgno)
	}
	slices.Sort(pgnos)

	// Write each element as a varint-encoded tuple.
	buf := make([]byte, 0, 3*binary.MaxVarintLen64)
	for _, pgno := range pgnos {
		elem := enc.index[pgno]

		buf = binary.AppendUvarint(buf[:0], uint64(pgno))
		buf = binary.AppendUvarint(buf, uint64(elem.Offset))
		buf = binary.AppendUvarint(buf, uint64(elem.Size))

		if _, err := enc.write(buf); err != nil {
			return fmt.Errorf("write page index element: %w", err)
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
	} else if enc.state != stateHeader {
		return fmt.Errorf("cannot encode header frame, expected %s", enc.state)
	} else if err := hdr.Validate(); err != nil {
		return err
	}

	enc.header = hdr

	// Initialize hash.
	enc.hash = crc64.New(crc64.MakeTable(crc64.ISO))

	// Write header to underlying writer.
	b, err := enc.header.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal header: %w", err)
	} else if _, err := enc.write(b); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	// Move writer state to write page headers.
	enc.state = statePage // file must have at least one page

	return nil
}

// EncodePage writes hdr & data to the file's page block.
func (enc *Encoder) EncodePage(hdr PageHeader, data []byte) (err error) {
	if enc.state == stateClosed {
		return ErrEncoderClosed
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

	// Write page header.
	b, err := hdr.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	} else if _, err := enc.write(b); err != nil {
		return fmt.Errorf("write page header: %w", err)
	}

	// Write data size (4 bytes, big-endian).
	sizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuf, uint32(len(writeData)))
	if _, err := enc.write(sizeBuf); err != nil {
		return fmt.Errorf("write data size: %w", err)
	}

	// Write page data (compressed or uncompressed).
	if _, err := enc.w.Write(writeData); err != nil {
		return fmt.Errorf("write page data: %w", err)
	}
	_, _ = enc.hash.Write(data) // hash the uncompressed data
	enc.n += int64(len(writeData))

	enc.pagesWritten++
	enc.prevPgno = hdr.Pgno
	enc.index[hdr.Pgno] = PageIndexElem{
		Offset: offset,
		Size:   enc.n - offset,
	}

	return nil
}

// write to the uncompressed writer & add to the checksum.
func (enc *Encoder) write(b []byte) (n int, err error) {
	n, err = enc.w.Write(b)
	enc.writeToHash(b[:n])
	return n, err
}

func (enc *Encoder) writeToHash(b []byte) {
	_, _ = enc.hash.Write(b)
	enc.n += int64(len(b))
}

type PageIndexElem struct {
	Level   int
	MinTXID TXID
	MaxTXID TXID

	Offset int64
	Size   int64
}
