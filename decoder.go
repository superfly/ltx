package ltx

import (
	"bytes"
	"encoding/binary"
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

	chksum Checksum
	hash   hash.Hash64
	pageN  int   // pages read
	n      int64 // bytes read
}

// NewDecoder returns a new instance of Decoder.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{
		r:     r,
		zr:    lz4.NewReader(r),
		state: stateHeader,
		hash:  crc64.New(crc64.MakeTable(crc64.ISO)),
	}
}

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

	// Slurp the remaining data in to memory so we can use the ByteReader interface.
	remainingBytes, err := io.ReadAll(dec.r)
	if err != nil {
		return fmt.Errorf("read all: %w", err)
	}
	remaining := bytes.NewReader(remainingBytes)

	// Write everything but the file checksum to the hash.
	dec.writeToHash(remainingBytes[:len(remainingBytes)-ChecksumSize])

	// Read page index.
	if dec.pageIndex, err = DecodePageIndex(remaining, 0, dec.header.MinTXID, dec.header.MaxTXID); err != nil {
		return fmt.Errorf("read page index: %w", err)
	}

	// Read trailer.
	b := make([]byte, TrailerSize)
	if _, err := io.ReadFull(remaining, b); err != nil {
		return err
	} else if err := dec.trailer.UnmarshalBinary(b); err != nil {
		return fmt.Errorf("unmarshal trailer: %w", err)
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

// DecodePageIndex decodes the page index from r.
func DecodePageIndex(r io.ByteReader, level int, minTXID, maxTXID TXID) (map[uint32]PageIndexElem, error) {
	pageIndex := make(map[uint32]PageIndexElem)

	for {
		pgno, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, fmt.Errorf("read page index pgno: %w", err)
		} else if pgno == 0 {
			break // End when we hit the end marker.
		}

		offset, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, fmt.Errorf("read page index offset: %w", err)
		}
		size, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, fmt.Errorf("read page index size: %w", err)
		}

		pageIndex[uint32(pgno)] = PageIndexElem{
			Level:   level,
			MinTXID: minTXID,
			MaxTXID: maxTXID,
			Offset:  int64(offset),
			Size:    int64(size),
		}
	}

	// Read size of page index.
	var size uint64
	if err := binary.Read(r.(io.Reader), binary.BigEndian, &size); err != nil {
		return nil, fmt.Errorf("read page index size: %w", err)
	}

	return pageIndex, nil
}
