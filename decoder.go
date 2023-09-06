package ltx

import (
	"fmt"
	"hash"
	"hash/crc64"
	"io"

	"github.com/pierrec/lz4/v4"
)

// Decoder represents a decoder of an LTX file.
type Decoder struct {
	underlying io.Reader // main reader
	r          io.Reader // current reader (either main or lz4)

	header  Header
	trailer Trailer
	state   string

	chksum Checksum
	hash   hash.Hash64
	pageN  int   // pages read
	n      int64 // bytes read
}

// NewDecoder returns a new instance of Decoder.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{
		underlying: r,
		r:          r,
		state:      stateHeader,
		chksum:     ChecksumFlag,
		hash:       crc64.New(crc64.MakeTable(crc64.ISO)),
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

// Close verifies the reader is at the end of the file and that the checksum matches.
func (dec *Decoder) Close() error {
	if dec.state == stateClosed {
		return nil // no-op
	} else if dec.state != stateClose {
		return fmt.Errorf("cannot close, expected %s", dec.state)
	}

	// Read trailer.
	b := make([]byte, TrailerSize)
	if _, err := io.ReadFull(dec.r, b); err != nil {
		return err
	} else if err := dec.trailer.UnmarshalBinary(b); err != nil {
		return fmt.Errorf("unmarshal trailer: %w", err)
	}

	// TODO: Ensure last read page is equal to the commit for snapshot LTX files

	dec.writeToHash(b[:TrailerChecksumOffset])

	// Compare checksum with checksum in trailer.
	if chksum := ChecksumFlag | Checksum(dec.hash.Sum64()); chksum != dec.trailer.FileChecksum {
		return ErrChecksumMismatch
	}

	// Verify post-apply checksum for snapshot files.
	if dec.header.IsSnapshot() {
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

	// Use LZ4 reader if compression is enabled.
	if dec.header.Flags&HeaderFlagCompressLZ4 != 0 {
		dec.r = lz4.NewReader(dec.underlying)
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
		// Revert back to regular reader if we were using a compressed reader.
		// We need to read off the LZ4 trailer frame to ensure we hit EOF.
		if zr, ok := dec.r.(*lz4.Reader); ok {
			if _, err := io.ReadFull(zr, make([]byte, 1)); err != io.EOF {
				return fmt.Errorf("expected lz4 end frame")
			}
			dec.r = dec.underlying
		}

		dec.state = stateClose
		return io.EOF
	}

	if err := hdr.Validate(); err != nil {
		return err
	}

	// Read page data next.
	if _, err := io.ReadFull(dec.r, data); err != nil {
		return err
	}
	dec.writeToHash(data)
	dec.pageN++

	// Calculate checksum while decoding snapshots.
	if dec.header.IsSnapshot() {
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
