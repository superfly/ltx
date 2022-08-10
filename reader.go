package ltx

import (
	"fmt"
	"hash"
	"hash/crc64"
	"io"
)

// Reader represents a reader of an LTX file.
type Reader struct {
	r io.Reader

	lr io.LimitedReader

	header  Header
	trailer Trailer
	state   string

	hash hash.Hash64
	n    int64 // bytes read
}

// NewReader returns a new instance of Reader.
func NewReader(r io.Reader) *Reader {
	return &Reader{
		r:     r,
		lr:    io.LimitedReader{R: r, N: 0},
		state: stateHeader,
		hash:  crc64.New(crc64.MakeTable(crc64.ISO)),
	}
}

// N returns the number of bytes read.
func (r *Reader) N() int64 { return r.n }

// Header returns a copy of the header.
func (r *Reader) Header() Header { return r.header }

// Trailer returns a copy of the trailer. File checksum available after Close().
func (r *Reader) Trailer() Trailer { return r.trailer }

// Checksum returns the checksum of the file. Only valid after close.
func (r *Reader) Checksum() uint64 {
	return ChecksumFlag | r.hash.Sum64()
}

// Close verifies the reader is at the end of the file and that the checksum matches.
func (r *Reader) Close() error {
	if r.state == stateClosed {
		return nil // no-op
	} else if r.state != stateClose {
		return fmt.Errorf("cannot close, expected %s", r.state)
	}

	// Read trailer.
	b := make([]byte, TrailerSize)
	if _, err := io.ReadFull(r.r, b); err != nil {
		return err
	} else if err := r.trailer.UnmarshalBinary(b); err != nil {
		return fmt.Errorf("unmarshal trailer: %w", err)
	}

	r.writeToHash(b[:TrailerChecksumOffset])

	// Compare checksum with checksum in header.
	if chksum := r.Checksum(); chksum != r.trailer.FileChecksum {
		println("dbg/chksum", chksum, r.trailer.FileChecksum)
		return ErrChecksumMismatch
	}

	// Update state to mark as closed.
	r.state = stateClosed

	return nil
}

// ReadHeader reads the LTX file header frame and stores it internally.
// Call Header() to retrieve the header after this is successfully called.
func (r *Reader) ReadHeader() error {
	b := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r.r, b); err != nil {
		return err
	} else if err := r.header.UnmarshalBinary(b); err != nil {
		return fmt.Errorf("unmarshal header: %w", err)
	}

	r.writeToHash(b)
	r.state = statePage

	return r.header.Validate()
}

// ReadPage reads the next page header into hdr and associated page data.
func (r *Reader) ReadPage(hdr *PageHeader, data []byte) error {
	if r.state == stateClosed {
		return ErrReaderClosed
	} else if r.state == stateClose {
		return io.EOF
	} else if r.state != statePage {
		return fmt.Errorf("cannot read page header, expected %s", r.state)
	} else if uint32(len(data)) != r.header.PageSize {
		return fmt.Errorf("invalid page buffer size: %d, expecting %d", len(data), r.header.PageSize)
	}

	// Read and unmarshal page header.
	b := make([]byte, PageHeaderSize)
	if _, err := io.ReadFull(r.r, b); err != nil {
		return err
	} else if err := hdr.UnmarshalBinary(b); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	r.writeToHash(b)

	// An empty page header indicates the end of the page block.
	if hdr.IsZero() {
		r.state = stateClose
		return io.EOF
	}

	if err := hdr.Validate(); err != nil {
		return err
	}

	// Read page data next.
	if _, err := io.ReadFull(r.r, data); err != nil {
		return err
	}
	r.writeToHash(data)

	return nil
}

// Verify reads the entire file and returns the header & trailer.
// All page data is discarded.
func (r *Reader) Verify() (Header, Trailer, error) {
	if err := r.ReadHeader(); err != nil {
		return Header{}, Trailer{}, fmt.Errorf("read header: %w", err)
	}

	var pageHeader PageHeader
	data := make([]byte, r.header.PageSize)
	for i := 0; ; i++ {
		if err := r.ReadPage(&pageHeader, data); err == io.EOF {
			break
		} else if err != nil {
			return Header{}, Trailer{}, fmt.Errorf("read page %d: %w", i, err)
		}
	}

	if err := r.Close(); err != nil {
		return Header{}, Trailer{}, fmt.Errorf("close reader: %w", err)
	}
	return r.header, r.trailer, nil
}

func (r *Reader) writeToHash(b []byte) {
	_, _ = r.hash.Write(b)
	r.n += int64(len(b))
}
