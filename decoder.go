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

	hash hash.Hash64
	n    int64 // bytes read
}

// NewDecoder returns a new instance of Decoder.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{
		underlying: r,
		r:          r,
		state:      stateHeader,
		hash:       crc64.New(crc64.MakeTable(crc64.ISO)),
	}
}

// N returns the number of bytes read.
func (dec *Decoder) N() int64 { return dec.n }

// Header returns a copy of the header.
func (dec *Decoder) Header() Header { return dec.header }

// Trailer returns a copy of the trailer. File checksum available after Close().
func (dec *Decoder) Trailer() Trailer { return dec.trailer }

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

	dec.writeToHash(b[:TrailerChecksumOffset])

	// Compare checksum with checksum in trailer.
	if chksum := ChecksumFlag | dec.hash.Sum64(); chksum != dec.trailer.FileChecksum {
		return ErrChecksumMismatch
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
	if dec.header.Flags&HeaderFlagCompressLZ4 == 1 {
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

	return nil
}

// Verify reads the entire file and returns the header & trailer.
// All page data is discarded.
func (dec *Decoder) Verify() (Header, Trailer, error) {
	if err := dec.DecodeHeader(); err != nil {
		return Header{}, Trailer{}, fmt.Errorf("read header: %w", err)
	}

	var pageHeader PageHeader
	data := make([]byte, dec.header.PageSize)
	for i := 0; ; i++ {
		if err := dec.DecodePage(&pageHeader, data); err == io.EOF {
			break
		} else if err != nil {
			return Header{}, Trailer{}, fmt.Errorf("read page %d: %w", i, err)
		}
	}

	if err := dec.Close(); err != nil {
		return Header{}, Trailer{}, fmt.Errorf("close reader: %w", err)
	}
	return dec.header, dec.trailer, nil
}

func (dec *Decoder) writeToHash(b []byte) {
	_, _ = dec.hash.Write(b)
	dec.n += int64(len(b))
}
