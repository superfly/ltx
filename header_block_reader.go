package ltx

import (
	"fmt"
	"hash"
	"hash/crc64"
	"io"
)

type HeaderBlockReader struct {
	r  io.Reader
	lr io.LimitedReader

	hdr   Header
	state string

	hash hash.Hash64
	n    int // bytes read

	pageHeadersRead  uint32
	eventHeadersRead uint32
	eventDataRead    uint32
}

func NewHeaderBlockReader(r io.Reader) *HeaderBlockReader {
	return &HeaderBlockReader{
		r:     r,
		lr:    io.LimitedReader{R: r, N: 0},
		state: stateHeader,
		hash:  crc64.New(crc64.MakeTable(crc64.ISO)),
	}
}

// Checksum returns the checksum of the file. Only valid after close.
func (r *HeaderBlockReader) Checksum() uint64 {
	return r.hash.Sum64()
}

// Close verifies the reader is at the end of the file and that the checksum matches.
func (r *HeaderBlockReader) Close() error {
	if r.state == stateClosed {
		return nil // no-op
	} else if r.state != stateClose {
		return fmt.Errorf("cannot close, expected %s", r.state)
	}

	// Compare checksum with checksum in header.
	if r.Checksum() != r.hdr.HeaderBlockChecksum {
		return ErrHeaderBlockChecksumMismatch
	}

	// Update state to mark as closed.
	r.state = stateClosed

	return nil
}

// ReadHeader returns the LTX file header frame.
func (r *HeaderBlockReader) ReadHeader(hdr *Header) error {
	b := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r.r, b); err != nil {
		return err
	} else if err := hdr.UnmarshalBinary(b); err != nil {
		return fmt.Errorf("unmarshal header frame: %w", err)
	}

	_, _ = r.hash.Write(b[:HeaderBlockChecksumOffset])
	r.n += len(b)

	r.hdr = *hdr
	r.state = statePageHeader

	return hdr.Validate()
}

// ReadPageHeader reads the next page header into hdr and initializes reader
// to read associated page data.
func (r *HeaderBlockReader) ReadPageHeader(hdr *PageHeader) error {
	if r.state == stateClosed {
		return ErrReaderClosed
	} else if r.state != statePageHeader && r.state != stateClose {
		return fmt.Errorf("cannot read page header, expected %s", r.state)
	}

	b := make([]byte, PageHeaderSize)
	if _, err := io.ReadFull(r.r, b); err != nil {
		return err
	} else if err := hdr.UnmarshalBinary(b); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	_, _ = r.hash.Write(b)
	r.n += len(b)

	r.pageHeadersRead++
	if r.pageHeadersRead == r.hdr.PageN {
		if r.hdr.EventN > 0 {
			r.state = stateEventHeader
		} else {
			r.state = stateClose
		}
	}

	if err := hdr.Validate(); err != nil {
		return err
	}

	if r.state == stateClose {
		if err := r.readPadding(); err != nil {
			return fmt.Errorf("cannot read header block padding after page header: %w", err)
		}
	}

	return nil
}

// ReadEventHeader returns true if more events are available.
func (r *HeaderBlockReader) ReadEventHeader(hdr *EventHeader) error {
	if r.state == stateClosed {
		return ErrReaderClosed
	} else if r.state != stateEventHeader && r.state != statePageData {
		return fmt.Errorf("cannot read event header, expected %s", r.state)
	}

	b := make([]byte, EventHeaderSize)
	if _, err := io.ReadFull(r.r, b); err != nil {
		return err
	} else if err := hdr.UnmarshalBinary(b); err != nil {
		return fmt.Errorf("unmarshal event header: %w", err)
	}

	_, _ = r.hash.Write(b)
	r.n += len(b)

	if err := hdr.Validate(); err != nil {
		return err
	}

	// Set read limiter for event size.
	r.lr.N = int64(hdr.Size)
	r.state = stateEventData
	r.eventHeadersRead++

	return nil
}

// Read reads bytes for an event or page data frame.
// Only valid after a call to ReadEventHeader() or ReadPageHeader().
func (r *HeaderBlockReader) Read(p []byte) (n int, err error) {
	if r.state != stateEventData {
		return 0, io.EOF
	}

	n, err = r.lr.Read(p)
	_, _ = r.hash.Write(p[:n])
	r.n += n
	if err != nil && err != io.EOF {
		return n, err
	} else if r.lr.N > 0 {
		return n, nil // more to read
	}

	// More events to read, move state back to event header.
	r.eventDataRead++
	if r.eventHeadersRead < r.hdr.EventN {
		r.state = stateEventHeader
		return n, nil
	}

	// No more events to read, close reader.
	r.state = stateClose
	if err := r.readPadding(); err != nil {
		return n, fmt.Errorf("cannot read header block padding after event data: %w", err)
	}
	return n, nil
}

func (r *HeaderBlockReader) readPadding() error {
	sz := PageAlign(int64(r.n), r.hdr.PageSize) - int64(r.n)
	if sz == 0 {
		return nil
	}

	written, err := io.CopyN(r.hash, r.r, sz)
	r.n += int(written)
	return err
}
