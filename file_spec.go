package ltx

import (
	"fmt"
	"io"

	"github.com/superfly/ltx/internal"
)

// FileSpec is an in-memory representation of an LTX file. Typically used for testing.
type FileSpec struct {
	Header       Header
	PageHeaders  []PageHeader
	EventHeaders []EventFrameHeader
	EventData    [][]byte
	PageData     [][]byte
}

// MustBytes returns the encoded file spec. Panic on error.
func (s *FileSpec) MustBytes() []byte {
	b, err := s.Bytes()
	if err != nil {
		panic(err)
	}
	return b
}

// Bytes returns the encoded file spec.
func (s *FileSpec) Bytes() ([]byte, error) {
	var buf internal.Buffer
	if x, y := len(s.PageHeaders), len(s.PageData); x != y {
		return nil, fmt.Errorf("page header count and page data count mismatch: %d != %d", x, y)
	}
	if x, y := len(s.EventHeaders), len(s.EventData); x != y {
		return nil, fmt.Errorf("event header count and event data count mismatch: %d != %d", x, y)
	}

	hw := NewHeaderBlockWriter(&buf)
	if err := hw.WriteHeader(s.Header); err != nil {
		return nil, fmt.Errorf("write header: %s", err)
	}

	for i, hdr := range s.PageHeaders {
		if err := hw.WritePageHeader(hdr); err != nil {
			return nil, fmt.Errorf("write page header[%d]: %s", i, err)
		}
	}

	for i, hdr := range s.EventHeaders {
		data := s.EventData[i]
		if err := hw.WriteEventHeader(hdr); err != nil {
			return nil, fmt.Errorf("write event header[%d]: %s", i, err)
		} else if _, err := hw.Write(data); err != nil {
			return nil, fmt.Errorf("write event data[%d]: %s", i, err)
		}
	}

	pw := NewPageBlockWriter(&buf, s.Header.PageN, s.Header.PageSize)
	for i, data := range s.PageData {
		if _, err := pw.Write(data); err != nil {
			return nil, fmt.Errorf("write page data[%d]: %s", i, err)
		}
	}
	hw.SetPageBlockChecksum(pw.Checksum())

	if err := pw.Close(); err != nil {
		return nil, fmt.Errorf("close page block writer: %s", err)
	} else if err := hw.Close(); err != nil {
		return nil, fmt.Errorf("close header block writer: %s", err)
	}

	// Update checksums on s.
	s.Header.HeaderBlockChecksum = hw.Checksum()
	s.Header.PageBlockChecksum = pw.Checksum()

	return buf.Bytes(), nil
}

// Write encodes a file spec to a file.
func (s *FileSpec) WriteTo(w io.Writer) (n int64, err error) {
	b, err := s.Bytes()
	if err != nil {
		return 0, err
	}
	nn, err := w.Write(b)
	return int64(nn), err
}

// ReadFromFile encodes a file spec to a file. Always return n of zero.
func (s *FileSpec) ReadFrom(r io.Reader) (n int, err error) {
	hr := NewHeaderBlockReader(r)

	// Read header frame and initialize spec slices to correct size.
	if err := hr.ReadHeader(&s.Header); err != nil {
		return 0, fmt.Errorf("read header: %s", err)
	}

	s.PageHeaders = make([]PageHeader, s.Header.PageN)
	s.PageData = make([][]byte, s.Header.PageN)

	if s.Header.EventFrameN > 0 {
		s.EventHeaders = make([]EventFrameHeader, s.Header.EventFrameN)
		s.EventData = make([][]byte, s.Header.EventFrameN)
	}

	// Read page headers.
	for i := range s.PageHeaders {
		hdr := &s.PageHeaders[i]
		if err := hr.ReadPageHeader(hdr); err != nil {
			return 0, fmt.Errorf("read page header[%d]: %s", i, err)
		}
	}

	// Read event frames.
	for i := range s.EventHeaders {
		hdr := &s.EventHeaders[i]
		if err := hr.ReadEventHeader(hdr); err != nil {
			return 0, fmt.Errorf("read event header[%d]: %s", i, err)
		}

		s.EventData[i] = make([]byte, hdr.Size)
		if _, err := io.ReadFull(hr, s.EventData[i]); err != nil {
			return 0, fmt.Errorf("read event data[%d]: %s", i, err)
		}
	}

	// Open page block reader to read remaining page data from reader.
	pr := NewPageBlockReader(r, s.Header.PageN, s.Header.PageSize, s.Header.PageBlockChecksum)
	for i := range s.PageData {
		s.PageData[i] = make([]byte, s.Header.PageSize)
		if _, err := io.ReadFull(pr, s.PageData[i]); err != nil {
			return 0, fmt.Errorf("read page data[%d]: %s", i, err)
		}
	}

	// Verify checksums.
	if err := hr.Close(); err != nil {
		return 0, fmt.Errorf("close header block reader: %s", err)
	} else if err := pr.Close(); err != nil {
		return 0, fmt.Errorf("close header block reader: %s", err)
	}

	return 0, nil
}
