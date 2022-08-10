package ltx

import (
	"fmt"
	"io"
)

// FileSpec is an in-memory representation of an LTX file. Typically used for testing.
type FileSpec struct {
	Header  Header
	Pages   []PageSpec
	Trailer Trailer
}

// Write encodes a file spec to a file.
func (s *FileSpec) WriteTo(dst io.Writer) (n int64, err error) {
	w := NewWriter(dst)
	if err := w.WriteHeader(s.Header); err != nil {
		return 0, fmt.Errorf("write header: %s", err)
	}

	for i, page := range s.Pages {
		if err := w.WritePage(page.Header, page.Data); err != nil {
			return 0, fmt.Errorf("write page[%d]: %s", i, err)
		}
	}

	w.SetPostApplyChecksum(s.Trailer.PostApplyChecksum)

	if err := w.Close(); err != nil {
		return 0, fmt.Errorf("close writer: %s", err)
	}

	// Update checksums.
	s.Trailer = w.Trailer()

	return w.N(), nil
}

// ReadFromFile encodes a file spec to a file. Always return n of zero.
func (s *FileSpec) ReadFrom(src io.Reader) (n int, err error) {
	r := NewReader(src)

	// Read header frame and initialize spec slices to correct size.
	if err := r.ReadHeader(); err != nil {
		return 0, fmt.Errorf("read header: %s", err)
	}
	s.Header = r.Header()

	// Read page frames.
	for {
		page := PageSpec{Data: make([]byte, s.Header.PageSize)}
		if err := r.ReadPage(&page.Header, page.Data); err == io.EOF {
			break
		} else if err != nil {
			return 0, fmt.Errorf("read page header: %s", err)
		}

		s.Pages = append(s.Pages, page)
	}

	if err := r.Close(); err != nil {
		return 0, fmt.Errorf("close reader: %s", err)
	}
	s.Trailer = r.Trailer()

	return int(r.N()), nil
}

// PageSpec is an in-memory representation of an LTX page frame. Typically used for testing.
type PageSpec struct {
	Header PageHeader
	Data   []byte
}
