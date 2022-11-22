package ltx

import (
	"errors"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
)

// Reader is a passthrough that validates the contents of the underlying reader.
type Reader struct {
	r     io.Reader
	state string
	buf   []byte // header buffer

	header  Header
	trailer Trailer
	pageN   int
	hash    hash.Hash64
}

// NewReader returns a new instance of Reader.
func NewReader(r io.Reader) *Reader {
	return &Reader{
		r:     r,
		state: stateHeader,
		hash:  crc64.New(crc64.MakeTable(crc64.ISO)),
	}
}

// Header returns a copy of the header. Available after DecodeHeader().
func (r *Reader) Header() Header { return r.header }

// Trailer returns a copy of the trailer. Available after Close().
func (r *Reader) Trailer() Trailer { return r.trailer }

// PageN returns the number of pages read from the underlying LTX file.
func (r *Reader) PageN() int { return r.pageN }

// PeekHeader reads the header into a buffer and allows the caller to inspect it.
func (r *Reader) PeekHeader() error {
	if r.state != stateHeader {
		return fmt.Errorf("ltx header already read")
	}

	buf := make([]byte, HeaderSize)
	_, err := r.Read(buf)
	r.buf = buf
	return err
}

// Read reads bytes from the underlying reader into p.
// Returns io.ErrShortBuffer if len(p) is less than the size of the page frame.
func (r *Reader) Read(p []byte) (n int, err error) {
	// Use the temporary buffer if set. This is used for peeking at the header.
	if r.buf != nil {
		if len(p) < len(r.buf) {
			return 0, io.ErrShortBuffer
		}

		// Copy and remove buffer.
		n := len(r.buf)
		copy(p[:n], r.buf[:n])
		r.buf = nil
		return n, nil
	}

	switch r.state {
	case stateHeader:
		return r.readHeader(p)
	case statePage:
		return r.readPage(p)
	case stateClose:
		return r.readTrailer(p)
	default: // closed
		return 0, io.EOF
	}
}

func (r *Reader) readHeader(p []byte) (n int, err error) {
	if len(p) < HeaderSize {
		return 0, io.ErrShortBuffer
	}

	if n, err := io.ReadFull(r.r, p[:HeaderSize]); err != nil {
		return n, err
	} else if err := r.header.UnmarshalBinary(p[:HeaderSize]); err != nil {
		return n, fmt.Errorf("unmarshal header: %w", err)
	}

	_, _ = r.hash.Write(p[:HeaderSize])
	r.state = statePage

	return HeaderSize, r.header.Validate()
}

func (r *Reader) readPage(p []byte) (n int, err error) {
	pageFrameSize := int(PageHeaderSize + r.header.PageSize)
	if len(p) < pageFrameSize {
		return 0, io.ErrShortBuffer
	}

	if n, err = io.ReadFull(r.r, p[:PageHeaderSize]); err != nil {
		return n, err
	}
	_, _ = r.hash.Write(p[:n])

	var pageHeader PageHeader
	if err := pageHeader.UnmarshalBinary(p); err != nil {
		return n, fmt.Errorf("unmarshal page header: %w", err)
	}
	if pageHeader.IsZero() {
		r.state = stateClose // end of page block
		return n, nil
	}
	if err := pageHeader.Validate(); err != nil {
		return n, err
	}

	// Read page data.
	pData := p[PageHeaderSize:pageFrameSize]
	if n, err := io.ReadFull(r.r, pData); err != nil {
		return PageHeaderSize + n, err
	}
	_, _ = r.hash.Write(pData)
	r.pageN++

	return pageFrameSize, nil
}

func (r *Reader) readTrailer(p []byte) (n int, err error) {
	if len(p) < TrailerSize {
		return 0, io.ErrShortBuffer
	}

	if n, err = io.ReadFull(r.r, p[:TrailerSize]); err != nil {
		return n, err
	}
	_, _ = r.hash.Write(p[:TrailerChecksumOffset])

	if err := r.trailer.UnmarshalBinary(p[:TrailerSize]); err != nil {
		return n, fmt.Errorf("unmarshal trailer: %w", err)
	}

	// Compare checksum with checksum in trailer.
	chksum := ChecksumFlag | r.hash.Sum64()
	if chksum != r.trailer.FileChecksum {
		return n, ErrChecksumMismatch
	}

	// Update state to mark as closed.
	r.state = stateClosed

	return n, nil
}

// WriteTo implements io.WriterTo.
// It prevents io.Copy() from using a small buffer when copying using io.ReaderFrom.
func (r *Reader) WriteTo(dst io.Writer) (written int64, err error) {
	buf := make([]byte, MaxPageSize+PageHeaderSize)

	for {
		nr, er := r.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw < 0 || nr < nw {
				nw = 0
				if ew == nil {
					ew = errors.New("invalid write result")
				}
			}
			written += int64(nw)
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return written, err
}
