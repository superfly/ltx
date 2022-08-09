package ltx

import (
	"fmt"
	"hash"
	"hash/crc64"
	"io"
)

// Writer implements a writer an LTX file.
type Writer struct {
	w     io.Writer
	state string

	header  Header
	trailer Trailer
	hash    hash.Hash64
	n       int64 // bytes written

	// Track how many of each write has occurred to move state.
	prevPgno     uint32
	pagesWritten uint32
}

// NewWriter returns a new instance of Writer.
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		w:     w,
		state: stateHeader,
	}
}

// N returns the number of bytes written.
func (w *Writer) N() int64 { return w.n }

// Header returns a copy of the header.
func (w *Writer) Header() Header { return w.header }

// Trailer returns a copy of the trailer. File checksum available after Close().
func (w *Writer) Trailer() Trailer { return w.trailer }

// SetPostApplyChecksum sets the post-apply checksum of the database.
// Must call before Close().
func (w *Writer) SetPostApplyChecksum(chksum uint64) {
	w.trailer.PostApplyChecksum = chksum
}

// Close flushes the checksum to the header.
func (w *Writer) Close() error {
	if w.state == stateClosed {
		return nil // no-op
	} else if w.state != statePage {
		return fmt.Errorf("cannot close, expected %s", w.state)
	}

	// Marshal empty page header to mark end of page block.
	b0, err := (&PageHeader{}).MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal empty page header: %w", err)
	} else if _, err := w.w.Write(b0); err != nil {
		return fmt.Errorf("write empty page header: %w", err)
	}
	w.writeToHash(b0)

	// Marshal trailer to bytes.
	b1, err := w.trailer.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal trailer: %w", err)
	}
	w.writeToHash(b1[:TrailerChecksumOffset])
	w.trailer.FileChecksum = ChecksumFlag | w.hash.Sum64()

	// Remarshal with correct checksum.
	b1, err = w.trailer.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal trailer: %w", err)
	} else if _, err := w.w.Write(b1); err != nil {
		return fmt.Errorf("write trailer: %w", err)
	}
	w.n += ChecksumSize

	w.state = stateClosed

	return nil
}

// WriteHeader writes hdr to the file's header block.
func (w *Writer) WriteHeader(hdr Header) error {
	if w.state == stateClosed {
		return ErrWriterClosed
	} else if w.state != stateHeader {
		return fmt.Errorf("cannot write header frame, expected %s", w.state)
	} else if err := hdr.Validate(); err != nil {
		return err
	}

	w.header = hdr

	// Write header to underlying writer.
	b, err := w.header.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal header: %w", err)
	} else if _, err := w.w.Write(b); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	// Begin computing the checksum with the upper bytes of the header.
	w.hash = crc64.New(crc64.MakeTable(crc64.ISO))
	w.writeToHash(b)

	// Move writer state to write page headers.
	w.state = statePage // file must have at least one page

	return nil
}

// WritePage writes hdr & data to the file's page block.
func (w *Writer) WritePage(hdr PageHeader, data []byte) (err error) {
	if w.state == stateClosed {
		return ErrWriterClosed
	} else if w.state != statePage {
		return fmt.Errorf("cannot write page header, expected %s", w.state)
	} else if hdr.Pgno > w.header.Commit {
		return fmt.Errorf("page number %d out-of-bounds for commit size %d", hdr.Pgno, w.header.Commit)
	} else if err := hdr.Validate(); err != nil {
		return err
	} else if uint32(len(data)) != w.header.PageSize {
		return fmt.Errorf("invalid page buffer size: %d, expecting %d", len(data), w.header.PageSize)
	}

	// Snapshots must start with page 1 and include all pages up to the commit size.
	// Non-snapshot files can include any pages but they must be in order.
	if w.header.IsSnapshot() {
		if w.prevPgno == 0 && hdr.Pgno != 1 {
			return fmt.Errorf("snapshot transaction file must start with page number 1")
		} else if w.prevPgno != 0 && hdr.Pgno != w.prevPgno+1 {
			return fmt.Errorf("nonsequential page numbers in snapshot transaction: %d,%d", w.prevPgno, hdr.Pgno)
		}
	} else {
		if w.prevPgno >= hdr.Pgno {
			return fmt.Errorf("out-of-order page numbers: %d,%d", w.prevPgno, hdr.Pgno)
		}
	}

	// Encode & write header.
	b, err := hdr.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	} else if _, err := w.w.Write(b); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	w.writeToHash(b)

	// Write data to file.
	if _, err = w.w.Write(data); err != nil {
		return fmt.Errorf("write page data: %w", err)
	}
	w.writeToHash(data)

	w.pagesWritten++
	w.prevPgno = hdr.Pgno
	return nil
}

func (w *Writer) writeToHash(b []byte) {
	_, _ = w.hash.Write(b)
	w.n += int64(len(b))
}
