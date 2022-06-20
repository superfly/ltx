package ltx

import (
	"fmt"
	"hash"
	"hash/crc64"
	"io"
)

// HeaderBlockWriter implements a writer for the header block of an LTX file.
type HeaderBlockWriter struct {
	w     io.WriteSeeker
	state string

	hdr  Header
	hash hash.Hash64
	n    int // bytes written

	// Track how many of each write has occurred to move state.
	prevPgno           uint32
	pageHeadersWritten uint32

	eventHdr               EventFrameHeader
	eventFramesWritten     uint32
	eventBytesTotal        int64
	eventFrameBytesWritten int64
}

// NewHeaderBlockWriter returns a new instance of HeaderBlockWriter.
func NewHeaderBlockWriter(w io.WriteSeeker) *HeaderBlockWriter {
	return &HeaderBlockWriter{
		w:     w,
		state: stateHeader,
	}
}

// Checksum returns the checksum of the header. Only valid after close.
func (w *HeaderBlockWriter) Checksum() uint64 {
	return w.hash.Sum64()
}

// SetPageBlockChecksum sets the checksum of the page block.
// Must call after WriteHeaderFrame() & before Close().
func (w *HeaderBlockWriter) SetPageBlockChecksum(chksum uint64) {
	w.hdr.PageBlockChecksum = chksum
}

// Close flushes the checksum to the header.
func (w *HeaderBlockWriter) Close() error {
	if w.state == stateClosed {
		return nil // no-op
	} else if w.state != stateClose {
		return fmt.Errorf("cannot close, expected %s", w.state)
	}

	// Update checksum on header.
	w.hdr.HeaderBlockChecksum = w.hash.Sum64()

	// Rewrite header with new checksum.
	if b, err := w.hdr.MarshalBinary(); err != nil {
		return fmt.Errorf("marshal header: %w", err)
	} else if _, err := w.w.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("header seek: %w", err)
	} else if _, err := w.w.Write(b); err != nil {
		return fmt.Errorf("rewrite header: %w", err)
	}

	w.state = stateClosed

	return nil
}

// WriteHeader writes hdr to the file's header block.
func (w *HeaderBlockWriter) WriteHeader(hdr Header) error {
	if w.state == stateClosed {
		return ErrWriterClosed
	} else if w.state != stateHeader {
		return fmt.Errorf("cannot write header frame, expected %s", w.state)
	} else if err := hdr.Validate(); err != nil {
		return err
	}

	w.hdr = hdr
	w.hdr.HeaderBlockChecksum = 0

	// Write header without checksums. We'll write the checksum at the end.
	b, err := w.hdr.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	} else if _, err := w.w.Write(b); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	// Begin computing the checksum with the upper bytes of the header.
	w.hash = crc64.New(crc64.MakeTable(crc64.ISO))
	_, _ = w.hash.Write(b[:HeaderBlockChecksumOffset])
	w.n += len(b)

	// Move writer state to write page headers.
	w.state = statePageHeader // file must have at least one page

	return nil
}

// WritePageHeader writes hdr to the file's page header block.
func (w *HeaderBlockWriter) WritePageHeader(hdr PageHeader) (err error) {
	if w.state == stateClosed {
		return ErrWriterClosed
	} else if w.state != statePageHeader {
		return fmt.Errorf("cannot write page header, expected %s", w.state)
	} else if hdr.Pgno > w.hdr.Commit {
		return fmt.Errorf("page number %d out-of-bounds for commit size %d", hdr.Pgno, w.hdr.Commit)
	} else if err := hdr.Validate(); err != nil {
		return err
	}

	// Snapshots must start with page 1 and include all pages up to the commit size.
	// Non-snapshot files can include any pages but they must be in order.
	if w.hdr.IsSnapshot() {
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
	_, _ = w.hash.Write(b)
	w.n += len(b)

	w.pageHeadersWritten++
	w.prevPgno = hdr.Pgno

	// Exit if we still have more headers to write.
	if w.pageHeadersWritten < w.hdr.PageN {
		return nil
	}

	// Move to writing events if the are specified in the header.
	if w.hdr.EventFrameN > 0 {
		w.state = stateEventHeader
		return nil
	}

	// If there are no events, pad header block to align page data.
	w.state = stateClose
	if err := w.writePadding(); err != nil {
		return fmt.Errorf("cannot write header block padding after page headers: %w", err)
	}
	return nil
}

// WriteEventHeader writes hdr to the file's event frame.
func (w *HeaderBlockWriter) WriteEventHeader(hdr EventFrameHeader) (err error) {
	if w.state == stateClosed {
		return ErrWriterClosed
	} else if w.state != stateEventHeader {
		return fmt.Errorf("cannot write event header, expected %s", w.state)
	} else if err := hdr.Validate(); err != nil {
		return err
	}

	// Ensure event data does not exceed total event bytes and that it matches
	// total event data size on the last written frame.
	if total := w.eventBytesTotal + int64(hdr.Size); total > int64(w.hdr.EventDataSize) {
		return fmt.Errorf("total event data size of %d bytes exceeds header event data size of %d bytes", total, w.hdr.EventDataSize)
	} else if w.eventFramesWritten+1 == w.hdr.EventFrameN && total != int64(w.hdr.EventDataSize) { // last frame only
		return fmt.Errorf("total event data size of %d bytes does not match header event data size of %d bytes", total, w.hdr.EventDataSize)
	}

	b, err := hdr.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	} else if _, err := w.w.Write(b); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	_, _ = w.hash.Write(b)
	w.n += len(b)

	// Move to writing event data.
	w.state = stateEventData
	w.eventHdr = hdr
	w.eventBytesTotal += int64(hdr.Size)
	w.eventFrameBytesWritten = 0

	return nil
}

// Write writes data to a single event frame. Should only be called after a
// successful call to WriteEventHeader().
func (w *HeaderBlockWriter) Write(p []byte) (n int, err error) {
	if w.state == stateClosed {
		return n, ErrWriterClosed
	} else if w.state != stateEventData {
		return n, fmt.Errorf("cannot write event data, expected %s", w.state)
	} else if total := w.eventFrameBytesWritten + int64(len(p)); total > int64(w.eventHdr.Size) {
		return n, fmt.Errorf("total event data size of %d bytes exceeds size specified in header of %d bytes", total, w.eventHdr.Size)
	}

	// Write data to file.
	n, err = w.w.Write(p)
	_, _ = w.hash.Write(p[:n])
	w.n += n

	// Return if there are still bytes remaining in frame.
	w.eventFrameBytesWritten += int64(n)
	if w.eventFrameBytesWritten < int64(w.eventHdr.Size) {
		return n, err
	}

	// Mark frame as complete if we have written all the bytes.
	w.eventFramesWritten++
	if w.eventFramesWritten < w.hdr.EventFrameN {
		w.state = stateEventHeader // move to next event frame
		return n, err
	}

	// If we have written all event frames, move state and write padding bytes.
	w.state = stateClose
	if err := w.writePadding(); err != nil {
		return n, fmt.Errorf("cannot write header block padding after event data: %w", err)
	}

	return n, err
}

func (w *HeaderBlockWriter) writePadding() error {
	sz := PageAlign(int64(w.n), w.hdr.PageSize) - int64(w.n)
	if sz == 0 {
		return nil
	}

	b := make([]byte, sz)
	n, err := w.w.Write(b)
	_, _ = w.hash.Write(b)
	w.n += n
	return err
}
