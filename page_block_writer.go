package ltx

import (
	"fmt"
	"hash"
	"hash/crc64"
	"io"
)

// PageBlockWriter implements a writer for the page data block of the LTX file format.
type PageBlockWriter struct {
	w    io.Writer
	hash hash.Hash64

	pageN        uint32
	pageSize     uint32
	bytesWritten int64
}

// NewPageBlockWriter returns a new instance of PageBlockWriter.
func NewPageBlockWriter(w io.Writer, pageN, pageSize uint32) *PageBlockWriter {
	return &PageBlockWriter{
		w:        w,
		hash:     crc64.New(crc64.MakeTable(crc64.ISO)),
		pageN:    pageN,
		pageSize: pageSize,
	}
}

// Checksum returns the checksum of the header. Only valid after close.
func (w *PageBlockWriter) Checksum() uint64 {
	return w.hash.Sum64()
}

// Size returns the expected number of bytes to be written to the writer.
func (w *PageBlockWriter) Size() int64 {
	return int64(w.pageN * w.pageSize)
}

// Remaining returns the number of bytes to be written to the writer.
func (w *PageBlockWriter) Remaining() int64 {
	return w.Size() - w.bytesWritten
}

// Close returns an error if all bytes have not been written to the writer.
func (w *PageBlockWriter) Close() error {
	if w.bytesWritten != w.Size() {
		return fmt.Errorf("only %d of %d bytes written to page data", w.bytesWritten, w.Size())
	}
	return nil
}

// Write writes data to the file's page data block.
func (w *PageBlockWriter) Write(p []byte) (n int, err error) {
	if w.pageN == 0 {
		return n, fmt.Errorf("page count required")
	} else if !IsValidPageSize(w.pageSize) {
		return n, fmt.Errorf("invalid page size: %d", w.pageSize)
	} else if int64(len(p)) > w.Remaining() {
		return n, fmt.Errorf("write of %d bytes too large, only %d bytes remaining in page block", len(p), w.Remaining())
	}

	n, err = w.w.Write(p)
	w.bytesWritten += int64(n)

	_, _ = w.hash.Write(p[:n])

	return n, err
}
