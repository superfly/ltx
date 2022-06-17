package ltx

import (
	"fmt"
	"hash"
	"hash/crc64"
	"io"
)

type PageBlockReader struct {
	r        io.Reader
	pageN    uint32
	pageSize uint32
	checksum uint64 // expected checksum

	hash      hash.Hash64
	bytesRead int64
}

func NewPageBlockReader(r io.Reader, pageN, pageSize uint32, checksum uint64) *PageBlockReader {
	return &PageBlockReader{
		r:        r,
		pageN:    pageN,
		pageSize: pageSize,
		checksum: checksum,

		hash: crc64.New(crc64.MakeTable(crc64.ISO)),
	}
}

// Close verifies the reader is at the end of the file and that the checksum matches.
func (r *PageBlockReader) Close() error {
	if n := r.Remaining(); n != 0 {
		return fmt.Errorf("cannot close page block, %d bytes remaining", n)
	}

	// Compare checksum with expected checksum.
	if r.Checksum() != r.checksum {
		return ErrPageBlockChecksumMismatch
	}

	return nil
}

// Checksum returns the calculated checksum of the page data read.
func (r *PageBlockReader) Checksum() uint64 {
	return r.hash.Sum64()
}

// Size returns the expected number of bytes to be read from the reader.
func (r *PageBlockReader) Size() int64 {
	return int64(r.pageN * r.pageSize)
}

// Remaining returns the number of bytes to be read from the reader.
func (r *PageBlockReader) Remaining() int64 {
	return r.Size() - r.bytesRead
}

func (r *PageBlockReader) Read(p []byte) (n int, err error) {
	if r.pageN == 0 {
		return n, fmt.Errorf("page count required")
	} else if !IsValidPageSize(r.pageSize) {
		return n, fmt.Errorf("invalid page size: %d", r.pageSize)
	}

	// Cap read to only what is remaining in page block.
	remaining := r.Remaining()
	if remaining == 0 {
		return 0, io.EOF
	} else if int64(len(p)) > remaining {
		p = p[:remaining]
	}

	// Read from underlying reader.
	n, err = r.r.Read(p)
	r.bytesRead += int64(n)

	_, _ = r.hash.Write(p[:n])

	return n, err
}
