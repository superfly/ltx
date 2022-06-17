package internal

import (
	"fmt"
	"io"
)

// Buffer is a simplified version of bytes.Buffer that implements io.Seeker.
type Buffer struct {
	buf []byte
	ro  int // read offset
	wo  int // write offset
}

// Bytes returns the remaining bytes from the buffer.
func (b *Buffer) Bytes() []byte { return b.buf[b.ro:] }

// Len returns the number of remaining bytes from the buffer.
func (b *Buffer) Len() int { return len(b.buf) - b.ro }

// Seek moves write offset to the given position.
func (b *Buffer) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		b.wo = int(offset)
	case io.SeekCurrent:
		b.wo += int(offset)
	case io.SeekEnd:
		b.wo = len(b.buf) - int(offset)
	default:
		return 0, fmt.Errorf("invalid whence")
	}
	return int64(b.wo), nil
}

// Write writes p to the current position in the buffer.
func (b *Buffer) Write(p []byte) (n int, err error) {
	sz := b.wo + len(p) // size after write
	if sz > cap(b.buf) {
		b.buf = append(b.buf[:cap(b.buf)], make([]byte, sz-cap(b.buf))...)[:len(b.buf)]
	}
	if sz > len(b.buf) {
		b.buf = b.buf[:sz]
	}
	n = copy(b.buf[b.wo:], p)
	b.wo += n
	return n, nil
}

// Read reads from the current read position into p.
func (b *Buffer) Read(p []byte) (n int, err error) {
	if b.ro >= len(b.buf) {
		return 0, io.EOF
	}
	n = copy(p, b.buf[b.ro:])
	b.ro += n
	return n, nil
}
