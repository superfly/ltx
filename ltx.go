// Package ltx reads and writes Liteserver Transaction (LTX) files.
package ltx

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	// Magic is the first 4 bytes of every LTX file.
	Magic = "LTX1"

	// Version is the current version of the LTX file format.
	Version = 1
)

// Header size constants.
const (
	HeaderSize           = 76
	PageHeaderSize       = 32
	EventFrameHeaderSize = 32
)

// Checksum size & positions.
const (
	ChecksumSize = 8

	HeaderBlockChecksumOffset = HeaderSize - ChecksumSize - ChecksumSize
	PageBlockChecksumOffset   = HeaderBlockChecksumOffset + ChecksumSize
)

// Errors
var (
	ErrInvalidFile  = errors.New("invalid LTX file")
	ErrReaderClosed = errors.New("reader closed")
	ErrWriterClosed = errors.New("writer closed")

	ErrHeaderBlockChecksumMismatch = errors.New("header block checksum mismatch")
	ErrPageBlockChecksumMismatch   = errors.New("header page checksum mismatch")
)

// internal reader/writer states
const (
	stateHeader      = "header"
	stateEventHeader = "event header"
	stateEventData   = "event data"
	statePageHeader  = "page header"
	statePageData    = "page data"
	stateClose       = "close"
	stateClosed      = "closed"
)

// Header represents the header frame of an LTX file.
type Header struct {
	Version             int    // based on magic
	Flags               uint32 // reserved flags
	PageSize            uint32 // page size, in bytes
	PageN               uint32 // page count in ltx file
	EventFrameN         uint32 // event frame count in ltx file
	EventDataSize       uint32 // total size of all event data
	Commit              uint32 // db size after transaction, in pages
	DBID                uint64 // database ID
	MinTXID             uint64 // minimum transaction ID
	MaxTXID             uint64 // maximum transaction ID
	Timestamp           uint64 // seconds since unix epoch
	HeaderBlockChecksum uint64 // crc64 checksum of header block
	PageBlockChecksum   uint64 // crc64 checksum of page block
}

// IsSnapshot returns true if header represents a complete database snapshot.
// This is true if the header includes the initial transaction. Snapshots must
// include all pages in the database.
func (h *Header) IsSnapshot() bool {
	return h.MinTXID == 1
}

// HeaderBlockSize returns the total size of the header block, in bytes.
// Must be a valid header frame.
func (h *Header) HeaderBlockSize() int64 {
	sz := HeaderSize +
		(int64(h.PageN) * PageHeaderSize) +
		(int64(h.EventFrameN) * EventFrameHeaderSize) + int64(h.EventDataSize)
	return PageAlign(sz, h.PageSize)
}

// Validate returns an error if h is invalid.
func (h *Header) Validate() error {
	if h.Version != Version {
		return fmt.Errorf("invalid version")
	} else if !IsValidHeaderFlags(h.Flags) {
		return fmt.Errorf("invalid flags: 0x%08x", h.Flags)
	} else if !IsValidPageSize(h.PageSize) {
		return fmt.Errorf("invalid page size: %d", h.PageSize)
	} else if h.PageN == 0 {
		return fmt.Errorf("page count required")
	} else if h.Commit == 0 {
		return fmt.Errorf("commit record required")
	} else if h.EventFrameN == 0 && h.EventDataSize != 0 {
		return fmt.Errorf("event data size must be zero if no event frames exist")
	} else if h.EventFrameN != 0 && h.EventDataSize == 0 {
		return fmt.Errorf("event data size must be specified if event frames exist")
	} else if h.DBID == 0 {
		return fmt.Errorf("database id required")
	} else if h.MinTXID == 0 {
		return fmt.Errorf("minimum transaction id required")
	} else if h.MaxTXID == 0 {
		return fmt.Errorf("maximum transaction id required")
	} else if h.MinTXID > h.MaxTXID {
		return fmt.Errorf("transaction ids out of order: (%d,%d)", h.MinTXID, h.MaxTXID)
	} else if h.IsSnapshot() && h.PageN < h.Commit {
		return fmt.Errorf("snapshot page count %d must equal commit size %d", h.PageN, h.Commit)
	}
	return nil
}

// MarshalBinary encodes h to a byte slice.
func (h *Header) MarshalBinary() ([]byte, error) {
	b := make([]byte, HeaderSize)
	copy(b[0:4], Magic)
	binary.BigEndian.PutUint32(b[4:], h.Flags)
	binary.BigEndian.PutUint32(b[8:], h.PageSize)
	binary.BigEndian.PutUint32(b[12:], h.PageN)
	binary.BigEndian.PutUint32(b[16:], h.EventFrameN)
	binary.BigEndian.PutUint32(b[20:], h.EventDataSize)
	binary.BigEndian.PutUint32(b[24:], h.Commit)
	binary.BigEndian.PutUint64(b[28:], h.DBID)
	binary.BigEndian.PutUint64(b[36:], h.MinTXID)
	binary.BigEndian.PutUint64(b[44:], h.MaxTXID)
	binary.BigEndian.PutUint64(b[52:], h.Timestamp)
	binary.BigEndian.PutUint64(b[HeaderBlockChecksumOffset:], h.HeaderBlockChecksum)
	binary.BigEndian.PutUint64(b[PageBlockChecksumOffset:], h.PageBlockChecksum)
	return b, nil
}

// UnmarshalBinary decodes h from a byte slice.
func (h *Header) UnmarshalBinary(b []byte) error {
	if len(b) < HeaderSize {
		return io.ErrShortBuffer
	} else if string(b[0:4]) != Magic {
		return ErrInvalidFile
	}

	h.Version = Version
	h.Flags = binary.BigEndian.Uint32(b[4:])
	h.PageSize = binary.BigEndian.Uint32(b[8:])
	h.PageN = binary.BigEndian.Uint32(b[12:])
	h.EventFrameN = binary.BigEndian.Uint32(b[16:])
	h.EventDataSize = binary.BigEndian.Uint32(b[20:])
	h.Commit = binary.BigEndian.Uint32(b[24:])
	h.DBID = binary.BigEndian.Uint64(b[28:])
	h.MinTXID = binary.BigEndian.Uint64(b[36:])
	h.MaxTXID = binary.BigEndian.Uint64(b[44:])
	h.Timestamp = binary.BigEndian.Uint64(b[52:])
	h.HeaderBlockChecksum = binary.BigEndian.Uint64(b[HeaderBlockChecksumOffset:])
	h.PageBlockChecksum = binary.BigEndian.Uint64(b[PageBlockChecksumOffset:])

	return nil
}

// IsValidHeaderFlags returns true if flags are unset. Flags are reserved.
func IsValidHeaderFlags(flags uint32) bool {
	return flags == 0
}

// IsValidPageSize returns true if sz is between 512 and 64K and a power of two.
func IsValidPageSize(sz uint32) bool {
	for i := uint32(512); i <= 65536; i *= 2 {
		if sz == i {
			return true
		}
	}
	return false
}

// EventFrameHeader represents the header for a single event frame in an LTX file.
type EventFrameHeader struct {
	Size  uint32
	Nonce [12]byte
	Tag   [16]byte
}

// Validate returns an error if h is invalid.
func (h *EventFrameHeader) Validate() error {
	if h.Size == 0 {
		return fmt.Errorf("size required")
	}
	return nil
}

// MarshalBinary encodes h to a byte slice.
func (h *EventFrameHeader) MarshalBinary() ([]byte, error) {
	b := make([]byte, EventFrameHeaderSize)
	binary.BigEndian.PutUint32(b[0:], h.Size)
	copy(b[4:], h.Nonce[:])
	copy(b[16:], h.Tag[:])
	return b, nil
}

// UnmarshalBinary decodes h from a byte slice.
func (h *EventFrameHeader) UnmarshalBinary(b []byte) error {
	if len(b) < EventFrameHeaderSize {
		return io.ErrShortBuffer
	}

	h.Size = binary.BigEndian.Uint32(b[0:])
	copy(h.Nonce[:], b[4:])
	copy(h.Tag[:], b[16:])
	return nil
}

// PageHeader represents the header for a single page in an LTX file.
type PageHeader struct {
	Pgno  uint32
	Nonce [12]byte
	Tag   [16]byte
}

// Validate returns an error if h is invalid.
func (h *PageHeader) Validate() error {
	if h.Pgno == 0 {
		return fmt.Errorf("page number required")
	}
	return nil
}

// MarshalBinary encodes h to a byte slice.
func (h *PageHeader) MarshalBinary() ([]byte, error) {
	b := make([]byte, PageHeaderSize)
	binary.BigEndian.PutUint32(b[0:], h.Pgno)
	copy(b[4:], h.Nonce[:])
	copy(b[16:], h.Tag[:])
	return b, nil
}

// UnmarshalBinary decodes h from a byte slice.
func (h *PageHeader) UnmarshalBinary(b []byte) error {
	if len(b) < PageHeaderSize {
		return io.ErrShortBuffer
	}

	h.Pgno = binary.BigEndian.Uint32(b[0:])
	copy(h.Nonce[:], b[4:])
	copy(h.Tag[:], b[16:])
	return nil
}

// PageAlign returns v if it a multiple of pageSize.
// Otherwise returns next multiple of pageSize.
func PageAlign(v int64, pageSize uint32) int64 {
	return int64((uint64(v) + uint64(pageSize) - 1) &^ (uint64(pageSize) - 1))
	//if v %int64(pageSize) {
	//	return v
	//}
	//return v + (int64(pageSize)-(v%int64(pageSize)))
}

// FormatTXID returns id formatted as a fixed-width hex number.
func FormatTXID(id uint64) string {
	return fmt.Sprintf("%016x", id)
}

// FormatTXIDRange returns min & max formatted as a single number if equal or a range if different.
func FormatTXIDRange(min, max uint64) string {
	if min == max {
		return fmt.Sprintf("%d", min)
	}
	return fmt.Sprintf("%d-%d", min, max)
}
