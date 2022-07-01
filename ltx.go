// Package ltx reads and writes Liteserver Transaction (LTX) files.
package ltx

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"regexp"
	"strconv"
)

const (
	// Magic is the first 4 bytes of every LTX file.
	Magic = "LTX1"

	// Version is the current version of the LTX file format.
	Version = 1
)

// Header size constants.
const (
	HeaderSize      = 100
	PageHeaderSize  = 32
	EventHeaderSize = 32
)

// Checksum size & positions.
const (
	ChecksumSize = 8

	HeaderChecksumOffset      = HeaderSize - ChecksumSize
	PageBlockChecksumOffset   = HeaderChecksumOffset - ChecksumSize
	HeaderBlockChecksumOffset = PageBlockChecksumOffset - ChecksumSize
)

// Errors
var (
	ErrInvalidFile  = errors.New("invalid LTX file")
	ErrReaderClosed = errors.New("reader closed")
	ErrWriterClosed = errors.New("writer closed")

	ErrNoHeaderChecksum            = errors.New("no header checksum")
	ErrInvalidHeaderChecksumFormat = errors.New("invalid header checksum format")
	ErrHeaderChecksumMismatch      = errors.New("header checksum mismatch")

	ErrHeaderBlockChecksumMismatch = errors.New("header block checksum mismatch")
	ErrPageBlockChecksumMismatch   = errors.New("header page checksum mismatch")
)

const (
	// ChecksumFlag is a flag on the checksum to ensure it is non-zero.
	ChecksumFlag uint64 = 1 << 63

	// ChecksumMask is the mask of the bits used for the page checksum.
	ChecksumMask uint64 = (1 << 63) - 1
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
	EventN              uint32 // event count in ltx file
	EventDataSize       uint32 // total size of all event data
	Commit              uint32 // db size after transaction, in pages
	DBID                uint64 // database ID
	MinTXID             uint64 // minimum transaction ID
	MaxTXID             uint64 // maximum transaction ID
	Timestamp           uint64 // seconds since unix epoch
	PreChecksum         uint64 // checksum of database at previous transaction
	PostChecksum        uint64 // checksum of database after this LTX file is applied
	HeaderBlockChecksum uint64 // crc64 checksum of header block, excluding header
	PageBlockChecksum   uint64 // crc64 checksum of page block
	HeaderChecksum      uint64 // crc64 checksum of header
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
		(int64(h.EventN) * EventHeaderSize) + int64(h.EventDataSize)
	return PageAlign(sz, h.PageSize)
}

// Validate returns an error if h is invalid. This checks all fields including
// calculated fields such as checksums.
func (h *Header) Validate() error {
	// Prevalidation checks all fields that are not calculated. This includes
	// all fields except for most of the checksums.
	if err := h.Prevalidate(); err != nil {
		return err
	}

	if h.PostChecksum == 0 {
		return fmt.Errorf("post-checksum required")
	} else if h.PostChecksum&ChecksumFlag == 0 {
		return fmt.Errorf("invalid post-checksum format")
	}

	if h.HeaderBlockChecksum == 0 {
		return fmt.Errorf("header block checksum required")
	} else if h.HeaderBlockChecksum&ChecksumFlag == 0 {
		return fmt.Errorf("invalid header block checksum format")
	}

	if h.PageBlockChecksum == 0 {
		return fmt.Errorf("page block checksum required")
	} else if h.PageBlockChecksum&ChecksumFlag == 0 {
		return fmt.Errorf("invalid page block checksum format")
	}

	return nil
}

// Prevalidate returns an error if h is invalid. This function does not check
// calculated fields, which includes most of the checksums. It is called when
// initializing a writer so that we can perform basic validation before writing
// a lot of data.
func (h *Header) Prevalidate() error {
	if h.Version != Version {
		return fmt.Errorf("invalid version")
	}
	if !IsValidHeaderFlags(h.Flags) {
		return fmt.Errorf("invalid flags: 0x%08x", h.Flags)
	}
	if !IsValidPageSize(h.PageSize) {
		return fmt.Errorf("invalid page size: %d", h.PageSize)
	}
	if h.PageN == 0 {
		return fmt.Errorf("page count required")
	}
	if h.Commit == 0 {
		return fmt.Errorf("commit record required")
	}
	if h.EventN == 0 && h.EventDataSize != 0 {
		return fmt.Errorf("event data size must be zero if no events exist")
	}
	if h.EventN != 0 && h.EventDataSize == 0 {
		return fmt.Errorf("event data size must be specified if events exist")
	}
	if h.DBID == 0 {
		return fmt.Errorf("database id required")
	}
	if h.MinTXID == 0 {
		return fmt.Errorf("minimum transaction id required")
	}
	if h.MaxTXID == 0 {
		return fmt.Errorf("maximum transaction id required")
	}
	if h.MinTXID > h.MaxTXID {
		return fmt.Errorf("transaction ids out of order: (%d,%d)", h.MinTXID, h.MaxTXID)
	}

	// Snapshots are LTX files which have a minimum TXID of 1. This means they
	// must have all database pages included in them and they have no previous checksum.
	if h.IsSnapshot() {
		if h.PreChecksum != 0 {
			return fmt.Errorf("pre-checksum must be zero on snapshots")
		}
		if h.PageN < h.Commit {
			return fmt.Errorf("snapshot page count %d must equal commit size %d", h.PageN, h.Commit)
		}
	} else {
		if h.PreChecksum == 0 {
			return fmt.Errorf("pre-checksum required on non-snapshot files")
		}
		if h.PreChecksum&ChecksumFlag == 0 {
			return fmt.Errorf("invalid pre-checksum format")
		}
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
	binary.BigEndian.PutUint32(b[16:], h.EventN)
	binary.BigEndian.PutUint32(b[20:], h.EventDataSize)
	binary.BigEndian.PutUint32(b[24:], h.Commit)
	binary.BigEndian.PutUint64(b[28:], h.DBID)
	binary.BigEndian.PutUint64(b[36:], h.MinTXID)
	binary.BigEndian.PutUint64(b[44:], h.MaxTXID)
	binary.BigEndian.PutUint64(b[52:], h.Timestamp)
	binary.BigEndian.PutUint64(b[60:], h.PreChecksum)
	binary.BigEndian.PutUint64(b[68:], h.PostChecksum)
	binary.BigEndian.PutUint64(b[HeaderBlockChecksumOffset:], h.HeaderBlockChecksum)
	binary.BigEndian.PutUint64(b[PageBlockChecksumOffset:], h.PageBlockChecksum)

	// Checksum entire header.
	hash := crc64.New(crc64.MakeTable(crc64.ISO))
	_, _ = hash.Write(b[:HeaderChecksumOffset])
	h.HeaderChecksum = ChecksumFlag | hash.Sum64()
	binary.BigEndian.PutUint64(b[HeaderChecksumOffset:], h.HeaderChecksum)

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
	h.EventN = binary.BigEndian.Uint32(b[16:])
	h.EventDataSize = binary.BigEndian.Uint32(b[20:])
	h.Commit = binary.BigEndian.Uint32(b[24:])
	h.DBID = binary.BigEndian.Uint64(b[28:])
	h.MinTXID = binary.BigEndian.Uint64(b[36:])
	h.MaxTXID = binary.BigEndian.Uint64(b[44:])
	h.Timestamp = binary.BigEndian.Uint64(b[52:])
	h.PreChecksum = binary.BigEndian.Uint64(b[60:])
	h.PostChecksum = binary.BigEndian.Uint64(b[68:])
	h.HeaderBlockChecksum = binary.BigEndian.Uint64(b[HeaderBlockChecksumOffset:])
	h.PageBlockChecksum = binary.BigEndian.Uint64(b[PageBlockChecksumOffset:])
	h.HeaderChecksum = binary.BigEndian.Uint64(b[HeaderChecksumOffset:])

	// Ensure header checksum is set.
	if h.HeaderChecksum == 0 {
		return ErrNoHeaderChecksum
	} else if h.HeaderChecksum&ChecksumFlag == 0 {
		return ErrInvalidHeaderChecksumFormat
	}

	// Validate checksum of header.
	hash := crc64.New(crc64.MakeTable(crc64.ISO))
	_, _ = hash.Write(b[:HeaderChecksumOffset])
	chksum := ChecksumFlag | hash.Sum64()

	if chksum != h.HeaderChecksum {
		return ErrHeaderChecksumMismatch
	}

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

// EventHeader represents the header for a single event frame in an LTX file.
type EventHeader struct {
	Size  uint32
	Nonce [12]byte
	Tag   [16]byte
}

// Validate returns an error if h is invalid.
func (h *EventHeader) Validate() error {
	if h.Size == 0 {
		return fmt.Errorf("size required")
	}
	return nil
}

// MarshalBinary encodes h to a byte slice.
func (h *EventHeader) MarshalBinary() ([]byte, error) {
	b := make([]byte, EventHeaderSize)
	binary.BigEndian.PutUint32(b[0:], h.Size)
	copy(b[4:], h.Nonce[:])
	copy(b[16:], h.Tag[:])
	return b, nil
}

// UnmarshalBinary decodes h from a byte slice.
func (h *EventHeader) UnmarshalBinary(b []byte) error {
	if len(b) < EventHeaderSize {
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

// ChecksumPage returns a CRC64 checksum that combines the page number & page data.
func ChecksumPage(pgno uint32, data []byte) uint64 {
	h := crc64.New(crc64.MakeTable(crc64.ISO))
	_ = binary.Write(h, binary.BigEndian, pgno)
	_, _ = h.Write(data)
	return h.Sum64() & ChecksumMask
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

// ParseFilename parses a transaction range from an LTX file.
func ParseFilename(name string) (minTXID, maxTXID uint64, err error) {
	a := filenameRegex.FindStringSubmatch(name)
	if a == nil {
		return 0, 0, fmt.Errorf("invalid ltx filename: %s", name)
	}

	minTXID, _ = strconv.ParseUint(a[1], 16, 64)
	maxTXID, err = strconv.ParseUint(a[2], 16, 64)
	return minTXID, maxTXID, nil
}

var filenameRegex = regexp.MustCompile(`^([0-9a-f]{16})-([0-9a-f]{16})\.ltx$`)

// FormatFilename returns an LTX filename representing a range of transactions.
func FormatFilename(minTXID, maxTXID uint64) string {
	return fmt.Sprintf("%016x-%016x.ltx", minTXID, maxTXID)
}
