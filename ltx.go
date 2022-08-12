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

// Size constants.
const (
	HeaderSize     = 52
	PageHeaderSize = 4
	TrailerSize    = 16
)

// Checksum size & positions.
const (
	ChecksumSize          = 8
	TrailerChecksumOffset = TrailerSize - ChecksumSize
)

// Errors
var (
	ErrInvalidFile  = errors.New("invalid LTX file")
	ErrReaderClosed = errors.New("reader closed")
	ErrWriterClosed = errors.New("writer closed")

	ErrNoChecksum            = errors.New("no file checksum")
	ErrInvalidChecksumFormat = errors.New("invalid file checksum format")
	ErrChecksumMismatch      = errors.New("file checksum mismatch")
)

const (
	// ChecksumFlag is a flag on the checksum to ensure it is non-zero.
	ChecksumFlag uint64 = 1 << 63

	// ChecksumMask is the mask of the bits used for the page checksum.
	ChecksumMask uint64 = (1 << 63) - 1
)

// internal reader/writer states
const (
	stateHeader = "header"
	statePage   = "page"
	stateClose  = "close"
	stateClosed = "closed"
)

// Header represents the header frame of an LTX file.
type Header struct {
	Version          int    // based on magic
	Flags            uint32 // reserved flags
	PageSize         uint32 // page size, in bytes
	Commit           uint32 // db size after transaction, in pages
	DBID             uint32 // database ID
	MinTXID          uint64 // minimum transaction ID
	MaxTXID          uint64 // maximum transaction ID
	Timestamp        uint64 // seconds since unix epoch
	PreApplyChecksum uint64 // rolling checksum of database before applying this LTX file
}

// IsSnapshot returns true if header represents a complete database snapshot.
// This is true if the header includes the initial transaction. Snapshots must
// include all pages in the database.
func (h *Header) IsSnapshot() bool {
	return h.MinTXID == 1
}

// Validate returns an error if h is invalid.
func (h *Header) Validate() error {
	if h.Version != Version {
		return fmt.Errorf("invalid version")
	}
	if !IsValidHeaderFlags(h.Flags) {
		return fmt.Errorf("invalid flags: 0x%08x", h.Flags)
	}
	if !IsValidPageSize(h.PageSize) {
		return fmt.Errorf("invalid page size: %d", h.PageSize)
	}
	if h.Commit == 0 {
		return fmt.Errorf("commit record required")
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
		if h.PreApplyChecksum != 0 {
			return fmt.Errorf("pre-apply checksum must be zero on snapshots")
		}
	} else {
		if h.PreApplyChecksum == 0 {
			return fmt.Errorf("pre-apply checksum required on non-snapshot files")
		}
		if h.PreApplyChecksum&ChecksumFlag == 0 {
			return fmt.Errorf("invalid pre-apply checksum format")
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
	binary.BigEndian.PutUint32(b[12:], h.Commit)
	binary.BigEndian.PutUint32(b[16:], h.DBID)
	binary.BigEndian.PutUint64(b[20:], h.MinTXID)
	binary.BigEndian.PutUint64(b[28:], h.MaxTXID)
	binary.BigEndian.PutUint64(b[36:], h.Timestamp)
	binary.BigEndian.PutUint64(b[44:], h.PreApplyChecksum)
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
	h.Commit = binary.BigEndian.Uint32(b[12:])
	h.DBID = binary.BigEndian.Uint32(b[16:])
	h.MinTXID = binary.BigEndian.Uint64(b[20:])
	h.MaxTXID = binary.BigEndian.Uint64(b[28:])
	h.Timestamp = binary.BigEndian.Uint64(b[36:])
	h.PreApplyChecksum = binary.BigEndian.Uint64(b[44:])

	return nil
}

// IsValidHeaderFlags returns true if flags are unset. Flags are reserved.
func IsValidHeaderFlags(flags uint32) bool {
	return flags == 0
}

// Trailer represents the ending frame of an LTX file.
type Trailer struct {
	PostApplyChecksum uint64 // rolling checksum of database after this LTX file is applied
	FileChecksum      uint64 // crc64 checksum of entire file
}

// Validate returns an error if t is invalid.
func (t *Trailer) Validate() error {
	if t.PostApplyChecksum == 0 {
		return fmt.Errorf("post-apply checksum required")
	} else if t.PostApplyChecksum&ChecksumFlag == 0 {
		return fmt.Errorf("invalid post-checksum format")
	}

	if t.FileChecksum == 0 {
		return fmt.Errorf("file checksum required")
	} else if t.FileChecksum&ChecksumFlag == 0 {
		return fmt.Errorf("invalid file checksum format")
	}
	return nil
}

// MarshalBinary encodes h to a byte slice.
func (t *Trailer) MarshalBinary() ([]byte, error) {
	b := make([]byte, TrailerSize)
	binary.BigEndian.PutUint64(b[0:], t.PostApplyChecksum)
	binary.BigEndian.PutUint64(b[8:], t.FileChecksum)
	return b, nil
}

// UnmarshalBinary decodes h from a byte slice.
func (t *Trailer) UnmarshalBinary(b []byte) error {
	if len(b) < TrailerSize {
		return io.ErrShortBuffer
	}

	t.PostApplyChecksum = binary.BigEndian.Uint64(b[0:])
	t.FileChecksum = binary.BigEndian.Uint64(b[8:])
	return nil
}

// MaxPageSize is the maximum allowed page size for SQLite.
const MaxPageSize = 65536

// IsValidPageSize returns true if sz is between 512 and 64K and a power of two.
func IsValidPageSize(sz uint32) bool {
	for i := uint32(512); i <= MaxPageSize; i *= 2 {
		if sz == i {
			return true
		}
	}
	return false
}

// PageHeader represents the header for a single page in an LTX file.
type PageHeader struct {
	Pgno uint32
}

// IsZero returns true if the header is empty.
func (h *PageHeader) IsZero() bool {
	return *h == (PageHeader{})
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
	return b, nil
}

// UnmarshalBinary decodes h from a byte slice.
func (h *PageHeader) UnmarshalBinary(b []byte) error {
	if len(b) < PageHeaderSize {
		return io.ErrShortBuffer
	}

	h.Pgno = binary.BigEndian.Uint32(b[0:])
	return nil
}

// ChecksumPage returns a CRC64 checksum that combines the page number & page data.
func ChecksumPage(pgno uint32, data []byte) uint64 {
	h := crc64.New(crc64.MakeTable(crc64.ISO))
	_ = binary.Write(h, binary.BigEndian, pgno)
	_, _ = h.Write(data)
	return h.Sum64() & ChecksumMask
}

// ChecksumReader reads an entire database file from r and computes its rolling checksum.
func ChecksumReader(r io.Reader, pageSize int) (uint64, error) {
	data := make([]byte, pageSize)

	var chksum uint64
	for pgno := uint32(1); ; pgno++ {
		if _, err := io.ReadFull(r, data); err == io.EOF {
			break
		} else if err != nil {
			return chksum, err
		}
		chksum ^= ChecksumPage(pgno, data)
	}
	return ChecksumFlag | chksum, nil
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
	maxTXID, _ = strconv.ParseUint(a[2], 16, 64)
	return minTXID, maxTXID, nil
}

var filenameRegex = regexp.MustCompile(`^([0-9a-f]{16})-([0-9a-f]{16})\.ltx$`)

// FormatFilename returns an LTX filename representing a range of transactions.
func FormatFilename(minTXID, maxTXID uint64) string {
	return fmt.Sprintf("%016x-%016x.ltx", minTXID, maxTXID)
}
