// Package ltx reads and writes Liteserver Transaction (LTX) files.
package ltx

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
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
	HeaderSize     = 100
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
	ErrInvalidFile   = errors.New("invalid LTX file")
	ErrDecoderClosed = errors.New("ltx decoder closed")
	ErrEncoderClosed = errors.New("ltx encoder closed")

	ErrNoChecksum            = errors.New("no file checksum")
	ErrInvalidChecksumFormat = errors.New("invalid file checksum format")
	ErrChecksumMismatch      = errors.New("file checksum mismatch")
)

// ChecksumFlag is a flag on the checksum to ensure it is non-zero.
const ChecksumFlag uint64 = 1 << 63

// internal reader/writer states
const (
	stateHeader = "header"
	statePage   = "page"
	stateClose  = "close"
	stateClosed = "closed"
)

// Pos represents the transactional position of a database.
type Pos struct {
	TXID              uint64
	PostApplyChecksum uint64
}

// String returns a string representation of the position.
func (p Pos) String() string {
	return fmt.Sprintf("%016x/%016x", p.TXID, p.PostApplyChecksum)
}

// IsZero returns true if the position is empty.
func (p Pos) IsZero() bool {
	return p == (Pos{})
}

// Marshal serializes the position into JSON.
func (p Pos) MarshalJSON() ([]byte, error) {
	var v posJSON
	v.TXID = FormatTXID(p.TXID)
	v.PostApplyChecksum = fmt.Sprintf("%016x", p.PostApplyChecksum)
	return json.Marshal(v)
}

// Unmarshal deserializes the position from JSON.
func (p *Pos) UnmarshalJSON(data []byte) (err error) {
	var v posJSON
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}

	if p.TXID, err = ParseTXID(v.TXID); err != nil {
		return fmt.Errorf("cannot parse txid: %q", v.TXID)
	}
	if p.PostApplyChecksum, err = strconv.ParseUint(v.PostApplyChecksum, 16, 64); err != nil {
		return fmt.Errorf("cannot parse post-apply checksum: %q", v.PostApplyChecksum)
	}
	return nil
}

type posJSON struct {
	TXID              string `json:"txid"`
	PostApplyChecksum string `json:"postApplyChecksum"`
}

// PosMismatchError is returned when an LTX file is not contiguous with the current position.
type PosMismatchError struct {
	Pos Pos `json:"pos"`
}

// NewPosMismatchError returns a new instance of PosMismatchError.
func NewPosMismatchError(pos Pos) *PosMismatchError {
	return &PosMismatchError{Pos: pos}
}

// Error returns the string representation of the error.
func (e *PosMismatchError) Error() string {
	return fmt.Sprintf("ltx position mismatch (%s)", e.Pos)
}

// Header flags.
const (
	HeaderFlagMask = uint32(0x00000001)

	HeaderFlagCompressLZ4 = uint32(0x00000001)
)

// Header represents the header frame of an LTX file.
type Header struct {
	Version          int    // based on magic
	Flags            uint32 // reserved flags
	PageSize         uint32 // page size, in bytes
	Commit           uint32 // db size after transaction, in pages
	MinTXID          uint64 // minimum transaction ID
	MaxTXID          uint64 // maximum transaction ID
	Timestamp        int64  // milliseconds since unix epoch
	PreApplyChecksum uint64 // rolling checksum of database before applying this LTX file
	WALOffset        int64  // file offset from original WAL; zero if journal
	WALSize          int64  // size of original WAL segment; zero if journal
	WALSalt1         uint32 // header salt-1 from original WAL; zero if journal or compaction
	WALSalt2         uint32 // header salt-2 from original WAL; zero if journal or compaction
	NodeID           uint64 // node id where the LTX file was created, zero if unset
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
	if h.MinTXID == 0 {
		return fmt.Errorf("minimum transaction id required")
	}
	if h.MaxTXID == 0 {
		return fmt.Errorf("maximum transaction id required")
	}
	if h.MinTXID > h.MaxTXID {
		return fmt.Errorf("transaction ids out of order: (%d,%d)", h.MinTXID, h.MaxTXID)
	}

	if h.WALOffset < 0 {
		return fmt.Errorf("wal offset cannot be negative: %d", h.WALOffset)
	}
	if h.WALSize < 0 {
		return fmt.Errorf("wal size cannot be negative: %d", h.WALSize)
	}

	if h.WALSalt1 != 0 || h.WALSalt2 != 0 {
		if h.WALOffset == 0 {
			return fmt.Errorf("wal offset required if salt exists")
		}
		if h.WALSize == 0 {
			return fmt.Errorf("wal size required if salt exists")
		}
	}

	if h.WALOffset != 0 && h.WALSize == 0 {
		return fmt.Errorf("wal size required if wal offset exists")
	}
	if h.WALOffset == 0 && h.WALSize != 0 {
		return fmt.Errorf("wal offset required if wal size exists")
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
	binary.BigEndian.PutUint64(b[16:], h.MinTXID)
	binary.BigEndian.PutUint64(b[24:], h.MaxTXID)
	binary.BigEndian.PutUint64(b[32:], uint64(h.Timestamp))
	binary.BigEndian.PutUint64(b[40:], h.PreApplyChecksum)
	binary.BigEndian.PutUint64(b[48:], uint64(h.WALOffset))
	binary.BigEndian.PutUint64(b[56:], uint64(h.WALSize))
	binary.BigEndian.PutUint32(b[64:], h.WALSalt1)
	binary.BigEndian.PutUint32(b[68:], h.WALSalt2)
	binary.BigEndian.PutUint64(b[72:], h.NodeID)
	return b, nil
}

// UnmarshalBinary decodes h from a byte slice.
func (h *Header) UnmarshalBinary(b []byte) error {
	if len(b) < HeaderSize {
		return io.ErrShortBuffer
	}

	h.Flags = binary.BigEndian.Uint32(b[4:])
	h.PageSize = binary.BigEndian.Uint32(b[8:])
	h.Commit = binary.BigEndian.Uint32(b[12:])
	h.MinTXID = binary.BigEndian.Uint64(b[16:])
	h.MaxTXID = binary.BigEndian.Uint64(b[24:])
	h.Timestamp = int64(binary.BigEndian.Uint64(b[32:]))
	h.PreApplyChecksum = binary.BigEndian.Uint64(b[40:])
	h.WALOffset = int64(binary.BigEndian.Uint64(b[48:]))
	h.WALSize = int64(binary.BigEndian.Uint64(b[56:]))
	h.WALSalt1 = binary.BigEndian.Uint32(b[64:])
	h.WALSalt2 = binary.BigEndian.Uint32(b[68:])
	h.NodeID = binary.BigEndian.Uint64(b[72:])

	if string(b[0:4]) != Magic {
		return ErrInvalidFile
	}
	h.Version = Version

	return nil
}

// IsValidHeaderFlags returns true unless flags outside the valid mask are set.
func IsValidHeaderFlags(flags uint32) bool {
	return flags == (flags & HeaderFlagMask)
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

// NewHasher returns a new CRC64-ISO hasher.
func NewHasher() hash.Hash64 {
	return crc64.New(crc64.MakeTable(crc64.ISO))
}

// ChecksumPage returns a CRC64 checksum that combines the page number & page data.
func ChecksumPage(pgno uint32, data []byte) uint64 {
	return ChecksumPageWithHasher(NewHasher(), pgno, data)
}

// ChecksumPageWithHasher returns a CRC64 checksum that combines the page number & page data.
func ChecksumPageWithHasher(h hash.Hash64, pgno uint32, data []byte) uint64 {
	h.Reset()
	_ = binary.Write(h, binary.BigEndian, pgno)
	_, _ = h.Write(data)
	return ChecksumFlag | h.Sum64()
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
		chksum = ChecksumFlag | (chksum ^ ChecksumPage(pgno, data))
	}
	return chksum, nil
}

// FormatTXID returns id formatted as a fixed-width hex number.
func FormatTXID(id uint64) string {
	return fmt.Sprintf("%016x", id)
}

// ParseTXID parses a 16-character hex string into a transaction ID.
func ParseTXID(s string) (uint64, error) {
	if len(s) != 16 {
		return 0, fmt.Errorf("invalid formatted transaction id length: %q", s)
	}
	v, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid transaction id format: %q", s)
	}
	return uint64(v), nil
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

const PENDING_BYTE = 0x40000000

// LockPgno returns the page number where the PENDING_BYTE exists.
func LockPgno(pageSize uint32) uint32 {
	return uint32(PENDING_BYTE/int64(pageSize)) + 1
}
