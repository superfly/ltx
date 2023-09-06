// Package ltx reads and writes Liteserver Transaction (LTX) files.
package ltx

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"regexp"
	"strconv"
	"time"
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

// RFC3339Milli is the standard time format for LTX timestamps.
// It uses fixed-width millisecond resolution which makes it sortable.
const RFC3339Milli = "2006-01-02T15:04:05.000Z07:00"

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
const ChecksumFlag Checksum = 1 << 63

// internal reader/writer states
const (
	stateHeader = "header"
	statePage   = "page"
	stateClose  = "close"
	stateClosed = "closed"
)

// Pos represents the transactional position of a database.
type Pos struct {
	TXID              TXID
	PostApplyChecksum Checksum
}

// NewPos returns a new instance of Pos.
func NewPos(txID TXID, postApplyChecksum Checksum) Pos {
	return Pos{
		TXID:              txID,
		PostApplyChecksum: postApplyChecksum,
	}
}

// ParsePos parses Pos from its string representation.
func ParsePos(s string) (Pos, error) {
	if len(s) != 33 {
		return Pos{}, fmt.Errorf("invalid formatted position length: %q", s)
	}

	txid, err := ParseTXID(s[:16])
	if err != nil {
		return Pos{}, err
	}

	checksum, err := ParseChecksum(s[17:])
	if err != nil {
		return Pos{}, err
	}

	return Pos{
		TXID:              txid,
		PostApplyChecksum: checksum,
	}, nil
}

// String returns a string representation of the position.
func (p Pos) String() string {
	return fmt.Sprintf("%s/%s", p.TXID, p.PostApplyChecksum)
}

// IsZero returns true if the position is empty.
func (p Pos) IsZero() bool {
	return p == (Pos{})
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

// TXID represents a transaction ID.
type TXID uint64

// ParseTXID parses a 16-character hex string into a transaction ID.
func ParseTXID(s string) (TXID, error) {
	if len(s) != 16 {
		return 0, fmt.Errorf("invalid formatted transaction id length: %q", s)
	}
	v, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid transaction id format: %q", s)
	}
	return TXID(v), nil
}

// String returns id formatted as a fixed-width hex number.
func (t TXID) String() string {
	return fmt.Sprintf("%016x", uint64(t))
}

func (t TXID) MarshalJSON() ([]byte, error) {
	return []byte(`"` + t.String() + `"`), nil
}

func (t *TXID) UnmarshalJSON(data []byte) (err error) {
	var s *string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("cannot unmarshal TXID from JSON value")
	}

	// Set to zero if value is nil.
	if s == nil {
		*t = 0
		return nil
	}

	txID, err := ParseTXID(*s)
	if err != nil {
		return fmt.Errorf("cannot parse TXID from JSON string: %q", *s)
	}
	*t = TXID(txID)

	return nil
}

// Checksum represents an LTX checksum.
type Checksum uint64

// ParseChecksum parses a 16-character hex string into a checksum.
func ParseChecksum(s string) (Checksum, error) {
	if len(s) != 16 {
		return 0, fmt.Errorf("invalid formatted checksum length: %q", s)
	}
	v, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid checksum format: %q", s)
	}
	return Checksum(v), nil
}

// String returns c formatted as a fixed-width hex number.
func (c Checksum) String() string {
	return fmt.Sprintf("%016x", uint64(c))
}

func (c Checksum) MarshalJSON() ([]byte, error) {
	return []byte(`"` + c.String() + `"`), nil
}

func (c *Checksum) UnmarshalJSON(data []byte) (err error) {
	var s *string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("cannot unmarshal checksum from JSON value")
	}

	// Set to zero if value is nil.
	if s == nil {
		*c = 0
		return nil
	}

	chksum, err := ParseChecksum(*s)
	if err != nil {
		return fmt.Errorf("cannot parse checksum from JSON string: %q", *s)
	}
	*c = Checksum(chksum)

	return nil
}

// Header flags.
const (
	HeaderFlagMask = uint32(0x00000001)

	HeaderFlagCompressLZ4 = uint32(0x00000001)
)

// Header represents the header frame of an LTX file.
type Header struct {
	Version          int      // based on magic
	Flags            uint32   // reserved flags
	PageSize         uint32   // page size, in bytes
	Commit           uint32   // db size after transaction, in pages
	MinTXID          TXID     // minimum transaction ID
	MaxTXID          TXID     // maximum transaction ID
	Timestamp        int64    // milliseconds since unix epoch
	PreApplyChecksum Checksum // rolling checksum of database before applying this LTX file
	WALOffset        int64    // file offset from original WAL; zero if journal
	WALSize          int64    // size of original WAL segment; zero if journal
	WALSalt1         uint32   // header salt-1 from original WAL; zero if journal or compaction
	WALSalt2         uint32   // header salt-2 from original WAL; zero if journal or compaction
	NodeID           uint64   // node id where the LTX file was created, zero if unset
}

// IsSnapshot returns true if header represents a complete database snapshot.
// This is true if the header includes the initial transaction. Snapshots must
// include all pages in the database.
func (h *Header) IsSnapshot() bool {
	return h.MinTXID == 1
}

// LockPgno returns the lock page number based on the header's page size.
func (h *Header) LockPgno() uint32 {
	return LockPgno(h.PageSize)
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

// PreApplyPos returns the replication position before the LTX file is applies.
func (h Header) PreApplyPos() Pos {
	return Pos{
		TXID:              h.MinTXID - 1,
		PostApplyChecksum: h.PreApplyChecksum,
	}
}

// MarshalBinary encodes h to a byte slice.
func (h *Header) MarshalBinary() ([]byte, error) {
	b := make([]byte, HeaderSize)
	copy(b[0:4], Magic)
	binary.BigEndian.PutUint32(b[4:], h.Flags)
	binary.BigEndian.PutUint32(b[8:], h.PageSize)
	binary.BigEndian.PutUint32(b[12:], h.Commit)
	binary.BigEndian.PutUint64(b[16:], uint64(h.MinTXID))
	binary.BigEndian.PutUint64(b[24:], uint64(h.MaxTXID))
	binary.BigEndian.PutUint64(b[32:], uint64(h.Timestamp))
	binary.BigEndian.PutUint64(b[40:], uint64(h.PreApplyChecksum))
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
	h.MinTXID = TXID(binary.BigEndian.Uint64(b[16:]))
	h.MaxTXID = TXID(binary.BigEndian.Uint64(b[24:]))
	h.Timestamp = int64(binary.BigEndian.Uint64(b[32:]))
	h.PreApplyChecksum = Checksum(binary.BigEndian.Uint64(b[40:]))
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

// PeekHeader reads & unmarshals the header from r.
// It returns a new io.Reader that prepends the header data back on.
func PeekHeader(r io.Reader) (Header, io.Reader, error) {
	buf := make([]byte, HeaderSize)
	n, err := io.ReadFull(r, buf)
	r = io.MultiReader(bytes.NewReader(buf[:n]), r)
	if err != nil {
		return Header{}, r, err
	}

	var hdr Header
	err = hdr.UnmarshalBinary(buf)
	return hdr, r, err
}

// IsValidHeaderFlags returns true unless flags outside the valid mask are set.
func IsValidHeaderFlags(flags uint32) bool {
	return flags == (flags & HeaderFlagMask)
}

// Trailer represents the ending frame of an LTX file.
type Trailer struct {
	PostApplyChecksum Checksum // rolling checksum of database after this LTX file is applied
	FileChecksum      Checksum // crc64 checksum of entire file
}

// Validate returns an error if t is invalid.
func (t *Trailer) Validate() error {
	if t.PostApplyChecksum == 0 {
		return fmt.Errorf("post-apply checksum required")
	} else if t.PostApplyChecksum&ChecksumFlag == 0 {
		return fmt.Errorf("invalid post-apply checksum format")
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
	binary.BigEndian.PutUint64(b[0:], uint64(t.PostApplyChecksum))
	binary.BigEndian.PutUint64(b[8:], uint64(t.FileChecksum))
	return b, nil
}

// UnmarshalBinary decodes h from a byte slice.
func (t *Trailer) UnmarshalBinary(b []byte) error {
	if len(b) < TrailerSize {
		return io.ErrShortBuffer
	}

	t.PostApplyChecksum = Checksum(binary.BigEndian.Uint64(b[0:]))
	t.FileChecksum = Checksum(binary.BigEndian.Uint64(b[8:]))
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
func ChecksumPage(pgno uint32, data []byte) Checksum {
	return ChecksumPageWithHasher(NewHasher(), pgno, data)
}

// ChecksumPageWithHasher returns a CRC64 checksum that combines the page number & page data.
func ChecksumPageWithHasher(h hash.Hash64, pgno uint32, data []byte) Checksum {
	h.Reset()
	_ = binary.Write(h, binary.BigEndian, pgno)
	_, _ = h.Write(data)
	return ChecksumFlag | Checksum(h.Sum64())
}

// ChecksumReader reads an entire database file from r and computes its rolling checksum.
func ChecksumReader(r io.Reader, pageSize int) (Checksum, error) {
	data := make([]byte, pageSize)

	var chksum Checksum
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

// ParseFilename parses a transaction range from an LTX file.
func ParseFilename(name string) (minTXID, maxTXID TXID, err error) {
	a := filenameRegex.FindStringSubmatch(name)
	if a == nil {
		return 0, 0, fmt.Errorf("invalid ltx filename: %s", name)
	}

	min, _ := strconv.ParseUint(a[1], 16, 64)
	max, _ := strconv.ParseUint(a[2], 16, 64)
	return TXID(min), TXID(max), nil
}

// FormatTimestamp returns t with a fixed-width, millisecond-resolution UTC format.
func FormatTimestamp(t time.Time) string {
	return t.UTC().Format(RFC3339Milli)
}

// ParseTimestamp parses a timestamp as RFC3339Milli (fixed-width) but will
// fallback to RFC3339Nano if it fails. This is to support timestamps written
// before the introduction of the standard time format.
func ParseTimestamp(value string) (time.Time, error) {
	// Attempt standard format first.
	t, err := time.Parse(RFC3339Milli, value)
	if err == nil {
		return t, nil
	}

	// If the standard fails, fallback to stdlib format but truncate to milliseconds.
	t2, err2 := time.Parse(time.RFC3339Nano, value)
	if err2 != nil {
		return t, err // use original error on failure.
	}
	return t2.Truncate(time.Millisecond), nil
}

var filenameRegex = regexp.MustCompile(`^([0-9a-f]{16})-([0-9a-f]{16})\.ltx$`)

// FormatFilename returns an LTX filename representing a range of transactions.
func FormatFilename(minTXID, maxTXID TXID) string {
	return fmt.Sprintf("%s-%s.ltx", minTXID.String(), maxTXID.String())
}

const PENDING_BYTE = 0x40000000

// LockPgno returns the page number where the PENDING_BYTE exists.
func LockPgno(pageSize uint32) uint32 {
	return uint32(PENDING_BYTE/int64(pageSize)) + 1
}
