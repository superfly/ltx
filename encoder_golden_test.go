package ltx_test

import (
	"bytes"
	"testing"

	"github.com/superfly/ltx"
)

// TestEncoder_PageIndexGolden pins the file checksum of deterministic files
// captured from the map-based encoder, so the append-only page index stays
// byte-for-byte compatible on the wire (the checksum covers the page index
// tuples in order, so any reordering or size drift changes it).
func TestEncoder_PageIndexGolden(t *testing.T) {
	page := func(seed byte) []byte {
		b := make([]byte, 512)
		for i := range b {
			b[i] = seed + byte(i%7)
		}
		return b
	}

	t.Run("SparseNonSnapshot", func(t *testing.T) {
		var buf bytes.Buffer
		enc, err := ltx.NewEncoder(&buf)
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodeHeader(ltx.Header{
			Version:          ltx.Version,
			PageSize:         512,
			Commit:           16,
			MinTXID:          2,
			MaxTXID:          3,
			Timestamp:        1000,
			PreApplyChecksum: ltx.ChecksumFlag | 1,
		}); err != nil {
			t.Fatal(err)
		}
		for _, pgno := range []uint32{2, 5, 9, 16} {
			if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, page(byte(pgno))); err != nil {
				t.Fatal(err)
			}
		}
		enc.SetPostApplyChecksum(ltx.ChecksumFlag | 2)
		if err := enc.Close(); err != nil {
			t.Fatal(err)
		}
		if got, want := enc.Trailer().FileChecksum, ltx.Checksum(0xe27f4c18f0da2c10); got != want {
			t.Fatalf("FileChecksum=%#x, want %#x", uint64(got), uint64(want))
		}
	})

	t.Run("Snapshot", func(t *testing.T) {
		var buf bytes.Buffer
		enc, err := ltx.NewEncoder(&buf)
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodeHeader(ltx.Header{
			Version:   ltx.Version,
			PageSize:  512,
			Commit:    6,
			MinTXID:   1,
			MaxTXID:   4,
			Timestamp: 1000,
		}); err != nil {
			t.Fatal(err)
		}
		for pgno := uint32(1); pgno <= 6; pgno++ {
			if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, page(byte(pgno))); err != nil {
				t.Fatal(err)
			}
		}
		enc.SetPostApplyChecksum(ltx.ChecksumFlag | 3)
		if err := enc.Close(); err != nil {
			t.Fatal(err)
		}
		if got, want := enc.Trailer().FileChecksum, ltx.Checksum(0xd926967672f7ca53); got != want {
			t.Fatalf("FileChecksum=%#x, want %#x", uint64(got), uint64(want))
		}
	})
}

// TestEncoder_PageIndexChunkBoundaries encodes enough pages to span several
// index chunks (including the maximum chunk size) and verifies the decoded
// index has every page in ascending order with contiguous offsets.
func TestEncoder_PageIndexChunkBoundaries(t *testing.T) {
	const pageSize, n = 512, 2*65536 + 259

	page := make([]byte, pageSize)
	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.EncodeHeader(ltx.Header{
		Version:          ltx.Version,
		PageSize:         pageSize,
		Commit:           n + 1,
		MinTXID:          2,
		MaxTXID:          2,
		Timestamp:        1000,
		PreApplyChecksum: ltx.ChecksumFlag | 1,
	}); err != nil {
		t.Fatal(err)
	}
	for pgno := uint32(2); pgno <= n+1; pgno++ {
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, page); err != nil {
			t.Fatalf("pgno %d: %v", pgno, err)
		}
	}
	enc.SetPostApplyChecksum(ltx.ChecksumFlag | 2)
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	dec := ltx.NewDecoder(bytes.NewReader(buf.Bytes()))
	if err := dec.Verify(); err != nil {
		t.Fatal(err)
	}
	index := dec.PageIndex()
	if got, want := len(index), n; got != want {
		t.Fatalf("index size=%d, want %d", got, want)
	}
	var prev ltx.PageIndexElem
	for pgno := uint32(2); pgno <= n+1; pgno++ {
		elem, ok := index[pgno]
		if !ok {
			t.Fatalf("pgno %d missing from index", pgno)
		}
		if pgno > 2 && elem.Offset != prev.Offset+prev.Size {
			t.Fatalf("pgno %d offset=%d, want contiguous after %d+%d", pgno, elem.Offset, prev.Offset, prev.Size)
		}
		prev = elem
	}
}
