package ltx_test

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/superfly/ltx"
)

func TestHeader_Validate(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		hdr := ltx.Header{Version: 1, PageSize: 1024, PageN: 1, Commit: 2, DBID: 1, MinTXID: 3, MaxTXID: 4}
		if err := hdr.Validate(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("ErrVersion", func(t *testing.T) {
		hdr := ltx.Header{}
		if err := hdr.Validate(); err == nil || err.Error() != `invalid version` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrFlags", func(t *testing.T) {
		hdr := ltx.Header{Version: 1, Flags: 2}
		if err := hdr.Validate(); err == nil || err.Error() != `invalid flags: 0x00000002` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrInvalidPageSize", func(t *testing.T) {
		hdr := ltx.Header{Version: 1, PageSize: 1000}
		if err := hdr.Validate(); err == nil || err.Error() != `invalid page size: 1000` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrPageCountRequired", func(t *testing.T) {
		hdr := ltx.Header{Version: 1, PageSize: 1024}
		if err := hdr.Validate(); err == nil || err.Error() != `page count required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrCommitRecordRequired", func(t *testing.T) {
		hdr := ltx.Header{Version: 1, PageSize: 1024, PageN: 1}
		if err := hdr.Validate(); err == nil || err.Error() != `commit record required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrEventDataSizeRequired", func(t *testing.T) {
		hdr := ltx.Header{Version: 1, PageSize: 1024, EventN: 1, PageN: 1, Commit: 1}
		if err := hdr.Validate(); err == nil || err.Error() != `event data size must be specified if events exist` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrEventNRequired", func(t *testing.T) {
		hdr := ltx.Header{Version: 1, PageSize: 1024, EventDataSize: 1, PageN: 1, Commit: 1}
		if err := hdr.Validate(); err == nil || err.Error() != `event data size must be zero if no events exist` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrDBIDRequired", func(t *testing.T) {
		hdr := ltx.Header{Version: 1, PageSize: 1024, PageN: 1, Commit: 2}
		if err := hdr.Validate(); err == nil || err.Error() != `database id required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrMinTXIDRequired", func(t *testing.T) {
		hdr := ltx.Header{Version: 1, PageSize: 1024, PageN: 1, Commit: 2, DBID: 1}
		if err := hdr.Validate(); err == nil || err.Error() != `minimum transaction id required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrMaxTXIDRequired", func(t *testing.T) {
		hdr := ltx.Header{Version: 1, PageSize: 1024, PageN: 1, Commit: 2, DBID: 1, MinTXID: 3}
		if err := hdr.Validate(); err == nil || err.Error() != `maximum transaction id required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrTXIDOutOfOrderRequired", func(t *testing.T) {
		hdr := ltx.Header{Version: 1, PageSize: 1024, PageN: 1, Commit: 2, DBID: 1, MinTXID: 3, MaxTXID: 2}
		if err := hdr.Validate(); err == nil || err.Error() != `transaction ids out of order: (3,2)` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrSnapshotPageCount", func(t *testing.T) {
		hdr := ltx.Header{Version: 1, PageSize: 1024, PageN: 3, Commit: 4, DBID: 1, MinTXID: 1, MaxTXID: 3}
		if err := hdr.Validate(); err == nil || err.Error() != `snapshot page count 3 must equal commit size 4` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestHeader_MarshalBinary(t *testing.T) {
	hdr := ltx.Header{
		Version:             ltx.Version,
		Flags:               0,
		PageSize:            1024,
		PageN:               4,
		EventN:              5,
		Commit:              6,
		MinTXID:             7,
		MaxTXID:             8,
		Timestamp:           9,
		HeaderBlockChecksum: 10,
		PageBlockChecksum:   11,
	}

	var other ltx.Header
	if b, err := hdr.MarshalBinary(); err != nil {
		t.Fatal(err)
	} else if err := other.UnmarshalBinary(b); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(hdr, other) {
		t.Fatalf("mismatch:\ngot=%#v\nwant=%#v", hdr, other)
	}
}

func TestHeader_UnmarshalBinary(t *testing.T) {
	t.Run("ErrShortBuffer", func(t *testing.T) {
		var hdr ltx.Header
		if err := hdr.UnmarshalBinary(make([]byte, 10)); err != io.ErrShortBuffer {
			t.Fatal(err)
		}
	})
	t.Run("ErrInvalidFile", func(t *testing.T) {
		var hdr ltx.Header
		if err := hdr.UnmarshalBinary(make([]byte, ltx.HeaderSize)); err != ltx.ErrInvalidFile {
			t.Fatal(err)
		}
	})
}

func TestEventHeader_Validate(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		hdr := ltx.EventHeader{Size: 1}
		if err := hdr.Validate(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("ErrSizeRequired", func(t *testing.T) {
		hdr := ltx.EventHeader{}
		if err := hdr.Validate(); err == nil || err.Error() != `size required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestEventHeader_MarshalBinary(t *testing.T) {
	hdr := ltx.EventHeader{
		Size:  1000,
		Nonce: [12]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
		Tag:   [16]byte{12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27},
	}

	var other ltx.EventHeader
	if b, err := hdr.MarshalBinary(); err != nil {
		t.Fatal(err)
	} else if err := other.UnmarshalBinary(b); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(hdr, other) {
		t.Fatalf("mismatch:\ngot=%#v\nwant=%#v", hdr, other)
	}
}

func TestEventHeader_UnmarshalBinary(t *testing.T) {
	t.Run("ErrShortBuffer", func(t *testing.T) {
		var hdr ltx.EventHeader
		if err := hdr.UnmarshalBinary(make([]byte, 10)); err != io.ErrShortBuffer {
			t.Fatal(err)
		}
	})
}

func TestPageHeader_Validate(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		hdr := ltx.PageHeader{Pgno: 1}
		if err := hdr.Validate(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("ErrPgnoRequired", func(t *testing.T) {
		hdr := ltx.PageHeader{}
		if err := hdr.Validate(); err == nil || err.Error() != `page number required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestPageHeader_MarshalBinary(t *testing.T) {
	hdr := ltx.PageHeader{
		Pgno:  1000,
		Nonce: [12]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
		Tag:   [16]byte{12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27},
	}

	var other ltx.PageHeader
	if b, err := hdr.MarshalBinary(); err != nil {
		t.Fatal(err)
	} else if err := other.UnmarshalBinary(b); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(hdr, other) {
		t.Fatalf("mismatch:\ngot=%#v\nwant=%#v", hdr, other)
	}
}

func TestPageHeader_UnmarshalBinary(t *testing.T) {
	t.Run("ErrShortBuffer", func(t *testing.T) {
		var hdr ltx.PageHeader
		if err := hdr.UnmarshalBinary(make([]byte, 10)); err != io.ErrShortBuffer {
			t.Fatal(err)
		}
	})
}

func TestIsValidHeaderFlags(t *testing.T) {
	if !ltx.IsValidHeaderFlags(0) {
		t.Fatal("expected valid")
	} else if ltx.IsValidHeaderFlags(1) {
		t.Fatal("expected invalid")
	}
}

func TestIsValidPageSize(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		for _, sz := range []uint32{512, 1024, 2048, 4096, 8192, 16384, 32768, 65536} {
			if !ltx.IsValidPageSize(sz) {
				t.Fatalf("expected page size of %d to be valid", sz)
			}
		}
	})
	t.Run("TooSmall", func(t *testing.T) {
		if ltx.IsValidPageSize(256) {
			t.Fatal("expected invalid")
		}
	})
	t.Run("TooLarge", func(t *testing.T) {
		if ltx.IsValidPageSize(131072) {
			t.Fatal("expected invalid")
		}
	})
	t.Run("NotPowerOfTwo", func(t *testing.T) {
		if ltx.IsValidPageSize(1000) {
			t.Fatal("expected invalid")
		}
	})
}
func TestPageAlign(t *testing.T) {
	if got, want := ltx.PageAlign(0, 1024), int64(0); got != want {
		t.Fatalf("PageAlign=%d, want %d", got, want)
	}
	if got, want := ltx.PageAlign(100, 1024), int64(1024); got != want {
		t.Fatalf("PageAlign=%d, want %d", got, want)
	}
	if got, want := ltx.PageAlign(1023, 1024), int64(1024); got != want {
		t.Fatalf("PageAlign=%d, want %d", got, want)
	}
	if got, want := ltx.PageAlign(1024, 1024), int64(1024); got != want {
		t.Fatalf("PageAlign=%d, want %d", got, want)
	}
	if got, want := ltx.PageAlign(1025, 2048), int64(2048); got != want {
		t.Fatalf("PageAlign=%d, want %d", got, want)
	}
}

// createFile creates a file and returns the file handle. Closes on cleanup.
func createFile(tb testing.TB, name string) *os.File {
	tb.Helper()
	f, err := os.Create(name)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { f.Close() })
	return f
}

// openFile opens a file and returns the file handle. Fail on error.
func openFile(tb testing.TB, name string) *os.File {
	tb.Helper()
	f, err := os.Open(name)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { f.Close() })
	return f
}

// writeFileSpec is a helper function for writing a spec to a file.
func writeFileSpec(tb testing.TB, filename string, spec *ltx.FileSpec) {
	tb.Helper()
	if _, err := spec.WriteTo(createFile(tb, filename)); err != nil {
		tb.Fatal(err)
	}
}

// readFileSpec is a helper function for reading a spec from a file.
func readFileSpec(tb testing.TB, filename string) *ltx.FileSpec {
	tb.Helper()
	var spec ltx.FileSpec
	if _, err := spec.ReadFrom(openFile(tb, filename)); err != nil {
		tb.Fatal(err)
	}
	return &spec
}

// compactFileSpecs compacts a set of file specs to a new spec.
func compactFileSpecs(tb testing.TB, c *ltx.Compactor, inputs ...*ltx.FileSpec) (*ltx.FileSpec, error) {
	tb.Helper()
	dir := tb.TempDir()
	defer os.RemoveAll(dir)

	// Write input specs to file.
	var filenames []string
	for i, input := range inputs {
		filename := filepath.Join(dir, fmt.Sprintf("input%d", i))
		writeFileSpec(tb, filename, input)
		filenames = append(filenames, filename)
	}

	// Compact files together.
	if err := c.Compact(filepath.Join(dir, "output"), filenames); err != nil {
		return nil, err
	}
	return readFileSpec(tb, filepath.Join(dir, "output")), nil
}

// assertFileSpecEqual checks x & y for equality. Fail on inequality.
func assertFileSpecEqual(tb testing.TB, x, y *ltx.FileSpec) {
	tb.Helper()

	// Do not match on checksums in this assertion as most specs will be in-memory.
	hx, hy := x.Header, y.Header
	hx.HeaderBlockChecksum, hx.PageBlockChecksum = 0, 0
	hy.HeaderBlockChecksum, hy.PageBlockChecksum = 0, 0

	if got, want := hx, hy; got != want {
		tb.Fatalf("header mismatch:\ngot=%#v\nwant=%#v", got, want)
	}

	if got, want := len(x.PageHeaders), len(y.PageHeaders); got != want {
		tb.Fatalf("page header count: %d, want %d", got, want)
	}
	for i := range x.PageHeaders {
		if got, want := x.PageHeaders[i], y.PageHeaders[i]; got != want {
			tb.Fatalf("page header mismatch: i=%d\ngot=%#v\nwant=%#v", i, got, want)
		}
	}

	if got, want := len(x.EventHeaders), len(y.EventHeaders); got != want {
		tb.Fatalf("event header count: %d, want %d", got, want)
	}
	for i := range x.EventHeaders {
		if got, want := x.EventHeaders[i], y.EventHeaders[i]; got != want {
			tb.Fatalf("event header mismatch: i=%d\ngot=%#v\nwant=%#v", i, got, want)
		}
	}

	if got, want := len(x.PageData), len(y.PageData); got != want {
		tb.Fatalf("page data count: %d, want %d", got, want)
	}
	for i := range x.PageData {
		if got, want := x.PageData[i], y.PageData[i]; !bytes.Equal(got, want) {
			tb.Fatalf("page data mismatch: i=%d\ngot=%#v\nwant=%#v", i, got, want)
		}
	}

	if got, want := len(x.EventData), len(y.EventData); got != want {
		tb.Fatalf("event data count: %d, want %d", got, want)
	}
	for i := range x.EventData {
		if got, want := x.EventData[i], y.EventData[i]; !bytes.Equal(got, want) {
			tb.Fatalf("event data mismatch: i=%d\ngot=%#v\nwant=%#v", i, got, want)
		}
	}
}

// assertFileSpecChecksum checks x & y checksums for equality. Fail on inequality.
func assertFileSpecChecksum(tb testing.TB, x, y *ltx.FileSpec) {
	tb.Helper()

	if got, want := x.Header.HeaderBlockChecksum, y.Header.HeaderBlockChecksum; got != want {
		tb.Fatalf("header block checksum=%016x, want=%016x", got, want)
	}
	if got, want := x.Header.PageBlockChecksum, y.Header.PageBlockChecksum; got != want {
		tb.Fatalf("page block checksum=%016x, want=%016x", got, want)
	}
}
