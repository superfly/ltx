package ltx_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/superfly/ltx"
)

func TestHeader_Validate(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		hdr := ltx.Header{
			Version:          1,
			PageSize:         1024,
			Commit:           2,
			DBID:             1,
			MinTXID:          3,
			MaxTXID:          4,
			PreApplyChecksum: ltx.ChecksumFlag,
		}
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
	t.Run("ErrCommitRecordRequired", func(t *testing.T) {
		hdr := ltx.Header{Version: 1, PageSize: 1024}
		if err := hdr.Validate(); err == nil || err.Error() != `commit record required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrDBIDRequired", func(t *testing.T) {
		hdr := ltx.Header{Version: 1, PageSize: 1024, Commit: 2}
		if err := hdr.Validate(); err == nil || err.Error() != `database id required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrMinTXIDRequired", func(t *testing.T) {
		hdr := ltx.Header{Version: 1, PageSize: 1024, Commit: 2, DBID: 1}
		if err := hdr.Validate(); err == nil || err.Error() != `minimum transaction id required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrMaxTXIDRequired", func(t *testing.T) {
		hdr := ltx.Header{Version: 1, PageSize: 1024, Commit: 2, DBID: 1, MinTXID: 3}
		if err := hdr.Validate(); err == nil || err.Error() != `maximum transaction id required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrTXIDOutOfOrderRequired", func(t *testing.T) {
		hdr := ltx.Header{Version: 1, PageSize: 1024, Commit: 2, DBID: 1, MinTXID: 3, MaxTXID: 2}
		if err := hdr.Validate(); err == nil || err.Error() != `transaction ids out of order: (3,2)` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrSnapshotPreApplyChecksumNotAllowed", func(t *testing.T) {
		hdr := ltx.Header{Version: 1, PageSize: 1024, Commit: 4, DBID: 1, MinTXID: 1, MaxTXID: 3, PreApplyChecksum: 1}
		if err := hdr.Validate(); err == nil || err.Error() != `pre-apply checksum must be zero on snapshots` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrNonSnapshotPreApplyChecksumRequired", func(t *testing.T) {
		hdr := ltx.Header{Version: 1, PageSize: 1024, Commit: 4, DBID: 1, MinTXID: 2, MaxTXID: 3}
		if err := hdr.Validate(); err == nil || err.Error() != `pre-apply checksum required on non-snapshot files` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrInvalidPreApplyChecksumFormat", func(t *testing.T) {
		hdr := ltx.Header{Version: 1, PageSize: 1024, Commit: 4, DBID: 1, MinTXID: 2, MaxTXID: 3, PreApplyChecksum: 1}
		if err := hdr.Validate(); err == nil || err.Error() != `invalid pre-apply checksum format` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestHeader_MarshalBinary(t *testing.T) {
	hdr := ltx.Header{
		Version:   ltx.Version,
		Flags:     0,
		PageSize:  1024,
		Commit:    6,
		MinTXID:   7,
		MaxTXID:   8,
		Timestamp: 9,
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
	hdr := ltx.PageHeader{Pgno: 1000}

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
		if err := hdr.UnmarshalBinary(make([]byte, 2)); err != io.ErrShortBuffer {
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

func TestParseFilename(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		if min, max, err := ltx.ParseFilename("0000000000000001-00000000000003e8.ltx"); err != nil {
			t.Fatal(err)
		} else if got, want := min, uint64(1); got != want {
			t.Fatalf("min=%d, want %d", got, want)
		} else if got, want := max, uint64(1000); got != want {
			t.Fatalf("max=%d, want %d", got, want)
		}
	})

	t.Run("ErrInvalid", func(t *testing.T) {
		if _, _, err := ltx.ParseFilename("000000000000000z-00000000000003e8.ltx"); err == nil {
			t.Fatal("expected error")
		}
		if _, _, err := ltx.ParseFilename("0000000000000001.ltx"); err == nil {
			t.Fatal("expected error")
		}
		if _, _, err := ltx.ParseFilename("000000000000000z-00000000000003e8.zzz"); err == nil {
			t.Fatal("expected error")
		}
	})
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
func writeFileSpec(tb testing.TB, w io.Writer, spec *ltx.FileSpec) int64 {
	tb.Helper()
	n, err := spec.WriteTo(w)
	if err != nil {
		tb.Fatal(err)
	}
	return int64(n)
}

// readFileSpec is a helper function for reading a spec from a file.
func readFileSpec(tb testing.TB, r io.Reader, size int64) *ltx.FileSpec {
	tb.Helper()
	var spec ltx.FileSpec
	if _, err := spec.ReadFrom(r); err != nil {
		tb.Fatal(err)
	}
	return &spec
}

// compactFileSpecs compacts a set of file specs to a new spec.
func compactFileSpecs(tb testing.TB, inputs ...*ltx.FileSpec) (*ltx.FileSpec, error) {
	tb.Helper()

	// Write input specs to file.
	wtrs := make([]io.Writer, len(inputs))
	rdrs := make([]io.Reader, len(inputs))
	for i, input := range inputs {
		var buf bytes.Buffer
		wtrs[i], rdrs[i] = &buf, &buf
		writeFileSpec(tb, wtrs[i], input)
	}

	// Compact files together.
	var output bytes.Buffer
	c := ltx.NewCompactor(&output, rdrs)
	if err := c.Compact(context.Background()); err != nil {
		return nil, err
	}
	return readFileSpec(tb, &output, int64(output.Len())), nil
}

// assertFileSpecEqual checks x & y for equality. Fail on inequality.
func assertFileSpecEqual(tb testing.TB, x, y *ltx.FileSpec) {
	tb.Helper()

	if got, want := x.Header, y.Header; got != want {
		tb.Fatalf("header mismatch:\ngot=%#v\nwant=%#v", got, want)
	}

	if got, want := len(x.Pages), len(y.Pages); got != want {
		tb.Fatalf("page count: %d, want %d", got, want)
	}
	for i := range x.Pages {
		if got, want := x.Pages[i].Header, y.Pages[i].Header; got != want {
			tb.Fatalf("page header mismatch: i=%d\ngot=%#v\nwant=%#v", i, got, want)
		}
		if got, want := x.Pages[i].Data, y.Pages[i].Data; !bytes.Equal(got, want) {
			tb.Fatalf("page data mismatch: i=%d\ngot=%#v\nwant=%#v", i, got, want)
		}
	}

	if got, want := x.Trailer, y.Trailer; got != want {
		tb.Fatalf("trailer mismatch:\ngot=%#v\nwant=%#v", got, want)
	}
}
