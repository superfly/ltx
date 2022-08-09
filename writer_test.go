package ltx_test

import (
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/superfly/ltx"
)

func TestWriter(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		rnd := rand.New(rand.NewSource(0))
		page0 := make([]byte, 4096)
		rnd.Read(page0)
		page1 := make([]byte, 4096)
		rnd.Read(page1)

		w := ltx.NewWriter(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err := w.WriteHeader(ltx.Header{
			Version:          1,
			PageSize:         4096,
			Commit:           3,
			DBID:             4,
			MinTXID:          5,
			MaxTXID:          6,
			Timestamp:        2000,
			PreApplyChecksum: ltx.ChecksumFlag | 5,
		}); err != nil {
			t.Fatal(err)
		}

		// Write pages.
		if err := w.WritePage(ltx.PageHeader{Pgno: 1}, page0); err != nil {
			t.Fatal(err)
		}

		if err := w.WritePage(ltx.PageHeader{Pgno: 2}, page1); err != nil {
			t.Fatal(err)
		}

		// Flush checksum to header.
		w.SetPostApplyChecksum(ltx.ChecksumFlag | 6)
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		// Double close should be a no-op.
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestWriter_Close(t *testing.T) {
	t.Run("ErrInvalidState", func(t *testing.T) {
		w := ltx.NewWriter(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err := w.Close(); err == nil || err.Error() != `cannot close, expected header` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrClosed", func(t *testing.T) {
		w := ltx.NewWriter(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err := w.WriteHeader(ltx.Header{Version: 1, PageSize: 1024, Commit: 1, DBID: 1, MinTXID: 1, MaxTXID: 1}); err != nil {
			t.Fatal(err)
		} else if err := w.WritePage(ltx.PageHeader{Pgno: 1}, make([]byte, 1024)); err != nil {
			t.Fatal(err)
		} else if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		// Ensure all methods return an error after close.
		if err := w.WriteHeader(ltx.Header{}); err != ltx.ErrWriterClosed {
			t.Fatal(err)
		} else if err := w.WritePage(ltx.PageHeader{}, nil); err != ltx.ErrWriterClosed {
			t.Fatal(err)
		}
	})
}

func TestWriter_WriteHeader(t *testing.T) {
	t.Run("ErrInvalidState", func(t *testing.T) {
		w := ltx.NewWriter(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err := w.WriteHeader(ltx.Header{Version: 1, PageSize: 1024, Commit: 1, DBID: 1, MinTXID: 1, MaxTXID: 1}); err != nil {
			t.Fatal(err)
		}
		if err := w.WriteHeader(ltx.Header{}); err == nil || err.Error() != `cannot write header frame, expected page` {
			t.Fatal(err)
		}
	})
}

func TestWriter_WritePage(t *testing.T) {
	t.Run("ErrInvalidState", func(t *testing.T) {
		w := ltx.NewWriter(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err := w.WritePage(ltx.PageHeader{}, nil); err == nil || err.Error() != `cannot write page header, expected header` {
			t.Fatal(err)
		}
	})

	t.Run("ErrPageNumberRequired", func(t *testing.T) {
		w := ltx.NewWriter(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err := w.WriteHeader(ltx.Header{Version: 1, PageSize: 1024, Commit: 1, DBID: 1, MinTXID: 1, MaxTXID: 1}); err != nil {
			t.Fatal(err)
		} else if err := w.WritePage(ltx.PageHeader{Pgno: 0}, nil); err == nil || err.Error() != `page number required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrPageNumberOutOfBounds", func(t *testing.T) {
		w := ltx.NewWriter(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err := w.WriteHeader(ltx.Header{Version: 1, PageSize: 1024, Commit: 4, DBID: 1, MinTXID: 2, MaxTXID: 2, PreApplyChecksum: ltx.ChecksumFlag | 2}); err != nil {
			t.Fatal(err)
		} else if err := w.WritePage(ltx.PageHeader{Pgno: 5}, nil); err == nil || err.Error() != `page number 5 out-of-bounds for commit size 4` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrSnapshotInitialPage", func(t *testing.T) {
		w := ltx.NewWriter(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err := w.WriteHeader(ltx.Header{Version: 1, PageSize: 1024, Commit: 2, DBID: 1, MinTXID: 1, MaxTXID: 2}); err != nil {
			t.Fatal(err)
		} else if err := w.WritePage(ltx.PageHeader{Pgno: 2}, make([]byte, 1024)); err == nil || err.Error() != `snapshot transaction file must start with page number 1` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrSnapshotNonsequentialPages", func(t *testing.T) {
		w := ltx.NewWriter(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err := w.WriteHeader(ltx.Header{Version: 1, PageSize: 1024, Commit: 3, DBID: 1, MinTXID: 1, MaxTXID: 1}); err != nil {
			t.Fatal(err)
		}
		if err := w.WritePage(ltx.PageHeader{Pgno: 1}, make([]byte, 1024)); err != nil {
			t.Fatal(err)
		}

		if err := w.WritePage(ltx.PageHeader{Pgno: 3}, make([]byte, 1024)); err == nil || err.Error() != `nonsequential page numbers in snapshot transaction: 1,3` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrOutOfOrderPages", func(t *testing.T) {
		w := ltx.NewWriter(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err := w.WriteHeader(ltx.Header{Version: 1, PageSize: 1024, Commit: 2, DBID: 1, MinTXID: 2, MaxTXID: 2, PreApplyChecksum: ltx.ChecksumFlag | 2}); err != nil {
			t.Fatal(err)
		}
		if err := w.WritePage(ltx.PageHeader{Pgno: 2}, make([]byte, 1024)); err != nil {
			t.Fatal(err)
		}
		if err := w.WritePage(ltx.PageHeader{Pgno: 1}, make([]byte, 1024)); err == nil || err.Error() != `out-of-order page numbers: 2,1` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}
