package ltx_test

import (
	"io"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/superfly/ltx"
)

func TestEncoder(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		rnd := rand.New(rand.NewSource(0))
		page0 := make([]byte, 4096)
		rnd.Read(page0)
		page1 := make([]byte, 4096)
		rnd.Read(page1)

		enc, err := ltx.NewEncoder(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodeHeader(ltx.Header{
			Version:          ltx.Version,
			PageSize:         4096,
			Commit:           3,
			MinTXID:          5,
			MaxTXID:          6,
			Timestamp:        2000,
			PreApplyChecksum: ltx.ChecksumFlag | 5,
		}); err != nil {
			t.Fatal(err)
		}

		// Write pages.
		if err := enc.EncodePage(ltx.PageHeader{Pgno: 1}, page0); err != nil {
			t.Fatal(err)
		}

		if err := enc.EncodePage(ltx.PageHeader{Pgno: 2}, page1); err != nil {
			t.Fatal(err)
		}

		// Flush checksum to header.
		enc.SetPostApplyChecksum(ltx.ChecksumFlag | 6)
		if err := enc.Close(); err != nil {
			t.Fatal(err)
		}

		// Double close should be a no-op.
		if err := enc.Close(); err != nil {
			t.Fatal(err)
		}

		if got, want := enc.Header().PreApplyPos(), (ltx.Pos{4, ltx.ChecksumFlag | 5}); got != want {
			t.Fatalf("PreApplyPos=%s, want %s", got, want)
		}
		if got, want := enc.PostApplyPos(), (ltx.Pos{6, ltx.ChecksumFlag | 6}); got != want {
			t.Fatalf("PostApplyPos=%s, want %s", got, want)
		}
	})

	// Ensure encoder can generate LTX files with a zero commit and no pages.
	t.Run("CommitZero", func(t *testing.T) {
		enc, err := ltx.NewEncoder(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodeHeader(ltx.Header{
			Version:          ltx.Version,
			PageSize:         4096,
			Commit:           0,
			MinTXID:          5,
			MaxTXID:          6,
			Timestamp:        2000,
			PreApplyChecksum: ltx.ChecksumFlag | 5,
		}); err != nil {
			t.Fatal(err)
		}

		enc.SetPostApplyChecksum(ltx.ChecksumFlag)
		if err := enc.Close(); err != nil {
			t.Fatal(err)
		}

		if got, want := enc.Header().PreApplyPos(), (ltx.Pos{4, ltx.ChecksumFlag | 5}); got != want {
			t.Fatalf("PreApplyPos=%s, want %s", got, want)
		}
		if got, want := enc.PostApplyPos(), (ltx.Pos{6, ltx.ChecksumFlag}); got != want {
			t.Fatalf("PostApplyPos=%s, want %s", got, want)
		}
	})

	// Ensure encoder has an empty post-apply checksum when encoding a deletion file.
	t.Run("ErrInvalidCommitZeroPostApplyChecksum", func(t *testing.T) {
		enc, err := ltx.NewEncoder(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, PageSize: 4096, Commit: 0, MinTXID: 5, MaxTXID: 6, Timestamp: 2000, PreApplyChecksum: ltx.ChecksumFlag | 5}); err != nil {
			t.Fatal(err)
		}

		enc.SetPostApplyChecksum(ltx.ChecksumFlag | 1)
		if err := enc.Close(); err == nil || err.Error() != `post-apply checksum must be empty for zero-length database` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestEncode_Close(t *testing.T) {
	t.Run("ErrInvalidState", func(t *testing.T) {
		enc, err := ltx.NewEncoder(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.Close(); err == nil || err.Error() != `cannot close, expected header` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrClosed", func(t *testing.T) {
		enc, err := ltx.NewEncoder(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, PageSize: 1024, Commit: 1, MinTXID: 1, MaxTXID: 1}); err != nil {
			t.Fatal(err)
		} else if err := enc.EncodePage(ltx.PageHeader{Pgno: 1}, make([]byte, 1024)); err != nil {
			t.Fatal(err)
		}

		enc.SetPostApplyChecksum(ltx.ChecksumFlag)
		if err := enc.Close(); err != nil {
			t.Fatal(err)
		}

		// Ensure all methods return an error after close.
		if err := enc.EncodeHeader(ltx.Header{}); err != ltx.ErrEncoderClosed {
			t.Fatal(err)
		} else if err := enc.EncodePage(ltx.PageHeader{}, nil); err != ltx.ErrEncoderClosed {
			t.Fatal(err)
		}
	})
}

func TestEncode_EncodeHeader(t *testing.T) {
	t.Run("ErrInvalidState", func(t *testing.T) {
		enc, err := ltx.NewEncoder(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, PageSize: 1024, Commit: 1, MinTXID: 1, MaxTXID: 1}); err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodeHeader(ltx.Header{}); err == nil || err.Error() != `cannot encode header frame, expected page` {
			t.Fatal(err)
		}
	})
}

func TestEncode_EncodePage(t *testing.T) {
	t.Run("ErrInvalidState", func(t *testing.T) {
		enc, err := ltx.NewEncoder(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodePage(ltx.PageHeader{}, nil); err == nil || err.Error() != `cannot encode page header, expected header` {
			t.Fatal(err)
		}
	})

	t.Run("ErrPageNumberRequired", func(t *testing.T) {
		enc, err := ltx.NewEncoder(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, PageSize: 1024, Commit: 1, MinTXID: 1, MaxTXID: 1}); err != nil {
			t.Fatal(err)
		} else if err := enc.EncodePage(ltx.PageHeader{Pgno: 0}, nil); err == nil || err.Error() != `page number required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrPageNumberOutOfBounds", func(t *testing.T) {
		enc, err := ltx.NewEncoder(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, PageSize: 1024, Commit: 4, MinTXID: 2, MaxTXID: 2, PreApplyChecksum: ltx.ChecksumFlag | 2}); err != nil {
			t.Fatal(err)
		} else if err := enc.EncodePage(ltx.PageHeader{Pgno: 5}, nil); err == nil || err.Error() != `page number 5 out-of-bounds for commit size 4` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrSnapshotInitialPage", func(t *testing.T) {
		enc, err := ltx.NewEncoder(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, PageSize: 1024, Commit: 2, MinTXID: 1, MaxTXID: 2}); err != nil {
			t.Fatal(err)
		} else if err := enc.EncodePage(ltx.PageHeader{Pgno: 2}, make([]byte, 1024)); err == nil || err.Error() != `snapshot transaction file must start with page number 1` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrSnapshotNonsequentialPages", func(t *testing.T) {
		enc, err := ltx.NewEncoder(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, PageSize: 1024, Commit: 3, MinTXID: 1, MaxTXID: 1}); err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: 1}, make([]byte, 1024)); err != nil {
			t.Fatal(err)
		}

		if err := enc.EncodePage(ltx.PageHeader{Pgno: 3}, make([]byte, 1024)); err == nil || err.Error() != `nonsequential page numbers in snapshot transaction: 1,3` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrCannotEncodeLockPage", func(t *testing.T) {
		enc, err := ltx.NewEncoder(io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, PageSize: 4096, Commit: 262145, MinTXID: 1, MaxTXID: 1}); err != nil {
			t.Fatal(err)
		}

		pageBuf := make([]byte, 4096)
		for pgno := uint32(1); pgno <= 262144; pgno++ {
			if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, pageBuf); err != nil {
				t.Fatal(err)
			}
		}

		// Try to encode lock page.
		if err := enc.EncodePage(ltx.PageHeader{Pgno: 262145}, pageBuf); err == nil || err.Error() != `cannot encode lock page: pgno=262145` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrSnapshotNonsequentialPagesAfterLockPage", func(t *testing.T) {
		enc, err := ltx.NewEncoder(io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, PageSize: 4096, Commit: 262147, MinTXID: 1, MaxTXID: 1}); err != nil {
			t.Fatal(err)
		}

		pageBuf := make([]byte, 4096)
		for pgno := uint32(1); pgno <= 262144; pgno++ {
			if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, pageBuf); err != nil {
				t.Fatal(err)
			}
		}

		// Try to encode lock page.
		if err := enc.EncodePage(ltx.PageHeader{Pgno: 262147}, pageBuf); err == nil || err.Error() != `nonsequential page numbers in snapshot transaction (skip lock page): 262144,262147` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrOutOfOrderPages", func(t *testing.T) {
		enc, err := ltx.NewEncoder(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, PageSize: 1024, Commit: 2, MinTXID: 2, MaxTXID: 2, PreApplyChecksum: ltx.ChecksumFlag | 2}); err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: 2}, make([]byte, 1024)); err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: 1}, make([]byte, 1024)); err == nil || err.Error() != `out-of-order page numbers: 2,1` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}
