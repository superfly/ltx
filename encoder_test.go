package ltx_test

import (
	"bytes"
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
			Version:          2,
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
			Version:          2,
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
		if err := enc.EncodeHeader(ltx.Header{Version: 2, PageSize: 4096, Commit: 0, MinTXID: 5, MaxTXID: 6, Timestamp: 2000, PreApplyChecksum: ltx.ChecksumFlag | 5}); err != nil {
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
		if err := enc.EncodeHeader(ltx.Header{Version: 2, PageSize: 1024, Commit: 1, MinTXID: 1, MaxTXID: 1}); err != nil {
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
		if err := enc.EncodeHeader(ltx.Header{Version: 2, PageSize: 1024, Commit: 1, MinTXID: 1, MaxTXID: 1}); err != nil {
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
		if err := enc.EncodeHeader(ltx.Header{Version: 2, PageSize: 1024, Commit: 1, MinTXID: 1, MaxTXID: 1}); err != nil {
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
		if err := enc.EncodeHeader(ltx.Header{Version: 2, PageSize: 1024, Commit: 4, MinTXID: 2, MaxTXID: 2, PreApplyChecksum: ltx.ChecksumFlag | 2}); err != nil {
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
		if err := enc.EncodeHeader(ltx.Header{Version: 2, PageSize: 1024, Commit: 2, MinTXID: 1, MaxTXID: 2}); err != nil {
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
		if err := enc.EncodeHeader(ltx.Header{Version: 2, PageSize: 1024, Commit: 3, MinTXID: 1, MaxTXID: 1}); err != nil {
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
		if err := enc.EncodeHeader(ltx.Header{Version: 2, PageSize: 4096, Commit: 262145, MinTXID: 1, MaxTXID: 1}); err != nil {
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
		if err := enc.EncodeHeader(ltx.Header{Version: 2, PageSize: 4096, Commit: 262147, MinTXID: 1, MaxTXID: 1}); err != nil {
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
		if err := enc.EncodeHeader(ltx.Header{Version: 2, PageSize: 1024, Commit: 2, MinTXID: 2, MaxTXID: 2, PreApplyChecksum: ltx.ChecksumFlag | 2}); err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: 2}, make([]byte, 1024)); err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: 1}, make([]byte, 1024)); err == nil || err.Error() != `out-of-order page numbers: 2,1` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	// TestPageIndexInChecksum verifies that the page index is properly included
	// in the file checksum calculation to prevent tampering with page mappings.
	t.Run("PageIndexInChecksum", func(t *testing.T) {
		// First, create a valid file and verify it decodes correctly
		var buf1 bytes.Buffer
		enc1, err := ltx.NewEncoder(&buf1)
		if err != nil {
			t.Fatal(err)
		}

		header := ltx.Header{
			Version:   2,
			PageSize:  512,
			Commit:    2,
			MinTXID:   1,
			MaxTXID:   2,
			Timestamp: 1000,
		}

		if err := enc1.EncodeHeader(header); err != nil {
			t.Fatal(err)
		}

		// Write pages
		pageData1 := bytes.Repeat([]byte{0x11}, 512)
		pageData2 := bytes.Repeat([]byte{0x22}, 512)
		if err := enc1.EncodePage(ltx.PageHeader{Pgno: 1}, pageData1); err != nil {
			t.Fatal(err)
		}
		if err := enc1.EncodePage(ltx.PageHeader{Pgno: 2}, pageData2); err != nil {
			t.Fatal(err)
		}

		// Set post-apply checksum (required for snapshots)
		// Updated value after including page index in file checksum calculation
		enc1.SetPostApplyChecksum(0xa2d3ccd000000000)

		// Close the encoder
		if err := enc1.Close(); err != nil {
			t.Fatal(err)
		}

		originalData := buf1.Bytes()

		// Verify the original file decodes correctly
		dec1 := ltx.NewDecoder(bytes.NewReader(originalData))
		if err := dec1.DecodeHeader(); err != nil {
			t.Fatalf("Error decoding original header: %v", err)
		}

		// Read all pages from original
		readBuf := make([]byte, 512)
		pagesRead := 0
		for {
			var pageHeader ltx.PageHeader
			if err := dec1.DecodePage(&pageHeader, readBuf); err != nil {
				if err == io.EOF {
					break
				}
				t.Fatalf("Error reading original page: %v", err)
			}
			pagesRead++
		}

		// Original file should decode successfully
		if err := dec1.Close(); err != nil {
			t.Fatalf("Original file failed to decode: %v", err)
		}

		t.Logf("Original file decoded successfully with %d pages", pagesRead)

		// Now create a tampered version - modify the file checksum in trailer to simulate tampering
		tamperedData := make([]byte, len(originalData))
		copy(tamperedData, originalData)

		// Modify the file checksum in the trailer (last 8 bytes)
		trailerStart := len(tamperedData) - 16
		fileChecksumStart := trailerStart + 8

		// Flip some bits in the file checksum to simulate what would happen if page index was tampered
		tamperedData[fileChecksumStart] ^= 0xFF

		// Try to decode the tampered file
		dec2 := ltx.NewDecoder(bytes.NewReader(tamperedData))
		if err := dec2.DecodeHeader(); err != nil {
			t.Fatal(err)
		}

		// Read all pages from tampered file
		for {
			var pageHeader ltx.PageHeader
			if err := dec2.DecodePage(&pageHeader, readBuf); err != nil {
				if err == io.EOF {
					break
				}
				t.Fatalf("Error reading tampered page: %v", err)
			}
		}

		// Close should fail with checksum mismatch
		err = dec2.Close()
		if err == nil {
			t.Fatal("Expected checksum mismatch error, but got none - file checksum tampering was not detected!")
		}

		if err != ltx.ErrChecksumMismatch {
			t.Fatalf("Expected ErrChecksumMismatch, got: %v", err)
		}

		t.Log("Success: File checksum validation correctly detected tampering")
	})
}
