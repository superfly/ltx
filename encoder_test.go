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
	// in the file checksum calculation to prevent tampering.
	// This test creates two identical files except one has the page index tampered
	// and verifies that the tampered file is detected.
	t.Run("PageIndexInChecksum", func(t *testing.T) {
		// Test with checksums enabled to verify page index is included
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
		if err := enc1.EncodePage(ltx.PageHeader{Pgno: 1}, bytes.Repeat([]byte{0x11}, 512)); err != nil {
			t.Fatal(err)
		}
		if err := enc1.EncodePage(ltx.PageHeader{Pgno: 2}, bytes.Repeat([]byte{0x22}, 512)); err != nil {
			t.Fatal(err)
		}

		// Set post-apply checksum (required for snapshots)
		enc1.SetPostApplyChecksum(ltx.ChecksumFlag | 0x1234)

		// Close the encoder
		if err := enc1.Close(); err != nil {
			t.Fatal(err)
		}

		originalData := buf1.Bytes()
		originalChecksum := enc1.Trailer().FileChecksum
		
		// Now tamper with the page index
		tamperedData := make([]byte, len(originalData))
		copy(tamperedData, originalData)
		
		// Find page index location
		trailerStart := len(tamperedData) - 16
		pageIndexSizeStart := trailerStart - 8
		
		// Find the empty page header (4 zeros)
		emptyHeaderPos := -1
		for i := 100; i < len(tamperedData)-20; i++ {
			if tamperedData[i] == 0 && tamperedData[i+1] == 0 && 
			   tamperedData[i+2] == 0 && tamperedData[i+3] == 0 {
				emptyHeaderPos = i
				break
			}
		}
		
		if emptyHeaderPos == -1 {
			t.Fatal("Could not find empty page header")
		}
		
		// Tamper with page index data
		pageIndexStart := emptyHeaderPos + 4
		if pageIndexStart < pageIndexSizeStart {
			// Modify a byte in the page index
			tamperedData[pageIndexStart] ^= 0xFF
		}
		
		// Try to decode the tampered file
		dec := ltx.NewDecoder(bytes.NewReader(tamperedData))
		if err := dec.DecodeHeader(); err != nil {
			t.Fatal(err)
		}
		
		// Read all pages
		pageData := make([]byte, 512)
		pagesRead := 0
		for {
			var pageHeader ltx.PageHeader
			if err := dec.DecodePage(&pageHeader, pageData); err != nil {
				if err == io.EOF {
					break
				}
				t.Fatalf("Error reading page: %v", err)
			}
			pagesRead++
		}
		
		t.Logf("Read %d pages before Close", pagesRead)
		
		// Close should fail with checksum mismatch if page index is included
		err = dec.Close()
		if err == nil {
			t.Fatal("Expected checksum mismatch error, but got none - page index is NOT in checksum!")
		}
		
		if err != ltx.ErrChecksumMismatch {
			t.Fatalf("Expected ErrChecksumMismatch, got: %v", err)
		}
		
		t.Log("Success: Page index tampering was detected by checksum validation")
		t.Logf("Original checksum: %x", originalChecksum)
	})
}
