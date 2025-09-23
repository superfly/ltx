package ltx_test

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/superfly/ltx"
)

func TestDecoder(t *testing.T) {
	spec := &ltx.FileSpec{
		Header: ltx.Header{
			Version:   ltx.Version,
			PageSize:  1024,
			Commit:    2,
			MinTXID:   1,
			MaxTXID:   1,
			Timestamp: 1000,
		},
		Pages: []ltx.PageSpec{
			{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("2"), 1024)},
			{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte("3"), 1024)},
		},
		Trailer: ltx.Trailer{PostApplyChecksum: 0xe1899b6d587aaaaa},
	}

	// Write spec to file.
	var buf bytes.Buffer
	writeFileSpec(t, &buf, spec)
	fileSpecData := buf.Bytes()

	// Read and verify data matches spec.
	dec := ltx.NewDecoder(&buf)

	// Verify header.
	if err := dec.DecodeHeader(); err != nil {
		t.Fatal(err)
	} else if got, want := dec.Header(), spec.Header; !reflect.DeepEqual(got, want) {
		t.Fatalf("header mismatch:\ngot=%#v\nwant=%#v", got, want)
	}

	// Verify page headers.
	for i := range spec.Pages {
		var hdr ltx.PageHeader
		data := make([]byte, 1024)
		if err := dec.DecodePage(&hdr, data); err != nil {
			t.Fatal(err)
		} else if got, want := hdr, spec.Pages[i].Header; got != want {
			t.Fatalf("page hdr mismatch:\ngot=%#v\nwant=%#v", got, want)
		} else if got, want := data, spec.Pages[i].Data; !bytes.Equal(got, want) {
			t.Fatalf("page data mismatch:\ngot=%#v\nwant=%#v", got, want)
		}
	}

	if err := dec.DecodePage(&ltx.PageHeader{}, make([]byte, 1024)); err != io.EOF {
		t.Fatalf("expected page header eof, got: %s", err)
	}

	// Close reader to verify integrity.
	if err := dec.Close(); err != nil {
		t.Fatal(err)
	}

	// Verify page index.
	index := dec.PageIndex()
	if got, want := index, map[uint32]ltx.PageIndexElem{
		1: {MinTXID: 1, MaxTXID: 1, Offset: 100, Size: 51},
		2: {MinTXID: 1, MaxTXID: 1, Offset: 151, Size: 51},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("page index mismatch:\ngot=%#v\nwant=%#v", got, want)
	}

	// Read page 1 by offset.
	if hdr, data, err := ltx.DecodePageData(fileSpecData[100:]); err != nil {
		t.Fatal(err)
	} else if got, want := hdr, (ltx.PageHeader{Pgno: 1}); got != want {
		t.Fatalf("page header mismatch:\ngot=%#v\nwant=%#v", got, want)
	} else if got, want := data, bytes.Repeat([]byte("2"), 1024); !bytes.Equal(got, want) {
		t.Fatalf("page data mismatch:\ngot=%#v\nwant=%#v", got, want)
	}

	// Read page 2 by offset.
	if hdr, data, err := ltx.DecodePageData(fileSpecData[151:]); err != nil {
		t.Fatal(err)
	} else if got, want := hdr, (ltx.PageHeader{Pgno: 2}); got != want {
		t.Fatalf("page header mismatch:\ngot=%#v\nwant=%#v", got, want)
	} else if got, want := data, bytes.Repeat([]byte("3"), 1024); !bytes.Equal(got, want) {
		t.Fatalf("page data mismatch:\ngot=%#v\nwant=%#v", got, want)
	}

	if got, want := dec.Header().PreApplyPos(), (ltx.Pos{}); got != want {
		t.Fatalf("PreApplyPos=%s, want %s", got, want)
	}
	if got, want := dec.PostApplyPos(), (ltx.Pos{1, 0xe1899b6d587aaaaa}); got != want {
		t.Fatalf("PostApplyPos=%s, want %s", got, want)
	}
}

func TestDecoder_Decode_CommitZero(t *testing.T) {
	spec := &ltx.FileSpec{
		Header: ltx.Header{
			Version:   ltx.Version,
			Flags:     0,
			PageSize:  1024,
			Commit:    0,
			MinTXID:   1,
			MaxTXID:   1,
			Timestamp: 1000,
		},
		Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag},
	}

	// Write spec to file.
	var buf bytes.Buffer
	writeFileSpec(t, &buf, spec)

	// Read and verify data matches spec.
	dec := ltx.NewDecoder(&buf)

	// Verify header.
	if err := dec.DecodeHeader(); err != nil {
		t.Fatal(err)
	} else if got, want := dec.Header(), spec.Header; !reflect.DeepEqual(got, want) {
		t.Fatalf("header mismatch:\ngot=%#v\nwant=%#v", got, want)
	}

	if err := dec.DecodePage(&ltx.PageHeader{}, make([]byte, 1024)); err != io.EOF {
		t.Fatal("expected page header eof")
	}

	// Close reader to verify integrity.
	if err := dec.Close(); err != nil {
		t.Fatal(err)
	}

	if got, want := dec.Header().PreApplyPos(), (ltx.Pos{}); got != want {
		t.Fatalf("PreApplyPos=%s, want %s", got, want)
	}
	if got, want := dec.PostApplyPos(), (ltx.Pos{1, ltx.ChecksumFlag}); got != want {
		t.Fatalf("PostApplyPos=%s, want %s", got, want)
	}
}

func TestDecoder_DecodeDatabaseTo(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		spec := &ltx.FileSpec{
			Header: ltx.Header{Version: ltx.Version, Flags: 0, PageSize: 512, Commit: 2, MinTXID: 1, MaxTXID: 2, Timestamp: 1000},
			Pages: []ltx.PageSpec{
				{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("2"), 512)},
				{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte("3"), 512)},
			},
			Trailer: ltx.Trailer{PostApplyChecksum: 0x8b87423eeeeeeeee},
		}

		// Decode serialized LTX file.
		var buf bytes.Buffer
		writeFileSpec(t, &buf, spec)
		dec := ltx.NewDecoder(&buf)

		var out bytes.Buffer
		if err := dec.DecodeDatabaseTo(&out); err != nil {
			t.Fatal(err)
		} else if got, want := out.Bytes(), append(bytes.Repeat([]byte("2"), 512), bytes.Repeat([]byte("3"), 512)...); !bytes.Equal(got, want) {
			t.Fatal("output mismatch")
		}
	})

	t.Run("WithLockPage", func(t *testing.T) {
		if testing.Short() {
			t.Skip("skipping in short mode")
		}

		lockPgno := ltx.LockPgno(4096)
		commit := lockPgno + 10

		var want bytes.Buffer
		var buf bytes.Buffer
		enc, err := ltx.NewEncoder(&buf)
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, Flags: 0, PageSize: 4096, Commit: commit, MinTXID: 1, MaxTXID: 2, Timestamp: 1000}); err != nil {
			t.Fatal(err)
		}

		pageData := bytes.Repeat([]byte("x"), 4096)
		for pgno := uint32(1); pgno <= commit; pgno++ {
			if pgno == lockPgno {
				_, _ = want.Write(make([]byte, 4096))
				continue
			}

			_, _ = want.Write(pageData)
			if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, pageData); err != nil {
				t.Fatal(err)
			}
		}

		enc.SetPostApplyChecksum(0xc19b668c376662c7)
		if err := enc.Close(); err != nil {
			t.Fatal(err)
		}

		// Decode serialized LTX file.
		dec := ltx.NewDecoder(&buf)

		var out bytes.Buffer
		if err := dec.DecodeDatabaseTo(&out); err != nil {
			t.Fatal(err)
		} else if got, want := out.Bytes(), want.Bytes(); !bytes.Equal(got, want) {
			t.Fatal("output mismatch")
		}
	})

	t.Run("ErrNonSnapshot", func(t *testing.T) {
		spec := &ltx.FileSpec{
			Header: ltx.Header{Version: ltx.Version, Flags: 0, PageSize: 512, Commit: 2, MinTXID: 2, MaxTXID: 2, Timestamp: 1000, PreApplyChecksum: ltx.ChecksumFlag | 1},
			Pages: []ltx.PageSpec{
				{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte("3"), 512)},
			},
			Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 1},
		}

		// Decode serialized LTX file.
		var buf bytes.Buffer
		writeFileSpec(t, &buf, spec)
		dec := ltx.NewDecoder(&buf)
		if err := dec.DecodeDatabaseTo(io.Discard); err == nil || err.Error() != `cannot decode non-snapshot LTX file to SQLite database` {
			t.Fatal(err)
		}
	})
}

// TestDecoder_SnapshotCompleteness validates that the decoder properly checks
// for snapshot completeness when closing. For snapshot LTX files (MinTXID=1),
// the decoder must verify that all pages from 1 to Commit have been read,
// excluding the lock page which is always zero and never written.
func TestDecoder_SnapshotCompleteness(t *testing.T) {
	t.Run("CompleteSnapshot", func(t *testing.T) {
		// PURPOSE: Verify that a complete snapshot with all expected pages
		// passes validation when closing the decoder.
		//
		// TEST SETUP: Create a snapshot (MinTXID=1) with Commit=3, which means
		// we expect pages 1, 2, and 3 to be present in the file.
		spec := &ltx.FileSpec{
			Header: ltx.Header{
				Version:   2,
				Flags:     ltx.HeaderFlagNoChecksum,
				PageSize:  512,
				Commit:    3,
				MinTXID:   1,
				MaxTXID:   1,
				Timestamp: 1000,
			},
			Pages: []ltx.PageSpec{
				{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)},
				{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte("2"), 512)},
				{Header: ltx.PageHeader{Pgno: 3}, Data: bytes.Repeat([]byte("3"), 512)},
			},
			Trailer: ltx.Trailer{},
		}

		var buf bytes.Buffer
		writeFileSpec(t, &buf, spec)
		dec := ltx.NewDecoder(&buf)

		if err := dec.DecodeHeader(); err != nil {
			t.Fatal(err)
		}

		// Decode all pages in the file (pages 1, 2, and 3)
		for i := 0; i < 3; i++ {
			var hdr ltx.PageHeader
			data := make([]byte, 512)
			if err := dec.DecodePage(&hdr, data); err != nil {
				t.Fatal(err)
			}
		}

		// Read one more time to hit the zero page header (end marker).
		// This transitions the decoder to the "close" state, which is
		// required before calling Close().
		var hdr ltx.PageHeader
		data := make([]byte, 512)
		if err := dec.DecodePage(&hdr, data); err != io.EOF {
			t.Fatalf("expected EOF, got: %v", err)
		}

		// EXPECTED RESULT: Close() should succeed because we have read all
		// expected pages (1, 2, 3) matching the Commit value.
		if err := dec.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("IncompleteSnapshot", func(t *testing.T) {
		// PURPOSE: Verify that the decoder detects incomplete snapshots where
		// not all expected pages are present in the file.
		//
		// TEST SETUP: Create a snapshot with Commit=3 (expecting pages 1,2,3)
		// but only include pages 1 and 2 in the file.
		spec := &ltx.FileSpec{
			Header: ltx.Header{
				Version:   2,
				Flags:     ltx.HeaderFlagNoChecksum,
				PageSize:  512,
				Commit:    3,
				MinTXID:   1,
				MaxTXID:   1,
				Timestamp: 1000,
			},
			Pages: []ltx.PageSpec{
				{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("1"), 512)},
				{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte("2"), 512)},
				// Missing page 3
			},
			Trailer: ltx.Trailer{},
		}

		var buf bytes.Buffer
		writeFileSpec(t, &buf, spec)
		dec := ltx.NewDecoder(&buf)

		if err := dec.DecodeHeader(); err != nil {
			t.Fatal(err)
		}

		// Decode only the pages that are present (pages 1 and 2).
		// Note: Page 3 is missing from the file.
		for i := 0; i < 2; i++ {
			var hdr ltx.PageHeader
			data := make([]byte, 512)
			if err := dec.DecodePage(&hdr, data); err != nil {
				t.Fatal(err)
			}
		}

		// Read the end marker to transition to close state.
		// The decoder tracks that the last page read was page 2.
		var hdr ltx.PageHeader
		data := make([]byte, 512)
		if err := dec.DecodePage(&hdr, data); err != io.EOF {
			t.Fatalf("expected EOF, got: %v", err)
		}

		// EXPECTED RESULT: Close() should fail with a specific error indicating
		// that we expected to read up to page 3 (the Commit value) but only
		// read up to page 2.
		if err := dec.Close(); err == nil || err.Error() != "snapshot incomplete: expected last page 3, got 2" {
			t.Fatalf("expected snapshot incomplete error, got: %v", err)
		}
	})

	t.Run("SnapshotWithLockPage", func(t *testing.T) {
		// PURPOSE: Test the special case where the Commit value equals the lock page number.
		// SQLite never writes the lock page (it's always zero), so when Commit equals
		// the lock page, the last actual page we expect is Commit-1.
		//
		// BACKGROUND: The lock page number is calculated as 1073741824 / PageSize.
		// For a 4096 byte page size, this is page 262144.
		//
		// TEST SETUP: Create a snapshot with Commit=262144 (the lock page number)
		// and include all pages from 1 to 262143.
		lockPgno := ltx.LockPgno(4096)
		
		spec := &ltx.FileSpec{
			Header: ltx.Header{
				Version:   2,
				Flags:     ltx.HeaderFlagNoChecksum,
				PageSize:  4096,
				Commit:    lockPgno, // Commit is exactly the lock page
				MinTXID:   1,
				MaxTXID:   1,
				Timestamp: 1000,
			},
			Pages: []ltx.PageSpec{},
			Trailer: ltx.Trailer{},
		}

		// Add all pages from 1 to 262143 (lockPgno-1).
		// The lock page itself (262144) is never written to LTX files.
		for pgno := uint32(1); pgno < lockPgno; pgno++ {
			spec.Pages = append(spec.Pages, ltx.PageSpec{
				Header: ltx.PageHeader{Pgno: pgno},
				Data:   bytes.Repeat([]byte{byte(pgno % 256)}, 4096),
			})
		}

		var buf bytes.Buffer
		writeFileSpec(t, &buf, spec)
		dec := ltx.NewDecoder(&buf)

		if err := dec.DecodeHeader(); err != nil {
			t.Fatal(err)
		}

		// Decode all pages (1 through 262143).
		// This simulates reading a large database up to the lock page.
		for i := 0; i < len(spec.Pages); i++ {
			var hdr ltx.PageHeader
			data := make([]byte, 4096)
			if err := dec.DecodePage(&hdr, data); err != nil {
				t.Fatal(err)
			}
		}

		// Read the end marker to transition to close state.
		// The decoder will verify that the last page read (262143)
		// is correct for a snapshot with Commit=262144 (lock page).
		var hdr ltx.PageHeader
		data := make([]byte, 4096)
		if err := dec.DecodePage(&hdr, data); err != io.EOF {
			t.Fatalf("expected EOF, got: %v", err)
		}

		// EXPECTED RESULT: Close() should succeed because we've read all pages
		// up to lockPgno-1 (262143), which is correct when Commit equals the
		// lock page number.
		if err := dec.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("NonSnapshot", func(t *testing.T) {
		// PURPOSE: Verify that the snapshot completeness validation is NOT applied
		// to non-snapshot LTX files (MinTXID > 1).
		//
		// BACKGROUND: Non-snapshot files contain incremental changes and may only
		// include specific pages that changed, not all pages in the database.
		//
		// TEST SETUP: Create a non-snapshot file (MinTXID=2) that only contains
		// page 2, even though Commit=3.
		spec := &ltx.FileSpec{
			Header: ltx.Header{
				Version:          2,
				Flags:            0,
				PageSize:         512,
				Commit:           3,
				MinTXID:          2,
				MaxTXID:          2,
				Timestamp:        1000,
				PreApplyChecksum: ltx.ChecksumFlag | 1,
			},
			Pages: []ltx.PageSpec{
				{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte("2"), 512)},
				// Only page 2, not a complete set
			},
			Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 2},
		}

		var buf bytes.Buffer
		writeFileSpec(t, &buf, spec)
		dec := ltx.NewDecoder(&buf)

		if err := dec.DecodeHeader(); err != nil {
			t.Fatal(err)
		}

		// Decode the single page (page 2) that's in the file.
		// For non-snapshots, it's normal to have sparse pages.
		var hdr ltx.PageHeader
		data := make([]byte, 512)
		if err := dec.DecodePage(&hdr, data); err != nil {
			t.Fatal(err)
		}

		// Read the end marker to transition to close state.
		// No validation will occur because this is not a snapshot.
		if err := dec.DecodePage(&hdr, data); err != io.EOF {
			t.Fatalf("expected EOF, got: %v", err)
		}

		// EXPECTED RESULT: Close() should succeed even though we only have page 2
		// and not pages 1 and 3, because the completeness check only applies to
		// snapshot files (MinTXID=1).
		if err := dec.Close(); err != nil {
			t.Fatal(err)
		}
	})
}
