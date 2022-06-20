package ltx_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/superfly/ltx"
)

func TestCompactor_Compact(t *testing.T) {
	t.Run("SingleFilePageDataOnly", func(t *testing.T) {
		dir := t.TempDir()
		input := &ltx.FileSpec{
			Header: ltx.Header{
				Version:    1,
				PageSize:   512,
				PageFrameN: 1,
				Commit:     1,
				DBID:       1,
				MinTXID:    1,
				MaxTXID:    1,
				Timestamp:  1000,
			},
			PageHeaders: []ltx.PageFrameHeader{
				{
					Pgno:  1,
					Nonce: [12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7},
					Tag:   [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8},
				},
			},
			PageData: [][]byte{
				bytes.Repeat([]byte("1"), 512),
			},
		}
		writeFileSpec(t, filepath.Join(dir, "input0"), input)

		c := ltx.NewCompactor()
		if err := c.Compact(filepath.Join(dir, "output"), []string{filepath.Join(dir, "input0")}); err != nil {
			t.Fatal(err)
		}

		spec := readFileSpec(t, filepath.Join(dir, "output"))
		assertFileSpecEqual(t, spec, input)
		assertFileSpecChecksum(t, spec, input) // output should be exact copy
	})

	t.Run("SnapshotPageDataOnly", func(t *testing.T) {
		spec, err := compactFileSpecs(t, ltx.NewCompactor(),
			&ltx.FileSpec{
				Header: ltx.Header{Version: 1, PageSize: 1024, PageFrameN: 3, Commit: 3, DBID: 1, MinTXID: 1, MaxTXID: 1, Timestamp: 1000},
				PageHeaders: []ltx.PageFrameHeader{
					{Pgno: 1},
					{Pgno: 2},
					{Pgno: 3},
				},
				PageData: [][]byte{
					bytes.Repeat([]byte{0x81}, 1024),
					bytes.Repeat([]byte{0x82}, 1024),
					bytes.Repeat([]byte{0x83}, 1024),
				},
			},
			&ltx.FileSpec{
				Header: ltx.Header{Version: 1, PageSize: 1024, PageFrameN: 2, Commit: 3, DBID: 1, MinTXID: 2, MaxTXID: 2, Timestamp: 2000},
				PageHeaders: []ltx.PageFrameHeader{
					{Pgno: 1},
					{Pgno: 3},
				},
				PageData: [][]byte{
					bytes.Repeat([]byte{0x91}, 1024),
					bytes.Repeat([]byte{0x93}, 1024),
				},
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		assertFileSpecEqual(t, spec, &ltx.FileSpec{
			Header: ltx.Header{Version: 1, PageSize: 1024, PageFrameN: 3, Commit: 3, DBID: 1, MinTXID: 1, MaxTXID: 2, Timestamp: 1000},
			PageHeaders: []ltx.PageFrameHeader{
				{Pgno: 1},
				{Pgno: 2},
				{Pgno: 3},
			},
			PageData: [][]byte{
				bytes.Repeat([]byte{0x91}, 1024),
				bytes.Repeat([]byte{0x82}, 1024),
				bytes.Repeat([]byte{0x93}, 1024),
			},
		})
	})
	t.Run("NonSnapshotPageDataOnly", func(t *testing.T) {
		spec, err := compactFileSpecs(t, ltx.NewCompactor(),
			&ltx.FileSpec{
				Header: ltx.Header{Version: 1, PageSize: 1024, PageFrameN: 1, Commit: 3, DBID: 1, MinTXID: 2, MaxTXID: 3, Timestamp: 1000},
				PageHeaders: []ltx.PageFrameHeader{
					{Pgno: 3},
				},
				PageData: [][]byte{
					bytes.Repeat([]byte{0x83}, 1024),
				},
			},
			&ltx.FileSpec{
				Header: ltx.Header{Version: 1, PageSize: 1024, PageFrameN: 1, Commit: 3, DBID: 1, MinTXID: 4, MaxTXID: 5, Timestamp: 2000},
				PageHeaders: []ltx.PageFrameHeader{
					{Pgno: 1},
				},
				PageData: [][]byte{
					bytes.Repeat([]byte{0x91}, 1024),
				},
			},
			&ltx.FileSpec{
				Header: ltx.Header{Version: 1, PageSize: 1024, PageFrameN: 3, Commit: 5, DBID: 1, MinTXID: 6, MaxTXID: 9, Timestamp: 3000},
				PageHeaders: []ltx.PageFrameHeader{
					{Pgno: 2},
					{Pgno: 3},
					{Pgno: 5},
				},
				PageData: [][]byte{
					bytes.Repeat([]byte{0xa2}, 1024),
					bytes.Repeat([]byte{0xa3}, 1024),
					bytes.Repeat([]byte{0xa5}, 1024),
				},
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		assertFileSpecEqual(t, spec, &ltx.FileSpec{
			Header: ltx.Header{Version: 1, PageSize: 1024, PageFrameN: 4, Commit: 5, DBID: 1, MinTXID: 2, MaxTXID: 9, Timestamp: 1000},
			PageHeaders: []ltx.PageFrameHeader{
				{Pgno: 1},
				{Pgno: 2},
				{Pgno: 3},
				{Pgno: 5},
			},
			PageData: [][]byte{
				bytes.Repeat([]byte{0x91}, 1024),
				bytes.Repeat([]byte{0xa2}, 1024),
				bytes.Repeat([]byte{0xa3}, 1024),
				bytes.Repeat([]byte{0xa5}, 1024),
			},
		})
	})

	t.Run("SingleFileWithPageAndEventData", func(t *testing.T) {
		dir := t.TempDir()
		input := &ltx.FileSpec{
			Header: ltx.Header{
				Version:       1,
				PageSize:      512,
				PageFrameN:    1,
				EventFrameN:   1,
				EventDataSize: 60,
				Commit:        1,
				DBID:          1,
				MinTXID:       1,
				MaxTXID:       1,
				Timestamp:     1000,
			},
			PageHeaders: []ltx.PageFrameHeader{{Pgno: 1,
				Nonce: [12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7},
				Tag:   [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8},
			}},
			PageData: [][]byte{
				bytes.Repeat([]byte{0x81}, 512),
			},
			EventHeaders: []ltx.EventFrameHeader{{
				Size:  60,
				Nonce: [12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7},
				Tag:   [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8},
			}},
			EventData: [][]byte{
				bytes.Repeat([]byte{0x80}, 60),
			},
		}
		writeFileSpec(t, filepath.Join(dir, "input0"), input)

		c := ltx.NewCompactor()
		if err := c.Compact(filepath.Join(dir, "output"), []string{filepath.Join(dir, "input0")}); err != nil {
			t.Fatal(err)
		}

		spec := readFileSpec(t, filepath.Join(dir, "output"))
		assertFileSpecEqual(t, spec, input)
		assertFileSpecChecksum(t, spec, input) // output should be exact copy
	})

	t.Run("MultiFileWithPageAndEventData", func(t *testing.T) {
		spec, err := compactFileSpecs(t, ltx.NewCompactor(),
			&ltx.FileSpec{
				Header: ltx.Header{Version: 1, PageSize: 1024, PageFrameN: 1, EventFrameN: 2, EventDataSize: 130, Commit: 1, DBID: 1, MinTXID: 1, MaxTXID: 1, Timestamp: 1000},
				PageHeaders: []ltx.PageFrameHeader{
					{Pgno: 1},
				},
				PageData: [][]byte{
					bytes.Repeat([]byte{0x81}, 1024),
				},
				EventHeaders: []ltx.EventFrameHeader{
					{Size: 60},
					{Size: 70},
				},
				EventData: [][]byte{
					bytes.Repeat([]byte{0x10}, 60),
					bytes.Repeat([]byte{0x20}, 70),
				},
			},
			&ltx.FileSpec{
				Header: ltx.Header{Version: 1, PageSize: 1024, PageFrameN: 1, Commit: 1, DBID: 1, MinTXID: 2, MaxTXID: 2, Timestamp: 2000},
				PageHeaders: []ltx.PageFrameHeader{
					{Pgno: 1},
				},
				PageData: [][]byte{
					bytes.Repeat([]byte{0x91}, 1024),
				},
			},
			&ltx.FileSpec{
				Header: ltx.Header{Version: 1, PageSize: 1024, PageFrameN: 1, EventFrameN: 1, EventDataSize: 80, Commit: 1, DBID: 1, MinTXID: 3, MaxTXID: 3, Timestamp: 3000},
				PageHeaders: []ltx.PageFrameHeader{
					{Pgno: 1},
				},
				PageData: [][]byte{
					bytes.Repeat([]byte{0xa1}, 1024),
				},
				EventHeaders: []ltx.EventFrameHeader{
					{Size: 80},
				},
				EventData: [][]byte{
					bytes.Repeat([]byte{0x30}, 80),
				},
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		assertFileSpecEqual(t, spec, &ltx.FileSpec{
			Header: ltx.Header{Version: 1, PageSize: 1024, PageFrameN: 1, EventFrameN: 3, EventDataSize: 210, Commit: 1, DBID: 1, MinTXID: 1, MaxTXID: 3, Timestamp: 1000},
			PageHeaders: []ltx.PageFrameHeader{
				{Pgno: 1},
			},
			PageData: [][]byte{
				bytes.Repeat([]byte{0xa1}, 1024),
			},
			EventHeaders: []ltx.EventFrameHeader{
				{Size: 60},
				{Size: 70},
				{Size: 80},
			},
			EventData: [][]byte{
				bytes.Repeat([]byte{0x10}, 60),
				bytes.Repeat([]byte{0x20}, 70),
				bytes.Repeat([]byte{0x30}, 80),
			},
		})
	})

	t.Run("ErrOutputFileRequired", func(t *testing.T) {
		if err := ltx.NewCompactor().Compact("", nil); err == nil || err.Error() != `output filename required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrInputFileRequired", func(t *testing.T) {
		if err := ltx.NewCompactor().Compact("output", nil); err == nil || err.Error() != `at least one input file required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrInputFileNotFound", func(t *testing.T) {
		if err := ltx.NewCompactor().Compact("output", []string{filepath.Join(t.TempDir(), "no_such_file")}); !os.IsNotExist(err) {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrDBIDMismatch", func(t *testing.T) {
		_, err := compactFileSpecs(t, ltx.NewCompactor(),
			&ltx.FileSpec{
				Header:      ltx.Header{Version: 1, PageSize: 1024, PageFrameN: 1, Commit: 1, DBID: 1, MinTXID: 1, MaxTXID: 1, Timestamp: 1000},
				PageHeaders: []ltx.PageFrameHeader{{Pgno: 1}},
				PageData:    [][]byte{bytes.Repeat([]byte{0x81}, 1024)},
			},
			&ltx.FileSpec{
				Header:      ltx.Header{Version: 1, PageSize: 1024, PageFrameN: 1, Commit: 1, DBID: 2, MinTXID: 1, MaxTXID: 1, Timestamp: 1000},
				PageHeaders: []ltx.PageFrameHeader{{Pgno: 1}},
				PageData:    [][]byte{bytes.Repeat([]byte{0x91}, 1024)},
			},
		)
		if err == nil || err.Error() != `input files have mismatched database ids: 1 != 2` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrPageSizeMismatch", func(t *testing.T) {
		_, err := compactFileSpecs(t, ltx.NewCompactor(),
			&ltx.FileSpec{
				Header:      ltx.Header{Version: 1, PageSize: 512, PageFrameN: 1, Commit: 1, DBID: 1, MinTXID: 1, MaxTXID: 1, Timestamp: 1000},
				PageHeaders: []ltx.PageFrameHeader{{Pgno: 1}},
				PageData:    [][]byte{bytes.Repeat([]byte{0x81}, 512)},
			},
			&ltx.FileSpec{
				Header:      ltx.Header{Version: 1, PageSize: 1024, PageFrameN: 1, Commit: 1, DBID: 1, MinTXID: 1, MaxTXID: 1, Timestamp: 1000},
				PageHeaders: []ltx.PageFrameHeader{{Pgno: 1}},
				PageData:    [][]byte{bytes.Repeat([]byte{0x91}, 1024)},
			},
		)
		if err == nil || err.Error() != `input files have mismatched page sizes: 512 != 1024` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrNonContiguousTXID", func(t *testing.T) {
		_, err := compactFileSpecs(t, ltx.NewCompactor(),
			&ltx.FileSpec{
				Header:      ltx.Header{Version: 1, PageSize: 1024, PageFrameN: 1, Commit: 1, DBID: 1, MinTXID: 1, MaxTXID: 2, Timestamp: 1000},
				PageHeaders: []ltx.PageFrameHeader{{Pgno: 1}},
				PageData:    [][]byte{bytes.Repeat([]byte{0x81}, 1024)},
			},
			&ltx.FileSpec{
				Header:      ltx.Header{Version: 1, PageSize: 1024, PageFrameN: 1, Commit: 1, DBID: 1, MinTXID: 2, MaxTXID: 2, Timestamp: 1000},
				PageHeaders: []ltx.PageFrameHeader{{Pgno: 1}},
				PageData:    [][]byte{bytes.Repeat([]byte{0x91}, 1024)},
			},
		)
		if err == nil || err.Error() != `non-contiguous transaction ids in input files: 1-2,2` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrCannotCreateOutputFile", func(t *testing.T) {
		dir := t.TempDir()
		writeFileSpec(t, filepath.Join(dir, "input"), &ltx.FileSpec{
			Header:      ltx.Header{Version: 1, PageSize: 1024, PageFrameN: 1, Commit: 1, DBID: 1, MinTXID: 1, MaxTXID: 2, Timestamp: 1000},
			PageHeaders: []ltx.PageFrameHeader{{Pgno: 1}},
			PageData:    [][]byte{bytes.Repeat([]byte{0x81}, 1024)},
		})
		if err := ltx.NewCompactor().Compact(filepath.Join(dir, "parent", "output"), []string{filepath.Join(dir, "input")}); err == nil || !strings.Contains(err.Error(), `no such file or directory`) {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestCompactor_Compact_quick(t *testing.T) {
	t.Skip("TODO")
}
