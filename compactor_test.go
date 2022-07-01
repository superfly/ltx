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
				Version:             1,
				PageSize:            512,
				PageN:               1,
				Commit:              1,
				DBID:                1,
				MinTXID:             1,
				MaxTXID:             1,
				Timestamp:           1000,
				PreChecksum:         0,
				PostChecksum:        ltx.ChecksumFlag | 1,
				HeaderBlockChecksum: 0x90f21f4960b7564f,
				PageBlockChecksum:   0x966605a36b05fc7c,
				HeaderChecksum:      0xebacd45d7115aff8,
			},
			PageHeaders: []ltx.PageHeader{
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
				Header: ltx.Header{
					Version:      1,
					PageSize:     1024,
					PageN:        3,
					Commit:       3,
					DBID:         1,
					MinTXID:      1,
					MaxTXID:      1,
					Timestamp:    1000,
					PostChecksum: ltx.ChecksumFlag | 1,
				},
				PageHeaders: []ltx.PageHeader{
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
				Header: ltx.Header{
					Version:      1,
					PageSize:     1024,
					PageN:        2,
					Commit:       3,
					DBID:         1,
					MinTXID:      2,
					MaxTXID:      2,
					Timestamp:    2000,
					PreChecksum:  ltx.ChecksumFlag | 2,
					PostChecksum: ltx.ChecksumFlag | 2,
				},
				PageHeaders: []ltx.PageHeader{
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
			Header: ltx.Header{
				Version:             1,
				PageSize:            1024,
				PageN:               3,
				Commit:              3,
				DBID:                1,
				MinTXID:             1,
				MaxTXID:             2,
				Timestamp:           1000,
				PostChecksum:        ltx.ChecksumFlag | 2,
				HeaderBlockChecksum: 0xb50e9d3a90f21f49,
				PageBlockChecksum:   0x966605a36b05fc7c,
				HeaderChecksum:      0x9181fcbe2b18ae3d,
			},
			PageHeaders: []ltx.PageHeader{
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
				Header: ltx.Header{
					Version:      1,
					PageSize:     1024,
					PageN:        1,
					Commit:       3,
					DBID:         1,
					MinTXID:      2,
					MaxTXID:      3,
					Timestamp:    1000,
					PreChecksum:  ltx.ChecksumFlag | 2,
					PostChecksum: ltx.ChecksumFlag | 3,
				},
				PageHeaders: []ltx.PageHeader{
					{Pgno: 3},
				},
				PageData: [][]byte{
					bytes.Repeat([]byte{0x83}, 1024),
				},
			},
			&ltx.FileSpec{
				Header: ltx.Header{Version: 1,
					PageSize:     1024,
					PageN:        1,
					Commit:       3,
					DBID:         1,
					MinTXID:      4,
					MaxTXID:      5,
					Timestamp:    2000,
					PreChecksum:  ltx.ChecksumFlag | 4,
					PostChecksum: ltx.ChecksumFlag | 5,
				},
				PageHeaders: []ltx.PageHeader{
					{Pgno: 1},
				},
				PageData: [][]byte{
					bytes.Repeat([]byte{0x91}, 1024),
				},
			},
			&ltx.FileSpec{
				Header: ltx.Header{Version: 1, PageSize: 1024, PageN: 3, Commit: 5, DBID: 1, MinTXID: 6, MaxTXID: 9, Timestamp: 3000, PreChecksum: ltx.ChecksumFlag | 6, PostChecksum: ltx.ChecksumFlag | 9},
				PageHeaders: []ltx.PageHeader{
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
			Header: ltx.Header{
				Version:             1,
				PageSize:            1024,
				PageN:               4,
				Commit:              5,
				DBID:                1,
				MinTXID:             2,
				MaxTXID:             9,
				Timestamp:           1000,
				PreChecksum:         ltx.ChecksumFlag | 2,
				PostChecksum:        ltx.ChecksumFlag | 9,
				HeaderBlockChecksum: 0xc0426093e3a6fc4d,
				PageBlockChecksum:   0xd66f0548adf3c72a,
				HeaderChecksum:      0xeefebce0b319b5fa,
			},
			PageHeaders: []ltx.PageHeader{
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
				PageN:         1,
				EventN:        1,
				EventDataSize: 60,
				Commit:        1,
				DBID:          1,
				MinTXID:       1,
				MaxTXID:       1,
				Timestamp:     1000,
				PostChecksum:  ltx.ChecksumFlag | 1,
			},
			PageHeaders: []ltx.PageHeader{{Pgno: 1,
				Nonce: [12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7},
				Tag:   [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8},
			}},
			PageData: [][]byte{
				bytes.Repeat([]byte{0x81}, 512),
			},
			EventHeaders: []ltx.EventHeader{{
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
				Header: ltx.Header{
					Version:       1,
					PageSize:      1024,
					PageN:         1,
					EventN:        2,
					EventDataSize: 130,
					Commit:        1,
					DBID:          1,
					MinTXID:       1,
					MaxTXID:       1,
					Timestamp:     1000,
					PostChecksum:  ltx.ChecksumFlag | 1,
				},
				PageHeaders: []ltx.PageHeader{
					{Pgno: 1},
				},
				PageData: [][]byte{
					bytes.Repeat([]byte{0x81}, 1024),
				},
				EventHeaders: []ltx.EventHeader{
					{Size: 60},
					{Size: 70},
				},
				EventData: [][]byte{
					bytes.Repeat([]byte{0x10}, 60),
					bytes.Repeat([]byte{0x20}, 70),
				},
			},
			&ltx.FileSpec{
				Header: ltx.Header{
					Version:      1,
					PageSize:     1024,
					PageN:        1,
					Commit:       1,
					DBID:         1,
					MinTXID:      2,
					MaxTXID:      2,
					Timestamp:    2000,
					PreChecksum:  ltx.ChecksumFlag | 2,
					PostChecksum: ltx.ChecksumFlag | 2,
				},
				PageHeaders: []ltx.PageHeader{
					{Pgno: 1},
				},
				PageData: [][]byte{
					bytes.Repeat([]byte{0x91}, 1024),
				},
			},
			&ltx.FileSpec{
				Header: ltx.Header{
					Version:       1,
					PageSize:      1024,
					PageN:         1,
					EventN:        1,
					EventDataSize: 80,
					Commit:        1,
					DBID:          1,
					MinTXID:       3,
					MaxTXID:       3,
					Timestamp:     3000,
					PreChecksum:   ltx.ChecksumFlag | 3,
					PostChecksum:  ltx.ChecksumFlag | 3,
				},
				PageHeaders: []ltx.PageHeader{
					{Pgno: 1},
				},
				PageData: [][]byte{
					bytes.Repeat([]byte{0xa1}, 1024),
				},
				EventHeaders: []ltx.EventHeader{
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
			Header: ltx.Header{
				Version:             1,
				PageSize:            1024,
				PageN:               1,
				EventN:              3,
				EventDataSize:       210,
				Commit:              1,
				DBID:                1,
				MinTXID:             1,
				MaxTXID:             3,
				Timestamp:           1000,
				PostChecksum:        ltx.ChecksumFlag | 3,
				HeaderBlockChecksum: 0xabbaf9c03bf0613f,
				PageBlockChecksum:   0xea0a288a8aaaaaaa,
				HeaderChecksum:      0xbbdc601fe5c0b04a,
			},
			PageHeaders: []ltx.PageHeader{
				{Pgno: 1},
			},
			PageData: [][]byte{
				bytes.Repeat([]byte{0xa1}, 1024),
			},
			EventHeaders: []ltx.EventHeader{
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
				Header:      ltx.Header{Version: 1, PageSize: 1024, PageN: 1, Commit: 1, DBID: 1, MinTXID: 1, MaxTXID: 1, Timestamp: 1000, PostChecksum: ltx.ChecksumFlag | 1},
				PageHeaders: []ltx.PageHeader{{Pgno: 1}},
				PageData:    [][]byte{bytes.Repeat([]byte{0x81}, 1024)},
			},
			&ltx.FileSpec{
				Header:      ltx.Header{Version: 1, PageSize: 1024, PageN: 1, Commit: 1, DBID: 2, MinTXID: 1, MaxTXID: 1, Timestamp: 1000, PostChecksum: ltx.ChecksumFlag | 1},
				PageHeaders: []ltx.PageHeader{{Pgno: 1}},
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
				Header:      ltx.Header{Version: 1, PageSize: 512, PageN: 1, Commit: 1, DBID: 1, MinTXID: 1, MaxTXID: 1, Timestamp: 1000, PostChecksum: ltx.ChecksumFlag | 1},
				PageHeaders: []ltx.PageHeader{{Pgno: 1}},
				PageData:    [][]byte{bytes.Repeat([]byte{0x81}, 512)},
			},
			&ltx.FileSpec{
				Header:      ltx.Header{Version: 1, PageSize: 1024, PageN: 1, Commit: 1, DBID: 1, MinTXID: 1, MaxTXID: 1, Timestamp: 1000, PostChecksum: ltx.ChecksumFlag | 1},
				PageHeaders: []ltx.PageHeader{{Pgno: 1}},
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
				Header:      ltx.Header{Version: 1, PageSize: 1024, PageN: 1, Commit: 1, DBID: 1, MinTXID: 1, MaxTXID: 2, Timestamp: 1000, PostChecksum: ltx.ChecksumFlag | 2},
				PageHeaders: []ltx.PageHeader{{Pgno: 1}},
				PageData:    [][]byte{bytes.Repeat([]byte{0x81}, 1024)},
			},
			&ltx.FileSpec{
				Header:      ltx.Header{Version: 1, PageSize: 1024, PageN: 1, Commit: 1, DBID: 1, MinTXID: 2, MaxTXID: 2, Timestamp: 1000, PreChecksum: ltx.ChecksumFlag | 2, PostChecksum: ltx.ChecksumFlag | 2},
				PageHeaders: []ltx.PageHeader{{Pgno: 1}},
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
			Header:      ltx.Header{Version: 1, PageSize: 1024, PageN: 1, Commit: 1, DBID: 1, MinTXID: 1, MaxTXID: 2, Timestamp: 1000, PostChecksum: ltx.ChecksumFlag | 2},
			PageHeaders: []ltx.PageHeader{{Pgno: 1}},
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
