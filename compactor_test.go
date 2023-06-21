package ltx_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/superfly/ltx"
)

func TestCompactor_Compact(t *testing.T) {
	t.Run("SingleFilePageDataOnly", func(t *testing.T) {
		input := &ltx.FileSpec{
			Header: ltx.Header{
				Version:          1,
				PageSize:         512,
				Commit:           1,
				MinTXID:          1,
				MaxTXID:          1,
				Timestamp:        1000,
				PreApplyChecksum: 0,
			},
			Pages: []ltx.PageSpec{
				{
					Header: ltx.PageHeader{Pgno: 1},
					Data:   bytes.Repeat([]byte("1"), 512),
				},
			},
			Trailer: ltx.Trailer{
				PostApplyChecksum: ltx.ChecksumFlag | 1,
				FileChecksum:      0x897cc5d024cd382a,
			},
		}
		var buf0 bytes.Buffer
		writeFileSpec(t, &buf0, input)

		var output bytes.Buffer
		c := ltx.NewCompactor(&output, []io.Reader{&buf0})
		if err := c.Compact(context.Background()); err != nil {
			t.Fatal(err)
		}

		spec := readFileSpec(t, &output, int64(output.Len()))
		assertFileSpecEqual(t, spec, input)
		// assertFileSpecChecksum(t, spec, input) // output should be exact copy

		// Ensure header/trailer available.
		if got, want := c.Header(), input.Header; got != want {
			t.Fatalf("Header()=%#v, want %#v", got, want)
		}
		if got, want := c.Trailer(), input.Trailer; got != want {
			t.Fatalf("Trailer()=%#v, want %#v", got, want)
		}
	})

	t.Run("SnapshotPageDataOnly", func(t *testing.T) {
		spec, err := compactFileSpecs(t,
			&ltx.FileSpec{
				Header: ltx.Header{
					Version:   1,
					PageSize:  1024,
					Commit:    3,
					MinTXID:   1,
					MaxTXID:   1,
					Timestamp: 1000,
				},
				Pages: []ltx.PageSpec{
					{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{0x81}, 1024)},
					{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte{0x82}, 1024)},
					{Header: ltx.PageHeader{Pgno: 3}, Data: bytes.Repeat([]byte{0x83}, 1024)},
				},
				Trailer: ltx.Trailer{
					PostApplyChecksum: ltx.ChecksumFlag | 1,
				},
			},
			&ltx.FileSpec{
				Header: ltx.Header{
					Version:          1,
					PageSize:         1024,
					Commit:           3,
					MinTXID:          2,
					MaxTXID:          2,
					Timestamp:        2000,
					PreApplyChecksum: ltx.ChecksumFlag | 2,
				},
				Pages: []ltx.PageSpec{
					{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{0x91}, 1024)},
					{Header: ltx.PageHeader{Pgno: 3}, Data: bytes.Repeat([]byte{0x93}, 1024)},
				},
				Trailer: ltx.Trailer{
					PostApplyChecksum: ltx.ChecksumFlag | 2,
				},
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		assertFileSpecEqual(t, spec, &ltx.FileSpec{
			Header: ltx.Header{
				Version:   1,
				PageSize:  1024,
				Commit:    3,
				MinTXID:   1,
				MaxTXID:   2,
				Timestamp: 2000,
			},
			Pages: []ltx.PageSpec{
				{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{0x91}, 1024)},
				{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte{0x82}, 1024)},
				{Header: ltx.PageHeader{Pgno: 3}, Data: bytes.Repeat([]byte{0x93}, 1024)},
			},
			Trailer: ltx.Trailer{
				PostApplyChecksum: ltx.ChecksumFlag | 2,
				FileChecksum:      0xc7387b8aaccc8d35,
			},
		})
	})
	t.Run("NonSnapshotPageDataOnly", func(t *testing.T) {
		spec, err := compactFileSpecs(t,
			&ltx.FileSpec{
				Header: ltx.Header{
					Version:          1,
					PageSize:         1024,
					Commit:           3,
					MinTXID:          2,
					MaxTXID:          3,
					Timestamp:        1000,
					PreApplyChecksum: ltx.ChecksumFlag | 2,
				},
				Pages: []ltx.PageSpec{
					{Header: ltx.PageHeader{Pgno: 3}, Data: bytes.Repeat([]byte{0x83}, 1024)},
				},
				Trailer: ltx.Trailer{
					PostApplyChecksum: ltx.ChecksumFlag | 3,
				},
			},
			&ltx.FileSpec{
				Header: ltx.Header{Version: 1,
					PageSize:         1024,
					Commit:           3,
					MinTXID:          4,
					MaxTXID:          5,
					Timestamp:        2000,
					PreApplyChecksum: ltx.ChecksumFlag | 4,
				},
				Pages: []ltx.PageSpec{
					{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{0x91}, 1024)},
				},
				Trailer: ltx.Trailer{
					PostApplyChecksum: ltx.ChecksumFlag | 5,
				},
			},
			&ltx.FileSpec{
				Header: ltx.Header{
					Version:          1,
					PageSize:         1024,
					Commit:           5,
					MinTXID:          6,
					MaxTXID:          9,
					Timestamp:        3000,
					PreApplyChecksum: ltx.ChecksumFlag | 6,
				},
				Pages: []ltx.PageSpec{
					{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte{0xa2}, 1024)},
					{Header: ltx.PageHeader{Pgno: 3}, Data: bytes.Repeat([]byte{0xa3}, 1024)},
					{Header: ltx.PageHeader{Pgno: 5}, Data: bytes.Repeat([]byte{0xa5}, 1024)},
				},
				Trailer: ltx.Trailer{
					PostApplyChecksum: ltx.ChecksumFlag | 9,
				},
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		assertFileSpecEqual(t, spec, &ltx.FileSpec{
			Header: ltx.Header{
				Version:          1,
				PageSize:         1024,
				Commit:           5,
				MinTXID:          2,
				MaxTXID:          9,
				Timestamp:        3000,
				PreApplyChecksum: ltx.ChecksumFlag | 2,
			},
			Pages: []ltx.PageSpec{
				{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{0x91}, 1024)},
				{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte{0xa2}, 1024)},
				{Header: ltx.PageHeader{Pgno: 3}, Data: bytes.Repeat([]byte{0xa3}, 1024)},
				{Header: ltx.PageHeader{Pgno: 5}, Data: bytes.Repeat([]byte{0xa5}, 1024)},
			},
			Trailer: ltx.Trailer{
				PostApplyChecksum: ltx.ChecksumFlag | 9,
				FileChecksum:      0xead633959f3c67a8,
			},
		})
	})

	t.Run("Shrinking", func(t *testing.T) {
		spec, err := compactFileSpecs(t,
			&ltx.FileSpec{
				Header: ltx.Header{Version: 1, PageSize: 1024, Commit: 3, MinTXID: 2, MaxTXID: 3, Timestamp: 1000, PreApplyChecksum: ltx.ChecksumFlag | 2},
				Pages: []ltx.PageSpec{
					{Header: ltx.PageHeader{Pgno: 3}, Data: bytes.Repeat([]byte{0x83}, 1024)},
				},
				Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 3},
			},
			&ltx.FileSpec{
				Header: ltx.Header{Version: 1, PageSize: 1024, Commit: 2, MinTXID: 4, MaxTXID: 5, Timestamp: 2000, PreApplyChecksum: ltx.ChecksumFlag | 4},
				Pages: []ltx.PageSpec{
					{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{0x91}, 1024)},
				},
				Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 5},
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		assertFileSpecEqual(t, spec, &ltx.FileSpec{
			Header: ltx.Header{
				Version:          1,
				PageSize:         1024,
				Commit:           2,
				MinTXID:          2,
				MaxTXID:          5,
				Timestamp:        2000,
				PreApplyChecksum: ltx.ChecksumFlag | 2,
			},
			Pages: []ltx.PageSpec{
				{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{0x91}, 1024)},
			},
			Trailer: ltx.Trailer{
				PostApplyChecksum: ltx.ChecksumFlag | 5,
				FileChecksum:      0xf688132c3904f118,
			},
		})
	})

	t.Run("ErrInputReaderRequired", func(t *testing.T) {
		c := ltx.NewCompactor(&bytes.Buffer{}, nil)
		if err := c.Compact(context.Background()); err == nil || err.Error() != `at least one input reader required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrPageSizeMismatch", func(t *testing.T) {
		_, err := compactFileSpecs(t,
			&ltx.FileSpec{
				Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1, Timestamp: 1000},
				Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{0x81}, 512)}},
				Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 1},
			},
			&ltx.FileSpec{
				Header:  ltx.Header{Version: 1, PageSize: 1024, Commit: 1, MinTXID: 1, MaxTXID: 1, Timestamp: 1000},
				Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{0x91}, 1024)}},
				Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 1},
			},
		)
		if err == nil || err.Error() != `input files have mismatched page sizes: 512 != 1024` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrNonContiguousTXID", func(t *testing.T) {
		_, err := compactFileSpecs(t,
			&ltx.FileSpec{
				Header:  ltx.Header{Version: 1, PageSize: 1024, Commit: 1, MinTXID: 1, MaxTXID: 2, Timestamp: 1000},
				Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{0x81}, 1024)}},
				Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 1},
			},
			&ltx.FileSpec{
				Header:  ltx.Header{Version: 1, PageSize: 1024, Commit: 1, MinTXID: 4, MaxTXID: 4, Timestamp: 1000, PreApplyChecksum: ltx.ChecksumFlag | 2},
				Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{0x91}, 1024)}},
				Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 1},
			},
		)
		if err == nil || err.Error() != `non-contiguous transaction ids in input files: (0000000000000001,0000000000000002) -> (0000000000000004,0000000000000004)` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}
