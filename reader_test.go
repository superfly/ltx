package ltx_test

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/superfly/ltx"
)

func TestReader(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		spec := &ltx.FileSpec{
			Header: ltx.Header{
				Version:   1,
				PageSize:  1024,
				Commit:    2,
				DBID:      1,
				MinTXID:   1,
				MaxTXID:   1,
				Timestamp: 1000,
			},
			Pages: []ltx.PageSpec{
				{
					Header: ltx.PageHeader{Pgno: 1},
					Data:   bytes.Repeat([]byte("2"), 1024),
				},
				{
					Header: ltx.PageHeader{Pgno: 2},
					Data:   bytes.Repeat([]byte("3"), 1024),
				},
			},
			Trailer: ltx.Trailer{
				PostApplyChecksum: ltx.ChecksumFlag | 1,
			},
		}

		// Write spec to file.
		var buf bytes.Buffer
		writeFileSpec(t, &buf, spec)

		// Read and verify data matches spec.
		r := ltx.NewReader(&buf)

		// Verify header.
		if err := r.ReadHeader(); err != nil {
			t.Fatal(err)
		} else if got, want := r.Header(), spec.Header; !reflect.DeepEqual(got, want) {
			t.Fatalf("header mismatch:\ngot=%#v\nwant=%#v", got, want)
		}

		// Verify page headers.
		for i := range spec.Pages {
			var hdr ltx.PageHeader
			data := make([]byte, 1024)
			if err := r.ReadPage(&hdr, data); err != nil {
				t.Fatal(err)
			} else if got, want := hdr, spec.Pages[i].Header; got != want {
				t.Fatalf("page hdr mismatch:\ngot=%#v\nwant=%#v", got, want)
			} else if got, want := data, spec.Pages[i].Data; !bytes.Equal(got, want) {
				t.Fatalf("page data mismatch:\ngot=%#v\nwant=%#v", got, want)
			}
		}

		if err := r.ReadPage(&ltx.PageHeader{}, make([]byte, 1024)); err != io.EOF {
			t.Fatal("expected page header eof")
		}

		// Close reader to verify integrity.
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	})
}
