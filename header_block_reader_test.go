package ltx_test

import (
	"bytes"
	"io"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/superfly/ltx"
)

func TestHeaderBlockReader(t *testing.T) {
	t.Run("EventAndPageData", func(t *testing.T) {
		spec := &ltx.FileSpec{
			Header: ltx.Header{
				Version:       1,
				PageSize:      1024,
				EventN:        1,
				PageN:         2,
				Commit:        2,
				EventDataSize: 60,
				DBID:          1,
				MinTXID:       1,
				MaxTXID:       1,
				Timestamp:     1000,
			},
			EventHeaders: []ltx.EventHeader{
				{
					Size:  60,
					Nonce: [12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11},
					Tag:   [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12},
				},
			},
			PageHeaders: []ltx.PageHeader{
				ltx.PageHeader{
					Pgno:  1,
					Nonce: [12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7},
					Tag:   [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8},
				},
				ltx.PageHeader{
					Pgno:  2,
					Nonce: [12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9},
					Tag:   [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10},
				},
			},
			EventData: [][]byte{
				bytes.Repeat([]byte("1"), 60),
			},
			PageData: [][]byte{
				bytes.Repeat([]byte("2"), 1024),
				bytes.Repeat([]byte("3"), 1024),
			},
		}

		// Write spec to file.
		filename := filepath.Join(t.TempDir(), "ltx")
		writeFileSpec(t, filename, spec)

		// Read and verify data matches spec.
		r := ltx.NewHeaderBlockReader(openFile(t, filename))

		// Verify header.
		var hdr ltx.Header
		if err := r.ReadHeader(&hdr); err != nil {
			t.Fatal(err)
		} else if got, want := hdr, spec.Header; !reflect.DeepEqual(got, want) {
			t.Fatalf("header mismatch:\ngot=%#v\nwant=%#v", got, want)
		}

		// Verify page headers.
		for i := range spec.PageHeaders {
			var hdr ltx.PageHeader
			if err := r.ReadPageHeader(&hdr); err != nil {
				t.Fatal(err)
			} else if got, want := hdr, spec.PageHeaders[i]; got != want {
				t.Fatalf("page hdr mismatch:\ngot=%#v\nwant=%#v", got, want)
			}
		}

		// Verify event data.
		for i := range spec.EventHeaders {
			var hdr ltx.EventHeader
			if err := r.ReadEventHeader(&hdr); err != nil {
				t.Fatal(err)
			} else if got, want := hdr, spec.EventHeaders[i]; got != want {
				t.Fatalf("event hdr mismatch:\ngot=%#v\nwant=%#v", got, want)
			}

			if data, err := io.ReadAll(r); err != nil {
				t.Fatal(err)
			} else if !bytes.Equal(data, spec.EventData[i]) {
				t.Fatalf("event data mismatch: index=%d", i)
			}
		}

		// Close reader to verify integrity.
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	})
}
