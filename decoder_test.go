package ltx_test

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/superfly/ltx"
)

func TestDecoder(t *testing.T) {
	testDecoder(t, "OK", 0)
	testDecoder(t, "CompressLZ4", ltx.HeaderFlagCompressLZ4)
}

func testDecoder(t *testing.T, name string, flags uint32) {
	t.Run(name, func(t *testing.T) {
		spec := &ltx.FileSpec{
			Header: ltx.Header{
				Version:   1,
				Flags:     flags,
				PageSize:  1024,
				Commit:    2,
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
			t.Fatal("expected page header eof")
		}

		// Close reader to verify integrity.
		if err := dec.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestDecoder_DecodeDatabaseTo(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		spec := &ltx.FileSpec{
			Header: ltx.Header{Version: 1, Flags: 0, PageSize: 512, Commit: 2, MinTXID: 1, MaxTXID: 2, Timestamp: 1000},
			Pages: []ltx.PageSpec{
				{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte("2"), 512)},
				{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte("3"), 512)},
			},
			Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 1},
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

	t.Run("ErrNonSnapshot", func(t *testing.T) {
		spec := &ltx.FileSpec{
			Header: ltx.Header{Version: 1, Flags: 0, PageSize: 512, Commit: 2, MinTXID: 2, MaxTXID: 2, Timestamp: 1000, PreApplyChecksum: ltx.ChecksumFlag | 1},
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
