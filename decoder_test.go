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
