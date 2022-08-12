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

func TestVerifyingReader(t *testing.T) {
	// Build a simple LTX file that most tests can use.
	var srcBuf bytes.Buffer
	writeFileSpec(t, &srcBuf, &ltx.FileSpec{
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
				Data:   bytes.Repeat([]byte("1"), 1024),
			},
			{
				Header: ltx.PageHeader{Pgno: 2},
				Data:   bytes.Repeat([]byte("2"), 1024),
			},
		},
		Trailer: ltx.Trailer{
			PostApplyChecksum: ltx.ChecksumFlag | 1,
		},
	})
	src := srcBuf.Bytes()

	t.Run("OK", func(t *testing.T) {
		var dst bytes.Buffer
		if n, err := io.Copy(&dst, ltx.NewVerifyingReader(bytes.NewReader(src))); err != nil {
			t.Fatal(err)
		} else if got, want := int(n), len(src); got != want {
			t.Fatalf("n=%d, want %d", got, want)
		} else if got, want := dst.Len(), len(src); got != want {
			t.Fatalf("dst.Len()=%d, want %d", got, want)
		}
	})

	t.Run("Header", func(t *testing.T) {
		t.Run("ErrShortBuffer", func(t *testing.T) {
			buf := make([]byte, 1028)
			r := ltx.NewVerifyingReader(bytes.NewReader(src))
			if _, err := r.Read(buf[:10]); err != io.ErrShortBuffer {
				t.Fatalf("unexpected error: %s", err)
			}
		})
		t.Run("ErrUnexpectedEOF", func(t *testing.T) {
			if _, err := io.Copy(io.Discard, ltx.NewVerifyingReader(bytes.NewReader(src[:10]))); err != io.ErrUnexpectedEOF {
				t.Fatalf("unexpected error: %s", err)
			}
		})
		t.Run("ErrInvalidHeader", func(t *testing.T) {
			r := ltx.NewVerifyingReader(
				io.MultiReader(
					bytes.NewReader(make([]byte, 4)),
					bytes.NewReader(src[4:])),
			)
			if _, err := io.Copy(io.Discard, r); err == nil || err.Error() != `unmarshal header: invalid LTX file` {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	})

	t.Run("Page", func(t *testing.T) {
		t.Run("ErrShortBuffer", func(t *testing.T) {
			buf := make([]byte, 1028)
			r := ltx.NewVerifyingReader(bytes.NewReader(src))
			if _, err := r.Read(buf[:ltx.HeaderSize]); err != nil {
				t.Fatal(err)
			} else if _, err := r.Read(buf[:10]); err != io.ErrShortBuffer {
				t.Fatalf("unexpected error: %s", err)
			}
		})
		t.Run("ErrUnexpectedEOF/Header", func(t *testing.T) {
			if _, err := io.Copy(io.Discard, ltx.NewVerifyingReader(bytes.NewReader(src[:ltx.HeaderSize+1]))); err != io.ErrUnexpectedEOF {
				t.Fatalf("unexpected error: %s", err)
			}
		})
		t.Run("ErrUnexpectedEOF/Data", func(t *testing.T) {
			if _, err := io.Copy(io.Discard, ltx.NewVerifyingReader(bytes.NewReader(src[:ltx.HeaderSize+ltx.PageHeaderSize+1]))); err != io.ErrUnexpectedEOF {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	})

	t.Run("Trailer", func(t *testing.T) {
		t.Run("ErrShortBuffer", func(t *testing.T) {
			buf := make([]byte, 1028)
			r := ltx.NewVerifyingReader(bytes.NewReader(src))
			if _, err := r.Read(buf); err != nil {
				t.Fatal(err)
			} else if _, err := r.Read(buf); err != nil { // page 1
				t.Fatal(err)
			} else if _, err := r.Read(buf); err != nil { // page 2
				t.Fatal(err)
			} else if _, err := r.Read(buf); err != nil { // end of page block
				t.Fatal(err)
			} else if _, err := r.Read(buf[:1]); err != io.ErrShortBuffer {
				t.Fatalf("unexpected error: %s", err)
			}
		})
		t.Run("ErrUnexpectedEOF", func(t *testing.T) {
			if _, err := io.Copy(io.Discard, ltx.NewVerifyingReader(bytes.NewReader(src[:len(src)-1]))); err != io.ErrUnexpectedEOF {
				t.Fatalf("unexpected error: %s", err)
			}
		})
		t.Run("ErrChecksumMismatch", func(t *testing.T) {
			other := make([]byte, len(src))
			copy(other, src)
			other[len(other)-1] = 0
			if _, err := io.Copy(io.Discard, ltx.NewVerifyingReader(bytes.NewReader(other))); err != ltx.ErrChecksumMismatch {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	})
}
