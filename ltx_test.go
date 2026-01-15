package ltx_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/superfly/ltx"
)

func TestNewPos(t *testing.T) {
	pos := ltx.NewPos(1000, 2000)
	if got, want := pos.TXID, ltx.TXID(1000); got != want {
		t.Fatalf("TXID=%s, want %s", got, want)
	}
	if got, want := pos.PostApplyChecksum, ltx.Checksum(2000); got != want {
		t.Fatalf("PostApplyChecksum=%v, want %v", got, want)
	}
}

func TestPos_String(t *testing.T) {
	pos := ltx.NewPos(1000, 2000)
	if got, want := pos.String(), "00000000000003e8/00000000000007d0"; got != want {
		t.Fatalf("Pos = %s, want = %s", got, want)
	}
}

func TestParsePos(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		if v, err := ltx.ParsePos("00000000000003e8/00000000000007d0"); err != nil {
			t.Fatal(err)
		} else if got, want := v, ltx.NewPos(1000, 2000); got != want {
			t.Fatalf("got=%d, want %d", got, want)
		}
	})
	t.Run("ErrTooShort", func(t *testing.T) {
		if _, err := ltx.ParsePos("00000000000003e8"); err == nil || err.Error() != `invalid formatted position length: "00000000000003e8"` {
			t.Fatal(err)
		}
	})
}

func TestHeader_Validate(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		hdr := ltx.Header{
			Version:          ltx.Version,
			PageSize:         1024,
			Commit:           2,
			MinTXID:          3,
			MaxTXID:          4,
			PreApplyChecksum: ltx.ChecksumFlag,
			WALSalt1:         5,
			WALSalt2:         6,
			WALOffset:        7,
			WALSize:          8,
		}
		if err := hdr.Validate(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("CommitZero", func(t *testing.T) {
		hdr := ltx.Header{
			Version:          ltx.Version,
			PageSize:         1024,
			Commit:           0,
			MinTXID:          5,
			MaxTXID:          5,
			PreApplyChecksum: ltx.ChecksumFlag,
		}
		if err := hdr.Validate(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("ErrVersion", func(t *testing.T) {
		hdr := ltx.Header{}
		if err := hdr.Validate(); err == nil || err.Error() != `invalid version` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrFlags", func(t *testing.T) {
		hdr := ltx.Header{Version: ltx.Version, Flags: 1 << 3}
		if err := hdr.Validate(); err == nil || err.Error() != `invalid flags: 0x00000008` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrInvalidPageSize", func(t *testing.T) {
		hdr := ltx.Header{Version: ltx.Version, PageSize: 1000}
		if err := hdr.Validate(); err == nil || err.Error() != `invalid page size: 1000` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrMinTXIDRequired", func(t *testing.T) {
		hdr := ltx.Header{Version: ltx.Version, PageSize: 1024, Commit: 2}
		if err := hdr.Validate(); err == nil || err.Error() != `minimum transaction id required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrMaxTXIDRequired", func(t *testing.T) {
		hdr := ltx.Header{Version: ltx.Version, PageSize: 1024, Commit: 2, MinTXID: 3}
		if err := hdr.Validate(); err == nil || err.Error() != `maximum transaction id required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrTXIDOutOfOrderRequired", func(t *testing.T) {
		hdr := ltx.Header{Version: ltx.Version, PageSize: 1024, Commit: 2, MinTXID: 3, MaxTXID: 2}
		if err := hdr.Validate(); err == nil || err.Error() != `transaction ids out of order: (3,2)` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrNegativeWALOffset", func(t *testing.T) {
		hdr := ltx.Header{Version: ltx.Version, PageSize: 1024, Commit: 2, MinTXID: 1, MaxTXID: 1, WALOffset: -1000}
		if err := hdr.Validate(); err == nil || err.Error() != `wal offset cannot be negative: -1000` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrNegativeWALSize", func(t *testing.T) {
		hdr := ltx.Header{Version: ltx.Version, PageSize: 1024, Commit: 2, MinTXID: 1, MaxTXID: 1, WALOffset: 32, WALSize: -1000}
		if err := hdr.Validate(); err == nil || err.Error() != `wal size cannot be negative: -1000` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrWALOffsetRequiredWithWALSalt", func(t *testing.T) {
		hdr := ltx.Header{Version: ltx.Version, PageSize: 1024, Commit: 2, MinTXID: 1, MaxTXID: 1, WALSalt1: 100}
		if err := hdr.Validate(); err == nil || err.Error() != `wal offset required if salt exists` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrWALOffsetRequiredWithWALSize", func(t *testing.T) {
		hdr := ltx.Header{Version: ltx.Version, PageSize: 1024, Commit: 2, MinTXID: 1, MaxTXID: 1, WALSize: 1000}
		if err := hdr.Validate(); err == nil || err.Error() != `wal offset required if wal size exists` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrSnapshotPreApplyChecksumNotAllowed", func(t *testing.T) {
		hdr := ltx.Header{Version: ltx.Version, PageSize: 1024, Commit: 4, MinTXID: 1, MaxTXID: 3, PreApplyChecksum: 1}
		if err := hdr.Validate(); err == nil || err.Error() != `pre-apply checksum must be zero on snapshots` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrNonSnapshotPreApplyChecksumRequired", func(t *testing.T) {
		hdr := ltx.Header{Version: ltx.Version, PageSize: 1024, Commit: 4, MinTXID: 2, MaxTXID: 3}
		if err := hdr.Validate(); err == nil || err.Error() != `pre-apply checksum required on non-snapshot files` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrInvalidPreApplyChecksumFormat", func(t *testing.T) {
		hdr := ltx.Header{Version: ltx.Version, PageSize: 1024, Commit: 4, MinTXID: 2, MaxTXID: 3, PreApplyChecksum: 1}
		if err := hdr.Validate(); err == nil || err.Error() != `invalid pre-apply checksum format` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestHeader_MarshalBinary(t *testing.T) {
	hdr := ltx.Header{
		Version:          ltx.Version,
		Flags:            0,
		PageSize:         1024,
		Commit:           1006,
		MinTXID:          1007,
		MaxTXID:          1008,
		Timestamp:        1009,
		PreApplyChecksum: 1011,
		WALSalt1:         1012,
		WALSalt2:         1013,
		WALOffset:        1014,
		WALSize:          1015,
	}

	var other ltx.Header
	if b, err := hdr.MarshalBinary(); err != nil {
		t.Fatal(err)
	} else if err := other.UnmarshalBinary(b); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(hdr, other) {
		t.Fatalf("mismatch:\ngot=%#v\nwant=%#v", hdr, other)
	}
}

func TestHeader_UnmarshalBinary(t *testing.T) {
	t.Run("ErrShortBuffer", func(t *testing.T) {
		var hdr ltx.Header
		if err := hdr.UnmarshalBinary(make([]byte, 10)); err != io.ErrShortBuffer {
			t.Fatal(err)
		}
	})
	t.Run("ErrInvalidFile", func(t *testing.T) {
		var hdr ltx.Header
		if err := hdr.UnmarshalBinary(make([]byte, ltx.HeaderSize)); err != ltx.ErrInvalidFile {
			t.Fatal(err)
		}
	})
}

func TestPeekHeader(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		hdr := ltx.Header{
			Version:          ltx.Version,
			Flags:            0,
			PageSize:         1024,
			Commit:           1006,
			MinTXID:          1007,
			MaxTXID:          1008,
			Timestamp:        1009,
			PreApplyChecksum: 1011,
			WALSalt1:         1012,
			WALSalt2:         1013,
			WALOffset:        1014,
			WALSize:          1015,
		}
		b, err := hdr.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}

		var buf bytes.Buffer
		buf.Write(b)
		buf.Write([]byte("foobar"))

		// Read the header once.
		other, r, err := ltx.PeekHeader(&buf)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(hdr, other) {
			t.Fatalf("mismatch:\ngot=%#v\nwant=%#v", hdr, other)
		}

		// Read it again from the returned reader.
		if other, _, err = ltx.PeekHeader(r); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(hdr, other) {
			t.Fatalf("mismatch:\ngot=%#v\nwant=%#v", hdr, other)
		}

		// Read the rest of the data.
		if trailing, err := io.ReadAll(r); err != nil {
			t.Fatal(err)
		} else if got, want := string(trailing), "foobar"; got != want {
			t.Fatalf("trailing=%s, want %s", got, want)
		}
	})

	t.Run("EOF", func(t *testing.T) {
		if _, _, err := ltx.PeekHeader(bytes.NewReader(nil)); err != io.EOF {
			t.Fatal(err)
		}
	})
	t.Run("ErrUnexpectedEOF", func(t *testing.T) {
		if _, _, err := ltx.PeekHeader(bytes.NewReader([]byte("foo"))); err != io.ErrUnexpectedEOF {
			t.Fatal(err)
		}
	})
}

func TestPageHeader_Validate(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		hdr := ltx.PageHeader{Pgno: 1}
		if err := hdr.Validate(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("ErrPgnoRequired", func(t *testing.T) {
		hdr := ltx.PageHeader{}
		if err := hdr.Validate(); err == nil || err.Error() != `page number required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrFlagsNotAllowed", func(t *testing.T) {
		hdr := ltx.PageHeader{Pgno: 1, Flags: 4}
		if err := hdr.Validate(); err == nil || err.Error() != `invalid page header flags: 0x0004` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestPageHeader_MarshalBinary(t *testing.T) {
	hdr := ltx.PageHeader{Pgno: 1000}

	var other ltx.PageHeader
	if b, err := hdr.MarshalBinary(); err != nil {
		t.Fatal(err)
	} else if err := other.UnmarshalBinary(b); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(hdr, other) {
		t.Fatalf("mismatch:\ngot=%#v\nwant=%#v", hdr, other)
	}
}

func TestPageHeader_UnmarshalBinary(t *testing.T) {
	t.Run("ErrShortBuffer", func(t *testing.T) {
		var hdr ltx.PageHeader
		if err := hdr.UnmarshalBinary(make([]byte, 2)); err != io.ErrShortBuffer {
			t.Fatal(err)
		}
	})
}

func TestTrailer_Validate(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		trailer := ltx.Trailer{
			PostApplyChecksum: ltx.ChecksumFlag | 1,
			FileChecksum:      ltx.ChecksumFlag | 2,
		}
		if err := trailer.Validate(ltx.Header{}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("ErrPostApplyChecksumNotAllowed", func(t *testing.T) {
		trailer := ltx.Trailer{PostApplyChecksum: 1}
		if err := trailer.Validate(ltx.Header{Flags: ltx.HeaderFlagNoChecksum}); err == nil || err.Error() != `post-apply checksum not allowed` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrPostApplyChecksumRequired", func(t *testing.T) {
		trailer := ltx.Trailer{}
		if err := trailer.Validate(ltx.Header{}); err == nil || err.Error() != `post-apply checksum required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrInvalidPostApplyChecksum", func(t *testing.T) {
		trailer := ltx.Trailer{PostApplyChecksum: 1}
		if err := trailer.Validate(ltx.Header{}); err == nil || err.Error() != `invalid post-apply checksum format` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrFileChecksumRequired", func(t *testing.T) {
		trailer := ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag}
		if err := trailer.Validate(ltx.Header{}); err == nil || err.Error() != `file checksum required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrInvalidFileChecksum", func(t *testing.T) {
		trailer := ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag, FileChecksum: 1}
		if err := trailer.Validate(ltx.Header{}); err == nil || err.Error() != `invalid file checksum format` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestIsValidHeaderFlags(t *testing.T) {
	if !ltx.IsValidHeaderFlags(0) {
		t.Fatal("expected valid")
	} else if ltx.IsValidHeaderFlags(100) {
		t.Fatal("expected invalid")
	}
}

func TestIsValidPageSize(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		for _, sz := range []uint32{512, 1024, 2048, 4096, 8192, 16384, 32768, 65536} {
			if !ltx.IsValidPageSize(sz) {
				t.Fatalf("expected page size of %d to be valid", sz)
			}
		}
	})
	t.Run("TooSmall", func(t *testing.T) {
		if ltx.IsValidPageSize(256) {
			t.Fatal("expected invalid")
		}
	})
	t.Run("TooLarge", func(t *testing.T) {
		if ltx.IsValidPageSize(131072) {
			t.Fatal("expected invalid")
		}
	})
	t.Run("NotPowerOfTwo", func(t *testing.T) {
		if ltx.IsValidPageSize(1000) {
			t.Fatal("expected invalid")
		}
	})
}

func TestParseFilename(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		if min, max, err := ltx.ParseFilename("0000000000000001-00000000000003e8.ltx"); err != nil {
			t.Fatal(err)
		} else if got, want := min, ltx.TXID(1); got != want {
			t.Fatalf("min=%d, want %d", got, want)
		} else if got, want := max, ltx.TXID(1000); got != want {
			t.Fatalf("max=%d, want %d", got, want)
		}
	})

	t.Run("ErrInvalid", func(t *testing.T) {
		if _, _, err := ltx.ParseFilename("000000000000000z-00000000000003e8.ltx"); err == nil {
			t.Fatal("expected error")
		}
		if _, _, err := ltx.ParseFilename("0000000000000001.ltx"); err == nil {
			t.Fatal("expected error")
		}
		if _, _, err := ltx.ParseFilename("000000000000000z-00000000000003e8.zzz"); err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestChecksumReader(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		r := io.MultiReader(
			bytes.NewReader(bytes.Repeat([]byte("\x01"), 512)),
			bytes.NewReader(bytes.Repeat([]byte("\x02"), 512)),
			bytes.NewReader(bytes.Repeat([]byte("\x03"), 512)),
		)
		if chksum, err := ltx.ChecksumReader(r, 512); err != nil {
			t.Fatal(err)
		} else if got, want := chksum, ltx.Checksum(0xefffffffecd99000); got != want {
			t.Fatalf("got=%x, want %x", got, want)
		}
	})

	t.Run("ErrUnexpectedEOF", func(t *testing.T) {
		r := bytes.NewReader(bytes.Repeat([]byte("\x01"), 512))
		if _, err := ltx.ChecksumReader(r, 1024); err != io.ErrUnexpectedEOF {
			t.Fatal(err)
		}
	})
}

func TestTXID_MarshalJSON(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		txID := ltx.TXID(1000)
		if buf, err := json.Marshal(txID); err != nil {
			t.Fatal(err)
		} else if got, want := string(buf), `"00000000000003e8"`; got != want {
			t.Fatalf("got=%q, want %q", got, want)
		}
	})
	t.Run("Map", func(t *testing.T) {
		m := map[string]ltx.TXID{"x": 1000, "y": 2000}
		if buf, err := json.Marshal(m); err != nil {
			t.Fatal(err)
		} else if got, want := string(buf), `{"x":"00000000000003e8","y":"00000000000007d0"}`; got != want {
			t.Fatalf("got=%q, want %q", got, want)
		}
	})
}

func TestTXID_UnmarshalJSON(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		var txID ltx.TXID
		if err := json.Unmarshal([]byte(`"00000000000003e8"`), &txID); err != nil {
			t.Fatal(err)
		} else if got, want := txID, ltx.TXID(1000); got != want {
			t.Fatalf("got=%q, want %q", got, want)
		}
	})
	t.Run("Null", func(t *testing.T) {
		var txID ltx.TXID
		if err := json.Unmarshal([]byte(`null`), &txID); err != nil {
			t.Fatal(err)
		} else if got, want := txID, ltx.TXID(0); got != want {
			t.Fatalf("got=%q, want %q", got, want)
		}
	})
	t.Run("Map", func(t *testing.T) {
		var m map[string]ltx.TXID
		if err := json.Unmarshal([]byte(`{"x":"00000000000003e8","y":"00000000000007d0"}`), &m); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(m, map[string]ltx.TXID{"x": 1000, "y": 2000}) {
			t.Fatalf("unexpected map: %#v", m)
		}
	})
	t.Run("ErrInvalidType", func(t *testing.T) {
		var txID ltx.TXID
		if err := json.Unmarshal([]byte(`123`), &txID); err == nil || err.Error() != `cannot unmarshal TXID from JSON value` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrStringFormat", func(t *testing.T) {
		var txID ltx.TXID
		if err := json.Unmarshal([]byte(`"xyz"`), &txID); err == nil || err.Error() != `cannot parse TXID from JSON string: "xyz"` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestTXID_String(t *testing.T) {
	if got, want := ltx.TXID(0).String(), "0000000000000000"; got != want {
		t.Fatalf("got=%q, want %q", got, want)
	}
	if got, want := ltx.TXID(1000).String(), "00000000000003e8"; got != want {
		t.Fatalf("got=%q, want %q", got, want)
	}
	if got, want := ltx.TXID(math.MaxUint64).String(), "ffffffffffffffff"; got != want {
		t.Fatalf("got=%q, want %q", got, want)
	}
}

func TestParseTXID(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		if v, err := ltx.ParseTXID("0000000000000000"); err != nil {
			t.Fatal(err)
		} else if got, want := v, ltx.TXID(0); got != want {
			t.Fatalf("got=%d, want %d", got, want)
		}

		if v, err := ltx.ParseTXID("00000000000003e8"); err != nil {
			t.Fatal(err)
		} else if got, want := v, ltx.TXID(1000); got != want {
			t.Fatalf("got=%d, want %d", got, want)
		}

		if v, err := ltx.ParseTXID("ffffffffffffffff"); err != nil {
			t.Fatal(err)
		} else if got, want := v, ltx.TXID(math.MaxUint64); got != want {
			t.Fatalf("got=%d, want %d", got, want)
		}
	})
	t.Run("ErrTooShort", func(t *testing.T) {
		if _, err := ltx.ParseTXID("000000000e38"); err == nil || err.Error() != `invalid formatted transaction id length: "000000000e38"` {
			t.Fatal(err)
		}
	})
	t.Run("ErrTooLong", func(t *testing.T) {
		if _, err := ltx.ParseTXID("ffffffffffffffff0"); err == nil || err.Error() != `invalid formatted transaction id length: "ffffffffffffffff0"` {
			t.Fatal(err)
		}
	})
	t.Run("ErrInvalidFormat", func(t *testing.T) {
		if _, err := ltx.ParseTXID("xxxxxxxxxxxxxxxx"); err == nil || err.Error() != `invalid transaction id format: "xxxxxxxxxxxxxxxx"` {
			t.Fatal(err)
		}
	})
}

func TestChecksum_MarshalJSON(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		chksum := ltx.Checksum(1000)
		if buf, err := json.Marshal(chksum); err != nil {
			t.Fatal(err)
		} else if got, want := string(buf), `"00000000000003e8"`; got != want {
			t.Fatalf("got=%q, want %q", got, want)
		}
	})
	t.Run("Map", func(t *testing.T) {
		m := map[string]ltx.Checksum{"x": 1000, "y": 2000}
		if buf, err := json.Marshal(m); err != nil {
			t.Fatal(err)
		} else if got, want := string(buf), `{"x":"00000000000003e8","y":"00000000000007d0"}`; got != want {
			t.Fatalf("got=%q, want %q", got, want)
		}
	})
}

func TestChecksum_UnmarshalJSON(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		var chksum ltx.Checksum
		if err := json.Unmarshal([]byte(`"00000000000003e8"`), &chksum); err != nil {
			t.Fatal(err)
		} else if got, want := chksum, ltx.Checksum(1000); got != want {
			t.Fatalf("got=%q, want %q", got, want)
		}
	})
	t.Run("Null", func(t *testing.T) {
		var chksum ltx.Checksum
		if err := json.Unmarshal([]byte(`null`), &chksum); err != nil {
			t.Fatal(err)
		} else if got, want := chksum, ltx.Checksum(0); got != want {
			t.Fatalf("got=%q, want %q", got, want)
		}
	})
	t.Run("Map", func(t *testing.T) {
		var m map[string]ltx.Checksum
		if err := json.Unmarshal([]byte(`{"x":"00000000000003e8","y":"00000000000007d0"}`), &m); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(m, map[string]ltx.Checksum{"x": 1000, "y": 2000}) {
			t.Fatalf("unexpected map: %#v", m)
		}
	})
	t.Run("ErrInvalidType", func(t *testing.T) {
		var chksum ltx.Checksum
		if err := json.Unmarshal([]byte(`123`), &chksum); err == nil || err.Error() != `cannot unmarshal checksum from JSON value` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrStringFormat", func(t *testing.T) {
		var chksum ltx.Checksum
		if err := json.Unmarshal([]byte(`"xyz"`), &chksum); err == nil || err.Error() != `cannot parse checksum from JSON string: "xyz"` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestChecksum_String(t *testing.T) {
	if got, want := ltx.Checksum(0).String(), "0000000000000000"; got != want {
		t.Fatalf("got=%q, want %q", got, want)
	}
	if got, want := ltx.Checksum(1000).String(), "00000000000003e8"; got != want {
		t.Fatalf("got=%q, want %q", got, want)
	}
	if got, want := ltx.Checksum(math.MaxUint64).String(), "ffffffffffffffff"; got != want {
		t.Fatalf("got=%q, want %q", got, want)
	}
}

func TestParseChecksum(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		if v, err := ltx.ParseChecksum("0000000000000000"); err != nil {
			t.Fatal(err)
		} else if got, want := v, ltx.Checksum(0); got != want {
			t.Fatalf("got=%d, want %d", got, want)
		}

		if v, err := ltx.ParseChecksum("00000000000003e8"); err != nil {
			t.Fatal(err)
		} else if got, want := v, ltx.Checksum(1000); got != want {
			t.Fatalf("got=%d, want %d", got, want)
		}

		if v, err := ltx.ParseChecksum("ffffffffffffffff"); err != nil {
			t.Fatal(err)
		} else if got, want := v, ltx.Checksum(math.MaxUint64); got != want {
			t.Fatalf("got=%d, want %d", got, want)
		}
	})
	t.Run("ErrTooShort", func(t *testing.T) {
		if _, err := ltx.ParseChecksum("000000000e38"); err == nil || err.Error() != `invalid formatted checksum length: "000000000e38"` {
			t.Fatal(err)
		}
	})
	t.Run("ErrTooLong", func(t *testing.T) {
		if _, err := ltx.ParseChecksum("ffffffffffffffff0"); err == nil || err.Error() != `invalid formatted checksum length: "ffffffffffffffff0"` {
			t.Fatal(err)
		}
	})
	t.Run("ErrInvalidFormat", func(t *testing.T) {
		if _, err := ltx.ParseChecksum("xxxxxxxxxxxxxxxx"); err == nil || err.Error() != `invalid checksum format: "xxxxxxxxxxxxxxxx"` {
			t.Fatal(err)
		}
	})
}

func TestFormatTimestamp(t *testing.T) {
	for _, tt := range []struct {
		t    time.Time
		want string
	}{
		{time.Date(2000, 10, 20, 30, 40, 50, 0, time.UTC), "2000-10-21T06:40:50.000Z"},
		{time.Date(2000, 10, 20, 30, 40, 50, 123000000, time.UTC), "2000-10-21T06:40:50.123Z"},
		{time.Date(2000, 10, 20, 30, 40, 50, 120000000, time.UTC), "2000-10-21T06:40:50.120Z"},
		{time.Date(2000, 10, 20, 30, 40, 50, 100000000, time.UTC), "2000-10-21T06:40:50.100Z"},
		{time.Date(2000, 10, 20, 30, 40, 50, 100000, time.UTC), "2000-10-21T06:40:50.000Z"}, // submillisecond
	} {
		if got := ltx.FormatTimestamp(tt.t); got != tt.want {
			t.Fatalf("got=%s, want %s", got, tt.want)
		}
	}
}

func TestParseTimestamp(t *testing.T) {
	for _, tt := range []struct {
		str  string
		want time.Time
	}{
		{"2000-10-21T06:40:50.000Z", time.Date(2000, 10, 20, 30, 40, 50, 0, time.UTC)},
		{"2000-10-21T06:40:50.123Z", time.Date(2000, 10, 20, 30, 40, 50, 123000000, time.UTC)},
		{"2000-10-21T06:40:50.120Z", time.Date(2000, 10, 20, 30, 40, 50, 120000000, time.UTC)},
		{"2000-10-21T06:40:50.100Z", time.Date(2000, 10, 20, 30, 40, 50, 100000000, time.UTC)},
		{"2000-10-21T06:40:50Z", time.Date(2000, 10, 20, 30, 40, 50, 0, time.UTC)},
		{"2000-10-21T06:40:50.000123Z", time.Date(2000, 10, 20, 30, 40, 50, 0, time.UTC)},
		{"2000-10-21T06:40:50.000000123Z", time.Date(2000, 10, 20, 30, 40, 50, 0, time.UTC)},
	} {
		if got, err := ltx.ParseTimestamp(tt.str); err != nil {
			t.Fatal(err)
		} else if !got.Equal(tt.want) {
			t.Fatalf("got=%s, want %s", got, tt.want)
		}
	}
}

func TestIsContiguous(t *testing.T) {
	if !ltx.IsContiguous(0, 1, 10) {
		t.Fatal("expected contiguous")
	}
	if !ltx.IsContiguous(11, 11, 12) {
		t.Fatal("expected contiguous")
	}
}

func BenchmarkChecksumPage(b *testing.B) {
	for _, pageSize := range []int{512, 1024, 2048, 4096, 8192, 16384, 32768, 65536} {
		b.Run(fmt.Sprint(pageSize), func(b *testing.B) {
			benchmarkChecksumPage(b, pageSize)
		})
	}
}

func benchmarkChecksumPage(b *testing.B, pageSize int) {
	data := make([]byte, pageSize)
	_, _ = rand.Read(data)
	b.ReportAllocs()
	b.SetBytes(int64(pageSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ltx.ChecksumPage(uint32(i%math.MaxUint32), data)
	}
}

func BenchmarkChecksumPageWithHasher(b *testing.B) {
	for _, pageSize := range []int{512, 1024, 2048, 4096, 8192, 16384, 32768, 65536} {
		b.Run(fmt.Sprint(pageSize), func(b *testing.B) {
			benchmarkChecksumPageWithHasher(b, pageSize)
		})
	}
}

func benchmarkChecksumPageWithHasher(b *testing.B, pageSize int) {
	data := make([]byte, pageSize)
	_, _ = rand.Read(data)
	b.ReportAllocs()
	b.SetBytes(int64(pageSize))
	b.ResetTimer()

	h := ltx.NewHasher()
	for i := 0; i < b.N; i++ {
		ltx.ChecksumPageWithHasher(h, uint32(i%math.MaxUint32), data)
	}
}

// BenchmarkXOR simulates the sum of checksums for a 1GB database (assuming 4KB pages).
func BenchmarkXOR(b *testing.B) {
	const pageSize = 4096
	const pageN = (1 << 30) / pageSize

	m := make(map[uint32]ltx.Checksum)
	page := make([]byte, pageSize)
	for pgno := uint32(1); pgno <= pageN; pgno++ {
		_, _ = rand.Read(page)
		m[pgno] = ltx.ChecksumPage(pgno, page)
	}
	b.SetBytes(int64(pageN * pageSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var chksum ltx.Checksum
		for pgno := uint32(1); pgno <= pageN; pgno++ {
			chksum ^= m[pgno]
		}
	}
}

// createFile creates a file and returns the file handle. Closes on cleanup.
func createFile(tb testing.TB, name string) *os.File {
	tb.Helper()
	f, err := os.Create(name)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = f.Close() })
	return f
}

// writeFileSpec is a helper function for writing a spec to a file.
func writeFileSpec(tb testing.TB, w io.Writer, spec *ltx.FileSpec) int64 {
	tb.Helper()
	n, err := spec.WriteTo(w)
	if err != nil {
		tb.Fatal(err)
	}
	return int64(n)
}

// readFileSpec is a helper function for reading a spec from a file.
func readFileSpec(tb testing.TB, r io.Reader) *ltx.FileSpec {
	tb.Helper()
	var spec ltx.FileSpec
	if _, err := spec.ReadFrom(r); err != nil {
		tb.Fatal(err)
	}
	return &spec
}

// compactFileSpecs compacts a set of file specs to a new spec.
func compactFileSpecs(tb testing.TB, inputs ...*ltx.FileSpec) (*ltx.FileSpec, error) {
	tb.Helper()

	// Write input specs to file.
	wtrs := make([]io.Writer, len(inputs))
	rdrs := make([]io.Reader, len(inputs))
	for i, input := range inputs {
		var buf bytes.Buffer
		wtrs[i], rdrs[i] = &buf, &buf
		writeFileSpec(tb, wtrs[i], input)
	}

	// Compact files together.
	var output bytes.Buffer
	c, err := ltx.NewCompactor(&output, rdrs)
	if err != nil {
		return nil, err
	}
	if err := c.Compact(context.Background()); err != nil {
		return nil, err
	}
	return readFileSpec(tb, &output), nil
}

// assertFileSpecEqual checks x & y for equality. Fail on inequality.
func assertFileSpecEqual(tb testing.TB, x, y *ltx.FileSpec) {
	tb.Helper()

	if got, want := x.Header, y.Header; got != want {
		tb.Fatalf("header mismatch:\ngot=%#v\nwant=%#v", got, want)
	}

	if got, want := len(x.Pages), len(y.Pages); got != want {
		tb.Fatalf("page count: %d, want %d", got, want)
	}
	for i := range x.Pages {
		// Compare only Pgno, not Flags. The encoder sets PageHeaderFlagSize
		// on output, so Flags won't match the input spec.
		if got, want := x.Pages[i].Header.Pgno, y.Pages[i].Header.Pgno; got != want {
			tb.Fatalf("page header pgno mismatch: i=%d\ngot=%d\nwant=%d", i, got, want)
		}
		if got, want := x.Pages[i].Data, y.Pages[i].Data; !bytes.Equal(got, want) {
			tb.Fatalf("page data mismatch: i=%d\ngot=%#v\nwant=%#v", i, got, want)
		}
	}

	// Compare only PostApplyChecksum, not FileChecksum. FileChecksum depends on
	// the exact byte representation of the file, which changes when the format changes.
	if got, want := x.Trailer.PostApplyChecksum, y.Trailer.PostApplyChecksum; got != want {
		tb.Fatalf("trailer PostApplyChecksum mismatch:\ngot=0x%x\nwant=0x%x", got, want)
	}
}
