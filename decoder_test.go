package ltx_test

import (
	"bytes"
	"crypto/rand"
	"io"
	"reflect"
	"testing"

	"github.com/superfly/ltx"
)

func TestDecoder(t *testing.T) {
	spec := &ltx.FileSpec{
		Header: ltx.Header{
			Version:   ltx.Version3,
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
		}
		// Encoder now sets PageHeaderFlagSize, so compare only Pgno.
		if got, want := hdr.Pgno, spec.Pages[i].Header.Pgno; got != want {
			t.Fatalf("page hdr pgno mismatch:\ngot=%d\nwant=%d", got, want)
		}
		if got, want := hdr.Flags, uint16(ltx.PageHeaderFlagSize); got != want {
			t.Fatalf("page hdr flags mismatch:\ngot=0x%x\nwant=0x%x", got, want)
		}
		if got, want := data, spec.Pages[i].Data; !bytes.Equal(got, want) {
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
	// Block format: PageHeader(6) + Size(4) + compressed block data (~26 bytes for repetitive data).
	index := dec.PageIndex()
	if got, want := index, map[uint32]ltx.PageIndexElem{
		1: {MinTXID: 1, MaxTXID: 1, Offset: 100, Size: 36},
		2: {MinTXID: 1, MaxTXID: 1, Offset: 136, Size: 36},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("page index mismatch:\ngot=%#v\nwant=%#v", got, want)
	}

	// Read page 1 by offset.
	if hdr, data, err := ltx.DecodePageData(fileSpecData[100:]); err != nil {
		t.Fatal(err)
	} else if got, want := hdr.Pgno, uint32(1); got != want {
		t.Fatalf("page header pgno mismatch:\ngot=%d\nwant=%d", got, want)
	} else if got, want := data, bytes.Repeat([]byte("2"), 1024); !bytes.Equal(got, want) {
		t.Fatalf("page data mismatch:\ngot=%#v\nwant=%#v", got, want)
	}

	// Read page 2 by offset. Offset is 136 with block format.
	if hdr, data, err := ltx.DecodePageData(fileSpecData[136:]); err != nil {
		t.Fatal(err)
	} else if got, want := hdr.Pgno, uint32(2); got != want {
		t.Fatalf("page header pgno mismatch:\ngot=%d\nwant=%d", got, want)
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
			Version:   ltx.Version3,
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

		// This is the canonical lock-page-excluded checksum for the fixture.
		const canonicalPostApplyChecksum = ltx.Checksum(0xc19b668c376662c7)

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

		postApplyChecksum, err := ltx.ChecksumReader(bytes.NewReader(want.Bytes()), 4096)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := postApplyChecksum, canonicalPostApplyChecksum; got != want {
			t.Fatalf("post-apply checksum=%s, want canonical lock-page-excluded checksum %s", got, want)
		}
		enc.SetPostApplyChecksum(postApplyChecksum)
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

func TestDecoder_Encrypted(t *testing.T) {
	t.Run("RoundTrip", func(t *testing.T) {
		pub, priv, err := ltx.GenerateKeyPair()
		if err != nil {
			t.Fatal(err)
		}

		page1Data := bytes.Repeat([]byte("x"), 1024)
		page2Data := bytes.Repeat([]byte("y"), 1024)

		chksum := ltx.ChecksumFlag
		chksum = ltx.ChecksumFlag | (chksum ^ ltx.ChecksumPage(1, page1Data))
		chksum = ltx.ChecksumFlag | (chksum ^ ltx.ChecksumPage(2, page2Data))

		var buf bytes.Buffer
		enc, err := ltx.NewEncoder(&buf)
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.SetEncryption([][]byte{pub}); err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodeHeader(ltx.Header{
			Version:   ltx.Version,
			PageSize:  1024,
			Commit:    2,
			MinTXID:   1,
			MaxTXID:   1,
			Timestamp: 1000,
		}); err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: 1}, page1Data); err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: 2}, page2Data); err != nil {
			t.Fatal(err)
		}
		enc.SetPostApplyChecksum(chksum)
		if err := enc.Close(); err != nil {
			t.Fatal(err)
		}

		// Verify header has encryption flag.
		hdr := enc.Header()
		if !hdr.Encrypted() {
			t.Fatal("expected encrypted flag")
		}
		if hdr.RecipientCount != 1 {
			t.Fatalf("recipient count=%d, want 1", hdr.RecipientCount)
		}

		// Decode.
		dec := ltx.NewDecoder(&buf)
		dec.SetDecryptionKey(priv)
		if err := dec.DecodeHeader(); err != nil {
			t.Fatal(err)
		}

		var pageHdr ltx.PageHeader
		data := make([]byte, 1024)

		if err := dec.DecodePage(&pageHdr, data); err != nil {
			t.Fatal(err)
		}
		if pageHdr.Pgno != 1 {
			t.Fatalf("pgno=%d, want 1", pageHdr.Pgno)
		}
		if !bytes.Equal(data, page1Data) {
			t.Fatal("page 1 data mismatch")
		}

		if err := dec.DecodePage(&pageHdr, data); err != nil {
			t.Fatal(err)
		}
		if pageHdr.Pgno != 2 {
			t.Fatalf("pgno=%d, want 2", pageHdr.Pgno)
		}
		if !bytes.Equal(data, page2Data) {
			t.Fatal("page 2 data mismatch")
		}

		if err := dec.DecodePage(&pageHdr, data); err != io.EOF {
			t.Fatalf("expected EOF, got %v", err)
		}
		if err := dec.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("MultiRecipient", func(t *testing.T) {
		pub1, priv1, _ := ltx.GenerateKeyPair()
		pub2, priv2, _ := ltx.GenerateKeyPair()

		page1Data := bytes.Repeat([]byte("a"), 1024)
		chksum := ltx.ChecksumFlag | ltx.ChecksumPage(1, page1Data)

		var buf bytes.Buffer
		enc, err := ltx.NewEncoder(&buf)
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.SetEncryption([][]byte{pub1, pub2}); err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodeHeader(ltx.Header{
			Version: ltx.Version, PageSize: 1024, Commit: 1,
			MinTXID: 1, MaxTXID: 1, Timestamp: 1000,
		}); err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: 1}, page1Data); err != nil {
			t.Fatal(err)
		}
		enc.SetPostApplyChecksum(chksum)
		if err := enc.Close(); err != nil {
			t.Fatal(err)
		}

		encoded := buf.Bytes()

		// Both keys should decrypt.
		for i, priv := range [][]byte{priv1, priv2} {
			dec := ltx.NewDecoder(bytes.NewReader(encoded))
			dec.SetDecryptionKey(priv)
			if err := dec.DecodeHeader(); err != nil {
				t.Fatalf("recipient %d: decode header: %v", i, err)
			}

			var hdr ltx.PageHeader
			data := make([]byte, 1024)
			if err := dec.DecodePage(&hdr, data); err != nil {
				t.Fatalf("recipient %d: decode page: %v", i, err)
			}
			if !bytes.Equal(data, page1Data) {
				t.Fatalf("recipient %d: data mismatch", i)
			}

			if err := dec.DecodePage(&hdr, data); err != io.EOF {
				t.Fatalf("recipient %d: expected EOF", i)
			}
			if err := dec.Close(); err != nil {
				t.Fatalf("recipient %d: close: %v", i, err)
			}
		}
	})

	// encodeEncrypted builds a single-page encrypted file for the keyless tests.
	encodeEncrypted := func(t *testing.T) []byte {
		t.Helper()

		pub, _, err := ltx.GenerateKeyPair()
		if err != nil {
			t.Fatal(err)
		}

		var buf bytes.Buffer
		enc, err := ltx.NewEncoder(&buf)
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.SetEncryption([][]byte{pub}); err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodeHeader(ltx.Header{
			Version: ltx.Version, PageSize: 1024, Commit: 1,
			MinTXID: 1, MaxTXID: 1, Timestamp: 1000,
		}); err != nil {
			t.Fatal(err)
		}
		page := bytes.Repeat([]byte("s"), 1024)
		if err := enc.EncodePage(ltx.PageHeader{Pgno: 1}, page); err != nil {
			t.Fatal(err)
		}
		enc.SetPostApplyChecksum(ltx.ChecksumPage(1, page))
		if err := enc.Close(); err != nil {
			t.Fatal(err)
		}
		return buf.Bytes()
	}

	// The file checksum of an encrypted file covers the ciphertext as written,
	// so integrity is verifiable by a holder of the file alone. Contents are
	// not: DecodePage must refuse rather than hand back an empty buffer.
	t.Run("NoKeyVerifiesIntegrity", func(t *testing.T) {
		b := encodeEncrypted(t)

		if err := ltx.NewDecoder(bytes.NewReader(b)).Verify(); err != nil {
			t.Fatalf("keyless Verify should succeed on an intact file: %v", err)
		}
	})

	t.Run("NoKeyRefusesPageContents", func(t *testing.T) {
		b := encodeEncrypted(t)

		dec := ltx.NewDecoder(bytes.NewReader(b))
		if err := dec.DecodeHeader(); err != nil {
			t.Fatalf("keyless DecodeHeader should succeed: %v", err)
		}

		var hdr ltx.PageHeader
		data := make([]byte, 1024)
		if err := dec.DecodePage(&hdr, data); err != ltx.ErrDecryptionKeyRequired {
			t.Fatalf("expected ErrDecryptionKeyRequired, got: %v", err)
		}
	})

	t.Run("NoKeyDetectsCorruption", func(t *testing.T) {
		b := encodeEncrypted(t)

		// Flip a bit in the final page's ciphertext. Keyless verification must
		// still catch it, otherwise hashing ciphertext buys nothing.
		corrupt := make([]byte, len(b))
		copy(corrupt, b)
		corrupt[len(corrupt)-40] ^= 0x01

		if err := ltx.NewDecoder(bytes.NewReader(corrupt)).Verify(); err == nil {
			t.Fatal("keyless Verify accepted a corrupted file")
		}
	})

	t.Run("WrongKeyForEncryptedFile", func(t *testing.T) {
		pub, _, _ := ltx.GenerateKeyPair()
		_, wrongPriv, _ := ltx.GenerateKeyPair()

		var buf bytes.Buffer
		enc, _ := ltx.NewEncoder(&buf)
		_ = enc.SetEncryption([][]byte{pub})
		_ = enc.EncodeHeader(ltx.Header{
			Version: ltx.Version, PageSize: 1024, Commit: 1,
			MinTXID: 1, MaxTXID: 1, Timestamp: 1000,
		})
		_ = enc.EncodePage(ltx.PageHeader{Pgno: 1}, make([]byte, 1024))
		enc.SetPostApplyChecksum(ltx.ChecksumFlag)
		_ = enc.Close()

		dec := ltx.NewDecoder(&buf)
		dec.SetDecryptionKey(wrongPriv)
		if err := dec.DecodeHeader(); err == nil {
			t.Fatal("expected error with wrong key")
		}
	})
}

func TestFileSpec_StripEncryptionWhenNoRecipients(t *testing.T) {
	pub, priv, _ := ltx.GenerateKeyPair()

	pageData := make([]byte, 1024)
	pageData[0] = 0xAB

	chksum := ltx.ChecksumFlag | ltx.ChecksumPage(1, pageData)

	// Create encrypted file.
	var encBuf bytes.Buffer
	spec := ltx.FileSpec{
		Header: ltx.Header{
			Version: ltx.Version, PageSize: 1024, Commit: 1,
			MinTXID: 1, MaxTXID: 1, Timestamp: 1000,
		},
		Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: pageData}},
		Trailer: ltx.Trailer{PostApplyChecksum: chksum},

		RecipientPublicKeys: [][]byte{pub},
	}
	if _, err := spec.WriteTo(&encBuf); err != nil {
		t.Fatal(err)
	}

	// Read back with key.
	var spec2 ltx.FileSpec
	if _, err := spec2.ReadFromWithKey(bytes.NewReader(encBuf.Bytes()), priv); err != nil {
		t.Fatal(err)
	}

	// Write without recipients — should produce a valid unencrypted file.
	spec2.RecipientPublicKeys = nil
	var plainBuf bytes.Buffer
	if _, err := spec2.WriteTo(&plainBuf); err != nil {
		t.Fatal(err)
	}

	// Verify the output is readable without a key.
	var spec3 ltx.FileSpec
	if _, err := spec3.ReadFrom(bytes.NewReader(plainBuf.Bytes())); err != nil {
		t.Fatalf("expected unencrypted file to be readable without key: %v", err)
	}
	if spec3.Header.Encrypted() {
		t.Fatal("expected header to not have encrypted flag")
	}
	if !bytes.Equal(spec3.Pages[0].Data, pageData) {
		t.Fatal("page data mismatch")
	}
}

func TestDecoder_64KBPageSize(t *testing.T) {
	const pageSize = 65536 // 64KB - maximum SQLite page size

	t.Run("Compressible", func(t *testing.T) {
		// Test with compressible data (repetitive pattern).
		page1Data := bytes.Repeat([]byte("A"), pageSize)
		page2Data := bytes.Repeat([]byte("B"), pageSize)

		// Calculate correct post-apply checksum.
		chksum := ltx.ChecksumFlag
		chksum = ltx.ChecksumFlag | (chksum ^ ltx.ChecksumPage(1, page1Data))
		chksum = ltx.ChecksumFlag | (chksum ^ ltx.ChecksumPage(2, page2Data))

		var buf bytes.Buffer
		enc, err := ltx.NewEncoder(&buf)
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodeHeader(ltx.Header{
			Version:   ltx.Version,
			PageSize:  pageSize,
			Commit:    2,
			MinTXID:   1,
			MaxTXID:   1,
			Timestamp: 1000,
		}); err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: 1}, page1Data); err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: 2}, page2Data); err != nil {
			t.Fatal(err)
		}
		enc.SetPostApplyChecksum(chksum)
		if err := enc.Close(); err != nil {
			t.Fatal(err)
		}

		// Decode and verify.
		dec := ltx.NewDecoder(&buf)
		if err := dec.DecodeHeader(); err != nil {
			t.Fatal(err)
		}

		var hdr ltx.PageHeader
		data := make([]byte, pageSize)

		if err := dec.DecodePage(&hdr, data); err != nil {
			t.Fatal(err)
		}
		if hdr.Pgno != 1 {
			t.Fatalf("expected pgno 1, got %d", hdr.Pgno)
		}
		if !bytes.Equal(data, page1Data) {
			t.Fatal("page 1 data mismatch")
		}
		if hdr.Flags != ltx.PageHeaderFlagSize {
			t.Fatalf("expected size flag, got 0x%x", hdr.Flags)
		}

		if err := dec.DecodePage(&hdr, data); err != nil {
			t.Fatal(err)
		}
		if hdr.Pgno != 2 {
			t.Fatalf("expected pgno 2, got %d", hdr.Pgno)
		}
		if !bytes.Equal(data, page2Data) {
			t.Fatal("page 2 data mismatch")
		}

		if err := dec.DecodePage(&hdr, data); err != io.EOF {
			t.Fatalf("expected EOF, got %v", err)
		}
		if err := dec.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Incompressible", func(t *testing.T) {
		// Test with incompressible data (truly random bytes).
		page1Data := make([]byte, pageSize)
		if _, err := rand.Read(page1Data); err != nil {
			t.Fatal(err)
		}

		// Calculate correct post-apply checksum.
		chksum := ltx.ChecksumFlag | ltx.ChecksumPage(1, page1Data)

		var buf bytes.Buffer
		enc, err := ltx.NewEncoder(&buf)
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodeHeader(ltx.Header{
			Version:   ltx.Version,
			PageSize:  pageSize,
			Commit:    1,
			MinTXID:   1,
			MaxTXID:   1,
			Timestamp: 1000,
		}); err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: 1}, page1Data); err != nil {
			t.Fatal(err)
		}
		enc.SetPostApplyChecksum(chksum)
		if err := enc.Close(); err != nil {
			t.Fatal(err)
		}

		// Decode and verify.
		dec := ltx.NewDecoder(&buf)
		if err := dec.DecodeHeader(); err != nil {
			t.Fatal(err)
		}

		var hdr ltx.PageHeader
		data := make([]byte, pageSize)

		if err := dec.DecodePage(&hdr, data); err != nil {
			t.Fatal(err)
		}
		if hdr.Pgno != 1 {
			t.Fatalf("expected pgno 1, got %d", hdr.Pgno)
		}
		if !bytes.Equal(data, page1Data) {
			t.Fatal("page 1 data mismatch")
		}
		// Random data is still compressed (even if slightly larger).
		if hdr.Flags != ltx.PageHeaderFlagSize {
			t.Fatalf("expected size flag 0x%x, got 0x%x", ltx.PageHeaderFlagSize, hdr.Flags)
		}

		if err := dec.DecodePage(&hdr, data); err != io.EOF {
			t.Fatalf("expected EOF, got %v", err)
		}
		if err := dec.Close(); err != nil {
			t.Fatal(err)
		}
	})
}
