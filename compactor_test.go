package ltx_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/superfly/ltx"
)

func TestCompactor_Compact(t *testing.T) {
	t.Run("SingleFilePageDataOnly", func(t *testing.T) {
		input := &ltx.FileSpec{
			Header: ltx.Header{
				Version:          ltx.Version3,
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
				PostApplyChecksum: 0xeb1a999231044ddd,
				FileChecksum:      0x897cc5d024cd382a,
			},
		}
		var buf0 bytes.Buffer
		writeFileSpec(t, &buf0, input)

		var output bytes.Buffer
		c, err := ltx.NewCompactor(&output, []io.Reader{&buf0})
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Compact(context.Background()); err != nil {
			t.Fatal(err)
		}

		spec := readFileSpec(t, &output)
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
					Version:   ltx.Version,
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
					PostApplyChecksum: 0x8a249272ad9f7dea,
				},
			},
			&ltx.FileSpec{
				Header: ltx.Header{
					Version:          ltx.Version,
					PageSize:         1024,
					Commit:           3,
					MinTXID:          2,
					MaxTXID:          2,
					Timestamp:        2000,
					PreApplyChecksum: 0x8a249272ad9f7dea,
				},
				Pages: []ltx.PageSpec{
					{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{0x91}, 1024)},
					{Header: ltx.PageHeader{Pgno: 3}, Data: bytes.Repeat([]byte{0x93}, 1024)},
				},
				Trailer: ltx.Trailer{
					PostApplyChecksum: 0x8a249272ad9f7dea,
				},
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		assertFileSpecEqual(t, spec, &ltx.FileSpec{
			Header: ltx.Header{
				Version:   ltx.Version3,
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
				PostApplyChecksum: 0x8a249272ad9f7dea,
				FileChecksum:      0xe836ad7ac0be7594,
			},
		})
	})
	t.Run("NonSnapshotPageDataOnly", func(t *testing.T) {
		spec, err := compactFileSpecs(t,
			&ltx.FileSpec{
				Header: ltx.Header{
					Version:          ltx.Version,
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
				Header: ltx.Header{
					Version:          ltx.Version,
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
					Version:          ltx.Version,
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
				Version:          ltx.Version3,
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
				FileChecksum:      0xc08218012f50038e,
			},
		})
	})

	t.Run("Shrinking", func(t *testing.T) {
		spec, err := compactFileSpecs(t,
			&ltx.FileSpec{
				Header: ltx.Header{Version: ltx.Version, PageSize: 1024, Commit: 3, MinTXID: 2, MaxTXID: 3, Timestamp: 1000, PreApplyChecksum: ltx.ChecksumFlag | 2},
				Pages: []ltx.PageSpec{
					{Header: ltx.PageHeader{Pgno: 3}, Data: bytes.Repeat([]byte{0x83}, 1024)},
				},
				Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 3},
			},
			&ltx.FileSpec{
				Header: ltx.Header{Version: ltx.Version, PageSize: 1024, Commit: 2, MinTXID: 4, MaxTXID: 5, Timestamp: 2000, PreApplyChecksum: ltx.ChecksumFlag | 4},
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
				Version:          ltx.Version3,
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
				FileChecksum:      0x83c62dde98efed0f,
			},
		})
	})

	t.Run("ErrInputReaderRequired", func(t *testing.T) {
		c, err := ltx.NewCompactor(&bytes.Buffer{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Compact(context.Background()); err == nil || err.Error() != `at least one input reader required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrDecodeHeader", func(t *testing.T) {
		c, err := ltx.NewCompactor(io.Discard, []io.Reader{bytes.NewReader(nil)})
		if err != nil {
			t.Fatal(err)
		}

		err = c.Compact(context.Background())
		if err == nil || err.Error() != `decode header for input 0: EOF` {
			t.Fatalf("unexpected error: %v", err)
		}
		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected EOF, got: %v", err)
		}
	})
	t.Run("ErrPageSizeMismatch", func(t *testing.T) {
		_, err := compactFileSpecs(t,
			&ltx.FileSpec{
				Header:  ltx.Header{Version: ltx.Version, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1, Timestamp: 1000},
				Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{0x81}, 512)}},
				Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 1},
			},
			&ltx.FileSpec{
				Header:  ltx.Header{Version: ltx.Version, PageSize: 1024, Commit: 1, MinTXID: 1, MaxTXID: 1, Timestamp: 1000},
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
				Header:  ltx.Header{Version: ltx.Version, PageSize: 1024, Commit: 1, MinTXID: 1, MaxTXID: 2, Timestamp: 1000},
				Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{0x81}, 1024)}},
				Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 1},
			},
			&ltx.FileSpec{
				Header:  ltx.Header{Version: ltx.Version, PageSize: 1024, Commit: 1, MinTXID: 4, MaxTXID: 4, Timestamp: 1000, PreApplyChecksum: ltx.ChecksumFlag | 2},
				Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{0x91}, 1024)}},
				Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 1},
			},
		)
		if err == nil || err.Error() != `non-contiguous transaction ids in input files: (0000000000000001,0000000000000002) -> (0000000000000004,0000000000000004)` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("AllowNonContiguousTXID", func(t *testing.T) {
		bufs := make([]bytes.Buffer, 2)
		writeFileSpec(t, &bufs[0], &ltx.FileSpec{
			Header:  ltx.Header{Version: ltx.Version, PageSize: 1024, Commit: 1, MinTXID: 1, MaxTXID: 2, Timestamp: 1000},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{0x81}, 1024)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xeb953fc47685d740},
		})

		writeFileSpec(t, &bufs[1], &ltx.FileSpec{
			Header:  ltx.Header{Version: ltx.Version, PageSize: 1024, Commit: 1, MinTXID: 4, MaxTXID: 4, Timestamp: 1000, PreApplyChecksum: ltx.ChecksumFlag | 2},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{0x91}, 1024)}},
			Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 1},
		})

		// Compact files together.
		c, err := ltx.NewCompactor(io.Discard, []io.Reader{&bufs[0], &bufs[1]})
		if err != nil {
			t.Fatal(err)
		}
		c.AllowNonContiguousTXIDs = true
		if err := c.Compact(context.Background()); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("EncryptedToEncrypted", func(t *testing.T) {
		pub, priv, _ := ltx.GenerateKeyPair()

		page1Data := bytes.Repeat([]byte{0x81}, 1024)
		page2Data := bytes.Repeat([]byte{0x82}, 1024)

		// Create encrypted input.
		var encBuf bytes.Buffer
		enc, _ := ltx.NewEncoder(&encBuf)
		_ = enc.SetEncryption([][]byte{pub})
		_ = enc.EncodeHeader(ltx.Header{
			Version: ltx.Version, PageSize: 1024, Commit: 2,
			MinTXID: 1, MaxTXID: 1, Timestamp: 1000,
		})
		_ = enc.EncodePage(ltx.PageHeader{Pgno: 1}, page1Data)
		_ = enc.EncodePage(ltx.PageHeader{Pgno: 2}, page2Data)
		chksum := ltx.ChecksumFlag
		chksum = ltx.ChecksumFlag | (chksum ^ ltx.ChecksumPage(1, page1Data))
		chksum = ltx.ChecksumFlag | (chksum ^ ltx.ChecksumPage(2, page2Data))
		enc.SetPostApplyChecksum(chksum)
		_ = enc.Close()

		// Compact encrypted → encrypted.
		var output bytes.Buffer
		c, _ := ltx.NewCompactor(&output, []io.Reader{&encBuf})
		c.DecryptionKey = priv
		c.OutputEncryption = [][]byte{pub}
		if err := c.Compact(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Verify output is decryptable.
		dec := ltx.NewDecoder(&output)
		dec.SetDecryptionKey(priv)
		if err := dec.DecodeHeader(); err != nil {
			t.Fatal(err)
		}
		if !dec.Header().Encrypted() {
			t.Fatal("expected encrypted output")
		}

		var hdr ltx.PageHeader
		data := make([]byte, 1024)
		if err := dec.DecodePage(&hdr, data); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(data, page1Data) {
			t.Fatal("page 1 data mismatch")
		}
		if err := dec.DecodePage(&hdr, data); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(data, page2Data) {
			t.Fatal("page 2 data mismatch")
		}
		if err := dec.DecodePage(&hdr, data); err != io.EOF {
			t.Fatal("expected EOF")
		}
		if err := dec.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("EncryptedToUnencrypted", func(t *testing.T) {
		pub, priv, _ := ltx.GenerateKeyPair()

		page1Data := bytes.Repeat([]byte{0x81}, 1024)

		var encBuf bytes.Buffer
		enc, _ := ltx.NewEncoder(&encBuf)
		_ = enc.SetEncryption([][]byte{pub})
		_ = enc.EncodeHeader(ltx.Header{
			Version: ltx.Version, PageSize: 1024, Commit: 1,
			MinTXID: 1, MaxTXID: 1, Timestamp: 1000,
		})
		_ = enc.EncodePage(ltx.PageHeader{Pgno: 1}, page1Data)
		enc.SetPostApplyChecksum(ltx.ChecksumFlag | ltx.ChecksumPage(1, page1Data))
		_ = enc.Close()

		// Compact encrypted → unencrypted.
		var output bytes.Buffer
		c, _ := ltx.NewCompactor(&output, []io.Reader{&encBuf})
		c.DecryptionKey = priv
		if err := c.Compact(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Verify output is unencrypted.
		dec := ltx.NewDecoder(&output)
		if err := dec.DecodeHeader(); err != nil {
			t.Fatal(err)
		}
		if dec.Header().Encrypted() {
			t.Fatal("expected unencrypted output")
		}

		var hdr ltx.PageHeader
		data := make([]byte, 1024)
		if err := dec.DecodePage(&hdr, data); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(data, page1Data) {
			t.Fatal("data mismatch")
		}
		if err := dec.DecodePage(&hdr, data); err != io.EOF {
			t.Fatal("expected EOF")
		}
		if err := dec.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("UnencryptedToEncrypted", func(t *testing.T) {
		pub, priv, _ := ltx.GenerateKeyPair()

		page1Data := bytes.Repeat([]byte{0x81}, 1024)

		var buf bytes.Buffer
		enc, _ := ltx.NewEncoder(&buf)
		_ = enc.EncodeHeader(ltx.Header{
			Version: ltx.Version, PageSize: 1024, Commit: 1,
			MinTXID: 1, MaxTXID: 1, Timestamp: 1000,
		})
		_ = enc.EncodePage(ltx.PageHeader{Pgno: 1}, page1Data)
		enc.SetPostApplyChecksum(ltx.ChecksumFlag | ltx.ChecksumPage(1, page1Data))
		_ = enc.Close()

		// Compact unencrypted → encrypted.
		var output bytes.Buffer
		c, _ := ltx.NewCompactor(&output, []io.Reader{&buf})
		c.OutputEncryption = [][]byte{pub}
		if err := c.Compact(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Verify output is encrypted and decryptable.
		dec := ltx.NewDecoder(&output)
		dec.SetDecryptionKey(priv)
		if err := dec.DecodeHeader(); err != nil {
			t.Fatal(err)
		}
		if !dec.Header().Encrypted() {
			t.Fatal("expected encrypted output")
		}

		var hdr ltx.PageHeader
		data := make([]byte, 1024)
		if err := dec.DecodePage(&hdr, data); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(data, page1Data) {
			t.Fatal("data mismatch")
		}
		if err := dec.DecodePage(&hdr, data); err != io.EOF {
			t.Fatal("expected EOF")
		}
		if err := dec.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Status", func(t *testing.T) {
		bufs := make([]bytes.Buffer, 2)
		writeFileSpec(t, &bufs[0], &ltx.FileSpec{
			Header: ltx.Header{Version: ltx.Version, PageSize: 1024, Commit: 3, MinTXID: 1, MaxTXID: 1, Timestamp: 1000},
			Pages: []ltx.PageSpec{
				{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{0x81}, 1024)},
				{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte{0x82}, 1024)},
				{Header: ltx.PageHeader{Pgno: 3}, Data: bytes.Repeat([]byte{0x83}, 1024)},
			},
			Trailer: ltx.Trailer{PostApplyChecksum: 0x8a249272ad9f7dea},
		})
		writeFileSpec(t, &bufs[1], &ltx.FileSpec{
			Header: ltx.Header{Version: ltx.Version, PageSize: 1024, Commit: 5, MinTXID: 2, MaxTXID: 2, Timestamp: 2000, PreApplyChecksum: 0x8a249272ad9f7dea},
			Pages: []ltx.PageSpec{
				{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{0x91}, 1024)},
				{Header: ltx.PageHeader{Pgno: 4}, Data: bytes.Repeat([]byte{0x94}, 1024)},
				{Header: ltx.PageHeader{Pgno: 5}, Data: bytes.Repeat([]byte{0x95}, 1024)},
			},
			Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 2},
		})

		// Compact files together.
		c, err := ltx.NewCompactor(io.Discard, []io.Reader{&bufs[0], &bufs[1]})
		if err != nil {
			t.Fatal(err)
		}

		// Status should be zero before compaction.
		if got := c.Status(); !got.IsZero() {
			t.Fatalf("expected zero status before compaction, got N=%d, Total=%d", got.N, got.Total)
		}
		// Pct should return 0 when Total is 0 (avoid divide by zero).
		if got := c.Status().Pct(); got != 0 {
			t.Fatalf("expected Pct()=0 before compaction, got %f", got)
		}

		if err := c.Compact(context.Background()); err != nil {
			t.Fatal(err)
		}

		// After compaction, Status should reflect progress.
		status := c.Status()
		if got, want := status.Total, uint32(5); got != want {
			t.Fatalf("Status().Total=%d, want %d", got, want)
		}
		// N should be the last page that was written (page 5).
		if got, want := status.N, uint32(5); got != want {
			t.Fatalf("Status().N=%d, want %d", got, want)
		}
		// Pct should return 1.0 when complete.
		if got, want := status.Pct(), 1.0; got != want {
			t.Fatalf("Status().Pct()=%f, want %f", got, want)
		}
	})
}
