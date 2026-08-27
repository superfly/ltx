package ltx_test

import (
	"bytes"
	"encoding/binary"
	"hash/crc64"
	"math"
	"testing"

	"github.com/superfly/ltx"
)

func encodeIndexedFile(t *testing.T, pgnos []uint32, commit uint32) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.EncodeHeader(ltx.Header{
		Version:          ltx.Version,
		PageSize:         512,
		Commit:           commit,
		MinTXID:          2,
		MaxTXID:          2,
		Timestamp:        1000,
		PreApplyChecksum: ltx.ChecksumFlag | 1,
	}); err != nil {
		t.Fatal(err)
	}
	page := make([]byte, 512)
	for _, pgno := range pgnos {
		page[0] = byte(pgno)
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, page); err != nil {
			t.Fatal(err)
		}
	}
	enc.SetPostApplyChecksum(ltx.ChecksumFlag | 2)
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestDecoder_PageIndexRetention(t *testing.T) {
	data := encodeIndexedFile(t, []uint32{2, 5, 9}, 16)

	t.Run("RetainedByDefault", func(t *testing.T) {
		dec := ltx.NewDecoder(bytes.NewReader(data))
		if err := dec.Verify(); err != nil {
			t.Fatal(err)
		}
		index := dec.PageIndex()
		if got, want := len(index), 3; got != want {
			t.Fatalf("len(index)=%d, want %d", got, want)
		}
		if index[5].Offset <= index[2].Offset || index[9].Offset <= index[5].Offset {
			t.Fatalf("offsets not ascending: %+v", index)
		}
		if index[2].MinTXID != 2 || index[2].MaxTXID != 2 {
			t.Fatalf("txid range not populated: %+v", index[2])
		}
	})

	t.Run("Discarded", func(t *testing.T) {
		dec := ltx.NewDecoder(bytes.NewReader(data))
		dec.SetRetainPageIndex(false)
		if err := dec.Verify(); err != nil {
			t.Fatal(err)
		}
		if index := dec.PageIndex(); index != nil {
			t.Fatalf("expected nil page index, got %d entries", len(index))
		}
		if dec.Trailer().FileChecksum == 0 {
			t.Fatal("trailer not decoded")
		}
	})

	t.Run("SameChecksumEitherWay", func(t *testing.T) {
		a := ltx.NewDecoder(bytes.NewReader(data))
		b := ltx.NewDecoder(bytes.NewReader(data))
		b.SetRetainPageIndex(false)
		if err := a.Verify(); err != nil {
			t.Fatal(err)
		}
		if err := b.Verify(); err != nil {
			t.Fatal(err)
		}
		if a.Trailer() != b.Trailer() {
			t.Fatalf("trailers differ: %+v vs %+v", a.Trailer(), b.Trailer())
		}
		if a.N() != b.N() {
			t.Fatalf("bytes read differ: %d vs %d", a.N(), b.N())
		}
	})
}

func TestDecoder_PageIndexStreamValidation(t *testing.T) {
	data := encodeIndexedFile(t, []uint32{2, 5, 9}, 16)
	sizeOffset := len(data) - ltx.TrailerSize - 8

	t.Run("SizeMismatch", func(t *testing.T) {
		corrupt := bytes.Clone(data)
		size := binary.BigEndian.Uint64(corrupt[sizeOffset:])
		binary.BigEndian.PutUint64(corrupt[sizeOffset:], size+1)
		dec := ltx.NewDecoder(bytes.NewReader(corrupt))
		if err := dec.Verify(); err == nil {
			t.Fatal("expected error for page index size mismatch")
		}
	})

	t.Run("TrailingGarbage", func(t *testing.T) {
		corrupt := append(bytes.Clone(data), 0xFF)
		dec := ltx.NewDecoder(bytes.NewReader(corrupt))
		if err := dec.Verify(); err == nil {
			t.Fatal("expected error for data after trailer")
		}
	})

	t.Run("Truncated", func(t *testing.T) {
		dec := ltx.NewDecoder(bytes.NewReader(data[:len(data)-4]))
		if err := dec.Verify(); err == nil {
			t.Fatal("expected error for truncated trailer")
		}
	})
}

// indexRecords returns the (pgno, offset, size) tuples of data's page index.
func indexRecords(t *testing.T, data []byte) [][3]uint64 {
	t.Helper()
	sizeStart := len(data) - ltx.TrailerSize - 8
	indexSize := binary.BigEndian.Uint64(data[sizeStart:])
	r := bytes.NewReader(data[sizeStart-int(indexSize) : sizeStart])
	var records [][3]uint64
	for {
		pgno, err := binary.ReadUvarint(r)
		if err != nil {
			t.Fatal(err)
		}
		if pgno == 0 {
			return records
		}
		offset, err := binary.ReadUvarint(r)
		if err != nil {
			t.Fatal(err)
		}
		size, err := binary.ReadUvarint(r)
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, [3]uint64{pgno, offset, size})
	}
}

// withIndex replaces data's page index with records (plus a correct size
// field) and recomputes the file checksum from the known page plaintext, so
// the result is checksum-valid and only structural validation can reject it.
func withIndex(t *testing.T, data []byte, records [][3]uint64, plaintext func(pgno uint32) []byte) []byte {
	t.Helper()
	trailerStart := len(data) - ltx.TrailerSize
	sizeStart := trailerStart - 8
	indexStart := sizeStart - int(binary.BigEndian.Uint64(data[sizeStart:]))

	var index []byte
	for _, r := range records {
		index = binary.AppendUvarint(index, r[0])
		index = binary.AppendUvarint(index, r[1])
		index = binary.AppendUvarint(index, r[2])
	}
	index = binary.AppendUvarint(index, 0)
	index = binary.BigEndian.AppendUint64(index, uint64(len(index)))

	// Hash exactly what the encoder hashes: header, then per page the page
	// header, the 4-byte compressed size, and the uncompressed page; then
	// the end-of-pages marker, the index, and the trailer's post-apply field.
	h := crc64.New(crc64.MakeTable(crc64.ISO))
	pos := 0
	h.Write(data[:ltx.HeaderSize])
	pos = ltx.HeaderSize
	for {
		hdrBytes := data[pos : pos+ltx.PageHeaderSize]
		var hdr ltx.PageHeader
		if err := hdr.UnmarshalBinary(hdrBytes); err != nil {
			t.Fatal(err)
		}
		h.Write(hdrBytes)
		pos += ltx.PageHeaderSize
		if hdr.Pgno == 0 {
			break
		}
		sizeBytes := data[pos : pos+4]
		h.Write(sizeBytes)
		n := int(binary.BigEndian.Uint32(sizeBytes))
		pos += 4 + n
		h.Write(plaintext(hdr.Pgno))
	}
	if pos != indexStart {
		t.Fatalf("page block ends at %d, index starts at %d", pos, indexStart)
	}
	h.Write(index)
	h.Write(data[trailerStart : trailerStart+ltx.TrailerChecksumOffset])

	out := append([]byte{}, data[:indexStart]...)
	out = append(out, index...)
	out = append(out, data[trailerStart:]...)
	binary.BigEndian.PutUint64(out[len(out)-ltx.ChecksumSize:], uint64(ltx.ChecksumFlag|ltx.Checksum(h.Sum64())))
	return out
}

func TestDecoder_PageIndexStructure(t *testing.T) {
	plaintext := func(pgno uint32) []byte {
		b := make([]byte, 512)
		b[0] = byte(pgno)
		return b
	}
	data := encodeIndexedFile(t, []uint32{2, 5, 9}, 16)
	valid := indexRecords(t, data)
	if len(valid) != 3 {
		t.Fatalf("expected 3 records, got %d", len(valid))
	}

	t.Run("RebuiltIndexStillVerifies", func(t *testing.T) {
		if err := ltx.NewDecoder(bytes.NewReader(withIndex(t, data, valid, plaintext))).Verify(); err != nil {
			t.Fatalf("checksum reconstruction is wrong: %v", err)
		}
	})

	t.Run("PhantomEntryRejected", func(t *testing.T) {
		records := append(append([][3]uint64{}, valid...), [3]uint64{12, valid[2][1] + valid[2][2], valid[2][2]})
		if err := ltx.NewDecoder(bytes.NewReader(withIndex(t, data, records, plaintext))).Verify(); err == nil {
			t.Fatal("expected error for an index entry without a decoded page")
		}
	})

	t.Run("MissingEntryRejected", func(t *testing.T) {
		if err := ltx.NewDecoder(bytes.NewReader(withIndex(t, data, valid[:2], plaintext))).Verify(); err == nil {
			t.Fatal("expected error for a decoded page without an index entry")
		}
	})

	t.Run("OverflowRejected", func(t *testing.T) {
		records := append([][3]uint64{}, valid...)
		records[2][1] = math.MaxUint64
		if err := ltx.NewDecoder(bytes.NewReader(withIndex(t, data, records, plaintext))).Verify(); err == nil {
			t.Fatal("expected error for an offset above MaxInt64")
		}
	})

	t.Run("OverlapRejected", func(t *testing.T) {
		records := append([][3]uint64{}, valid...)
		records[1][1] = records[0][1] // second frame starts inside the first
		if err := ltx.NewDecoder(bytes.NewReader(withIndex(t, data, records, plaintext))).Verify(); err == nil {
			t.Fatal("expected error for overlapping frames")
		}
	})

	t.Run("RenamedEntryRejected", func(t *testing.T) {
		records := append([][3]uint64{}, valid...)
		records[2][0] = 10 // same count and valid frames, but page 9 was decoded, not 10
		if err := ltx.NewDecoder(bytes.NewReader(withIndex(t, data, records, plaintext))).Verify(); err == nil {
			t.Fatal("expected error for an index entry naming a page that was not decoded")
		}
	})

	t.Run("BeyondCommitRejected", func(t *testing.T) {
		records := append([][3]uint64{}, valid...)
		records[2][0] = 17 // commit is 16
		if err := ltx.NewDecoder(bytes.NewReader(withIndex(t, data, records, plaintext))).Verify(); err == nil {
			t.Fatal("expected error for a page number beyond commit")
		}
	})

	t.Run("DeletionFile", func(t *testing.T) {
		var buf bytes.Buffer
		enc, err := ltx.NewEncoder(&buf)
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, PageSize: 512, Commit: 0, MinTXID: 1, MaxTXID: 1, Timestamp: 1000}); err != nil {
			t.Fatal(err)
		}
		enc.SetPostApplyChecksum(ltx.ChecksumFlag)
		if err := enc.Close(); err != nil {
			t.Fatal(err)
		}
		for _, retain := range []bool{true, false} {
			dec := ltx.NewDecoder(bytes.NewReader(buf.Bytes()))
			dec.SetRetainPageIndex(retain)
			if err := dec.Verify(); err != nil {
				t.Fatalf("retain=%v: %v", retain, err)
			}
		}
	})

	t.Run("NoChecksumSnapshot", func(t *testing.T) {
		var buf bytes.Buffer
		enc, err := ltx.NewEncoder(&buf)
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, Flags: ltx.HeaderFlagNoChecksum, PageSize: 512, Commit: 2, MinTXID: 1, MaxTXID: 1, Timestamp: 1000}); err != nil {
			t.Fatal(err)
		}
		for pgno := uint32(1); pgno <= 2; pgno++ {
			if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, make([]byte, 512)); err != nil {
				t.Fatal(err)
			}
		}
		if err := enc.Close(); err != nil {
			t.Fatal(err)
		}
		dec := ltx.NewDecoder(bytes.NewReader(buf.Bytes()))
		dec.SetRetainPageIndex(false)
		if err := dec.Verify(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("ExportedDecodePageIndexValidates", func(t *testing.T) {
		var index []byte
		for _, r := range [][3]uint64{{5, 100, 40}, {3, 140, 40}} { // out of order
			index = binary.AppendUvarint(index, r[0])
			index = binary.AppendUvarint(index, r[1])
			index = binary.AppendUvarint(index, r[2])
		}
		index = binary.AppendUvarint(index, 0)
		index = binary.BigEndian.AppendUint64(index, uint64(len(index)))
		if _, err := ltx.DecodePageIndex(bytes.NewReader(index), 0, 1, 1); err == nil {
			t.Fatal("expected DecodePageIndex to reject out-of-order entries")
		}
	})
}
