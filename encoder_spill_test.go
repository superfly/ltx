package ltx_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/superfly/ltx"
)

var errInjected = errors.New("injected write failure")

func encodeWithSpill(t *testing.T, spillDir string, threshold int, pgnos []uint32, commit uint32, closeIt bool) (*ltx.Encoder, []byte) {
	t.Helper()
	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if spillDir != "" {
		enc.SetSpillDir(spillDir)
	}
	if threshold > 0 {
		enc.SetSpillThreshold(threshold)
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
			t.Fatalf("pgno %d: %v", pgno, err)
		}
	}
	enc.SetPostApplyChecksum(ltx.ChecksumFlag | 2)
	if closeIt {
		if err := enc.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return enc, buf.Bytes()
}

func dirEntries(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func requireDirectoryRemovalFailure(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not enforce Unix directory write permissions")
	}
	if os.Getuid() == 0 {
		t.Skip("root can remove files from read-only directories")
	}
}

func TestEncoder_SpillFile(t *testing.T) {
	pgnos := make([]uint32, 0, 1000)
	for pgno := uint32(3); pgno <= 3000; pgno += 3 {
		pgnos = append(pgnos, pgno)
	}

	t.Run("OutputIdenticalWithAndWithoutSpill", func(t *testing.T) {
		dir := t.TempDir()
		_, plain := encodeWithSpill(t, "", 0, pgnos, 4000, true)
		_, spilled := encodeWithSpill(t, dir, 8, pgnos, 4000, true)
		if !bytes.Equal(plain, spilled) {
			t.Fatal("spilled output differs from in-memory output")
		}
		if names := dirEntries(t, dir); len(names) != 0 {
			t.Fatalf("spill file left behind after Close: %v", names)
		}
		dec := ltx.NewDecoder(bytes.NewReader(spilled))
		if err := dec.Verify(); err != nil {
			t.Fatal(err)
		}
		if got, want := len(dec.PageIndex()), len(pgnos); got != want {
			t.Fatalf("decoded index len=%d, want %d", got, want)
		}
	})

	t.Run("BelowThresholdNeverTouchesDisk", func(t *testing.T) {
		dir := t.TempDir()
		encodeWithSpill(t, dir, len(pgnos)+1, pgnos, 4000, true)
		if names := dirEntries(t, dir); len(names) != 0 {
			t.Fatalf("unexpected spill file: %v", names)
		}
	})

	t.Run("NoSpillDirNeverSpills", func(t *testing.T) {
		enc, _ := encodeWithSpill(t, "", 8, pgnos, 4000, true)
		if enc.Spilled() {
			t.Fatal("encoder spilled without a spill dir")
		}
	})

	t.Run("CleanupWithoutClose", func(t *testing.T) {
		dir := t.TempDir()
		enc, _ := encodeWithSpill(t, dir, 8, pgnos, 4000, false)
		if !enc.Spilled() {
			t.Fatal("expected encoder to have spilled")
		}
		if names := dirEntries(t, dir); len(names) != 1 {
			t.Fatalf("expected one spill file while open, got %v", names)
		}
		if err := enc.Cleanup(); err != nil {
			t.Fatal(err)
		}
		if err := enc.Cleanup(); err != nil { // idempotent
			t.Fatal(err)
		}
		if names := dirEntries(t, dir); len(names) != 0 {
			t.Fatalf("spill file left behind after Cleanup: %v", names)
		}
	})

	t.Run("UnwritableSpillDir", func(t *testing.T) {
		notADir := filepath.Join(t.TempDir(), "file")
		if err := os.WriteFile(notADir, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		enc, err := ltx.NewEncoder(&buf)
		if err != nil {
			t.Fatal(err)
		}
		enc.SetSpillDir(notADir)
		enc.SetSpillThreshold(2)
		if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, PageSize: 512, Commit: 4, MinTXID: 2, MaxTXID: 2, Timestamp: 1000, PreApplyChecksum: ltx.ChecksumFlag | 1}); err != nil {
			t.Fatal(err)
		}
		page := make([]byte, 512)
		if err := enc.EncodePage(ltx.PageHeader{Pgno: 1}, page); err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: 2}, page); err == nil {
			t.Fatal("expected spill error for unwritable spill dir")
		}
	})
}

func TestEncoder_SpillAcrossChunkBoundaries(t *testing.T) {
	const n = 2*65536 + 259
	pgnos := make([]uint32, 0, n)
	for pgno := uint32(2); pgno <= n+1; pgno++ {
		pgnos = append(pgnos, pgno)
	}
	dir := t.TempDir()
	_, plain := encodeWithSpill(t, "", 0, pgnos, n+1, true)
	_, spilled := encodeWithSpill(t, dir, 70000, pgnos, n+1, true)
	if !bytes.Equal(plain, spilled) {
		t.Fatal("spilled output differs from in-memory output across chunk boundaries")
	}
}

// failingWriter fails every write once failAfter bytes have been accepted.
type failingWriter struct {
	buf       bytes.Buffer
	failAfter int
	failed    bool
	err       error
}

func (w *failingWriter) Write(p []byte) (int, error) {
	if w.failed || w.buf.Len()+len(p) > w.failAfter {
		w.failed = true
		return 0, w.err
	}
	return w.buf.Write(p)
}

func newSpilledEncoder(t *testing.T, w interface{ Write([]byte) (int, error) }, dir string, pages int) *ltx.Encoder {
	t.Helper()
	enc, err := ltx.NewEncoder(w)
	if err != nil {
		t.Fatal(err)
	}
	enc.SetSpillDir(dir)
	enc.SetSpillThreshold(2)
	if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, PageSize: 512, Commit: uint32(pages + 1), MinTXID: 2, MaxTXID: 2, Timestamp: 1000, PreApplyChecksum: ltx.ChecksumFlag | 1}); err != nil {
		t.Fatal(err)
	}
	page := make([]byte, 512)
	for i := 0; i < pages; i++ {
		if err := enc.EncodePage(ltx.PageHeader{Pgno: uint32(i + 2)}, page); err != nil {
			t.Fatal(err)
		}
	}
	enc.SetPostApplyChecksum(ltx.ChecksumFlag | 2)
	if !enc.Spilled() {
		t.Fatal("expected encoder to have spilled")
	}
	return enc
}

func TestEncoder_SpillLifecycle(t *testing.T) {
	t.Run("CleanupBeforeCloseAborts", func(t *testing.T) {
		dir := t.TempDir()
		var buf bytes.Buffer
		enc := newSpilledEncoder(t, &buf, dir, 4)
		if err := enc.Cleanup(); err != nil {
			t.Fatal(err)
		}
		if err := enc.Close(); err != ltx.ErrEncoderAborted {
			t.Fatalf("Close()=%v, want ErrEncoderAborted", err)
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: 9}, make([]byte, 512)); err != ltx.ErrEncoderAborted {
			t.Fatalf("EncodePage()=%v, want ErrEncoderAborted", err)
		}
		if names := dirEntries(t, dir); len(names) != 0 {
			t.Fatalf("spill file left behind: %v", names)
		}
	})

	t.Run("FirstCloseWriteFailureRemovesSpill", func(t *testing.T) {
		dir := t.TempDir()
		w := &failingWriter{err: errInjected, failAfter: 1 << 30}
		enc, err := ltx.NewEncoder(w)
		if err != nil {
			t.Fatal(err)
		}
		enc.SetSpillDir(dir)
		enc.SetSpillThreshold(2)
		if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, PageSize: 512, Commit: 8, MinTXID: 2, MaxTXID: 2, Timestamp: 1000, PreApplyChecksum: ltx.ChecksumFlag | 1}); err != nil {
			t.Fatal(err)
		}
		page := make([]byte, 512)
		for pgno := uint32(2); pgno <= 5; pgno++ {
			if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, page); err != nil {
				t.Fatal(err)
			}
		}
		w.failAfter = w.buf.Len() // the end-of-pages marker is the first write to fail
		enc.SetPostApplyChecksum(ltx.ChecksumFlag | 2)
		if err := enc.Close(); err == nil {
			t.Fatal("expected Close to fail")
		}
		if names := dirEntries(t, dir); len(names) != 0 {
			t.Fatalf("spill file leaked after failed Close: %v", names)
		}
		if err := enc.Close(); err != ltx.ErrEncoderAborted {
			t.Fatalf("second Close()=%v, want ErrEncoderAborted", err)
		}
	})

	t.Run("FailedSpilledCloseIsNotRetryable", func(t *testing.T) {
		dir := t.TempDir()
		w := &failingWriter{err: errInjected, failAfter: 1 << 30}
		enc, err := ltx.NewEncoder(w)
		if err != nil {
			t.Fatal(err)
		}
		enc.SetSpillDir(dir)
		enc.SetSpillThreshold(2)
		if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, PageSize: 512, Commit: 8, MinTXID: 2, MaxTXID: 2, Timestamp: 1000, PreApplyChecksum: ltx.ChecksumFlag | 1}); err != nil {
			t.Fatal(err)
		}
		page := make([]byte, 512)
		for pgno := uint32(2); pgno <= 5; pgno++ {
			if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, page); err != nil {
				t.Fatal(err)
			}
		}
		w.failAfter = w.buf.Len() + ltx.PageHeaderSize + 1 // marker succeeds, index copy fails
		enc.SetPostApplyChecksum(ltx.ChecksumFlag | 2)
		if err := enc.Close(); err == nil {
			t.Fatal("expected Close to fail during index copy")
		}
		w.failed, w.failAfter = false, 1<<30
		if err := enc.Close(); err != ltx.ErrEncoderAborted {
			t.Fatalf("retried Close()=%v, want ErrEncoderAborted", err)
		}
		if names := dirEntries(t, dir); len(names) != 0 {
			t.Fatalf("spill file leaked: %v", names)
		}
	})

	t.Run("CleanupReportsRemovalFailure", func(t *testing.T) {
		requireDirectoryRemovalFailure(t)
		dir := t.TempDir()
		var buf bytes.Buffer
		enc := newSpilledEncoder(t, &buf, dir, 4)
		if err := os.Chmod(dir, 0o500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
		if err := enc.Cleanup(); err == nil {
			t.Fatal("expected Cleanup to report the removal failure")
		}
		if err := os.Chmod(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := enc.Cleanup(); err != nil {
			t.Fatalf("retried Cleanup()=%v", err)
		}
		if names := dirEntries(t, dir); len(names) != 0 {
			t.Fatalf("spill file left behind after retry: %v", names)
		}
	})

	t.Run("CloseRetriesFailedSpillRemoval", func(t *testing.T) {
		requireDirectoryRemovalFailure(t)
		dir := t.TempDir()
		var buf bytes.Buffer
		enc := newSpilledEncoder(t, &buf, dir, 4)
		if err := os.Chmod(dir, 0o500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
		if err := enc.Close(); err == nil {
			t.Fatal("expected Close to report the spill removal failure")
		}
		// The file itself is complete and must verify.
		if err := ltx.NewDecoder(bytes.NewReader(buf.Bytes())).Verify(); err != nil {
			t.Fatal(err)
		}
		if err := enc.Close(); err == nil {
			t.Fatal("expected the second Close to keep reporting the pending removal")
		}
		if err := os.Chmod(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := enc.Close(); err != nil {
			t.Fatalf("Close() after fixing permissions = %v", err)
		}
		if names := dirEntries(t, dir); len(names) != 0 {
			t.Fatalf("spill file left behind: %v", names)
		}
		if len(buf.Bytes()) == 0 || enc.Spilled() {
			t.Fatal("expected spill reference cleared after successful removal")
		}
	})

	t.Run("FailedCloseRetriesFailedSpillRemoval", func(t *testing.T) {
		requireDirectoryRemovalFailure(t)
		dir := t.TempDir()
		w := &failingWriter{err: errInjected, failAfter: 1 << 30}
		enc := newSpilledEncoder(t, w, dir, 4)
		w.failAfter = w.buf.Len()
		if err := os.Chmod(dir, 0o500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
		err := enc.Close()
		if !errors.Is(err, errInjected) {
			t.Fatalf("Close()=%v, want injected write failure", err)
		}
		var pathErr *os.PathError
		if !errors.As(err, &pathErr) {
			t.Fatalf("Close()=%v, want spill cleanup failure", err)
		}
		if names := dirEntries(t, dir); len(names) != 1 || !enc.Spilled() {
			t.Fatalf("expected pending spill after failed removal: %v", names)
		}
		if err := os.Chmod(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := enc.Close(); err != ltx.ErrEncoderAborted {
			t.Fatalf("retried Close()=%v, want ErrEncoderAborted", err)
		}
		if names := dirEntries(t, dir); len(names) != 0 || enc.Spilled() {
			t.Fatalf("spill file left behind after retry: %v", names)
		}
	})

	t.Run("NoChecksumAndDeletionOutputMatch", func(t *testing.T) {
		encode := func(dir string, commit uint32, pages int, flags uint32) []byte {
			var buf bytes.Buffer
			enc, err := ltx.NewEncoder(&buf)
			if err != nil {
				t.Fatal(err)
			}
			if dir != "" {
				enc.SetSpillDir(dir)
				enc.SetSpillThreshold(1)
			}
			hdr := ltx.Header{Version: ltx.Version, Flags: flags, PageSize: 512, Commit: commit, MinTXID: 1, MaxTXID: 1, Timestamp: 1000}
			if err := enc.EncodeHeader(hdr); err != nil {
				t.Fatal(err)
			}
			page := make([]byte, 512)
			for i := 0; i < pages; i++ {
				if err := enc.EncodePage(ltx.PageHeader{Pgno: uint32(i + 1)}, page); err != nil {
					t.Fatal(err)
				}
			}
			switch {
			case commit == 0:
				enc.SetPostApplyChecksum(ltx.ChecksumFlag) // deletion files carry the empty checksum
			case flags&ltx.HeaderFlagNoChecksum == 0:
				enc.SetPostApplyChecksum(ltx.ChecksumFlag | 2)
			}
			if err := enc.Close(); err != nil {
				t.Fatal(err)
			}
			return buf.Bytes()
		}
		if a, b := encode("", 3, 3, ltx.HeaderFlagNoChecksum), encode(t.TempDir(), 3, 3, ltx.HeaderFlagNoChecksum); !bytes.Equal(a, b) {
			t.Fatal("NoChecksum output differs with spill")
		}
		if a, b := encode("", 0, 0, 0), encode(t.TempDir(), 0, 0, 0); !bytes.Equal(a, b) {
			t.Fatal("deletion output differs with spill")
		}
	})
}

func TestEncoder_PageWriteFailureAborts(t *testing.T) {
	w := &failingWriter{err: errInjected, failAfter: 1 << 30}
	enc, err := ltx.NewEncoder(w)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, PageSize: 512, Commit: 4, MinTXID: 2, MaxTXID: 2, Timestamp: 1000, PreApplyChecksum: ltx.ChecksumFlag | 1}); err != nil {
		t.Fatal(err)
	}
	page := make([]byte, 512)
	if err := enc.EncodePage(ltx.PageHeader{Pgno: 1}, page); err != nil {
		t.Fatal(err)
	}
	w.failAfter = w.buf.Len() + ltx.PageHeaderSize + 2 // fail inside the second page's size field
	if err := enc.EncodePage(ltx.PageHeader{Pgno: 2}, page); err == nil {
		t.Fatal("expected page write to fail")
	}
	w.failed, w.failAfter = false, 1<<30
	enc.SetPostApplyChecksum(ltx.ChecksumFlag | 2)
	if err := enc.Close(); err != ltx.ErrEncoderAborted {
		t.Fatalf("Close() after failed page write = %v, want ErrEncoderAborted", err)
	}
}

func TestEncoder_HeaderWriteFailureAborts(t *testing.T) {
	w := &failingWriter{err: errInjected, failAfter: 10}
	enc, err := ltx.NewEncoder(w)
	if err != nil {
		t.Fatal(err)
	}
	hdr := ltx.Header{Version: ltx.Version, PageSize: 512, Commit: 4, MinTXID: 2, MaxTXID: 2, Timestamp: 1000, PreApplyChecksum: ltx.ChecksumFlag | 1}
	if err := enc.EncodeHeader(hdr); err == nil {
		t.Fatal("expected header write to fail")
	}
	w.failed, w.failAfter = false, 1<<30
	if err := enc.EncodeHeader(hdr); err != ltx.ErrEncoderAborted {
		t.Fatalf("retried EncodeHeader()=%v, want ErrEncoderAborted", err)
	}
}
