package ltx

import (
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestChecksumPages(t *testing.T) {
	// files divisible into pages
	testChecksumPages(t, 1024*4, 4, 1024, 1)
	testChecksumPages(t, 1024*4, 4, 1024, 2)
	testChecksumPages(t, 1024*4, 4, 1024, 3)
	testChecksumPages(t, 1024*4, 4, 1024, 4)

	// short pages
	testChecksumPages(t, 1024*3+100, 4, 1024, 1)
	testChecksumPages(t, 1024*3+100, 4, 1024, 2)
	testChecksumPages(t, 1024*3+100, 4, 1024, 3)
	testChecksumPages(t, 1024*3+100, 4, 1024, 4)

	// empty files
	testChecksumPages(t, 0, 4, 1024, 1)
	testChecksumPages(t, 0, 4, 1024, 2)
	testChecksumPages(t, 0, 4, 1024, 3)
	testChecksumPages(t, 0, 4, 1024, 4)
}

func TestChecksumPages_LockPage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	const pageSize = uint32(4096)
	lockPgno := LockPgno(pageSize)
	nPages := lockPgno + 100
	path := filepath.Join(t.TempDir(), "test.db")

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := f.Truncate(int64(nPages) * int64(pageSize)); err != nil {
		t.Fatal(err)
	}

	checksums := make([]Checksum, nPages)
	checksums[lockPgno-1] = ChecksumFlag | 1
	lastPage, err := ChecksumPages(path, pageSize, nPages, 0, checksums)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := lastPage, nPages; got != want {
		t.Fatalf("last page=%d, want %d", got, want)
	}
	if got := checksums[lockPgno-1]; got != 0 {
		t.Fatalf("lock page checksum=%s, want 0", got)
	}

	zeroPage := make([]byte, pageSize)
	for _, pgno := range []uint32{lockPgno - 1, lockPgno + 1} {
		if got, want := checksums[pgno-1], ChecksumPage(pgno, zeroPage); got != want {
			t.Fatalf("page %d checksum=%s, want %s", pgno, got, want)
		}
	}
}

func testChecksumPages(t *testing.T, fileSize, nPages, pageSize, nWorkers uint32) {
	t.Run(fmt.Sprintf("fileSize=%d,nPages=%d,pageSize=%d,nWorkers=%d", fileSize, nPages, pageSize, nWorkers), func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.db")
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if _, err := io.CopyN(f, rand.Reader, int64(fileSize)); err != nil {
			t.Fatal(err)
		}

		legacyCS := make([]Checksum, nPages)
		legacyLastPage, legacyErr := legacyChecksumPages(path, pageSize, nPages, legacyCS)
		newCS := make([]Checksum, nPages)
		newLastPage, newErr := ChecksumPages(path, pageSize, nPages, nWorkers, newCS)

		if legacyErr != newErr {
			t.Fatalf("legacy error: %v, new error: %v", legacyErr, newErr)
		}
		if legacyLastPage != newLastPage {
			t.Fatalf("legacy last page: %d, new last page: %d", legacyLastPage, newLastPage)
		}
		if len(legacyCS) != len(newCS) {
			t.Fatalf("legacy checksums: %d, new checksums: %d", len(legacyCS), len(newCS))
		}
		for i := range legacyCS {
			if legacyCS[i] != newCS[i] {
				t.Fatalf("mismatch at index %d: legacy: %v, new: %v", i, legacyCS[i], newCS[i])
			}
		}
	})
}

// logic copied from litefs repo
func legacyChecksumPages(dbPath string, pageSize, nPages uint32, checksums []Checksum) (uint32, error) {
	f, err := os.Open(dbPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	buf := make([]byte, pageSize)

	for pgno := uint32(1); pgno <= nPages; pgno++ {
		offset := int64(pgno-1) * int64(pageSize)
		if _, err := readFullAt(f, buf, offset); err != nil {
			return pgno - 1, err
		}

		checksums[pgno-1] = ChecksumPage(pgno, buf)
	}

	return nPages, nil
}

// copied from litefs/internal
func readFullAt(r io.ReaderAt, buf []byte, off int64) (n int, err error) {
	for n < len(buf) && err == nil {
		var nn int
		nn, err = r.ReadAt(buf[n:], off+int64(n))
		n += nn
	}
	if n >= len(buf) {
		return n, nil
	} else if n > 0 && err == io.EOF {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}
