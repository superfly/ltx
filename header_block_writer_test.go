package ltx_test

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/superfly/ltx"
	"github.com/superfly/ltx/mock"
)

func TestHeaderBlockWriter(t *testing.T) {
	t.Run("PageDataOnly", func(t *testing.T) {
		rnd := rand.New(rand.NewSource(0))
		page0 := make([]byte, 4096)
		rnd.Read(page0)
		page1 := make([]byte, 4096)
		rnd.Read(page1)

		w := ltx.NewHeaderBlockWriter(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err := w.WriteHeader(ltx.Header{
			Version:      1,
			PageSize:     4096,
			PageN:        2,
			Commit:       3,
			DBID:         4,
			MinTXID:      5,
			MaxTXID:      6,
			Timestamp:    2000,
			PreChecksum:  ltx.PageChecksumFlag | 5,
			PostChecksum: ltx.PageChecksumFlag | 6,
		}); err != nil {
			t.Fatal(err)
		}

		// Write page headers.
		if err := w.WritePageHeader(ltx.PageHeader{Pgno: 1}); err != nil {
			t.Fatal(err)
		} else if err := w.WritePageHeader(ltx.PageHeader{Pgno: 2}); err != nil {
			t.Fatal(err)
		}

		// Flush checksum to header.
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		// Double close should be a no-op.
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("EventAndPageData", func(t *testing.T) {
		// Generate pseudorandom page & event data.
		rnd := rand.New(rand.NewSource(0))
		page0 := make([]byte, 1024)
		rnd.Read(page0)
		page1 := make([]byte, 1024)
		rnd.Read(page1)
		eventData := make([]byte, 60)
		rnd.Read(eventData)

		w := ltx.NewHeaderBlockWriter(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err := w.WriteHeader(ltx.Header{
			Version:       1,
			PageSize:      1024,
			EventN:        1,
			PageN:         2,
			Commit:        4000,
			EventDataSize: 60,
			DBID:          1,
			MinTXID:       2,
			MaxTXID:       2,
			Timestamp:     1000,
			PreChecksum:   ltx.PageChecksumFlag | 2,
			PostChecksum:  ltx.PageChecksumFlag | 2,
		}); err != nil {
			t.Fatal(err)
		}

		// Write page headers.
		if err := w.WritePageHeader(ltx.PageHeader{
			Pgno:  2000,
			Nonce: [12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7},
			Tag:   [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8},
		}); err != nil {
			t.Fatal(err)
		}

		if err := w.WritePageHeader(ltx.PageHeader{
			Pgno:  3000,
			Nonce: [12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9},
			Tag:   [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10},
		}); err != nil {
			t.Fatal(err)
		}

		// Write event header.
		if err := w.WriteEventHeader(ltx.EventHeader{
			Size:  60,
			Nonce: [12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11},
			Tag:   [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12},
		}); err != nil {
			t.Fatal(err)
		}

		// Write event data.
		if _, err := w.Write(eventData); err != nil {
			t.Fatal(err)
		}

		// Flush checksum to header.
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("ErrNoPageData", func(t *testing.T) {
		w := ltx.NewHeaderBlockWriter(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err := w.WriteHeader(ltx.Header{
			Version:      1,
			PageSize:     1024,
			PageN:        0,
			Commit:       3,
			MinTXID:      1,
			MaxTXID:      1,
			PostChecksum: ltx.PageChecksumFlag | 1,
		}); err == nil || err.Error() != `page count required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestHeaderBlockWriter_Close(t *testing.T) {
	t.Run("ErrInvalidState", func(t *testing.T) {
		w := ltx.NewHeaderBlockWriter(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err := w.Close(); err == nil || err.Error() != `cannot close, expected header` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrHeaderSeek", func(t *testing.T) {
		ws := &mock.WriteSeeker{
			WriteFunc: func(p []byte) (n int, err error) { return len(p), nil },
			SeekFunc: func(offset int64, whence int) (int64, error) {
				if offset == 0 {
					return 0, fmt.Errorf("marker")
				}
				return 0, nil
			},
		}

		w := ltx.NewHeaderBlockWriter(ws)
		if err := w.WriteHeader(ltx.Header{Version: 1, PageSize: 1024, PageN: 1, Commit: 1, DBID: 1, MinTXID: 1, MaxTXID: 1, PostChecksum: ltx.PageChecksumFlag | 1}); err != nil {
			t.Fatal(err)
		} else if err := w.WritePageHeader(ltx.PageHeader{Pgno: 1}); err != nil {
			t.Fatal(err)
		}

		if err := w.Close(); err == nil || err.Error() != `header seek: marker` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrWriteChecksum", func(t *testing.T) {
		var seeked bool
		ws := &mock.WriteSeeker{
			WriteFunc: func(p []byte) (n int, err error) {
				if seeked {
					return 0, fmt.Errorf("marker")
				}
				return len(p), nil
			},
			SeekFunc: func(offset int64, whence int) (int64, error) {
				if offset == 0 {
					seeked = true // next write will be header rewrite
				}
				return 0, nil
			},
		}

		w := ltx.NewHeaderBlockWriter(ws)
		if err := w.WriteHeader(ltx.Header{Version: 1, PageSize: 1024, PageN: 1, Commit: 1, DBID: 1, MinTXID: 1, MaxTXID: 1, PostChecksum: ltx.PageChecksumFlag | 1}); err != nil {
			t.Fatal(err)
		} else if err := w.WritePageHeader(ltx.PageHeader{Pgno: 1}); err != nil {
			t.Fatal(err)
		}

		if err := w.Close(); err == nil || err.Error() != `rewrite header: marker` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrClosed", func(t *testing.T) {
		w := ltx.NewHeaderBlockWriter(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err := w.WriteHeader(ltx.Header{Version: 1, PageSize: 1024, PageN: 1, Commit: 1, DBID: 1, MinTXID: 1, MaxTXID: 1, PostChecksum: ltx.PageChecksumFlag | 1}); err != nil {
			t.Fatal(err)
		} else if err := w.WritePageHeader(ltx.PageHeader{Pgno: 1}); err != nil {
			t.Fatal(err)
		} else if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		// Ensure all methods return an error after close.
		if err := w.WriteHeader(ltx.Header{}); err != ltx.ErrWriterClosed {
			t.Fatal(err)
		} else if err := w.WritePageHeader(ltx.PageHeader{}); err != ltx.ErrWriterClosed {
			t.Fatal(err)
		} else if err := w.WriteEventHeader(ltx.EventHeader{}); err != ltx.ErrWriterClosed {
			t.Fatal(err)
		} else if _, err := w.Write(nil); err != ltx.ErrWriterClosed {
			t.Fatal(err)
		}
	})
}

func TestHeaderBlockWriter_WriteHeader(t *testing.T) {
	t.Run("ErrInvalidState", func(t *testing.T) {
		w := ltx.NewHeaderBlockWriter(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err := w.WriteHeader(ltx.Header{Version: 1, PageSize: 1024, PageN: 1, Commit: 1, DBID: 1, MinTXID: 1, MaxTXID: 1, PostChecksum: ltx.PageChecksumFlag | 1}); err != nil {
			t.Fatal(err)
		}
		if err := w.WriteHeader(ltx.Header{}); err == nil || err.Error() != `cannot write header frame, expected page header` {
			t.Fatal(err)
		}
	})

	t.Run("ErrWrite", func(t *testing.T) {
		ws := &mock.WriteSeeker{
			WriteFunc: func(p []byte) (n int, err error) {
				if len(p) == ltx.PageHeaderSize {
					return 0, fmt.Errorf("marker")
				}
				return len(p), nil
			},
			SeekFunc: func(offset int64, whence int) (int64, error) { return 0, nil },
		}

		w := ltx.NewHeaderBlockWriter(ws)
		if err := w.WriteHeader(ltx.Header{Version: 1, PageSize: 1024, PageN: 1, Commit: 1, DBID: 1, MinTXID: 1, MaxTXID: 1, PostChecksum: ltx.PageChecksumFlag | 1}); err != nil {
			t.Fatal(err)
		}
		if err := w.WritePageHeader(ltx.PageHeader{Pgno: 1}); err == nil || err.Error() != `write: marker` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestHeaderBlockWriter_WriteEventHeader(t *testing.T) {
	t.Run("ErrInvalidState", func(t *testing.T) {
		w := ltx.NewHeaderBlockWriter(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err := w.WriteEventHeader(ltx.EventHeader{}); err == nil || err.Error() != `cannot write event header, expected header` {
			t.Fatal(err)
		}
	})

	t.Run("ErrSizeRequired", func(t *testing.T) {
		w := ltx.NewHeaderBlockWriter(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err := w.WriteHeader(ltx.Header{Version: 1, PageSize: 1024, EventN: 1, PageN: 1, Commit: 1, EventDataSize: 1, DBID: 1, MinTXID: 1, MaxTXID: 1, PostChecksum: ltx.PageChecksumFlag | 1}); err != nil {
			t.Fatal(err)
		} else if err := w.WritePageHeader(ltx.PageHeader{Pgno: 1}); err != nil {
			t.Fatal(err)
		} else if err := w.WriteEventHeader(ltx.EventHeader{Size: 0}); err == nil || err.Error() != `size required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestHeaderBlockWriter_WritePageHeader(t *testing.T) {
	t.Run("ErrInvalidState", func(t *testing.T) {
		w := ltx.NewHeaderBlockWriter(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err := w.WritePageHeader(ltx.PageHeader{}); err == nil || err.Error() != `cannot write page header, expected header` {
			t.Fatal(err)
		}
	})

	t.Run("ErrPageNumberRequired", func(t *testing.T) {
		w := ltx.NewHeaderBlockWriter(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err := w.WriteHeader(ltx.Header{Version: 1, PageSize: 1024, PageN: 1, Commit: 1, DBID: 1, MinTXID: 1, MaxTXID: 1, PostChecksum: ltx.PageChecksumFlag | 1}); err != nil {
			t.Fatal(err)
		} else if err := w.WritePageHeader(ltx.PageHeader{Pgno: 0}); err == nil || err.Error() != `page number required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrPageNumberOutOfBounds", func(t *testing.T) {
		w := ltx.NewHeaderBlockWriter(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err := w.WriteHeader(ltx.Header{Version: 1, PageSize: 1024, PageN: 1, Commit: 4, DBID: 1, MinTXID: 2, MaxTXID: 2, PreChecksum: ltx.PageChecksumFlag | 2, PostChecksum: ltx.PageChecksumFlag | 2}); err != nil {
			t.Fatal(err)
		} else if err := w.WritePageHeader(ltx.PageHeader{Pgno: 5}); err == nil || err.Error() != `page number 5 out-of-bounds for commit size 4` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrSnapshotInitialPage", func(t *testing.T) {
		w := ltx.NewHeaderBlockWriter(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err := w.WriteHeader(ltx.Header{Version: 1, PageSize: 1024, PageN: 2, Commit: 2, DBID: 1, MinTXID: 1, MaxTXID: 2, PostChecksum: ltx.PageChecksumFlag | 2}); err != nil {
			t.Fatal(err)
		} else if err := w.WritePageHeader(ltx.PageHeader{Pgno: 2}); err == nil || err.Error() != `snapshot transaction file must start with page number 1` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrSnapshotNonsequentialPages", func(t *testing.T) {
		w := ltx.NewHeaderBlockWriter(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err := w.WriteHeader(ltx.Header{Version: 1, PageSize: 1024, PageN: 3, Commit: 3, DBID: 1, MinTXID: 1, MaxTXID: 1, PostChecksum: ltx.PageChecksumFlag | 1}); err != nil {
			t.Fatal(err)
		} else if err := w.WritePageHeader(ltx.PageHeader{Pgno: 1}); err != nil {
			t.Fatal(err)
		} else if err := w.WritePageHeader(ltx.PageHeader{Pgno: 3}); err == nil || err.Error() != `nonsequential page numbers in snapshot transaction: 1,3` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrOutOfOrderPages", func(t *testing.T) {
		w := ltx.NewHeaderBlockWriter(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err := w.WriteHeader(ltx.Header{Version: 1, PageSize: 1024, PageN: 2, Commit: 2, DBID: 1, MinTXID: 2, MaxTXID: 2, PreChecksum: ltx.PageChecksumFlag | 2, PostChecksum: ltx.PageChecksumFlag | 2}); err != nil {
			t.Fatal(err)
		} else if err := w.WritePageHeader(ltx.PageHeader{Pgno: 2}); err != nil {
			t.Fatal(err)
		} else if err := w.WritePageHeader(ltx.PageHeader{Pgno: 1}); err == nil || err.Error() != `out-of-order page numbers: 2,1` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestHeaderBlockWriter_WriteEventData(t *testing.T) {
	t.Run("ErrInvalidState", func(t *testing.T) {
		w := ltx.NewHeaderBlockWriter(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if _, err := w.Write(nil); err == nil || err.Error() != `cannot write event data, expected header` {
			t.Fatal(err)
		}
	})

	t.Run("ErrSizeMismatch", func(t *testing.T) {
		w := ltx.NewHeaderBlockWriter(createFile(t, filepath.Join(t.TempDir(), "ltx")))
		if err := w.WriteHeader(ltx.Header{Version: 1, PageSize: 1024, EventN: 1, PageN: 1, Commit: 1, EventDataSize: 10, DBID: 1, MinTXID: 2, MaxTXID: 2, PreChecksum: ltx.PageChecksumFlag | 2, PostChecksum: ltx.PageChecksumFlag | 2}); err != nil {
			t.Fatal(err)
		} else if err := w.WritePageHeader(ltx.PageHeader{Pgno: 1}); err != nil {
			t.Fatal(err)
		} else if err := w.WriteEventHeader(ltx.EventHeader{Size: 10}); err != nil {
			t.Fatal(err)
		} else if _, err := w.Write(make([]byte, 15)); err == nil || err.Error() != `total event data size of 15 bytes exceeds size specified in header of 10 bytes` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

/*
func TestHeaderBlockWriter_WritePageData(t *testing.T) {
	t.Run("VerifyPageAlignment", func(t *testing.T) {
		w := ltx.NewHeaderBlockWriter(createFile(t, filepath.Join(t.TempDir(), "ltx") ))
		if err := w.WriteHeaderFrame(ltx.HeaderFrame{Version: 1, PageSize: 1024, PageFrameN: 1, Commit: 1, DBID: 1, MinTXID: 2, MaxTXID: 2}); err != nil {
			t.Fatal(err)
		} else if err := w.WritePageHeader(ltx.PageHeader{Pgno: 1}); err != nil {
			t.Fatal(err)
		} else if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		if fi, err := f.Stat(); err != nil {
			t.Fatal(err)
		} else if got, want := fi.Size(), int64(2048); got != want {
			t.Fatalf("fileSize=%d, want %d", got, want)
		}
	})

	t.Run("ErrInvalidState", func(t *testing.T) {
		w := ltx.NewHeaderBlockWriter(createFile(t, filepath.Join(t.TempDir(), "ltx") ))
		if err := w.WritePageData(nil); err == nil || err.Error() != `cannot write page data, expected header` {
			t.Fatal(err)
		}
	})

	t.Run("ErrSizeMismatch", func(t *testing.T) {
		w := ltx.NewHeaderBlockWriter(createFile(t, filepath.Join(t.TempDir(), "ltx") ))
		if err := w.WriteHeaderFrame(ltx.HeaderFrame{Version: 1, PageSize: 1024, PageFrameN: 1, Commit: 1, DBID: 1, MinTXID: 2, MaxTXID: 2}); err != nil {
			t.Fatal(err)
		} else if err := w.WritePageHeader(ltx.PageHeader{Pgno: 1}); err != nil {
			t.Fatal(err)
		} else if err := w.WritePageData(make([]byte, 500)); err == nil || err.Error() != `page must be size specified in header (1024 bytes)` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}
*/
