package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/superfly/ltx"
)

func TestEncodeDBCommand(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		dir := t.TempDir()
		dbPath := filepath.Join(dir, "db")
		writeSQLiteDatabase(t, dbPath)

		outPath := filepath.Join(dir, "ltx")
		oldData := bytes.Repeat([]byte("x"), 1<<20)
		if err := os.WriteFile(outPath, oldData, 0o644); err != nil {
			t.Fatal(err)
		}

		if err := NewEncodeDBCommand().Run(context.Background(), []string{"-o", outPath, dbPath}); err != nil {
			t.Fatal(err)
		}

		f, err := os.Open(outPath)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = f.Close() })

		dec := ltx.NewDecoder(f)
		if err := dec.Verify(); err != nil {
			t.Fatal(err)
		} else if got, want := dec.Header().Version, ltx.Version; got != want {
			t.Fatalf("version=%d, want %d", got, want)
		}

		if info, err := f.Stat(); err != nil {
			t.Fatal(err)
		} else if got, wantMax := info.Size(), int64(len(oldData)-1); got > wantMax {
			t.Fatalf("size=%d, want <= %d", got, wantMax)
		}
	})

	t.Run("ErrSameFile", func(t *testing.T) {
		for _, tt := range []struct {
			name  string
			alias func(*testing.T, string) string
		}{
			{
				name: "SamePath",
				alias: func(t *testing.T, dbPath string) string {
					return dbPath
				},
			},
			{
				name: "HardLink",
				alias: func(t *testing.T, dbPath string) string {
					t.Helper()
					outPath := filepath.Join(filepath.Dir(dbPath), "ltx")
					if err := os.Link(dbPath, outPath); err != nil {
						t.Fatal(err)
					}
					return outPath
				},
			},
			{
				name: "SymbolicLink",
				alias: func(t *testing.T, dbPath string) string {
					t.Helper()
					outPath := filepath.Join(filepath.Dir(dbPath), "ltx")
					if err := os.Symlink(dbPath, outPath); err != nil {
						t.Fatal(err)
					}
					return outPath
				},
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				dbPath := filepath.Join(t.TempDir(), "db")
				want := writeSQLiteDatabase(t, dbPath)
				outPath := tt.alias(t, dbPath)

				if err := NewEncodeDBCommand().Run(context.Background(), []string{"-o", outPath, dbPath}); err == nil || err.Error() != "input and output files are the same" {
					t.Fatalf("unexpected error: %v", err)
				}

				if got, err := os.ReadFile(dbPath); err != nil {
					t.Fatal(err)
				} else if !bytes.Equal(got, want) {
					t.Fatal("database changed")
				}
			})
		}
	})

	t.Run("ErrPreservesOutput", func(t *testing.T) {
		dir := t.TempDir()
		dbPath := filepath.Join(dir, "db")
		db := writeSQLiteDatabase(t, dbPath)
		if err := os.WriteFile(dbPath, db[:len(db)/2], 0o644); err != nil {
			t.Fatal(err)
		}

		outPath := filepath.Join(dir, "ltx")
		want := []byte("existing output")
		if err := os.WriteFile(outPath, want, 0o644); err != nil {
			t.Fatal(err)
		}

		if err := NewEncodeDBCommand().Run(context.Background(), []string{"-o", outPath, dbPath}); err == nil || err.Error() != "read page 2: EOF" {
			t.Fatalf("unexpected error: %v", err)
		}

		if got, err := os.ReadFile(outPath); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(got, want) {
			t.Fatalf("output=%q, want %q", got, want)
		}

		if entries, err := os.ReadDir(dir); err != nil {
			t.Fatal(err)
		} else if got, want := len(entries), 2; got != want {
			t.Fatalf("entry count=%d, want %d", got, want)
		}
	})
}

func writeSQLiteDatabase(t *testing.T, path string) []byte {
	t.Helper()

	const (
		pageSize = 512
		pageN    = 2
	)

	b := make([]byte, pageSize*pageN)
	copy(b, SQLITE_DATABASE_HEADER_STRING)
	binary.BigEndian.PutUint16(b[16:], pageSize)
	binary.BigEndian.PutUint32(b[28:], pageN)
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	return b
}
