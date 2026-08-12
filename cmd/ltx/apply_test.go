package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/superfly/ltx"
)

func TestApplyCommand_NoChecksumSnapshot(t *testing.T) {
	const pageSize = 512

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")
	ltxPath := filepath.Join(dir, "snapshot.ltx")
	want := bytes.Repeat([]byte{0x12}, pageSize)
	writeApplyTestLTX(t, ltxPath, &ltx.FileSpec{
		Header: ltx.Header{
			Version:  ltx.Version,
			Flags:    ltx.HeaderFlagNoChecksum,
			PageSize: pageSize,
			Commit:   1,
			MinTXID:  1,
			MaxTXID:  1,
		},
		Pages: []ltx.PageSpec{
			{Header: ltx.PageHeader{Pgno: 1}, Data: want},
		},
	})

	if err := NewApplyCommand().Run(context.Background(), []string{"-db", dbPath, ltxPath}); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(dbPath); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(got, want) {
		t.Fatalf("database mismatch:\ngot=%x\nwant=%x", got, want)
	}
}

func TestApplyCommand_NoChecksumIncremental(t *testing.T) {
	const pageSize = 512

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")
	ltxPath := filepath.Join(dir, "incremental.ltx")
	initial := bytes.Repeat([]byte{0x34}, 2*pageSize)
	want := bytes.Repeat([]byte{0x56}, pageSize)
	if err := os.WriteFile(dbPath, initial, 0o666); err != nil {
		t.Fatal(err)
	}
	writeApplyTestLTX(t, ltxPath, &ltx.FileSpec{
		Header: ltx.Header{
			Version:  ltx.Version,
			Flags:    ltx.HeaderFlagNoChecksum,
			PageSize: pageSize,
			Commit:   1,
			MinTXID:  2,
			MaxTXID:  2,
		},
		Pages: []ltx.PageSpec{
			{Header: ltx.PageHeader{Pgno: 1}, Data: want},
		},
	})

	if err := NewApplyCommand().Run(context.Background(), []string{"-db", dbPath, ltxPath}); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(dbPath); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(got, want) {
		t.Fatalf("database mismatch:\ngot=%x\nwant=%x", got, want)
	}
}

func TestApplyCommand_SnapshotOverExistingDatabase(t *testing.T) {
	const pageSize = 512

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")
	ltxPath := filepath.Join(dir, "snapshot.ltx")
	initial := bytes.Repeat([]byte{0x78}, 2*pageSize)
	want := bytes.Repeat([]byte{0x9a}, pageSize)
	if err := os.WriteFile(dbPath, initial, 0o666); err != nil {
		t.Fatal(err)
	}
	writeApplyTestLTX(t, ltxPath, &ltx.FileSpec{
		Header: ltx.Header{
			Version:  ltx.Version,
			PageSize: pageSize,
			Commit:   1,
			MinTXID:  1,
			MaxTXID:  1,
		},
		Pages: []ltx.PageSpec{
			{Header: ltx.PageHeader{Pgno: 1}, Data: want},
		},
		Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumPage(1, want)},
	})

	if err := NewApplyCommand().Run(context.Background(), []string{"-db", dbPath, ltxPath}); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(dbPath); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(got, want) {
		t.Fatalf("database mismatch:\ngot=%x\nwant=%x", got, want)
	}
}

func writeApplyTestLTX(t *testing.T, path string, spec *ltx.FileSpec) {
	t.Helper()

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := spec.WriteTo(f); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}
