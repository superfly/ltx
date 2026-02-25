package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/superfly/ltx"
	"github.com/superfly/ltx/internal"
)

// DumpCommand represents a command to print the contents of a single LTX file.
type DumpCommand struct{}

// NewDumpCommand returns a new instance of DumpCommand.
func NewDumpCommand() *DumpCommand {
	return &DumpCommand{}
}

// Run executes the command.
func (c *DumpCommand) Run(ctx context.Context, args []string) (ret error) {
	fs := flag.NewFlagSet("ltx-dump", flag.ContinueOnError)
	keyHex := fs.String("key", "", "hex-encoded private key for decryption")
	fs.Usage = func() {
		fmt.Println(`
The dump command writes out all data for a single LTX file.

Usage:

	ltx dump [arguments] PATH

Arguments:
`[1:])
		fs.PrintDefaults()
		fmt.Println()
	}
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() == 0 {
		return fmt.Errorf("filename required")
	} else if fs.NArg() > 1 {
		return fmt.Errorf("too many arguments")
	}

	var decryptionKey []byte
	if *keyHex != "" {
		var err error
		decryptionKey, err = hex.DecodeString(*keyHex)
		if err != nil {
			return fmt.Errorf("invalid -key: %w", err)
		}
	}

	f, err := os.Open(fs.Arg(0))
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	dec := ltx.NewDecoder(f)
	if decryptionKey != nil {
		dec.SetDecryptionKey(decryptionKey)
	}

	// Read & print header information.
	err = dec.DecodeHeader()
	hdr := dec.Header()
	fmt.Printf("# HEADER\n")
	fmt.Printf("Version:   %d\n", hdr.Version)
	fmt.Printf("Flags:     0x%08x\n", hdr.Flags)
	fmt.Printf("Page size: %d\n", hdr.PageSize)
	fmt.Printf("Commit:    %d\n", hdr.Commit)
	fmt.Printf("Min TXID:  %s (%d)\n", hdr.MinTXID.String(), hdr.MinTXID)
	fmt.Printf("Max TXID:  %s (%d)\n", hdr.MaxTXID.String(), hdr.MaxTXID)
	fmt.Printf("Timestamp: %s (%d)\n", time.UnixMilli(int64(hdr.Timestamp)).UTC().Format(time.RFC3339Nano), hdr.Timestamp)
	fmt.Printf("Pre-apply: %s\n", hdr.PreApplyChecksum)
	fmt.Printf("WAL offset: %d\n", hdr.WALOffset)
	fmt.Printf("WAL size:   %d\n", hdr.WALSize)
	fmt.Printf("WAL salt:   %08x %08x\n", hdr.WALSalt1, hdr.WALSalt2)
	if hdr.Encrypted() {
		fmt.Printf("Encrypted: yes (recipients=%d, KEM=0x%04x, KDF=0x%04x, AEAD=0x%04x)\n",
			hdr.RecipientCount, hdr.KEMID, hdr.KDFID, hdr.AEADID)
	} else {
		fmt.Printf("Encrypted: no\n")
	}
	fmt.Printf("\n")
	if err != nil {
		return err
	}

	fmt.Printf("# PAGE DATA\n")
	for i := 0; ; i++ {
		var pageHeader ltx.PageHeader
		data := make([]byte, hdr.PageSize)
		if err := dec.DecodePage(&pageHeader, data); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("decode page frame %d: %w", i, err)
		}

		fmt.Printf("Frame #%d: pgno=%d\n", i, pageHeader.Pgno)
		fmt.Print(internal.Hexdump(data))
		fmt.Println()
	}
	fmt.Printf("\n")

	// Close & verify file, print trailer.
	err = dec.Close()
	trailer := dec.Trailer()

	fmt.Printf("# TRAILER\n")
	fmt.Printf("Post-apply:    %s\n", trailer.PostApplyChecksum)
	fmt.Printf("File Checksum: %s\n", trailer.FileChecksum)
	fmt.Printf("\n")
	if err != nil {
		return err
	}

	return nil
}
