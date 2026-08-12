package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"os"

	"github.com/superfly/ltx"
)

// VerifyCommand represents a command to verify the integrity of LTX files.
type VerifyCommand struct{}

// NewVerifyCommand returns a new instance of VerifyCommand.
func NewVerifyCommand() *VerifyCommand {
	return &VerifyCommand{}
}

// Run executes the command.
func (c *VerifyCommand) Run(ctx context.Context, args []string) (ret error) {
	fs := flag.NewFlagSet("ltx-verify", flag.ContinueOnError)
	keyHex := fs.String("key", "", "hex-encoded private key for decryption")
	fs.Usage = func() {
		fmt.Println(`
The verify command reads one or more LTX files and verifies its integrity.

Usage:

	ltx verify [arguments] PATH [PATH...]

Arguments:
`[1:])
		fs.PrintDefaults()
		fmt.Println()
	}
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() == 0 {
		return fmt.Errorf("at least one LTX file must be specified")
	}

	var decryptionKey []byte
	if *keyHex != "" {
		var err error
		decryptionKey, err = hex.DecodeString(*keyHex)
		if err != nil {
			return fmt.Errorf("invalid -key: %w", err)
		}
	}

	var okN, errorN int
	for _, filename := range fs.Args() {
		if err := c.verifyFile(ctx, filename, decryptionKey); err != nil {
			errorN++
			fmt.Printf("%s: %s\n", filename, err)
			continue
		}

		okN++
	}

	if errorN != 0 {
		return fmt.Errorf("%d ok, %d invalid", okN, errorN)
	}

	fmt.Println("ok")
	return nil
}

func (c *VerifyCommand) verifyFile(_ context.Context, filename string, decryptionKey []byte) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	dec := ltx.NewDecoder(f)
	if decryptionKey != nil {
		dec.SetDecryptionKey(decryptionKey)
	}
	if err := dec.Verify(); err != nil {
		return err
	}

	// An encrypted file verified without a key has had its integrity checked
	// against the file checksum, which covers the ciphertext. Say so, rather
	// than letting a bare "ok" imply the contents were checked too.
	if dec.Header().Encrypted() && decryptionKey == nil {
		fmt.Printf("%s: integrity ok (encrypted; contents not verified, no key supplied)\n", filename)
	}
	return nil
}
