package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/superfly/ltx"
)

type RekeyCommand struct{}

func NewRekeyCommand() *RekeyCommand {
	return &RekeyCommand{}
}

func (c *RekeyCommand) Run(_ context.Context, args []string) error {
	fs := flag.NewFlagSet("ltx-rekey", flag.ContinueOnError)
	keyHex := fs.String("key", "", "hex-encoded private key for decryption")
	encryptTo := fs.String("encrypt-to", "", "comma-separated hex-encoded public keys for encryption")
	outPath := fs.String("o", "", "output path")
	fs.Usage = func() {
		fmt.Println(`
The rekey command reads an LTX file with one key and re-encrypts with new recipients.

Usage:

	ltx rekey [arguments] PATH

Arguments:
`[1:])
		fs.PrintDefaults()
		fmt.Println()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() == 0 {
		fs.Usage()
		return flag.ErrHelp
	}
	if *outPath == "" {
		return fmt.Errorf("required: -o PATH")
	}

	var decryptionKey []byte
	if *keyHex != "" {
		var err error
		decryptionKey, err = hex.DecodeString(*keyHex)
		if err != nil {
			return fmt.Errorf("invalid -key: %w", err)
		}
	}

	var recipientKeys [][]byte
	if *encryptTo != "" {
		for _, s := range strings.Split(*encryptTo, ",") {
			key, err := hex.DecodeString(strings.TrimSpace(s))
			if err != nil {
				return fmt.Errorf("invalid -encrypt-to key: %w", err)
			}
			recipientKeys = append(recipientKeys, key)
		}
	}

	// Read input.
	f, err := os.Open(fs.Arg(0))
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	var spec ltx.FileSpec
	if _, err := spec.ReadFromWithKey(f, decryptionKey); err != nil {
		return fmt.Errorf("read input: %w", err)
	}

	// Write output with new encryption.
	spec.RecipientPublicKeys = recipientKeys
	out, err := os.Create(*outPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer func() { _ = out.Close() }()

	if _, err := spec.WriteTo(out); err != nil {
		return fmt.Errorf("write output: %w", err)
	}

	if err := out.Sync(); err != nil {
		return err
	}

	return nil
}

