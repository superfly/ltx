package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"

	"github.com/superfly/ltx"
)

type KeygenCommand struct{}

func NewKeygenCommand() *KeygenCommand {
	return &KeygenCommand{}
}

func (c *KeygenCommand) Run(_ context.Context, args []string) error {
	fs := flag.NewFlagSet("ltx-keygen", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Println(`
The keygen command generates an X25519 keypair for HPKE encryption.

Usage:

	ltx keygen

Output is two lines of hex-encoded keys: public key, then private key.
`[1:])
	}
	if err := fs.Parse(args); err != nil {
		return err
	}

	pub, priv, err := ltx.GenerateKeyPair()
	if err != nil {
		return fmt.Errorf("generate keypair: %w", err)
	}

	fmt.Printf("public:  %s\n", hex.EncodeToString(pub))
	fmt.Printf("private: %s\n", hex.EncodeToString(priv))

	return nil
}
