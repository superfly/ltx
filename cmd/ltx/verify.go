package main

import (
	"context"
	"flag"
	"fmt"
	"io"
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
	fs.Usage = func() {
		fmt.Println(`
The verify command reads one or more LTX files and verifies its integrity.

Usage:

	ltx verify PATH [PATH...]

`[1:],
		)
	}
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() == 0 {
		return fmt.Errorf("at least one LTX file must be specified")
	}

	var okN, errorN int
	for _, filename := range fs.Args() {
		if err := c.verifyFile(ctx, filename); err != nil {
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

func (c *VerifyCommand) verifyFile(ctx context.Context, filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return err
	}

	n, err := io.Copy(io.Discard, ltx.NewReader(f))
	if err != nil {
		return err
	} else if int64(n) > fi.Size() {
		return fmt.Errorf("contains %d bytes past end of LTX contents (%d bytes)", int64(n)-fi.Size(), fi.Size())
	}

	return nil
}
