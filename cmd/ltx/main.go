package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
)

// Build information.
var (
	Version = "dev"
)

func main() {
	m := NewMain()
	if err := m.Run(context.Background(), os.Args[1:]); err == flag.ErrHelp {
		os.Exit(1)
	} else if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// Main represents the main program execution.
type Main struct{}

// NewMain returns a new instance of Main.
func NewMain() *Main {
	return &Main{}
}

// Run executes the program.
func (m *Main) Run(ctx context.Context, args []string) (err error) {
	// Extract command name.
	var cmd string
	if len(args) > 0 {
		cmd, args = args[0], args[1:]
	}

	switch cmd {
	case "dump":
		return NewDumpCommand().Run(ctx, args)
	case "verify":
		return NewVerifyCommand().Run(ctx, args)
	default:
		if cmd == "" || cmd == "help" || strings.HasPrefix(cmd, "-") {
			m.Usage()
			return flag.ErrHelp
		}
		return fmt.Errorf("ltx %s: unknown command", cmd)
	}
}

// Usage prints the help screen to STDOUT.
func (m *Main) Usage() {
	fmt.Println(`
ltx is a command-line tool for inspecting LTX files.

Usage:

	ltx <command> [arguments]

The commands are:

	verify       reads & verifies checksums of a set of LTX files
`[1:])
}