package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"text/tabwriter"
	"time"

	"github.com/superfly/ltx"
)

// ListCommand represents a command to print the header/trailer of one or more
// LTX files in a table.
type ListCommand struct{}

// NewListCommand returns a new instance of ListCommand.
func NewListCommand() *ListCommand {
	return &ListCommand{}
}

// Run executes the command.
func (c *ListCommand) Run(ctx context.Context, args []string) (ret error) {
	fs := flag.NewFlagSet("ltx-list", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Println(`
The list command lists header & trailer information for a set of LTX files.

Usage:

	ltx list [arguments] PATH [PATH...]

Arguments:
`[1:])
		fs.PrintDefaults()
		fmt.Println()
	}
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() == 0 {
		return fmt.Errorf("at least one LTX file is required")
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	defer w.Flush()

	fmt.Fprintln(w, "min_txid\tmax_txid\tcommit\tpages\tpreapply\tpostapply\ttimestamp")
	for _, arg := range fs.Args() {
		if err := c.printFile(w, arg); err != nil {
			fmt.Fprintf(os.Stderr, "%s: %s\n", arg, err)
		}
	}

	return nil
}

func (c *ListCommand) printFile(w *tabwriter.Writer, filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	r := ltx.NewReader(f)
	if _, err := io.Copy(io.Discard, r); err != nil {
		return err
	}

	// Only show timestamp if it is actually set.
	timestamp := time.Unix(int64(r.Header().Timestamp), 0).UTC().Format(time.RFC3339)
	if r.Header().Timestamp == 0 {
		timestamp = ""
	}

	fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%016x\t%016x\t%s\n",
		ltx.FormatTXID(r.Header().MinTXID),
		ltx.FormatTXID(r.Header().MaxTXID),
		r.Header().Commit,
		r.PageN(),
		r.Header().PreApplyChecksum,
		r.Trailer().PostApplyChecksum,
		timestamp,
	)

	return nil
}
