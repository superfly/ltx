package ltx

import (
	"context"
	"fmt"
	"io"
	"sort"
)

// Compactor represents a compactor of LTX files.
type Compactor struct {
	w      *Writer
	inputs []*compactorInput
}

// NewCompactor returns a new instance of Compactor with default settings.
func NewCompactor(w io.Writer, rdrs []io.Reader) *Compactor {
	c := &Compactor{w: NewWriter(w)}
	c.inputs = make([]*compactorInput, len(rdrs))
	for i := range c.inputs {
		c.inputs[i] = &compactorInput{r: NewReader(rdrs[i])}
	}
	return c
}

// Compact merges the input readers into a single LTX writer.
func (c *Compactor) Compact(ctx context.Context) (retErr error) {
	if len(c.inputs) == 0 {
		return fmt.Errorf("at least one input reader required")
	}

	// Read headers from all inputs.
	var hdr Header
	for _, input := range c.inputs {
		if err := input.r.ReadHeader(&hdr); err != nil {
			return
		}
	}

	// Sort inputs by transaction ID.
	sort.Slice(c.inputs, func(i, j int) bool {
		return c.inputs[i].r.Header().MinTXID < c.inputs[j].r.Header().MaxTXID
	})

	// Validate that reader page sizes match & TXIDs are contiguous.
	for i := 1; i < len(c.inputs); i++ {
		prevHdr := c.inputs[i-1].r.Header()
		hdr := c.inputs[i].r.Header()

		if prevHdr.DBID != hdr.DBID {
			return fmt.Errorf("input files have mismatched database ids: %d != %d", prevHdr.DBID, hdr.DBID)
		}
		if prevHdr.PageSize != hdr.PageSize {
			return fmt.Errorf("input files have mismatched page sizes: %d != %d", prevHdr.PageSize, hdr.PageSize)
		}
		if prevHdr.MaxTXID+1 != hdr.MinTXID {
			return fmt.Errorf("non-contiguous transaction ids in input files: %s,%s",
				FormatTXIDRange(prevHdr.MinTXID, prevHdr.MaxTXID),
				FormatTXIDRange(hdr.MinTXID, hdr.MaxTXID),
			)
		}
	}

	// Fetch the first and last headers from the sorted readers.
	minHdr := c.inputs[0].r.Header()
	maxHdr := c.inputs[len(c.inputs)-1].r.Header()

	// Generate output header.
	if err := c.w.WriteHeader(Header{
		Version:          Version,
		PageSize:         minHdr.PageSize,
		Commit:           maxHdr.Commit,
		DBID:             minHdr.DBID,
		MinTXID:          minHdr.MinTXID,
		MaxTXID:          maxHdr.MaxTXID,
		Timestamp:        minHdr.Timestamp,
		PreApplyChecksum: minHdr.PreApplyChecksum,
	}); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	// Write page headers & data.
	if err := c.writePageBlock(ctx); err != nil {
		return err
	}

	// Close readers to ensure they're valid.
	for i, input := range c.inputs {
		if err := input.r.Close(); err != nil {
			return fmt.Errorf("close reader %d: %w", i, err)
		}
	}

	// Close writer.
	c.w.SetPostApplyChecksum(c.inputs[len(c.inputs)-1].r.Trailer().PostApplyChecksum)
	if err := c.w.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}

	return nil
}

func (c *Compactor) writePageBlock(ctx context.Context) error {
	// Allocate buffers.
	for _, input := range c.inputs {
		input.buf.data = make([]byte, c.w.Header().PageSize)
	}

	// Iterate over readers and merge together.
	for {
		// Read next page frame for each buffer.
		pgno, err := c.fillPageBuffers(ctx)
		if err != nil {
			return err
		} else if pgno == 0 {
			break // no more page frames, exit.
		}

		// Write page from latest input.
		if err := c.writePageBuffer(ctx, pgno); err != nil {
			return err
		}
	}

	return nil
}

// fillPageBuffers reads the next page frame into each input buffer.
func (c *Compactor) fillPageBuffers(ctx context.Context) (pgno uint32, err error) {
	for i := range c.inputs {
		input := c.inputs[i]

		// Fill buffer if it is empty.
		if input.buf.hdr.IsZero() {
			if err := input.r.ReadPage(&input.buf.hdr, input.buf.data); err == io.EOF {
				continue // end of page block
			} else if err != nil {
				return 0, fmt.Errorf("read page header %d: %w", i, err)
			}
		}

		// Find the lowest page number among the buffers.
		if pgno == 0 || input.buf.hdr.Pgno < pgno {
			pgno = input.buf.hdr.Pgno
		}
	}
	return pgno, nil
}

// writePageBuffer writes the buffer with a matching pgno from the latest input.
func (c *Compactor) writePageBuffer(ctx context.Context, pgno uint32) error {
	var pageWritten bool
	for i := len(c.inputs) - 1; i >= 0; i-- {
		input := c.inputs[i]
		// Skip if buffer does have matching page number.
		if input.buf.hdr.Pgno != pgno {
			continue
		}

		// If page number has not been written yet, copy from input file.
		if !pageWritten {
			pageWritten = true
			if err := c.w.WritePage(input.buf.hdr, input.buf.data); err != nil {
				return fmt.Errorf("copy page %d header: %w", pgno, err)
			}
		}

		// Clear buffer.
		input.buf.hdr = PageHeader{}
	}

	return nil
}

type compactorInput struct {
	r   *Reader
	buf struct {
		hdr  PageHeader
		data []byte
	}
}
