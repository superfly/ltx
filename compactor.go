package ltx

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// Compactor represents a compactor of LTX files.
type Compactor struct {
	// If true, the compactor calls fsync() after a successful compaction.
	Sync bool

	// If true, the output file will include all event frames from input files.
	IncludeEvents bool
}

// NewCompactor returns a new instance of Compactor with default settings.
func NewCompactor() *Compactor {
	return &Compactor{
		Sync:          true,
		IncludeEvents: true,
	}
}

// Compact merges the LTX files specified in inputFilenames and combines them
// into a single LTX file which is written to outputFilename.
func (c *Compactor) Compact(outputFilename string, inputFilenames []string) (retErr error) {
	if outputFilename == "" {
		return fmt.Errorf("output filename required")
	} else if len(inputFilenames) == 0 {
		return fmt.Errorf("at least one input file required")
	}

	// Open input files & readers.
	inputFiles, err := c.openInputFiles(inputFilenames)
	if err != nil {
		return err
	}
	defer func() { _ = c.closeInputFiles(inputFiles) }()

	// Sort files by transaction ID.
	sort.Slice(inputFiles, func(i, j int) bool {
		return inputFiles[i].hdr.MinTXID < inputFiles[j].hdr.MaxTXID
	})
	for i := range inputFiles {
		inputFilenames[i] = inputFiles[i].filename // re-sort input list
	}

	// Validate that reader page sizes match & TXIDs are contiguous.
	for i := 1; i < len(inputFiles); i++ {
		if hdr0, hdr1 := inputFiles[0].hdr, inputFiles[i].hdr; hdr0.DBID != hdr1.DBID {
			return fmt.Errorf("input files have mismatched database ids: %d != %d", hdr0.DBID, hdr1.DBID)
		} else if hdr0, hdr1 := inputFiles[0].hdr, inputFiles[i].hdr; hdr0.PageSize != hdr1.PageSize {
			return fmt.Errorf("input files have mismatched page sizes: %d != %d", hdr0.PageSize, hdr1.PageSize)
		}

		if hdr0, hdr1 := inputFiles[i-1].hdr, inputFiles[i].hdr; hdr0.MaxTXID+1 != hdr1.MinTXID {
			return fmt.Errorf("non-contiguous transaction ids in input files: %s,%s",
				FormatTXIDRange(hdr0.MinTXID, hdr0.MaxTXID),
				FormatTXIDRange(hdr1.MinTXID, hdr1.MaxTXID),
			)
		}
	}

	// Determine unique page count from input files.
	pageFrameN, err := UniquePageFrameN(inputFilenames)
	if err != nil {
		return fmt.Errorf("input page frame count: %w", err)
	}

	if err := c.writeToOutputFile(inputFiles, outputFilename, pageFrameN); err != nil {
		return err
	}

	return nil
}

func (c *Compactor) writeToOutputFile(inputFiles []*compactionInputFile, filename string, pageFrameN uint32) (retErr error) {
	tempFilename := filename + ".tmp"
	defer func() { _ = os.Remove(tempFilename) }()

	// Generate output header frame.
	hdr := HeaderFrame{
		Version:    Version,
		PageSize:   inputFiles[0].hdr.PageSize,
		PageFrameN: pageFrameN,
		Commit:     inputFiles[len(inputFiles)-1].hdr.Commit,
		DBID:       inputFiles[0].hdr.DBID,
		MinTXID:    inputFiles[0].hdr.MinTXID,
		MaxTXID:    inputFiles[len(inputFiles)-1].hdr.MaxTXID,
		Timestamp:  inputFiles[0].hdr.Timestamp,
	}

	// Determine event info if events are included in the output file.
	if c.IncludeEvents {
		for _, inputFile := range inputFiles {
			hdr.EventFrameN += inputFile.hdr.EventFrameN
			hdr.EventDataSize += inputFile.hdr.EventDataSize
		}
	}

	// Open header block file handle & writer.
	hbf, err := os.OpenFile(tempFilename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer hbf.Close()
	hbw := NewHeaderBlockWriter(hbf)

	// Open page block file handle & writer.
	pbf, err := os.OpenFile(tempFilename, os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer hbf.Close()

	if _, err := pbf.Seek(hdr.HeaderBlockSize(), io.SeekStart); err != nil {
		return fmt.Errorf("seek to page data block: %w", err)
	}
	pbw := NewPageBlockWriter(pbf, hdr.PageFrameN, hdr.PageSize)

	// Write output header.
	if err := hbw.WriteHeaderFrame(hdr); err != nil {
		return fmt.Errorf("write header frame: %w", err)
	}

	// Write page headers & data.
	if err := c.writePageFrames(hbw, pbw, inputFiles); err != nil {
		return fmt.Errorf("write page frames: %w", err)
	}
	hbw.SetPageBlockChecksum(pbw.Checksum())

	// Write event frames.
	if hdr.EventFrameN > 0 {
		if err := c.writeEventFrames(hbw, inputFiles); err != nil {
			return fmt.Errorf("write event frames: %w", err)
		}
	}

	// Flush and close file.
	if err := hbw.Close(); err != nil {
		return fmt.Errorf("close header block writer: %w", err)
	} else if err := pbw.Close(); err != nil {
		return fmt.Errorf("close page block writer: %w", err)
	}

	// Sync, if enabled.
	if c.Sync {
		if err := hbf.Sync(); err != nil {
			return err
		}
	}

	if err := hbf.Close(); err != nil {
		return fmt.Errorf("close header block writer file: %w", err)
	} else if err := pbf.Close(); err != nil {
		return fmt.Errorf("close page block writer file: %w", err)
	}

	// Close input files to verify integrity before rename.
	if err := c.closeInputFiles(inputFiles); err != nil {
		return err
	}

	// Atomically rename to destination file.
	if err := os.Rename(tempFilename, filename); err != nil {
		return fmt.Errorf("rename: %w", err)
	}

	// Sync parent directory.
	if c.Sync {
		if f, err := os.Open(filepath.Dir(filename)); err != nil {
			return err
		} else if err := f.Sync(); err != nil {
			_ = f.Close()
			return err
		} else if err := f.Close(); err != nil {
			return err
		}
	}

	return nil
}

// openInputFiles opens file handles & readers for each input file.
func (c *Compactor) openInputFiles(filenames []string) (_ []*compactionInputFile, err error) {
	inputFiles := make([]*compactionInputFile, 0, len(filenames))
	defer func() {
		if err != nil {
			_ = c.closeInputFiles(inputFiles)
		}
	}()

	for _, filename := range filenames {
		inputFile := &compactionInputFile{
			filename: filename,
		}

		// Open header block file & reader.
		if inputFile.hbf, err = os.Open(inputFile.filename); err != nil {
			return nil, err
		}
		inputFile.hbr = NewHeaderBlockReader(inputFile.hbf)

		// Read header.
		if err := inputFile.hbr.ReadHeaderFrame(&inputFile.hdr); err != nil {
			_ = inputFile.Close()
			return nil, fmt.Errorf("read header frame on %s: %w", inputFile.filename, err)
		}

		// Open page block file & reader.
		if inputFile.pbf, err = os.Open(inputFile.filename); err != nil {
			_ = inputFile.Close()
			return nil, err
		} else if _, err := inputFile.pbf.Seek(inputFile.hdr.HeaderBlockSize(), io.SeekStart); err != nil {
			return nil, fmt.Errorf("seek to page block: %w", err)
		}
		inputFile.pbr = NewPageBlockReader(inputFile.pbf, inputFile.hdr.PageFrameN, inputFile.hdr.PageSize, inputFile.hdr.PageBlockChecksum)

		// Add file to list of inputs.
		inputFiles = append(inputFiles, inputFile)
	}

	return inputFiles, nil
}

func (c *Compactor) closeInputFiles(a []*compactionInputFile) (err error) {
	for _, f := range a {
		if e := f.Close(); err == nil {
			err = e
		}
	}
	return err
}

func (c *Compactor) writePageFrames(hbw *HeaderBlockWriter, pbw *PageBlockWriter, inputFiles []*compactionInputFile) error {
	inputs := make([]struct {
		n   uint32
		hdr PageFrameHeader
	}, len(inputFiles))

	// Initialize buffers with total page count for each reader.
	for i, inputFile := range inputFiles {
		inputs[i].n = inputFile.hdr.PageFrameN
	}

	// Iterate over readers and merge together.
	pageSize := int64(inputFiles[0].hdr.PageSize)
	for {
		// Fill buffer & determine next page number.
		var pgno uint32
		for i := range inputs {
			input := &inputs[i]
			if input.n == 0 {
				continue // no more headers, skip reader
			}

			// Fill buffer for reader, if empty.
			if input.hdr.Pgno == 0 {
				if err := inputFiles[i].hbr.ReadPageHeader(&input.hdr); err != nil {
					return fmt.Errorf("read page header %d: %w", i, err)
				} else if input.hdr.Pgno == 0 {
					return fmt.Errorf("page header has zero pgno: %s", inputFiles[i].filename)
				}
				input.n--
			}

			// Find the lowest page number among the buffers.
			if pgno == 0 || input.hdr.Pgno < pgno {
				pgno = input.hdr.Pgno
			}
		}

		// Exit when no more headers exist.
		if pgno == 0 {
			break
		}

		// Find latest input file with matching page number.
		var pageWritten bool
		for i := len(inputs) - 1; i >= 0; i-- {
			input := &inputs[i]

			// Skip if buffer does have matching page number.
			if input.hdr.Pgno != pgno {
				continue
			}

			// Copy out and clear header from buffer.
			hdr := input.hdr
			input.hdr = PageFrameHeader{}

			// If page number has not been written yet, copy from input file.
			if !pageWritten {
				pageWritten = true
				if err := hbw.WritePageHeader(hdr); err != nil {
					return fmt.Errorf("copy page %d header from %s: %w", pgno, inputFiles[i].filename, err)
				}
				if n, err := io.CopyN(pbw, inputFiles[i].pbr, pageSize); err != nil {
					return fmt.Errorf("copy page %d from %s: n=%d err=%w", pgno, inputFiles[i].filename, n, err)
				}
				continue
			}

			// Otherwise discard page from input.
			if _, err := io.CopyN(io.Discard, inputFiles[i].pbr, pageSize); err != nil {
				return fmt.Errorf("discard page from %s: %w", inputFiles[i].filename, err)
			}
		}
	}

	return nil
}

func (c *Compactor) writeEventFrames(hbw *HeaderBlockWriter, inputFiles []*compactionInputFile) error {
	for _, inputFile := range inputFiles {
		for i := uint32(0); i < inputFile.hdr.EventFrameN; i++ {
			var hdr EventFrameHeader
			if err := inputFile.hbr.ReadEventHeader(&hdr); err != nil {
				return fmt.Errorf("read event header from %s: %w", inputFile.filename, err)
			} else if err := hbw.WriteEventHeader(hdr); err != nil {
				return fmt.Errorf("write event header from %s: %w", inputFile.filename, err)
			}

			if _, err := io.CopyN(hbw, inputFile.hbr, int64(hdr.Size)); err != nil {
				return fmt.Errorf("copy event header from %s: %w", inputFile.filename, err)
			}
		}
	}
	return nil
}

type compactionInputFile struct {
	filename string
	hdr      HeaderFrame

	hbf *os.File
	hbr *HeaderBlockReader

	pbf *os.File
	pbr *PageBlockReader
}

func (f *compactionInputFile) Close() (err error) {
	if f.hbf != nil {
		if e := f.hbf.Close(); err == nil {
			err = e
		}
	}
	if f.pbf != nil {
		if e := f.pbf.Close(); err == nil {
			err = e
		}
	}
	if f.hbr != nil {
		if e := f.hbr.Close(); err == nil {
			err = e
		}
	}
	return err
}

// UniquePageFrameN returns the unique page number count of a set of LTX files.
func UniquePageFrameN(filenames []string) (pageFrameN uint32, err error) {
	inputs := make([]struct {
		r   *HeaderBlockReader
		n   uint32
		hdr PageFrameHeader
	}, len(filenames))

	// Initialize readers & input page counts.
	for i, filename := range filenames {
		f, err := os.Open(filename)
		if err != nil {
			return 0, err
		}
		defer f.Close()
		inputs[i].r = NewHeaderBlockReader(f)

		var hdr HeaderFrame
		if err := inputs[i].r.ReadHeaderFrame(&hdr); err != nil {
			return 0, fmt.Errorf("read header frame %d: %w", i, err)
		}
		inputs[i].n = hdr.PageFrameN
	}

	// Iterate over readers and merge together.
	for {
		// Fill buffer & determine next page number.
		var pgno uint32
		for i := range inputs {
			input := &inputs[i]
			if input.n == 0 {
				continue // no more headers, skip reader
			}

			// Fill buffer for reader, if empty.
			if input.hdr.Pgno == 0 {
				if err := input.r.ReadPageHeader(&input.hdr); err != nil {
					return 0, fmt.Errorf("read page header %d: %w", i, err)
				} else if input.hdr.Pgno == 0 {
					return 0, fmt.Errorf("page header has zero pgno: %s", filenames[i])
				}
				input.n--
			}

			// Find the lowest page number among the buffers.
			if pgno == 0 || input.hdr.Pgno < pgno {
				pgno = input.hdr.Pgno
			}
		}

		// Exit when no more headers exist.
		if pgno == 0 {
			return pageFrameN, nil
		}

		// Clear buffers with matching page number.
		for i := range inputs {
			if inputs[i].hdr.Pgno == pgno {
				inputs[i].hdr = PageFrameHeader{}
			}
		}

		// Increment total page count.
		pageFrameN++
	}
}
