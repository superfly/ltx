package ltx

import (
	"fmt"
	"hash"
	"hash/crc64"
	"io"

	"github.com/pierrec/lz4/v4"
)

// Encoder implements a encoder for an LTX file.
type Encoder struct {
	underlying io.Writer // main writer
	w          io.Writer // current writer (main or lz4 writer)
	state      string

	header  Header
	trailer Trailer
	hash    hash.Hash64
	n       int64 // bytes written

	// Track how many of each write has occurred to move state.
	prevPgno     uint32
	pagesWritten uint32
}

// NewEncoder returns a new instance of Encoder.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{
		underlying: w,
		w:          w,
		state:      stateHeader,
	}
}

// N returns the number of bytes written.
func (enc *Encoder) N() int64 { return enc.n }

// Header returns a copy of the header.
func (enc *Encoder) Header() Header { return enc.header }

// Trailer returns a copy of the trailer. File checksum available after Close().
func (enc *Encoder) Trailer() Trailer { return enc.trailer }

// PostApplyPos returns the replication position after underlying the LTX file is applied.
// Only valid after successful Close().
func (enc *Encoder) PostApplyPos() Pos {
	return Pos{
		TXID:              enc.header.MaxTXID,
		PostApplyChecksum: enc.trailer.PostApplyChecksum,
	}
}

// SetPostApplyChecksum sets the post-apply checksum of the database.
// Must call before Close().
func (enc *Encoder) SetPostApplyChecksum(chksum Checksum) {
	enc.trailer.PostApplyChecksum = chksum
}

// Close flushes the checksum to the header.
func (enc *Encoder) Close() error {
	if enc.state == stateClosed {
		return nil // no-op
	} else if enc.state != statePage {
		return fmt.Errorf("cannot close, expected %s", enc.state)
	}

	// Marshal empty page header to mark end of page block.
	b0, err := (&PageHeader{}).MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal empty page header: %w", err)
	} else if _, err := enc.write(b0); err != nil {
		return fmt.Errorf("write empty page header: %w", err)
	}

	// Close the compressed writer, if in use.
	if zw, ok := enc.w.(*lz4.Writer); ok {
		if err := zw.Close(); err != nil {
			return fmt.Errorf("cannot close lz4 writer: %w", err)
		}
	}

	// Revert to original writer now that we've passed the compressed body.
	enc.w = enc.underlying

	// Marshal trailer to bytes.
	b1, err := enc.trailer.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal trailer: %w", err)
	}
	enc.writeToHash(b1[:TrailerChecksumOffset])
	enc.trailer.FileChecksum = ChecksumFlag | Checksum(enc.hash.Sum64())

	// Validate trailer now that we have the file checksum.
	if err := enc.trailer.Validate(enc.header); err != nil {
		return fmt.Errorf("validate trailer: %w", err)
	}

	// If we are encoding a deletion LTX file then ensure that we have an empty checksum.
	if enc.header.Commit == 0 && enc.trailer.PostApplyChecksum != ChecksumFlag {
		return fmt.Errorf("post-apply checksum must be empty for zero-length database")
	}

	// Remarshal with correct checksum.
	b1, err = enc.trailer.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal trailer: %w", err)
	} else if _, err := enc.w.Write(b1); err != nil {
		return fmt.Errorf("write trailer: %w", err)
	}
	enc.n += ChecksumSize

	enc.state = stateClosed

	return nil
}

// EncodeHeader writes hdr to the file's header block.
func (enc *Encoder) EncodeHeader(hdr Header) error {
	if enc.state == stateClosed {
		return ErrEncoderClosed
	} else if enc.state != stateHeader {
		return fmt.Errorf("cannot encode header frame, expected %s", enc.state)
	} else if err := hdr.Validate(); err != nil {
		return err
	}

	enc.header = hdr

	// Initialize hash.
	enc.hash = crc64.New(crc64.MakeTable(crc64.ISO))

	// Write header to underlying writer.
	b, err := enc.header.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal header: %w", err)
	} else if _, err := enc.write(b); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	// Use a compressed writer for the body if LZ4 is enabled.
	if enc.header.Flags&HeaderFlagCompressLZ4 != 0 {
		zw := lz4.NewWriter(enc.underlying)
		if err := zw.Apply(lz4.BlockSizeOption(lz4.Block64Kb)); err != nil { // minimize memory allocation
			return fmt.Errorf("cannot set lz4 block size: %w", err)
		}
		if err := zw.Apply(lz4.CompressionLevelOption(lz4.Fast)); err != nil {
			return fmt.Errorf("cannot set lz4 compression level: %w", err)
		}
		enc.w = zw
	}

	// Move writer state to write page headers.
	enc.state = statePage // file must have at least one page

	return nil
}

// EncodePage writes hdr & data to the file's page block.
func (enc *Encoder) EncodePage(hdr PageHeader, data []byte) (err error) {
	if enc.state == stateClosed {
		return ErrEncoderClosed
	} else if enc.state != statePage {
		return fmt.Errorf("cannot encode page header, expected %s", enc.state)
	} else if hdr.Pgno > enc.header.Commit {
		return fmt.Errorf("page number %d out-of-bounds for commit size %d", hdr.Pgno, enc.header.Commit)
	} else if err := hdr.Validate(); err != nil {
		return err
	} else if uint32(len(data)) != enc.header.PageSize {
		return fmt.Errorf("invalid page buffer size: %d, expecting %d", len(data), enc.header.PageSize)
	}

	lockPgno := LockPgno(enc.header.PageSize)
	if hdr.Pgno == lockPgno {
		return fmt.Errorf("cannot encode lock page: pgno=%d", hdr.Pgno)
	}

	// Snapshots must start with page 1 and include all pages up to the commit size.
	// Non-snapshot files can include any pages but they must be in order.
	if enc.header.IsSnapshot() {
		if enc.prevPgno == 0 && hdr.Pgno != 1 {
			return fmt.Errorf("snapshot transaction file must start with page number 1")
		}

		if enc.prevPgno == lockPgno-1 {
			if hdr.Pgno != enc.prevPgno+2 { // skip lock page
				return fmt.Errorf("nonsequential page numbers in snapshot transaction (skip lock page): %d,%d", enc.prevPgno, hdr.Pgno)
			}
		} else if enc.prevPgno != 0 && hdr.Pgno != enc.prevPgno+1 {
			return fmt.Errorf("nonsequential page numbers in snapshot transaction: %d,%d", enc.prevPgno, hdr.Pgno)
		}
	} else {
		if enc.prevPgno >= hdr.Pgno {
			return fmt.Errorf("out-of-order page numbers: %d,%d", enc.prevPgno, hdr.Pgno)
		}
	}

	// Encode & write header.
	b, err := hdr.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	} else if _, err := enc.write(b); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	// Write data to file.
	if _, err = enc.write(data); err != nil {
		return fmt.Errorf("write page data: %w", err)
	}

	enc.pagesWritten++
	enc.prevPgno = hdr.Pgno
	return nil
}

// write to the current writer & add to the checksum.
func (enc *Encoder) write(b []byte) (n int, err error) {
	n, err = enc.w.Write(b)
	enc.writeToHash(b[:n])
	return n, err
}

func (enc *Encoder) writeToHash(b []byte) {
	_, _ = enc.hash.Write(b)
	enc.n += int64(len(b))
}
