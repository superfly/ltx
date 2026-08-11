Lite Transaction File (LTX)
=================================

The LTX file format provides a way to store SQLite transactional data in
a way that can be encrypted and compacted and is optimized for performance.

## File Format

This document describes format version 3. The numeric version is not stored in
the file. The `LTX1` magic number is the only on-disk version discriminator, and
decoding it sets `Header.Version` in memory.

An LTX file is composed of four sections:

1. Header
2. Page block
3. Page index
4. Trailer

The header contains metadata about the file, the page block contains page
frames, the page index enables random access to frames, and the trailer contains
checksums for the file and the database end state. Unless otherwise specified,
all fixed-width integer fields use big-endian byte order.


#### Header

The 100-byte header describes the database state and transaction ID (TXID)
range represented by the file.

| Offset | Size | Field             | Description                                      |
| ------ | ---- | ----------------- | ------------------------------------------------ |
| 0      | 4    | Magic             | Always `LTX1`.                                   |
| 4      | 4    | Flags             | Header flags.                                    |
| 8      | 4    | PageSize          | Database page size, in bytes.                    |
| 12     | 4    | Commit            | Database size after applying the file, in pages. |
| 16     | 8    | MinTXID           | Minimum transaction ID.                          |
| 24     | 8    | MaxTXID           | Maximum transaction ID.                          |
| 32     | 8    | Timestamp         | Milliseconds since the Unix epoch.               |
| 40     | 8    | PreApplyChecksum  | Database checksum before applying the file.      |
| 48     | 8    | WALOffset         | Offset in the original WAL; zero for a journal.  |
| 56     | 8    | WALSize           | WAL segment size; zero for a journal.            |
| 64     | 4    | WALSalt1          | First WAL salt; zero for a journal or compaction. |
| 68     | 4    | WALSalt2          | Second WAL salt; zero for journal or compaction. |
| 72     | 8    | NodeID            | Creator node ID; zero if unset.                  |
| 80     | 20   | Reserved          | Written as zero by the current encoder.          |

##### Header flags

| Flag         | Name                 | Description                         |
| ------------ | -------------------- | ----------------------------------- |
| `0x00000002` | HeaderFlagNoChecksum | Disable database checksum tracking. |

`HeaderFlagNoChecksum` is bit 1 (`1 << 1`). When set, the pre-apply and
post-apply database checksums are zero. All other header flag bits are currently
invalid. The file checksum is still required when database checksum tracking is
disabled.


#### Page block

The page block stores page frames in ascending page-number order. Each frame
written by the current encoder has this layout:

| Offset | Size | Field          | Description                                      |
| ------ | ---- | -------------- | ------------------------------------------------ |
| 0      | 4    | Pgno           | One-based database page number.                  |
| 4      | 2    | Flags          | Page header flags.                               |
| 6      | 4    | CompressedSize | Size of the compressed payload, in bytes.        |
| 10     | N    | Data           | LZ4 block-compressed database page data.         |

##### Page header flags

| Flag     | Name               | Description                                |
| -------- | ------------------ | ------------------------------------------ |
| `0x0001` | PageHeaderFlagSize | A four-byte compressed-size field follows. |

`PageHeaderFlagSize` is bit 0 (`1 << 0`). The payload must decompress to
`Header.PageSize` bytes. The current encoder always sets this flag. The decoder
also supports legacy page frames without the flag; those store page data as an
LZ4 frame without a size prefix. All other page header flag bits are invalid.

A six-byte zero page header terminates the page block and has no size prefix or
page data.


#### Page index

The page index follows the zero page header. It contains one entry per page,
sorted by page number. Each entry is encoded as three consecutive unsigned
varints:

1. Page number
2. Absolute byte offset of the page frame from the start of the file
3. Encoded frame size, including its header, optional size prefix, and payload

A zero page-number varint terminates the entries. An eight-byte big-endian
unsigned integer follows and contains the total byte size of the varint entries,
including the zero terminator but excluding the size field itself.


#### Trailer

The 16-byte trailer contains the database checksum after applying the file and
the file checksum.

| Offset | Size | Field              | Description                                |
| ------ | ---- | ------------------ | ------------------------------------------ |
| 0      | 8    | PostApplyChecksum  | Database checksum after applying the file. |
| 8      | 8    | FileChecksum       | CRC-ISO-64 checksum described below.       |


#### File checksum

The file checksum is a CRC-ISO-64 over the following input, in order:

1. The header bytes.
2. For every page, the page header and compressed-size prefix, when present, as
   stored, followed by the **decompressed** page data instead of the compressed
   payload bytes.
3. The zero page header that terminates the page block.
4. All page index bytes, including its zero terminator and size field.
5. The trailer's post-apply checksum field.

The final eight-byte file checksum field is not included. The stored value is
the calculated CRC with `ChecksumFlag` (`1 << 63`) set.

This checksum provides logical-content integrity plus integrity for selected
structural metadata; it is not a byte-for-byte checksum of the file on disk. In
particular, different valid LZ4 payload bytes produce the same checksum when
they decompress to the same page data and do not change the hashed size or index
values.
