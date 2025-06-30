Lite Transaction File (LTX)
=================================

The LTX file format provides a way to store SQLite transactional data in
a way that can be encrypted and compacted and is optimized for performance.

## File Format

An LTX file is composed of several sections:

1. Header
2. Page block
3. Trailer

The header contains metadata about the file, the page block contains page
frames, and the trailer contains checksums of the file and the database end state.


#### Header

The header provides information about the number of page frames as well as
database information such as the page size and database size. LTX files
can be compacted together so each file contains the transaction ID (TXID) range
that it represents. A timestamp provides users with a rough approximation of
the time the transaction occurred and the checksum provides a basic integrity
check.

| Offset | Size | Description                                     |
| -------| ---- | ----------------------------------------------- |
| 0      | 4    | Magic number. Always "LTX1".                    |
| 4      | 4    | Flags. See below.                               |
| 8      | 4    | Page size, in bytes.                            |
| 12     | 4    | Size of DB after transaction, in pages.         |
| 16     | 8    | Minimum transaction ID.                         |
| 24     | 8    | Maximum transaction ID.                         |
| 32     | 8    | Timestamp (Milliseconds since epoch)            |
| 40     | 8    | Pre-apply DB checksum (CRC-ISO-64)              |
| 48     | 8    | File offset in WAL, zero if journal             |
| 56     | 8    | Size of WAL segment, zero if journal            |
| 64     | 4    | Salt-1 from WAL, zero if journal or compacted   |
| 68     | 4    | Salt-2 from WAL, zero if journal or compacted   |
| 72     | 8    | ID of the node that created file, zero if unset |
| 80     | 20   | Reserved.                                       |


##### Header flags

| Flag       | Description                 |
| ---------- | --------------------------- |
| 0x00000001 | Data is compressed with LZ4 |


#### Page block

This block stores a series of page headers and page data.

| Offset | Size | Description                 |
| -------| ---- | --------------------------- |
| 0      | 4    | Page number.                |
| 4      | N    | Page data.                  |


#### Trailer

The trailer provides checksum for the LTX file data, a rolling checksum of the
database state after the LTX file is applied, and the checksum of the trailer
itself.

| Offset | Size | Description                             |
| -------| ---- | --------------------------------------- |
| 0      | 8    | Post-apply DB checksum (CRC-ISO-64)     |
| 8      | 8    | File checksum (CRC-ISO-64)              |


