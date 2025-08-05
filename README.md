Lite Transaction File (LTX)
=================================

The LTX file format provides a way to store SQLite transactional data in
a way that can be encrypted and compacted and is optimized for performance.

File Format
-----------

An LTX file is composed of several sections:

1. Header
2. Page block
3. Trailer

The header contains metadata about the file, the page block contains page
frames, and the trailer contains checksums of the file and the database end state.

Header
------

The header provides information about the number of page frames as well as
database information such as the page size and database size. LTX files
can be compacted together so each file contains the transaction ID (TXID) range
that it represents. A timestamp provides users with a rough approximation of
the time the transaction occurred and the checksum provides a basic integrity
check.

| Offset | Size | Description                             |
| -------| ---- | --------------------------------------- |
| 0      | 4    | Magic number. Always "LTX1".            |
| 4      | 4    | Flags. Reserved for future use.         |
| 8      | 4    | Page size, in bytes.                    |
| 12     | 4    | Size of DB after transaction, in pages. |
| 16     | 8    | Minimum transaction ID.                 |
| 24     | 8    | Maximum transaction ID.                 |
| 32     | 8    | Timestamp (Milliseconds since epoch)    |
| 40     | 8    | Pre-apply DB checksum (CRC-ISO-64)      |
| 48     | 8    | WAL file offset from original WAL       |
| 56     | 8    | Size of original WAL segment            |
| 64     | 4    | Header salt-1 from original WAL        |
| 68     | 4    | Header salt-2 from original WAL        |
| 72     | 8    | Node ID where LTX file was created      |
| 80     | 20   | Reserved.                               |

Page block
----------

This block stores a series of page headers and page data.

| Offset | Size | Description                 |
| -------| ---- | --------------------------- |
| 0      | 4    | Page number.                |
| 4      | N    | Page data.                  |

Trailer
-------

The trailer provides checksum for the LTX file data, a rolling checksum of the
database state after the LTX file is applied, and the checksum of the trailer
itself.

| Offset | Size | Description                             |
| -------| ---- | --------------------------------------- |
| 0      | 8    | Post-apply DB checksum (CRC-ISO-64)     |
| 8      | 8    | File checksum (CRC-ISO-64)              |

Checksum Design
---------------

LTX uses checksums in two distinct ways:

Database Checksum
-----------------

- **Purpose**: Tracks the overall state of the database
- **Computation**: XOR of all page-level checksums in the database
- **Maintenance**: Incrementally maintained by removing old page checksums
  and adding new ones
- **Storage**: `PreApplyChecksum` and `PostApplyChecksum` fields in header
  and trailer

File Checksum
-------------

- **Purpose**: Ensures the LTX file itself hasn't been tampered with
- **Computation**: Computed over the file contents up to (but not including)
  the file checksum field in the trailer
- **Important**: The page index **is included** in the file checksum calculation
- **Rationale**: Including the page index prevents tampering with page offset/size
  mappings, which could redirect reads to incorrect data
- **Storage**: `FileChecksum` field in the trailer

**Security**: The page index is included in the file checksum to detect tampering
with page mappings. While page data itself has individual checksums, the index
mappings must also be protected to prevent malicious redirection attacks.
