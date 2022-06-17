Liteserver Transaction File (LTX)
=================================

The LTX file format provides a way to store SQLite transactional data in
a way that can be encrypted and compacted and is optimized for performance.
It also provides built-in support for simple event data associated with each
transaction.

## File Format

An LTX file is composed of 2 blocks:

1. Header block
2. Page data block

The header block contains a header frame, event frames, and page headers. The
page data block contains raw page data and is page-aligned. These two blocks
are separated to allow them to be read & written separately to reduce memory
usage.


### Header block

The header block is composed of 3 sections:

1. Header frame
2. Event frames
3. Page headers


#### Header frame

The header provides information about the number of data and event frames as
well as database information such as the page size and database size. LTX files
can be compacted together so each file contains the transaction ID (TXID) range
that it represents. A timestamp provides users with a rough approximation of
the time the transaction occurred and the checksum provides a basic integrity
check.

| Offset | Size | Description                             |
| -------| ---- | --------------------------------------- |
| 0      | 4    | Magic number. Always "LTX1".            |
| 4      | 4    | Flags. Reserved. Always 0.              |
| 8      | 4    | Page size, in bytes.                    |
| 12     | 4    | Event frame count.                      |
| 16     | 4    | Total event data size.                  |
| 20     | 4    | Page frame count.                       |
| 24     | 4    | Size of DB after transaction, in pages. |
| 28     | 8    | Minimum transaction ID.                 |
| 36     | 8    | Maximum transaction ID.                 |
| 44     | 8    | Timestamp (seconds since Unix epoch)    |
| 52     | 8    | Header checksum (CRC-ISO-64)            |
| 60     | 8    | Page data checksum (CRC-ISO-64)         |


#### Event frames

These frames store the size of the event payload, in bytes, as well as the nonce
and authentication tag which is used for AES-GCM-256 encryption. The event frame
data follows afterward for the number of bytes specified by the size field.

| Offset | Size | Description                 |
| -------| ---- | --------------------------- |
| 0      | 4    | Event data size, in bytes.  |
| 4      | 12   | AES-GCM nonce.              |
| 16     | 16   | AES-GCM authentication tag. |
| 32     | *    | Event data                  |


#### Page headers

This header stores the page number associated with the page data as well as the
nonce & authentication tag which is used for AES-GCM-256 encryption.

| Offset | Size | Description                 |
| -------| ---- | --------------------------- |
| 0      | 4    | Page number.                |
| 4      | 12   | AES-GCM nonce.              |
| 16     | 16   | AES-GCM authentication tag. |



### Page data block

The page data block holds encrypted pages in the same order the page headers.
This block is aligned to the page size specified in the header. The size can
be calculated as the number of pages specified in the header frame multiplied
by the page size.
