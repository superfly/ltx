Lite Transaction File (LTX)
=================================

The LTX file format provides a way to store SQLite transactional data in
a way that can be encrypted and compacted and is optimized for performance.

## File Format

This document describes format versions 3 and 4.

Versions 2 and 3 share the `LTX1` magic number and carry no on-disk version
field, so a reader cannot tell them apart from the file alone and must know
which it has out of band. They differ in page frame layout: version 2 used a
four-byte page header with no flags and no compressed-size prefix, while
version 3 uses a six-byte page header and, in the current encoding, a four-byte
compressed-size prefix.

Version 4 adds per-page encryption and is the one version that *is* self
identifying, because it changed the magic to `LTX4`. Encryption is the only
feature version 4 adds, so the encoder writes an unencrypted file as version 3
with the `LTX1` magic: an unencrypted version 4 header would be byte-identical
to a version 3 header anyway, and emitting `LTX4` would stop existing readers
accepting a file that had not otherwise changed. In practice `LTX4` therefore
means encrypted.

An LTX file is composed of four sections, or five when encrypted:

1. Header
2. Recipient block (encrypted files only)
3. Page block
4. Page index
5. Trailer

The header contains metadata about the file, the recipient block carries the
content encryption key wrapped to each recipient, the page block contains page
frames, the page index enables random access to frames, and the trailer contains
checksums for the file and the database end state. Unless otherwise specified,
all fixed-width integer fields use big-endian byte order.


#### Header

The 100-byte header describes the database state and transaction ID (TXID)
range represented by the file.

| Offset | Size | Field             | Description                                      |
| ------ | ---- | ----------------- | ------------------------------------------------ |
| 0      | 4    | Magic             | `LTX1` for version 3, `LTX4` for version 4.      |
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
| 80     | 2    | RecipientCount    | Encrypted recipients; zero if unencrypted.       |
| 82     | 2    | KEMID             | HPKE KEM identifier; zero if unencrypted.        |
| 84     | 2    | KDFID             | HPKE KDF identifier; zero if unencrypted.        |
| 86     | 2    | AEADID            | HPKE AEAD identifier; zero if unencrypted.       |
| 88     | 12   | Reserved          | Written as zero by the current encoder.          |

Bytes 80 through 87 are the version 4 encryption parameters. They fall inside
the region version 3 reserved and wrote as zero, which is why an unencrypted
version 4 header is byte-identical to a version 3 one.

##### Header flags

| Flag         | Name                    | Description                          |
| ------------ | ----------------------- | ------------------------------------ |
| `0x00000002` | HeaderFlagNoChecksum    | Disable database checksum tracking.  |
| `0x00000004` | HeaderFlagEncryptedHPKE | Pages are encrypted (version 4 only). |

`HeaderFlagNoChecksum` is bit 1 (`1 << 1`). When set, the pre-apply and
post-apply database checksums are zero. The file checksum is still required when
database checksum tracking is disabled.

`HeaderFlagEncryptedHPKE` is bit 2 (`1 << 2`). When set, the file must be
version 4, `RecipientCount` must be non-zero, and a recipient block follows the
header. All other header flag bits are currently invalid.


#### Recipient block

Present only when `HeaderFlagEncryptedHPKE` is set, immediately after the
header, and repeated immediately before the trailer. It holds
`RecipientCount` entries of 80 bytes each.

Each entry is an HPKE (RFC 9180) single-shot sealing of the 32-byte content
encryption key (CEK) to one recipient public key, and is laid out as the
32-byte encapsulated key, the 32-byte wrapped CEK, and a 16-byte authentication
tag. A recipient recovers the CEK by trying to open each entry with its private
key.

The CEK is generated fresh for every file. Two keys are derived from it with
HKDF-SHA256: a page key using the info string `ltx-page-key`, and an index key
using `ltx-index-key`.


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
`Header.PageSize` bytes. The current encoder always sets this flag. Within
version 3, the decoder uses it as a per-frame encoding heuristic and supports
legacy frames without the flag; those store page data as an LZ4 frame without a
size prefix. The flag is not a format version field. All other page header flag
bits are invalid.

A six-byte zero page header terminates the page block and has no size prefix or
page data.

##### Encrypted page frames

When `HeaderFlagEncryptedHPKE` is set, the payload is the LZ4-compressed page
data sealed with ChaCha20-Poly1305 under the page key, and `CompressedSize`
counts the sealed payload rather than the compressed one:

| Offset | Size | Field      | Description                                    |
| ------ | ---- | ---------- | ---------------------------------------------- |
| 0      | 12   | Nonce      | Random per-page nonce.                         |
| 12     | M    | Ciphertext | Sealed LZ4-compressed page data.               |
| 12+M   | 16   | Tag        | Poly1305 authentication tag.                   |

The additional authenticated data is the 32-byte SHA-256 hash of the header,
the four-byte page number, and the six-byte page header, concatenated in that
order. Binding those in means a frame cannot be moved to a different page
number, or into a different file, without detection.

Encrypted files must use the size-prefixed frame layout; a frame without
`PageHeaderFlagSize` is rejected rather than treated as a legacy frame.


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

In an encrypted file the index itself is not encrypted, but a 16-byte
Poly1305 tag follows it, computed over the index bytes as additional data under
the index key with an empty plaintext. The duplicate recipient block follows the
tag, and the trailer follows that. Verifying the tag requires the index key, so
a reader without a decryption key skips it; the file checksum still covers those
bytes.


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

Encrypted files differ on step 2: the sealed payload is hashed exactly as
written, rather than the plaintext it protects, and the recipient blocks are
hashed as well. That is deliberate. It means whoever holds the file can verify
its integrity without holding any decryption key, which is what lets `ltx
verify` check an archived encrypted file. Such a check covers structure and
bytes only; it says nothing about page contents, and the tool reports the
narrower guarantee rather than a bare `ok`.

The post-apply database checksum is unaffected by encryption. It is always a
rolling checksum over plaintext pages, so verifying it does require a key.
