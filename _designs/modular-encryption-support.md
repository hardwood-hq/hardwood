# Design document:

This document describes the crypto support added for reading encrypted Parquet files as per the [Apache Parquet Modular Encryption spec](https://github.com/apache/parquet-format/blob/master/Encryption.md).

---

## Context

Parquet files use one of two magic numbers:

- **PAR1** — standard file, plaintext footer. Column data may still be encrypted if `encryptionAlgorithm` is set in `FileMetaData`.
- **PARE** — encrypted footer file. `FileCryptoMetaData` precedes the encrypted `FileMetaData` in the footer region.

### Encryption Algorithms

| Algorithm | Page headers & metadata | Page data |
|---|---|---|
| `AES_GCM_V1` | AES-GCM | AES-GCM |
| `AES_GCM_CTR_V1` | AES-GCM | AES-CTR |

### Encrypted Module Format

Every encrypted blob in the file follows this layout:

```
[ 4-byte length (LE) | 12-byte nonce | ciphertext | 16-byte GCM tag (GCM only) ]
```

The **length field** = nonce length + ciphertext length. This is important when reading sequential blobs from the same buffer (encrypted page header immediately followed by encrypted page data) — using `remaining()` instead would consume both blobs at once.

### AAD Structure

Every GCM-encrypted module has an AAD (Additional Authenticated Data) reconstructed by the reader:

```
[ aadPrefix (optional) | aadFileUnique | moduleType (1 byte) | rowGroupOrdinal (2 bytes, little-endian) | columnOrdinal (2 bytes, little-endian) | pageOrdinal (2 bytes, little-endian, data pages only) ]
```

AAD is not stored in the file. It is computed fresh from context the reader already knows to provide protection against swapping modules between files, row groups, or columns. I there is any mismatch, GCM authentication fails.

### Key Hierarchy

- **Footer key** — encrypts/signs the footer, and optionally encrypts all column data
- **Column keys** — per-column, each column can have an independent key
- Keys are never stored directly. A `keyMetadata` byte array is stored per column/footer, passed to the caller's `DecryptionKeyProvider` to retrieve the actual AES key bytes.

---

## New Crypto Classes

These three classes form the complete crypto layer. They have no dependency on any specific reader implementation.

### `ParquetModuleType`

Constants class — the 10 module type byte values from the spec:

```
FOOTER(0x00), COLUMN_META(0x01), DATA_PAGE(0x02), DICT_PAGE(0x03),
DATA_PAGE_HEADER(0x04), DICT_PAGE_HEADER(0x05),
COLUMN_INDEX(0x06), OFFSET_INDEX(0x07),
BLOOM_FILTER_HEADER(0x08), BLOOM_FILTER_BITSET(0x09)
```

Used when building AAD for any module. Only `DATA_PAGE` and `DATA_PAGE_HEADER` include `pageOrdinal` in their AAD.

---

### `ParquetCryptoHelper`

Static utility for raw crypto operations. No knowledge of Parquet structure.

| Method | Purpose |
|---|---|
| `decryptGcm(buf, key, aad)` | AES-GCM decrypt. Reads exactly `length` bytes using the length prefix, advancing `buf.position()` past this blob. Used for footer, page headers, and page data in `AES_GCM_V1`. |
| `decryptCtr(buf, key)` | AES-CTR decrypt. No AAD. IV = 12-byte nonce + `0x00000001`. Used for page data in `AES_GCM_CTR_V1`. |
| `buildFooterAad(aadPrefix, aadFileUnique)` | AAD for footer — no ordinals. |
| `buildPageAad(aadPrefix, aadFileUnique, moduleType, rgOrdinal, colOrdinal, pageOrdinal)` | AAD for page modules. Pass `pageOrdinal=-1` for modules that don't include it. Ordinals are little-endian 2-byte shorts. |

---

### `ColumnDecryptor`

**The central crypto object for reading a column chunk.** Immutable, one instance per column per row group. Holds all stable crypto context and exposes simple per-page decrypt methods.

**Fields (all final):**
- `byte[] key` — resolved AES key (footer key or column key)
- `byte[] aadPrefix`, `byte[] aadFileUnique` — AAD ingredients
- `int rowGroupOrdinal`, `int columnOrdinal`
- `boolean useCtr` — true for `AES_GCM_CTR_V1`

**Methods:**

| Method | Notes |
|---|---|
| `decryptPageHeader(buf, pageOrdinal)` | Always GCM. Module type: `DATA_PAGE_HEADER`. |
| `decryptDictPageHeader(buf)` | Always GCM. Module type: `DICT_PAGE_HEADER`. No pageOrdinal — at most one dict page per column chunk. |
| `decryptPageData(buf, pageOrdinal)` | GCM or CTR depending on `useCtr`. |
| `decryptDictPageData(buf)` | Always GCM. Module type: `DICT_PAGE`. |

**Static factory:**

```java
ColumnDecryptor.forColumnChunk(fileMetaData, columnChunk, rowGroupOrdinal, columnOrdinal, keyProvider, aadPrefixProvider)
```

Returns `null` if the file is not encrypted. Otherwise resolves the key:

```
columnChunk.cryptoMetadata() == null          → footer key
columnChunk.cryptoMetadata().footerKey() != null  → footer key  
columnChunk.cryptoMetadata().columnKey() != null  → column key
```

**Thread safety:** Instances are immutable. AAD is computed fresh per call.
---

## Reading Pipeline

The crypto layer integrates at three points in the reading pipeline:

```
┌─────────────────────────────────────────────────────────┐
│ 1. FILE OPEN                                            │
│    ParquetMetadataReader.readMetadata()                 │
│    → PAR1: parse footer directly                        │
│    → PARE: read FileCryptoMetaData, decryptGcm()        │
│            decrypt footer, then parse FileMetaData      │
└─────────────────────┬───────────────────────────────────┘
                      │ fileMetaData (with encryptionAlgorithm
                      │ and footerSigningKeyMetadata set)
                      ▼
┌─────────────────────────────────────────────────────────┐
│ 2. PER COLUMN CHUNK (when iterating row groups)         │
│    ColumnDecryptor.forColumnChunk(...)                  │
│    → resolves key (footer or column)                    │
│    → captures rowGroupOrdinal, columnOrdinal            │
│    → determines GCM vs CTR                             │
│                                                         │
│    One ColumnDecryptor per column per row group.        │
│    pageOrdinal resets to 0 for each new column chunk.   │
└─────────────────────┬───────────────────────────────────┘
                      │ ColumnDecryptor instance
                      ▼
┌─────────────────────────────────────────────────────────┐
│ 3. PER PAGE                                             │
│    Dict pages:                                          │
│      decryptDictPageHeader(buf)                         │
│      decryptDictPageData(buf)                           │
│      → then decompress → parse dictionary               │
│                                                         │
│    Data pages:                                          │
│      decryptPageHeader(buf, pageOrdinal)                │
│      → parse PageHeader (sizes, encoding etc)           │
│      decryptPageData(buf, pageOrdinal)                  │
│      → decompress → decode values                       │
│      pageOrdinal++ after each data page                 │
└─────────────────────────────────────────────────────────┘
```

**Key integration rules for any reader implementation:**

1. `fileMetaData` and `keyProvider`/`aadPrefixProvider` must flow from the entry point (e.g. `ParquetFileReader`) down to wherever column chunks are iterated.
2. `ColumnDecryptor.forColumnChunk()` is called once per column chunk. If it returns `null`, the column is not encrypted — skip all decryption.
3. `ColumnDecryptor` is passed to wherever dict pages are parsed (decrypt there directly), and wherever data pages are decoded (pass to the page decoder).
4. `pageOrdinal` is tracked by whoever iterates data pages — starts at 0, resets when a new column chunk begins, increments after each data page.
5. For encrypted files, the page buffer contains two sequential encrypted blobs: `[encrypted header | encrypted data]`. Decrypt header first — buffer advances automatically — then decrypt data from current position.

---

## Public API Change

A builder was added to `ParquetFileReader` for supplying crypto params:

```java
ParquetFileReader reader = ParquetFileReader.builder(InputFile.of(path))
        .keyProvider(keyMetadata -> myKeyStore.getKey(keyMetadata))
        .aadPrefixProvider(() -> "my-file-prefix".getBytes())
        .open();
```

The existing `open()` static methods remain unchanged for backward compatibility.

---

## Encryption Flows Supported

| Flow | Footer | Column keys | Algorithm |
|---|---|---|---|
| 1 | PAR1 plaintext | Per-column keys | GCM or CTR |
| 2 | PAR1 plaintext | Footer key for all | GCM or CTR |
| 3 | PARE encrypted | Footer key for all | AES_GCM_V1 |
| 4 | PARE encrypted | Footer key for all | AES_GCM_CTR_V1 |
| 5 | PARE encrypted | Per-column keys | GCM or CTR |
| 6 | Either | Either | AAD prefix stored in file |
| 7 | Either | Either | AAD prefix supplied by caller |
| 8 | PARE encrypted | All same key | Uniform encryption |

**-Diagrams generated with inputs from Sonnet 4.6.**