# Design: Parquet Modular Encryption (read path)

**Status: Under implementation**

**Issues:** #128 (Support Parquet Modular Encryption)

## Scope

This document only takes into account decryption support since Hardwood currently have a read-only implementation. 
The write path is planned for later stages. Also, key management is out of scope, and public API should accept 
keys/key providers from the caller.

## Goal

This document describes the crypto support added for reading encrypted Parquet files as per the [spec](https://github.com/apache/parquet-format/blob/master/Encryption.md).
Parquet files use one of two magic numbers:

- **PAR1** — standard file, plaintext footer. Column data may still be encrypted if `encryptionAlgorithm` is set in `FileMetaData`.
- **PARE** — encrypted footer file. `FileCryptoMetaData` precedes the encrypted `FileMetaData` in the footer.

### Encryption Algorithms

| Algorithm | Page headers & metadata | Page data |
|---|---|---|
| `AES_GCM_V1` | AES-GCM | AES-GCM |
| `AES_GCM_CTR_V1` | AES-GCM | AES-CTR |

### Encrypted Module Format

Encrypted blobs in the file follows this layout:

```
[ 4-byte length (LE) | 12-byte nonce | ciphertext | 16-byte GCM tag (GCM only) ]
```

GCM tag is a cryptographic fingerprint of ciphertext + AAD + key + nonce combined. Any change to any of these — wrong key, wrong ordinals, swapped pages, tampered ciphertext — causes the tag comparison to fail.

The length field = nonce length + ciphertext length. This is critical when reading sequential blobs from the same buffer (encrypted page header immediately followed by encrypted page data) — using `remaining()` instead would consume both blobs at once.

### AAD Structure

Every GCM-encrypted module has an AAD (Additional Authenticated Data) reconstructed by the reader:

```
[ aadPrefix (optional) | aadFileUnique | moduleType (1 byte) | rowGroupOrdinal (2 bytes, little-endian) | columnOrdinal (2 bytes, little-endian) | pageOrdinal (2 bytes, little-endian, data pages only) ]
```

During decryption, AAD is computed from context the reader has.
aadPrefix and aadFileUnique are stored in the file metadata (Thrift structs):
- For PARE files — inside FileCryptoMetaData.encryption_algorithm
- For PAR1 files — inside FileMetaData.encryption_algorithm

### AAD Prefix Resolution

AAD prefix is either stored in the file or is to be provided by the caller(via public API construct AadPrefixProvider)
Hardwood parquet reader makes use of supplyAadPrefix boolean flag stored in the file's EncryptionAlgorithm struct:
```
if supplyAadPrefix == false:
    aadPrefix = read from EncryptionAlgorithm.aadPrefix
    AadPrefixProvider is ignored even if supplied by caller

if supplyAadPrefix == true:
    aadPrefix = AadPrefixProvider.provideAadPrefix() (caller needs to supply)
    if AadPrefixProvider is null , reader throws ParquetEncryptionException
    if AadPrefixProvider returns null, reader throws ParquetEncryptionException thrown
```

### Key Hierarchy

- **Footer key** — encrypts/signs the footer, and optionally encrypts all column data
- **Column keys** — per-column, each column can have an independent key
- Keys are never stored directly. A `keyMetadata` byte array is stored per column/footer, passed to the caller's `DecryptionKeyProvider` to retrieve the actual AES key.

---

## New Crypto Classes

These three classes form the complete crypto layer. They have no dependency on any specific reader implementation.
These classes are placed in `dev.hardwood.internal.reader` package.

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

Static utility class for raw crypto operations. It has no knowledge of Parquet structure.

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
|---|--|
| `decryptPageHeader(buf, pageOrdinal)` | Always GCM. Module type: `DATA_PAGE_HEADER`. |
| `decryptDictPageHeader(buf)` | Always GCM. Module type: `DICT_PAGE_HEADER`. No pageOrdinal — at most one dict page per column chunk. |
| `decryptPageData(buf, pageOrdinal)` | GCM or AES-CTR depending on algorithm (AES_GCM_V1 / AES_GCM_CTR_V1) |
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

The same DecryptionKeyProvider is used for both column key and footer key resolution.

Returns `null` to stay consistent with existing codebase pattern — `null` is used throughout the reader pipeline for absent optional values. 
Calling sites handle it with a `if (columnDecryptor != null)` check.

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

## Public API

A builder has been added to `ParquetFileReader` for supplying crypto params:

```java
ParquetFileReader reader = ParquetFileReader.builder(InputFile.of(path))
        .keyProvider(keyMetadata -> myKeyStore.getKey(keyMetadata))
        .aadPrefixProvider(() -> "my-file-prefix".getBytes())
        .open();
```
The existing factories are kept unchanged for backward compatibility:
The builder is an addition to public API. It covers the encrypted file case and also allows supplying a shared `HardwoodContext`:

```java
ParquetFileReader.builder(InputFile.of(path))
        .keyProvider(kp)                // optional — skip for plaintext files
        .aadPrefixProvider(ap)          // optional — skip if AAD prefix stored in file
        .context(sharedContext)         // optional — skip for dedicated context
        .open();
```

Hardwood users who don't need encryption continue using `open()`.

### Interface signatures

These interfaces are placed inside package `dev.hardwood`;

```java
@FunctionalInterface
public interface DecryptionKeyProvider {
    /// Evaluates a raw AES key from key metadata stored in the file.
    ///
    /// The content of keyMetadata is defined by the application that generated the parquet file.
    /// For simple setups it may be a UTF-8 based string.
    /// For KMS-backed setups it may be a wrapped DEK blob.
    /// The provider is responsible for any unwrapping, Hardwood only uses the
    /// returned bytes directly as the AES key.
    ///
    /// @param keyMetadata bytes stored in the file by the writer to identify or wrap the key.
    /// Content is defined by application. Examples: a UTF-8 key ID string, 
    /// or a KMS-wrapped data encryption key (DEK).
    /// @return raw AES key bytes (16, 24, or 32 bytes for AES-128/192/256)
    /// @throws IOException if key resolution fails (e.g. KMS unreachable)
    byte[] provideKey(byte[] keyMetadata) throws IOException;
}

@FunctionalInterface
public interface AadPrefixProvider {
    /// Supplies the AAD prefix for files written with supplyAadPrefix=true.
    ///
    /// If a parquet file is generated with no AAD prefix stored internally,
    /// the reader needs to supply the same prefix that was used during writing.
    /// Typically this is a file identifier such as a path or URI.
    ///
    /// @return AAD prefix bytes — must match exactly what was used during writing
    /// @throws IOException if the prefix cannot be determined
    byte[] provideAadPrefix() throws IOException;
}
```

### Error handling

| Situation                                                            | Behaviour |
|----------------------------------------------------------------------|---|
| File is encrypted, `keyProvider` is null                             | `ParquetEncryptionException` thrown during `open()` when PARE footer is encountered, or during first read for PAR1 with encrypted columns |
| `keyProvider` returns null for a key                                 | `ParquetEncryptionException` with clear message identifying which column |
| `keyProvider` throws `IOException`                                   | Propagated as-is wrapped in `UncheckedIOException` through the async pipeline |
| File requires caller-supplied AAD prefix but `aadPrefixProvider` is null | `ParquetEncryptionException` thrown when first encrypted column chunk is processed |
| GCM authentication tag mismatch                                      | `ParquetEncryptionException` wrapping the `AEADBadTagException` — indicates wrong key, wrong AAD prefix, or tampered data |
| PARE file with no `FileCryptoMetaData`                               | `IOException` — file is corrupt |
| Unknown algorithm — neither `AesGcmV1` nor `AesGcmCtrV1` present | `IOException` — file is corrupt or uses unsupported algorithm |
| Truncated module — length prefix claims more bytes than buffer contains | `IOException` wrapping `BufferUnderflowException` |

A dedicated `ParquetEncryptionException` to keep crypto failures separate from I/O failures and makes it easy for Hardwood users to handle them separately.
```java
/// Thrown when error occurs during decryption
public class ParquetEncryptionException extends IOException {

    public ParquetEncryptionException(String message) {
        super(message);
    }
}
```


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

### Plaintext Footer Signing

For PAR1 files with encrypted columns, the writer may sign the plaintext footer using the footer key. The signature is a 28-byte blob (12-byte nonce + 16-byte GCM tag) appended after the footer bytes. The GCM tag is computed over the footer bytes using the footer key and `FOOTER` module type AAD.

**This is explicitly deferred.** Footer signature verification is not implemented in this phase. 
Hardwood reads PAR1 plaintext footers without verifying the signature, which. means tampered footers will not be detected. This will be addressed in a follow-up.

The `footerSigningKeyMetadata` field in `FileMetaData` identifies the key used for signing — it is read and stored but not yet used for verification.

### Encrypted Indexes and Bloom Filters

| Module                | Status                                                                                                                                                 |
|-----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| `OFFSET_INDEX`        | In scope — `ColumnDecryptor.decryptOffsetIndex()` decrypts before passing to `OffsetIndexReader`                                                       |
| `COLUMN_INDEX`        | Deferred |
| `BLOOM_FILTER_HEADER` | Deferred                                                                                                                                               |
| `BLOOM_FILTER_BITSET` | Deferred                                                                                                                                               |

## Performance impact of Cipher calls

Initial implementation creates a new `javax.crypto.Cipher` instance per decryption call. As Cipher is stateful and not thread-safe, potential for reuse needs careful observation and measurement.
Thread-local reuse can be a good starting point for initial optimization of repeated Cipher.getInstance() calls.
We will evaluate this trade-off with benchmarks and optimize in a follow-up implementation if needed.
