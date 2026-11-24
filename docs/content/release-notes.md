<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Release Notes

See [GitHub Releases](https://github.com/hardwood-hq/hardwood/releases) for downloads and more information.

## 1.1.0.CR1 (2026-11-24)

[Announcement blog post](https://www.morling.dev/blog/s3-writes-parallel-encoding-alp-hardwood-1-1-0-cr1/) ·
[API changes](/api-changes/1.1.0.CR1/)

Highlights of this release:

- Writing to S3 and parallel writes
    - `S3Source.outputFile(...)` streams a file to S3 as a multipart upload while it is written, uploading parts in the background; nothing is visible at the key until the writer closes
    - A row group's column chunks are encoded and compressed in parallel on the pool of a `HardwoodContext`, passed to `ParquetFileWriter.create(...)`; the file is byte-identical whichever thread encoded what
- Further writer additions
    - Compression levels for `ZSTD`, `GZIP`, `BROTLI` and `LZ4_RAW`, as in `Compression.zstd().level(9)`
    - `sortingColumns` declares and enforces the row order, recorded as `sorting_columns`
    - `DATA_PAGE_V2` pages on request, and `SizeStatistics` with level histograms, so Hardwood and other readers can prune `isNull` and `isNotNull` on groups in files Hardwood wrote
    - `StructBuilder.setValue` takes a value in the form `getValue` returns, so rows copy without switching on field types
    - `PqInterval.of(months, days, milliseconds)` creates an `INTERVAL` value for `setInterval` and filter literals
- New format features
    - ALP-encoded `FLOAT` and `DOUBLE` columns are read, and written with `ColumnEncoding.ALP`
    - The `FILE` logical type: a group referencing inline or external bytes is read as a validated `PqFile` and written with `setFile` and `addFile`
    - Field ids (`field_id`, used by Iceberg, Delta Lake and DuckLake) are exposed on `ColumnSchema` and `SchemaNode`, kept when a file is rewritten with a schema read from another, and set through `FileSchema.Builder`
    - `INT96` columns declaring `INT96_TIMESTAMP_ORDER` are pruned by their statistics
- A read's CPU work runs on its `HardwoodContext`'s pool and its I/O on virtual threads: `HardwoodContext.create(n)` bounds all decoding to `n` threads, and prefetches and footer loads no longer use the JVM's common pool
- Faster remote reads
    - An S3 object of up to 256 KiB is read in the one request that opens it; for a larger file the footer and page index come with that request
    - A multi-file read opens the next files ahead, fetching their footers and first data while it reads earlier ones, and holds only the footers of the files it is reading
    - Requests are cut at 16 MiB so decoding starts after the first one arrives, and a range read whose connection fails midway re-requests only the missing bytes
    - `S3Source.Builder.anonymous()` reads public buckets with unsigned requests
- Faster local reads: uncompressed `PLAIN` fixed-width values are read where they lie in the page, row readers projecting only top-level primitives and non-repeated structs take the flat path, and no humongous G1 objects are allocated for batches, dictionaries or decompressed pages on heaps below 4 GB
- Correctness fixes
    - A read that can no longer make progress, or whose decode runs out of heap, fails instead of hanging
    - `DATA_PAGE` levels in the deprecated `BIT_PACKED` encoding are read correctly instead of yielding wrong nulls and nesting
    - Shredded Variants: arrays with null or empty shredded objects read correctly, rows setting both `value` and `typed_value` for a non-object are rejected, and an unsupported spec version is refused
    - `getValue` on `INT(8)` and `INT(16)` refuses a stored value outside the annotation's range instead of returning its low bits
    - Sibling fields sharing a name are each reachable by index, and lookups by that name no longer silently pick one of them
    - Creating a writer on an `OutputFile` another writer has used fails instead of overwriting the published file
    - In a GraalVM native image, read failures keep the exception type they have on the JVM
- CLI
    - `--log-file` captures Hardwood's detailed logging, such as the byte ranges an S3 read fetches, for every command that reads a file
    - S3 reads fall back to unsigned requests when no credentials are configured
    - `print` and `convert` report how many rows they wrote when a read fails part-way
    - BSON values render as bytes, and `print`, `convert` and `dive` render every value the same way

**Breaking Changes:**

- The experimental method `ColumnReader.getBinaryValues()` returns a `BinaryValues` giving each value as a window (`buffer(i)`, `start(i)`, `end(i)`) instead of a `byte[]` ([#1578](https://github.com/hardwood-hq/hardwood/issues/1578))
- `ColumnSchema` and `SchemaNode.GroupNode` take a last record component `fieldId`, so schemas differing only in field ids are no longer equal; `SchemaNode.PrimitiveNode` has the single component `column`, and its other accessors remain ([#957](https://github.com/hardwood-hq/hardwood/issues/957), [#1892](https://github.com/hardwood-hq/hardwood/issues/1892))
- `LogicalType` permits the new `FileType`, and `Encoding` has the new constant `ALP`; an exhaustive `switch` over them needs a case for each ([#1413](https://github.com/hardwood-hq/hardwood/issues/1413), [#581](https://github.com/hardwood-hq/hardwood/issues/581))
- A lookup by a name that several sibling fields share, such as `getColumn(String)`, `FileSchema.getField` or a row accessor, throws `IllegalArgumentException` instead of returning the first or last match; a projection by such a name selects all of them ([#1744](https://github.com/hardwood-hq/hardwood/issues/1744))
- `FileSchema.fromSchemaElements` refuses an element list whose child counts do not describe its elements, a primitive element that declares children, and annotations a field cannot carry ([#1890](https://github.com/hardwood-hq/hardwood/issues/1890))
- The native CLI is no longer published for macOS on Intel; use the Docker image there ([#1606](https://github.com/hardwood-hq/hardwood/issues/1606))

**Changes to 1.1 preview API:**

Code written against 1.1.0.Beta1 to Beta3 needs the following changes in addition to the Breaking Changes above; the [API changes](/api-changes/1.1.0.CR1/) list every signature.

- `WriterConfig.Builder.codec(CompressionCodec)` is replaced by `compression(Compression)`, `codec()` by `compression()`, and `DEFAULT_CODEC` by `DEFAULT_COMPRESSION` ([#1530](https://github.com/hardwood-hq/hardwood/issues/1530))
- `FileSchema.Builder` and `StructBuilder` declare leaves with `primitive(...)` instead of `addColumn(...)`, and `Builder`, `StructBuilder` and `ElementBuilder` set a leaf's or map key's logical type and type length in a `PrimitiveBuilder` lambda, as in `primitive("name", BYTE_ARRAY, REQUIRED, col -> col.logicalType(LogicalType.string()))`; the `primitive(...)` and `map(...)` overloads taking a `typeLength` or `LogicalType` are removed ([#957](https://github.com/hardwood-hq/hardwood/issues/957), [#1883](https://github.com/hardwood-hq/hardwood/issues/1883))
- The `SchemaElement` factories `root`, `group`, `primitive` and `fixedLengthPrimitive` are removed; build a schema with `FileSchema.builder(...)` ([#1951](https://github.com/hardwood-hq/hardwood/issues/1951))
- `LogicalType.nullType()` is renamed to `unknown()` ([#1890](https://github.com/hardwood-hq/hardwood/issues/1890))
- The writer refuses `setString` on a `BYTE_ARRAY` column without a `STRING` annotation (use `setBinary`), and `FileSchema.Builder` refuses null arguments, sibling fields of one name and annotations a field cannot carry where they are declared ([#1890](https://github.com/hardwood-hq/hardwood/issues/1890))
- A `LIST`, `MAP` or `FILE` shape the format forbids, including a `MAP` with an optional or missing key, fails with `IllegalArgumentException` instead of `UnsupportedOperationException`, and a schema without columns with `UnsupportedOperationException` ([#1899](https://github.com/hardwood-hq/hardwood/issues/1899))
- `ColumnEncoding` has the new constant `ALP`; an exhaustive `switch` over it needs a case for it ([#581](https://github.com/hardwood-hq/hardwood/issues/581))
- `InMemoryOutputFile.create()`, `close()` and `discard()` declare `IOException`, and `position()` throws `IllegalStateException` after `close()` ([#1747](https://github.com/hardwood-hq/hardwood/issues/1747))
- `ColumnReader.getBinaryStarts()` and `getBinaryEnds()` are removed; `getBinaryValues()` gives each value's `buffer(i)`, `start(i)` and `end(i)` ([#1578](https://github.com/hardwood-hq/hardwood/issues/1578))

**Deprecations:**

- `HardwoodContext.executor()` is deprecated for removal in the next major release, without a replacement ([#1581](https://github.com/hardwood-hq/hardwood/issues/1581))
- `dev.hardwood.jfr.PrefetchMissEvent` is deprecated for removal in the next major release; Hardwood never emits it, and `dev.hardwood.BatchWait` reports the consumer's stalls ([#1042](https://github.com/hardwood-hq/hardwood/issues/1042))

**Security:**

- A crafted file cannot make a read allocate without bound or fail with an unchecked JDK exception: a size, count, length or level its bytes cannot back fails the read with `ParquetReadException` before anything is allocated from it, and a page larger than the read's memory limit fails before it is decoded or fetched. The footer and page-header decoding are fuzzed ([#610](https://github.com/hardwood-hq/hardwood/issues/610), [#1644](https://github.com/hardwood-hq/hardwood/issues/1644), [#1685](https://github.com/hardwood-hq/hardwood/issues/1685), [#1715](https://github.com/hardwood-hq/hardwood/issues/1715), [#1842](https://github.com/hardwood-hq/hardwood/issues/1842))
- An S3 read fails when the object was replaced since the file was opened, and a response body that stalls for `requestTimeout` is retried instead of blocking the read ([#1686](https://github.com/hardwood-hq/hardwood/issues/1686), [#1393](https://github.com/hardwood-hq/hardwood/issues/1393))
- The native CLI is built with GraalVM CE 25 and embeds JDK 25.0.4.1, the Docker image is based on Fedora 44, and CLI releases publish `checksums_sha256.txt` and build provenance attestations, verifiable with `gh attestation verify` ([#1607](https://github.com/hardwood-hq/hardwood/issues/1607))

See the [1.1.0.CR1 milestone](https://github.com/hardwood-hq/hardwood/milestone/10?closed=1) on GitHub for the full list of resolved issues.

Thank you to all contributors to this release: [Alex Petrov](https://github.com/ifesdjeen), [Alex Sorokoumov](https://github.com/Gerrrr), [André Rouél](https://github.com/arouel), [Arnab Nandy](https://github.com/arnabnandy7), [Chandan Dhamande](https://github.com/nitrogen404), [Fawzi Essam](https://github.com/iifawzi), [Gunnar Morling](https://github.com/gunnarmorling), [Jeronimo López](https://github.com/jerolba), [Manish](https://github.com/mghildiy), [Matt Van Horn](https://github.com/mvanhorn), [Matthew Mullett](https://github.com/25mattm), [Omar Abdelmawgoud](https://github.com/OmarAbdelmawgoud5), [ommkd](https://github.com/ommkd), [Perfloop Agent](https://github.com/perfloop-agent), [Rion Williams](https://github.com/rionmonster).

## 1.1.0.Beta3 (2026-10-21)

**Bug fixes:**

- The native CLI rejects TLS certificates issued under intermediate CAs whose private keys have been compromised, including with a trust store passed via `-Djavax.net.ssl.trustStore` ([#1604](https://github.com/hardwood-hq/hardwood/issues/1604))

## 1.1.0.Beta2 (2026-10-11)

[Announcement blog post](https://www.morling.dev/blog/row-group-control-faster-s3-reads-footer-caching-hardwood-1-1-0-beta2/) ·
[API changes](/api-changes/1.1.0.Beta2/)

Highlights of this release:

- Written files support row-group and page skipping
    - A page index (`ColumnIndex` and `OffsetIndex`) per column chunk, with pages of at most `WriterConfig.pageTargetRows` records
    - Split-block Bloom filters for the columns named in `WriterConfig.Builder.bloomFilter(...)`
    - `encoding_stats`, so a reader can prune row groups by their dictionary
- Further writer additions
    - `ParquetFileWriter.endRowGroup()` closes a row group at a boundary the caller chooses
    - `ParquetFileWriter.abort()` abandons a write, and `close()` after a failed write discards the output instead of publishing a partial file
    - `RowWriter.tryWriteRow(...)` reports a rejected record without failing the writer
    - `OutputFile.inMemory()` writes a file to memory
- Fewer requests on remote reads
    - The page index is fetched for the projected columns only, for several row groups per request
    - Bloom filters are fetched as the read reaches a row group, together with those of the neighbouring row groups
    - Opening a file takes no separate request for the leading magic, and the next row group's first chunk is prefetched
    - A multi-file read opens each file as it reaches it, so time to first row no longer grows with the number of files
- Parsed footers can be reused across readers
    - A `MetadataSource` installed through `HardwoodContext.builder()` supplies the parsed footer of every file a reader opens, so a cached footer spares the footer read and parse
    - `InputFile.identity()` (the S3 ETag, or size, modification time and file key locally) detects a changed file, which raises `StaleMetadataException`
- Dictionary-encoded batches on `ColumnReader`
    - `getDictionaryIds()` and `getBinaryDictionary()` expose each value's dictionary entry for `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` and `INT96` columns
    - Binary values are read through per-value views into the batch buffer, built only when read; `getStrings()` no longer spends most of a read in garbage collection under G1
- Filter predicates
    - One literal rule for every predicate: a literal must be a value the column's accessors return, adding `byte[]`, `LocalDateTime`, `PqInterval` and `Instant` on `INT96` columns, and `in` for every literal type but `boolean`
    - `isNull` and `isNotNull` on struct, `LIST` and `MAP` groups
    - A binary predicate is evaluated once per dictionary entry instead of once per row
    - A filter-only column is not read in row groups whose statistics prove every row matches, and row groups and pages that hold only `NaN` are skipped
    - `in` and `notIn` in the parquet-java compatibility `FilterApi`
- A reworked exception model: `IOException` signals a transport failure only, a corrupt file raises the unchecked `ParquetReadException`, and read failures name the row group and column they occurred in
- `TIMESTAMP` columns over `FIXED_LEN_BYTE_ARRAY(12)`, spanning the years 0001 to 9999 at nanosecond precision, are read, filtered and written
- Correctness fixes
    - Filters, including parquet-java filters through the compatibility layer, no longer return wrong rows for `NaN` values, unsigned integers, inverted min/max bounds, statistics in an unknown sort order, and byte literals on `DECIMAL` and `FLOAT16` columns
    - `INTERVAL` columns written by parquet-java are read as `INTERVAL` instead of as `NULL` columns
    - A column whose name contains a dot can be projected
    - `byteRange` on a multi-file reader applies to every file, not the first one only
    - A statistics value larger than 1 KB no longer fails the read
    - S3 requests to an endpoint that names its default port no longer fail with `SignatureDoesNotMatch`
    - Metadata lacking a required field raises `ParquetReadException` instead of being read with default values
- CLI
    - `dive` jumps to a given row or row group, and `print` and `convert` select the same rows with `--skip` and `--row-group`
    - `print`, `convert`, `inspect` and `dive` render values of a logical type identically; `convert --format json` writes nested values as JSON objects and arrays
    - `schema -F AVRO` and `-F PROTO` emit schemas that Avro and `protoc` accept
    - The native binary reads `ZSTD`-compressed files

**Breaking Changes:**

- Index-based accessors on `ColumnReaders`, `RowReader` and `PqStruct` follow the order `ColumnProjection.columns(...)` names the columns in, a nested field is selected by its full path only, and `getProjectedColumnNames()` returns a `List` ([#1066](https://github.com/hardwood-hq/hardwood/issues/1066))
- A corrupt file raises `ParquetReadException` (unchecked; `SchemaIncompatibleException` extends it) instead of `IOException`, and the reader's build, iteration and close methods declare `IOException` for transport failures; see [Error Handling](reference/error-handling.md) for every condition and its type ([#1104](https://github.com/hardwood-hq/hardwood/issues/1104))
- A filter literal must be a value the column's accessors return, such as a `String` for text columns only and an `Instant` for UTC timestamps only; other literals throw `IllegalArgumentException` when the predicate is resolved. `FilterPredicate.SignedBinaryColumnPredicate` is removed, and `inStrings` is deprecated in favour of `in(String, String...)` ([#1198](https://github.com/hardwood-hq/hardwood/issues/1198), [#1190](https://github.com/hardwood-hq/hardwood/issues/1190))
- `getString` throws `IllegalArgumentException` on a column that does not hold text ([#1196](https://github.com/hardwood-hq/hardwood/issues/1196))
- `LogicalType.DecimalType` takes its precision before its scale; the static factories such as `LogicalType.decimal(18, 2)` are the documented way to construct a logical type ([#1074](https://github.com/hardwood-hq/hardwood/issues/1074))
- `ColumnReader.getBinaryOffsets()` is replaced by `getBinaryStarts()` and `getBinaryEnds()`, and a batch ends at every row-group boundary when the read includes a binary column ([#1416](https://github.com/hardwood-hq/hardwood/issues/1416), [#513](https://github.com/hardwood-hq/hardwood/issues/513))
- A `ColumnReader` obtained from `ColumnReaders` advances only through `ColumnReaders.nextBatch()` ([#1336](https://github.com/hardwood-hq/hardwood/issues/1336))
- `InputFile.of(ByteBuffer)` and `ofBuffers(...)` read a buffer from its position to its limit, so a buffer filled with `put(...)` must be flipped first ([#1343](https://github.com/hardwood-hq/hardwood/issues/1343))
- `convert --format json` writes nested values as JSON objects and arrays instead of strings, and `convert --format csv` writes a list or map cell as JSON text ([#1021](https://github.com/hardwood-hq/hardwood/issues/1021))
- The `dev.hardwood.RowGroupFilter` JFR event counts row groups dropped by statistics only; Bloom filter and dictionary drops have their own events ([#735](https://github.com/hardwood-hq/hardwood/issues/735), [#1259](https://github.com/hardwood-hq/hardwood/issues/1259))

See the [1.1.0.Beta2 milestone](https://github.com/hardwood-hq/hardwood/milestone/9?closed=1) on GitHub for the full list of resolved issues.

Thank you to all contributors to this release: [Arnab Nandy](https://github.com/arnabnandy7), [Chandan Dhamande](https://github.com/nitrogen404), [Doug Hoard](https://github.com/dhoard), [Fawzi Essam](https://github.com/iifawzi), [Fhatuwani Sikhwari](https://github.com/Fhatu12), [Gunnar Morling](https://github.com/gunnarmorling), [Kohinoor Gupta](https://github.com/kogupta), [Mingjie Zhao](https://github.com/ZhaoMJ), [Mohamed Ibrahim Elsawy](https://github.com/mohamedibrahim54), [Movindu Jayathilake](https://github.com/MovinduJay), [Shril Kumar](https://github.com/shril).

## 1.1.0.Beta1 (2026-08-31)

[Announcement blog post](https://www.morling.dev/blog/parquet-file-write-support-bloom-filters-improved-performance-hardwood-1-1-0-beta1/) ·
[API changes](/api-changes/1.1.0.Beta1/)

Highlights of this release:

- Parquet files can be written — `RowWriter` a record at a time, `ColumnWriter` in aligned batches of primitive arrays — over the full type system, nested structs, lists and maps included
    - Every encoding and compression codec the format defines, bar the deprecated `LZ4` framing and `LZO`
    - Statistics and `nan_count` are written, page indexes and `SizeStatistics` are not
    - Footer key-value metadata and the `created_by` identifier are set on `ParquetFileWriter`, at any point until `close()` writes the footer
- Two push-down sources for skipping non-matching row groups
    - A column's Bloom filter, for `eq` and `in` predicates
    - The chunk's dictionary page, when its encoding stats show every data page is dictionary-encoded
- Further improved read performance
    - A fast path for clean [fixed-length `LIST` pages](https://www.morling.dev/blog/fast-path-for-fixed-length-lists-in-parquet/)
    - Bulk-unpacked `DELTA_BINARY_PACKED` miniblocks
    - Dictionary indices of 9 to 32 bits read a word at a time
    - All-present definition levels are no longer materialized
- Multi-file reading
    - Columns are matched by field path, so files may declare them in any order; a real mismatch raises `SchemaIncompatibleException` as the file opens
    - Physical `skip(N)` is a true global offset across the concatenated files
    - Per-file metadata through `ParquetFileReader.getFileCount()` and `getFileMetaData(int)`
- Correctness fixes
    - A `FilterPredicate` naming a group resolved to one of its leaves and returned wrong rows; a group name is now rejected with `IllegalArgumentException` at reader creation
    - Batch sizing accounts for list fan-out, so a high-fan-out repeated column no longer drains a whole file into a single batch and exhausts the heap
    - `GeographyType.algorithm` is read as the enum it is, so a non-spherical geography column no longer reads as `SPHERICAL`; an unrecognized algorithm reports `EdgeInterpolationAlgorithm.UNKNOWN`
    - Variant decoding bounds-checks lengths, guards 32-bit offset arithmetic, corrects the metadata offset-size read, and limits nesting depth
- The `hardwood` CLI moves from picocli/Quarkus to aesh, which makes it start faster
    - `dive` navigation is unified across all screens: every key moves the cursor, `▶` marks what `Enter` acts on
    - `convert` preserves types and NULL — JSON writes real scalars and `null`, CSV an empty field, with `--null-string` to override
    - `info` surfaces the file's key-value metadata

**Breaking Changes:**

- `Validity` moves to `dev.hardwood`, and `ColumnReader`'s raw `getDefinitionLevels()` / `getRepetitionLevels()` give way to the layer model ([#9](https://github.com/hardwood-hq/hardwood/issues/9), [#749](https://github.com/hardwood-hq/hardwood/issues/749))
- `ColumnIndex.nullPages()` and `nullCounts()` return `boolean[]` and `long[]`, and the canonical constructors of `ColumnChunk`, `ColumnIndex`, `ColumnMetaData` and `OffsetIndex` take further components — reading these records is unaffected, constructing them directly is not ([#607](https://github.com/hardwood-hq/hardwood/issues/607), [#856](https://github.com/hardwood-hq/hardwood/issues/856), [#903](https://github.com/hardwood-hq/hardwood/issues/903))
- `S3InputFile.length()` declares `throws IOException`, matching the `InputFile` contract ([#1072](https://github.com/hardwood-hq/hardwood/issues/1072))
- `hardwood help <command>` is removed — use `hardwood <command> --help` ([#686](https://github.com/hardwood-hq/hardwood/issues/686))

See the [1.1.0.Beta1 milestone](https://github.com/hardwood-hq/hardwood/milestone/8?closed=1) on GitHub for the full list of resolved issues.

Thank you to all contributors to this release: [Arnab Nandy](https://github.com/arnabnandy7), [Chandan Dhamande](https://github.com/nitrogen404), [Fawzi Essam](https://github.com/iifawzi), [Florian Meyer](https://github.com/alloutflo), [Gunnar Morling](https://github.com/gunnarmorling), [Hursh](https://github.com/hurshh), [Hyungun](https://github.com/chlgusrbs0), [Joshua Buss](https://github.com/chicagobuss), [Karen Barseghyan](https://github.com/k-barseghyan), [Kohinoor Gupta](https://github.com/kogupta), [Mehmet Turac](https://github.com/mturac), [Mingjie Zhao](https://github.com/ZhaoMJ), [Morax](https://github.com/fzlzjerry), [Nikulin Nikita](https://github.com/w3lld1), [Rion Williams](https://github.com/rionmonster), [Sebastian Legarraga](https://github.com/slegarraga), [Semyon Sinchenko](https://github.com/SemyonSinchenko), [Shaik Sameer](https://github.com/samsameer2804-cloud), [Shril Kumar](https://github.com/shril), [Ståle Pedersen](https://github.com/stalep).

## 1.0.1.Final (2026-10-21)

**Bug fixes:**

- The native CLI rejects TLS certificates issued under intermediate CAs whose private keys have been compromised, including with a trust store passed via `-Djavax.net.ssl.trustStore` ([#1604](https://github.com/hardwood-hq/hardwood/issues/1604))

## 1.0.0.Final (2026-06-25)

[Announcement blog post](https://www.morling.dev/blog/hardwood-1-0-fast-lightweight-apache-parquet-reader-for-the-jvm/) · [API changes](/api-changes/1.0.0.Final/)

Highlights of this release:

- Float and double row group and page pruning honors the file's column order, with `ColumnOrder` surfaced on the API; `ResolvedPredicate` float/double convenience constructors are now public
- Legacy list encodings from older writers — un-annotated repeated fields and 2-level lists — are recognized as lists
- MAP columns without a value field (key-only `key_value` groups) are read instead of throwing
- Sub-field projections into MAP values and VARIANT groups pull in the structural columns they require (the MAP's key, every VARIANT leaf)
- `AvroRowReader` honors column projections, and DECIMAL, UUID, UINT_32, and FIXED columns now read correctly
- The multi-file `Hardwood` entry point accepts a caller-supplied `HardwoodContext`, for control over decoder thread-pool sizing and sharing a context across readers
- `ColumnReader` and `RowReader` `close()` methods are now idempotent, fixing a close-time performance regression from Beta2
- Logical types render as Parquet-style annotation tokens (e.g. `STRING`)
- `hardwood dive` fails fast with a clear message when stdout is not an interactive terminal
- `hardwood print` short option names follow the conventional single-dash, single-character form (`-s`, `-w`, `-i`, `-d`); `--transpose` is long-only

See the [1.0.0.Final milestone](https://github.com/hardwood-hq/hardwood/milestone/7?closed=1) on GitHub for the full list of resolved issues.

Thank you to all contributors to this release: [Fawzi Essam](https://github.com/iifawzi), [Gunnar Morling](https://github.com/gunnarmorling), [Leo Chashnikov](https://github.com/RayanRal), [Mohamed Ibrahim Elsawy](https://github.com/mohamedibrahim54), [Rion Williams](https://github.com/rionmonster), [Yash Priyadarshan](https://github.com/yashpriyadarshan).

## 1.0.0.CR2 (2026-06-07)

[API changes](/api-changes/1.0.0.CR2/)

Highlights of this release:

- **Breaking:** `RowReaderBuilder.firstRow()` renamed to `skip()`, which now composes with a filter as a logical `OFFSET` — rows are skipped after the filter is applied
- Docker distribution of the `hardwood` CLI, published as a multi-arch image to GHCR
- Configurable read batch size for `ColumnReader` / `ColumnReaders`, with the default now sized adaptively from the projected column widths instead of a fixed record count
- TIMESTAMP accessors honor `isAdjustedToUTC`, with dedicated accessors for local (non-UTC) timestamps
- Stricter metadata validation: negative sizes, counts, and offsets are rejected, as are shredded Variant objects repeating a field across `typed_value` and `value`
- NaN-safe row group and page pruning for `float` and `double` columns
- Duplicate map keys resolve to the last value per the Parquet spec
- `head()` with a filter caps matched rows rather than scanned rows
- Legacy MAP columns from older parquet-mr / Hive / Impala writers (which annotate only the inner `key_value` group) are now recognized as maps
- API change reports are now published alongside the JavaDoc on the website

See the [1.0.0.CR2 milestone](https://github.com/hardwood-hq/hardwood/milestone/4?closed=1) on GitHub for the full list of resolved issues.

Thank you to all contributors to this release: [Alexei Zenin](https://github.com/AlexeiZenin), [Fawzi Essam](https://github.com/iifawzi), [Gunnar Morling](https://github.com/gunnarmorling), [Mohamed Ibrahim Elsawy](https://github.com/mohamedibrahim54).

## 1.0.0.CR1 (2026-05-31)

[Announcement blog post](https://www.morling.dev/blog/improved-column-reader-api-geospatial-support-hardwood-1-0-0-cr1-available/) · [API changes](/api-changes/1.0.0.CR1/)

Highlights of this release:

- **Breaking:** `ColumnReader` rebuilt around a layer model, with per-layer validity, offsets, and real-item-only sizing for nested data (see the [Layer Model](how-to/column-reader.md#reading-nested-data-the-layer-model) docs); `ColumnReader` is now marked `@Experimental`
- More performant evaluation of multi-column filter expressions
- Split-aware reading via `RowGroupPredicate.byteRange(...)`, for Hadoop-style split integrations
- Coordinated multi-column reads via `ColumnReaders.nextBatch()` / `getRecordCount()`
- Richer `RowReader` value model: by-index field access on `PqStruct`, key-based lookup and typed accessors on `PqMap`, typed `List` accessors on `PqList`, and additional variant accessors
- Float16 logical type support (readable values and filter predicates) and recognition of the `NullType` logical annotation
- First-cut geospatial support (GEOMETRY/GEOGRAPHY logical types and bounding-box metadata)
- Reading of local files larger than 2 GB
- CLI: exhaustive logical-type formatting; `hardwood dive`: faster navigation of large collections and corrected "go to latest" in the data preview

See the [1.0.0.CR1 milestone](https://github.com/hardwood-hq/hardwood/milestone/6?closed=1) on GitHub for the full list of resolved issues.

Thank you to all contributors to this release: [Carlos Sousa](https://github.com/CarlosEduR), [Fawzi Essam](https://github.com/iifawzi), [Gunnar Morling](https://github.com/gunnarmorling), [Manish](https://github.com/mghildiy), [Mohamed Ibrahim Elsawy](https://github.com/mohamedibrahim54), [muhannd Sayed](https://github.com/muhannd2004), [polo](https://github.com/polo7), [Prashant Khanal](https://github.com/prshnt), [Rion Williams](https://github.com/rionmonster), [Said Boudjelda](https://github.com/bmscomp).

## 1.0.0.Beta2 (2026-04-29)

[Announcement blog post](https://www.morling.dev/blog/variant-support-interactive-parquet-file-tui-hardwood-1.0.0.beta2-is-out/) · [API changes](/api-changes/1.0.0.Beta2/)

Highlights of this release:

- Interactive `hardwood dive` TUI for exploring Parquet files
- Parquet Variant logical type, including shredded reassembly
- Additional logical types: INTERVAL, MAP/LIST, INT96 timestamps
- Faster reads via a parallel per-column pipeline and per-column in-page row skipping
- Reduced S3 traffic via byte-range caching, coalesced GETs, and small-column fetches
- Unified reader API based on builders
- CLI with reorganized `inspect` subcommands

See the [1.0.0.Beta2 milestone](https://github.com/hardwood-hq/hardwood/milestone/3?closed=1) on GitHub for the full list of resolved issues.

Thank you to all contributors to this release: [André Rouél](https://github.com/arouel), [Brandon Brown](https://github.com/brbrown25), [Bruno Borges](https://github.com/brunoborges), [Fawzi Essam](https://github.com/iifawzi), [Gunnar Morling](https://github.com/gunnarmorling), [Manish](https://github.com/mghildiy), [polo](https://github.com/polo7), [Rion Williams](https://github.com/rionmonster), [Sabarish Rajamohan](https://github.com/sabarish98), [Trevin Chow](https://github.com/tmchow).

## 1.0.0.Beta1 (2026-04-02)

[Announcement blog post](https://www.morling.dev/blog/hardwood-reaches-beta-s3-predicate-push-down-cli/) · [API changes](/api-changes/1.0.0.Beta1/)

Highlights of this release:

- S3 and remote object store support with coalesced reads
- CLI tool for inspecting and querying Parquet files
- Avro `GenericRecord` support via the `hardwood-avro` module
- Row group filtering with predicate push-down and page-level column index filtering
- `InputFile` abstraction for pluggable file sources
- S3 support and filtering in the parquet-java compatibility layer
- Project documentation site

See the [1.0.0.Beta1 milestone](https://github.com/hardwood-hq/hardwood/milestone/1?closed=1) on GitHub for the full list of resolved issues.

Thank you to all contributors to this release: [Arnav Balyan](https://github.com/ArnavBalyan), [Brandon Brown](https://github.com/brbrown25), [Gunnar Morling](https://github.com/gunnarmorling), [Manish](https://github.com/mghildiy), [Nicolas Grondin](https://github.com/ngrondin), [Rion Williams](https://github.com/rionmonster), [Romain Manni-Bucau](https://github.com/rmannibucau), [Said Boudjelda](https://github.com/bmscomp).

## 1.0.0.Alpha1 (2026-02-26)

[Announcement blog post](https://www.morling.dev/blog/hardwood-new-parser-for-apache-parquet/)

Highlights of this release:

- Zero-dependency Parquet file reader for Java
- Row-oriented and columnar read APIs
- Support for flat and nested schemas (lists, maps, structs)
- All standard encodings (RLE, DELTA_BINARY_PACKED, DELTA_BYTE_ARRAY, BYTE_STREAM_SPLIT, etc.)
- Compression: Snappy, ZSTD, LZ4, GZIP, Brotli
- Projection push-down, parallel page pre-fetching, and memory-mapped file I/O
- Multi-file reader and `parquet-java` compatibility layer
- Optional Vector API acceleration on Java 22+
- JFR events for observability
- BOM for dependency management

Thank you to all contributors to this release: [Andres Almiray](https://github.com/aalmiray), [Gunnar Morling](https://github.com/gunnarmorling), [Rion Williams](https://github.com/rionmonster).
