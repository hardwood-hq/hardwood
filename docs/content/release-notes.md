<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Release Notes

See [GitHub Releases](https://github.com/hardwood-hq/hardwood/releases) for downloads and more information.

## 1.1.0.Beta2 (2026-10-06)

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

Thank you to all contributors to this release: [Chandan Dhamande](https://github.com/nitrogen404), [Doug Hoard](https://github.com/dhoard), [Fawzi Essam](https://github.com/iifawzi), [Fhatuwani Sikhwari](https://github.com/Fhatu12), [Gunnar Morling](https://github.com/gunnarmorling), [Kohinoor Gupta](https://github.com/kogupta), [Mingjie Zhao](https://github.com/ZhaoMJ), [Mohamed Ibrahim Elsawy](https://github.com/mohamedibrahim54), [Movindu Jayathilake](https://github.com/MovinduJay), [Shril Kumar](https://github.com/shril).

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
