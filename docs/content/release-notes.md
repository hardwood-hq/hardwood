<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Release Notes

See [GitHub Releases](https://github.com/hardwood-hq/hardwood/releases) for downloads and more information.

## 1.1.0-SNAPSHOT

- Physical `skip(N)` on multi-file row readers is now a true global offset over the concatenated input files; skipped files have their footers read for row counts, but their data pages are not decoded.

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
