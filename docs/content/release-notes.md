<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Release Notes

See [GitHub Releases](https://github.com/hardwood-hq/hardwood/releases) for downloads and more information.

## Unreleased

- **Breaking:** `ColumnReader` rebuilt around a layer model. Each non-leaf node along the schema chain contributes zero or one layer — `OPTIONAL` groups become `STRUCT` layers, `LIST`/`MAP`-annotated groups become `REPEATED` layers — and the API is structured per-layer rather than per-Parquet-repetition-level. Validity is returned as a dedicated [Validity](/api/latest/dev/hardwood/reader/Validity.html) sealed type with two implementations: a `Validity.NO_NULLS` singleton for the O(1) no-nulls fast path, and a `BitSet`-backed wrapper otherwise. Predicates are `isNull(i)` / `isNotNull(i)` / `hasNulls()`. `LIST`/`MAP` layers encode "empty" as `offsets[r+1] - offsets[r] == 0` and "null" as a cleared validity bit, dropping the separate empty-marker bitmap. The leaf array and layer offsets are sized to **real items only** — phantom slots from null/empty parents are excluded. Permitted by `ColumnReader`'s existing `@Experimental` annotation; no deprecation cycle is run. Subsumes [#436](https://github.com/hardwood-hq/hardwood/issues/436)'s struct-null-vs-leaf-null disambiguation. See the [Layer Model](usage/column-reader.md#reading-nested-data-the-layer-model) section and [#430](https://github.com/hardwood-hq/hardwood/issues/430).

    Migration table:

    | Before | After |
    |---|---|
    | `BitSet nulls = reader.getElementNulls();` | `Validity v = reader.getLeafValidity();` |
    | `if (nulls != null && nulls.get(i)) skip;` | `if (v.isNull(i)) skip;` |
    | `if (nulls == null \|\| !nulls.get(i)) keep;` | `if (v.isNotNull(i)) keep;` |
    | `nulls == null` ⇒ no nulls in batch | `!v.hasNulls()` ⇒ no nulls in batch |
    | `reader.getLevelNulls(level)` (returns `BitSet`, set bit = null) | `reader.getLayerValidity(layer)` (returns `Validity`) |
    | `reader.getEmptyListMarkers(level)` | encoded as `offsets[i+1] - offsets[i] == 0` |
    | `reader.getOffsets(level)` | `reader.getLayerOffsets(layer)` (sentinel-suffixed; length `count + 1`) |
    | `reader.getNestingDepth()` | `reader.getLayerCount()` |
    | `byte[][] reader.getBinaries()` (primary) | `byte[] reader.getBinaryValues()` + `int[] reader.getBinaryOffsets()` (primary). `getBinaries()` is retained as a convenience accessor that allocates per row. |
    | `reader.getStrings()` (primary) | retained as a convenience accessor; hot loops should consult `getBinaryValues()` + `getBinaryOffsets()` directly. |
    | `getBinaries()[recordIndex]` / `getStrings()[recordIndex]` | the returned array is sized to `getValueCount()` (real leaf count), not `getRecordCount()`. For flat columns the two coincide; under nesting (e.g. `list<string>`) they differ — index through the appropriate layer offsets rather than by record. |

    Layer indexing also changes shape: a column whose chain is `optional group a { list<list<int>> b }` moves from "two repetition levels" to "three layers" (one `STRUCT` for `a`, two `REPEATED` for `b`). Callers that index numerically must be re-keyed.
- `RowGroupPredicate` for split-aware row group selection. Pass `RowGroupPredicate.byteRange(start, end)` to any reader builder's `filter(...)` to restrict reading to the row groups whose midpoint falls in the given file byte range — the standard split convention used by Hadoop-style integrations (Flink `BulkFormat`, Spark file source, …). Combines with `FilterPredicate` via intersection. See [Split-Aware Reading](usage/query-controls.md#split-aware-reading) and [#431](https://github.com/hardwood-hq/hardwood/issues/431).
- Coordinated `ColumnReaders.nextBatch()` and `ColumnReaders.getRecordCount()` for multi-column reads. A single call advances every underlying reader in lockstep, returns `false` when any is exhausted, and validates that the readers' record counts agree. Replaces the prior `col0.nextBatch() & col1.nextBatch() & …` idiom and gives consumers structural alignment instead of an implicit invariant. See [Reading Multiple Columns](usage/column-reader.md#reading-multiple-columns), [#434](https://github.com/hardwood-hq/hardwood/issues/434), and the related contract gap [#61](https://github.com/hardwood-hq/hardwood/issues/61).
- `SchemaNode.GroupNode.getMapKey()` / `getMapValue()` for navigating MAP groups, symmetric with the existing `getListElement()`. Returns the key and value nodes from the standard `map.key_value.key` / `map.key_value.value` encoding; returns `null` for non-MAP or malformed groups, matching `getListElement()`'s behavior. See [#435](https://github.com/hardwood-hq/hardwood/issues/435).

## 1.0.0.Beta2 (2026-04-29)

[Announcement blog post](https://www.morling.dev/blog/variant-support-interactive-parquet-file-tui-hardwood-1.0.0.beta2-is-out/)

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

[Announcement blog post](https://www.morling.dev/blog/hardwood-reaches-beta-s3-predicate-push-down-cli/)

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
