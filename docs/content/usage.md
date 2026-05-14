<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Usage

This section describes how to read Parquet files with Hardwood. Pick the page that matches what you need:

- [**Row-Oriented Reading**](usage/row-reader.md) — `RowReader`, typed accessors, nested structs / lists / maps.
- [**Column-Oriented Reading**](usage/column-reader.md) — `ColumnReader` and `ColumnReaders`, the layer model, hot-loop patterns.
- [**Predicate Pushdown, Projection, Limits, and Splits**](usage/query-controls.md) — filters, column projection, row limits, split-aware reading. Apply to both reader types.
- [**Reading Multiple Files**](usage/multi-file.md) — `Hardwood.openAll(...)` with cross-file prefetching and shared thread pool.
- [**Accessing File Metadata**](usage/metadata.md) — file metadata, row groups, column chunks, schema introspection.
- [**Variant Columns**](usage/variant.md) — `getVariant` and the `PqVariant` API.
- [**Geospatial Support**](usage/geospatial.md) — GEOMETRY / GEOGRAPHY columns, bounding-box filter pushdown.

For detailed class-level documentation, see the [JavaDoc](/api/latest/).

## Choosing a Reader

Hardwood provides two reader APIs:

- **`RowReader`** — row-oriented access with typed getters, including nested structs, lists, and maps. Best for general-purpose reading where you process one row at a time.
- **`ColumnReader`** — batch-oriented columnar access with typed primitive arrays. Best for analytical workloads where you process columns independently (e.g. summing a column, computing statistics).

Both support column projection and predicate pushdown. Each reader has a no-arg shortcut for default reads and a builder form for filtered or limited reads:

| Reader | Shortcut | Builder |
|--------|----------|---------|
| `RowReader` | `reader.rowReader()` | `reader.buildRowReader().…build()` |
| `ColumnReader` (single) | `reader.columnReader("id")` | `reader.buildColumnReader("id").…build()` |
| `ColumnReaders` (multiple) | `reader.columnReaders(projection)` | `reader.buildColumnReaders(projection).…build()` |

To read multiple files as a single dataset with cross-file prefetching, open the `ParquetFileReader` with a list of `InputFile`s via the `Hardwood` class — see [Reading Multiple Files](usage/multi-file.md).

## Error Handling

Hardwood throws specific exceptions for common error conditions:

| Exception | When |
|-----------|------|
| `IOException` | Any I/O error: invalid Parquet file (bad magic number, corrupt footer), local-disk read errors, S3 transport failures (after retry exhaustion — see [S3](s3.md)) |
| `UnsupportedOperationException` | Compression codec library not on classpath — the message names the required dependency |
| `IllegalArgumentException` | Accessing a column not in the projection, type mismatch on accessor, or invalid column name |
| `NullPointerException` | Calling a primitive accessor (`getInt`, `getLong`, etc.) on a null field without checking `isNull()` first |
| `NoSuchElementException` | Calling `next()` on a `RowReader` when `hasNext()` returns `false` |
| `IllegalStateException` | Calling `ColumnReader` accessors before `nextBatch()`, or calling nested-column methods on a flat column |
