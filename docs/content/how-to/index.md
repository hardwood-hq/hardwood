<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# How-to Guides

Read Parquet files with Hardwood — pick the guide that matches what you need:

- [**Read Row by Row**](row-reader.md) — `RowReader`, typed accessors, nested structs / lists / maps.
- [**Read Column by Column**](column-reader.md) — `ColumnReader` and `ColumnReaders`, the layer model, hot-loop patterns.
- [**Filter, Project, Limit, and Split**](query-controls.md) — predicate pushdown, column projection, row limits, split-aware reading. Apply to both reader types.
- [**Read Multiple Files as One Dataset**](multi-file.md) — `Hardwood.openAll(...)` with cross-file prefetching and a shared thread pool.
- [**Read into Avro GenericRecords**](avro.md) — `AvroRowReader` and schema conversion (`hardwood-avro` module).
- [**Read from S3**](s3.md) — object-store reading without Hadoop (`hardwood-s3` module).
- [**Read with the parquet-java API**](compat.md) — drop-in `org.apache.parquet.*` replacement (experimental).
- [**Read Variant Columns**](variant.md) — `getVariant` and the `PqVariant` API.
- [**Read Geospatial Columns**](geospatial.md) — GEOMETRY / GEOGRAPHY columns, bounding-box filter pushdown.
- [**Inspect File Metadata**](metadata.md) — file metadata, row groups, column chunks, schema introspection.

!!! example "Runnable examples"
    Each guide below links to a matching standalone example in the
    [hardwood-examples](https://github.com/hardwood-hq/hardwood-examples) repository — every example is a
    complete project you can clone and run with a single command. New to Hardwood? Start with
    [Hello Hardwood](https://github.com/hardwood-hq/hardwood-examples/tree/main/hello-hardwood).

For detailed class-level documentation, see the [JavaDoc](/api/latest/).

## Choosing a Reader

Hardwood provides two reader APIs:

- **`RowReader`** — row-oriented access with typed getters, including nested structs, lists, and maps. Best for general-purpose reading where you process one row at a time.
- **`ColumnReader`** — batch-oriented columnar access with typed primitive arrays. Best for analytical workloads where you process columns independently (e.g. summing a column, computing statistics).

For the reasoning behind the two APIs and the ergonomics-versus-throughput trade-off, see [RowReader vs. ColumnReader](../concepts/reader-models.md).

Both support column projection and predicate pushdown. Each reader has a no-arg shortcut for default reads and a builder form for filtered or limited reads:

| Reader | Shortcut | Builder |
|--------|----------|---------|
| `RowReader` | `reader.rowReader()` | `reader.buildRowReader().…build()` |
| `ColumnReader` (single) | `reader.columnReader("id")` | `reader.buildColumnReader("id").…build()` |
| `ColumnReaders` (multiple) | `reader.columnReaders(projection)` | `reader.buildColumnReaders(projection).…build()` |

To read multiple files as a single dataset with cross-file prefetching, open the `ParquetFileReader` with a list of `InputFile`s via the `Hardwood` class — see [Reading Multiple Files](multi-file.md).

For the exceptions the readers can throw and when, see [Error Handling](../reference/error-handling.md).
