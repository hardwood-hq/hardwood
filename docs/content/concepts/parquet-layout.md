<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# How a Parquet File Is Laid Out

Most of Hardwood's behavior, such as column projection, predicate pushdown, parallel decode and
split reading, follows directly from how the Parquet format arranges bytes on disk.

## The hierarchy

A Parquet file is a nested structure of row groups holding column chunks holding pages, and the
metadata that records where each piece lives sits at the *end* of the file. Laid
out as bytes on disk, from the first byte to the last:

```
+------------------------------------------------------------+
|  PAR1                       (4-byte magic, file start)     |
+------------------------------------------------------------+
|                                                            |
|   +===============  ROW GROUP 1  ======================+   |
|   |  Column Chunk A   |  Column Chunk B   |   ...      |   |
|   |  +--------------+ | +--------------+  |            |   |
|   |  | DictPage     | | | DataPage v2  |  |            |   |
|   |  | DataPage     | | | DataPage v2  |  |            |   |
|   |  | DataPage     | | | DataPage v2  |  |            |   |
|   |  +--------------+ | +--------------+  |            |   |
|   +====================================================+   |
|                                                            |
|   +===============  ROW GROUP 2  ======================+   |
|   |   ...                                              |   |
|   +====================================================+   |
|                                                            |
+------------------------------------------------------------+
|  FOOTER (Thrift)   schema | rg meta | stats | page index   |
+------------------------------------------------------------+
|  footer length (4B)  |  PAR1                               |
+------------------------------------------------------------+
```

The data comes first and the **footer** comes last: a Thrift metadata block describing every row
group and column chunk (their byte offsets, sizes, compression codecs, and statistics, plus the
schema and optional page index), followed by the footer length and a closing `PAR1`. A reader
starts at that trailing magic, steps back four bytes to read the footer length, then reads the
footer to learn where every row group, column chunk, and page lives before touching any values.

### Row group

A row group holds a contiguous range of rows, stored column by column.

### Column chunk

The data for one column within one row group, stored contiguously. Hardwood reads a column chunk
as a single in-memory region, which bounds its size; see [Limits](../reference/reader.md#limits).

### Page

A column chunk is divided into pages, and the page is where compression and encoding
happen. Each page is compressed independently, so the page is the smallest unit Hardwood
decompresses and decodes, and therefore the smallest unit it can decode in parallel or skip.

A file may carry a **Column Index** and **Offset Index**: per-page min/max statistics and byte
offsets, stored near the footer. On a remote backend like S3, a page these let Hardwood skip is
never fetched.

## Why the layout matters

| Capability | What in the layout makes it work |
|---|---|
| **Column projection** | Columns are stored in separate chunks; the footer gives each chunk's byte range, so unprojected columns are never read. |
| **[Predicate pushdown](../how-to/query-controls.md#predicate-pushdown-filter)** | Min/max statistics at the row-group level, and the Column Index at the page level, let whole row groups and individual pages be skipped before decoding. |
| **Parallel decode** | Pages are independently compressed, so they can be decompressed and decoded concurrently across a thread pool. |
| **[Split reading](../how-to/query-controls.md#split-aware-reading)** | Row groups are self-contained, so a file partitions cleanly across parallel readers at row-group boundaries. |
| **Seek / head / tail** | The footer records each row group's row count, so the reader can jump to the row group containing an absolute row without scanning earlier ones. |

## Logical structure: the schema

Orthogonal to the physical hierarchy is the **schema**, also stored in the footer. Parquet's
schema is a tree: leaf columns carry the actual values, and group nodes express nesting:
structs, lists, and maps. A leaf's position in that tree, together with Parquet's repetition and
definition levels, encodes which values belong to which (possibly null, possibly empty) parent.
How Hardwood surfaces that nesting differs between the two reader APIs; see
[RowReader vs. ColumnReader](reader-models.md).

## Further reading

- [Inspect File Metadata](../how-to/metadata.md) — read this hierarchy programmatically.
- [The Concurrency Model](concurrency-model.md) — how the independently-compressed pages become
  parallel work.
- The [Apache Parquet format specification](https://parquet.apache.org/docs/file-format/) — the
  authoritative description of the on-disk format.
