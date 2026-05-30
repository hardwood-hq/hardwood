<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# How a Parquet File Is Laid Out

Most of Hardwood's behavior — column projection, predicate pushdown, parallel decode, split
reading, etc. — follows directly from how the Parquet format arranges
bytes on disk. Understanding that layout makes the rest of Hardwood's behavior fall out as
consequences rather than rules to memorize.

To read this hierarchy programmatically at runtime, see [Inspect File Metadata](../how-to/metadata.md).

## The hierarchy

A Parquet file is a nested structure — row groups holding column chunks holding pages — and the
metadata that records where each piece lives sits at the *end* of the file, not the start. Laid
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

The data comes first and the **footer** comes last — a Thrift metadata block describing every row
group and column chunk (their byte offsets, sizes, compression codecs, and statistics, plus the
schema and optional page index), followed by the footer length and a closing `PAR1`. A reader
starts at that trailing magic, steps back four bytes to read the footer length, then reads the
footer to learn where every row group, column chunk, and page lives — before touching any values.

### File

The unit you open. Its defining feature is that the schema and the byte offsets of everything in
the file are in the footer at the *end*, not the start. A reader seeks to the tail, reads the
footer, and from then on knows the exact byte range of every piece of data without scanning. A
read of three columns out of fifty touches only those three columns' bytes.

### Row group

A row group holds a contiguous range of rows, but stores them column by column. A 10-million-row
file might be split into ten row groups of a million rows each. Row groups are the unit of:

- **Parallelism and splitting.** Independent row groups can be decoded concurrently, and a file
  can be partitioned across parallel readers at row-group boundaries — this is what
  [split-aware reading](../how-to/query-controls.md#split-aware-reading) assigns by byte range.
- **Coarse skipping.** Each column chunk carries min/max statistics; if a row group's statistics
  prove no row can match a predicate, the entire row group is skipped before any data is read.

### Column chunk

The data for one column within one row group, stored contiguously. Because a column's values sit
together — rather than interleaved with other columns as in a row-oriented format — they
compress well (similar values adjacent) and can be read in isolation (projection). The footer
records each chunk's compressed and uncompressed size, codec, and statistics.

!!! info "2 GB column-chunk limit"
    A column chunk is addressed within Hardwood as a single in-memory region, so each chunk must
    be at most 2 GB of *compressed* data — the limit is per chunk, not per file. Local
    memory-mapped files may be arbitrarily large overall; the in-memory (`ByteBuffer`) and
    object-store backends additionally cap the *whole file* at 2 GB. Datasets larger than that
    are split across multiple files and read together — see
    [Read Multiple Files as One Dataset](../how-to/multi-file.md).

### Page

A column chunk is divided into pages, and the page is where compression and encoding actually
happen. A chunk typically begins with one **dictionary page** — the column's distinct values —
followed by **data pages** whose entries are indices into that dictionary. Each page is
compressed independently, so the page is the smallest unit Hardwood decompresses and decodes,
and therefore the smallest unit it can decode in parallel or skip.

When a file carries a **Column Index** and **Offset Index** (per-page min/max statistics and
byte offsets, stored near the footer), Hardwood can skip individual *pages* within a surviving
row group — the second tier of [predicate pushdown](../how-to/query-controls.md#predicate-pushdown-filter).
On a remote backend like S3, a skipped page is never even fetched.

## Why the layout matters

Each capability elsewhere in the docs is the layout showing through:

| Capability | What in the layout makes it work |
|---|---|
| **Column projection** | Columns are stored in separate chunks; the footer gives each chunk's byte range, so unprojected columns are never read. |
| **Predicate pushdown** | Statistics at the row-group level, and the Column Index at the page level, let whole row groups and individual pages be skipped before decoding. |
| **Parallel decode** | Pages are independently compressed, so they can be decompressed and decoded concurrently across a thread pool. |
| **Split reading** | Row groups are self-contained, so a file partitions cleanly across parallel readers at row-group boundaries. |
| **Seek / head / tail** | The footer records each row group's row count, so the reader can jump to the row group containing an absolute row without scanning earlier ones. |

## Logical structure: the schema

Orthogonal to the physical hierarchy is the **schema**, also stored in the footer. Parquet's
schema is a tree: leaf columns carry the actual values, and group nodes express nesting —
structs, lists, and maps. A leaf's position in that tree, together with Parquet's repetition and
definition levels, encodes which values belong to which (possibly null, possibly empty) parent.
How Hardwood surfaces that nesting differs between the two reader APIs — see
[RowReader vs. ColumnReader](reader-models.md).

## Further reading

- [Inspect File Metadata](../how-to/metadata.md) — read this hierarchy programmatically.
- [The Concurrency Model](concurrency-model.md) — how the independently-compressed pages become
  parallel work.
- The [Apache Parquet format specification](https://parquet.apache.org/docs/file-format/) — the
  authoritative description of the on-disk format.
