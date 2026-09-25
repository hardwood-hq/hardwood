<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# RowReader vs. ColumnReader

Hardwood offers two reader APIs over the same files: `RowReader` and `ColumnReader`. For a
decision table on which to pick, see the [How-to overview](../how-to/index.md#choosing-a-reader).

Both read the same bytes through the same pipeline and differ in what they hand to your loop.

## The trade-off: ergonomics vs. throughput

The two APIs sit at opposite ends of an ergonomics-versus-throughput spectrum, because Parquet
stores data by column (see [How a Parquet File Is Laid Out](parquet-layout.md)) and a row must be
assembled from it.

**`RowReader` optimizes for ergonomics.** Records are how most application code is written: you
want "this trip's distance and fare," not "the distance column and the fare column." To present
a row, the reader must gather the current value from each projected column and, for nested data,
reassemble structs, lists, and maps into the [`PqStruct` / `PqList` / `PqMap`](../how-to/row-reader.md)
flyweights. That reassembly, and the per-field accessor calls, cost CPU and sometimes boxing.

**`ColumnReader` optimizes for throughput.** Analytical work, such as summing a column, computing a
distribution or scanning for matches, touches one or a few columns across many rows and never
needs a whole record assembled. `ColumnReader` skips the row-assembly step entirely and gives you
the decoded values as a primitive array (`double[]`, `int[]`, …) plus a
[`Validity`](/api/latest/dev/hardwood/Validity.html) bitmap for nulls, which you iterate with no
per-element method call and no boxing. On that kind of workload it is markedly faster; the cost
is that *you* handle the layout (nulls via the bitmap, nested structure via offsets; see below),
which is more to get right.

## Nesting

`RowReader` materializes nesting into objects: a list becomes a `PqList` you iterate, a struct a
`PqStruct` you index by field, and it allocates these flyweights as you descend. `ColumnReader`
never materializes a container object: it describes nesting as validity bitmaps and offset arrays
over one flat leaf array, which you walk with zero allocation in the hot loop. See
[The Layer Model](nested-columns.md).

## Several columns means one `ColumnReaders`, not several `ColumnReader`s

A `ColumnReader` is a cursor over one column, and each one built separately is sized for itself.
Batch capacity is a byte budget divided by the widths of the columns that reader projects, so a
`BYTE_ARRAY` column batches at fewer rows than an `INT32` one over the same file, and a filtered
reader is sized for its predicate's columns as well as its own. Two readers built independently
therefore reach different rows on their *n*th batch.

That makes `while (a.nextBatch() & b.nextBatch())` the wrong shape for reading two columns of the
same rows: the loop stops when whichever reader has the larger batches runs out, and the values it
took from each on any given turn came from different rows. Neither reader reports a problem,
because neither has one: each is a complete, correct read of its own column.

`ColumnReaders` is the multi-column read: `ColumnReaders.nextBatch()` advances all its readers
together (see [Reading Multiple Columns](../how-to/column-reader.md#reading-multiple-columns)).
Reach for a bare `ColumnReader` when you want one column, and for `ColumnReaders` the moment you
want two.

## They are not exclusive

Nothing forces a single choice per file. A pipeline can open a `ColumnReader` to compute an
aggregate over one column and a `RowReader` elsewhere to materialize matching records, both
against the same file, sharing one [context and worker pool](concurrency-model.md).

## Further reading

- [Read Row by Row](../how-to/row-reader.md) — the `RowReader` API in full.
- [Read Column by Column](../how-to/column-reader.md) — the `ColumnReader` API in full.
- [The Layer Model](nested-columns.md) — how `ColumnReader` represents nested columns.
- [How a Parquet File Is Laid Out](parquet-layout.md) — why the storage is columnar to begin with.
