<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# RowReader vs. ColumnReader

Hardwood offers two reader APIs over the same files: `RowReader` and `ColumnReader`. They exist
for genuinely different reasons, and the trade-off between them is real. For a quick decision
table on which to pick, see the [How-to overview](../how-to/index.md#choosing-a-reader).

## One file, two shapes of access

Parquet stores data by column (see [How a Parquet File Is Laid Out](parquet-layout.md)). That
single physical layout can be presented to a consumer in two different shapes, and which shape
fits depends on whether your code thinks in *records* or in *columns*.

- **`RowReader`** reassembles each row into typed field accesses: `getLong("id")`,
  `getString("name")`, `getStruct("address")`. You move one row at a time and pull the fields you
  want by name or index.
- **`ColumnReader`** hands you a column at a time, a batch at a time, as a typed primitive array
  (`double[]`, `int[]`, …) plus a [`Validity`](/api/latest/dev/hardwood/Validity.html)
  bitmap for nulls. You loop over the array directly.

Both read the same bytes through the same pipeline. The difference is entirely in what they hand
to your loop.

## The trade-off: ergonomics vs. throughput

The two APIs sit at opposite ends of an ergonomics-versus-throughput spectrum, because columnar
storage and row-shaped access are a fundamental impedance mismatch.

**`RowReader` optimizes for ergonomics.** Records are how most application code is written — you
want "this trip's distance and fare," not "the distance column and the fare column." To present
a row, the reader must gather the current value from each projected column and, for nested data,
reassemble structs, lists, and maps into the [`PqStruct` / `PqList` / `PqMap`](../how-to/row-reader.md)
flyweights. That reassembly, and the per-field accessor calls, cost CPU and sometimes boxing. For
general-purpose reading that cost is invisible and the convenience is decisive.

**`ColumnReader` optimizes for throughput.** Analytical work — summing a column, computing a
distribution, scanning for matches — touches one or a few columns across many rows and never
needs a whole record assembled. `ColumnReader` skips the row-assembly step entirely and gives you
the decoded values as a primitive array you iterate with no per-element method call and no
boxing. On that kind of workload it is markedly faster; the cost is that *you* handle the layout
(nulls via the bitmap, nested structure via offsets — see below), which is more to get right.

The rule of thumb that the [decision table](../how-to/index.md#choosing-a-reader) encodes:

- Processing whole records, especially nested ones, with readability mattering more than peak
  throughput → **`RowReader`**.
- Aggregating or scanning a few columns over many rows, where throughput is the point →
  **`ColumnReader`**.

## The layer model

The two APIs also differ in how they expose nesting, and this is where the throughput trade-off
becomes concrete.

`RowReader` materializes nesting into objects: a list becomes a `PqList` you iterate, a struct a
`PqStruct` you index by field. Natural to consume, but it allocates flyweights as you descend.

`ColumnReader` exposes nesting structurally, as a sequence of **layers**, and never materializes
a container object. Each `OPTIONAL` group along a column's schema chain contributes a `STRUCT`
layer (carrying a validity bitmap); each `LIST`/`MAP` group contributes a `REPEATED` layer
(carrying validity *and* an offsets array that says which leaf values belong to which parent).
The leaf values arrive as one flat primitive array holding real values only — phantom slots from
null or empty parents are excluded. You walk the offsets and check the bitmaps to reconstruct
whatever structure you need, with zero allocation in the hot loop.

This is more work to consume than a `PqList`, which is exactly the trade-off restated: the layer
model is the price `ColumnReader` pays for not allocating, and the reason it wins on analytical
scans. The full mechanics — layer counts per schema shape, the empty-vs-null distinction, the
hot-loop null-check shapes — are documented in
[Read Column by Column](../how-to/column-reader.md).

## Several columns means one `ColumnReaders`, not several `ColumnReader`s

A `ColumnReader` is a cursor over one column, and each one built separately is sized for itself.
Batch capacity is a byte budget divided by the widths of the columns that reader projects, so a
`BYTE_ARRAY` column batches at fewer rows than an `INT32` one over the same file, and a filtered
reader is sized for its predicate's columns as well as its own. Two readers built independently
therefore reach different rows on their *n*th batch.

That makes `while (a.nextBatch() & b.nextBatch())` the wrong shape for reading two columns of the
same rows: the loop stops when whichever reader has the larger batches runs out, and the values it
took from each on any given turn came from different rows. Neither reader reports a problem,
because neither has one — each is a complete, correct read of its own column.

`ColumnReaders` is the multi-column read. Its readers share one row-group iterator and one batch
size, and `ColumnReaders.nextBatch()` advances all of them together and checks that they agree.
Reach for a bare `ColumnReader` when you want one column, and for `ColumnReaders` the moment you
want two.

## They are not exclusive

Nothing forces a single choice per file. A pipeline can open a `ColumnReader` to compute an
aggregate over one column and a `RowReader` elsewhere to materialize matching records — both
against the same file, sharing one [context and worker pool](concurrency-model.md). Pick per task,
not per program.

## Further reading

- [Read Row by Row](../how-to/row-reader.md) — the `RowReader` API in full.
- [Read Column by Column](../how-to/column-reader.md) — the `ColumnReader` API and the layer model
  in detail.
- [How a Parquet File Is Laid Out](parquet-layout.md) — why the storage is columnar to begin with.
