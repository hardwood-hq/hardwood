# Design: the column-read pipeline behind `ColumnReader`

**Status: Implemented.** Tracking issue: #1281. Describes the plumbing behind the column
readers, including the exact filtering of
[EXACT_COLUMN_READER_FILTERING.md](EXACT_COLUMN_READER_FILTERING.md) (#624).

## Scope

Every column-reader read — a single `ColumnReader`, an unfiltered `ColumnReaders` group, a
filtered group, a filtered single column — runs on one internal model:

- a **cursor** per decoded column, owning that column's pipeline;
- one **scan** per read, owning every cursor and advancing them together;
- **views**: `ColumnReader` and `ColumnReaders` expose the scan's current step and own no
  pipeline state.

## Cursor

`ColumnCursor` (package-private in `dev.hardwood.reader`) owns one decoded column's worker
(`FlatColumnWorker` or `NestedColumnWorker`), its `BatchExchange` and its current raw batch.
It answers:

- `advance()`: polls the exchange for the next batch, rethrowing a pipeline error; returns
  `false` at end of stream;
- the current batch (`flatBatch()` / `nestedBatch()`), whether it is nested, its record count
  and its file name;
- `filterAlwaysMatches()`: whether row-group statistics proved every record of the current batch
  to match the filter;
- `close()`: stops the worker.

It carries no accessor, layer or cache state. `ColumnCursor.create(...)` builds and starts the
worker and applies the routing rule: a column whose schema chain contributes a `STRUCT` or
`REPEATED` layer, or that repeats, decodes through `NestedColumnWorker`; every other column
through `FlatColumnWorker`.

A cursor's exchange is detaching: every batch it hands out is fresh, since a caller may keep the
arrays a `ColumnReader` returns. The exception is a flat filter-only column's cursor, whose batches
never reach the caller: it draws them from a recycling exchange and hands each back on its next
`advance()` (see [FILTER_ONLY_COLUMN_SKIP.md](FILTER_ONLY_COLUMN_SKIP.md)).

## Scan

`ColumnScan` (package-private in `dev.hardwood.reader`) owns:

- the cursors of every decoded column, in the order of `ReadProjection#decoded()`: the payload
  columns first, then any filter-only columns, the predicate columns outside the projection;
- for a filtered read, the `SelectionEngine`, which reads predicate values from the cursors'
  current batches, on its record-matcher backend through the `PredicateView` the row readers
  evaluate against;
- the `RowGroupIterator` the cursors draw from, and the per-file `RecordFilterTally`.

`advance()` polls every payload cursor once, in lockstep, then every filter-only cursor unless
the first cursor's batch reports `filterAlwaysMatches()`: a filter-only column is not read in a
row group statistics proved, so its cursor has no batch for such a step (see
[FILTER_ONLY_COLUMN_SKIP.md](FILTER_ONLY_COLUMN_SKIP.md)). It checks that the cursors it polled
agree: every cursor produced a batch, or none did, and all batches have the same record count; a
cursor exhausted before the first, or a differing record count, throws `IllegalStateException`.
When the first cursor reaches the end of the input, `advance()` drains every other cursor,
filter-only ones included, and one that still produces a batch throws `IllegalStateException`.
With a filter, it then computes the selection and compacts each payload cursor's batch to the
matching records: flat primitive values are gathered in place, binary values through
`LeafCompaction`, and a nested batch is sliced per record across its level and value arrays.
When the first cursor's batch reports `filterAlwaysMatches()`, the selection is every record and
the predicate is not evaluated; every worker flushes where the flag changes, so that cursor's batch
speaks for the step. It records the step's record count and increments a **generation** counter
for every step: each batch is a step, and so is the end of the input, the first time it is reached.

A read in which pruning dropped every row group gets a scan with no cursors, whose first
`advance()` returns `false`.

`close()` closes every cursor, then releases the iterator, even when a cursor's teardown
fails.

## Views

`ColumnReader` holds its scan and the index of its payload cursor, plus per-batch caches (the
real-items view, materialised binaries and strings), which it drops whenever it adopts a new
step. Its accessors read the cursor's current (compacted) batch.

**Advancing.** Each view remembers the generation it last consumed. `ColumnReader.nextBatch()`
advances the scan when the view has already consumed the current generation, adopts the step a
sibling advanced to when the view is one generation behind, and throws `IllegalStateException`
when it is further behind, since adopting would skip a batch. `ColumnReaders.nextBatch()`
advances the scan and marks every member as having consumed the new generation. So calling
`nextBatch()` on each member in turn moves the group once, in every group, filtered or not; a
member cannot pair rows from different steps, and a member left behind fails instead of
silently dropping batches. A member that is never advanced does not hold the others back. `ColumnReaders.getRecordCount()` reads the
scan's current step, so it reports the batch a member advanced the group to.

**Closing.** Closing any view of a group closes the scan: the members share one pipeline and
cannot outlive it separately.

A single-column reader, filtered or not, is the only view of a one-payload-column scan. A
filtered single column's scan also holds the cursors of the predicate columns. A predicate
column outside the projection has a cursor in the scan and no `ColumnReader`.

## Testing

The column-reader suites (`ColumnReaderExactFilterTest`, `ColumnReadersTest`,
`ColumnReaderLayerModelTest`, `ColumnReaderBatchArrayIdentityTest` and the multi-file and S3
variants) pin results. `ColumnReadersTest`, `IteratorTrackingTest` and `PrunedToEmptyReadTest`
cover the shared advance and close:

- advancing the members of an unfiltered group one at a time yields aligned rows;
- a member advanced alone moves the group, and the group's record count follows it;
- a member the group moved past by more than one step, before a batch or at the end of the
  input, throws;
- closing one member of a group closes the group;
- a read that pruning emptied yields no batch from any member.

## Documentation

`docs/content/how-to/column-reader.md` and the Javadoc of `ColumnReaders.nextBatch()` and
`ColumnReader.nextBatch()` state the shared advance and the group-wide close.
