# Design: the column-read pipeline behind `ColumnReader`

**Status: Proposed.** Tracking issue: #1281. Restructures the plumbing behind
[EXACT_COLUMN_READER_FILTERING.md](EXACT_COLUMN_READER_FILTERING.md) (#624); results and the
public API's signatures are unchanged.

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
- the current batch (`flatBatch()` / `nestedBatch()`), whether it is nested, its record count,
  its file name and its `filterAlwaysMatches` flag;
- `close()`: stops the worker.

It carries no accessor, layer or cache state. `ColumnCursor.create(...)` takes over the worker
construction and routing rule of today's `ColumnReader.createFromIterator` (a column whose
schema chain contributes a layer, or that repeats, is nested).

## Scan

`ColumnScan` (package-private in `dev.hardwood.reader`) owns:

- the cursors of every decoded column, in decoded order: the payload columns first, then any
  predicate columns outside the projection;
- for a filtered read, the `SelectionEngine`, which reads predicate values from the cursors'
  current batches;
- the `RowGroupIterator` the cursors draw from, and the per-file `RecordFilterTally`.

`advance()` polls every cursor once, in lockstep, and checks that they agree: every cursor
produced a batch, or none did, and all batches have the same record count. The mismatch
errors are those of today's two loops. With a filter, it then computes the selection and
compacts each payload cursor's batch to the matching records (flat: in place through
`LeafCompaction`; nested: the raw level/value triplet, as today). It records the step's
record count and increments a **generation** counter.

A read in which pruning dropped every row group gets a scan with no cursors, whose first
`advance()` returns `false`.

`close()` closes every cursor, then releases the iterator, even when a cursor's teardown
fails.

## Views

`ColumnReader` holds its scan and the index of its payload cursor, plus the per-batch caches it
has today (the real-items view, materialised binaries and strings), which it drops whenever it
adopts a new step. Its accessors read the cursor's current (compacted) batch.

**Advancing.** Each view remembers the generation it last consumed. `ColumnReader.nextBatch()`
advances the scan when the view has already consumed the current generation, and otherwise
adopts the step a sibling already advanced to. `ColumnReaders.nextBatch()` advances the scan
and marks every member as having consumed the new generation. So calling `nextBatch()` on each
member in turn moves the group once, in every group, filtered or not; a member cannot run ahead
of its siblings and pair rows from different steps.

**Closing.** Closing any view of a group closes the scan: the members share one pipeline and
cannot outlive it separately.

A single-column reader, filtered or not, is the only view of a one-payload-column scan. A
filtered single column's scan also holds the cursors of the predicate columns.

## What goes away

- `FilterCoordinator`: its lockstep advance, selection and compaction move into `ColumnScan`.
- The second lockstep loop in `ColumnReaders.nextBatch()`.
- `ColumnReader`'s pipeline fields and package-private hooks: the worker and exchange,
  `rawNextBatch`, `rawRecordCount`, `rawClose`, `setCoordinator`, `syncGeneration`,
  `applySelection`, `currentFlatBatch` / `currentNestedBatch`, `isNested`, and the coordinator
  field.
- `ColumnReader` instances for predicate columns outside the projection; those are cursors.

## Testing

The existing column-reader suites (`ColumnReaderExactFilterTest`, `ColumnReadersTest`,
`ColumnReaderLayerModelTest`, `ColumnReaderBatchArrayIdentityTest` and the multi-file and S3
variants) pin results and are unchanged. New:

- advancing the members of an **unfiltered** group one at a time yields aligned rows, as it
  does for a filtered group;
- closing one member of an unfiltered group closes the group;
- a read that pruning emptied yields no batch from any member.

## Documentation

`docs/content/how-to/column-reader.md` states that calling `ColumnReader.nextBatch()` on a
member of a group leaves its siblings behind and misaligns rows. It is rewritten to the
behaviour above: members advanced one at a time share the group's advance, and
`ColumnReaders.nextBatch()` remains the way to drive a group. The same holds for the Javadoc
of `ColumnReaders.nextBatch()` and `ColumnReader.nextBatch()`.
