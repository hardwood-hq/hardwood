# Design: skipping filter-only columns in fully matching row groups

**Status: Proposed.** Tracking issue: #1274. Builds on
[ROW_READER_AUGMENTED_PROJECTION.md](ROW_READER_AUGMENTED_PROJECTION.md) (#1242),
[EXACT_COLUMN_READER_FILTERING.md](EXACT_COLUMN_READER_FILTERING.md) (#624) and
[UNIT_STATISTICS_CONVERGENCE.md](UNIT_STATISTICS_CONVERGENCE.md) (#1177).

## Scope

A **filter-only column** is a leaf a filtered read decodes because the predicate references it,
while the projection does not include it. It sits in `ReadProjection#decoded()` at an index at
or past `ReadProjection#payloadColumnCount()` (`ReadProjection#isFilterOnly`).

Its values serve one consumer, the predicate, and the predicate needs them only in row groups
whose decision is `MIGHT_MATCH`:

| decision | projected column | filter-only column |
|---|---|---|
| `CANNOT_MATCH` | not read | not read |
| `MIGHT_MATCH` | read | read |
| `ALWAYS_MATCHES` | read | **not read** |

In an `ALWAYS_MATCHES` row group a filter-only column is neither fetched nor decompressed nor
decoded. This holds for every reader that decodes an augmented projection: `FlatRowReader`,
`NestedRowReader`, and the filtered `ColumnReader` / `ColumnReaders`, for flat and nested
filter-only columns alike.

The granularity is the row group. Page-level `ALWAYS_MATCHES` remains outside this design, for
the reasons recorded under Boundaries in UNIT_STATISTICS_CONVERGENCE.md.

## The contract

For a reader built with projection `p` and filter `pred`:

- The rows returned are exactly those satisfying `pred`.
- The reader's accessors resolve the leaves of `p` and nothing else. A name outside `p` raises
  `IllegalArgumentException`, and an index at or past `getFieldCount()` raises
  `IndexOutOfBoundsException`. That holds for a predicate column outside
  `p` exactly as for any other unprojected column, in every row group, whatever statistics decided.
- At every depth, a `PqStruct` resolves only the children that `p` reaches.

## Model

### Two accessor surfaces

A row reader holds two views over the batches it loads:

- The **payload view** backs the public accessors. Its projected schema is
  `ReadProjection#payload()`: the leaves of `p` alone. Because the payload leads the decoded
  columns, its batches are the prefix `[0, payloadColumnCount)` of the batch array. `FlatRowReader`'s name-to-index map and value arrays, and `NestedRowReader`'s
  `NestedBatchDataView` with its `TopLevelFieldMap`, are built from the payload projection, so an
  unprojected name or index has no entry. No accessor branches on whether a column is
  filter-only.
- The **predicate view** is the `StructAccessor` the record matcher tests. It spans the predicate
  columns, whether projected or filter-only, and nothing else. `PredicateView`, in
  `dev.hardwood.internal.reader`, is fed batches by projected index, so the row readers and the
  column readers' `SelectionEngine` evaluate `pred` through one accessor. Flat predicate columns
  are served from their typed arrays, nested ones through a `NestedBatchDataView` over a
  projection of the predicate paths. It is refreshed per batch only when the batch is evaluated.
  Its indexed accessors address the flat predicate columns first and the nested projection's
  top-level fields after them; `PredicateView#indexOf` maps a top-level file leaf column into that
  space for `RecordFilterCompiler`.

The drain-side path (`BatchFilterCompiler`, `BatchMatchMerger`) reads batches by projected index
and does not use either accessor surface.

### Planning the skip

`RowGroupIterator#computeFetchPlans` gives a column a **skip plan** exactly when the column is
filter-only and the work item carries `filterAlwaysMatches`. That is the condition under which the
consumers take no batch of the column for the row group (see Conditional polling), and the two must
not diverge: a filter-only column read in a proven row group would publish batches no consumer
takes, and the next undecided step would take them in place of its own.

Nothing else enters the decision. A page mask that narrows the payload columns in a proven row
group (page filtering against a column index that disagrees with the chunk statistics) changes how
many rows they emit, not where they close their batches, and the skipped column emits none.

The skipped column gets the `SkippedColumnFetchPlan`, which yields one
`PageInfo#BOUNDARY_MARKER`: a page that carries no bytes, no metadata and no rows. The plan holds
no `ChunkHandle`, takes no part in `coalesceAcrossColumns`, and the next row group's prefetch
passes over it.

The column emits no `RowGroupScanned` JFR event for the row group, since no page of it is scanned.

### The boundary marker

The retriever writes the marker straight into its reorder-buffer slot and submits no decode task.
The drain runs its per-page bookkeeping for the marker and assembles nothing:

- the flush on a change of file;
- the `activeMaxRows` rule, under which the marker, as a page of a proven row group, leaves the
  cap in place;
- the flush on a change of the always-match flag, with the marker counting as a proven page.

The last one keeps the column's batches aligned with the payload columns'. Each payload column
flushes where a proven row group begins, so the filter-only column publishes its batch in progress
at the same row. Without that flush it would join the undecided rows before and after the proven
row group into one batch. Within a run of undecided row groups every column sees the same rows,
pages, masks and batch capacity, and every batch starts empty at the start of the run, so every
column closes its batches at the same rows.

A filter-only column's exchange therefore holds a batch for each step whose batches carry no
`filterAlwaysMatches`, and none for a proven step.

### Conditional polling

Every consumer takes a step's batches from the payload columns first, at decoded indices below
`payloadColumnCount`. When the first payload batch carries `filterAlwaysMatches`, the consumer
takes no batch from a filter-only column for that step. Otherwise it takes one from every
filter-only column, and the lockstep checks cover all columns: a column that is exhausted while
the first is not, or whose batch has another record count, raises `IllegalStateException`. At the
end of the stream every exchange is checked for an error, so a failure on a filter-only column's
worker surfaces.

- **`FlatRowReader` / `NestedRowReader`:** a proven step is not evaluated, and the predicate view
  is not refreshed for it. A filter-only column's slot among the step's batches stays empty, so
  the next step recycles only the batches the reader took. Without a cap, `activeMatcher` is
  `null`. With a `head(N)` cap, the reader counts every row of the batch as a match without
  calling the matcher. On the drain-side path, a proven step is not merged: its survivor bitmap is
  all ones, and the tally counts the batch whole.
- **`ColumnScan`:** in a proven step a filter-only cursor keeps the batch of the last evaluated
  step, and nothing reads it. `SelectionEngine` returns the every-record result (`-1`) for the
  step before either backend runs, taking the flag from the first cursor, a payload column's; the
  tally takes the file name from the same cursor.
- **`FlatColumnWorker`'s drain-side matcher** writes the all-ones mask for an always-match batch
  of a payload column instead of reading values.

A `head(N)` cap is held by the drain only up to the first page of an undecided row group, where
every column drops it. Until then only proven row groups have been read: the payload columns
count their rows against the cap, and a filter-only column assembles no rows. A payload column
that reaches the cap inside a proven row group ends the stream there, and the reader returns once
it has counted `N` matches, without taking a filter-only batch. Under a cap, a filter-only column
may read ahead of the rows the reader returns by at most the pipeline's in-flight depth, as every
column does.

## Cost

The skip turns the whole decode of a filter-only column into one marker per fully matching row
group: no I/O, no decompression, no decoding, no value copy and no batch. In a `MIGHT_MATCH` row
group a filter-only column is read like any other. Reads without a filter, and filtered reads whose
predicate columns are all projected, have no filter-only column, so no plan is a skip plan.

The record-matcher fallback on the row readers evaluates through the predicate view instead of the
reader. The view resolves each flat field's array once per batch, so a per-record access is an
array index, the same as the reader's indexed accessors.

## Testing

`FilterOnlyColumnSkipTest` writes a flat and a nested fixture of three row groups each, so that
one filter proves a row group fully matching, leaves one undecided and prunes one.

- For each reader (flat row, nested row, column, grouped columns), drain-side and record-matcher
  predicates on a column outside the projection return exactly the matching rows, and
  `RowGroupScanned` shows the filter-only column unread in the fully matching row group and read
  in the undecided one.
- The fully matching row group is placed before the undecided one, after it, and between two
  undecided ones (`500 <= id < 2500`). The last arrangement needs the filter-only column's batch
  in progress closed where the fully matching row group begins; 64-row batches on the
  column-reader path end mid row group, so that batch is a short one there.
- `head(N)` ending inside, at the end of, and beyond a fully matching row group that comes first,
  and inside a fully matching or undecided row group that follows an undecided one.
- Accessors: the name and the index of a filter-only column raise in every row of every row group,
  including a predicate leaf below a projected struct.
- Filter-only columns that decode as nested on the column-reader path.
- A fully matching row group whose column index disagrees with its chunk statistics, so that page
  filtering narrows the payload columns there, followed by an undecided row group; row and column
  readers alike return exactly the matching rows.
- `head(N)` on the nested row reader across a fully matching row group that comes first.

## Documentation

The reference page for row-reader projection states the contract above: accessors resolve the
projected columns only, and a predicate column outside the projection is not readable.
