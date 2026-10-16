# Plan: decode pages straight into batches

Tracking issue: #1287. Part of #1476 (its stage 3). Supersedes stage 6 of
[READ_PATH_CONVERGENCE.md](READ_PATH_CONVERGENCE.md) (#1024, #750, and the staging garbage of
#1045).

Every read today decodes a page into arrays of its own, and a drain thread per column copies
them into batches, deciding the batch boundaries as it goes. This plan replaces both. Batch
boundaries are decided once per read, from metadata, as row ranges. Each page is decoded once,
on the decode pool, into the arrays of the batches its rows belong to, for every column kind
(flat and nested, fixed-width and binary), every read (filtered or not, capped or not) and both
exchange modes. The drain thread goes away.

## What it removes

| Column | Today, per value | After |
| --- | --- | --- |
| Flat fixed-width | page array allocated and zeroed, decoded into, copied into the batch on the drain | decoded into the batch |
| Flat dictionary binary | page `int[]` of entry ids allocated, copied into the batch's `dictIndices` | ids unpacked into the batch |
| Flat plain binary | one `byte[]` per value, appended into the batch's byte buffer | bytes copied once, page to batch |
| Nested | page arrays; per-element copy into growable accumulators (doubled as they fill); accumulators copied out at publish (`trimValues`, `Arrays.copyOf` of levels and offsets) | values, levels and bytes decoded into arrays allocated once at their exact size |

Plus a thread and a hand-off per column and page, and, for Snappy through a direct buffer, the
two copies around native decompression (#1015; stage 7 below).

On the taxi flat scan the flat row alone was measured at 306 → 209 ms on eight threads (#1287,
prototype). On an Overture nested scan the append path is 26% of inclusive time (#1024).

## Design

### Batch plan

Every input to a batch boundary is known before any page is decoded, in rows, and is the same
for every column: the batch capacity (`BatchSizing`), file and row-group boundaries, the
always-match flag (`WorkItem.filterAlwaysMatches`, from row-group statistics), the rows page
masks keep (masks apply to every decoded column of a row group or to none), and the row cap.
The read plans its batches once, as `RowGroupIterator` plans work items: a sequence of batches,
each with its kept-record count, file, row group, always-match flag and whether the cap ends it.
Planning is incremental, as work items are ([READ_PIPELINE.md](../_designs/READ_PIPELINE.md#incremental-planning)).

Columns are aligned because there is one plan. No column evaluates a close rule of its own.
This step lands first and on its own (#1503), with the drain closing batches at the planned
counts.

### Pages onto batches

Each page of a column covers a range of the column's kept records. Where the range is known
from metadata, the retriever computes it while it walks the page headers in order:

| Page | Record range from |
| --- | --- |
| leaf without repetition | the header's value count (`V1`) or row count (`V2`); a running sum over the column's pages |
| leaf with repetition, `V2` or with an offset index | the header's row count, or the offset index's `first_row_index` |
| leaf with repetition, `V1` without an offset index | its repetition levels: known only once the page is measured |

A mask shifts the range into kept records. The record range gives the batches a page touches.
A page splits into one **segment** per batch it touches: `(batch, record range, value range,
byte range)`.

A record never spans two batches. A `V1` page with repetition may begin inside a record: its
values before its first record start belong to the previous page's last record and go into that
record's batch. A batch's last record therefore ends at the next record start or at the end of
the column chunk, which may lie in a later page.

### Cells

The work of one column for one batch is a **cell**. A cell knows its arrays once it knows its
sizes:

| Array | Size known | Allocated |
| --- | --- | --- |
| values of a fixed-width leaf without repetition; validity | the batch's record count, from the plan | when the batch is planned |
| dictionary ids of a binary leaf | with its values | with its values |
| values and levels of a leaf with repetition; record offsets | the sum of the value counts of its pages' segments | when the last page touching the cell is measured |
| byte buffer of a variable-length leaf | the sum of the byte counts of its pages' segments | when the last page touching the cell is measured |

Validity is allocated only if a page touching the cell is not all-present. When the cell's
sizes are complete, the value and byte ranges of its segments are a running sum over its pages
in page order. Every array is allocated once at its final size; nothing grows and nothing is
trimmed.

On the recycling exchange (the row readers), "allocated" means taking a holder from the free
queue. A cell that finds the queue empty is parked, and the reader returning a holder allocates
it and resubmits its waiting fills. No pool thread blocks on the queue: a blocked pool thread
could hold up the fill that would let the reader return a holder.

### Measure and fill

| Step | Runs on | Does |
| --- | --- | --- |
| **Measure** | the pool, one task per page | Decompresses the page, decodes its levels and, for a variable-length leaf, its value lengths. Reports to each cell the page touches its segment's value and byte counts and whether it is all-present. Keeps a value decoder positioned at the first value. |
| **Fill** | the pool, continuing the page's task | Decodes the page's values, levels and bytes into its segments, in order, keeping the decoder state from one segment to the next. |

What a page reports depends on its leaf. A leaf without repetition has its value positions at
its record positions, nulls included: its counts follow from its record range, and its
definition levels serve the fill and the all-present flag. A leaf with repetition counts its
level positions per record from its repetition levels. A variable-length leaf maps its value
lengths onto level positions through its definition levels.

A fixed-width page without repetition whose record range came from metadata needs no
measurement: its cells are allocated at planning, and one task decompresses and decodes it into
them.

A fill whose next segment's cell is not allocated yet records its position and ends; the cell's
allocation resubmits it. Every encoding is therefore filled by its sequential decoder, and no
decoder has to seek.

The fill writes values (typed kernels per encoding: dictionary lookup, `PLAIN`,
`BYTE_STREAM_SPLIT`, delta), dictionary ids, bytes with each value's start and end, levels and
record offsets as the batch's index mode keeps them, and validity, with an atomic OR on the
words at a segment's edges, which a page filled at the same time can share.

### Publish

A cell is complete when every segment placed into it is filled. The task that completes it runs
what the drain runs at publish today, while the cell's arrays are in its cache: the column's
filter fragment, nested `computeIndex` and the real view. Each column publishes its cells in
plan order into its `BatchExchange`: a cell that completes before its predecessor is published
by the task that publishes the predecessor. The readers consume per column as today.

### Threads and lifecycle

- Per column, the retriever stays: it fetches, walks page headers, computes record ranges where
  metadata gives them and submits pages. The drain thread is removed.
- A reorder slot's level scratch and decompressed bytes live until the page's fill completes.
  The retriever's throttle advances on fill completion, so a column holds at most
  `MAX_INFLIGHT_PAGES` pages, as encoded bytes and batch arrays.
- Batches are planned no further ahead than the exchange admits, which bounds the cells, and
  so the batch arrays, in flight.
- Measure and fill tasks are tracked like decode tasks are today: `close()` waits for them, and
  a failure is placed with its page's file, row group and page ordinal.

### Invariants for READ_PIPELINE.md

- The batch plan is the only place batch boundaries are decided.
- A cell's arrays are allocated once, at their final size, and a segment is written by one fill
  task; two segments of a cell share no element except validity words, which are merged
  atomically.
- A column publishes a batch only after every segment of its cell is filled, and in plan order.
- A slot is reused only after its page's fill completed.
- No pool thread blocks on the exchange.

## Stages

Each stage lands as its own PR, keeps every read correct, and is judged on the benchmarks named.
A stage's gate is also: no benchmark of `run-regression.sh` slower than the 1.1.0.Beta2 baseline
on the N300.

| # | Stage | Removes | Gate |
| --- | --- | --- | --- |
| 1 | Batch plan per read; the drain closes batches at the planned counts (#1503) | the per-column close rules | neutral |
| 2 | Pages onto batches, cells, measure and fill for every column and read, and the drain thread removed. Every page kind is filled by decoding it as today and copying into its segments. | the drain; the nested accumulators, their doubling and the publish copies (#1024, #750, finding 6 of READ_PATH_CONVERGENCE) | `run-nested.sh` faster |
| 3 | Kernels: fixed-width leaves without repetition (dictionary, `PLAIN`, `BYTE_STREAM_SPLIT`, delta `INT32`/`INT64`, `BOOLEAN`), the one-task path for them, dictionary ids of binary leaves, fixed-width leaves with repetition | steps 3–5 of #1287; the per-page id array | `run-1brc.sh`, `run-flat.sh` faster |
| 4 | Kernels: binary leaves without repetition, `PLAIN` and delta bytes | the `byte[]` per value | `run-dictfilter.sh` and a plain `BYTE_ARRAY` scan (a benchmark to add) faster |
| 5 | Kernels: binary leaves with repetition; fixed-size lists | the page arrays of these leaves | `run-nested.sh`, `run-fixedlist.sh` faster |
| 6 | Remove the decode-and-copy fill, fold the end state into `READ_PIPELINE.md`, `VALUE_DECODE.md` and `NESTED_DECODE.md`, delete this plan | — | — |
| 7 | Snappy decompresses heap to heap (#1015) | steps 1–2 of #1287 | `run-filter.sh` (SNAPPY) faster |

Stage 2 sets the contract later stages fill in: what a page reports, the segment, the cell and
its allocation, for every page kind. Stages 3 to 5 add kernels and change neither placement nor
allocation. Stage 7 is independent of the others and can land at any point.

## Tests

- **Batch plan.** Capacity cuts, file changes, the row-group cut, always-match transitions,
  masks, the row cap, incremental planning across files, filter-only boundary markers.
- **Differential.** A test reads each fixture with the fill kernels and with the
  decode-and-copy fill (an internal switch until stage 6, then against the prior release's
  output recorded as expected batches) and compares every batch: pages spanning many batches and
  pages smaller than one, required and optional leaves, `V1` pages beginning inside a record,
  single-entry dictionaries, dictionary and `PLAIN` pages mixed in one chunk, masks of one and of
  several intervals, null placeholders, boundary markers, the row cap inside a page, the file cut
  and the row-group cut, and both exchange modes.
- **Parallel fills.** Pages filled concurrently into one cell, with nulls at the page edges, for
  the atomic validity merge.
- **Lifecycle.** `close()` during fills, a measure or fill failure, a recycling exchange with one
  free holder.
- The existing suites (`DifferentialReadTest`, the parquet-testing runner, `MisalignedPageBoundariesTest`,
  `WideListBatchBoundTest`, the fixed-size-list tests) run unchanged.

## Taken from the spike and the prototype

From `scatter-decode-spike`: the segment loop of `ScatterScan.decodePage` (a page over
consecutive batch segments, decoder state kept across them), the `scatter` and `unpackIds`
kernels, and `markValidity`. From `1287-proto-deferred-dict`: the per-type dictionary loops with
and without definition levels, and the zero-width (single-entry dictionary) case.

## Not in scope

- `NestedLevelComputer.computeRealView` (#1023) and leaf compaction (#756): derived per cell at
  publish, unchanged here.
- Parallel consumption: stage 4 of [1BRC_THROUGHPUT.md](1BRC_THROUGHPUT.md). Its opt-in
  callback (#1475) would run on the task that completes a batch's last cell.
- One retriever per read instead of per column: stage 5 of [1BRC_THROUGHPUT.md](1BRC_THROUGHPUT.md).
