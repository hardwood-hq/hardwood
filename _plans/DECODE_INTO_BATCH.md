# Plan: decode pages straight into batches

Tracking issue: #1287. Part of #1476 (its stage 3). Supersedes stage 6 of
[READ_PATH_CONVERGENCE.md](READ_PATH_CONVERGENCE.md) (#1024, #750, and the staging garbage of
#1045).

Every read today decodes a page into arrays of its own, and a drain thread per column copies
them into batches. Each column has its own retriever, which submits that column's pages to the
decode pool independently of the other columns. This plan replaces all three. Batch boundaries
are decided once per read, from metadata, as row ranges. One scheduler per read submits the
pages of every column in row order. Each page is decoded once, on the decode pool, into the
arrays of the batches its rows belong to. The drain threads go away.

## What it removes

| Column | Today, per value | After |
| --- | --- | --- |
| Flat fixed-width | page array allocated and zeroed, decoded into, copied into the batch on the drain | decoded into the batch |
| Flat dictionary binary | page `int[]` of entry ids allocated, copied into the batch's `dictIndices` | ids unpacked into the batch |
| Flat plain binary | one `byte[]` per value, appended into the batch's byte buffer | bytes copied once, page to batch |
| Nested | page arrays; per-element copy into growable accumulators (doubled as they fill); accumulators copied out at publish (`trimValues`, `Arrays.copyOf` of levels and offsets) | values, levels and bytes decoded into arrays allocated once at their exact size |

Plus a thread and a hand-off per column and page, and, for Snappy through a direct buffer, the
two copies around native decompression (#1015; stage 7 below).

## Evidence

| Measurement (taxi, N300) | Result |
| --- | --- |
| Prototype `1287-proto-deferred-dict`: dictionary numeric pages gathered into the batch on the drain, 20 columns, 8 threads | wall 306 → 209 ms, CPU 2100 → 1460 ms |
| Spike `scatter-decode-spike`: one scheduler, decode into batches, 20 columns | CPU −40% on 8 threads, wall −28% on 1 thread; behind the prototype on 8 threads (262 ms), its scheduler thread doing every column's I/O, header and dictionary parsing |
| Spike, in-flight bound counted in batches past the consumer | a 4-batch window starved pages of ~870K rows; 16 did not |
| Window of 8 batches counted in batch capacity (#1516), 3 columns of the 2016-01 file, pages of ~0.7M rows | clamped at `MAX_BATCH`, the window held about 1.5 pages per column: 46 ms against `main`'s 33 ms in a container with fast cores, while on the N300 it was 23% faster than `main`; with the window in byte-budget rows while the read waits for decoding and a decode thread idles, 31 ms in the container, and the 20-column read and 1BRC's eight concurrent readers unchanged on the N300 |
| Read-ahead depth: each column's next pages up to the pool's thread count, or its next batches up to that count, within 8 batch byte budgets, in place of the window and its idle and wait gates | one flat column of pages spanning about two batches (`flatFloor`): N300 15.6–16.1 → 14.9–15.2 ms (`main` 14.0–14.6), container 14.6–15.2 → 12.3–13.6 ms (`main` 12.1–12.7); 3-column read unchanged (N300 110–115 ms, `main` 158–172; container 30–33 ms, `main` 32–36); 20-column read and 1BRC's eight readers within noise. 8 batch budgets as the only bound decoded the whole one-column file ahead and cost 25% on the N300; batches of the clamped capacity as the only bound starved the 3-column read by 50% in the container; pages alone as the depth starved a column of 53 small pages, about 7 to a batch, by 37% (ZSTD `BYTE_STREAM_SPLIT`, container 36 → 50 ms), its threads idling on each batch's last page; with batches counted too, 34–36 ms (`main` 40–42) |
| Submission on the thread that changes the state, in place of a scheduler thread | one-column read on the N300: 73–78 → 68–70 ms CPU per read (`main` 65–70), wall unchanged |
| Cell allocation under a parking lock, in place of a monitor | the second task reaching a cell spun while the first zeroed its array: `ObjectMonitor::try_spin` 9.7% of the instructions of the one-column read on the N300; with the lock −0.5 ms wall, −5 ms CPU per read. Allocating on the fetcher instead: +2.5 ms, one thread zeroing every cell; installing with a compare-and-set: twice the read's time, both tasks zeroing an array |
| One task per column batch, prototype for flat fixed-width columns: a page spanning batches decoded by a page task into pooled buffers, each batch filled by one batch task | N300 against page tasks with the depth rule: one-column read level (14.9–15.1 ms, CPU 60–63 against 70–71 ms), 3-column read 114–130 against 110–114 ms, 20-column read 297–303 against 292–296 ms, 1BRC 674–682 against 645–658 ms; the remaining gap is dictionary ids read on another core than the one that decoded them. Its batch tasks size a batch before writing, but a page's decoded state then outlives its task (pooled scratch, owned bytes, a reader count, a cursor per reader), which costs about what the cross-task sizing it removes does |
| Per-column pipeline without a drain, cells filled by copy (#1516) | per-column throttles cannot serve both shapes: with a window of 3 batches per column, the 20-column flat read was 10% and the nested read 18–27% faster than `main`, and a 3-column read of the 2016 and 2019 files 30–50% slower; with cells allocated before decode and no window, the 3-column read was close to `main` and the 20-column read 26–32% slower |

The first two rows are where the gains come from: no page arrays and no copy. The rows on the
read-ahead window, the depth and the per-column pipeline are why the scheduling
changes with them. With one retriever per column on a first-in-first-out
pool, decodes reach the pool in whatever order the retrievers race ahead, and the batch the
consumer waits for queues behind other columns' read-ahead. A narrow read needs deep
read-ahead per column to keep the pool busy; a wide read must not have it.

## Design

### Batch plan

Every input to a batch boundary is known before any page is decoded, in rows, and is the same
for every column: the batch capacity (`BatchSizing`), file and row-group boundaries, the
always-match flag (`WorkItem.filterAlwaysMatches`, from row-group statistics), the rows page
masks keep (masks apply to every decoded column of a row group or to none), and the row cap.
The read plans its batches once, incrementally, as `RowGroupIterator` plans work items
([READ_PIPELINE.md](../_designs/READ_PIPELINE.md#incremental-planning)). Columns are aligned
because there is one plan. Landed in #1503.

### Pages onto batches

Each page of a column covers a range of the column's kept records:

| Page | Record range from |
| --- | --- |
| leaf without repetition | the header's value count (`V1`) or row count (`V2`); a running sum over the column's pages |
| leaf with repetition, `V2` or with an offset index | the header's row count, or the offset index's `first_row_index` |
| leaf with repetition, `V1` without an offset index | its repetition levels: known only once the page is measured |

A mask shifts the range into kept records. A page splits into one **segment** per batch it
touches: `(batch, record range, value range, byte range)`. A record never spans two batches; a
`V1` page with repetition may begin inside a record, and its values before its first record
start go into the previous record's batch.

### Scheduler

One scheduler per read decides the order in which pages reach the decode pool. It does no I/O
and no decoding, and has no thread of its own: it runs on whichever thread changes what may be
submitted, one thread at a time.

| Role | Thread | Does |
| --- | --- | --- |
| **Fetcher** | one virtual thread per column (today's retriever) | Pulls `PageInfo`s from the column's `PageSource`: fetches, parses page headers and dictionaries, computes record ranges where metadata gives them. Keeps a short queue of fetched pages. |
| **Scheduler** | the thread that queued a page, ended a task or published a batch | Takes the head page with the lowest first row across the columns' queues and submits its task, while the bounds allow. |
| **Page task** | the decode pool | Decodes the page into its segments, completes the cells whose last segment it fills, publishes. |

A page whose first row is not known yet (a `V1` page with repetition and no offset index) is
ordered by its column's last known row, a lower bound, so it is never submitted late. Such a
column's pages are placed once measured, as in [Measure and fill](#measure-and-fill), and the
scheduler holds at most a small fixed number of them unplaced.

**Read-ahead** has two bounds:

- pages submitted and not finished, across all columns, a few per decode thread;
- per column, its pages submitted and not published fewer than the pool's thread count, or the
  page starting fewer batches past the batch it publishes next than the pool has threads, within
  a few batch byte budgets of that batch; a page holding rows of that batch is always within it.

The depth gives every thread work on a column alone, counted in whichever unit runs out first:
pages, where a page spans batches, or batches, where many small pages make up one.

The pool needs no priorities: tasks arrive in row order, so the tasks of the batch the consumer
waits for are ahead of every read-ahead task in the queue.

### Cells

The work of one column for one batch is a **cell**. A cell owns its arrays; the exchange's
holder is attached at publish.

| Array | Size known | Allocated |
| --- | --- | --- |
| values of a fixed-width leaf without repetition; validity | the batch's record count, from the plan | by the first page task that reaches the cell |
| dictionary ids of a binary leaf whose chunk is dictionary-encoded throughout (encoding stats) | the batch's record count | by the first page task that reaches the cell |
| values and levels of a leaf with repetition; record offsets; byte buffer of a variable-length leaf | the sum of the counts of its pages' segments | when the last page touching the cell is measured |

Validity is allocated only if a page touching the cell is not all-present. Every array is
allocated once at its final size. Allocation stays off the scheduler, which would otherwise
zero every column's arrays on one thread.

A plan-sized cell is never waited for: whichever task reaches it first allocates it. Only a
measured cell can be reached by a page task before it is allocated; that task records its
position and ends, and the allocation resubmits it.

### Measure and fill

| Page | Task |
| --- | --- |
| fixed-width, or dictionary ids, without repetition | one step: decompress and decode straight into the segments |
| with repetition, or variable-length | **measure** (decompress, decode levels and value lengths, report each segment's counts and whether it is all-present), then **fill** the segments in order, keeping the decoder state from one segment to the next |

The fill writes values (typed kernels per encoding: dictionary lookup, `PLAIN`,
`BYTE_STREAM_SPLIT`, delta), dictionary ids, bytes with each value's start and end, levels and
record offsets as the batch's index mode keeps them, and validity, with an atomic OR on the
words at a segment's edges, which a page filled at the same time can share.

### Publish

A cell is complete when every segment placed into it is filled. The task that completes it runs
what the drain runs at publish today, while the cell's arrays are in its cache: the column's
filter fragment, nested `computeIndex` and the real view. Each column publishes its cells in
plan order into its `BatchExchange`.

Publishing takes room in the exchange: a holder from the free queue (recycling exchange, the
row readers) or rows from the budget (detaching exchange, `ColumnReader`). A completed cell
that finds no room stays at the head of the column's publish order, and the consumer that frees
room publishes it, on its own thread, as bookkeeping only. No pool thread waits on the exchange,
and no publish is queued behind read-ahead on the pool. On the recycling exchange, a returned
holder's arrays go to the column's free arrays, which the next allocation of a cell takes.

### Threads and lifecycle

- Per column: one fetcher. Submission runs on the threads that make pages ready; the drain
  threads are removed.
- Every `InputFile` access stays on the fetchers and the speculative tasks, as today.
- Page tasks are tracked: `close()` stops the fetchers, then waits for every
  task submitted; a failure is placed with its page's file, row group and page ordinal.

### Invariants for READ_PIPELINE.md

- The batch plan is the only place batch boundaries are decided.
- Pages reach the decode pool in row order across the read's columns.
- A cell's arrays are allocated once, at their final size; two segments of a cell share no
  element except validity words, which are merged atomically.
- A column publishes a batch only after every segment of its cell is filled, and in plan order.
- No pool thread blocks on the exchange.

## Stages

Each stage lands as its own PR, keeps every read correct, and is gated against `main` on the
N300: the benchmarks named, plus no benchmark of `run-regression.sh` slower. Every gate includes
a narrow read: three columns of the 2016-01 taxi file (one row group of 10.9M rows, pages of
about 0.7M rows, so about 15 pages per column), the shape `FlatPerformanceTest` reads and the
regression preset does not. Stage 2 adds it to hardwood-benchmarks.

Columns not yet moved stay on today's per-column pipeline in the same read; both publish
batches cut by the one batch plan.

| # | Stage | Removes | Gate |
| --- | --- | --- | --- |
| 1 | Batch plan per read (#1503) | the per-column close rules | done |
| 2 | Scheduler, fetchers and cells for flat fixed-width leaves on `ColumnReader`; `V1` dictionary and `PLAIN` pages read in place, every other page through a page array on its task | the drain of those columns, and their page arrays and copy for the common encodings | `run-flat.sh` columnar faster, narrow read not slower |
| 3 | The same columns on the row readers (recycling exchange); in-place kernels for `V2` pages, `BYTE_STREAM_SPLIT`, delta `INT32`/`INT64` and `BOOLEAN` | the remaining page arrays of those columns | `run-flat.sh` row readers faster |
| 4 | Measure and fill for flat variable-length leaves (`PLAIN` and delta bytes), dictionary ids of flat binary leaves, including chunks with a `PLAIN` fallback | the `byte[]` per value, the per-page id array | `run-1brc.sh`, `run-dictfilter.sh` and a plain `BYTE_ARRAY` scan (a benchmark to add) faster |
| 5 | Leaves with repetition: measure, placement after measure, kernels; fixed-size lists | the nested accumulators, their doubling and the publish copies (#1024, #750, finding 6 of READ_PATH_CONVERGENCE) | `run-nested.sh`, `run-fixedlist.sh` faster |
| 6 | Remove the per-column pipeline, fold the end state into `READ_PIPELINE.md`, `VALUE_DECODE.md` and `NESTED_DECODE.md`, delete this plan | the drain | — |
| 7 | Snappy decompresses heap to heap (#1015) | steps 1–2 of #1287 | `run-filter.sh` (SNAPPY) faster |

Stage 2 carries the gain the spike and the prototype measured, on the read they measured it on;
every later stage moves more columns onto a pipeline already shown faster. Stage 7 is
independent of the others and can land at any point.

## Tests

- **Batch plan.** Capacity cuts, file changes, the row-group cut, always-match transitions,
  masks, the row cap, incremental planning across files, filter-only boundary markers.
- **Scheduler order.** Pages of columns with different page sizes are submitted in row order;
  the read-ahead bounds hold with one column and with many; a page spanning more batches than
  the window is submitted.
- **Differential.** Each fixture read with the new pipeline and with today's (an internal switch
  until stage 6), every batch compared: pages spanning many batches and pages smaller than one,
  required and optional leaves, `V1` pages beginning inside a record, single-entry dictionaries,
  dictionary and `PLAIN` pages mixed in one chunk, masks of one and of several intervals, null
  placeholders, boundary markers, the row cap inside a page, the file cut and the row-group cut,
  and both exchange modes. The result-comparing suites also run at small batch capacities.
- **Parallel fills.** Pages filled concurrently into one cell, with nulls at the page edges, for
  the atomic validity merge; two tasks reaching an unallocated plan-sized cell at once.
- **Lifecycle.** `close()` during fills, a page task failure, a pool rejecting tasks (a closed
  context), a recycling exchange with one free holder.
- The existing suites (`DifferentialReadTest`, the parquet-testing runner, `MisalignedPageBoundariesTest`,
  `WideListBatchBoundTest`, the fixed-size-list tests) run unchanged.

## Taken from the spike and the prototype

From `scatter-decode-spike`: the row-ordered scheduler loop of `ScatterScan.schedule`, the
segment loop of `ScatterScan.decodePage` (a page over consecutive batch segments, decoder state
kept across them), the `scatter` and `unpackIds` kernels, and `markValidity`. From
`1287-proto-deferred-dict`: the per-type dictionary loops with and without definition levels,
and the zero-width (single-entry dictionary) case. From #1516: the batch-plan-driven placement
(`Placer`, `Segment`, `Cell`), cells published in plan order, the exchange's admission listener,
and its tests at small batch capacities and with a closed context.

## Not in scope

- `NestedLevelComputer.computeRealView` (#1023) and leaf compaction (#756): derived per cell at
  publish, unchanged here.
- Parallel consumption: stage 4 of [1BRC_THROUGHPUT.md](1BRC_THROUGHPUT.md). Its opt-in
  callback (#1475) would run on the task that completes a batch's last cell.
- Priorities in the decode pool: row-ordered submission makes them unnecessary.
