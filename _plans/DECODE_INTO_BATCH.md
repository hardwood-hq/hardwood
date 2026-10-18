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
| Nested | page arrays; per-element copy into growable accumulators (doubled as they fill); accumulators copied out at publish (`trimValues`, `Arrays.copyOf` of levels and offsets) | values and bytes decoded into arrays allocated once at their exact size; levels and record starts decoded once per page and copied into the batch as its index mode keeps them |

Plus a thread and a hand-off per column and page, and, for Snappy through a direct buffer, the
two copies around native decompression (#1015; stage 8 below).

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
| Leaves with repetition on the scheduler, cells filled from page arrays (stage 5), Overture places file, container, 8 threads | all columns through `ColumnReaders` 1215 → 914 ms wall, 8350 → 6150 ms CPU; a row scan without record assembly 1054 → 733 ms wall, 6740 → 4310 ms CPU; `run-nested.sh`, which assembles every record, unchanged; fixed-size lists (`k` 768) through the regular path 5–7% faster |
| Stage 5 profile, Overture places file, all columns through `ColumnReaders`, container | CPU: `computeRealView` 53% (`main` 36%); the copy from page arrays into cells about 3% (dictionary ids; primitive values below 1%); `main`'s per-value assembly (`copyOneValue`, `appendEmpty`, `closeSegmentAtRecordStart`) 27% of its CPU, gone. Allocation about 10 GB per read: `computeRealView` 25%, page arrays of values and ids 10%, page levels and record starts 22% |
| Cells against pages, stage 5 | Overture: 57,330 cells, one spanning two pages, 3,931 records per cell. Fixed-size lists (`k` 768, 8M values): 16 cells, every one spanning pages, 2.75 pages per cell |
| Stage 5 profile, fixed-size lists (`k` 768) through the fast path, container | 12.50 ms (`main`) against 12.55 ms; decode of `PLAIN` floats 38%, the copy of uncompressed pages into the decompressor's buffer 29%, the copy into cells about 2% (`main`'s worker copies into its batch at the same cost) |

| `run-nested.sh` (whole Overture file through `RowReader`) on 1.1.0.Beta2, JFR, container | the consumer thread 71% of the CPU samples (record assembly, `NestedBatchIndex.decodeLeaf`; `String` creation 18.5%), the drain 15.6%, page decode 8.1%; the consumer parked for 23 ms of 20 s in parks of 20 ms or more. The read is bound by its consumer, which the pipeline does not touch |
| Overture places file layout | 55 columns × 256 row groups of about 18K rows, 14,081 data pages: one page per column chunk but one. Batches end at row groups, so no cell crosses a page there by the file's layout |
| Two passes (prototype, `V1` `PLAIN` and dictionary pages of leaves with repetition read in place at the seal) against stage 5, N300 pinned at 1.50 GHz, two rounds, median ms | Overture as written: columns 3223/3331 against 3254/3325, rows 3205/2803 against 2961/3295 (bimodal on both). Overture with PyArrow's defaults (23 row groups, about 10 pages per chunk): columns 3960/4195 against 4189/4295, rows 3753/4039 against 4051/4156, CPU 2–7% lower, 0.8–1.3 GB less allocated per read. `main` on that file: columns 4201/4237, rows 3865/3918, GC 303–353 ms against stage 5's 570–919 and the two passes' 492–786 |
| Stage 5 on Overture with PyArrow's defaults, row scan, N300 pinned: GC | 848–871 ms per read against `main`'s 313–342; every collection promotes at age 1 in both (tenuring threshold 1 in about 90% of collections, nothing past age 2), stage 5 in 148–152 collections against 87–90, promoting 13.3–13.9 GB against 2.6–2.7. Live heap at a full collection 377–436 MB against `main`'s 446–454; pages and dictionaries each decoded once; read-ahead depth 1 still promotes 10 GB; bounding finished batches ahead of the consumer changes nothing |
| Each nested page allocating its own scratch (levels, record starts, ids), against a pool per column returned once the page's cells are filled; two passes and the pool together, allocation per read (container) | Overture as written: columns 11.5 → 7.9 GB, rows 11.6 → 8.5 GB (`main` 10.6, 11.6); with PyArrow's defaults: columns 12.2 → 7.9 GB, rows 13.4 → 8.6 GB (`main` 11.5, 12.0) |
| The same, N300 pinned, two rounds, ms | as written: columns 3304/3337 → 3151/3337 (`main` 4429/4127), rows 3051/2911 → 2917/2915 (`main` 3951/3867); with PyArrow's defaults: columns 4290/4022 → 3557/3469 (`main` 4721/4146), rows 4198/4211 → 3409/3328 (`main` 3932/4218), GC 903/853 → 557/555 ms (`main` 366/491), promoted 13.0/11.7 → 7.6/7.5 GB (`main` 3.0/3.8) |

**Other readers, repeated leaves** (source read at the commits given in #1287):

| Reader | Copies per value | Output size from | A column's pages in parallel |
| --- | --- | --- | --- |
| arrow-rs | 1, plus regrowth across page chunks, and an in-place move for nulls | levels, per page chunk, as decoded | no |
| Arrow C++ | 1, plus power-of-2 regrowth | growth | no |
| Polars | 1 for `PLAIN` fixed-width, 2 for delta and cast types | the chunk's level count, an upper bound | no |
| DuckDB | 2 (a read vector, then `ListVector::Append`) | power-of-2 growth | no |
| Velox | 1 | a pre-pass over the chunk's levels, then exact per batch | no |
| cuDF | 1 | a pre-pass over the levels of a bounded set of pages; an exclusive scan gives each page its output offset per nesting depth | yes, each page at its scanned offset |

Every reader that writes a repeated leaf's values once without a size protocol decodes the column
in order on one thread, so the next write position is always known. A read of one column on
every core rules that out here. cuDF is the reader with the same constraint, and it writes once
by measuring levels first and decoding values second, at offsets the measure gives.

The first two rows are where the flat gains come from: no page arrays and no copy. For leaves
with repetition the gain comes from removing the per-value assembly and its accumulators; the
copy from page arrays is a few percent of the read, and what remains is dominated by
`computeRealView` (not in scope, #1023). The decode straight into cells applies to every column
nonetheless, so that flat columns are a specialization of one fill path rather than a design of
their own. The rows on the read-ahead window, the depth and the per-column pipeline are why
the scheduling changes with them. With one retriever per column on a first-in-first-out pool,
decodes reach the pool in whatever order the retrievers race ahead, and the batch the consumer
waits for queues behind other columns' read-ahead. A narrow read needs deep
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

The work of one column for one batch is a **cell**. A cell takes its batch from the column's
exchange when its first page task reaches it: a returned holder of a recycling exchange when one
is free, whose arrays it reuses, a new batch otherwise.

| Array | Size known | Allocated |
| --- | --- | --- |
| values of a fixed-width leaf without repetition; validity; dictionary ids and views of a variable-length leaf without repetition | the batch's record count, from the plan | by the first page task that reaches the cell; ids and views by the first segment that writes them |
| byte buffer of a variable-length leaf | the sum of its segments' bytes | when the cell is sealed and the last page touching it is measured |
| values and levels of a leaf with repetition; record offsets | the sum of the counts of its pages' segments | when the last page touching the cell is measured |

Validity is allocated only if a page touching the cell is not all-present. Every array is
allocated once at its final size. Allocation stays off the scheduler, which would otherwise
zero every column's arrays on one thread.

A plan-sized cell is never waited for: whichever task reaches it first allocates it. Only a
measured cell can be reached by a page task before it is allocated.

### Two passes

A page whose sizes metadata does not give is read in two passes, and its values are written once,
from its decompressed bytes into its cells.

1. **Measure** (the page's task): decompress the page into a buffer the page owns, decode its
   levels and, for a dictionary-encoded page, its ids, and measure it: record starts, value
   counts and, for `PLAIN` binary, byte lengths. Nothing is written into a cell.
2. **Place** (in page order, under the column lock): split each measured page into segments of
   the batch plan's cells. A cell is sealed once its last record's end is known, and its
   segments then give every array's size and every segment's offset: a running sum in page
   order, the scan cuDF computes per window.
3. **Fill:** the sealed cell's arrays are allocated at their final size, and each of its segments
   decodes its values from its page's kept bytes straight to its offset, with its levels and record
   offsets. A page releases its bytes once every cell it is placed into is filled.

**Every column kind is this model:**

- **Flat fixed-width:** metadata gives the placement and every size, so pass 1 is empty and the
  page's one task fills its segments.
- **Flat variable-length:** pass 1 measures the byte lengths; ids, views and validity are
  plan-sized and written at once.
- **Leaves with repetition:** pass 1 measures the levels.

The memory a page holds between its passes is its decompressed bytes, its levels and its ids,
bounded by the pages the scheduler holds unplaced and the pages of cells not yet sealed. No pool
thread waits for a cell.

**Every encoding can be read this way.** A page falls back to decoding into page arrays only
where pass 2 has no reader for its encoding yet:

| Page | Pass 2 reads its values |
| --- | --- |
| `V1` `PLAIN` and dictionary | in place, at any position (`FlatPageValues`, `BinaryPageValues`, shared with flat columns) |
| `V2` | in place: its levels lie uncompressed before its value region |
| `BYTE_STREAM_SPLIT`, `ALP`, `PLAIN` `BOOLEAN` | in place, at any position |
| delta encodings, `RLE` `BOOLEAN` | from the page's start only: one fill task per page, once every cell it touches is sealed, writes its segments in page order |
| masked pages | in place, skipping the rows the mask drops, as flat columns do |
| null placeholders, fixed-size lists | no levels to keep: placeholders write nulls, a fixed-size list's values are contiguous |

Once every row has its reader, the decode into page arrays and the copy at the seal leave the
scheduled pipeline, for flat and nested columns alike (stage 6).

**Prototype** (branch `1287-window-pass`, behind `hardwood.internal.twoPass`): leaves with
repetition, `V1` `PLAIN` and dictionary pages, values read in place at the seal through the flat
readers; every other page through page arrays. Measured against stage 5 on the Overture file as
written and rewritten with PyArrow's defaults (23 row groups of 200K rows, about 10 pages per
column chunk), container and N300, with allocation per read. Flat variable-length `PLAIN` against
stage 4's parked bytes on `run-binary.sh` and the plain 1BRC file follows if leaves with
repetition take the two passes.

The two passes are adopted if they are no slower than stage 5 on those reads. They reuse the flat
readers, so the code they add is the readers each encoding needs once, for flat and nested
columns together.

### Measure and fill

| Page | Task |
| --- | --- |
| fixed-width, without repetition | one step: decompress and decode straight into the segments |
| with repetition, or variable-length | **measure** (decompress into the page's own buffer, decode levels, ids and value lengths, report each segment's counts and whether it is all-present), place, then **fill** each sealed cell from its pages' kept bytes ([Two passes](#two-passes)) |

The fill writes values (typed kernels per encoding: dictionary lookup, `PLAIN`,
`BYTE_STREAM_SPLIT`, delta), dictionary ids, bytes with each value's start and end, levels and
record offsets as the batch's index mode keeps them, and validity, with an atomic OR on the
words at a segment's edges, which a page filled at the same time can share.

### Publish

A cell is complete when every segment placed into it is filled. The task that completes it runs
what the drain runs at publish today, while the cell's arrays are in its cache: the column's
filter fragment, nested `computeIndex` and the real view. Each column publishes its cells in
plan order into its `BatchExchange`.

Publishing takes room in the exchange: a slot in the bounded ready queue (recycling exchange,
the row readers) or rows from the budget (detaching exchange, `ColumnReader`). A completed cell
that finds no room stays at the head of the column's publish order, and the consumer that frees
room publishes it, on its own thread, as bookkeeping only. No pool thread waits on the exchange,
and no publish is queued behind read-ahead on the pool. The batches a column holds are bounded
by the read-ahead window, not by the exchange's holders.

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
- A segment's values and bytes are written once, from its page into its cell; no page array of
  values is copied into a cell.
- No pool thread waits for a cell to be allocated.
- A page's decompressed bytes outlive its task only until every cell it is placed into is filled.
- A column publishes a batch only after every segment of its cell is filled, and in plan order.
- No pool thread blocks on the exchange.

## Simplification

The coordination the pipeline adds costs more than its decoding does in the cases measured so
far: every regression against `main` found while landing stages 2 to 4 came from threads waiting
on each other, waking each other or writing each other's cache lines. Before stage 5 adds
more, each mechanism is checked against a simpler one. A mechanism
stays only with a measurement that needs it.

| Mechanism | Needed for | Measured | Decision |
| --- | --- | --- | --- |
| A scheduler thread per read, woken on every page queued, page ended and batch published | row-ordered submission across the read's columns | submitting on the thread that changes the state instead (the fetcher that queues a page, the task that ends, the consumer that frees room; one thread at a time, a call made meanwhile making it look again): `flatFloor` on the N300 73–78 → 68–70 ms CPU per read (`main` 65–70), in the container −1.5 ms; wall unchanged, narrow read unchanged, core tests green | submit inline; the thread goes |
| A plan-sized cell allocated by the first task that reaches it, under the cell's monitor | allocation off the scheduler and the fetcher | a page spans about two cells, so the second task waits while the first zeroes the array: `ObjectMonitor::try_spin` 9.7% of the instructions of `flatFloor` on the N300; a parking lock −0.5 ms wall; allocating on the fetcher +2.5 ms, one thread zeroing every cell | no task waits for an allocation: allocate outside the lock and install with a compare-and-set, the loser dropping its array (to measure) |
| Read-ahead window: batches, or budget rows while the slowest column's next batch waits for decoding and a decode thread of the context is idle | a narrow read keeping the pool busy (budget rows); eight concurrent reads (idle gate: an unconditional budget window made 1BRC's splits 7% slower); a consumer-bound row reader (wait gate: dropping it made the flat row reader 7% slower) | the wait gate starves a single column whose pages span about two batches while its consumer is momentarily ahead: `flatFloor` in the container 12.1 (`main`) against 13.5–14.4 ms, 12.5–13.2 ms without the gate | candidate: one bound in bytes past the consumer, and the pool ordering every read's tasks by how near their batch is to that read's consumer, in place of the idle and wait gates; it must pass the three measurements that made the gates and `flatFloor` (to measure) |
| One fetcher per column, a virtual thread keeping `FETCH_AHEAD` pages queued | header, dictionary and `InputFile` access off the decode pool | — | keep |
| Three treatments of a size known only once measured: parked bytes (flat variable-length), page arrays copied at the seal (leaves with repetition), and the waiting segments and fill tasks the plan had for stage 5 | cells allocated once at their final size | parked bytes: 68% of a plain 1BRC file's bytes, none of a dictionary-encoded one; the copy at the seal: 2–3% of the CPU of the Overture and fixed-size-list reads | the [two passes](#two-passes), to be measured against the other two |
| Cells written by every decode thread and read by the consumer | page-level parallelism within a column | `flatFloor` on the N300: the consumer's loop 0.8–0.9 ms slower than over `main`'s batches, which one drain thread writes; level with `main` when the read runs on one four-core L2 cluster | accepted as the cost of filling on the pool, unless the rows above leave a regression that it explains |

## Stages

Each stage lands as its own PR, keeps every read correct, and is gated against `main` on the
N300: the benchmarks named, plus no benchmark of `run-regression.sh` slower. Every stage's reads
also allocate less per read than `main`'s: the pipeline writes each value into an array allocated
once, so it never allocates more than the per-column pipeline it replaces. Scratch that outlives a
page's task (levels, record starts, ids, decompressed bytes) is borrowed from a pool on its
column and returned once the page's cells are filled. A batch's own arrays are not recycled on the nested
row reader: values it returns refer to their batch, so no later batch refills its arrays
(`FileLogicalTypeTest.valuesHeldAcrossBatchesKeepTheirInlineBytes`). Every gate includes
a narrow read: three columns of the 2016-01 taxi file (one row group of 10.9M rows, pages of
about 0.7M rows, so about 15 pages per column), the shape `FlatPerformanceTest` reads and the
regression preset does not. Stage 2 adds it to hardwood-benchmarks.

Columns not yet moved stay on today's per-column pipeline in the same read; both publish
batches cut by the one batch plan.

| # | Stage | Removes | Gate |
| --- | --- | --- | --- |
| 1 | Batch plan per read (#1503) | the per-column close rules | done |
| 2 | Scheduler, fetchers and cells for flat fixed-width leaves on `ColumnReader`; `V1` dictionary and `PLAIN` pages read in place, every other page through a page array on its task | the drain of those columns, and their page arrays and copy for the common encodings | `run-flat.sh` columnar faster, narrow read not slower |
| 3 | The same columns on the flat row reader (recycling exchange) | the drain of those columns | `run-flat.sh` row readers faster, narrow read not slower |
| 4 | Measure and fill for flat variable-length leaves (`PLAIN` and delta bytes), dictionary ids of flat binary leaves, including chunks with a `PLAIN` fallback; `V1` `PLAIN` and dictionary pages read in place, every other page through a page array on its task | the `byte[]` per value, the per-page id array | `run-1brc.sh`, `run-dictfilter.sh` and `run-binary.sh` (a plain `BYTE_ARRAY` scan) faster |
| 5 | Leaves with repetition: measure, placement after measure, and the [two passes](#two-passes) for `V1` `PLAIN` and dictionary pages, if the prototype carries them (the copy at the seal otherwise); flat variable-length bytes move onto the two passes with them, replacing their parked bytes; fixed-size lists; every column of `NestedRowReader`, its non-repeated fixed-width leaves included | the nested accumulators, their doubling and the publish copies (#1024, #750, finding 6 of READ_PATH_CONVERGENCE), the parked bytes of stage 4 | `run-nested.sh` faster; `run-fixedlist.sh` (`V2` pages, filled from page arrays until stage 6), `run-1brc.sh` and `run-binary.sh` not slower |
| 6 | Fill straight from the page for `V2` pages, `BYTE_STREAM_SPLIT`, delta `INT32`/`INT64`, `ALP` and `BOOLEAN`, flat and nested | the remaining page arrays and copies | `run-encodings.sh` faster |
| 7 | Remove the per-column pipeline (the differential tests' oracle: their expected batches are recorded from it first), fold the end state into `READ_PIPELINE.md`, `VALUE_DECODE.md` and `NESTED_DECODE.md`, delete this plan | the drain | — |
| 8 | Snappy decompresses heap to heap (#1015) | steps 1–2 of #1287 | `run-filter.sh` (SNAPPY) faster |

Stage 2 carries the gain the spike and the prototype measured, on the read they measured it on;
every later stage moves more columns onto a pipeline already shown faster. Binary leaves come
before leaves with repetition: they bring the first measured cells while placement still comes
from metadata, and with them every flat column of both readers, 1BRC's `station` included, is
on the new pipeline; leaves with repetition add placement after measure on top, and the two passes
would replace stage 4's parked bytes with them. The encodings of
stage 6 wait for a measurement, since Hardwood's writer and the default
PyArrow and parquet-java settings write `V1` pages without those encodings. Stage 8 is
independent of the others and can land at any point.

## Tests

- **Batch plan.** Capacity cuts, file changes, the row-group cut, always-match transitions,
  masks, the row cap, incremental planning across files, filter-only boundary markers.
- **Scheduler order.** Pages of columns with different page sizes are submitted in row order;
  the read-ahead bounds hold with one column and with many; a page spanning more batches than
  the window is submitted.
- **Differential.** Each fixture read with the new pipeline and with today's (an internal switch
  until stage 7), every batch compared: pages spanning many batches and pages smaller than one,
  required and optional leaves, `V1` pages beginning inside a record, single-entry dictionaries,
  dictionary and `PLAIN` pages mixed in one chunk, masks of one and of several intervals, null
  placeholders, boundary markers, the row cap inside a page, the file cut and the row-group cut,
  and both exchange modes. The result-comparing suites also run at small batch capacities.
- **Parallel fills.** Pages filled concurrently into one cell, with nulls at the page edges, for
  the atomic validity merge; two tasks reaching an unallocated plan-sized cell at once.
- **Two passes.** A page placed into two cells sealed in either order, filled from its kept bytes
  by two threads at once; a page measured before the page ahead of it; a failure, `close()` and
  the row cap between a page's passes, each page releasing its bytes once.
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
