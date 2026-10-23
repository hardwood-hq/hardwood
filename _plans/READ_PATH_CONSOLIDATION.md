# Plan: one fetch sequence per read

Tracking issue: #1662. Related: #1663 (the 1.1.0.CR1 read-path plan, which this plan precedes),
#1399 and #100 (small files), #1629 and #1393 (S3 transport).

This plan reduces the read path's fetching and planning to one mechanism that issues every byte
request of a read, sized by five numbers, without making any read slower. It precedes 1.1.0.CR1.
Building out remote reads (split and parallel requests, deeper read-ahead) follows it, as changes
to those numbers.

## Where the complexity is

Data reads go through `RangeRead`, whose future a fetch step yields on. Planning reads do not:
the footer, the page-index window, Bloom filters, pruning dictionaries and the page-format probe
call `InputFile.readRange` directly (11 call sites), so planning blocks the thread it runs on. The
following exist to work around that:

- three planning modes (`RowGroupIterator.PlanningMode`), planning on virtual threads, and
  `WorkItemPlanning` as the need a fetch step yields on;
- a second `PrefetchTasks` per read and `ReadExecutor.executePlanning`;
- `HardwoodContextImpl.compute` hops, which ship planning CPU from a blocking thread to the pool
  and wait;
- `FileMetadataCache`'s load hand-off and `DecodeExecutor.awaitFirst`, so a pool thread waiting for
  a footer can run its parse;
- `OnceCache`, and `ReentrantLock`s where monitors would do, so a virtual thread blocked in
  planning does not hold its carrier on JDK 21.

With no single place that issues bytes ahead, each read-ahead feature brought its own mechanism:

| Concern | Implementations today |
| --- | --- |
| Issue a byte request | `ChunkHandle`, `SharedRegion`, the `FileOpenAhead` span, `CoalescedRanges`, `DictionaryParser`, `RowGroupBloomFilterSource`, `PageFormatProbe`, `ParquetMetadataReader` |
| Read ahead | the `ChunkHandle` chain, the `SharedRegion` chain, `prefetchNextRowGroup` (index window, planning, first chunk), `FileMetadataCache.prefetch`, `FileOpenAhead` footers, `FileOpenAhead` spans through `FetchedAhead` |
| Coalesce | `coalescePages`, `coalesceAcrossColumns` (with `CoalescableFirstChunk`), `plannedSpan`, `CoalescedRanges` |
| Per-row-group state evicted in `releaseWorkItem` | three `OnceCache`s, reference counts, `nextRowGroupPrefetched`, `plannings`, `IndexWindow` releases, `FetchedAhead` spans |
| Report a speculative failure | `PrefetchTasks.report`, untaken `Error`s of `RangeRead` and `WorkItemPlanning`, `FileMetadataCache.close`, the sticky planning failure |

Request sizes and depth come from 10 `hardwood.internal.*` properties and about 16 constants.
`RowGroupIterator` (2.5k lines) holds multi-file planning, schema compatibility, pruning, row
limits, page selection and coalescing, read-ahead, its caches and close, and the planning modes.

The scheduler side (`ReadScheduler`, the scheduled columns, `ReadQueue`, `WorkingSet`,
`ReadAheadBuffer`, `BatchExchange`) is essential to the decode, zero-thread and untrusted-input
designs. Stage 1 tidies it; no stage restructures it.

## Target model

A read is steps and one fetch sequence. Steps do the CPU work and never wait; the sequence does
the I/O and never parses.

- **The fetch sequence** is the read's byte requests in the order the read consumes them: a file's
  footer, its page-index and Bloom window, the dictionaries pruning reads, each row group's data
  requests, then the next file. It starts requests in order while the bytes it holds ahead of the
  consumer stay within F, charging them to R. A request completes a future. For a file whose bytes
  are in memory every entry is complete when appended, so a local read costs a slice.
- **Planning steps** parse what the sequence delivered and append what follows from it: the footer
  gives work items; the index window gives each column's pages; a row group's needed ranges, from
  every column, are coalesced once and appended as its data requests. Planning steps run where
  fetch steps run, on the pool or on the queue of a read on a context of zero threads, and yield
  on the sequence like any step.
- **Fetch steps** take a page's bytes as a slice of a completed request, by offset.

Five numbers size every request and every read-ahead:

| | Meaning | Takes over |
| --- | --- | --- |
| G | the largest gap one request bridges (1 MiB) | `CoalescingPolicy.GAP_BYTES` |
| S | the largest request | `CoalescingPolicy.MAX_SPAN_BYTES`, `sequentialChunkSize`, the open-ahead span limit; later `indexWindowBytes` |
| F | the bytes one read holds fetched ahead of its consumer | the one-ahead chains, the next row group's prefetch, `maxFilesAhead`, `FOOTER_RESERVATION` |
| R | the bytes all reads of a context hold ahead (25 % of the heap) | unchanged |
| W | the working set of one read, a hard limit | unchanged |

What goes: `SharedRegion`, `CoalescableFirstChunk`, the `ChunkHandle` chains, `FetchedAhead`,
`FileOpenAhead`, `plannedSpan`, `prefetchNextRowGroup`, `PlanningMode`, `WorkItemPlanning`, the
planning `PrefetchTasks`, `ReadExecutor.executePlanning`, the `compute` hops in planning,
`FileMetadataCache`'s load hand-off and prefetch maps (a footer cache stays),
`DecodeExecutor.awaitFirst`, `RangeRead.Priority`, and the JDK 21 workarounds that exist only
because planning blocks.

The remote work that follows becomes changes to the numbers and to the sequence's order:

- split requests and parallel requests: a smaller S under a larger F, with up to F/S requests in
  flight;
- demand priority: the head of the sequence is what the consumer waits for;
- cross-file depth: F;
- overlapping a file's planning reads: they are entries of the sequence.

## Gates

Every PR passes one gate of at most 5 minutes on the dev container, against a baseline jar of
`4fbba14e7` built once:

- **Exact, in the build:** the tests that pin request shapes and counts, unchanged or fewer
  requests; the `-Pzero-threads` core suite and `./mvnw verify` green.
- **Exact, from the benchmark:** requests and bytes fetched per contender, from the S3 benchmark's
  metadata, no more than the baseline's.
- **Timed:** `run-gate.sh` in hardwood-benchmarks runs base and head interleaved, two rounds, on a
  fixed set of contenders with short iterations: the narrow `hardwood` contenders and
  `hardwoodColumnar` on local files, and `hardwoodS3`, `hardwoodFilteredScan` and
  `hardwoodManySmallFiles` at 30 ms emulated latency. No contender slower beyond the band the
  script reports for that contender, from the spread between its rounds.

Stage 0 builds the script and checks it against itself (base against base) for its band.

The long suites, `run-regression.sh` with rounds on the container and the N300 pinned at
1.50 GHz and `run-s3.sh` at 235 ms, run once before 1.1.0.CR1, and on a stage only when the quick
gate flags a contender it cannot settle. A stage changes no default the quick gate does not
cover. Each PR updates the design documents its change alters (`READ_PIPELINE.md`,
`FETCH_PLANNING.md`, `THREADING.md`, `FILE_METADATA.md`).

## Stages

| # | Stage | Removes | Risk |
| --- | --- | --- | --- |
| 0 | Baseline, cut-down #1399 | — | low |
| 1 | Deletions and moves | dead code, five hand-written waits, `RowGroupIterator` down to its work list | low |
| 2 | One coalescing pass per row group | `SharedRegion`, `CoalescableFirstChunk`, `plannedSpan` | medium |
| 3 | The fetch sequence for data | the chunk chains, the next row group's first-chunk prefetch, `FileOpenAhead` spans, `FetchedAhead` | medium |
| 4 | Planning reads through the sequence | `PlanningMode`, `WorkItemPlanning`, the planning `PrefetchTasks`, the load hand-off | high |
| 5 | Fold into the design documents | this plan | low |

Stages run in order, one PR each or more, every commit keyed to #1662.

### Stage 0: baseline

- Build `run-gate.sh` and the baseline jar, and measure the script's band base against base.
- #1399 lands cut down: a 256 KiB S3 tail (64 KiB when a `MetadataSource` supplies the footers),
  a range that ends inside the tail fetching only the bytes before it, the open-ahead charging the
  tail's size, and the tail dropped once a file's last work item is released. #100's early close
  and reopen stay out; #100 stays open.
- #1659 lands as the check of a row group's Bloom filters before its index window is fetched
  ahead, or folds into stage 3.
- #1630, #1629, #1393 and #1671 continue alongside: they touch fuzz targets and `S3Fetcher` only.

### Stage 1: deletions and moves

No behaviour changes. Nothing a later stage deletes is reworked here.

- Remove `RangeRead.Priority` and `raiseToDemand`, `ScheduledColumn.unparkScheduler` (no caller),
  `ScheduledColumn.fail` (an alias of `signalError`) and `ColumnPipeline` (one implementor).
- Move schema compatibility (`validateSchemaCompatibility`, `ChunkPathCheck`, the touched columns),
  pruning (statistics, Bloom and dictionary probes and their events) and page selection with
  coalescing out of `RowGroupIterator`, and reduce its five `initialize` overloads and four
  constructors to one each. `RowGroupIterator` keeps the work list and its lifecycle.
- Scheduler side:
  - one waiter for a release of R, in place of the three copies in `ReadScheduler`,
    `FileOpenAhead` and `WorkingSet.Column`;
  - one take-use-return helper in `DecodeExecutor`;
  - one ordered set in `ReadScheduler` for queued and pending columns;
  - one per-read count of tasks and yielded steps, in place of `ScheduledColumn.tasks`, the
    column-level `CloseClaim` and `ReadQueue`'s `outstanding`, `yielded` and `resumed` (a
    yielded step is counted twice today);
  - one per-read wait primitive, in place of the park loops in `BatchExchange`,
    `ScheduledColumn.close` and `ReadQueue.runUntil`.

### Stage 2: one coalescing pass per row group

A row group's plans are built from one list of requests: every column's needed ranges (page
groups, the dictionary page, a column without an offset index whole or, under a row limit, its
first part) coalesced across columns once, under G and S. A column's pages are slices of those
requests. The one pass sees every column's ranges at once, so no request is cut to keep a byte
from being fetched twice, which `isCoalesceSafe` and the `fetchedAlone` extents guard today. The first
request of a file opened ahead is the first entry of this list, so `plannedSpan` goes.

### Stage 3: the fetch sequence for data

A per-read sequence takes the row groups' request lists in work-list order and starts them while
the bytes held ahead stay within F, charged to R. It replaces the `ChunkHandle` and
`SharedRegion` chains, the first-chunk prefetch of the next row group, and the open-ahead spans
with `FetchedAhead`. Footers and index windows are appended as entries, still parsed where they
are today. F's default is set by the gates: the smallest value at which no contender is slower
than the baseline. `maxFilesAhead` stays as a bound on requests for files not reached, if the
many-small-files contender needs it.

### Stage 4: planning reads through the sequence

One PR per kind of read, in this order: footer, page-index and Bloom window, pruning dictionaries,
page-format probe. Each turns its planning code into a step that yields on a sequence entry. Once
the last lands, planning never blocks, and `PlanningMode`, `WorkItemPlanning`, the planning
`PrefetchTasks`, `executePlanning`, the planning `compute` hops, `FileMetadataCache`'s hand-off
and `DecodeExecutor.awaitFirst` go. Whether `OnceCache` and the `ReentrantLock`s still need their
JDK 21 reasons is checked then.

Two questions are settled in this stage, as the dictionary reads move:

- Whether a dictionary parse whose buffers exceed `DecodeBuffers.RETAINED_BYTES` still needs
  admission to R beside the read's own task, or W alone bounds it.
- Whether failures of speculative work need more than one reporter, which the sequence provides
  for every read.

### Stage 5: fold into the design documents

The end state goes into `READ_PIPELINE.md`, `FETCH_PLANNING.md`, `THREADING.md` and
`FILE_METADATA.md`, and this plan is deleted. #1663 then continues to 1.1.0.CR1.

## After this plan

The remote work superseded by the target model is closed and continues as measured changes to S,
F and the sequence's order, each with its own issue when it starts: split and parallel requests,
deeper cross-file read-ahead, overlapped planning reads. Their gates are the ones above, plus one
run on real S3 from a distant region (#1671).
