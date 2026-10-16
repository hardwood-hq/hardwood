# Read pipeline

Describes how a read turns the row groups it has planned into published batches: the shared `RowGroupIterator`, the per-column `PageSource` → `ColumnWorker` → `BatchExchange` chain, the scheduled columns of a column read (`ReadScheduler`, `ScheduledColumn`), the threads that run them, back-pressure, batch sizing, the batch plan, row-group and file transitions, multi-file planning, row limits, error propagation, teardown and `ReaderConfig`.

Related documents:

- [FETCH_PLANNING.md](FETCH_PLANNING.md): which bytes a read requests and how they are fetched (fetch plans, chunk handles, coalescing, page masks)
- [COLUMN_READER.md](COLUMN_READER.md): what a batch holds and how the column reader exposes it
- [ROW_READER.md](ROW_READER.md): how the row reader presents batches as rows
- [VALUE_DECODE.md](VALUE_DECODE.md): how pages become values
- [NESTED_DECODE.md](NESTED_DECODE.md): how pages become nested batches
- [RECORD_FILTERING.md](RECORD_FILTERING.md): filter evaluation over the published batches
- [docs/content/concepts/concurrency-model.md](../docs/content/concepts/concurrency-model.md): the user-facing model

## Components and threading

Every data reader (`FlatRowReader`, `NestedRowReader`, a `ColumnReaders` group, and a single `ColumnReader`, which is a group of one) runs the same pipeline, for flat and nested schemas and for one file or many. A single-file read is a multi-file read of one file; there are no separate reader classes for either.

| Component | Count | Job |
|---|---|---|
| `RowGroupIterator` | one per data reader | Plans the work list of `(file, row group)` pairs a file at a time, caches per-row-group metadata, fetch plans and batches (`BatchPlan`), prefetches the next row group and the next footer |
| `PageSource` | one per decoded column | Walks the work list with its own cursor and yields that column's `PageInfo`s, draining each row group's `FetchPlan` |
| `ColumnWorker` | one per decoded column not scheduled | Decodes pages in parallel and assembles them into batches, in file order (`FlatColumnWorker`, `NestedColumnWorker`) |
| `ScheduledColumn` | one per scheduled column | Places its pages into the cells of the batch plan; its page tasks decode them straight into the batches ([Scheduled columns](#scheduled-columns)) |
| `ReadScheduler` | one per read with a scheduled column | Submits the pages of the read's scheduled columns to the decode pool in row order |
| `BatchExchange` | one per decoded column | Bounded hand-off of published batches from the worker to the consumer |

The decoded columns are the projection plus any filter-only predicate columns (`ReadProjection.decoded()`, see [RECORD_FILTERING.md](RECORD_FILTERING.md#augmented-projection)). In a column read (`ColumnScan`) and a flat row read (`FlatRowReader`), every flat column is a scheduled column; every other column, and every column of a nested row read, has a `ColumnWorker`. Both kinds publish batches cut by the read's one batch plan into a `BatchExchange`, so a read mixes them freely.

**Ownership.** `ParquetFileReader` reads the first file's footer at `open`/`openAll` and owns the input files and a `FileMetadataCache` of parsed footers. When that fails, it closes every input file, and the context when the call created it, before the exception propagates, attaching close failures as suppressed. Each data reader it builds gets its own `RowGroupIterator` sharing that cache; the parent tracks the child until the child closes the iterator, and closing a child releases the iterator's caches and its tracking entry but leaves the files to the parent. Closing the parent closes every child still open, then the metadata cache, an owned context and the files, so no child's worker or speculative task reads a file after it is closed; it attempts every input file, throws the first close failure and suppresses the later ones under it. A child build holds the shared side of the parent's lifecycle lock and the parent's close the exclusive side: the close waits for the builds under way, each of which, finding the parent closed, closes the child it made and fails, and a build started after the close fails at once. An iterator built outside a `ParquetFileReader` owns its files and closes them.

Tests: `MultiFileRowReaderTest`, `IteratorTrackingTest`, `ParentCloseTest`.

### Thread inventory

| Thread | Count | Lifetime | Does |
|---|---|---|---|
| Retriever (virtual) | one per decoded column | whole read | Pulls pages from the `PageSource`, which plans files, computes fetch plans and fetches page bytes on demand; submits decode tasks; throttles |
| Drain (virtual) | one per decoded column | whole read | Takes decoded pages in sequence order, assembles batches, runs drain-side filter fragments, publishes |
| Decode task | up to the reorder depth per column | one page | Decompresses and decodes one page on the `HardwoodContext` pool, trims a masked nested page to its kept records ([NESTED_DECODE.md](NESTED_DECODE.md#masked-pages)), stores the result, wakes the drain |
| Fetcher (virtual) | one per scheduled column | whole read | Pulls pages from the `PageSource`, places them into cells, queues them for the scheduler |
| Page task | up to the read-ahead bound per read | one page | Decodes one page of a scheduled column into its cells on the `HardwoodContext` pool, sizes the binary cells its measure completes, completes and publishes the cells it finishes |
| Speculative task | as triggered | one step | On the common `ForkJoinPool` (a new thread per task when its parallelism is below 2): the next file's footer load, the next row group's fetch plans and first-chunk prefetch, one-ahead chunk and region prefetches. The iterator's `PrefetchTasks` tracks all but the footer load, which the `FileMetadataCache` tracks |
| Consumer | the caller's thread | caller-managed | Polls every column's exchange and reads the batches |

Decode runs only on the context's fixed platform-thread pool (`HardwoodContext.create(n)`, default one thread per processor), which every reader sharing the context submits to. The pool size is the bound on decode parallelism. Decode is CPU-bound and runs as one task per page, so it stays off the virtual-thread carriers, which every virtual thread in the process shares, and runs on a pool the caller sizes. The retriever and drain are virtual threads on the JVM's default scheduler: they are cheap per column and spend their time blocked on I/O, queues and each other.

**The drain does no I/O.** Every `InputFile` access during a read happens on the retriever or on a speculative task; decode tasks work on bytes the retriever resolved ([FETCH_PLANNING.md](FETCH_PLANNING.md#fetch-plans)). `ColumnWorker.close()` relies on this: it interrupts the drain to release it from a timed queue wait, and interrupting a thread inside a `FileChannel` operation closes the channel for every reader sharing it ([INPUT_FILES.md](INPUT_FILES.md#contract)). A change that gives the drain its own file access (fetching on demand instead of waiting for the retriever) must drop that interrupt first.

**Speculative work never reports a failure.** A failed plan or chunk prefetch leaves its plan or chunk unfetched, and the demand path repeats the work; a failed footer load stays cached in the `FileMetadataCache`, and the demand path rethrows it. Either way the failure is raised to a caller that is waiting for it. Work whose failure must surface stays off the speculative task: resolving the next work item, which validates the next file's schema, runs on the retriever before the speculative task is started, and the iterator keeps a failure there for the demand path to rethrow.

**Every wait is bounded.** The retriever's throttle, the drain's wait for a decode task, the fetcher's wait, and the exchange's queue operations on both sides are timed waits; the unparks and the exchange's end-of-stream sentinel make the common case immediate, and the bound guarantees a waiter is released at all. Untimed parks after timed parks on a virtual thread can be stranded by the runtime (JDK-8369227, fixed in 25.0.3 and 26.0.1), leaving a drain parked forever with its page already decoded.

Tests: `ColumnWorkerTest`, `ReaderCloseLatencyTest`, `ReaderEofLatencyTest`, `BatchExchangeTest`, `BatchExchangeRowLimitTest`.

## Per-column pipeline and back-pressure

```
PageSource.next() ─→ retriever ─→ decode task (context pool) ─→ reorderBuffer[seq % depth]
                        ↑ park while nextSeq - consumePosition ≥ depth          │
                        └──────────── unpark on consumePosition++ ←── drain ←───┘
                                                                        │ publish
                                                  consumer ←── readyQueue (BatchExchange)
                                                      └──→ freeQueue (recycling mode) ──→ drain
```

### Retriever, reorder buffer and drain

The retriever takes each `PageInfo` from its `PageSource` and gives it a sequence number and a slot in a circular reorder buffer (`AtomicReferenceArray`, `seq % MAX_INFLIGHT_PAGES`). A `PageInfo` is one of three kinds:

| Kind | Produced by | Handling |
|---|---|---|
| Data page | `IndexedFetchPlan`, `SequentialFetchPlan` | Decoded by a task on the context pool |
| Null placeholder | `SequentialFetchPlan`, when inline page statistics prove no row matches | Decoded to an all-null page without decompression, keeping the column row-aligned ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md)) |
| Boundary marker | `SkippedColumnFetchPlan` | Written straight into its slot; no decode task |

Before submitting a page, the retriever writes the slot's side buffers: file name, row-group index, page index and the row group's batches from the batch plan (`ColumnWorker.batchesBuffer`, [When a batch closes](#when-a-batch-closes)). These are plain writes; the decode task's volatile store into the reorder slot happens after them, and the drain's volatile read of that slot happens before it reads them, so the drain sees the values the retriever wrote. The slot's level scratch buffer follows the same lifecycle ([NESTED_DECODE.md](NESTED_DECODE.md)).

**Slot reuse.** The retriever reuses a slot only once `nextSeq - consumePosition < MAX_INFLIGHT_PAGES`, and the drain reads every side buffer of a slot before it advances `consumePosition` past it. A change to the throttle or to the drain's read-then-advance order must keep the previous occupant's values read before they are overwritten. Untested.

The drain takes slots in sequence order, whatever order the decode tasks finish in, so a column's pages are assembled in file order while decoded in parallel. At the first page of a row group it moves to that row group's batches ([When a batch closes](#when-a-batch-closes)), and it assembles every page that is not a boundary marker. The retriever rebuilds its `PageDecoder` when a page's column chunk metadata is incompatible with the current one, which is what a file transition brings. At the end of the source it writes an end sentinel into the next free slot; the drain publishes its partial batch and finishes the exchange. A failure on the retriever is handed over the same way, behind every page it submitted: the drain assembles those pages and then reports the failure. It publishes its partial batch first only when the retriever failed between two work items, where only a planning failure is raised. Every column meets that failure at the same row, the end of the files planned before it, so the partial batches stay aligned. A failure inside a work item is met by one column at a row the others read past; its partial batch would be shorter than theirs, and a reader taking the columns in lockstep would report the mismatch instead of the failure.

`FlatColumnWorker` copies page values into the batch's arrays and publishes the batch as soon as it holds its planned rows. `NestedColumnWorker` accumulates values and levels and closes a batch only at a record start (repetition level 0), at the next row group or at the end of the stream, so a record never spans two batches, even when it spans pages. What the arrays hold is in [COLUMN_READER.md](COLUMN_READER.md) and [NESTED_DECODE.md](NESTED_DECODE.md).

### When a batch closes

Where every batch of a read closes is decided once for the read, by its `BatchPlan`, and every decoded column's worker closes its batches there. A batch closes:

| Trigger | Applies |
|---|---|
| The batch holds its capacity in rows (flat) or records (nested) | always |
| The next row group comes from another file | always |
| The next row group's always-match flag differs from the batch's | filtered reads only ([RECORD_FILTERING.md](RECORD_FILTERING.md#homogeneous-batches)) |
| Its row group ends | reads whose decoded columns include a `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` or `INT96` leaf (`BatchPlan.endsBatchesAtRowGroupBoundaries`), so a batch draws on one column chunk's dictionary: for the column readers' dictionary ids ([COLUMN_READER.md](COLUMN_READER.md#dictionary-ids)) and the row readers' shared strings ([VALUE_DECODE.md](VALUE_DECODE.md)) |
| The row cap is reached | reads with a cap ([row limits](#row-limits)) |
| End of stream | always; the partial batch is published |

**The plan.** Every input to these rules is known before a page is decoded, counts rows, and is the same for every column: the capacity ([batch sizing](#batch-sizing)), the work items' files and always-match flags, the row cap, and each row group's kept rows, which are the rows of its `RowRanges` when page masks apply and all its rows otherwise. Masks apply to every decoded column of a row group or to none ([FETCH_PLANNING.md](FETCH_PLANNING.md#the-mask-capability-gate)), so every column keeps the same rows. A row group that keeps no row (its row ranges are empty, or its bloom filters or dictionaries drop it) takes no part: it closes no batch and leaves the row cap in place.

The plan is extended a row group at a time, as the row group's fetch plans are computed (`RowGroupIterator.computeFetchPlans`), and cached beside them. Row groups are planned in work-list order, so the plan follows the [incremental planning](#incremental-planning) of the work list and a row group's batches depend only on the row groups before it. Each row group is planned exactly once: its fetch plans and batches are computed together inside the fetch-plan cache's `computeIfAbsent`, whether on demand or by the prefetch, so every column reads the same batches, and `BatchPlan` fails the read on a row group planned out of order or a second time.

**Consumption.** A drain takes a row group's batches from the slot of its first page. It checks that it assembled every row the plan takes from the row group it leaves, publishes a batch the plan closes, and takes the file name and flag for the batches that follow. A flat worker publishes a batch the moment it holds its planned rows. A nested worker publishes it at the next record start, at the next row group or at the end of the stream, since the record's last values can follow on later pages.

**Alignment.** Every worker of a read closes its batches at the same rows by construction, since they all follow one plan. A worker whose column holds more or fewer rows of a row group than the plan takes from it fails the read with a `ParquetReadException` ("Batch plan mismatch") instead of publishing a batch out of line with its siblings. The readers poll one batch per payload column per step, and one per filter-only column in steps statistics did not prove ([RECORD_FILTERING.md](RECORD_FILTERING.md#filter-only-column-skip)), and raise `IllegalStateException` when a column has no batch or a batch of another record count. A new boundary rule belongs in `BatchPlan`, and its inputs must be known before decoding and be the same for every column.

The row-group cut costs every column of the read a batch hand-off per row group, which slows a scan over small row groups measurably, so only a read with a binary column, whose values draw on a dictionary per batch, pays it. A batch spans row groups of one file only in a read without a binary column, and **no batch straddles files**. `RecordFilterTally` attributes counts per file from the batch's file name, and both row readers take their current file name (for `FileAwareRowReader` and error context) from the first column's batch. A file is told apart by its position in the read, so a path listed twice in a row starts a new batch.

Tests: `BatchPlanTest`, `ColumnWorkerTest`, `RecordFilterEventTest`, `FileNameInExceptionTest`, `FilterOnlyColumnSkipTest`.

### BatchExchange

The exchange holds a ready queue (drain → consumer) and, in recycling mode, a free queue (consumer → drain):

| Mode | Used by | Batches | Ready-queue limit | Consumer contract |
|---|---|---|---|---|
| Recycling (`BatchExchange.recycling`) | `FlatRowReader`, `NestedRowReader` | A pool of one more holder than the ready queue holds, allocated up front; a scheduled column fills new holders past it as far as its read-ahead goes, and the pool keeps at most that many of those returned | Two batches | Returns each batch (`recycle`) when it moves to the next; a batch's arrays are valid until then |
| Detaching (`BatchExchange.detaching`) | `ColumnCursor` (column readers) | A fresh batch per publish | Two full batches' worth of rows | Owns every batch it takes; the arrays are never reused ([column-reader.md](../docs/content/how-to/column-reader.md#retaining-and-handing-off-batch-arrays)) |

A scheduled column publishes without waiting (`tryPublish`): in detaching mode an empty queue takes any batch, and a batch that would take the queue past its limit is not queued; in recycling mode a batch is queued while the bounded queue has a free slot. It takes the batch it fills with `newBatch`: a returned holder when the free queue has one, whose arrays it reuses when they are large enough, a new one from the exchange's factory otherwise. The consumer runs the column's room listener after each batch it takes, which publishes the batches that waited ([Scheduled columns](#scheduled-columns)).

**Detaching limit.** A batch ends early at a row group or a file ([When a batch closes](#when-a-batch-closes)), so a read over small row groups publishes many short batches. The detaching limit counts rows, so the queue holds what two full batches hold however the batches are cut, and a drain whose batch would take it past that waits until the consumer has taken it down to one full batch's worth. A flat batch counts the rows its arrays were allocated for rather than the rows it holds, since a batch left open into the next row group gets full-size arrays and can still end short, at a file, at a change of the always-match flag in a filtered read, or at the end of the stream; a nested batch's arrays are trimmed to its rows when it is published. Each wake-up of a waiting drain is paid on the consumer's thread; with the limit in rows, a drain is woken once per batch capacity of rows consumed rather than once per batch. A batch of no rows counts as one, so the limit also bounds the number of queued batches.

**Consumer protocol.** `poll()` tries a non-blocking poll first and reads the `finished` flag only after a poll comes back empty, so every batch published before `finish()` is delivered before the end is reported. `finish()` also offers an end-of-stream sentinel, which queues behind every batch and releases a consumer already waiting; it is best effort (the recycling queue, when full, has no room, and then the consumer has batches to take and is not waiting). In detaching mode `finish()` also wakes a drain waiting for room. A `BatchWait` JFR event spans only the timed wait, so its duration is the consumer's stall.

### Back-pressure chain

```
consumer stops polling
  → ready queue at its limit; drain blocks in publish (recycling: also on an empty free queue)
  → decoded pages accumulate in the reorder buffer
  → retriever parks on the throttle
  → PageSource.next() is not called: no further planning, fetching or decode submission for the column
consumer polls
  → drain publishes, drains the accumulated pages, advances consumePosition
  → retriever unparks and resumes
```

**How far a read runs ahead of its consumer is bounded by these queues, never by the size of the file or the number of row groups or files.** No row group's column data is fetched before a column's retriever reaches it, apart from the next row group's planning reads (the page index of its index window, and dictionaries under a filter), its first chunk and the one-ahead chunk prefetch ([FETCH_PLANNING.md](FETCH_PLANNING.md)); a consumer that stops reading stops the fetching. The per-column horizon is the reorder buffer's pages plus the batches the exchange holds, which exceeds `MAX_INFLIGHT_PAGES` pages (#370). For a scheduled column it is the scheduler's read-ahead window, in batches past the slowest column's first unpublished row, plus the span of the pages in flight ([Scheduled columns](#scheduled-columns)); the exchange's queues bound only the batches published.

Tests: `MultiFilePlanningTest`, `S3SelectiveReadJfrIT` (s3).

### Slow and fast columns

All columns submit decode tasks to the one context pool, and balancing follows from that plus per-column back-pressure; there is no balancing logic. A column with expensive pages keeps up to `MAX_INFLIGHT_PAGES` tasks queued and so holds more of the pool's threads. A column with cheap pages fills its exchange; its drain blocks, its retriever throttles and stops submitting, and the pool's capacity goes to the queued tasks of the slow column. When the consumer takes the fast column's batch, it resumes in a burst. Untested.

## Scheduled columns

```
PageSource.next() ─→ fetcher ─→ Placer: segments into cells ─→ queue (per column)
                                                                   │
                     ReadScheduler: lowest first row across the read's queues, within bounds,
                     on the thread that queued, ended or published  │ submit
                     page task (context pool): decode into cells ─→ complete ─→ publish in plan order
                                                                   │ tryPublish
                                              consumer ←── readyQueue (BatchExchange)
```

**Placement.** The fetcher takes each `PageInfo` from its `PageSource` and places it, under the column lock, with the column's `Placer`: following the row group's batches from the batch plan, it splits the page's kept records into one `Segment` per batch they fall into, opens a `Cell` at a batch's first record and seals it when the batch holds every record the plan puts in it. A flat page's kept records are known from its metadata (`PageInfo.records()`), so a page is placed before it is decoded. A boundary marker places nothing and closes the batch where the plan closes it. Placement checks the column against the plan as the drain does ("Batch plan mismatch"). The placed page joins the column's queue, which the fetcher keeps short.

**Row order.** Every page carries the read's row of its first kept record: the plan's rows before its row group (`RowGroupBatches.readRowsBefore`) plus the kept records the column placed before it in the row group. Every column computes it from the one plan, so the rows of two columns compare. The read's `ReadScheduler` submits the queued page with the lowest row, and holds a page back while a column whose queue is empty may still queue one that starts lower. The pool runs tasks in arrival order, so the tasks of the batch the consumer waits for are queued ahead of every read-ahead task, whatever the number of columns and their page sizes.

**Submission** has no thread of its own. It runs on whichever thread changes what may be submitted: a fetcher queuing a page, a page task ending, a column publishing a batch or ending. One thread submits at a time; a call made while another submits makes that thread look again (`ReadScheduler.submit`), so no change goes unseen and no thread waits to submit.

**Read-ahead** has two bounds:

| Bound | Counts | Size |
|---|---|---|
| Pages in flight | page tasks submitted and not finished, across the read's columns | a few per decode thread |
| Depth | a column's pages submitted whose rows are not all published, and the batches its next page starts past the one it publishes next | either fewer than the pool has threads, within several batch byte budgets (`BatchSizing.budgetRows`) of that batch |

The depth gives every decode thread work on a column alone, in whichever unit runs out first: pages, where a page spans several batches, or batches, where many small pages make up one and none of them stops counting until its whole batch is published. Beyond that a column decodes no further ahead than its threads need, which keeps the batches the consumer reads next in cache. The budgets cap the bytes a column decodes ahead where its pages are large against the read's byte budget. A page holding rows of the batch its column publishes next is always within the depth, which keeps the read live: that batch's cell completes once its page tasks have run. A column whose cells are all published counts from the row its next page starts at or after, so a filter-only column skipping row groups is not held back.

**Cells.** A cell takes its batch from the column's exchange and gives it arrays of at least the size the plan gives, on the first page task that reaches it (`Cell.allocate`); a task that reaches it meanwhile parks on the cell's lock until the arrays exist. A segment is written by its page's task alone. Segments of one cell are filled concurrently and write disjoint ranges of its values; a word of the validity bitmap can hold bits of two neighbouring segments, and a segment writes the words at its edges with an atomic OR (`FlatFill`). The cell completes exactly once, on the task that fills its last segment or on the fetcher when the seal comes last: a pending count starts at one for the seal, gains one per segment placed and loses one per segment filled and one at the seal.

**Measured cells.** A binary column's cell has one array the plan cannot size: the byte buffer of its plain values. Its page task first measures its page, the bytes of each of its segments, and only then fills. The cell is sized once it is sealed and every segment is measured, by whichever comes last, counted as the pending count is (`Cell.unsized`): the sizing gives each segment its offset in the buffer and the buffer the sum of their bytes, created once (`BinaryFill.size`). Every other array of the cell is plan-sized: the entry ids, installed by the first segment of dictionary ids, and the views, installed by the first segment that writes one, each under the batch's monitor (`BinaryBatchValues`). So only a segment of plain bytes can need what is not there yet. One filled before its cell is sized writes its views and validity in place and its bytes to an array of its own, with views relative to it, and parks on the cell (`Cell.park`); the sizing copies the bytes into place and moves the views by the segment's offset. Parking is per segment: the page task goes on with its other segments and ends, and nothing waits. A measured cell's pending count holds one more token, given up by the sizing once it wrote the parked segments, so the cell completes after it is sized. The completion marks the values of segments without entry ids as having none, when the batch has ids, and builds the dictionary values' views when the column's `ViewDemand` is raised or its filter reads every view ([COLUMN_READER.md](COLUMN_READER.md#variable-length)). A cell whose seal trails its last measure is sized on the fetcher, and a failure there, or in a completion the fetcher runs, fails the column at once, since a failure queued behind a cell that cannot complete is never raised. A binary read ends its batches at row-group boundaries, so its cells are sealed by the placement of their last page, before that page is measured, and the fetcher sizes none of them. Untested.

**Decoding.** A `V1` data page of a fixed-width numeric column, `PLAIN` or dictionary-encoded, is read in place by its fill (`PageDecoder.decodeFlat`, `FlatPageValues`): its values go from the decompressed page, or from the dictionary through the page's ids, straight into the cells, with no page-sized value array. A `V1` data page of a binary column is read in place the same way (`PageDecoder.decodeFlatBinary`, `BinaryPageValues`): a `PLAIN` page's measure walks its length prefixes into a prefix sum of bytes per row, and its fill copies each value's bytes from the decompressed page into the cell's buffer; a dictionary-encoded page's ids are its batch's entry ids, and it has no bytes. Every other page (`V2`, `BYTE_STREAM_SPLIT`, delta, `BOOLEAN`, a null placeholder) is decoded into a page as the decode task does it, and its fill copies from the page. The ids, definition levels and byte prefix live in the pool thread's scratch for the length of the task. A page that holds a different number of values than the rows its metadata gave its placement fails the read before it fills anything.

**Publishing.** Each column publishes its completed cells in plan order (`BatchExchange.tryPublish`), without waiting: a cell that finds the ready queue at its limit stays at the head of the column's publish order, and the consumer publishes it on its own thread when it takes a batch (`BatchExchange.setRoomListener`). No pool thread waits on the exchange, and no publish is queued behind read-ahead on the pool. A published cell drops its segments and its batch, which a page spanning several cells would otherwise keep alive. Whatever completes a cell also runs the column's filter fragment on its batch, as the drain does at publish ([RECORD_FILTERING.md](RECORD_FILTERING.md)): its last page task, or the fetcher once it has released the column lock when the seal comes last. A fragment's calls are serialized, since cells complete on several threads at once.

**Failures and close.** A page task's failure, placed with its page's file, row group and ordinal, ends the column's read at once. A fetcher's failure is raised once every cell before it is published: a page whose placement failed part way is still decoded into the cells it was placed into. A failure between work items seals the open cell first, as the drain publishes its partial batch, and one inside a work item abandons it. A failure while submitting ends every scheduled column's read. A decode pool that refuses a page task, which a closed `HardwoodContext` does, ends the read with an `IllegalStateException` whose cause is the refusal; the context's close lets the tasks already queued run, so no column waits for a task that never runs. `ScheduledColumn.close()` sets `done`, joins the fetcher and waits for every page task submitted for the column; a task is counted before its submission reads `done`, so a submission racing the close is either refused or waited for.

Tests: `ReadSchedulerTest`, `ParallelFillValidityTest`, `ParallelBinaryFillTest`, `ScheduledBinaryViewsTest`, `DictionaryFallbackNullsTest`, `BatchPlanTest`, `BatchExchangeTest`, `FilterOnlyColumnSkipTest`, `ContextClosedMidReadTest`, `ReaderCloseLatencyTest`, and the result-comparing suites, which read every fixture through the column readers at small batch capacities.

## Batch sizing

The batch capacity is fixed for a read and resolved once, at build time, by `ParquetFileReader.resolveBatchSize`, for every reader: the row readers through `createRowReader`, the column readers through `buildColumnReaders`. The workers, the exchanges and `BatchExchange.allocateArray` receive a concrete positive size.

- **Explicit.** `ColumnReaderBuilder.batchSize(int)` and `ColumnReadersBuilder.batchSize(int)` take a positive record count, used verbatim. The row readers have no explicit size.
- **Default.** `BatchSizing.computeOptimalBatchSize` divides a per-batch byte budget, sized so a batch amortises the per-batch overhead, by the bytes one row costs across the decoded columns, predicate columns included, since they allocate batch arrays too. A column costs its value width times its list fan-out, plus two `int` levels per value for a repeated column, whose nested worker holds both level arrays. Fan-out is the column's leaf value count over the row count, summed over row-group metadata (`BatchSizing.valuesPerRow`); a flat column's is 1. The per-value work of a batch is therefore roughly constant across fan-outs, and no row floor applies.
- **Reference arrays.** `ColumnReader.getStrings()` and `getBinaries()` build a reference array of a binary column's values per batch. Under G1, an object larger than half a heap region is humongous, allocated in whole old-generation regions, and allocating one per batch makes garbage collection dominate the read. `BatchSizing.MAX_BATCH` is the longest reference array that stays an ordinary object at G1's smallest region (1 MB) with compressed references, and every `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` and `INT96` column's values per batch, its rows times its fan-out, are capped at it. A primitive column's values are bounded by the byte budget alone, so a wide list of numbers keeps its batch. The cap uses the average fan-out, so a batch of a repeated binary column whose records run longer than average can still exceed it.
- **Clamps.** The default is at least 1 and at most `BatchSizing.MAX_BATCH`, which other components pre-size around (`FlatRowReader`'s all-present validity). It is also at most the rows the read can produce: the total row count of the file's row groups after the `RowGroupPredicate`, or `BatchSizing.ROWS_UNKNOWN` (no clamp) for a multi-file read. The bound must be an upper bound; pruning, a row cap and a skip only remove rows, so the clamp never shortens a batch the read could have filled.
- **First file only.** Fan-out and the row bound come from the first file's row groups, the one footer `openAll` reads. Sizing from later files would plan them before the first row (see [the planning invariant](#incremental-planning)).

The row path and the column path produce the same size for the same decoded projection. Independently built readers with different decoded columns size differently ([RECORD_FILTERING.md](RECORD_FILTERING.md#column-readers)).

Tests: `BatchSizingTest`, `WideListBatchBoundTest`.

## Row groups, files and multi-file planning

### The work list

`RowGroupIterator` holds an ordered work list of `WorkItem`s, one per surviving row group: the file, the row group and its index in the file, the file's schema and column ordinals, the item's index in the list, the rows planned before it, the row group's always-match flag and the per-leaf filter decisions its dictionaries sharpen later.

Each `PageSource` walks the list with an integer cursor through `workItemAt(i)`, which returns `null` when the read is done. For each item it asks `getColumnPlan`, whose plans for all decoded columns are computed by the first column to arrive and cached per item, and drains its column's plan. When the plan is exhausted it calls `releaseWorkItem`; each item carries a reference count of the decoded columns, and the last release evicts the item's cached metadata and plans. Page slices and decode tasks in flight keep their bytes alive until they finish.

### Incremental planning

`planNextFile()` plans exactly one file and appends its surviving row groups: it loads the footer through the `FileMetadataCache`, validates the file's schema against the reference schema (the first file's), resolves the file's own leaf ordinals for the touched columns, checks that the chunk at each touched ordinal carries that leaf's path, prunes row groups on their statistics ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md)), applies the physical skip and the unfiltered row budget, and starts loading the next file's footer. It returns `false` once every file is planned or the budget is spent. A row group's bloom filters and dictionaries are read when the read reaches it. The row budget, rows planned, skip residue and first-row-group skip are fields, since each call resumes where the last one stopped.

The cursor is an integer rather than an `Iterator` because the list grows behind it: an iterator would force the whole plan at construction, or fail on concurrent modification once planning appends.

**Invariant: reader structure is never derived from the contents of individual row groups in individual files.** A decision the reader takes once, at construction, from facts about row groups would force every file to be planned before the first row, returning time to first row to O(files). Facts about a row group reach the pipeline on its work item:

| Question | Answered |
|---|---|
| Does statistics prove every row matches? | Per row group: `WorkItem.filterAlwaysMatches` → batch plan → per-slot buffer → `Batch.filterAlwaysMatches` |
| Where do batches close, and does the row cap still hold under a filter? | Per row group, by the batch plan, as the row group's fetch plans are computed ([When a batch closes](#when-a-batch-closes), [row limits](#row-limits)) |
| Drain-side or per-row filter evaluation? | From the predicate's shape alone (`BatchFilterCompiler.tryCompile`) |
| Batch size | From the first file ([batch sizing](#batch-sizing)) |
| Does the read have any work? | `hasLiveWorkItem()`, asked by every reader before it starts its workers: walks the work list until the first row group its bloom filters and dictionaries do not drop, so it plans and probes only as far as that row group, and leaves that row group's page index to the workers |

Two questions concern the whole read and plan only as far as they must. `firstRowGroupSkip()` answers `0` without planning when there is no physical skip, and otherwise plans through the first work item, where the value is written. `canFastSkipAllRowGroups()`, asked by the tail-read path, needs every row group; that path is single-file, so a full plan is one footer. `getWorkItems()` plans the whole read and serves tests only.

**Concurrency.** Every column's retriever walks the list, and a step past the planned end plans another file, so planning is synchronized on the iterator: `workItemAt` and `getWorkItems` hold the monitor, and `getWorkItems` returns a copy. Reference counts are decremented outside the lock on every retriever, so they are `AtomicInteger`s in a `ConcurrentHashMap`. Planning blocks (a footer read) and must not run inside a `ConcurrentHashMap` mapping function, which holds a bin lock for its whole duration: `getColumnPlan` starts the next row group's prefetch only after its `computeIfAbsent` returns.

**Invariant: a planning failure is sticky.** Once planning a file has thrown, the iterator keeps the exception and rethrows it, as the type it was raised as, from every later planning call and every later `workItemAt` past the files planned before it. The column that ran the plan reports the failure only after it has left the monitor, so a column that arrived afterwards and found nothing left to plan would end its stream cleanly, and a consumer polling that column first would see a short read. The failed file is not planned again, since the row groups it appended before failing are already on the list, and no file after it is planned; its own row groups on the list are never handed out. A work item of a file planned before it still is: the column whose prefetch planned the failed file can be ahead of the others, and a column failed short of it would return fewer rows than they do.

**Error timing.** The first file's footer is read by `openAll`, so its errors surface there. A later file's I/O errors and its disagreement with the reference schema are raised when the read reaches the file, from the reading loop, and before any row of it is returned; a mismatched file is never read as data. The next file is planned when the previous file's last row group is entered, but a failure there is not raised until a column asks for the failed file's first work item, so every row of the files before it is returned first, and the failure is placed at the file it belongs to rather than at the row group the read was in.

**Reading every row opens every file.** Planning is deferred, not avoided. A read that stops early (a row budget, a drain-held cap, a consumer that closes) leaves the files past its stop unopened, up to the back-pressure horizon. A read whose data totals less than one batch walks every file to fill the first batch.

The user-facing contract (reference schema, when a mismatch surfaces) is in [multi-file.md](../docs/content/how-to/multi-file.md).

Tests: `MultiFilePlanningTest`, `SchemaCompatibilityTest`, `CrossFileColumnOrderTest`, `MultiFileRowReaderTest`, `RowGroupIteratorPlanningFailureTest`, `LaterFileFailureTest`.

### Row-group and file transitions

Within a file, a batch continues from one row group into the next, except where the always-match flag changes on a filtered read or the read ends its batches at row-group boundaries ([When a batch closes](#when-a-batch-closes)).

The first column to reach a row group also prefetches the next row group, once per row group, whether it computes the row group's plans or finds them cached by the previous prefetch. The prefetch task plans the next row group without passing through this step, so prefetch stays one row group ahead and does not cascade. The column resolves the next work item on its own thread through `workItemAt`, so the last row group of a file plans the next file. A planning failure there does not fail the current row group: the iterator keeps it, and each column's own `workItemAt` for the next item rethrows it. It then computes the next item's plans on the common pool and prefetches the first chunk of the first plan that reads one, passing over empty plans and skip plans. The chunks within a row group follow the same rule one level down ([FETCH_PLANNING.md](FETCH_PLANNING.md#chunk-handles-and-prefetch)). The task plans nothing if the iterator is closed or every column has already released the item, which would repeat its dictionary and index reads. Combined with the footer load started when the previous file was planned, the next file's metadata and first bytes are in flight by the time the current file is exhausted, and the transition does not stall the consumer. Local and remote files differ only in what a fetch costs ([FETCH_PLANNING.md](FETCH_PLANNING.md)).

### Filter flags in the pipeline

The always-match decision travels from the retriever to the drain in the row group's batches, in the per-slot `batchesBuffer`, beside the file name, under the same happens-before chain. A filter-only column in an always-match row group gets a `SkippedColumnFetchPlan`: its boundary marker goes straight into the retriever's reorder slot with no decode task, and the next row group's prefetch passes over the skip plan. On the marker the drain publishes the batch the plan closes before the row group, assembles nothing, and finishes where the row cap ends the read in the row group. The consumer-side rules are in [RECORD_FILTERING.md](RECORD_FILTERING.md#filter-only-column-skip).

## Row limits, errors and cancellation

### Row limits

`maxRows` (from `head(n)`, and `n + head` for a filtered `skip(n)`, see [ROW_READER.md](ROW_READER.md)) reaches the iterator and the batch plan of a row reader. For an unfiltered `skip`, the batch plan gets `head` plus the skip's residue within the first kept row group, which the reader discards. It is enforced at four levels:

| Level | Where | Unfiltered read | Filtered read |
|---|---|---|---|
| Planning | `planNextFile` row budget | Stops planning row groups and files once the budget is covered | No effect: every row group stays available |
| Fetch | Per-row-group remainder of the budget ([FETCH_PLANNING.md](FETCH_PLANNING.md)) | Truncates the last needed row group's pages where their row counts are known without decompressing: from the OffsetIndex, or from the value count of a flat column. A nested column without an OffsetIndex keeps its pages, and its retriever reads them until the drain reaches the cap | No truncation |
| Batch plan | `BatchPlan` | Ends the last batch at the cap; every worker publishes it and finishes | Holds the cap while every row group with kept rows is an always-match one; drops it for the rest of the read at the first undecided one |
| Reader | Match counting | — | Counts matches against the cap |

A page is the smallest decode unit: a page that straddles the cap is decoded whole and assembled in part. The column readers carry no row cap (#433).

Tests: `BatchPlanTest`, `ColumnWorkerTest`, `AlwaysMatchingRowGroupTest`, `MultiFilePlanningTest`, `S3SelectiveReadJfrIT` (s3).

### Error propagation

A failure on any of a worker's threads goes through `signalError`, which sets `done`, ends the exchange with the failure and wakes both threads; a decode task that has not started returns without decoding once `done` or an error is set. The exchange raises the first failure it receives, since that is the one that ended the read; each later failure of the same worker is attached to it as a suppressed exception, so a later `Error` such as an `OutOfMemoryError`, whose rethrow on a decode task's thread nothing observes, still reaches the caller.

Each failure is placed before it leaves the worker: the column path is the worker's own, and the file, row group and page come from what the failing thread holds (the retriever from its `PageSource`, a decode task and the drain from the page's slot buffers). A runtime failure is classified as a read failure (`ExceptionContext.asReadFailure`, [EXCEPTION_MODEL.md](EXCEPTION_MODEL.md#crossing-a-thread-boundary)); an `IOException` is restated as a checked `IOException` carrying the place, unless its message already names a file; an `Error` is recorded unplaced and rethrown on its thread. `BatchExchange.checkError` rethrows each as it stands, so the reader's signature declares `IOException` and nothing between the worker and the reader wraps. The exception types a caller sees are in [error-handling.md](../docs/content/reference/error-handling.md).

The consumer receives every batch published before the failure, then the failure. A reader that finds any column's stream ended checks every exchange for an error before reporting the end, so a failure on a column other than the first, a filter-only column included, is not reported as a short read.

Tests: `ColumnWorkerTest`, `ReadFailureTypingTest`, `ReadFailurePositionTest`, `ColumnReadersTransportFailureTest`.

### Cancellation and close

`ColumnWorker.close()` sets `done`, finishes the exchange, wakes both threads, interrupts the drain, joins the retriever and the drain, and then waits for every decode task that is running. It returns only once the column is quiescent, so the caller can release what the `InputFile` owns (mapped buffers, direct buffers, connections) without a decode task reading freed memory. The joins do not give up on an interrupt; a caller interrupted while closing gets its flag back afterwards.

After close, the retriever pulls no further page, so no chunk it has not reached is fetched beyond prefetches already started; decode tasks not yet started return without decoding; and speculative plan tasks see the iterator closed and plan nothing. A reader closes its workers, returns its held and queued batches to the pool (recycling mode), and then closes its `RowGroupIterator`. The iterator stops admitting speculative tasks, waits for those already started, clears its caches and, when it owns them, closes the files; its caller has stopped the workers first, so no retriever starts a task during the wait. A file is therefore never closed under a read in flight, whichever reader closes it. Close is idempotent, and no column's close waits out a timed queue wait, since the interrupt releases each drain at once.

Tests: `ColumnWorkerTest`, `ReaderCloseLatencyTest`, `RowReaderCloseIdempotencyTest`, `IteratorTrackingTest`, `ParentCloseTest`, `RowGroupIteratorCloseTest`, `S3SelectiveReadJfrIT` (s3).

## Configuration (`ReaderConfig`)

Read-time behaviour lives on `ReaderConfig`, runtime resources on `HardwoodContext`:

| Object | Nature | Lifetime | Holds |
|---|---|---|---|
| `HardwoodContext` | Stateful, `AutoCloseable`, shared | Spans many reads | Decode pool, libdeflate pool, decompressor factory |
| `ReaderConfig` | Immutable value, no lifecycle | One `open`/`openAll` | Behaviour options |

Sizing the decode pool is a resource concern and stays on the context. A behaviour option on the context would tie it to a thread pool, so reading two files with different settings would need two pools; one context backs reads with different configs.

`ReaderConfig` is a string-keyed option bag (`Builder.option(String, String)`) with no per-option API. The recognised keys, their defaults and their resolution are private to `ParquetFileReader`, which reads the map once at `open`/`openAll`, resolves each key to a plain value and threads it to the workers. An option can therefore be retired without changing a public type: its key leaves the recognised set, and callers passing it keep compiling. An unrecognised key is ignored and logged at `WARNING` when the reader opens, so a mistyped key does not silently take the default. Untested.

| Key | Default | Kind | Gates |
|---|---|---|---|
| `hardwood.fixed-list-fast-path` | `false` (opt-in) | Transitional: the default flips once the path is trusted, then the key is retired | The fixed-size-list decode fast path ([NESTED_DECODE.md](NESTED_DECODE.md)) |
| `hardwood.metadata-filtering` | `true` | Permanent escape hatch for files with unreliable metadata | Every metadata-derived filter decision ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md)) |

A stable knob that needs a typed contract stays typed on its builder and out of the map, as `batchSize(int)` does on the column-reader builders. The `hardwood.internal.*` system properties (reorder depth, coalescing span, sequential chunk size, page-index window budget, and `hardwood.internal.batchSize`, which replaces the auto batch size so the test suites can run at small batch capacities) are overrides for tuning and testing and not part of `ReaderConfig`. The user-facing option reference is [reader.md](../docs/content/reference/reader.md#reader-options).

Tests: `MetadataFilteringOptionTest`, `FixedSizeListFastPathReadTest`.

## Boundaries

- **Read-ahead beyond the throttle (#370).** The throttle counts pages in the reorder buffer only; the batches in the exchange add to the per-column horizon.
- **Files stay open until the reader closes (#100).** A multi-file read releases no file as it moves past it.
- **Single-threaded mode (#1022).** A read always uses the retriever, drain and decode threads, or the fetcher and page tasks, even on a one-thread context.
- **Schema drift across files (#941).** Every file must match the reference schema for the columns a read touches.
- **`BYTE_ARRAY` width estimate (#899).** Batch sizing guesses a fixed width per `BYTE_ARRAY` value instead of reading the recorded unencoded size.
- **Row caps on the column readers (#433).** `ColumnReaderBuilder` and `ColumnReadersBuilder` have no `head`, so no cap reaches their workers.
