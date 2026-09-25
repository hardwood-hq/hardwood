# Read pipeline

How a read turns the row groups it has planned into published batches: the shared `RowGroupIterator`, the per-column `PageSource` → `ColumnWorker` → `BatchExchange` chain, the threads that run it, back-pressure, batch sizing, row-group and file transitions, multi-file planning, row limits, error propagation, teardown and `ReaderConfig`. Which bytes a read requests and how they are fetched (fetch plans, chunk handles, coalescing, page masks) is in [FETCH_PLANNING.md](FETCH_PLANNING.md); what a batch holds and how a reader exposes it is in [COLUMN_READER.md](COLUMN_READER.md) and [ROW_READER.md](ROW_READER.md); how pages become values and nested batches is in [VALUE_DECODE.md](VALUE_DECODE.md) and [NESTED_DECODE.md](NESTED_DECODE.md). Filter evaluation over the published batches is in [RECORD_FILTERING.md](RECORD_FILTERING.md), and the user-facing model in [concurrency-model.md](../docs/content/concepts/concurrency-model.md).

## Components and threading

Every data reader (`FlatRowReader`, `NestedRowReader`, a `ColumnReaders` group, and a single `ColumnReader`, which is a group of one) runs the same pipeline, for flat and nested schemas and for one file or many. A single-file read is a multi-file read of one file; there are no separate reader classes for either.

| Component | Count | Job |
|---|---|---|
| `RowGroupIterator` | one per data reader | Plans the work list of `(file, row group)` pairs a file at a time, caches per-row-group metadata and fetch plans, prefetches the next row group and the next footer |
| `PageSource` | one per decoded column | Walks the work list with its own cursor and yields that column's `PageInfo`s, draining each row group's `FetchPlan` |
| `ColumnWorker` | one per decoded column | Decodes pages in parallel and assembles them into batches, in file order (`FlatColumnWorker`, `NestedColumnWorker`) |
| `BatchExchange` | one per decoded column | Bounded hand-off of published batches from the worker to the consumer |

The decoded columns are the projection plus any filter-only predicate columns (`ReadProjection.decoded()`, see [RECORD_FILTERING.md](RECORD_FILTERING.md#augmented-projection)).

**Ownership.** `ParquetFileReader` reads the first file's footer at `open`/`openAll` and owns the input files and a `FileMetadataCache` of parsed footers. When that fails, it closes every input file, and the context when the call created it, before the exception propagates, attaching close failures as suppressed. Each data reader it builds gets its own `RowGroupIterator` sharing that cache; the parent tracks the iterator until the child closes it, and closing a child releases the iterator's caches and its tracking entry but leaves the files to the parent. An iterator built outside a `ParquetFileReader` owns its files and closes them.

Tests: `MultiFileRowReaderTest`, `IteratorTrackingTest`.

### Thread inventory

| Thread | Count | Lifetime | Does |
|---|---|---|---|
| Retriever (virtual) | one per decoded column | whole read | Pulls pages from the `PageSource`, which plans files, computes fetch plans and fetches page bytes on demand; submits decode tasks; throttles |
| Drain (virtual) | one per decoded column | whole read | Takes decoded pages in sequence order, assembles batches, runs drain-side filter fragments, publishes |
| Decode task | up to the reorder depth per column | one page | Decompresses and decodes one page on the `HardwoodContext` pool, stores the result, wakes the drain |
| Speculative task | as triggered | one step | On the common `ForkJoinPool` (a new thread per task when its parallelism is below 2): the next file's footer load, the next row group's fetch plans and first-chunk prefetch, one-ahead chunk prefetches |
| Consumer | the caller's thread | caller-managed | Polls every column's exchange and reads the batches |

Decode runs only on the context's fixed platform-thread pool (`HardwoodContext.create(n)`, default one thread per processor), which every reader sharing the context submits to. The pool size is the bound on decode parallelism. Decode is CPU-bound and runs as one task per page, so it stays off the virtual-thread carriers, which every virtual thread in the process shares, and runs on a pool the caller sizes. The retriever and drain are virtual threads on the JVM's default scheduler: they are cheap per column and spend their time blocked on I/O, queues and each other.

**The drain does no I/O.** Every `InputFile` access happens on the retriever or on a decode task. `ColumnWorker.close()` relies on this: it interrupts the drain to release it from a timed queue wait, and interrupting a thread inside a `FileChannel` operation closes the channel for every reader sharing it. A change that gives the drain its own file access (fetching on demand instead of waiting for the retriever) must drop that interrupt first.

**Speculative work never reports a failure.** A failed plan or chunk prefetch leaves its plan or chunk unfetched, and the demand path repeats the work; a failed footer load stays cached in the `FileMetadataCache`, and the demand path rethrows it. Either way the failure is raised to a caller that is waiting for it. Work whose failure must surface stays on the demand path: resolving the next work item, which validates the next file's schema, runs on the retriever before the speculative task is started.

**Every wait is bounded.** The retriever's throttle, the drain's wait for a decode task, and the exchange's queue operations on both sides are timed waits; the unparks and the exchange's end-of-stream sentinel make the common case immediate, and the bound guarantees a waiter is released at all. Untimed parks after timed parks on a virtual thread can be stranded by the runtime (JDK-8369227, fixed in 25.0.3 and 26.0.1), leaving a drain parked forever with its page already decoded.

Tests: `ColumnWorkerTest`, `ReaderCloseLatencyTest`, `ReaderEofLatencyTest`, `BatchExchangeTest`.

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

Before submitting a page, the retriever writes the slot's side buffers: file name, row-group index, page index and the row group's always-match flag (`ColumnWorker.filterAlwaysMatchesBuffer`). These are plain writes; the decode task's volatile store into the reorder slot happens after them, and the drain's volatile read of that slot happens before it reads them, so the drain sees the values the retriever wrote. The slot's level scratch buffer follows the same lifecycle ([NESTED_DECODE.md](NESTED_DECODE.md)).

**Slot reuse.** The retriever reuses a slot only once `nextSeq - consumePosition < MAX_INFLIGHT_PAGES`, and the drain reads every side buffer of a slot before it advances `consumePosition` past it. A change to the throttle or to the drain's read-then-advance order must keep the previous occupant's values read before they are overwritten. Untested.

The drain takes slots in sequence order, whatever order the decode tasks finish in, so a column's pages are assembled in file order while decoded in parallel. For each page it runs the per-page bookkeeping (file-change flush, row-cap rule, always-match flush) and then assembles the page unless it is a boundary marker. The retriever rebuilds its `PageDecoder` when a page's column chunk metadata is incompatible with the current one, which is what a file transition brings. At the end of the source it writes an end sentinel into the next free slot; the drain publishes its partial batch and finishes the exchange.

`FlatColumnWorker` copies page values into arrays of fixed capacity. `NestedColumnWorker` accumulates values and levels and closes a batch only at a record start (repetition level 0), so a record never spans two batches, even when it spans pages. What the arrays hold is in [COLUMN_READER.md](COLUMN_READER.md) and [NESTED_DECODE.md](NESTED_DECODE.md).

### When a batch closes

| Trigger | Applies |
|---|---|
| The batch holds its capacity in rows (flat) or records (nested) | always |
| The next page comes from a file with another name | always |
| The next page's always-match flag differs from the batch's | filtered reads only ([RECORD_FILTERING.md](RECORD_FILTERING.md#homogeneous-batches)) |
| The row cap is reached | reads with a cap the drain holds |
| End of stream | always; the partial batch is published |

A batch may span row groups of one file, but **never straddles files**. `RecordFilterTally` attributes counts per file from the batch's file name, both row readers take their current file name (for `FileAwareRowReader` and error context) from the first column's batch, and a boundary marker takes part in the change-of-file flush like any page. The change is detected by file name, so a path listed twice in a row reads as one file.

**Alignment.** Every worker of a read applies the same rules to the same sequence of row groups, pages, masks and capacity, so all decoded columns close their batches at the same rows. Nothing coordinates the workers; the readers poll one batch per payload column per step, and one per filter-only column in steps statistics did not prove ([RECORD_FILTERING.md](RECORD_FILTERING.md#filter-only-column-skip)), and raise `IllegalStateException` when a column has no batch or a batch of another record count. Any new flush rule must be one every column evaluates identically.

Tests: `ColumnWorkerTest`, `RecordFilterEventTest`, `FileNameInExceptionTest`, `FilterOnlyColumnSkipTest`.

### BatchExchange

The exchange holds a bounded ready queue (drain → consumer) and, in recycling mode, a free queue (consumer → drain):

| Mode | Used by | Batches | Consumer contract |
|---|---|---|---|
| Recycling (`BatchExchange.recycling`) | `FlatRowReader`, `NestedRowReader` | A fixed pool of one more holder than the ready queue holds, allocated up front | Returns each batch (`recycle`) when it moves to the next; a batch's arrays are valid until then |
| Detaching (`BatchExchange.detaching`) | `ColumnCursor` (column readers) | A fresh batch per publish | Owns every batch it takes; the arrays are never reused ([column-reader.md](../docs/content/how-to/column-reader.md#retaining-and-handing-off-batch-arrays)) |

**Consumer protocol.** `poll()` tries a non-blocking poll first and reads the `finished` flag only after a poll comes back empty, so every batch published before `finish()` is delivered before the end is reported. `finish()` also offers an end-of-stream sentinel, which queues behind every batch and releases a consumer already waiting; it is best effort (a full queue has no room, and then the consumer has batches to take and is not waiting). A `BatchWait` JFR event spans only the timed wait, so its duration is the consumer's stall.

### Back-pressure chain

```
consumer stops polling
  → ready queue full; drain blocks in publish (recycling: also on an empty free queue)
  → decoded pages accumulate in the reorder buffer
  → retriever parks on the throttle
  → PageSource.next() is not called: no further planning, fetching or decode submission for the column
consumer polls
  → drain publishes, drains the accumulated pages, advances consumePosition
  → retriever unparks and resumes
```

**How far a read runs ahead of its consumer is bounded by these queues, never by the size of the file or the number of row groups or files.** No row group's column data is fetched before a column's retriever reaches it, apart from the next row group's planning reads (page index, and dictionaries under a filter), its first chunk and the one-ahead chunk prefetch ([FETCH_PLANNING.md](FETCH_PLANNING.md)); a consumer that stops reading stops the fetching. The per-column horizon is the reorder buffer's pages plus the batches the exchange holds, which exceeds `MAX_INFLIGHT_PAGES` pages (#370).

Tests: `MultiFilePlanningTest`, `S3SelectiveReadJfrIT` (s3).

### Slow and fast columns

All columns submit decode tasks to the one context pool, and balancing follows from that plus per-column back-pressure; there is no balancing logic. A column with expensive pages keeps up to `MAX_INFLIGHT_PAGES` tasks queued and so holds more of the pool's threads. A column with cheap pages fills its exchange; its drain blocks, its retriever throttles and stops submitting, and the pool's capacity goes to the queued tasks of the slow column. When the consumer takes the fast column's batch, it resumes in a burst. Untested.

## Batch sizing

The batch capacity is fixed for a read and resolved once, at build time, by `ParquetFileReader.resolveBatchSize`, for every reader: the row readers through `createRowReader`, the column readers through `buildColumnReaders`. The workers, the exchanges and `BatchExchange.allocateArray` receive a concrete positive size.

- **Explicit.** `ColumnReaderBuilder.batchSize(int)` and `ColumnReadersBuilder.batchSize(int)` take a positive record count, used verbatim. The row readers have no explicit size.
- **Default.** `BatchSizing.computeOptimalBatchSize` divides a per-batch byte budget sized for the L2 cache by the bytes one row costs across the decoded columns, predicate columns included, since they allocate batch arrays too. A column costs its value width times its list fan-out, plus two `int` levels per value for a repeated column, whose nested worker holds both level arrays. Fan-out is the column's leaf value count over the row count, summed over row-group metadata (`BatchSizing.valuesPerRow`); a flat column's is 1. The per-value work of a batch is therefore roughly constant across fan-outs, and no row floor applies.
- **Clamps.** The default is at least 1 and at most `BatchSizing.MAX_BATCH`, which other components pre-size around (`FlatRowReader`'s all-present validity). It is also at most the rows the read can produce: the total row count of the file's row groups after the `RowGroupPredicate`, or `BatchSizing.ROWS_UNKNOWN` (no clamp) for a multi-file read. The bound must be an upper bound; pruning, a row cap and a skip only remove rows, so the clamp never shortens a batch the read could have filled.
- **First file only.** Fan-out and the row bound come from the first file's row groups, the one footer `openAll` reads. Sizing from later files would plan them before the first row (see [the planning invariant](#incremental-planning)).

The row path and the column path produce the same size for the same decoded projection. Independently built readers with different decoded columns size differently ([RECORD_FILTERING.md](RECORD_FILTERING.md#column-readers)).

Tests: `BatchSizingTest`, `WideListBatchBoundTest`.

## Row groups, files and multi-file planning

### The work list

`RowGroupIterator` holds an ordered work list of `WorkItem`s, one per surviving row group: the file, the row group and its index in the file, the file's schema and column ordinals, the item's index in the list, the rows planned before it, the row group's always-match flag and the per-leaf filter decisions its dictionaries sharpen later.

Each `PageSource` walks the list with an integer cursor through `workItemAt(i)`, which returns `null` when the read is done. For each item it asks `getColumnPlan`, whose plans for all decoded columns are computed by the first column to arrive and cached per item, and drains its column's plan. When the plan is exhausted it calls `releaseWorkItem`; each item carries a reference count of the decoded columns, and the last release evicts the item's cached metadata and plans. Page slices and decode tasks in flight keep their bytes alive until they finish.

### Incremental planning

`planNextFile()` plans exactly one file and appends its surviving row groups: it loads the footer through the `FileMetadataCache`, validates the file's schema against the reference schema (the first file's), resolves the file's own leaf ordinals for the touched columns, checks that the chunk at each touched ordinal carries that leaf's path, prunes row groups (statistics and bloom filters, see [STATISTICS_PRUNING.md](STATISTICS_PRUNING.md)), applies the physical skip and the unfiltered row budget, and starts loading the next file's footer. It returns `false` once every file is planned or the budget is spent. A row group's dictionaries are read when the read reaches it. The row budget, rows planned, skip residue and first-row-group skip are fields, since each call resumes where the last one stopped.

The cursor is an integer rather than an `Iterator` because the list grows behind it: an iterator would force the whole plan at construction, or fail on concurrent modification once planning appends.

**Invariant: reader structure is never derived from the contents of individual row groups in individual files.** A decision the reader takes once, at construction, from facts about row groups would force every file to be planned before the first row, returning time to first row to O(files). Facts about a row group reach the pipeline on its work item:

| Question | Answered |
|---|---|
| Does statistics prove every row matches? | Per row group: `WorkItem.filterAlwaysMatches` → per-slot buffer → `Batch.filterAlwaysMatches` |
| May the drain hold the row cap under a filter? | Per page, from the same flag ([row limits](#row-limits)) |
| Drain-side or per-row filter evaluation? | From the predicate's shape alone (`BatchFilterCompiler.tryCompile`) |
| Batch size | From the first file ([batch sizing](#batch-sizing)) |
| Does the read have any work? | `workItemAt(0) == null`, which plans only as far as the first surviving row group |

Two questions concern the whole read and plan only as far as they must. `firstRowGroupSkip()` answers `0` without planning when there is no physical skip, and otherwise plans through the first work item, where the value is written. `canFastSkipAllRowGroups()`, asked by the tail-read path, needs every row group; that path is single-file, so a full plan is one footer. `getWorkItems()` plans the whole read and serves tests only.

**Concurrency.** Every column's retriever walks the list, and a step past the planned end plans another file, so planning is synchronized on the iterator: `workItemAt` and `getWorkItems` hold the monitor, and `getWorkItems` returns a copy. Reference counts are decremented outside the lock on every retriever, so they are `AtomicInteger`s in a `ConcurrentHashMap`. Planning blocks (a footer read, bloom-filter probes) and must not run inside a `ConcurrentHashMap` mapping function, which holds a bin lock for its whole duration: `getColumnPlan` starts the next row group's prefetch only after its `computeIfAbsent` returns.

**Error timing.** The first file's footer is read by `openAll`, so its errors surface there. A later file's I/O errors and its disagreement with the reference schema are raised when the read reaches the file, from the reading loop, and before any row of it is returned; a mismatched file is never read as data. Because the next file is planned when the previous file's last row group is entered, the error can arrive while that file's last batches are in flight.

**Reading every row opens every file.** Planning is deferred, not avoided. A read that stops early (a row budget, a drain-held cap, a consumer that closes) leaves the files past its stop unopened, up to the back-pressure horizon. A read whose data totals less than one batch walks every file to fill the first batch.

The user-facing contract (reference schema, when a mismatch surfaces) is in [multi-file.md](../docs/content/how-to/multi-file.md).

Tests: `MultiFilePlanningTest`, `SchemaCompatibilityTest`, `CrossFileColumnOrderTest`, `MultiFileRowReaderTest`.

### Row-group and file transitions

Within a file, a column moves from one row group to the next without flushing its batch, except where the always-match flag changes on a filtered read.

The first column to compute a row group's plans also prefetches the next row group. It resolves the next work item on its own thread through `workItemAt`, so the last row group of a file plans the next file, and a schema mismatch there propagates to the reader instead of being lost in a background task. It then computes the next item's plans on the common pool and prefetches the first chunk of the first plan that reads one, passing over empty plans and skip plans. The task plans nothing if the iterator is closed or every column has already released the item, which would repeat its dictionary and index reads. Combined with the footer load started when the previous file was planned, the next file's metadata and first bytes are in flight by the time the current file is exhausted, and the transition does not stall the consumer. Local and remote files differ only in what a fetch costs ([FETCH_PLANNING.md](FETCH_PLANNING.md)).

### Filter flags in the pipeline

The always-match decision travels from the retriever to the drain in the per-slot `filterAlwaysMatchesBuffer`, beside the file name, under the same happens-before chain. A filter-only column in an always-match row group gets a `SkippedColumnFetchPlan`: its boundary marker goes straight into the retriever's reorder slot with no decode task, the next row group's prefetch passes over the skip plan, and the drain treats the marker as a proven page, including in the change-of-file flush. The consumer-side rules are in [RECORD_FILTERING.md](RECORD_FILTERING.md#filter-only-column-skip).

## Row limits, errors and cancellation

### Row limits

`maxRows` (from `head(n)`, and `n + head` for a filtered `skip(n)`, see [ROW_READER.md](ROW_READER.md)) reaches the iterator and every worker of a row reader. For an unfiltered `skip`, the workers get `head` plus the skip's residue within the first kept row group, which the reader discards. It is enforced at four levels:

| Level | Where | Unfiltered read | Filtered read |
|---|---|---|---|
| Planning | `planNextFile` row budget | Stops planning row groups and files once the budget is covered | No effect: every row group stays available |
| Fetch | Per-row-group remainder of the budget ([FETCH_PLANNING.md](FETCH_PLANNING.md)) | Truncates the last needed row group's pages where their row counts are known without decompressing: from the OffsetIndex, or from the value count of a flat column. A nested column without an OffsetIndex keeps its pages, and its retriever reads them until the drain reaches the cap | No truncation |
| Drain | `ColumnWorker.activeMaxRows` | Assembles up to the cap, publishes the partial batch and finishes | Holds the cap while every page reached belongs to an always-match row group; drops it for the rest of the read at the first page of an undecided one |
| Reader | Match counting | — | Counts matches against the cap |

A page is the smallest decode unit: a page that straddles the cap is decoded whole and assembled in part. The column readers carry no row cap (#433).

Tests: `ColumnWorkerTest`, `AlwaysMatchingRowGroupTest`, `MultiFilePlanningTest`, `S3SelectiveReadJfrIT` (s3).

### Error propagation

A failure on any of a worker's threads goes through `signalError`, which sets `done`, ends the exchange with the failure and wakes both threads; a decode task that has not started returns without decoding once `done` or an error is set. The exchange raises the first failure it receives, since that is the one that ended the read; each later failure of the same worker is attached to it as a suppressed exception, so a later `Error` such as an `OutOfMemoryError`, whose rethrow on a decode task's thread nothing observes, still reaches the caller.

Each failure is placed before it leaves the worker: the column path is the worker's own, and the file, row group and page come from what the failing thread holds (the retriever from its `PageSource`, a decode task and the drain from the page's slot buffers). A runtime failure is classified as a read failure (`ExceptionContext.asReadFailure`): an `UncheckedIOException`, a `ParquetReadException` or an `UnsupportedOperationException` keeps its type, any other is wrapped in a `ParquetReadException` with the original as cause; an `IOException` is restated as a checked `IOException` carrying the place; an `Error` is recorded unplaced and rethrown on its thread. `BatchExchange.checkError` rethrows each as it stands, so the reader's signature declares `IOException` and nothing between the worker and the reader wraps. The exception types a caller sees are in [error-handling.md](../docs/content/reference/error-handling.md).

The consumer receives every batch published before the failure, then the failure. A reader that finds any column's stream ended checks every exchange for an error before reporting the end, so a failure on a column other than the first, a filter-only column included, is not reported as a short read.

Tests: `ColumnWorkerTest`, `ReadFailureTypingTest`, `ReadFailurePositionTest`, `ColumnReadersTransportFailureTest`.

### Cancellation and close

`ColumnWorker.close()` sets `done`, finishes the exchange, wakes both threads, interrupts the drain, joins the retriever and the drain, and then waits for every decode task that is running. It returns only once the column is quiescent, so the caller can release what the `InputFile` owns (mapped buffers, direct buffers, connections) without a decode task reading freed memory. The joins do not give up on an interrupt; a caller interrupted while closing gets its flag back afterwards.

After close, the retriever pulls no further page, so no chunk it has not reached is fetched beyond prefetches already started; decode tasks not yet started return without decoding; and speculative plan tasks see the iterator closed and plan nothing. A reader closes its workers, returns its held and queued batches to the pool (recycling mode), and then closes its `RowGroupIterator`, which clears its caches and, when it owns them, closes the files. Close is idempotent, and no column's close waits out a timed queue wait, since the interrupt releases each drain at once.

Tests: `ColumnWorkerTest`, `ReaderCloseLatencyTest`, `RowReaderCloseIdempotencyTest`, `IteratorTrackingTest`, `RowGroupIteratorCloseTest`, `S3SelectiveReadJfrIT` (s3).

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

A stable knob that needs a typed contract stays typed on its builder and out of the map, as `batchSize(int)` does on the column-reader builders. The `hardwood.internal.*` system properties (reorder depth, coalescing span, sequential chunk size) are tuning overrides and not part of `ReaderConfig`. The user-facing option reference is [reader.md](../docs/content/reference/reader.md#reader-options).

Tests: `MetadataFilteringOptionTest`, `FixedSizeListFastPathReadTest`.

## Boundaries

- **Planning failure read as end of stream (#1278).** `planNextFile` advances past a file before validating it, so when validation throws, a column whose retriever arrives later finds nothing left to plan and ends cleanly; a consumer that polls that column first can see a short read instead of the error.
- **Read-ahead beyond the throttle (#370).** The throttle counts pages in the reorder buffer only; the batches in the exchange add to the per-column horizon.
- **Files stay open until the reader closes (#100).** A multi-file read releases no file as it moves past it.
- **Single-threaded mode (#1022).** A read always uses the retriever, drain and decode threads, even on a one-thread context.
- **Schema drift across files (#941).** Every file must match the reference schema for the columns a read touches.
- **`BYTE_ARRAY` width estimate (#899).** Batch sizing guesses a fixed width per `BYTE_ARRAY` value instead of reading the recorded unencoded size.
- **Row caps on the column readers (#433).** `ColumnReaderBuilder` and `ColumnReadersBuilder` have no `head`, so no cap reaches their workers.
- **Batches spanning row groups (#1199).** A batch that spans two column chunks keeps one chunk's dictionary for string reuse.
