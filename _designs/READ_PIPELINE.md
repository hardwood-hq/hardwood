# Read pipeline

Describes how a read turns the row groups it has planned into published batches: the shared `RowGroupIterator`, each decoded column's `PageSource` → `ScheduledColumn` → `BatchExchange` chain, the read's `ReadScheduler`, read-ahead and back-pressure, batch sizing, the batch plan, row-group and file transitions, multi-file planning, row limits, error propagation, teardown and `ReaderConfig`.

Related documents:

- [FETCH_PLANNING.md](FETCH_PLANNING.md): which bytes a read requests and how they are fetched (fetch plans, the fetch sequence and its read-ahead, coalescing, page masks)
- [COLUMN_READER.md](COLUMN_READER.md): what a batch holds and how the column reader exposes it
- [ROW_READER.md](ROW_READER.md): how the row reader presents batches as rows
- [VALUE_DECODE.md](VALUE_DECODE.md): how a page's values are read in place
- [NESTED_DECODE.md](NESTED_DECODE.md): how pages become nested batches
- [RECORD_FILTERING.md](RECORD_FILTERING.md): filter evaluation over the published batches
- [THREADING.md](THREADING.md): which thread runs each component's work, and a context of zero threads
- [TESTING.md](../TESTING.md#task-orders): the harness that runs a read's page tasks in seeded random orders
- [docs/content/concepts/concurrency-model.md](../docs/content/concepts/concurrency-model.md): the user-facing model

## Components

Every data reader (`FlatRowReader`, `NestedRowReader`, a `ColumnReaders` group, and a single `ColumnReader`, which is a group of one) runs the same pipeline, for flat and nested schemas and for one file or many. A single-file read is a multi-file read of one file; there are no separate reader classes for either.

| Component | Count | Job |
|---|---|---|
| `RowGroupIterator` | one per data reader | Holds the read's work list of `(file, row group)` pairs as its columns see it, and its lifecycle: its `PlanningChain` plans the work list a file at a time and each work item once, its fetch plans and batches (`BatchPlan`) included, and its `FetchSequence` issues every request of the read, footers included |
| `PageSource` | one per decoded column | Walks the work list with its own cursor and yields that column's `PageInfo`s, draining each row group's `FetchPlan` |
| `ScheduledColumn` | one per decoded column | Places its pages into the cells of the batch plan; its page tasks decode them straight into the batches (`ScheduledFlatColumn`, `ScheduledNestedColumn`) |
| `ReadScheduler` | one per read | Submits the pages of the read's columns to the decode pool in row order, within the read-ahead cap |
| `BatchExchange` | one per decoded column | Bounded hand-off of published batches from the column to the consumer |

The decoded columns are the projection plus any filter-only predicate columns (`ReadProjection.decoded()`, see [RECORD_FILTERING.md](RECORD_FILTERING.md#augmented-projection)). Every decoded column publishes batches cut by the read's one batch plan into its `BatchExchange`.

**Ownership.** `ParquetFileReader` reads the first file's footer at `open`/`openAll` and owns the input files and a `FileMetadataCache` of parsed footers. When that fails, it closes every input file, and the context when the call created it, before the exception propagates, attaching close failures as suppressed. Each data reader it builds gets its own `RowGroupIterator` sharing that cache; the parent tracks the child until the child closes the iterator, and closing a child releases the iterator's caches and its tracking entry but leaves the files to the parent. Closing the parent closes every child still open, then the metadata cache, an owned context and the files, so no child's fetcher or speculative task reads a file after it is closed; it attempts every input file, throws the first close failure and suppresses the later ones under it. The parent's close runs once: a later or concurrent close waits for the exclusive side of the lifecycle lock and returns without closing anything again or rethrowing the first close's failure. A child build holds the shared side of the parent's lifecycle lock and the parent's close the exclusive side: the close waits for the builds under way, each of which, finding the parent closed, closes the child it made and fails, and a build started after the close fails at once. An iterator built outside a `ParquetFileReader` owns its files and closes them.

Tests: `MultiFileRowReaderTest`, `IteratorTrackingTest`, `ParentCloseTest`, `ParquetFileReaderCloseTest`.

Which thread runs each component's work is in [THREADING.md](THREADING.md).

## Scheduled columns

```
PageSource.next() ─→ fetcher ─→ Placer (flat): segments into cells ─→ queue (per column)
                                                                   │
                     ReadScheduler: lowest first row across the read's queues, within bounds,
                     on the thread that queued, ended or published  │ submit
                     page task (context pool): decode into cells ─→ complete ─→ publish in plan order
                                               (nested: decode, measure, place, fill sealed cells)
                                                                   │ tryPublish
                                              consumer ←── readyQueue (BatchExchange)
```

The fetcher takes each `PageInfo` from its `PageSource`. It builds a new `PageDecoder` for each column chunk its pages come from (`PageDecoder.decodesChunk`), since a decoder checks a page against its own chunk's metadata ([VALUE_DECODE.md](VALUE_DECODE.md#page-decode)). A `PageInfo` is one of three kinds:

| Kind | Produced by | Handling |
|---|---|---|
| Data page | `IndexedFetchPlan`, `SequentialFetchPlan` | Read in place by a page task ([VALUE_DECODE.md](VALUE_DECODE.md#page-decode)) |
| Null placeholder | `SequentialFetchPlan`, when inline page statistics prove no row matches | Read by a page task as a page of all-zero definition levels and no value bytes, without decompression (`PageDecoder.nullsInPlace`), keeping the column row-aligned ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md)) |
| Boundary marker | `SkippedColumnFetchPlan` | Placed with no page task ([Filter flags in the pipeline](#filter-flags-in-the-pipeline)) |

**Placement.** A flat column's fetcher places each page, under the column lock, with the column's `Placer`: following the row group's batches from the batch plan, it splits the page's kept records into one `Segment` per batch they fall into, opens a `Cell` at a batch's first record and seals it when the batch holds every record the plan puts in it. A flat page's kept records are known from its metadata (`PageInfo.records()`), so a page is placed before it is decoded. A boundary marker places nothing and closes the batch where the plan closes it. The placed page joins the column's queue, which the fetcher keeps a few pages long.

**Columns decoded through levels.** A column with repetition, or with a layer above its leaf, and every column of a nested row read (`ScheduledNestedColumn`) is placed after its pages are decoded, since only a page's levels say where its records start. Its page task decodes and measures the page, places the measured pages in page order under the column lock, without waiting for a page ahead of it, and fills each cell the placement seals ([NESTED_DECODE.md](NESTED_DECODE.md#cells)). A cell the fetcher seals, placing the end of a row group, of the stream or of its fetch, is filled by a task the column queues for the scheduler like a page, so every fill of such a column runs on the pool. The fetcher queues at most a fixed number of pages not yet placed. Until a queued page is placed, the column's first unpublished row is at most that page's first row. The end of the stream and the fetcher's failure are queued behind the unplaced pages and act once the placement reaches them.

**Row order.** Every page carries the read's row of its first kept record: the plan's rows before its row group (`RowGroupBatches.readRowsBefore`) plus the kept records the column placed before it in the row group. A page of a column with repetition, whose metadata gives no record count, carries the column's last known row instead, a lower bound of its first, so it is never submitted late. Every column computes it from the one plan, so the rows of two columns compare. The scheduler submits the queued page with the lowest row, and waits for a column whose queue is empty and whose next page may start lower, and passes a page only while that page is past the read-ahead cap (below). The pool runs tasks in arrival order, so the tasks of the batch the consumer waits for are queued ahead of every read-ahead task, whatever the number of columns and their page sizes. The exceptions go to the head of the queue: a dictionary page or other CPU work a thread off the pool is blocked on (`HardwoodContextImpl.decode` and `compute`, [VALUE_DECODE.md](VALUE_DECODE.md#decode-buffers)), a column's fetch step, since the read waits for the pages it fetches, and the planning chain while a column or a caller waits for it ([THREADING.md](THREADING.md#invariants)). **Invariant: every page task reaches the pool through `ReadScheduler`.** The pool has no priorities, so the order of submission is the only order it keeps: a column's read-ahead submitted past the scheduler queues ahead of another column's page of the batch the consumer waits for, and that page waits behind it. The cap bounds the read's read-ahead, and the order across columns is set once for the read, so one rule serves a narrow read, whose few columns must keep every decode thread busy, and a wide one.

**Submission** has no thread of its own. It runs on whichever thread changes what may be submitted: a fetcher queuing a page, a page task ending, a column publishing a batch or ending, the consumer publishing a batch that waited for room. One thread submits at a time; a call made while another submits makes that thread look again (`ReadScheduler.submit`), so no change goes unseen and no thread waits to submit. The scheduler keeps the read's columns ordered by the rows it compares (a column's next queued page, the next row of a column with nothing queued, its first unpublished row) and re-reads a column only when the column reports a change, so choosing the next page costs a logarithm of the column count rather than a pass over every column. **Invariant: every change of a column's queue, next row, published rows or end is followed by a call into the scheduler that names the column.** A change made without one is never seen, and a read waiting on it stalls.

**Read-ahead** has three bounds:

| Bound | Counts | Size |
|---|---|---|
| Pages in flight | page tasks submitted and not finished, across the read's columns | a few per decode thread |
| Cap (`ReadScheduler.admitted`) | the rows a page starts past the read's first unpublished row, the slowest column's | fewer than several batch byte budgets, in rows (`BatchSizing.readAheadBudgetRows`) |
| Large tasks (`WorkingSet.admit`) | the buffers of the read's page tasks and dictionary parses above the retention size, beyond its one such task | the context's read-ahead buffer ([VALUE_DECODE.md](VALUE_DECODE.md#page-decode)) |

The scheduler sizes a page's task from its header, which both fetch plans parse as they walk the chunk. A page the third bound does not admit waits, and the pages after it wait with it, until a task of the read ends; it holds the read's row order as a page past the cap does not. A fetch step that reaches a dictionary page the bound does not admit yields until a task of the read ends, as it yields for bytes.

The cap bounds the bytes a read decodes ahead of its consumer. It is counted from the slowest column, so every column reads ahead to the same row, and a fast column does not spend it decoding ahead of a slow one whose batches the consumer still waits for. Within it, the pages a read admits keep the decode threads busy and give each column its next page while the consumer reads the current one; the pages in flight bound how many of them decode at once. The cap is a byte budget converted to rows by what a row of the read costs: a fixed-width column its value width, a repeated one also its levels, each times its fan-out, and a `BYTE_ARRAY` column its chunks' decompressed bytes per value plus a start and an end offset, measured over the first file's row groups. A batch of a PLAIN-encoded binary column copies its values' bytes, and a batch of a dictionary-encoded one refers to its dictionary, while its pages hold mostly indices, so the measure follows what the batches hold. A page starting at most one batch capacity past its column's first unpublished row is always admitted, so where the cap is below one batch, as for a column of long binary values or a batch size set above it, each column reads ahead only its next batch. That page keeps the read live: such a page holds rows of the batch the column publishes next, or, for a column with repetition, the next record start that seals that batch's cell, and the cell completes once these page tasks have run. A column whose cells are all published counts from the row its next page starts at or after, so a filter-only column skipping row groups is not held back.

**Past a waiting page.** The page at the lowest row can be past the cap while a later column's page holds rows of the batch that column publishes next, as a column that has published far ahead of the read, such as a filter-only one or one whose consumer takes its batches separately, does. Every later page is past the cap too, so such a page is admitted only as its column's next batch. While fewer of the read's pages are in flight than the pool has threads, the scheduler looks past the waiting page, in row order and at no more columns than the pool has threads, for the first such page, and submits it; without that step, the column's consumer waits for a batch whose page no submission reaches until the slowest column's consumer moves. The walk ends at a page that a column with nothing queued may yet start below, so no page is submitted ahead of one not yet queued. The page submitted past the waiting one holds read-ahead for the read as a whole: the waiting page is past the cap, and as the lowest queued page, with no column of an empty queue able to start lower, it has every page below it submitted. Submitting only while the read leaves a decode thread free keeps a read whose threads are busy, such as a wide one, on strict row order. **Invariant: a page is submitted past the lowest queued page only while that page is past the cap.**

**Cells.** A flat column's cell takes its batch from the column's exchange (`BatchExchange.newBatch`) and gives it arrays of at least the size the plan gives, on the first page task that reaches it (`Cell.allocate`); a task that reaches it meanwhile parks on the cell's lock until the arrays exist. The allocation stays on the page tasks, which spread the zeroing of a read's batch arrays over the decode threads; a column's fetch steps run one at a time. A segment is written by its page's task alone. Segments of one cell are filled concurrently and write disjoint ranges of its values; a word of the validity bitmap can hold bits of two neighbouring segments, and a segment writes the words at its edges with an atomic OR (`FlatFill`). Concurrent fills of one cell are reached only by chance, and the atomic OR on a shared word is untested. The cell completes exactly once, on the task that fills its last segment or, when its fetcher's placement seals it after its last fill, on a `SealedFillTask` the column queues ahead of its pages at the cell's first row, as a nested column does. A failure in that completion, the column's filter fragment included, fails the column at once, since a failure queued behind a cell that cannot complete is never raised (`ScheduledFlatColumn.completeSealed`); that failure is untested.

**Binary cells.** A binary column's cell has the plan-sized arrays of any flat cell, and its plain values' bytes, which the plan cannot size. Each segment copies its own into chunks it adds to the batch's buffers ([COLUMN_READER.md](COLUMN_READER.md#variable-length)), so a segment is filled like any other, within its page's task, and no fill waits for another's bytes.

**Decoding.** Every page is read in place: its values are written once, from the decompressed page straight into the batch arrays, with no page-sized value array between ([VALUE_DECODE.md](VALUE_DECODE.md#page-decode)). A flat page's task decodes its levels and ids and locates its values (`PageDecoder.decodeFlat`, `decodeFlatBinary`), and each segment's fill reads its rows from the page through the page's value reader (`FlatPageValues`, `BinaryPageValues`). A page that holds a different number of values than the rows its metadata gave its placement fails the read before it fills anything. A page no in-place reader takes is refused before it is decompressed ([VALUE_DECODE.md](VALUE_DECODE.md#encodings-and-readers)).

**Scratch.** A page's levels, ids and decompressed bytes live in scratch whose lifetime matches the longest reader of the page:

| Column | Scratch | Held |
|---|---|---|
| Flat | the task's `DecodeBuffers` | for the page's task |
| Decoded through levels | borrowed from the column's pool | until every cell the page is placed into is filled (`NestedPageTask.unfilledSegments`, `ScheduledNestedColumn.releaseScratch`) |

A page decoded through levels keeps its scratch past its task because the cells it is placed into are filled when they are sealed, on another page's task. A pool keeps a bounded number of returned scratch buffers, and a page that finds the pool empty allocates one. Either kind keeps no array above `DecodeBuffers.RETAINED_BYTES` past its page ([VALUE_DECODE.md](VALUE_DECODE.md#decode-buffers)).

**Publishing.** Each column publishes its completed cells in plan order (`BatchExchange.tryPublish`), without waiting: a cell that finds the ready queue at its limit stays at the head of the column's publish order, and the consumer publishes it on its own thread when it takes a batch (`BatchExchange.setRoomListener`). No pool thread waits on the exchange, and no publish is queued behind read-ahead on the pool. A published cell drops its segments and its batch, which a page spanning several cells would otherwise keep alive. The thread that completes a cell also runs the column's filter fragment on its batch ([RECORD_FILTERING.md](RECORD_FILTERING.md#batch-matchers)): its last page task, or the `SealedFillTask` when the seal comes last. A fragment's calls are serialized, since cells complete on several threads at once. The column lock guards placement, the queue and the publish order and is held for bookkeeping only: no decoding, filling or I/O runs under it.

Tests: `ReadSchedulerTest`, `ReadAheadCapTest`, `HeadOfLineGatingTest`, `DecodeThreadTest`, `PlacerTest`, `BinaryFillChunksTest`, `ConcurrentPlainBinaryFillTest`, `ScheduledBinaryViewsTest`, `ScheduledNestedColumnTest`, `DictionaryFallbackNullsTest`, `BatchPlanTest`, `BatchExchangeTest`, `FilterOnlyColumnSkipTest`, `ShuffledTaskOrderTest`, which runs the page tasks in seeded random orders and counts the out-of-order runs and placements they reach, and the result-comparing suites, which read every fixture through the column readers at small batch capacities.

### When a batch closes

Where every batch of a read closes is decided once for the read, by its `BatchPlan`, and every decoded column closes its batches there. A batch closes:

| Trigger | Applies |
|---|---|
| The batch holds its capacity in rows (flat) or records (nested) | always |
| The next row group comes from another file | always |
| The next row group's always-match flag differs from the batch's | filtered reads only ([RECORD_FILTERING.md](RECORD_FILTERING.md#homogeneous-batches)) |
| Its row group ends | reads whose decoded columns include a `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` or `INT96` leaf (`BatchPlan.endsBatchesAtRowGroupBoundaries`), so a batch draws on one column chunk's dictionary: for the column readers' dictionary ids ([COLUMN_READER.md](COLUMN_READER.md#dictionary-ids)) and the row readers' shared strings ([VALUE_DECODE.md](VALUE_DECODE.md)) |
| The row cap is reached | reads with a cap ([row limits](#row-limits)) |
| End of stream | always; the partial batch is published |

**The plan.** Every input to these rules is known before a page is decoded, counts rows, and is the same for every column: the capacity ([batch sizing](#batch-sizing)), the work items' files and always-match flags, the row cap, and each row group's kept rows, which are the rows of its `RowRanges` when page masks apply and all its rows otherwise. Masks apply to every decoded column of a row group or to none ([FETCH_PLANNING.md](FETCH_PLANNING.md#the-mask-capability-gate)), so every column keeps the same rows. A row group that keeps no row (its row ranges are empty, or its bloom filters or dictionaries drop it) takes no part: it closes no batch and leaves the row cap in place.

The plan is extended a row group at a time, as the planning chain computes the row group's fetch plans, and kept beside them in the work item's state. Row groups are planned in work-list order, so the plan follows the [incremental planning](#incremental-planning) of the work list and a row group's batches depend only on the row groups before it. Each row group is planned exactly once, by the one chain, whether a column waits for it or the chain plans ahead, so every column reads the same batches, and `BatchPlan` fails the read on a row group planned out of order or a second time.

**Consumption.** A column's placement takes a row group's batches (`RowGroupBatches`) from the first page of the row group, which carries them from its `PageSource`. It checks that it placed every row the plan takes from the row group it leaves, seals a cell the plan closes, and takes the file name and flag for the cells that follow. A flat column seals a cell the moment it holds its planned rows. A column with repetition seals it at the next record start, at the next row group or at the end of the stream, since the record's last values can follow on later pages.

**Alignment.** Every column of a read closes its batches at the same rows by construction, since they all follow one plan. A column that holds more or fewer rows of a row group than the plan takes from it fails the read with a `ParquetReadException` ("Batch plan mismatch") instead of publishing a batch out of line with its siblings. The readers poll one batch per payload column per step, and one per filter-only column in steps statistics did not prove ([RECORD_FILTERING.md](RECORD_FILTERING.md#filter-only-column-skip)), and raise `IllegalStateException` when a column has no batch or a batch of another record count. A new boundary rule belongs in `BatchPlan`, and its inputs must be known before decoding and be the same for every column.

The row-group cut costs every column of the read a batch hand-off per row group, which slows a scan over small row groups measurably, so only a read with a binary column, whose values draw on a dictionary per batch, pays it. A batch spans row groups of one file only in a read without a binary column, and **no batch straddles files**. `RecordFilterTally` attributes counts per file from the batch's file name, and both row readers take their current file name (for `FileAwareRowReader` and error context) from the first column's batch. A file is told apart by its position in the read, so a path listed twice in a row starts a new batch.

Tests: `BatchPlanTest`, `PlacerTest`, `RecordFilterEventTest`, `FileNameInExceptionTest`, `FilterOnlyColumnSkipTest`.

### BatchExchange

The exchange holds a ready queue (column → consumer) and, in recycling mode, a free queue (consumer → column):

| Mode | Used by | Batches | Ready-queue limit | Consumer contract |
|---|---|---|---|---|
| Recycling (`BatchExchange.recycling`) | `FlatRowReader`, `NestedRowReader` | A pool of one more holder than the ready queue holds, allocated up front; a column fills new holders past it as far as its read-ahead goes. The flat row reader's free queue keeps every holder returned (`BatchExchange.recyclingEveryHolder`), so that a read reuses the arrays and binary chunks of every holder its read-ahead filled, which it holds until the read ends; the nested row reader's keeps at most three, since a nested cell allocates its arrays per batch | Two batches | Returns each batch (`recycle`) when it moves to the next; a flat batch's arrays are valid until then, a nested batch's for as long as they are referenced ([ROW_READER.md](ROW_READER.md#values-and-nullness)) |
| Detaching (`BatchExchange.detaching`) | `ColumnCursor` (column readers) | A fresh batch per cell | Two full batches' worth of rows | Owns every batch it takes; the arrays are never reused ([column-reader.md](../docs/content/how-to/column-reader.md#retaining-and-handing-off-batch-arrays)) |

A column publishes without waiting (`tryPublish`): in detaching mode an empty queue takes any batch, and a batch that would take the queue past its limit is not queued; in recycling mode a batch is queued while the bounded queue has a free slot. A cell takes the batch it fills with `newBatch`: a returned holder when the free queue has one, whose arrays a flat cell reuses when they are large enough, and whose full binary chunks it fills again ([COLUMN_READER.md](COLUMN_READER.md#variable-length)), a new one from the exchange's factory otherwise. A nested cell allocates its arrays for every batch, on either exchange ([NESTED_DECODE.md](NESTED_DECODE.md#cells)). The consumer runs the column's room listener after each batch it takes, which publishes the batches that waited ([Publishing](#scheduled-columns)).

**Detaching limit.** A batch ends early at a row group or a file ([When a batch closes](#when-a-batch-closes)), so a read over small row groups publishes many short batches. The detaching limit counts rows, so the queue holds what two full batches hold however the batches are cut. A flat batch counts the rows its arrays were allocated for (`Batch.capacity`), since a batch left open into the next row group gets full-size arrays and can still end short, at a file, at a change of the always-match flag in a filtered read, or at the end of the stream; a nested batch's arrays are allocated at the records it holds, and it counts those. A batch of no rows counts as one, so the limit also bounds the number of queued batches.

**Consumer protocol.** `poll()` tries a non-blocking poll first and reads the `finished` flag only after a poll comes back empty, so every batch published before `finish()` is delivered before the end is reported. `finish()` also offers an end-of-stream sentinel, which queues behind every batch and releases a consumer already waiting; it is best effort (the recycling queue, when full, has no room, and then the consumer has batches to take and is not waiting). A `BatchWait` JFR event spans only the timed wait, so its duration is the consumer's stall.

Tests: `BatchExchangeTest`, `BatchExchangeRowLimitTest`.

### Back-pressure chain

```
consumer stops polling
  → ready queue at its limit; completed cells wait at the head of each column's publish order
  → the column's first unpublished row stops; the cap admits no page further ahead
  → queued pages stay unsubmitted; the fetch step returns once its queue is full
  → PageSource.next() is not called: no further planning or fetching for the column
consumer polls
  → its take publishes the waiting cells (room listener); the first unpublished row advances
  → the scheduler submits the pages the cap admits; taking a page off a queue queues the
    column's next fetch step
```

**How far a read runs ahead of its consumer is bounded by the read-ahead cap and the queues, never by the size of the file or the number of row groups or files.** No column data is fetched before a column's fetcher reaches it, apart from the read's next unit and the speculative section requests it starts ahead, which F bounds per read and the context's read-ahead buffer across reads, and the planning reads of the row groups planned ahead ([FETCH_PLANNING.md](FETCH_PLANNING.md#section-requests)); a consumer that stops reading stops the fetching once its read holds F. The per-column horizon is the cap, counted from the slowest column's first unpublished row, plus the span of the pages in flight and queued, plus the batches the exchange holds.

Tests: `MultiFilePlanningTest`, `S3SelectiveReadJfrIT` (s3).

## Batch sizing

The batch capacity is fixed for a read and resolved once, at build time, by `ParquetFileReader.resolveBatchSize`, for every reader: the row readers through `createRowReader`, the column readers through `buildColumnReaders`. The columns, the exchanges and `BatchExchange.allocateArray` receive a concrete positive size.

- **Explicit.** `ColumnReaderBuilder.batchSize(int)` and `ColumnReadersBuilder.batchSize(int)` take a positive record count, used verbatim. The row readers have no explicit size.
- **Default.** `BatchSizing.computeOptimalBatchSize` divides a per-batch byte budget, sized so a batch amortises the per-batch overhead, by the bytes one row costs across the decoded columns, predicate columns included, since they allocate batch arrays too. A column costs its value width times its list fan-out, plus two `int` levels per value for a repeated column, whose nested batch carries both level arrays. Fan-out is the column's leaf value count over the row count, summed over row-group metadata (`BatchSizing.valuesPerRow`); a flat column's is 1. The per-value work of a batch is therefore roughly constant across fan-outs, and no row floor applies.
- **Reference arrays.** `ColumnReader.getStrings()` and `getBinaries()` build a reference array of a binary column's values per batch. Under G1, an object larger than half a heap region is humongous, allocated in whole old-generation regions, and allocating one per batch makes garbage collection dominate the read. `BatchSizing.MAX_BATCH` is the longest reference array that stays an ordinary object at G1's smallest region (1 MB) with compressed references, and every `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` and `INT96` column's values per batch, its rows times its fan-out, are capped at it. A primitive column's values are bounded by the byte budget alone, so a wide list of numbers keeps its batch. The cap uses the average fan-out, so a batch of a repeated binary column whose records run longer than average can still exceed it.
- **Clamps.** The default is at least 1 and at most `BatchSizing.MAX_BATCH`, which other components pre-size around (`FlatRowReader`'s all-present validity). It is also at most the rows the read can produce: the total row count of the file's row groups after the `RowGroupPredicate`, or `BatchSizing.ROWS_UNKNOWN` (no clamp) for a multi-file read. The bound must be an upper bound; pruning, a row cap and a skip only remove rows, so the clamp never shortens a batch the read could have filled.
- **First file only.** Fan-out and the row bound come from the first file's row groups, the one footer `openAll` reads. Sizing from later files would plan them before the first row (see [the planning invariant](#incremental-planning)).

The row path and the column path produce the same size for the same decoded projection. Independently built readers with different decoded columns size differently ([RECORD_FILTERING.md](RECORD_FILTERING.md#column-readers)).

Tests: `BatchSizingTest`, `WideListBatchBoundTest`.

## Row groups, files and multi-file planning

### The work list

The read's work list is an ordered list of `WorkItem`s, one per surviving row group: the file, the row group and its index in the file, the file's schema and column ordinals, the item's index in the list, the rows planned before it, the row group's always-match flag and the per-leaf filter decisions its dictionaries sharpen later. The planning chain (`PlanningChain`) plans files into it and holds one state per work item (its `Planning`): what its planning decided, the planning section requests it holds, the decoded columns that hold it, and whether the read has entered it and reported its drop. `RowGroupIterator` is the face the columns and callers see; the schema checks (`SchemaCompatibility`), the pruning by statistics, Bloom filters and dictionaries (`RowGroupPruning`) and the building of each row group's fetch plans (`FetchPlanBuilder`) are classes of their own, which the chain runs.

Each `PageSource` walks the list with an integer cursor: it asks whether the work item at `i` is planned (`isPlanned`), which asks the chain to plan it, yields until it is (`whenPlanned`), and then takes it (`plannedWorkItem`), whose work item is `null` when the read is done, and drains its own column's plan. When the plan is exhausted it calls `releaseWorkItem`, and the last decoded column to release a work item drops its state, its section requests and its planning section requests: the work list holds only the work items the read has not passed, so a read of many files holds the state of the files it is reading. Page slices and page tasks in flight keep their bytes alive until they finish.

### Incremental planning

Listing a file (`PlanningChain.planNextFile`) plans exactly one file and appends its surviving row groups: it loads the footer through the `FileMetadataCache`, yielding while the footer is being read, validates the file's schema against the reference schema (the first file's), resolves the file's own leaf ordinals for the touched columns, checks that the chunk at each touched ordinal carries that leaf's path, prunes row groups on their statistics ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md)), applies the physical skip and the unfiltered row budget, and lets the read's fetch sequence start the footer requests the planned file brings within reach: the next file's, or, for a multi-file read of files not all in memory and without a row limit, those of the files after it ([FETCH_PLANNING.md](FETCH_PLANNING.md#opening-files-ahead)), which the planning chain plans in order as far as its depth allows ([FETCH_PLANNING.md](FETCH_PLANNING.md#planning-ahead)). Once every file is planned or the budget is spent, the work list is complete. A row group's bloom filters and dictionaries are read when the chain takes its work item through them. The row budget, rows planned, skip residue and first-row-group skip are the chain's fields, since each listing resumes where the last one stopped.

The cursor is an integer rather than an `Iterator` because the list grows behind it: an iterator would force the whole plan at construction, or fail on concurrent modification once planning appends.

**Invariant: reader structure is never derived from the contents of individual row groups in individual files.** A decision the reader takes once, at construction, from facts about row groups would force every file to be planned before the first row, returning time to first row to O(files). Facts about a row group reach the pipeline on its work item:

| Question | Answered |
|---|---|
| Does statistics prove every row matches? | Per row group: `WorkItem.filterAlwaysMatches` → batch plan → the page's `RowGroupBatches` → cell → `Batch.filterAlwaysMatches` |
| Where do batches close, and does the row cap still hold under a filter? | Per row group, by the batch plan, as the row group's fetch plans are computed ([When a batch closes](#when-a-batch-closes), [row limits](#row-limits)) |
| Batch-matcher or per-row filter evaluation? | From the predicate's shape alone (`BatchFilterCompiler.tryCompile`) |
| Batch size | From the first file ([batch sizing](#batch-sizing)) |
| Does the read have any work? | `hasLiveWorkItem()`, asked by every reader before it starts its columns: walks the work list until the first row group its bloom filters and dictionaries do not drop, so it plans and probes only as far as that row group, and leaves that row group's page index to the fetchers |

Two questions concern the whole read and plan only as far as they must. `firstRowGroupSkip()` answers `0` without planning when there is no physical skip, and otherwise plans through the first work item, where the value is written. `canFastSkipAllRowGroups()`, asked by the tail-read path, needs every row group; that path is single-file, so a full plan is one footer. `getWorkItems()` plans the whole read and serves tests only.

**Concurrency.** Only the planning chain lists files and plans work items, under its claim, so no two threads plan. A column or caller that needs a work item past the planned end asks the chain and waits for its cursor; the chain writes a work item's state before it publishes the work item, and the columns read it after. The holders of a work item are counted down on every fetcher outside any lock. `getWorkItems` returns a copy.

**Invariant: a planning failure is sticky.** Once planning a file or a work item has failed, the chain keeps the failure, with the first work item it fails, and every later request for that work item or any after it rethrows it, as the type it was raised as: a column that found nothing left to plan would otherwise end its stream cleanly, and a consumer polling that column first would see a short read. Nothing after the failure is planned again: planning a file advances the row budget, the rows to skip and the next file, and a row group whose planning read failed cannot be read either way. A work item before the failure is still handed out: the column whose next unit reached the failure can be ahead of the others, and a column failed short of it would return fewer rows than they do.

**Error timing.** The first file's footer is read by `openAll`, so its errors surface there. A later file's I/O errors and its disagreement with the reference schema are raised when the read reaches the file, from the reading loop, and before any row of it is returned; a mismatched file is never read as data. The next file is planned when the previous file's last row group is entered, but a failure there is not raised until a column asks for the failed file's first work item, so every row of the files before it is returned first, and the failure is placed at the file it belongs to rather than at the row group the read was in.

**Reading every row opens every file.** Planning is deferred, not avoided. A read that stops early (a row budget, a row cap, a consumer that closes) leaves the files past its stop unopened, up to the back-pressure horizon. A read whose data totals less than one batch walks every file to fill the first batch.

The user-facing contract (reference schema, when a mismatch surfaces) is in [multi-file.md](../docs/content/how-to/multi-file.md).

Tests: `MultiFilePlanningTest`, `SchemaCompatibilityTest`, `CrossFileColumnOrderTest`, `MultiFileRowReaderTest`, `RowGroupIteratorPlanningFailureTest`, `LaterFileFailureTest`.

### Row-group and file transitions

Within a file, a batch continues from one row group into the next, except where the always-match flag changes on a filtered read or the read ends its batches at row-group boundaries ([When a batch closes](#when-a-batch-closes)).

The first column to enter a row group makes the next work item the read's next unit, once per row group ([FETCH_PLANNING.md](FETCH_PLANNING.md#planning-ahead)): the next unit's first section request starts at once, without F and uncharged, the column starts its window's Bloom filters or page index, and the planning chain plans it, the next file included when the row group is its file's last, and further ahead while F has room. A planning failure there does not fail the current row group: the chain keeps it, and each column that asks for the next work item takes it. Planning ahead plans nothing once the iterator is closed. Combined with the next file's footer request, started once the read reached the file before it, or the footer read in place for a file in memory, the next file's metadata and first bytes are in flight by the time the current file is exhausted, and the transition does not stall the consumer. A read that reads footers ahead has the footers and data of several files in flight at once, so a run of files too small to cover one round trip each does not put one round trip per file on the read ([FETCH_PLANNING.md](FETCH_PLANNING.md#opening-files-ahead)). Local and remote files differ only in what a fetch costs ([FETCH_PLANNING.md](FETCH_PLANNING.md)).

### Filter flags in the pipeline

The always-match decision travels from the work item to the column in the row group's batches (`RowGroupBatches`), which every page of the row group carries, and the placement stamps it on each cell it opens beside the file name. A filter-only column in an always-match row group gets a `SkippedColumnFetchPlan`: its boundary marker is placed with no page task, and the column holds none of the row group's section requests. On the marker the placement seals the cell the plan closes before the row group, places nothing, and ends the read where the row cap ends it in the row group. The consumer-side rules are in [RECORD_FILTERING.md](RECORD_FILTERING.md#filter-only-column-skip).

## Row limits, errors and cancellation

### Row limits

`maxRows` (from `head(n)`, and `n + head` for a filtered `skip(n)`, see [ROW_READER.md](ROW_READER.md)) reaches the iterator and the batch plan of a row reader. For an unfiltered `skip`, the batch plan gets `head` plus the skip's residue within the first kept row group, which the reader discards. It is enforced at four levels:

| Level | Where | Unfiltered read | Filtered read |
|---|---|---|---|
| Planning | `planNextFile` row budget | Stops planning row groups and files once the budget is covered | No effect: every row group stays available |
| Fetch | Per-row-group remainder of the budget ([FETCH_PLANNING.md](FETCH_PLANNING.md)) | Truncates the last needed row group's pages where their row counts are known without decompressing: from the OffsetIndex, or from the value count of a flat column. A nested column without an OffsetIndex keeps its pages, and its fetcher reads them until its placement reaches the cap | No truncation |
| Batch plan | `BatchPlan` | Ends the last batch at the cap; every column publishes it and finishes | Holds the cap while every row group with kept rows is an always-match one; drops it for the rest of the read at the first undecided one |
| Reader | Match counting | — | Counts matches against the cap |

A page is the smallest decode unit: a page that straddles the cap is decoded and filled up to the cap. The column readers carry no row cap (#433).

Tests: `BatchPlanTest`, `PlacerTest`, `AlwaysMatchingRowGroupTest`, `MultiFilePlanningTest`, `S3SelectiveReadJfrIT` (s3).

### Error propagation

A failure on any of a column's threads goes through `ScheduledColumn.signalError`, which sets `done`, ends the exchange with the failure, queues a fetch step, which ends the fetch, and lets the scheduler look again; a page task that has not started returns without decoding once `done` is set. The exchange raises the first failure it receives, since that is the one that ended the read; each later failure of the same column is attached to it as a suppressed exception, so a later `Error` such as an `OutOfMemoryError`, whose rethrow on a page task's thread nothing observes, still reaches the caller.

Each failure is placed before it leaves the column (`ScheduledColumn.placeFailure`): the column path is the column's own, and the file, row group and page come from what the failing thread holds (a fetch step from its `PageSource`, a page task from its page). A fill that runs on another page's task, a nested cell's, raises a segment's failure with the segment's own page (`SegmentFillException`), which `ScheduledColumn.pageFailed` places instead. A runtime failure is classified as a read failure (`ExceptionContext.asReadFailure`, [EXCEPTION_MODEL.md](EXCEPTION_MODEL.md#crossing-a-thread-boundary)); an `IOException` is restated as a checked `IOException` carrying the place, unless its message already names a file; an `Error` is recorded unplaced and rethrown on its thread. `BatchExchange.checkError` rethrows each as it stands, so the reader's signature declares `IOException` and nothing between the column and the reader wraps. The exception types a caller sees are in [error-handling.md](../docs/content/reference/error-handling.md).

Where a failure ends the stream depends on where it was met:

| Failure | Ends the stream |
|---|---|
| A page task's, or one sizing or filling a cell | at once |
| A fetch step's, between two work items, where only a planning failure is raised | after every cell before the failure is published; the open cell is sealed, or abandoned when the column holds fewer rows of its row group than the plan takes |
| A fetch step's, inside a work item | after every cell before the open one is published; the open cell is abandoned |
| One while submitting | at once, for every column of the read |
| The decode pool's refusal of a page task or a fetch step, which a closed `HardwoodContext` gives | at once, as an `IllegalStateException` whose cause is the refusal |

A page whose placement failed part way is still decoded into the cells it was placed into, so the cells before the failure complete. A failure between work items is met by every column at the same row, the end of the files planned before it, so the partial batches stay aligned; a column short of the plan's rows is not at that row, and its partial batch is abandoned as inside a work item, with the shortfall attached to the failure as a suppressed "Batch plan mismatch". A failure inside a work item is met by one column at a row the others read past; its partial batch would be shorter than theirs, and a reader taking the columns in lockstep would report the mismatch instead of the failure. The context's close lets the tasks already queued run, so no column waits for a task that never runs.

The consumer receives every batch published before the failure, then the failure. A reader that finds any column's stream ended checks every exchange for an error before reporting the end, so a failure on a column other than the first, a filter-only column included, is not reported as a short read.

Tests: `ReadFailureTypingTest`, `ReadFailurePositionTest`, `ColumnReadersTransportFailureTest`, `ContextClosedMidReadTest`, `PlacerTest`, `BatchPlanTest`, `BatchExchangeTest`.

### Cancellation and close

`ScheduledColumn.close()` first stops every column of the read (`ReadScheduler.stop()`): for each, it sets `done` under the column lock, so no publish runs after it, finishes the exchange and queues a fetch step, which ends the fetch. It then waits for every fetch step and page task submitted for the read's columns, which the read counts alike, once each (`ReadTasks`); a step that yielded on a read stays counted until the read completes and the step it queues has ended the fetch, so the close also waits for that read; one that yielded on a task's end stays counted until a task of the read ends, which every running task does. A reader closes its columns one after another; the first close stops every column and waits for the read, and the later ones find it stopped and quiescent, so the columns wind down together and the reader's close costs the slowest column's wait, not the sum of them: a column left running while an earlier one is awaited would go on fetching, submitting and decoding pages. A task is counted before its submission reads `done`, so a submission racing the close is either refused or waited for. A fetch step inside a fetch finishes that fetch before it returns. The close returns only once the read is quiescent, so the caller can release what the `InputFile` owns (mapped buffers, direct buffers, connections) without a page task reading freed memory. It interrupts no thread. The wait does not give up on an interrupt; a caller interrupted while closing gets its flag back afterwards.

After close, the fetch steps pull no further page, so no section request they have not reached is fetched beyond those already started; page tasks not yet started return without decoding; and the planning chain plans nothing more. A reader closes its columns, returns its held and queued batches to the pool (recycling mode), and then closes its `RowGroupIterator`. The iterator closes its fetch sequence, which starts no further section request and returns the section requests' charges, stops the planning chain, whose footer read-ahead returns the footers', and waits for a step of it that runs, discards the planning section requests, and waits for the reads the read started, its footer reads included (one that had not started returns without doing its work); the reader closes the files after it. Its caller has stopped the columns first, so no fetch step starts a task during the wait. A file is therefore never closed under a read in flight, whichever reader closes it. Close is idempotent, and a close on another thread while one runs, such as the parent reader's, waits for it to finish ([THREADING.md](THREADING.md#invariants)).

Tests: `ScheduledColumnCloseTest`, `ReaderCloseMidReadTest`, `RowReaderCloseIdempotencyTest`, `IteratorTrackingTest`, `ParentCloseTest`, `ParquetFileReaderCloseTest`, `PrefetchTasksTest`, `S3SelectiveReadJfrIT` (s3).

## Configuration (`ReaderConfig`)

Read-time behaviour lives on `ReaderConfig`, runtime resources on `HardwoodContext`:

| Object | Nature | Lifetime | Holds |
|---|---|---|---|
| `HardwoodContext` | Stateful, `AutoCloseable`, shared | Spans many reads | Decode pool, libdeflate pool, decompressor factory |
| `ReaderConfig` | Immutable value, no lifecycle | One `open`/`openAll` | Behaviour options |

Sizing the decode pool is a resource concern and stays on the context. A behaviour option on the context would tie it to a thread pool, so reading two files with different settings would need two pools; one context backs reads with different configs.

`ReaderConfig` is a string-keyed option bag (`Builder.option(String, String)`) with no per-option API. The recognised keys, their defaults and their resolution are private to `ParquetFileReader`, which reads the map once at `open`/`openAll`, resolves each key to a plain value and threads it to the columns. An option can therefore be retired without changing a public type: its key leaves the recognised set, and callers passing it keep compiling. An unrecognised key is ignored and logged at `WARNING` when the reader opens, so a mistyped key does not silently take the default. Untested.

| Key | Default | Kind | Gates |
|---|---|---|---|
| `hardwood.fixed-list-fast-path` | `false` (opt-in) | Transitional: the default flips once the path is trusted, then the key is retired | The fixed-size-list decode fast path ([NESTED_DECODE.md](NESTED_DECODE.md)) |
| `hardwood.metadata-filtering` | `true` | Permanent escape hatch for files with unreliable metadata | Every metadata-derived filter decision ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md)) |

A stable knob that needs a typed contract stays typed on its builder and out of the map, as `batchSize(int)` does on the column-reader builders. The `hardwood.internal.*` system properties (`pagesPerThread`, the pages in flight per decode thread; `maxOutstanding`, a nested column's unplaced pages; `maxCoalescedBytes`, the largest request, S; `fetchAheadBytes`, the speculative data bytes one read holds, F; `indexWindowBytes`, the page-index window budget; `maxFilesAhead`, the files a multi-file read opens ahead; `maxWorkingSetBytes` and `readAheadBytes`, the working-set limit and the read-ahead buffer ([VALUE_DECODE.md](VALUE_DECODE.md#page-decode)); `zeroThreads`, which makes every context one of zero threads so the test suites can run on them; and `batchSize`, which replaces the auto batch size so the test suites can run at small batch capacities) are overrides for tuning and testing and not part of `ReaderConfig`. The user-facing option reference is [reader.md](../docs/content/reference/reader.md#reader-options).

Tests: `MetadataFilteringOptionTest`, `FixedSizeListFastPathReadTest`.

## Boundaries

- **Read-ahead and the consumer's pace (#1577).** The cap is fixed and ignores the consumer's pace. A consumer-bound read fills it, and its decoded batches wait long enough to survive a young collection and be promoted. On a wide schema the cap, converted to rows, can end inside the current row group, so the next row group's pages are not read ahead and the consumer waits at each row-group boundary.
- **Files stay open until the reader closes (#100).** A multi-file read releases no file as it moves past it.
- **Schema drift across files (#941).** Every file must match the reference schema for the columns a read touches.
- **`BYTE_ARRAY` width estimate (#899).** Batch sizing guesses a fixed width per `BYTE_ARRAY` value instead of reading the recorded unencoded size.
- **Row caps on the column readers (#433).** `ColumnReaderBuilder` and `ColumnReadersBuilder` have no `head`, so no cap reaches their columns.
