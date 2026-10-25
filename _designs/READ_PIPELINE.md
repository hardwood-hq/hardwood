# Read pipeline

Describes how a read turns the row groups it plans into published batches: the read's `RowGroupIterator`, its one lock and the poll that decides what runs next, the jobs it runs, the decoded columns that place, decode and publish pages, read-ahead and back-pressure, batch sizing, the batch plan, row-group and file transitions, multi-file planning, row limits, error propagation, close and `ReaderConfig`.

Related documents:

- [FETCH_PLANNING.md](FETCH_PLANNING.md): which bytes a read requests and when its requests start (fetch plans, the read's requests and their read-ahead, coalescing, page masks)
- [COLUMN_READER.md](COLUMN_READER.md): what a batch holds and how the column reader exposes it
- [ROW_READER.md](ROW_READER.md): how the row reader presents batches as rows
- [VALUE_DECODE.md](VALUE_DECODE.md): how a page's values are read in place
- [NESTED_DECODE.md](NESTED_DECODE.md): how pages become nested batches
- [RECORD_FILTERING.md](RECORD_FILTERING.md): filter evaluation over the published batches
- [THREADING.md](THREADING.md): which thread runs each of the read's jobs, and a context of zero threads
- [TESTING.md](../TESTING.md#task-orders): the harness that runs a read's jobs in seeded random orders
- [docs/content/concepts/concurrency-model.md](../docs/content/concepts/concurrency-model.md): the user-facing model

## Components

Every data reader (`FlatRowReader`, `NestedRowReader`, a `ColumnReaders` group, and a single `ColumnReader`, which is a group of one) runs the same read, for flat and nested schemas and for one file or many. A single-file read is a multi-file read of one file; there are no separate reader classes for either.

| Component | Count | Job |
|---|---|---|
| `RowGroupIterator` (`internal.reader.engine`) | one per data reader | Holds every piece of the read's state under one lock: the work list, its planning, its requests and footers, each column's walk, the pages waiting for their decode, and its lifecycle. Its poll decides what runs next and emits it as jobs |
| `ReadPlanning`, `Planner` | one per read | Lists each file's surviving row groups into the work list and takes each work item through its stages (Bloom filters and dictionaries, page index and page filter, mask probe, fetch plans and batches); claims each footer from the reader's `FileMetadataCache` |
| `DecodedColumn` (`FlatColumn`, `NestedColumn`) | one per decoded column | Places its pages into the cells of the batch plan, decodes and fills them on jobs, and publishes its completed cells in plan order (`CellPublisher`) |
| `BatchExchange` | one per decoded column | Bounded hand-off of published batches from the column to the consumer |

The decoded columns are the projection plus any filter-only predicate columns (`ReadProjection.decoded()`, see [RECORD_FILTERING.md](RECORD_FILTERING.md#augmented-projection)). Every decoded column publishes batches cut by the read's one batch plan into its `BatchExchange`.

**Ownership.** `ParquetFileReader` reads the first file's footer at `open`/`openAll` and owns the input files and a `FileMetadataCache` of parsed footers. When that fails, it closes every input file, and the context when the call created it, before the exception propagates, attaching close failures as suppressed. Each data reader it builds gets its own `RowGroupIterator` sharing that cache; the parent tracks the child until the child closes the iterator, and closing a child releases the iterator's state and its tracking entry but leaves the files to the parent. Closing the parent closes every child still open, then the metadata cache, an owned context and the files, so no job of a child's read reads a file after it is closed; it attempts every input file, throws the first close failure and suppresses the later ones under it. The parent's close runs once: a later or concurrent close waits for the exclusive side of the lifecycle lock and returns without closing anything again or rethrowing the first close's failure. A child build holds the shared side of the parent's lifecycle lock and the parent's close the exclusive side: the close waits for the builds under way, each of which, finding the parent closed, closes the child it made and fails, and a build started after the close fails at once.

Tests: `MultiFileRowReaderTest`, `IteratorTrackingTest`, `ParentCloseTest`, `ParquetFileReaderCloseTest`.

## One decision point

A read holds its state in its `RowGroupIterator`, under one `ReentrantLock`, and one method, the poll, decides what happens next. The work leaves the lock as jobs, each of which runs outside it and returns its result; the result is applied under the lock and followed by a poll, and what the poll emits is dispatched once the lock is let go of. Which thread runs each job is in [THREADING.md](THREADING.md#thread-inventory).

| Job | Does | Runs on |
|---|---|---|
| Walk advance | Advances one column's walk over its current work item's fetch plan, up to the pages the column has room for, until the walk ends or stops on bytes not present; parses a chunk's dictionary page in place ([FETCH_PLANNING.md](FETCH_PLANNING.md#fetch-plans)) | the read's executor, at the head |
| Planning | Lists one file, takes one work item through one stage, or parses one footer | the read's executor, at the tail |
| Decode | Decodes one page: into its cells for a flat column, levels and ids into scratch, measured, for a column decoded through levels; or completes or fills the cells a placement sealed | the read's executor, at the tail |
| Read | Reads one request's bytes, or one file's footer as far as I/O goes ([FETCH_PLANNING.md](FETCH_PLANNING.md#requests)) | the context's I/O executor, a virtual thread each |

Each poll, on a read whose columns have started, takes up the files the read has reached (the footers it starts and the files it drops), plans, runs the walks that can go on, starts requests, and starts decodes. Before the columns start, it answers the question a reader asks of the read (see [the work list](#incremental-planning)) the same way. Placement, the cells' bookkeeping and publishing run under the lock, as part of applying a result; decoding, filling and I/O never do.

**Results.** On a read of several decode threads, a job's thread that finds the lock held queues its result and moves on, and the thread that holds the lock applies every queued result, and polls again, before it lets go of it, so a queued result waits for no later acquisition. On a read of one decode thread, a job's thread waits for the lock: no other job thread contends with it. A callback from outside the read (the consumer making room in an exchange, a release of the context's read-ahead buffer, another read's footer load completing, a need met) is a result like a job's. Bytes the read returns to the context's read-ahead buffer under the lock are returned once it lets go of it, since the buffer runs its waiters, other reads' among them, on the releasing thread.

**Counting.** The read counts every job from its emission until its result is applied and what that result's poll emitted is dispatched (`ReadTasks`). The close waits for the count to reach zero, and a poll on a context of zero threads that finds no batch and the count at zero fails ([THREADING.md](THREADING.md#a-context-of-zero-threads)).

**Stall check.** After each poll, a read whose columns have not all ended, with no job in flight and none emitted, waiting for nothing outside it (room in the read-ahead buffer, a task admission, another read's footer load, a need other than one of its own requests, a consumer making room), fails with an `IllegalStateException` that names its state ("Read stalled: …"). Only a rule of the read that misses a case brings it about. Untested.

Tests: `ReadYieldTest`, `PoolWaitTest`, `ShuffledTaskOrderTest`, `ZeroThreadContextTest`.

## Decoded columns

```
walk advance ─→ pages ─→ applied: placed into cells (flat) ─→ ready pages, in row order across columns
                                                                     │ poll: lowest row first, within the cap,
                                                                     │       the pages in flight and admission
                         decode job: decode into cells (flat) / decode and measure (nested)
                                                                     │ applied: cells completed, pages placed (nested),
                                                                     │          sealed cells to a job
                                              publish in plan order ─→ BatchExchange ─→ consumer
```

A column builds a new `PageDecoder` for each column chunk its pages come from (`PageDecoder.decodesChunk`), since a decoder checks a page against its own chunk's metadata ([VALUE_DECODE.md](VALUE_DECODE.md#page-decode)). A `PageInfo` is one of three kinds:

| Kind | Produced by | Handling |
|---|---|---|
| Data page | `IndexedFetchPlan`, `SequentialFetchPlan` | Read in place by a decode job ([VALUE_DECODE.md](VALUE_DECODE.md#page-decode)) |
| Null placeholder | `SequentialFetchPlan`, when inline page statistics prove no row matches | Read by a decode job as a page of all-zero definition levels and no value bytes, without decompression (`PageDecoder.nullsInPlace`), keeping the column row-aligned ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md)) |
| Boundary marker | `SkippedColumnFetchPlan` | Placed with no decode ([Filter flags](#filter-flags)) |

**Placement.** A flat column places each page as the walk's result is applied, with the column's `Placer`: following the row group's batches from the batch plan, it splits the page's kept records into one `Segment` per batch they fall into, opens a `Cell` at a batch's first record and seals it when the batch holds every record the plan puts in it. A flat page's kept records are known from its metadata (`PageInfo.records()`), so a page is placed before it is decoded. A boundary marker places nothing and closes the batch where the plan closes it.

**Columns decoded through levels.** A column with repetition, or with a layer above its leaf, and every column of a nested row read (`NestedColumn`) is placed after its pages are decoded, since only a page's levels say where its records start. Its decode job decodes and measures the page; as the results are applied, the measured pages at the head of the column's unplaced pages are placed, in page order, without waiting for a page ahead of it beyond that order, and the cells the placement seals are filled on a job ([NESTED_DECODE.md](NESTED_DECODE.md#cells)). A cell sealed by the end of a row group, of the read or of the column's walk is filled on a job too, so every fill of such a column runs on the read's executor. Until a page is placed, the column's first unpublished row is at most that page's first row. The column's end and its walk's failure are queued behind the unplaced pages and act once the placement reaches them.

**Row order.** Every page carries the read's row of its first kept record: the plan's rows before its row group (`RowGroupBatches.readRowsBefore`) plus the kept records the column placed before it in the row group. A page of a column with repetition, whose metadata gives no record count, carries the column's last known row instead, a lower bound of its first, so it is never ordered late. Every column computes it from the one plan, so the rows of two columns compare. The read keeps its pages waiting for a decode in one order across its columns, by row, and the poll starts decodes from the lowest row. The executor runs its tail in arrival order, so the decodes of the batch the consumer waits for are queued ahead of every read-ahead decode, whatever the number of columns and their page sizes; a walk advance, which the read waits for, goes to the head. **Invariant: every decode reaches the executor from the poll, in row order across the pages the read's columns have taken.** A column whose walk takes a page later can add a lower row behind decodes already started. The pool has no priorities, so the order of submission is the only order it keeps. The poll stops at the first page past the cap (below) that also starts more than one batch past its own column's first unpublished row; no page behind it starts in that poll.

**Read-ahead** has four bounds:

| Bound | Counts | Size |
|---|---|---|
| Pages in flight | decodes started and not applied, across the read's columns | a small multiple of the decode threads |
| Pages held | per column, the pages taken and not decoded (flat) or not placed (decoded through levels) | a few; a column's walk does not advance past them |
| Cap | the rows a page starts past the read's first unpublished row, the slowest column's | several batch byte budgets, in rows (`BatchSizing.readAheadBudgetRows`) |
| Large tasks (`WorkingSet.admit`) | the buffers of the read's decodes and dictionary parses above the retention size, beyond its one such task | the context's read-ahead buffer ([VALUE_DECODE.md](VALUE_DECODE.md#page-decode)) |

A decode is sized from the page's header, which both fetch plans parse as they walk the chunk. A page the fourth bound does not admit holds the pages after it until a task of the read ends or the buffer releases room, either of which runs the poll again. A decode's admission is released on its job, outside the read's lock, since the release runs the waiters of every read on the context. A walk that reaches a dictionary page the fourth bound does not admit stops until a task of the read ends, as it stops for bytes.

The cap bounds the bytes a read decodes ahead of its consumer. It is counted from the slowest column, so every column reads ahead to the same row, and a fast column does not spend it decoding ahead of a slow one whose batches the consumer still waits for. A column's walk stops once its next row lies more than the cap plus one batch past the read's first unpublished row, and goes on once that row moves. Within the cap, the pages a read admits keep the decode threads busy and give each column its next page while the consumer reads the current one; the pages in flight bound how many of them decode at once. The cap is a byte budget converted to rows by what a row of the read costs: a fixed-width column its value width, a repeated one also its levels, each times its fan-out, and a `BYTE_ARRAY` column its chunks' decompressed bytes per value plus a start and an end offset, measured over the first file's row groups. A batch of a PLAIN-encoded binary column copies its values' bytes, and a batch of a dictionary-encoded one refers to its dictionary, while its pages hold mostly indices, so the measure follows what the batches hold. A page starting at most one batch capacity past its column's first unpublished row decodes past the cap, so where the cap is below one batch, as for a column of long binary values or a batch size set above it, each column reads ahead only its next batch. That page keeps the read live: such a page holds rows of the batch the column publishes next, or, for a column with repetition, the next record start that seals that batch's cell. A column that has ended takes no part in the read's first unpublished row.

**Cells.** A flat column's cell takes its batch from the column's exchange (`BatchExchange.newBatch`) and gives it arrays of at least the size the plan gives, on the first decode job that reaches it (`Cell.allocate`); a job that reaches it meanwhile parks on the cell's lock until the arrays exist. The allocation stays on the decode jobs, which spread the zeroing of a read's batch arrays over the decode threads. A segment is written by its page's decode alone. Segments of one cell are filled concurrently and write disjoint ranges of its values; a word of the validity bitmap can hold bits of two neighbouring segments, and a segment writes the words at its edges with an atomic OR (`FlatFill`). Concurrent fills of one cell are reached only by chance, and the atomic OR on a shared word is untested. The cell completes exactly once: on the decode that fills its last segment or, when its placement seals it after its last fill, on a job the read emits for the sealed cells. A failure in that completion, the column's filter fragment included, ends the column at once; that failure is untested.

**Binary cells.** A binary column's cell has the plan-sized arrays of any flat cell, and its plain values' bytes, which the plan cannot size. Each segment copies its own into chunks it adds to the batch's buffers ([COLUMN_READER.md](COLUMN_READER.md#variable-length)), so a segment is filled like any other, within its page's decode, and no fill waits for another's bytes.

**Decoding.** Every page is read in place: its values are written once, from the decompressed page straight into the batch arrays, with no page-sized value array between ([VALUE_DECODE.md](VALUE_DECODE.md#page-decode)). A flat page's decode reads its levels and ids and locates its values (`PageDecoder.decodeFlat`, `decodeFlatBinary`), and each segment's fill reads its rows from the page through the page's value reader (`FlatPageValues`, `BinaryPageValues`). A page that holds a different number of values than the rows its metadata gave its placement fails the column before it fills anything. A page no in-place reader takes is refused before it is decompressed ([VALUE_DECODE.md](VALUE_DECODE.md#encodings-and-readers)).

**Scratch.** A page's levels, ids and decompressed bytes live in scratch whose lifetime matches the longest reader of the page:

| Column | Scratch | Held |
|---|---|---|
| Flat | the job's `DecodeBuffers` | for the page's decode |
| Decoded through levels | borrowed from the column's pool | until every cell the page is placed into is filled (`NestedColumn`) |

A page decoded through levels keeps its scratch past its decode because the cells it is placed into are filled when they are sealed, on a later job. A pool keeps a bounded number of returned scratch buffers, and a page that finds the pool empty allocates one. Either kind keeps no array above `DecodeBuffers.RETAINED_BYTES` past its page ([VALUE_DECODE.md](VALUE_DECODE.md#decode-buffers)).

**Publishing.** Each column publishes its completed cells in plan order (`CellPublisher`, `BatchExchange.tryPublish`), under the read's lock and without waiting: a cell that finds the ready queue at its limit stays at the head of the column's publish order, and the consumer's take runs the column's room listener (`BatchExchange.setRoomListener`), a result of the read that publishes it. No job waits on the exchange. A published cell drops its segments and its batch, which a page spanning several cells would otherwise keep alive. The job that completes a cell also runs the column's filter fragment on its batch ([RECORD_FILTERING.md](RECORD_FILTERING.md#batch-matchers)): the decode that fills it last, or the job completing a sealed cell. A fragment's calls are serialized, since cells complete on several threads at once.

Tests: `ReadAheadTest`, `PlacerTest`, `BinaryFillChunksTest`, `ConcurrentPlainBinaryFillTest`, `FlatColumnBinaryViewsTest`, `NestedColumnTest`, `DictionaryFallbackNullsTest`, `BatchPlanTest`, `BatchExchangeTest`, `FilterOnlyColumnSkipTest`, `TaskAdmissionTest`, `ShuffledTaskOrderTest`, which runs the jobs in seeded random orders, and the result-comparing suites, which read every fixture through the column readers at small batch capacities.

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

The plan is extended a row group at a time, by the stage that computes the row group's fetch plans, and kept beside them in the work item's state. Work items are planned one stage job at a time, in work-list order, so the plan follows the [incremental planning](#incremental-planning) of the work list and a row group's batches depend only on the row groups before it. Each row group is planned exactly once, whichever column reaches it first or however far planning runs ahead, so every column reads the same batches, and `BatchPlan` fails the read on a row group planned out of order.

**Consumption.** A column takes a work item's batches (`RowGroupBatches`) with the pages its walk hands out. Its placement checks that it placed every row the plan takes from the row group it leaves, seals a cell the plan closes, and takes the file name and flag for the cells that follow. A flat column seals a cell the moment it holds its planned rows. A column with repetition seals it at the next record start, at the next row group or at the end of the stream, since the record's last values can follow on later pages.

**Alignment.** Every column of a read closes its batches at the same rows by construction, since they all follow one plan. A column that holds more or fewer rows of a row group than the plan takes from it fails with a `ParquetReadException` ("Batch plan mismatch") before it publishes a batch out of line with its siblings. The readers poll one batch per payload column per step, and one per filter-only column in steps statistics did not prove ([RECORD_FILTERING.md](RECORD_FILTERING.md#filter-only-column-skip)), and raise `IllegalStateException` when a column has no batch or a batch of another record count. A new boundary rule belongs in `BatchPlan`, and its inputs must be known before decoding and be the same for every column.

The row-group cut costs every column of the read a batch hand-off per row group, which slows a scan over small row groups measurably, so only a read with a binary column, whose values draw on a dictionary per batch, pays it. A batch spans row groups of one file only in a read without a binary column, and **no batch straddles files**. `RecordFilterTally` attributes counts per file from the batch's file name, and both row readers take their current file name (for `FileAwareRowReader` and error context) from the first column's batch. A file is told apart by its position in the read, so a path listed twice in a row starts a new batch.

Tests: `BatchPlanTest`, `PlacerTest`, `RecordFilterEventTest`, `FileNameInExceptionTest`, `FilterOnlyColumnSkipTest`.

### BatchExchange

The exchange holds a ready queue (column → consumer) and, in recycling mode, a free queue (consumer → column):

| Mode | Used by | Batches | Ready-queue limit | Consumer contract |
|---|---|---|---|---|
| Recycling (`BatchExchange.recycling`) | `FlatRowReader`, `NestedRowReader` | A pool of one more holder than the ready queue holds, allocated up front; a column fills new holders past it as far as its read-ahead goes. The flat row reader's free queue keeps every holder returned (`BatchExchange.recyclingEveryHolder`), so that a read reuses the arrays and binary chunks of every holder its read-ahead filled, which it holds until the read ends; the nested row reader's keeps at most three, since a nested cell allocates its arrays per batch | Two batches | Returns each batch (`recycle`) when it moves to the next; a flat batch's arrays are valid until then, a nested batch's for as long as they are referenced ([ROW_READER.md](ROW_READER.md#values-and-nullness)) |
| Detaching (`BatchExchange.detaching`) | `ColumnCursor` (column readers) | A fresh batch per cell | Two full batches' worth of rows | Owns every batch it takes; the arrays are never reused ([column-reader.md](../docs/content/how-to/column-reader.md#retaining-and-handing-off-batch-arrays)) |

A column publishes without waiting (`tryPublish`): in detaching mode an empty queue takes any batch, and a batch that would take the queue past its limit is not queued; in recycling mode a batch is queued while the bounded queue has a free slot. A cell takes the batch it fills with `newBatch`: a returned holder when the free queue has one, whose arrays a flat cell reuses when they are large enough, and whose full binary chunks it fills again ([COLUMN_READER.md](COLUMN_READER.md#variable-length)), a new one from the exchange's factory otherwise. A nested cell allocates its arrays for every batch, on either exchange ([NESTED_DECODE.md](NESTED_DECODE.md#cells)). The consumer runs the column's room listener after each batch it takes, which publishes the batches that waited ([Publishing](#decoded-columns)).

**Detaching limit.** A batch ends early at a row group or a file ([When a batch closes](#when-a-batch-closes)), so a read over small row groups publishes many short batches. The detaching limit counts rows, so the queue holds what two full batches hold however the batches are cut. A flat batch counts the rows its arrays were allocated for (`Batch.capacity`), since a batch left open into the next row group gets full-size arrays and can still end short, at a file, at a change of the always-match flag in a filtered read, or at the end of the stream; a nested batch's arrays are allocated at the records it holds, and it counts those. A batch of no rows counts as one, so the limit also bounds the number of queued batches.

**Consumer protocol.** `poll()` tries a non-blocking poll first and reads the `finished` flag only after a poll comes back empty, so every batch published before `finish()` is delivered before the end is reported. `finish()` also offers an end-of-stream sentinel, which queues behind every batch and releases a consumer already waiting; it is best effort (the recycling queue, when full, has no room, and then the consumer has batches to take and is not waiting). A `BatchWait` JFR event spans only the timed wait, so its duration is the consumer's stall.

Tests: `BatchExchangeTest`, `BatchExchangeRowLimitTest`.

### Back-pressure chain

```
consumer stops polling
  → ready queue at its limit; completed cells wait at the head of each column's publish order
  → the read's first unpublished row stops; the poll starts no decode past the cap
  → each column's held pages reach their bound, or its next row passes the cap: its walk stops
  → planning stops at the next unit once the read's data not started fills F;
    no speculative request starts past F
consumer polls
  → its take runs the room listener: the waiting cells publish, the first unpublished row advances
  → the poll starts the decodes the cap admits and the walks the cap lets go on
```

**How far a read runs ahead of its consumer is bounded by the read-ahead cap, the pages a column holds and the exchanges, never by the size of the file or the number of row groups or files.** No column data is fetched before a column's walk reaches it, apart from the read's next unit and the speculative requests it starts ahead, which F bounds per read and the context's read-ahead buffer across reads, and the planning reads of the row groups planned ahead ([FETCH_PLANNING.md](FETCH_PLANNING.md#requests)); a consumer that stops reading stops the fetching once its read holds F. The per-column horizon is the cap, counted from the slowest column's first unpublished row, plus the span of the pages in flight and held, plus the batches the exchange holds.

Tests: `ReadAheadTest`, `MultiFilePlanningTest`, `S3SelectiveReadJfrIT` (s3).

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

The read's work list is an ordered list of `WorkItem`s, one per surviving row group: the file, the row group and its index in the file, the file's schema and column ordinals, the item's index in the list, the rows planned before it, the row group's always-match flag, the per-leaf filter decisions its dictionaries sharpen later, and its page-index window. `ReadPlanning` holds one state per work item (its `Planning`): what its stages decided and the planning reads it holds. The read holds, per work item, whether it is planned, its file, and how many decoded columns have still to pass it. The schema checks (`SchemaCompatibility`), the pruning by statistics, Bloom filters and dictionaries (`RowGroupPruning`) and the building of each row group's fetch plans (`FetchPlanBuilder`) are classes of their own, which `Planner` runs.

Each column walks the list with an integer index: once the work item at its index is planned, the column enters it (the read commits the work item's drop and page-filter events the first time a column enters it, and tells the working set which row group the column's batches draw on) and walks its own column's plan; when the walk ends, the column passes the work item. The last column to pass a work item releases its planning state, its planning reads and its share of its index window: the read holds the planning state of the work items it has not passed, so a read of many files holds the state of the files it is reading. Page slices and decodes in flight keep their bytes alive until they finish. The index is an integer because the list grows behind it as files are listed.

### Incremental planning

Listing a file (`ReadPlanning.listFile`, `Planner.planRowGroups`) plans exactly one file, once its footer is loaded, and appends its surviving row groups: it validates the file's schema against the reference schema (the first file's), resolves the file's own leaf ordinals for the touched columns, checks that the chunk at each touched ordinal carries that leaf's path, prunes row groups on their statistics ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md)), applies the physical skip and the unfiltered row budget, and forms the file's page-index windows. Once every file is listed or the budget is spent, the work list is complete. A row group's Bloom filters and dictionaries are read when its work item is taken through them. The row budget, rows planned, skip residue and first-row-group skip are the planner's fields, since each listing resumes where the last one stopped.

The read runs one planning job at a time, a listing or a stage of the work item next in work-list order, so no two threads plan, and applies its result under the lock before the work item counts as planned. It plans the work item the slowest column is in and the next, whatever F holds, and further while the bytes of the data requests it has inserted and not started stay below F ([FETCH_PLANNING.md](FETCH_PLANNING.md#planning-ahead)); it lists the next file under the same bound. A stage that lacks bytes returns what it lacks, and the read waits for it before it runs the next stage.

**Invariant: reader structure is never derived from the contents of individual row groups in individual files.** A decision the reader takes once, at construction, from facts about row groups would force every file to be planned before the first row, returning time to first row to O(files). Facts about a row group reach the read on its work item:

| Question | Answered |
|---|---|
| Does statistics prove every row matches? | Per row group: `WorkItem.filterAlwaysMatches` → batch plan → the work item's `RowGroupBatches` → cell → `Batch.filterAlwaysMatches` |
| Where do batches close, and does the row cap still hold under a filter? | Per row group, by the batch plan, as the row group's fetch plans are computed ([When a batch closes](#when-a-batch-closes), [row limits](#row-limits)) |
| Batch-matcher or per-row filter evaluation? | From the predicate's shape alone (`BatchFilterCompiler.tryCompile`) |
| Batch size | From the first file ([batch sizing](#batch-sizing)) |
| Does the read have any work? | `hasLiveWorkItem()`, asked by every reader before it starts its columns. A row group of no rows is not live, and a read without one starts no column. Without a filter: the first file has a row group of at least one row, or else the read's jobs list the files after it, while the caller waits, until one has. With one: the read's jobs list files and probe the work items' Bloom filters and dictionaries in order, while the caller waits, until the first one of at least one row they do not drop, reporting each drop as they reach it; the page index of that work item is left to planning |

Two more questions are asked before the columns start and plan only as far as they must, on the read's jobs while the caller waits. `firstRowGroupSkip()` lists files as far as the first work item, where the value is written. `canFastSkipAllRowGroups()`, asked by the tail-read path, takes every work item through its page index and mask probe; that path is single-file, so it reads one footer.

**Invariant: a planning failure is sticky.** Once listing a file, loading its footer or taking a work item through a stage has failed, the read keeps the failure at the work item it failed from: no file is listed and no work item planned from there, and each column that reaches that work item ends with the failure, placed at the column and, for a work item's failure, at its file and row group. A column that found nothing left to plan would otherwise end its stream cleanly, and a consumer polling that column first would see a short read. A work item before the failure is still handed out: the column that reaches the failure first can be ahead of the others, and a column failed short of it would return fewer rows than they do.

**Error timing.** The first file's footer is read by `openAll`, so its errors surface there. A later file's I/O errors and its disagreement with the reference schema are raised when a column reaches the file, and after every row of the files before it; a mismatched file is never read as data. The next file is listed ahead of the read, but a failure there is not raised until a column reaches the failed file's first work item, so every row of the files before it is returned first, and the failure is placed at the file it belongs to.

**Reading every row opens every file.** Planning is deferred, not avoided. A read that stops early (a row budget, a row cap, a consumer that closes) leaves the files past its stop unopened, apart from the footers it read ahead. A read whose data totals less than one batch walks every file to fill the first batch.

The user-facing contract (reference schema, when a mismatch surfaces) is in [multi-file.md](../docs/content/how-to/multi-file.md).

Tests: `MultiFilePlanningTest`, `SchemaCompatibilityTest`, `CrossFileColumnOrderTest`, `MultiFileRowReaderTest`, `LiveWorkItemTest`, `EmptyFilesReadTest`, `PlanningFailureTest`, `LaterFileFailureTest`, `LaterFileDataFailureTest`.

### Row-group and file transitions

Within a file, a batch continues from one row group into the next, except where the always-match flag changes on a filtered read or the read ends its batches at row-group boundaries ([When a batch closes](#when-a-batch-closes)).

The work item after the one the slowest column is in is the read's next unit ([FETCH_PLANNING.md](FETCH_PLANNING.md#planning-ahead)): its first data request starts at once, without F and uncharged, and it is planned whatever F holds, its Bloom filters and page index read as its stages demand them, the next file listed when the slowest column is in its file's last work item. A planning failure there does not fail the current work item: the read keeps it, and each column that reaches the failed work item ends with it. Planning plans nothing once the read is closed. With the next file's footer, started once the read reached the file before it, the next file's metadata and first bytes are in flight by the time the current file is exhausted, and the transition does not stall the consumer. A read that reads footers ahead has the footers and data of several files in flight at once, so a run of files too small to cover one round trip each does not put one round trip per file on the read ([FETCH_PLANNING.md](FETCH_PLANNING.md#opening-files-ahead)). Local and remote files differ only in what a fetch costs ([FETCH_PLANNING.md](FETCH_PLANNING.md)).

### Filter flags

The always-match decision travels from the work item to the column in the row group's batches (`RowGroupBatches`), and the placement stamps it on each cell it opens beside the file name. A filter-only column in an always-match row group gets a `SkippedColumnFetchPlan`: its boundary marker is placed with no decode, and the column holds none of the row group's requests. On the marker the placement seals the cell the plan closes before the row group, places nothing, and ends the column's placement where the row cap ends it in the row group. The consumer-side rules are in [RECORD_FILTERING.md](RECORD_FILTERING.md#filter-only-column-skip).

## Row limits, errors and close

### Row limits

`maxRows` (from `head(n)`, and `n + head` for a filtered `skip(n)`, see [ROW_READER.md](ROW_READER.md)) reaches the read and the batch plan of a row reader. For an unfiltered `skip`, the batch plan gets `head` plus the skip's residue within the first kept row group, which the reader discards. It is enforced at four levels:

| Level | Where | Unfiltered read | Filtered read |
|---|---|---|---|
| Planning | the listing's row budget (`Planner.planRowGroups`) | Stops listing row groups and files once the budget is covered | No effect: every row group stays available |
| Fetch | Per-row-group remainder of the budget ([FETCH_PLANNING.md](FETCH_PLANNING.md#row-limits-and-first-parts)) | Truncates the last needed row group's pages where their row counts are known without decompressing: from the OffsetIndex, or from the value count of a flat column. A nested column without an OffsetIndex keeps its pages, and its walk hands them out until its placement reaches the cap | No truncation |
| Batch plan | `BatchPlan` | Ends the last batch at the cap; every column publishes it and finishes | Holds the cap while every row group with kept rows is an always-match one; drops it for the rest of the read at the first undecided one |
| Reader | Match counting | — | Counts matches against the cap |

A column whose placement reaches the cap takes no further page, stops its walk, which passes the sections it holds, and passes every work item listed ([FETCH_PLANNING.md](FETCH_PLANNING.md#release-failure-and-close)); once every column's placement has, the read lists, plans and reaches nothing more. A read with a row limit starts no speculative request and reads only the next file's footer ahead ([FETCH_PLANNING.md](FETCH_PLANNING.md#opening-files-ahead)). A page is the smallest decode unit: a page that straddles the cap is decoded and filled up to the cap. The column readers carry no row cap (#433).

Tests: `BatchPlanTest`, `PlacerTest`, `AlwaysMatchingRowGroupTest`, `MultiFilePlanningTest`, `S3SelectiveReadJfrIT` (s3).

### Error propagation

A column's failure reaches its exchange, which raises the first failure it receives, since that is the one that ended the column; each later failure of the same column is attached to it as a suppressed exception. Where the failure ends the column's stream depends on where it was met:

| Failure | Ends the stream |
|---|---|
| A page's decode, a fill, or a cell's completion | at once, for its column only; the decode of a page a column decoded through levels took before its placement ended, which holds no row of the read, raises nothing (untested) |
| A walk's or a placement's, met before the walk took a page of a work item whose batches close the open batch | once the pages taken before it are decoded: after every cell before the failure is published; the open cell is sealed and published, or abandoned when the column holds fewer rows of its row group than the plan takes |
| A walk's or a placement's, inside a work item | once the pages taken before it are decoded: after every cell before the open one is published; the open cell is abandoned, and a placement keeps the segments it placed before the failure |
| A column's shortfall of the plan at the read's end | for its column only, placed at its last page: after every cell before the open one is published; the open cell is abandoned |
| Planning's (a footer, a listing, a work item's stage) | when the column reaches the work item it failed from: after every cell before it is published; the open cell is sealed, or abandoned when the column is short of the plan |
| An `Error` of a walk's own or a decode's; a failure applying a job's result or thrown by the poll, an `Error` included | at once, for every column of the read; the jobs the poll emitted before it threw still run and are counted. Untested |
| The executor's refusal of a job, which a closed `HardwoodContext` gives | at once, for every column of the read, as an `IllegalStateException` whose cause is the refusal |

A request whose read failed for good, with an exception or an `Error`, fails the walk that reaches it or waits for it, so it ends the column as a walk's failure does ([FETCH_PLANNING.md](FETCH_PLANNING.md#release-failure-and-close)). A decode's own failure among the pages a walk took before its failure is therefore raised first, as it would be had the walk not run ahead of the decodes. Since a decode's failure ends only its own column, a reader that takes its columns in order raises the failure of the first column it takes, whichever page failed first. A failure between work items is met by every column at the same row, the end of the work items before it, so the partial batches stay aligned; a column short of the plan's rows is not at that row, and its partial batch is abandoned, with the shortfall attached to the failure as a suppressed "Batch plan mismatch". A failure inside a work item is met by one column at a row the others read past; its partial batch would be shorter than theirs, and a reader taking the columns in lockstep would report the mismatch instead of the failure.

Each failure is placed before it reaches the exchange (`DecodedColumn.placeFailure`): the column path is the column's own, and the file, row group and page come from the page or the walk it was met at. A fill that runs on another page's job, a nested cell's, raises a segment's failure with the segment's own page (`SegmentFillException`), which the column places instead. A runtime failure is classified as a read failure (`ExceptionContext.asReadFailure`, [EXCEPTION_MODEL.md](EXCEPTION_MODEL.md#crossing-a-thread-boundary)); an `IOException` stays checked and gains the place; an `Error` stays unplaced. Where a `MetadataSource` supplied the footers, a read failure also carries a hint at the supplied footer. `BatchExchange.checkError` rethrows each as it stands, so the reader's signature declares `IOException` and nothing between the column and the reader wraps. The exception types a caller sees are in [error-handling.md](../docs/content/reference/error-handling.md).

The consumer receives every batch published before the failure, then the failure. A reader that finds any column's stream ended checks every exchange for an error before reporting the end, so a failure on a column other than the first, a filter-only column included, is not reported as a short read. The context's close lets the tasks already queued run, so no column waits for a job that never runs.

Tests: `ReadFailureTypingTest`, `ReadFailurePositionTest`, `SpeculativeErrorTest`, `ColumnReadersTransportFailureTest`, `ContextClosedMidReadTest`, `LaterFileDataFailureTest`, `BetweenWorkItemsFailureTest`, `ColumnFailureTest`, `PageRowCountTest`, `PlacerTest`, `BatchPlanTest`, `BatchExchangeTest`.

### Cancellation and close

`RowGroupIterator.close()` marks the read closed and stops every column under its lock: no publish runs after it, and each exchange is finished, so a consumer waiting for a batch finds the end. A poll of a closed read emits nothing. The close then waits until every job the read emitted has ended and its result is applied (walk advances, planning, decodes, reads and footer reads alike), running the read's queue meanwhile on a context of zero threads. A footer whose read completed is still parsed, so the reader's other reads find its load complete. Once the read is quiescent, the close returns the requests' charges to the context's read-ahead buffer, cancels its wait for a task admission, releases its planning reads and index windows, gives up the footer loads it claimed and did not complete, so that the next request for such a file loads it afresh, drops the tails and footers of the files whose footers it requested, and stops the parent's tracking of the read. An `Error` a footer's read or parse ahead failed with, which no column raised, is thrown by the close.

The close returns only once the read is quiescent, so the caller can release what the `InputFile` owns (mapped buffers, direct buffers, connections) without a job reading freed memory. It interrupts no thread. The wait does not give up on an interrupt; a caller interrupted while closing gets its flag back afterwards. A file is therefore never closed under a read in flight, whichever reader closes it. A row reader closes its read before it returns its held and queued batches to the pool (recycling mode), and a column read's scan closes its read likewise. The read's close is idempotent; each reader's close is claimed by one thread (`CloseClaim`), and a closer on another thread, such as the parent reader's, waits until the claimed close has completed.

Tests: `ReaderCloseMidReadTest`, `RowReaderCloseIdempotencyTest`, `ReadYieldTest`, `FilesAheadTest`, `IteratorTrackingTest`, `ParentCloseTest`, `ParquetFileReaderCloseTest`, `S3SelectiveReadJfrIT` (s3).

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

A stable knob that needs a typed contract stays typed on its builder and out of the map, as `batchSize(int)` does on the column-reader builders. The `hardwood.internal.*` system properties (`maxCoalescedBytes`, the largest request, S; `fetchAheadBytes`, the speculative data bytes one read holds, F; `indexWindowBytes`, the page-index window budget; `tailBytes`, the size of a file's tail; `maxFilesAhead`, the files a multi-file read opens ahead; `maxWorkingSetBytes` and `readAheadBytes`, the working-set limit and the read-ahead buffer ([VALUE_DECODE.md](VALUE_DECODE.md#page-decode)); `zeroThreads`, which makes every context one of zero threads so the test suites can run on them; and `batchSize`, which replaces the auto batch size so the test suites can run at small batch capacities) are overrides for tuning and testing and not part of `ReaderConfig`. The user-facing option reference is [reader.md](../docs/content/reference/reader.md#reader-options).

Tests: `MetadataFilteringOptionTest`, `FixedSizeListFastPathReadTest`.

## Boundaries

- **Read-ahead and the consumer's pace (#1577).** The cap is fixed and ignores the consumer's pace. A consumer-bound read fills it, and its decoded batches wait long enough to survive a young collection and be promoted. On a wide schema the cap, converted to rows, can end inside the current row group, so the next row group's pages are not read ahead and the consumer waits at each row-group boundary.
- **Files stay open until the reader closes (#100).** A multi-file read releases no file as it moves past it.
- **Schema drift across files (#941).** Every file must match the reference schema for the columns a read touches.
- **`BYTE_ARRAY` width estimate (#899).** Batch sizing guesses a fixed width per `BYTE_ARRAY` value instead of reading the recorded unencoded size.
- **Row caps on the column readers (#433).** `ColumnReaderBuilder` and `ColumnReadersBuilder` have no `head`, so no cap reaches their columns.
