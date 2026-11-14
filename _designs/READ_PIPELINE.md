# Read pipeline

Describes how a read turns the row groups of its files into published batches: the parts of a read's `ReadEngine`, the jobs they run and the one rule that orders them, the bounds on how far a read runs ahead, the lifetimes of what it holds, how pages are placed, decoded and published, batch sizing and the batch plan, multi-file reads, row limits, the failure model, close and `ReaderConfig`.

Related documents:

- [FETCH_PLANNING.md](FETCH_PLANNING.md): which bytes a read requests and in which requests (the per-row-group steps, page-index windows, coalescing, the file's tail, page masks)
- [COLUMN_READER.md](COLUMN_READER.md): what a batch holds and how the column reader exposes it
- [ROW_READER.md](ROW_READER.md): how the row reader presents batches as rows
- [VALUE_DECODE.md](VALUE_DECODE.md): how a page's values are read in place
- [NESTED_DECODE.md](NESTED_DECODE.md): how pages become nested batches
- [RECORD_FILTERING.md](RECORD_FILTERING.md): filter evaluation over the published batches
- [THREADING.md](THREADING.md): which thread runs each of the read's jobs, and a context of zero threads
- [TESTING.md](../TESTING.md#task-orders): the harness that runs a read's jobs in seeded random orders
- [docs/content/concepts/concurrency-model.md](../docs/content/concepts/concurrency-model.md): the user-facing model

## Components

Every data reader (`FlatRowReader`, `NestedRowReader`, a `ColumnReaders` group, and a single `ColumnReader`, which is a group of one) runs the same read, for flat and nested schemas and for one file or many. A single-file read is a multi-file read of one file.

A read is a `ReadEngine` (`internal.reader.engine`), one per data reader, started from its `PreparedRead` and built from parts that each own their state. Dependencies run one way, from the footers to the batches:

| Part | Does | Owns |
|---|---|---|
| `FooterReader` | reads the footers of the files after the first, through the reader's `FileMetadataCache`, and lists the files in order, each checked against the first file's schema | the footers being read and the files listed |
| `RowGroupPlanner` | moves each unit through its states: lists it, probes its Bloom filters and dictionaries, reads its page-index window, plans it, and batches the units in order; commits what pruning reports of each file and unit once the consumer reaches it | the read's units while they are planned, their page-index windows, and the pruning reports |
| `PagePlanner` | plans a unit's pages and ranges on its plan job, from its listing and page index | nothing of the read |
| `RangeFetcher` | fetches each range a read requests, the planner's and a batched unit's, as a job; re-keys a fetch not yet started; and releases a range once its last reader is done | nothing beyond the ranges' own state |
| `HeaderScanner` | scans each column chunk without an OffsetIndex header by header as its bytes arrive, and hands the pages it finds to placement | each such chunk's scan |
| `PageTracker` | tracks each page of a batched unit until its ranges, its dictionary and its placement are there, and starts its decode then | the batched units' pages and their chunks' dictionaries |
| `CellWriter` | writes each column's pages into the cells of the batch plan: places them in page order, decodes and fills them, completes each cell into its exchange's batch, and seals or abandons a column's open cell at a failure | each column's placement and cells |
| `ColumnContext` | gives the other parts each decoded column's schema, layers, working-set handle and failure context, and charges each column's dictionaries to the working set one at a time | the read's working set |
| `BatchPublisher` | publishes each column's batches in row order into its exchange, then its end or the read's failure once settled | the batches in plan order and the consumer's position |
| `JobScheduler` | decides which ready job starts next, by the one priority within the bounds; knows nothing of Parquet | the ready jobs and the bounds |
| `ReadDriver` | runs the jobs on the context's executors and applies their results under the read's one lock | the lock and the hand-off of results between threads |

The parts pass items between them, each holding the state of one thing the read handles. A part acts on items; an item holds state and calls no part, so whatever an item's state change needs of the scheduler or a fetch, a part does. An item keeps its state private: parts, an enclosing one included, read it through accessors and change it only through methods that name the change, so each item's state changes only in the ways it allows.

| Item | Holds |
|---|---|
| `Unit` | a row group of the read in one of its states ([a unit's states](#a-units-states)), each a record holding what the row group has in it: what its file's listing decided of it (`UnitListing`), then its plan (`UnitPlan`), then its batches (`RowGroupBatches`); the planner holds each unit's current state, and the parts after the planner read the batched one |
| `Output` | something a service waits for that ends once, with a value or a failure: a range's bytes, a dictionary's values, a flat page's placement; and the waiters it wakes as it ends |
| `RangeFetch` | the fetch of one range of a file a read requests as one (`FileRange`): its bytes, the fetch's output, and their charge |
| `TrackedPage` | a page of a batched unit until it is decoded: its ranges, its chunk's dictionary, and its task once placed |
| `ChunkDictionary` | a column chunk's dictionary: its values, the output of its dictionary page's decode |
| `Batch` | one batch of the batch plan: each column's cell for it, and the ranges it holds until every column has published it |
| `Charge` | the bytes something the read holds was charged and its readers; the scheduler takes it back once the last reader is done |
| `ReadFailure` | the read's failure: the first row the failed work served, the column it was met in, and whether it has settled; every part that can fail reports to it |
| `BatchExchange` | per decoded column, the bounded hand-off of published batches to the consumer |

The decoded columns are the projection plus any filter-only predicate columns (`ReadProjection.decoded()`, see [RECORD_FILTERING.md](RECORD_FILTERING.md#augmented-projection)). Every decoded column publishes batches cut by the read's one batch plan into its `BatchExchange`. A column is an index into the parts' arrays; no part keeps an object per column.

**Ownership.** `ParquetFileReader` reads the first file's footer at `open`/`openAll` and owns the input files and a `FileMetadataCache` of parsed footers. When that fails, it closes every input file, and the context when the call created it, before the exception propagates, attaching close failures as suppressed. Each data reader it builds starts from a `PreparedRead`, which checks the first file against the read's schema, prunes its row groups and finds where a skip lands, and holds nothing to close; once the reader has built its columns, the prepared read starts the reader's own `ReadEngine`, sharing that cache; a second start fails. The parent tracks a read from the start of its engine until the engine closes, so a build that fails before its read starts leaves nothing tracked, and a build that fails after it, its first batch included, closes the engine before the failure propagates (`ReadEngine.closeAfter`); so does an engine whose first footer listing throws while it starts (untested); closing a child releases the engine's state and its tracking entry but leaves the files to the parent. Closing the parent closes every child still open, then the metadata cache, an owned context and the files, so no job of a child's read reads a file after it is closed; it attempts every input file, throws the first close failure and suppresses the later ones under it. The parent's close runs once: a later or concurrent close waits for the exclusive side of the lifecycle lock and returns without closing anything again or rethrowing the first close's failure. A child build holds the shared side of the parent's lifecycle lock and the parent's close the exclusive side: the close waits for the builds under way, each of which, finding the parent closed, closes the child it made and fails, and a build started after the close fails at once.

Tests: `MultiFileRowReaderTest`, `EngineTrackingTest`, `ParentCloseTest`, `ParquetFileReaderCloseTest`.

## Scheduling

A read's work leaves its lock as jobs. A job's work runs outside the lock, and its end is applied under it through its callbacks (see Job ends and outputs below); the scheduler is then asked what the end made possible. Which thread runs each kind of job is in [THREADING.md](THREADING.md#thread-inventory).

| Kind | Does | Bounded by |
|---|---|---|
| Fetch | reads one range of a file: data, a dictionary, a page-index window, Bloom filters | F and R |
| Footer | reads one file's footer as far as I/O goes | F and R |
| Plan | parses a footer, probes a unit's Bloom filters and dictionaries, plans a unit's pages and ranges and parses the headers of those pages its file holds | CPU slots; a unit's probe and plan also the look-ahead where its file holds every byte; a probe's pass also R where a dictionary it may parse exceeds `DecodeBuffers.RETAINED_BYTES` |
| Decode | decodes a page into its cells, measures a page decoded through levels, parses a dictionary | the look-ahead and CPU slots; R where its task's buffers exceed `DecodeBuffers.RETAINED_BYTES` |
| CPU | scans a chunk's headers, parses the headers of the planned pages a request holds, fills a sealed nested cell, completes a sealed flat cell | CPU slots |

**Priority.** Every job is keyed by the first row of the read it serves: the rows of the files and units before it plus its row in its unit, counted in rows its filter keeps. A range of a chunk without an OffsetIndex is keyed by the row as far into the chunk as it lies until its chunk's scan finds a page in it, and then at that page's row, so the bytes of a page the consumer needs are never keyed past its need, and a footer by the end of the rows listed. The scheduler starts the lowest key first, across kinds. Planning takes a free CPU slot before any other CPU job, whatever its key: it leads to a fetch, whose latency the decodes queued ahead of it would otherwise wait out first. A unit whose file holds every byte (`HeldBytes.inMemory`) leads to no fetch, so its probe and plan run as decodes do, in key order within the look-ahead: planned ahead, every unit of such a file is batched at once, and its pages' state, made as their tracking starts and filled in as they decode, lives long enough to be promoted. Nothing keyed past the read's failure starts.

**Bounds.** Each bound governs its own kinds of job, and a refusal holds back only the later jobs of those kinds:

| Bound | Governs | Reason |
|---|---|---|
| R, the context's read-ahead buffer | everything the read holds by its choice, across every read of the context: the bytes it fetches ahead, and the buffers of each decode whose task exceeds `DecodeBuffers.RETAINED_BYTES` beside the one need forces, sized from the page's header as the working set sizes a task ([VALUE_DECODE.md](VALUE_DECODE.md#page-decode)); a probe's pass is charged so for each dictionary it may parse, those of the filter's columns whose pages it holds and has not parsed, from their headers | no read on a context starves another, and a read's large decodes and probes do not run at once on every thread, since its working set counts one task's buffers; a charge that does not fit waits for R's next release, and a refused decode or probe also for the next job of the read to end; a refused probe holds back the plans after it |
| F (`hardwood.internal.fetchAheadBytes`, twice S by default) | fetches ahead of the consumer's need: a fetch starts while less than F is fetched ahead, so a request larger than F still runs one at a time | the requests in flight decide a remote read's throughput |
| The look-ahead (`ReadEngine.CAP_BUDGETS` batch byte budgets, in rows), counted from the first row a column has not published | decodes | a buffer that absorbs the consumer's pauses once the CPU is saturated; decoding further holds memory for nothing |
| Files ahead (`hardwood.internal.maxFilesAhead`) | footers | F and R do not bound the footers of many small files ([multi-file reads](#multi-file-reads)) |
| CPU slots, twice the decode threads | CPU jobs | the pool |

**What the bounds are for.** A read runs at the rate of its slowest resource (request bandwidth, cores, R) or of its consumer, never slower for its scheduling: no fetch latency left unhidden, no CPU slot idle while a job is ready. Each bound spends one resource to keep one stage off the consumer's path, and a stage must run ahead by about its delay times the consumer's rate; further holds memory for nothing.

| Bound | Spends | Keeps the read off the consumer's path when |
|---|---|---|
| F | memory for compressed bytes, requests | fetch latency or one request's bandwidth would stall the consumer: about F over the range size ranges are in flight, so F is also the request parallelism (one range is never split into concurrent requests) |
| CPU slots | cores | decode throughput is below the consumer's average rate |
| The look-ahead | memory for decoded values | the consumer briefly runs faster than decode: a steady consumer needs one or two batches to keep the slots in work, a consumer slower than decode on average needs none, and one that takes batches back to back after a pause needs the shortfall over the burst |
| R | — | the cap on everything held, not a speed control |

Fetch and decode are bounded apart because their delays differ. A request has a fixed latency on every range, so F follows from latency and range size. A decode starts as soon as a slot is free and takes milliseconds a batch, so it needs only a few batches ahead, and decoded values are several times their compressed bytes, so decoding ahead costs more memory than fetching ahead; one bound sized for both either holds many times the values or starves the requests. Which bound binds follows from the read's rates, not from where its file lives, so no bound switches on location or on a mode: F binds only where fetching gets ahead of the consumer, the look-ahead only where decoding does. In memory F fills at once and the look-ahead binds; on a remote read a range's pages become decodable together as it arrives, and the look-ahead keeps most of them compressed until the consumer nears.

**Invariant: need is the progress guarantee and no bound of its own.** While no job keyed below the consumer's next batch is in flight, the lowest such job starts, refused by no bound (a fetch at once, a CPU job at the next free slot a plan job does not take): the look-ahead never reaches below need, and that one job holds what it reads uncharged to R and counts against no F. The next batch thus advances one job at a time where the bounds hold everything else back. Where the consumer's next batch is the last the batch plan has added, the row groups not yet batched may continue it, and the planning of the next of them is below need too. R holds what the read chose to fetch ahead and the large decodes and probes it chose to run beside the forced one, and a read never waits for room another read holds to give its consumer the next batch.

**Job ends and outputs.** A job's end reaches the service that created it through the job's two callbacks, run under the lock: one takes the value its work produced, the other its failure; a job that fails returns its charge. A job adds its context to its failure, in its work or in its failure callback, before it ends an output, so every service waiting on the output sees the same failure. Everything a service waits for is an `Output`: a range's bytes (`RangeFetch`), read by the range's pages, a scan and the planner, and ended by the fetch's callbacks; a chunk's dictionary's values (`ChunkDictionary`), read by its data pages, and ended by the dictionary page's decode; and a flat page's placement (`TrackedPage`), ended by its column's placement. An output exists from when a service decides it needs it, before whatever ends it; it ends once, with the value or a failure, and wakes its waiters in the order they registered, each a lambda carrying the page, scan or unit it waits for. A service that needs several outputs waits for the first it lacks and, woken, looks again: a page its ranges, then its dictionary, then, flat, its placement; the planner a window's Bloom filters or page index; a scan the next range of its chunk. A range a file holds in memory, and a dictionary the probe decoded, have an output ended with its value from the start.

**Results.** A job's thread that finds the lock held queues its result and moves on, and the thread that holds the lock applies every queued result, and asks the scheduler again, before it lets go of it, so a queued result waits for no later acquisition; no thread waits for the lock to apply a result. A callback from outside the read (the consumer making room in an exchange, a release of R, another read's footer load completing) is a result like a job's. Bytes the read returns to R under the lock are returned once it lets go of it, since R runs its waiters, other reads' among them, on the releasing thread. Placement, the cells' bookkeeping and publishing run under the lock, as part of applying a result; decoding, filling and I/O never do, apart from completing a cell its placement seals after its last fill (see Cells).

**Retries.** A fetch, a footer's read included, that fails with an exception is run once more inside its job; a second failure is final. Nothing else retries.

**Lifetimes.** Everything a read holds is a `Charge` returned by its last reader:

| Held | Readers | Returned |
|---|---|---|
| A range's bytes | its unit's tracking until its scans hold it; the pages that lie in it; a scan until it has passed it; the batch of a flat page's last cell until every column has published it | once the last is done; a range whose last reader is done before its fetch starts is not fetched |
| A footer read ahead | its file | once the consumer passes the file |
| A decode's task buffers, or a probe pass's dictionary parses, above `DecodeBuffers.RETAINED_BYTES` | its job | as the job ends, with its value or its failure |

A range a file holds already (a file in memory, the part of a file's tail the read holds) is sliced in place and charged to neither F nor R. Decoded values count against the read's working set ([VALUE_DECODE.md](VALUE_DECODE.md#page-decode)), not R. The read's close returns every charge still held.

Tests: `JobSchedulerTest`, `ReadDriverTest`, `ReadYieldTest`, `PoolWaitTest`, `ShuffledTaskOrderTest`, `ZeroThreadContextTest`, `LargeDecodeTaskTest`.

## Decoded columns

```
unit batched ─→ planned pages (OffsetIndex) / pages a scan finds (no OffsetIndex)
                    │ placed in page order into the batch plan's cells (flat: before decoding)
fetch jobs ─→ ranges arrive ─→ decode job: decode into cells (flat) / decode and measure (nested)
                    │ applied: cells completed, measured pages placed (nested), sealed cells to a job
                    publish in plan order ─→ BatchExchange ─→ consumer
```

A column builds a new `PageDecoder` for each column chunk its pages come from, since a decoder checks a page against its own chunk's metadata ([VALUE_DECODE.md](VALUE_DECODE.md#page-decode)). A page reaches its column from the unit's plan, listed from its OffsetIndex, or from a scan of its chunk ([FETCH_PLANNING.md](FETCH_PLANNING.md#pages)). A scan emits a null placeholder for a page inline statistics prove holds no matching row, read by a decode job as a page of all-zero definition levels and no value bytes, without decompression (`PageDecoder.nullsInPlace`), which keeps the column row-aligned ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md)).

**Page headers.** A page's decode job is charged against R from the page's header (uncompressed size, value count, encoding), and the job is built under the read's lock, so each data and dictionary page's header is parsed before then, outside the lock, once per read, and the decode takes it as parsed. A scan parses the headers of the pages it finds. A planned page's header is parsed once the request it starts in has arrived, on a CPU job that parses those of every page starting in the request before its bytes are handed on, reading any part past the request from the file's tail; where the file holds the page's bytes, no request reads them, and the unit's plan job parses its header ([FETCH_PLANNING.md](FETCH_PLANNING.md#coalescing-a-unit)). A header that does not parse charges nothing, and its decode parses it again and fails at the page, with the message any malformed page gives. A scan that meets such a header hands on the pages before it and fails at it.

**Placement.** A column's pages are placed in page order, a column's chunks unit after unit, through the column's `ColumnPlacer`: following the unit's batches from the batch plan, it splits the page's kept records into one `Segment` per batch they fall into, opens a `Cell` at a batch's first record and seals it when the batch holds every record the plan puts in it. A flat page's kept records are known from its metadata (`PageInfo.records()`), so a flat page is placed as soon as its predecessors are, and its decode waits for its placement as for its bytes. A filter-only column a unit does not read ([RECORD_FILTERING.md](RECORD_FILTERING.md#filter-only-column-skip)) places nothing and closes the batch where the plan closes it.

**Columns decoded through levels.** A column with repetition, or with a layer above its leaf, and every column of a nested row read, is placed after its pages are decoded, since only a page's levels say where its records start. Its decode job decodes and measures the page (`NestedPageTask.decodeAndMeasure`); as the results are applied, the measured pages at the head of the column's pages are placed, in page order, and the cells the placement seals are filled on a job (`NestedCell.fill`, [NESTED_DECODE.md](NESTED_DECODE.md#cells)).

**The look-ahead.** The look-ahead bounds the rows a read decodes past its consumer. It is counted from the first row some column has not published, so every column decodes ahead to the same row, and a fast column does not spend it decoding ahead of a slow one whose batches the consumer still waits for. It is a byte budget converted to rows by what a row of the read costs (`BatchSizing.readAheadBudgetRows`): a fixed-width column its value width, a repeated one also its levels, each times its fan-out, and a `BYTE_ARRAY` column its chunks' decompressed bytes per value plus a start and an end offset, measured over the first file's row groups. Where a column ahead puts the consumer's need past it, the look-ahead reaches to the need, so it never holds back a decode of the next batch.

**Cells.** A flat column's cell takes its batch from the column's exchange (`BatchExchange.newBatch`) and gives it arrays of at least the size the plan gives, on the first decode job that reaches it (`Cell.allocate`); a job that reaches it meanwhile parks on the cell's lock until the arrays exist. The allocation stays on the decode jobs, which spread the zeroing of a read's batch arrays over the decode threads. A segment is written by its page's decode alone. Segments of one cell are filled concurrently and write disjoint ranges of its values; a word of the validity bitmap can hold bits of two neighbouring segments, and a segment writes the words at its edges with an atomic OR (`FlatFill`). Concurrent fills of one cell are reached only by chance, and the atomic OR on a shared word is untested. The cell completes exactly once: on the decode that fills its last segment or, when its placement seals it after its last fill, under the read's lock as the placement seals it, which runs the completion's filter fragment and a binary cell's finish there.

**Binary cells.** A binary column's cell has the plan-sized arrays of any flat cell, and its plain values' bytes, which the plan cannot size. Each segment copies its own into chunks it adds to the batch's buffers ([COLUMN_READER.md](COLUMN_READER.md#variable-length)), so a segment is filled like any other, within its page's decode, and no fill waits for another's bytes. Where the column's reader takes windows (`ViewDemand`), the cell's completion also builds the batch's window, the view of every value laid out in value order, so a published batch is never revisited for it.

**Decoding.** Every page is read in place: its values are written once, from the decompressed page straight into the batch arrays, with no page-sized value array between ([VALUE_DECODE.md](VALUE_DECODE.md#page-decode)). A flat page's decode reads its levels and ids and locates its values (`PageDecoder.decodeFlat`, `decodeFlatBinary`), and each segment's fill reads its rows from the page through the page's value reader (`FlatPageValues`, `BinaryPageValues`). A page that holds a different number of values than the rows its metadata gave its placement fails before it fills anything. A page no in-place reader takes is refused before it is decompressed ([VALUE_DECODE.md](VALUE_DECODE.md#encodings-and-readers)).

**Scratch.** A page's levels, ids and decompressed bytes live in scratch whose lifetime matches the longest reader of the page:

| Column | Scratch | Held |
|---|---|---|
| Flat | the job's `DecodeBuffers` (`FlatFill.scratch`) | for the page's decode |
| Decoded through levels | borrowed from the column's pool | until every cell the page is placed into is filled (`NestedPageTask.releaseScratch`) |

A pool keeps a bounded number of returned scratch buffers, and a page that finds the pool empty allocates one. Either kind keeps no array above `DecodeBuffers.RETAINED_BYTES` past its page ([VALUE_DECODE.md](VALUE_DECODE.md#decode-buffers)).

**Publishing.** `BatchPublisher` publishes each column's completed batches in plan order (`BatchExchange.tryPublish`), under the read's lock and without waiting: a batch that finds the ready queue at its limit stays at the head of the column's order, and the consumer's take runs the column's room listener (`BatchExchange.setRoomListener`), a result of the read that publishes it. No job waits on the exchange. The job that completes a flat cell also runs the column's filter fragment on its batch ([RECORD_FILTERING.md](RECORD_FILTERING.md#batch-matchers)); a fragment's calls are serialized, since cells complete on several threads at once.

Tests: `ReadAheadTest`, `ColumnPlacerTest`, `BinaryFillChunksTest`, `ConcurrentPlainBinaryFillTest`, `FlatBinaryViewsTest`, `NestedColumnTest`, `DictionaryFallbackNullsTest`, `BatchPlanTest`, `BatchExchangeTest`, `FilterOnlyColumnSkipTest`, `ShuffledTaskOrderTest`, which runs the jobs in seeded random orders, and the result-comparing suites, which read every fixture through the column readers at small batch capacities.

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

**The plan.** Every input to these rules is known before a page is decoded, counts rows, and is the same for every column: the capacity ([batch sizing](#batch-sizing)), the units' files and always-match flags, the row cap, and each row group's kept rows, which are the rows of its `RowRanges` when page masks apply and all its rows otherwise. Masks apply to every decoded column of a row group or to none ([FETCH_PLANNING.md](FETCH_PLANNING.md#the-mask-capability-gate)), so every column keeps the same rows. A row group that keeps no row (its row ranges are empty, or its bloom filters or dictionaries drop it) takes no part: it closes no batch and leaves the row cap in place.

The plan is extended a row group at a time, as its unit is batched, in unit order ([units and files](#units-and-files)), so a row group's batches depend only on the row groups before it. Each row group is added exactly once, so every column reads the same batches, and `BatchPlan` fails the read on a row group added out of order.

**Consumption.** A column's placement takes a unit's batches (`RowGroupBatches`) with the unit's pages. Its placement checks that it placed every row the plan takes from the row group it leaves, seals a cell the plan closes, and takes the file name and flag for the cells that follow. A flat column seals a cell the moment it holds its planned rows. A column with repetition seals it at the next record start, at the next row group or at the end of the stream, since the record's last values can follow on later pages.

**Alignment.** Every column of a read closes its batches at the same rows by construction, since they all follow one plan. A column that holds more or fewer rows of a row group than the plan takes from it fails with a `ParquetReadException` ("Batch plan mismatch") before it publishes a batch out of line with its siblings. The readers poll one batch per payload column per step, and one per filter-only column in steps statistics did not prove ([RECORD_FILTERING.md](RECORD_FILTERING.md#filter-only-column-skip)), and raise `IllegalStateException` when a column has no batch or a batch of another record count. A new boundary rule belongs in `BatchPlan`, and its inputs must be known before decoding and be the same for every column.

The row-group cut costs every column of the read a batch hand-off per row group, which slows a scan over small row groups measurably, so only a read with a binary column, whose values draw on a dictionary per batch, pays it. A batch spans row groups of one file only in a read without a binary column, and **no batch straddles files**. `RecordFilterTally` attributes counts per file from the batch's file name, and both row readers take their current file name (for `FileAwareRowReader` and error context) from the first column's batch. A file is told apart by its position in the read, so a path listed twice in a row starts a new batch.

Tests: `BatchPlanTest`, `ColumnPlacerTest`, `RecordFilterEventTest`, `FileNameInExceptionTest`, `FilterOnlyColumnSkipTest`.

### BatchExchange

The exchange holds a ready queue (column → consumer) and, in recycling mode, a free queue (consumer → column):

| Mode | Used by | Batches | Ready-queue limit | Consumer contract |
|---|---|---|---|---|
| Recycling (`BatchExchange.recycling`) | `FlatRowReader`, `NestedRowReader` | A pool of one more holder than the ready queue holds, allocated up front; a column fills new holders past it as far as its read-ahead goes. The flat row reader's free queue keeps every holder returned (`BatchExchange.recyclingEveryHolder`), so that a read reuses the arrays and binary chunks of every holder its read-ahead filled, which it holds until the read ends; the nested row reader's keeps at most three, since a nested cell allocates its arrays per batch | Two batches | Returns each batch (`recycle`) when it moves to the next; a flat batch's arrays are valid until then, except a leaf's below a struct, whose arrays the flat row reader takes off the holder first, and a nested batch's for as long as they are referenced ([ROW_READER.md](ROW_READER.md#values-and-nullness)) |
| Detaching (`BatchExchange.detaching`) | `ColumnCursor` (column readers) | A fresh batch per cell | Two full batches' worth of rows | Owns every batch it takes; the arrays are never reused ([column-reader.md](../docs/content/how-to/column-reader.md#retaining-and-handing-off-batch-arrays)) |

A column publishes without waiting (`tryPublish`): in detaching mode an empty queue takes any batch, and a batch that would take the queue past its limit is not queued; in recycling mode a batch is queued while the bounded queue has a free slot. A cell takes the batch it fills with `newBatch`: a returned holder when the free queue has one, whose arrays a flat cell reuses when they are large enough, and whose full binary chunks it fills again ([COLUMN_READER.md](COLUMN_READER.md#variable-length)), a new one from the exchange's factory otherwise. A nested cell allocates its arrays for every batch, on either exchange ([NESTED_DECODE.md](NESTED_DECODE.md#cells)). The consumer runs the column's room listener after each batch it takes, which publishes the batches that waited ([Publishing](#decoded-columns)).

**Detaching limit.** A batch ends early at a row group or a file ([When a batch closes](#when-a-batch-closes)), so a read over small row groups publishes many short batches. The detaching limit counts rows, so the queue holds what two full batches hold however the batches are cut. A flat batch counts the rows its arrays were allocated for (`Batch.capacity`), since a batch left open into the next row group gets full-size arrays and can still end short, at a file, at a change of the always-match flag in a filtered read, or at the end of the stream; a nested batch's arrays are allocated at the records it holds, and it counts those. A batch of no rows counts as one, so the limit also bounds the number of queued batches.

**Consumer protocol.** `poll()` tries a non-blocking poll first and reads the `finished` flag only after a poll comes back empty, so every batch published before `finish()` is delivered before the end is reported. `finish()` also offers an end-of-stream sentinel, which queues behind every batch and releases a consumer already waiting; it is best effort (the recycling queue, when full, has no room, and then the consumer has batches to take and is not waiting). A `BatchWait` JFR event spans only the timed wait, so its duration is the consumer's stall.

Tests: `BatchExchangeTest`, `BatchExchangeRowLimitTest`.

### Back-pressure chain

```
consumer stops polling
  → ready queue at its limit; completed batches wait at the head of each column's order
  → the first unpublished row stops; no decode starts past the look-ahead
  → the ranges the waiting decodes hold stay held; fetches stop once F is fetched ahead,
    or R has no room
consumer polls
  → its take runs the room listener: the waiting batches publish, the first unpublished row advances
  → the decodes the look-ahead admits start; the ranges they finish with return their charges,
    and fetches start again
```

**How far a read runs ahead of its consumer is bounded by the look-ahead, F, R, the files ahead and the exchanges, never by the size of the file or the number of row groups or files.** A consumer that stops reading stops the fetching once its read holds F.

Tests: `ReadAheadTest`, `FilesAheadTest`, `MultiFilePlanningTest`, `S3SelectiveReadJfrIT` (s3).

## Batch sizing

The batch capacity is fixed for a read and resolved once, at build time, by `ParquetFileReader.resolveBatchSize`, for every reader: the row readers through `createRowReader`, the column readers through `buildColumnReaders`. The columns, the exchanges and `BatchExchange.allocateArray` receive a concrete positive size.

- **Explicit.** `ColumnReaderBuilder.batchSize(int)` and `ColumnReadersBuilder.batchSize(int)` take a positive record count, used verbatim. The row readers have no explicit size.
- **Default.** `BatchSizing.computeOptimalBatchSize` divides a per-batch byte budget, sized so a batch amortises the per-batch overhead, by the bytes one row costs across the decoded columns, predicate columns included, since they allocate batch arrays too. A column costs its value width times its list fan-out, plus two `int` levels per value for a repeated column, whose nested batch carries both level arrays. Fan-out is the column's leaf value count over the row count, summed over row-group metadata (`BatchSizing.valuesPerRow`); a flat column's is 1. The per-value work of a batch is therefore roughly constant across fan-outs, and no row floor applies.
- **Array bound.** Under G1, an object larger than half a heap region is humongous, allocated in whole old-generation regions, and G1 picks 1 MB regions for every heap below 4 GB; an array per batch above half of that starts concurrent cycles and fills the old generation. No per-value array of a batch exceeds `BatchSizing.MAX_ARRAY_BYTES`, half of the smallest region less an allowance for the array header: a column's values per batch, its rows times its fan-out, are capped at the bound over the element width of its widest per-value array. That is eight bytes for an `INT64` or `DOUBLE` column's values, and four for every other column, whose `int` levels, binary views and the reference array of `ColumnReader.getStrings()` and `getBinaries()` (with compressed references) take four bytes a value. A wide projection meets the byte budget first. The cap uses the average fan-out, so a batch of a repeated column whose records run longer than average can still exceed it.
- **Clamps.** The default is at least 1 and at most `BatchSizing.MAX_BATCH`, the array bound for four-byte elements, which other components pre-size around (`FlatRowReader`'s all-present validity). It is also at most the rows the read can produce: the total row count of the file's row groups after the `RowGroupPredicate`, or `BatchSizing.ROWS_UNKNOWN` (no clamp) for a multi-file read. The bound must be an upper bound; pruning, a row cap and a skip only remove rows, so the clamp never shortens a batch the read could have filled.
- **First file only.** Fan-out and the row bound come from the first file's row groups, the one footer `openAll` reads. Sizing from later files would plan them before the first row (see [the invariant on reader structure](#units-and-files)).

The row path and the column path produce the same size for the same decoded projection. Independently built readers with different decoded columns size differently ([RECORD_FILTERING.md](RECORD_FILTERING.md#column-readers)).

Tests: `BatchSizingTest`, `BatchArrayBoundTest`, `WideListBatchBoundTest`.

## Multi-file reads

### Units and files

The read's units are its row groups that statistics keep, in file order and row-group order: a unit holds its file, its row group and its index in the file, its key (the first row of it the read publishes), its always-match flag, its page-index window and, once planned, its pages and ranges. Listing a file (`FooterReader`, then `RowGroupPlanner.addFile`) takes place once its footer is loaded and the files before it are listed: it validates the file's schema against the reference schema (the first file's), matching columns by path and, where sibling fields of one name give several columns one path, the `k`-th of the path to the `k`-th (`SchemaCompatibility`), resolves the file's own leaf ordinals for the touched columns, checks that the chunk at each touched ordinal carries that leaf's path, prunes row groups on their statistics ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md)), applies the physical skip and the row budget, and forms the file's page-index windows. A unit's Bloom filters, dictionaries and page index are read as its window's fetches reach it ([FETCH_PLANNING.md](FETCH_PLANNING.md#per-row-group)), and the unit is planned on a plan job once they have arrived.

Units are planned in any order and batched in unit order: a unit is batched, its batches taken from the batch plan, and its pages handed to `PageTracker`, once it and every unit before it are planned, so batches stay in row order whichever plan finishes first.

#### A unit's states

```
LISTED ──── nothing to probe ─────────┐
  │ something to probe                │
  ▼                                   │
PROBING ⟲ lacks dictionary bytes:     │
  │       fetches them, probes again  │
  ├──── kept ─────────────────────────┤
  │                                   ▼
  │                             AWAITING_INDEX
  │                                   │ its page index arrived:
  │                                   │ its plan is made on a job
  │ dropped: planned with no row      ▼
  └─────────────────────────────→ PLANNED
                                      │ every unit before it is planned
                                      ▼
                                   BATCHED
```

| State | Entered when | The unit gains |
|---|---|---|
| `LISTED` | its file's footer is parsed and its statistics keep it | its listing: file, row group, key, always-match flag, chunks, and the rows the row limit reaches in it |
| `PROBING` | its Bloom filters have arrived and it has a leaf they or its dictionaries may decide | what its passes decided and decoded; a pass that lacks dictionary bytes fetches them and probes again |
| `AWAITING_INDEX` | its probe kept it, or it has nothing to probe | the dictionaries its probe decoded; its page index is read with its neighbours' ([FETCH_PLANNING.md](FETCH_PLANNING.md#per-row-group)), and its plan is made on a job once the index has arrived |
| `PLANNED` | its plan is made, or its probe dropped it, which plans it with no row | its plan: the rows its filter keeps, each column's pages and dictionary page, and its ranges |
| `BATCHED` | it and every unit before it are planned | its batches, from the batch plan; its ranges are fetched and its pages handed to its columns from here on |

Each state is a record of `Unit` (`Unit.Listed`, `Unit.Probing`, `Unit.AwaitingIndex`, `Unit.Planned`, `Unit.Batched`) holding exactly what the unit has in it, and a state's transitions return the next one, so a transition the diagram lacks does not compile. `RowGroupPlanner` owns each unit until it is batched: it keeps the unit's current state and replaces it on each transition. The parts after the planner take the immutable `Unit.Batched`; a probe's state accumulates in place across its passes and is the planner's alone. A unit whose probe or plan fails takes no further transition: the read fails at its first row, and no unit from there on is batched.

**Invariant: reader structure is never derived from the contents of individual row groups in individual files.** A decision the reader takes once, at construction, from facts about row groups would force every file to be planned before the first row, returning time to first row to O(files). Facts about a row group reach the read on its unit:

| Question | Answered |
|---|---|
| Does statistics prove every row matches? | Per row group: the unit's always-match flag → batch plan → the unit's `RowGroupBatches` → cell → `Batch.filterAlwaysMatches` |
| Where do batches close, and does the row cap still hold under a filter? | Per row group, by the batch plan, as the unit is batched ([When a batch closes](#when-a-batch-closes), [row limits](#row-limits)) |
| Batch-matcher or per-row filter evaluation? | From the predicate's shape alone (`BatchFilterCompiler.tryCompile`) |
| Batch size | From the first file ([batch sizing](#batch-sizing)) |
| Does the read have any work? | Found by the read itself: a read that lists no unit, or whose Bloom filters or dictionaries drop every unit, ends its columns at once and returns no row |

One question is asked of the read once it is set up, from metadata: `leadingRows()`, the rows the reader drops itself. A read takes one skip, physical for an unfiltered `skip(n)` and the rows before the tail for `tail(n)`. The listing drops the row groups the skip covers whole, and the row group it lands in keeps the rows before its end, the residue. Where that row group is of the first file and takes a page mask ([FETCH_PLANNING.md](FETCH_PLANNING.md#the-mask-capability-gate)), its plan masks the residue out and the reader drops nothing; otherwise the read returns the residue and the reader drops it. Finding the row group reads the footers of the files the skip reaches on the caller's thread.

**Opening files ahead.** The first file's footer is read by `openAll`. The footers of the files after it start up to the files-ahead bound past the file the read's data has reached, the file of the furthest fetch started or of the consumer's need where that is further, so the data never waits for a footer F would let it fetch. A footer is keyed at the end of the rows listed, behind every listed unit's data, and is charged its tail's size against F and R until the consumer passes its file. A read with a row limit, or of files all in memory, reads only the next file's footer ahead: it may need no further file. The read claims each footer from the reader's `FileMetadataCache`, so every read of the reader shares it: a footer loaded is taken as it stands; a load another read or a caller runs is waited for without a thread, and claimed again once it completes; otherwise the read loads it, its read on the I/O executor and its parse on a plan job. A footer whose file's bytes are in memory, and which is read from the file or on a context of zero threads, is read in place by its parse. The read drops the footer and the tail of each file the consumer has passed, but the first file's, which the reader keeps, so a read of many files holds the footers of the files it is reading. A file's `FileOpenedEvent` begins when its footer's load opens the file and is committed by the parse.

**Reading every row opens every file.** A read that stops early (a row budget, a row cap, a consumer that closes) leaves the files past its stop unopened, apart from the footers it read ahead. Local and remote files differ only in what a fetch costs ([FETCH_PLANNING.md](FETCH_PLANNING.md)).

The user-facing contract (reference schema, when a mismatch surfaces) is in [multi-file.md](../docs/content/how-to/multi-file.md).

Tests: `MultiFilePlanningTest`, `SchemaCompatibilityTest`, `CrossFileColumnOrderTest`, `MultiFileRowReaderTest`, `FilesAheadTest`, `EmptyFilesReadTest`, `FileMetadataCacheTest`.

### Row-group and file transitions

Within a file, a batch continues from one row group into the next, except where the always-match flag changes on a filtered read or the read ends its batches at row-group boundaries ([When a batch closes](#when-a-batch-closes)). The next unit's fetches are keyed just past the current unit's, so they start as F admits them while the consumer reads the current one, and its plan runs as soon as its window's bytes have arrived. Pruning is reported once the consumer reaches what it pruned: a file's statistics once the consumer reaches the file, a unit's drop and page filter once it reaches the unit.

### Filter flags

The always-match decision travels from the unit to the column in the row group's batches (`RowGroupBatches`), and the placement stamps it on each cell it opens beside the file name. A filter-only column in an always-match row group is not read: it holds none of the unit's ranges, places nothing, and its placement seals the cell the plan closes before the row group and ends where the row cap ends it in the row group. The consumer-side rules are in [RECORD_FILTERING.md](RECORD_FILTERING.md#filter-only-column-skip).

## Row limits, failures and close

### Row limits

`maxRows` (from `head(n)`, and `n + head` for a filtered `skip(n)`, see [ROW_READER.md](ROW_READER.md)) reaches the read and the batch plan of a row reader. For an unfiltered `skip`, the batch plan's cap also counts the residue the reader drops, where the read returns it. It is enforced at four levels:

| Level | Where | Unfiltered read | Filtered read |
|---|---|---|---|
| Listing | the planner's row budget (`RowGroupPlanner`) | Stops listing row groups and files once the budget is covered, so every fetch the read starts is needed | Counts the rows of row groups statistics prove to match and stops listing once they cover the budget; the first row group statistics leave undecided lifts it for the rest of the read |
| Pages | the unit's share of the budget ([FETCH_PLANNING.md](FETCH_PLANNING.md#row-limits)) | Truncates the last unit's pages where their rows are known from the OffsetIndex; a scan stops once its column's placement has placed its last row | As unfiltered while the listing budget holds; no truncation once it is lifted |
| Batch plan | `BatchPlan` | Ends the last batch at the cap; every column publishes it and finishes | Holds the cap while every row group with kept rows is an always-match one; drops it for the rest of the read at the first undecided one |
| Reader | Match counting | — | Counts matches against the cap |

A column whose placement reaches the cap takes no further page; once every column's has, the read plans nothing more. A page is the smallest decode unit: a page that straddles the cap is decoded and filled up to the cap. The column readers carry no row cap (#433).

Tests: `BatchPlanTest`, `ColumnPlacerTest`, `AlwaysMatchingRowGroupTest`, `MultiFilePlanningTest`, `S3SelectiveReadJfrIT` (s3).

### Failures

A read fails from the view of its rows, independently of which job ran first:

1. **One failure point.** The consumer gets every row before it, on every reader, then the failure.
2. **A failure belongs to the first row the failed work served.** A page's or a range's failure belongs to the page's row, a chunk scan's to the first row it has not passed, a unit's planning to its first row, a footer's, a listing's or a schema check's to its file's first row. The lowest row wins; at a tie, the lowest column. Nothing keyed past it starts.
3. **Work ahead of the consumer fails quietly.** Its failure is reported only once the consumer reaches its row, and never where the read closes or stops before it.
4. **A read is retried once, inside its job, and nowhere else.** A footer that failed ahead stays failed for the read. The reader's `FileMetadataCache` keeps no failed read, so another read of the reader reads the footer again.
5. **An `Error` is the JVM's, not a row's.** It is reported at the consumer's next call, whatever its row, and the read's close throws nothing.
6. **A failure ends the read.** No job starts, everything held is returned, and every waiting consumer wakes.

`ReadFailure` keeps the failure and its row. Once no work at or before that row is pending, the read settles it, and `BatchPublisher` ends each column with it at the first batch whose rows reach it. Where it lies decides the column's open cell (`CellWriter`): at a failure between two units, every column's open cell is sealed and published, or abandoned where the column holds fewer rows than the plan takes before the failure; at a failure inside a unit, the open cell is left, since the batch holding the failure's row is never published. A column places nothing past a chunk whose scan failed. A column a later file lacks reads as nulls and is no failure.

Each failure gains its context before it reaches the exchange (`ColumnContext.withContext`): the column path is the column's own, and the file, row group and page come from the page or the scan it was met at; a failure planning a row group names the column its own code met it in, a page index's or an OffsetIndex's, and the file and row group alone where it carries no context of its own, since planning serves every column. A fill that runs on another page's job, a nested cell's, raises a segment's failure with the segment's own page (`SegmentFillException`), whose context is used instead. A runtime failure is classified as a read failure (`ExceptionContext.asReadFailure`, [EXCEPTION_MODEL.md](EXCEPTION_MODEL.md#crossing-a-thread-boundary)); an `IOException` stays checked and gains the context; an `Error` gains none. Where a `MetadataSource` supplied the footers, a read failure also carries a hint at the supplied footer. `BatchExchange.checkError` rethrows each as it stands, so the reader's signature declares `IOException` and nothing between the read and the reader wraps. A job the executor refuses, as a closed `HardwoodContext` refuses it, fails the read with an `IllegalStateException` whose cause is the refusal. The exception types a caller sees are in [error-handling.md](../docs/content/reference/error-handling.md).

A reader that finds any column's stream ended checks every exchange for an error before reporting the end, so a failure on a column other than the first, a filter-only column included, is not reported as a short read.

Tests: `ReadFailureTypingTest`, `ReadFailurePositionTest`, `SpeculativeErrorTest`, `ColumnReadersTransportFailureTest`, `ContextClosedMidReadTest`, `LaterFileFailureTest`, `LaterFileDataFailureTest`, `PlanningFailureTest`, `FooterReadRetryTest`, `BetweenWorkItemsFailureTest`, `ColumnFailureTest`, `PageRowCountTest`, `ColumnPlacerTest`, `BatchPlanTest`, `BatchExchangeTest`.

### Cancellation and close

`ReadEngine.close()` stops the read under its lock: no job starts after it, no publish runs, and each exchange is finished, so a consumer waiting for a batch finds the end. The close then waits until every job the read started has ended and its result is applied, running the read's queue meanwhile on a context of zero threads, returns every charge still held to R, gives up the footer loads it claimed and did not complete, a footer whose read completed and whose parse had not started among them, so that the next request for such a file loads it afresh, and stops the parent's tracking of the read. Another read's footer load that ends after the close starts nothing in it. A failed read likewise gives up the claimed footer loads past its failure, which no job of it reads, so that the reader's other reads load those files themselves.

The close returns only once the read is quiescent, so the caller can release what the `InputFile` owns (mapped buffers, direct buffers, connections) without a job reading freed memory. It interrupts no thread. A file is therefore never closed under a read in flight, whichever reader closes it. A row reader closes its read before it returns its held and queued batches to the pool (recycling mode), and a column read's scan closes its read likewise. Each reader's close is claimed by one thread (`CloseClaim`), and a closer on another thread, such as the parent reader's, waits until the claimed close has completed.

Tests: `ReaderCloseMidReadTest`, `RowReaderCloseIdempotencyTest`, `ReadYieldTest`, `FilesAheadTest`, `EngineTrackingTest`, `ParentCloseTest`, `ParquetFileReaderCloseTest`, `S3SelectiveReadJfrIT` (s3).

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

- **The look-ahead and the consumer's pace (#1577).** The look-ahead is a fixed number of batch budgets and ignores the consumer's pace. It is the only bound on decoding, so a read whose fetches keep up decodes to it, and its decoded batches can wait long enough to survive a young collection and be promoted. How much a pausing consumer needs ahead is the consumer's to know; a look-ahead derived from the read's own state does not know it.
- **Files ahead and F are not derived from the read's rate.** What a read needs ahead is the requests that hide its latency at its consumer's rate. F, sized for large requests, binds nothing on small files, where the files-ahead count is the only bound; a read closed early has started the footers of up to that many files past its data. The data's reach passes the units listed once F lets the read fetch every listed unit, and the files ahead are then counted from the next file to list.
- **Pages' state made as tracking starts.** A unit's pages get their state (the page's work, its task and segments, its job) when the unit is batched, and the decodes fill it in later, so on a read that batches ahead of its decodes the state is promoted and every fill dirties an old card. It costs young pauses, not CPU, on a read of many small pages.
- **Concurrent first touches of a mapped file.** Pages of one chunk decode on several threads, which fault in the same parts of a mapped file concurrently; the extra faults show where faults are costly.
- **One lock per read.** Every result is applied under the read's one lock. Where a page decodes in tens of microseconds on many cores, the lock may bind before the cores do; nothing measures it.
- **Nothing detects a read that waits for nothing.** A rule of the read that misses a case leaves its consumer waiting on a context with a pool; a context of zero threads fails such a read ([THREADING.md](THREADING.md#a-context-of-zero-threads)).
- **Wide batches.** A batch the plan makes over a whole file holds its own values whatever the bounds; default batch sizing keeps it within the batch byte budget.
- **Files stay open until the reader closes (#100).** A multi-file read releases no file as it moves past it.
- **Schema drift across files (#941).** Every file must match the reference schema for the columns a read touches.
- **`BYTE_ARRAY` width estimate (#899).** Batch sizing guesses a fixed width per `BYTE_ARRAY` value instead of reading the recorded unencoded size.
- **Row caps on the column readers (#433).** `ColumnReaderBuilder` and `ColumnReadersBuilder` have no `head`, so no cap reaches their columns.
