# Fetch planning

Describes which bytes a read requests from its `InputFile`, and in which requests: the order of a read's requests per file and per row group, the gap policy every merge applies, the fetch plans that locate a column's pages in the read's requests, the rules by which the read starts, retries and releases its requests, how far the read plans ahead and which files a multi-file read opens ahead, per-page row masks and the gate that allows them, row limits on the fetch side, and the I/O budget tests hold reads to.

Related documents:

- [INPUT_FILES.md](INPUT_FILES.md): the `InputFile` contract and what a `readRange` costs on the mapped and in-memory backends
- [S3_STORAGE.md](S3_STORAGE.md): the S3 backend and what a `readRange` costs there
- [FILE_METADATA.md](FILE_METADATA.md): parsing the footer and the page index
- [READ_PIPELINE.md](READ_PIPELINE.md): the read that drives the plans (`RowGroupIterator`, its poll and jobs, the decoded columns, the work list, row-group transitions)
- [STATISTICS_PRUNING.md](STATISTICS_PRUNING.md): the CANNOT/MIGHT/ALWAYS decisions that choose rows and pages
- [THREADING.md](THREADING.md): the pool threads that run the read's walk advances and planning, and the virtual threads its requests run on

## Fetch sequence

On a local file up to 2 GB a `readRange` is a zero-copy slice of the whole-file mapping, so a fetch is a slice and reading ahead costs nothing; above 2 GB it is one `FileChannel.map` call ([INPUT_FILES.md](INPUT_FILES.md#local-files-memory-mapping)). On a remote file each `readRange` that is not served locally is a round trip. The order below is the same for both; only its cost differs.

### Per file

| Step | Where | Requests |
|---|---|---|
| Footer | `ParquetMetadataReader.readFooter` | One read of the file's tail, its last 256 KiB or the whole file where it is shorter, which holds the trailing length and magic and usually the footer; a footer longer than the tail costs a second read of the part before it. The leading magic is not read ([FILE_METADATA.md](FILE_METADATA.md#footer-read)). A footer a `MetadataSource` supplies costs the tail's read too, which checks the trailer ([FILE_METADATA.md](FILE_METADATA.md#supplied-footers)). On S3 the suffix `S3InputFile.open` fetches is the tail, so its read costs no request ([S3_STORAGE.md](S3_STORAGE.md)). A file whose bytes are in memory takes no tail ([The file's tail](#the-files-tail)) |
| Row-group pruning on footer statistics | `RowGroupPruning.filterRowGroups` | None. Records, per kept row group, the columns whose bloom filter a leaf would probe |

A file is listed once its footer is loaded, when the read's next unit reaches it, as the slowest column is in the previous file's last work item ([READ_PIPELINE.md](READ_PIPELINE.md#incremental-planning)), or earlier when planning runs ahead of the read ([Planning ahead](#planning-ahead)). Planning reads no bloom filter; which leaves are probed is in [STATISTICS_PRUNING.md](STATISTICS_PRUNING.md#row-groups).

### The file's tail

The footer entry's read of a file's tail (`TailedInputFile`) serves every later read that lies inside it as a slice, without a request: the footer, the page index and the Bloom filters that sit before it, and data. A file up to the tail's size costs that one request. Coalescing cuts a row group's ranges at the tail's start, so no data request crosses it: the part before the start is requested and the part after it is sliced. A page-index window merges no slices across the tail's start either, so of its slices only one that straddles the start is requested across it (`CoalescedRanges`, the one merge both use). The read holds a file's tail until it is done with the file, once every column has passed a work item of a later file, and drops it then, with the file's footer ([Opening files ahead](#opening-files-ahead)), or when it closes; a read of the file from then on is a request again. Each footer started past the next file is charged to R at the tail's size until then ([Opening files ahead](#opening-files-ahead)). The tail's size is an internal property (`hardwood.internal.tailBytes`, 256 KiB by default), read for each file; core's tests set it to the eight bytes of the trailer, so a fixture of a few kilobytes reads as a file larger than its tail.

A file in memory holds every byte while it is open, and a file whose tail the read took holds the tail until it is dropped; the read asks a file what it holds through `HeldBytes` (from which offset, whether for as long as the file is open, a slice without I/O), never by its class, so the tail cut in coalescing, the requests sliced in place and the dropping of a tail treat both alike.

Tests: `FileTailTest`.

### Per row group

A row group's reads happen when the read's planning takes its work item through the steps below, one stage job at a time, as the read plans the work item the slowest column is in, the next, or further ahead ([Planning ahead](#planning-ahead)). Planning builds every decoded column's plans for a work item at once; the work item's state is in [READ_PIPELINE.md](READ_PIPELINE.md#the-work-list). Each read of steps 1 to 4 is a planning request (`PlanningReads`), which a stage demands and the read waits for before the stage runs again, held by the work items it serves; a range inside the file's tail while the read holds it, or of a file in memory, is sliced in place. Planning then inserts the row group's data requests, which start by the read's rules ([Requests](#requests)). A fast `tail` has the read take every kept row group through step 4 before any column starts (`canFastSkipAllRowGroups`), and waits for it on the thread building the reader.

| # | Step | Where | Requests |
|---|---|---|---|
| 1 | Bloom filters of the columns planning recorded for the row group, in a row group statistics left undecided | `IndexWindow.bloomFilters`, then `RowGroupPruning.probeOnArrival` | Fetched for the row group's [index window](#page-index-windows) by the first member that has one: at most one request when the window's filters lie within the gap limit of each other, none when an earlier member fetched them. A filter whose footer entry gives no `bloom_filter_length` is left out of the window and read by `RowGroupBloomFilterSource` when a probe reaches it: one read, and a second when the filter outgrows the header probe. The probe reads every filter through one source, which serves a filter the window fetched from the window and any other from the row group's own planning requests |
| 2 | Dictionaries a filter can prune with, in a row group statistics left undecided: fully dictionary-encoded filter columns whose `EQ`/`IN` leaf statistics and bloom filters left undecided | `RowGroupDictionaryFilterSource` | One per such column, as the filter's fold reaches a leaf it can decide, sized by the gap to the first data page or by a bounded probe, and a second only when the page header declares more than that read held |
| 3 | Page index | `IndexWindow.buffersFor` | The OffsetIndex of every decoded column the row group reads and, under page filtering, the ColumnIndex and OffsetIndex of every filter column. Fetched for the row group's [index window](#page-index-windows) by the first of its members to ask: at most one request per structure when the window's slices lie within the gap limit of each other, none when an earlier member fetched the window, none for a structure no needed chunk has |
| 4 | Mask-capability probe | `PageFormatProbe` | At most one page-header probe per decoded nested column without an OffsetIndex (re-read with a larger window while the header is truncated, up to a bound that marks the file corrupt), none after the first column that closes the gate, and only when the row group's ranges are not all rows or a fast `tail` asks ([page masking](#page-masking)) |
| 5 | Fetch plans | `FetchPlanBuilder.columnPlans` | None |
| 6 | Data | the row group's data requests, inserted into the read (`Sections.insert`) | Every needed range of every decoded column coalesced at once under the [gap policy](#gap-policy) ([Coalescing a row group](#coalescing-a-row-group)), started by the read's rules ([Requests](#requests)) |

Before the first step that reads anything the row group's footer entries address (1 to 4, or 6), planning checks once that its chunks are stored in this file ([FILE_METADATA.md](FILE_METADATA.md)), and then takes the extents of the decoded columns' chunks from the footer and checks them against the file and against each other (`FetchPlanBuilder.extents`): a chunk outside the file, or two that overlap, fail the work item with a `ParquetReadException` naming the column, whichever step reaches it first. The same-file check comes first, since another file's chunk would otherwise fail as lying outside this one. Steps 5 and 6 build on the same extents.

Steps 1 and 2 decide over the bytes the read holds: a filter or dictionary not held yet counts as absent for that evaluation, so a drop it reaches stands, and a keep is evaluated again once the first range it lacked has arrived; the fold keeps each leaf's decision once final, so it reads the same dictionaries, in the same order, as a read that waited for each. A row group its bloom filters drop reads no dictionary. A row group its bloom filters or dictionaries drop gets `FetchPlan.EMPTY` for every column, and steps 4 to 6 read nothing for it; step 3 reads nothing for it unless another member of its window asks. The dictionaries read in step 2 stay in the row group's `SharedRowGroupMetadata` until every column has released the row group, and the fetch plans decode with them, so a dictionary page pruning read is not fetched again. How the bloom filters and dictionaries refine the decisions planning recorded is in [STATISTICS_PRUNING.md](STATISTICS_PRUNING.md#row-groups).

Page filtering applies when the read has a filter and metadata filtering is on, in a row group statistics left undecided. In a row group they proved to match in full every value of every page matches, so no page bound can rule a page out; its ColumnIndex is neither fetched nor evaluated, and its skipped filter-only columns need no index at all. `RowGroupIndexBuffers.forColumn` fails for a column outside the needed set, so a column the read forgot to ask for is not mistaken for one without an index.

### Page-index windows

Planning a file splits its work items into windows of consecutive row groups (`IndexWindow.plan`). A window holds three structures: the ColumnIndex slices, the OffsetIndex slices and the bloom-filter slices its members need. Step 1 fetches a window's bloom filters together, step 3 its page index. A window takes as many row groups as fit the bytes it fetches, gaps included, within a budget, and at least one, so a file whose page index and bloom filters fit the budget is read in one window. The budget bounds what a window holds, since a large file's page index reaches hundreds of megabytes; a read that stops early has fetched at most one budget of index and filters it does not use. A window is fetched only when one of its members asks, never at planning, and each structure only when a member needs it: a window whose members are all dropped by their bloom filters or dictionaries fetches no page index. Filters merge only as far as the budget allows: since a window holds at least one row group, a read whose bloom filters approach the budget takes a window per row group and one request per filter. Before it forms any window, planning checks every slice its work items need against the file: a ColumnIndex, OffsetIndex or bloom filter the footer places outside the file fails the read with a `ParquetReadException` naming the column and the structure. A bloom filter without a declared length is not in a window; a probe reads it and checks it against the file as it does.

Writers commonly lay the page index out by structure, then row group, then column: all ColumnIndexes, then all OffsetIndexes. A window merges the slices of each structure with each other under the [gap policy](#gap-policy), never one structure's with another's: the gap between a window's last ColumnIndex slice and its first OffsetIndex slice holds the ColumnIndexes of every later row group, which the following windows fetch themselves. Writers commonly place the bloom filters of all row groups together too, row group by row group, so a window's filters of one column are strided by the other columns' filters and merge within the gap limit. Consecutive windows therefore fetch disjoint bytes, and a read fetches no byte of the page index twice. Within a structure one column's slices are strided by the columns the read does not use, and a gap wider than the gap limit splits the request.

A window's requests are planning requests every member holds, inserted as its file is listed and started together when planning first needs a structure of them: the bloom filters for the first member that probes one, the page index for the first member its bloom filters and dictionaries keep (#1659). The read plans its next unit whatever F holds, so a window the next unit belongs to is in flight before a column reaches it. A request that fails twice ends planning at the member that needed it ([Planning ahead](#planning-ahead)). A window drops its bytes once every column has passed all its members, and every window drops them when the read closes. The windows holding bytes therefore run from the one the slowest column is in to the one of the last row group planning has planned ahead ([Planning ahead](#planning-ahead)).

**Data coalescing never crosses a row group.** Every merge of data ranges is computed from one row group's columns, so the requests of a row group are released with it and do not keep a neighbour's alive. Untested. Page-index windows cross row groups and hold their bytes until their last member is released.

Tests: `RowGroupIndexBuffersTest`, `IndexWindowTest`, `OutsideFileTest`, `IndexWindowIoTest`, `IndexWindowLifecycleTest`, `BloomFilterWindowIoTest`, `DictionaryPushDownIoTest`, `PageFormatProbeTest`, `S3SelectiveReadJfrIT` (s3).

## Gap policy

Two byte ranges are fetched as one request when the gap between them is at most G (`CoalescingPolicy.GAP_BYTES`) and the merged span at most S (`CoalescingPolicy.maxSpanBytes`). `CoalescingPolicy.merges` holds the rule, and every merge calls it:

| Merge | Where |
|---|---|
| A row group's needed data ranges, of every column at once, into its data requests | `CoalescingPolicy.coalesce` |
| Page-index and bloom-filter slices of one structure | `CoalescedRanges` |

A merge bridges the gap between two needed ranges. It never extends a request before the first needed byte or past the last one. A range longer than the span limit is a request of its own.

The gap limit is the serial break-even of a remote request: the bytes that take as long to transfer as one round trip (latency × bandwidth), so fetching a gap costs no more than issuing a second request. The span limit keeps each `readRange` bounded, which lets read-ahead overlap decode and lets a read that stops early leave the rest unfetched. The best gap depends on the storage and on how many requests run in parallel; the rule lives in one class so that a storage-dependent value replaces one constant. The span limit has a tuning override (`hardwood.internal.maxCoalescedBytes`), read once per read, which is not a public option; a value that is not a positive byte count is rejected with `IllegalArgumentException`.

Tests: `CoalescingPolicyTest`, `CoalescedRangesTest`, `PageRangeIoTest`, `DictionaryPrefixFetchTest`.

## Fetch plans

A `FetchPlan` is one decoded column's view of one row group: `isEmpty()` and a `PageWalk` over `PageInfo`s in file order. The read walks whichever plan a column has without knowing its type, and the only choice between the two page-reading plans is whether the column chunk has an OffsetIndex. A plan reads its bytes from the row group's requests that overlap its column chunk, a list fixed when the row group is planned, through a `SectionCursor` per walk. It sees each request as a `Section` (its range, its bytes when present, a slice of them, its release) and finds the row group's requests through `Sections`, which the read implements. A plan built outside a read, such as a tool's page walk, reads through `InPlaceSections`, which slices bytes the file holds and reads any other range on the thread that demands it.

**A walk never waits for I/O.** A `PageWalk` is a state machine over the bytes its sections hold: `advance()` returns the next page, or `null` with `pending()` naming the section the next page needs while its bytes are not present, or `null` at the end. The walk issues no read. The walk keeps the state it reached, a sequential walk's parsed page header included, so the next `advance()` resumes there, parses nothing twice and reads no byte twice. A walk advance, a job of the read, runs the walk until the column holds as many pages as it may or the walk stops; the read demands the pending section and runs the column's walk again once it has arrived ([THREADING.md](THREADING.md#invariants)). A walk over a file whose bytes are in memory never stops for bytes. Tests: `ReadYieldTest`.

**A walk reads forward.** A range that lies in one request is a zero-copy slice of it. A range that straddles the cut between two requests, a page or a header peek, is copied from every request holding a byte of it into one direct buffer, once all of them are present, so a copy is never left part done; this happens at most once per cut. No read starts before the header of the page being read, so the walk passes every request that ends at or before it, and every request it holds once it ends ([Release](#release-failure-and-close)).

| Plan | Chosen when | Pages | I/O at plan time |
|---|---|---|---|
| `IndexedFetchPlan` | the chunk has an OffsetIndex | Listed from the OffsetIndex at plan time, filtered and truncated | None |
| `SequentialFetchPlan` | the chunk has no OffsetIndex | Discovered by walking page headers through the requests that hold the chunk | None |
| `FetchPlan.EMPTY` | the filter's row ranges touch no page of an indexed column, the row ranges are empty, or the row group's dictionaries dropped it | None | None |
| `SkippedColumnFetchPlan` | a filter-only column in a row group statistics proved to match in full ([RECORD_FILTERING.md](RECORD_FILTERING.md#filter-only-column-skip)) | One boundary marker | None |

**A `PageInfo` carries resolved bytes.** The plan's walk resolves a page's bytes before it hands out the `PageInfo`, with the header it parsed (`DataPage`), so a decode does no I/O and parses no header again. A `PageInfo` holds no request. Its page slice keeps the request's buffer alive after every column has passed the request, until the decode and every fill that reads the page's [stored values](VALUE_DECODE.md#page-decode) in place are done with it. Untested.

**Empty row ranges read no page.** When no row of the row group survives page filtering (a leaf's column index rules out every page, or an `AND` intersects disjoint ranges), its `RowRanges` are empty (`RowRanges.isEmpty()`), and every decoded column gets `FetchPlan.EMPTY`: an indexed column through its needed pages, after reporting its skipped pages, and a column without an OffsetIndex directly, without a sequential walk. The mask gate is not probed, because no plan applies a mask.

### Indexed plans

`FetchPlanBuilder.columnPlans` builds an indexed plan in three steps:

1. **Needed pages.** Every page, each with `PageRowMask.ALL`, when the row ranges are all rows. Otherwise the pages whose rows `[firstRowIndex, next page's firstRowIndex)` overlap the matching `RowRanges`, each paired with the mask `RowRanges.maskForPage` gives it. No needed page yields `FetchPlan.EMPTY`, and nothing is fetched for the column. An OffsetIndex listing no page for a chunk with values is rejected at parse ([FILE_METADATA.md](FILE_METADATA.md#page-index)), so `FetchPlan.EMPTY` means only that the filter or the dictionaries left no needed page.
2. **Row limit.** On an unfiltered read the list is truncated to the pages starting before the row group's share of `maxRows` ([row limits](#row-limits-and-first-parts)).
3. **Dictionary.** The dictionary page spans from the chunk's first page to the first data page the OffsetIndex lists, and is a needed range of its own unless pruning has read the dictionary.

The needed pages and the dictionary page are the column's needed ranges, which the row group coalesces with every other column's ([Coalescing a row group](#coalescing-a-row-group)). An OffsetIndex page location's size includes the page header, so a page's slice is its header and body and the indexed path parses no header to find pages. The chunk's first page is at `dictionary_page_offset`, or at `data_page_offset` when the file declares none, as parquet-mr 1.12 does; a dictionary page declared after the first data page is rejected as malformed (`DictionaryParser.firstPageOffset`).

The plan parses its dictionary on the first advance of its walk, and then takes each page from the request that holds it.

### Sequential plans

A sequential plan knows the chunk's byte range, and discovers its pages as its walk reads the headers. The range starts at the chunk's first page, or where a dictionary pruning has read ends. Its needed range is the whole chunk, cut into pieces of at most S, or under a row limit the chunk's first part ([row limits](#row-limits-and-first-parts)); a walk that reads past the first part inserts the rest of the chunk, from where it reads to the next request or the chunk's end and at most S at a time, as a request the walk stops on and the read starts at once (`SectionCursor.Rest`, `Sections.insertDemanded`), so a read the limit covers fetches nothing more and a range a request holds is never requested again. A header is read with a bounded peek that grows while the header is truncated, up to a limit that marks the file corrupt; the page-format probe and the dictionary header read outside a read take theirs the same way, through the walk's `SectionCursor`, the planning's requests or the file, as a `ByteSource` (`PageFormatProbe.readHeader`). The page under way (its parsed header, its peek, its record count under masks and its inline-statistics decision) is held in fields until the walk moves past it, and the counters below move only then, so a walk that stops on a read in the middle of a page resumes it.

The walk skips non-data pages. For each data page it decides, in order: drop the page when its row mask is empty, without reading its body; emit a null placeholder when inline page statistics prove no row matches ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md#inline-page-statistics)); otherwise emit the page with its mask. It stops when the row limit is covered ([row limits](#row-limits-and-first-parts)) or the cursor has passed the last matching row.

**Counters.** `valuesRead` accumulates every data page's `num_values` and, under masks, `recordsRead` every data page's record count, whether the page was kept, dropped or replaced. A walk that reaches the end of the chunk checks `valuesRead` against the chunk's `num_values` and, under masks, `recordsRead` against the row group's row count, and raises `ParquetReadException` on a mismatch. A walk that stops early at the row limit or past the last matching row skips both checks.

Tests: `PageRangeIoTest`, `DictionaryPrefixFetchTest`, `SequentialFetchPlanEarlyExitTest`, `SequentialChunkCrossingIoTest`, `SectionCursorTest`, `ReadRequestsTest`, `S3SelectiveReadJfrIT` (s3).

### Coalescing a row group

A row group's data requests are built from every decoded column's needed ranges at once (`CoalescingPolicy.coalesce`): page groups, the dictionary page, a column without an OffsetIndex whole or its first part. Sorted by offset, a range joins the request before it when the gap policy merges it, so one request may serve the pages of several columns across a gap of at most G, and is at most S long unless one range alone is longer. Each request is held by every column whose chunk extent overlaps it, and its fetch reason names every column whose range it holds ([fetch reasons](#io-budget)).

A row group whose rows are all read and that no Bloom filter or dictionary can drop (`Planner.readsAllRows`: no page filtering, no tail or row limit cutting it, its chunks in this file) has its data requests built from its chunks' extents in the footer instead (`FetchPlanBuilder.chunkSpans`), cut into pieces of at most S and coalesced the same way, before its page index is read, so they can start while it is read. Its plans then locate their pages in those requests; a page that straddles a cut is copied as above.

**Validation.** Before coalescing, each needed range is checked against its column chunk's extent and each chunk's extent against the file's length, and the extents of the columns the read reads must not overlap. A range or extent that fails is a `ParquetReadException` naming the column, so a request two columns share never fails for a column whose ranges were valid.

A request of a file whose bytes are in memory, or inside the file's tail while the read holds the tail, is sliced in place as it is inserted: it starts no read and is charged to neither F nor R, since the tail is charged already.

Tests: `CoalescingPolicyTest`, `CrossColumnCoalesceTest`, `ReadRequestsTest`.

## Requests

Every read a read makes of a file whose bytes are not in memory is a request of its `RowGroupIterator`, run as a job on a virtual thread of the context's I/O executor: the data requests planning inserts for a work item, each a range of the file at a position by file, then work item, then offset; the rest of a chunk a walk reaches past a row limit's first part; the planning requests of steps 1 to 4 ([Per row group](#per-row-group)); and the footer reads ([Opening files ahead](#opening-files-ahead)). A read issues no other data request. The read starts each request by one of these rules:

| Rule | A request | F and R |
|---|---|---|
| Demand | a walk or a planning stage stopped on, and the rest of a chunk a walk reached, which starts at the read's next poll | neither |
| The next unit | the first data request, in position order, of the work item after the one the slowest column is in | neither |
| Speculative | every other data request, in position order, while the read's started speculative bytes stay within F and R has room | both, until released |

A planning request starts only when a stage demands it. F (`hardwood.internal.fetchAheadBytes`, twice S by default) bounds the speculative bytes one read holds; R is the context's read-ahead buffer ([VALUE_DECODE.md](VALUE_DECODE.md#page-decode)), which every read on the context shares. Nothing waits for R: a request that does not fit starts on a later release, which runs the read's poll again, and a release runs its waiters in registration order, decodes and requests alike. An idle read holds what it has started, at most F, until it is released or closed. A read with a row limit has an F of zero and starts no speculative request: it may need no row past its next unit.

The next unit is the look-ahead every read keeps whatever F and R hold. Speculation reaches no request of the next row group when the read has a row limit, when other reads hold R, or when the current row group's own requests fill F, as they do in any row group larger than F; in each case the next unit's first request is in flight when the slowest column reaches the row group, so moving to it costs no round trip. It is charged to neither F nor R: like a demanded request, it keeps the read moving, so it waits for no room another read holds; the read starts one such request, of at most S bytes, for each work item its slowest column moves to.

The speculative requests and the footers read ahead wait in one position order: by file, then work item, then offset, a file's footer ahead of its data. A walk asks for a request's bytes without waiting (`Section.poll`), and the read starts a request the walk stops on as demanded. The read's lock guards the requests and their positions and is held across no I/O; a walk polls without it.

Tests: `ReadYieldTest`, `ReadRequestsTest`, `ReadAheadTest`, `SequentialNextRowGroupPrefetchTest`.

### Release, failure and close

A data request is released once every column whose chunk extent overlaps it has passed it; a column with no pages in the row group, or one skipped, does not hold it. Its charges then go back to F and R and its bytes are dropped, except as a page slice holds them; that remainder is uncharged and at most S per column. A column whose placement ends at the row cap stops its walk where it is, which passes every section the walk holds, and passes every work item after it. Once every column has passed a work item, the read forgets its requests: each is released, whatever holders it has left, and a read of it still in flight drops its bytes as it arrives. The read therefore holds the requests of the work items its columns have not passed, and no others. A planning request is released once each work item holding it has been released.

A request whose read fails with an exception is read once more, started at once and uncharged, whether or not a walk or a stage waits for it; planning requests and the rest of a chunk alike. A second failure is final: the request fails with an `IOException` naming the file, offset and length ([EXCEPTION_MODEL.md](EXCEPTION_MODEL.md#crossing-a-thread-boundary)), which a walk that reaches it raises as its failure and a planning stage as a planning failure ([Planning ahead](#planning-ahead)). A request whose read fails with an `Error` is not read again, and its `Error` goes the same way: a walk that reaches the request, or waits for it, raises it as its column's failure, and a planning stage as a planning failure; an `Error` the read never raised is thrown once by its close ([THREADING.md](THREADING.md#invariants)). A failure of a read no walk or stage waits for is logged at `DEBUG` as it fails.

The read's close returns the charges of every request, discards its planning requests, and waits for the reads in flight without interrupting them ([READ_PIPELINE.md](READ_PIPELINE.md#cancellation-and-close)).

Tests: `ReadYieldTest` (a failed read read again, a read that fails twice, a walk holding no thread while its read is held, its close and a closed context), `RequestRetentionTest`, `SpeculativeErrorTest`, `SectionCursorTest`, `IndexWindowLifecycleTest`.

### Planning ahead

The read plans its work items one planning job at a time, in work-list order: a listing of the next file, or one stage of the next work item ([Per row group](#per-row-group)). A stage that lacks bytes returns what it lacks, a planning request the read then demands, or another need, and the read runs the stage again once it has arrived; each stage keeps what it has decided, so a stage run again parses nothing twice. The read applies a stage's result under its lock, and a work item counts as planned once its fetch plans and batches are made.

How far it plans: the work item the slowest column is in and the next unit, whatever F holds, and further while the bytes of the data requests it has inserted and not started stay below F. It lists the next file under the same bound, once that file's footer is loaded. A read with a row limit, whose F is zero, plans only the slowest column's work item and the next. A read whose files are all in memory holds no data request that waits to start, so F does not bound its planning: it plans one work item past the slowest column's on a pool, and on a context of zero threads none past the furthest work item a column has entered, so that its walks have each work item planned as they reach it. Before the columns start, a question a reader asks of the read (whether a work item is live, the skip's residue, whether every work item takes a page mask) is answered by planning jobs that run while the caller waits, as far as the answer needs ([READ_PIPELINE.md](READ_PIPELINE.md#incremental-planning)).

A failure listing a file or taking a work item through a stage ends planning there: no file is listed and no work item planned from the failed one on, and every column that reaches it ends with the same instance, a planning request's final failure included. Once the read is closed, it plans nothing more.

Tests: `SequentialNextRowGroupPrefetchTest`, `ReadRequestsTest`, `IndexWindowLifecycleTest`, `DictionaryPushDownIoTest`, `LiveWorkItemTest`, `PlanningFailureTest`, `InMemoryPlanningAheadTest`, `S3SelectiveReadJfrIT` (s3).

### Opening files ahead

A multi-file read loads each file's footer as a job of its own, from the reader's `FileMetadataCache`: the footer read opens the file and reads its tail, or for a `MetadataSource` takes the footer it supplies and checks it against the file, and the parse completes the cache's entry. The read has reached the file of the furthest work item a column has entered or of its next unit, or, for a work item not listed yet, the file it lists next; before the columns start, the file a question has listed or probed to. The footers through the file after the one reached start at once and uncharged, and so does the footer of a file the read has passed, as when statistics prune a run of files. A multi-file read of files not all in memory and without a row limit also queues the footers of the files after that, up to `hardwood.internal.maxFilesAhead` past the one reached (64 by default, read for each read), in position order behind the read's data of earlier files ([Requests](#requests)): each starts while it fits R, charged at the tail's size from its start until the read is done with its file ([The file's tail](#the-files-tail)), its load completes without a read of its own or fails, or the read closes. A read whose planned data fills F therefore reads no footer past the next, and a footer takes no room the read's nearer data waits for. A footer that does not fit starts on a later release of R. Footers are outside F. The cap on files ahead bounds what F and R do not bound for small files. A read with a row limit, or of files all in memory, reads only the next file's footer ahead: it may need no further file, and under a filter it learns only downstream that its limit is reached. Planning lists a file only once its footer is loaded, so the files it plans ahead are bounded by the files ahead and by R, also where their data lies in their tails and counts toward no F. A run of files statistics prune is listed while the footers of the files after it are in flight. A planning failure stops the footers queued ahead, since no file after the failed one is listed.

The read claims each footer from the reader's `FileMetadataCache` (`ReadPlanning.claimFooter`), so every read of the reader shares it: a footer loaded, or whose load failed, is taken as it stands; a load another read or a caller runs is waited for without a thread, and the footer claimed again once it completes; otherwise the read loads it. A footer whose file's bytes are in memory, and which is read from the file or on a context of zero threads, is read in place by its parse; any other is read on a virtual thread, where a `MetadataSource` that reads the footer from the file also runs, and parsed by a planning job of the read on its executor. A footer read that fails with an exception is read once more, at once and uncharged; a second failure, or an `Error`, completes the entry with the failure, and the read raises it when its listing reaches the file, so a file ahead that cannot be read fails the read only there ([THREADING.md](THREADING.md#invariants)). An `Error` no listing raised is thrown by the read's close. A footer whose read completed is parsed even once the read is closed, so the reader's other reads find its load complete; a load the read claimed and did not complete is given up by its close, and the next request for the file loads it afresh. A read drops the footer of each file it is done with, with the file's tail, but the first file's, which the reader keeps, so a read of many files holds the footers of the files it is reading; a later read reads a dropped footer again. A file's `FileOpenedEvent` begins when its footer's load opens the file and is committed by the parse.

Tests: `FilesAheadTest`, `LaterFileFailureTest`, `FooterReadAheadRetryTest`, `PlanningEventTimingTest`, `MultiFilePlanningTest`, `ReadRetentionTest`, `FileMetadataCacheTest`.

## Page masking

A `PageRowMask` selects the records of one page to keep, as sorted, non-overlapping `[start, end)` intervals relative to the page's first row, counted in values for a flat column and in top-level records (repetition level 0) for a nested one. `PageRowMask.ALL` keeps the page whole; a page with no matching row gets no mask and is not read. Masks come from the row group's `RowRanges`, which are the column-index result under a filter ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md#column-index)) or the range `[residue, numRows)` a fast `tail` sets on the first work item ([ROW_READER.md](ROW_READER.md#tail)). A nested column's decode trims the page to its kept records before measuring it, so nested placement and filling see only kept records ([NESTED_DECODE.md](NESTED_DECODE.md#masked-pages)); a flat column's fill reads each kept interval.

### The mask-capability gate

**Per-page masks apply to every decoded column of a row group or to none.** Columns have their own page boundaries; a column that ignored the masks its siblings applied would place rows its siblings dropped, and the batches would misalign. A column is mask-capable when it:

- has an OffsetIndex (its plan is indexed, and the OffsetIndex gives each page's first row); or
- is flat, so a sequential plan takes the record count from the page header's `num_values`; or
- is nested with `DATA_PAGE_V2` pages, whose repetition levels sit uncompressed ahead of the body and are counted without the codec (`PageRecordCounter`).

A nested column without an OffsetIndex whose pages are `DATA_PAGE` (v1) holds its repetition levels inside the compressed body, so counting its records would mean decompressing pages the mask exists to skip. Such a column closes the gate for the whole row group: the plans stage promotes the row group's ranges to all rows, so no plan applies a mask, and the filter falls to row-group statistics and per-row evaluation. A fast `tail` is not taken for the read ([ROW_READER.md](ROW_READER.md#tail)).

`FetchPlanBuilder.masksApplicableForRowGroup` decides the gate at most once per row group, over the bytes its probe holds, returning the range it lacks until they have arrived; the work item's planning state keeps the decision. The plans step asks for it only when the row group's ranges are neither all rows nor empty, and the tail path (`canFastSkipAllRowGroups`) has the read decide it for every row group, before the tail range exists, so an unfiltered read without a tail never probes. The v1/v2 question is answered by `PageFormatProbe`, which reads the page header at `data_page_offset` and accepts it only if it is a data page ending within the chunk; otherwise (some writers understate the offset or point it at the dictionary page) it walks from the chunk start past the dictionary page. A chunk with consistent offsets costs one request. Writers do not mix page versions within a chunk, so one header decides the chunk. A row group its dictionaries drop needs no gate, and a file whose chunks all have an OffsetIndex pays nothing.

### Sequential plans under a mask

With masks active, the sequential walk counts each data page's records (from `num_values` for a flat column, from the repetition-level prefix for a nested v2 one) and asks `RowRanges.maskForPage` for the page's mask. A page with no match is skipped without reading its body; its bytes may have been fetched as part of the chunk, so the saving is decompression and decode. A null placeholder under a partial mask stands for the masked record count, so sibling columns agree on row counts. Once `recordsRead` reaches `RowRanges.endRow()` (`0` for empty ranges, which planning does not hand a sequential plan), every remaining page would get no mask, and the walk stops without reading further headers. Reaching a v1 page on a nested column under a mask raises `IllegalStateException`, since the gate should have prevented it.

Tests: `MisalignedPageBoundariesTest`, `MisalignedPageBoundariesNoIndexTest`, `NestedV2NoIndexMaskingTest`, `NestedV1NoIndexFallbackTest`, `SequentialFetchPlanEarlyExitTest`, `EmptyPageRangesUnindexedColumnTest`, `PageFormatProbeTest`, `MaskProbeIoTest`.

## Row limits and first parts

`maxRows` narrows fetches only on an unfiltered read. With a filter, `head(n)` counts matching rows, which a scan may find anywhere, so every surviving page stays fetchable and the cap is enforced downstream ([READ_PIPELINE.md](READ_PIPELINE.md#row-limits)). Without one, each row group gets the remainder of the budget after the rows planned before it (`perRgMaxRows`, plus the skip residue on the first work item), since page row indices restart at 0 in every row group; earlier row groups are covered whole and only the last one is cut.

| Plan | Effect of the row group's share `m` |
|---|---|
| `IndexedFetchPlan` | Keeps the needed pages whose first row is below `m`; its needed ranges cover only those |
| `SequentialFetchPlan` | Needs the chunk's first part, sized from `m`, and inserts the rest as its walk reaches it. A flat column stops yielding pages once their values cover `m` rows; a nested column, whose record count is inside the compressed page, yields its pages until the column's placement reaches the cap |

A page is the smallest fetch and decode unit: a page that straddles the limit is fetched and decoded whole.

**First part of a chunk without an OffsetIndex** (`SequentialFetchPlan.computeChunkSize`, capped at the chunk's length):

| Case | First part |
|---|---|
| No row limit | the whole chunk, in pieces of at most S |
| Row limit, small chunk (below a fixed threshold) | the whole chunk |
| Row limit, larger chunk | average compressed bytes per value × `m` × a safety factor, between a fixed floor and S |

The estimate trades a possible second request for not fetching a large chunk whose head is all the read needs.

Tests: `SequentialFetchPlanChunkSizeTest`, `S3SelectiveReadJfrIT` (s3).

## I/O budget

`IoBudget` (core tests) computes, from the footer and the OffsetIndex alone and independently of the reader, the bytes a read of given columns and rows has to fetch: for every row group holding a requested row, each column's dictionary page and the data pages whose rows intersect the requested rows, or the whole chunk where the column has no OffsetIndex. A read is within budget when every data byte it fetches lies in a needed range or in a gap of at most `CoalescingPolicy.GAP_BYTES` between two, and no data byte is fetched twice. Data bytes are those inside a column chunk. Reads issued under a `pruning` fetch reason (dictionaries and bloom filters read to decide whether to read a chunk) are left out; the pruning tests pin their cost.

`CountingInputFile` wraps an `InputFile` and records every `readRange` with its offset, length and the `FetchReason` in effect. A change to the fetch path asserts its effect against the budget.

**Fetch reasons.** `FetchReason` is a per-thread tag each read runs under, for S3 fetch logs and for the budget. A request's read, a data or planning request's, runs under the request's own reason.

| Reason | Read |
|---|---|
| `footer-info`, `footer-body` | footer |
| `rg=N pruning`, `rg=A-B pruning` | the bloom filters of a window over row groups `A` to `B`; a row group's dictionaries, and a bloom filter its window did not fetch |
| `rg=N indexes`, `rg=A-B indexes` | a page-index window over row groups `A` to `B`, and the mask probe of row group `N` |
| `rg=N col=a col=b` | a data request of row group `N`: one `col=` per column whose ranges it holds, by name, in file order |

Tests: `CrossColumnCoalesceTest`, `PageRangeIoTest`, `DictionaryPrefixFetchTest`, `ReadRequestsTest`.

## Boundaries

- **Sequential chunks ignore row ranges (#1025).** A sequential plan under a mask fetches the chunk and skips unmatched page bodies after the fact.
- **Page-level skip for the `skip` residue (#381).** The residue of a physical `skip` is fetched and decoded, although a page mask could drop its leading pages as it does for `tail`.
- **Fast `tail` on nested v1 without an OffsetIndex (#1306).** Such a column closes the mask gate, so `tail` decodes and discards the residue.
- **Mask probe on bytes already fetched (#1354).** The mask-capability probe issues its own read, although the page headers it needs lie in bytes the read fetches anyway.
- **A storage-dependent gap (#763, #827).** The gap limit is one constant for every backend.
