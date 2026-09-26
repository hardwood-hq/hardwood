# Fetch planning

Which bytes a read requests from its `InputFile`, and in which requests: the fetch sequence per file and per row group, the gap policy every merge applies, the fetch plans that locate a column's pages, chunk handles and their prefetch, cross-column coalescing, per-page row masks and the gate that allows them, row limits on the fetch side, and the I/O budget tests hold reads to. The `InputFile` contract and what a `readRange` costs on each backend are in [INPUT_FILES.md](INPUT_FILES.md) and [S3_STORAGE.md](S3_STORAGE.md); parsing the footer and the page index is in [FILE_METADATA.md](FILE_METADATA.md). How the pipeline drains the plans (`PageSource`, `ColumnWorker`, the work list, row-group transitions) is in [READ_PIPELINE.md](READ_PIPELINE.md), and the CANNOT/MIGHT/ALWAYS decisions that choose rows and pages are in [STATISTICS_PRUNING.md](STATISTICS_PRUNING.md).

## Fetch sequence

On a local file up to 2 GB a `readRange` is a zero-copy slice of the whole-file mapping, so a fetch is a slice and a prefetch costs nothing; above 2 GB it is one `FileChannel.map` call ([INPUT_FILES.md](INPUT_FILES.md#local-files-memory-mapping)). On a remote file each `readRange` that is not served locally is a round trip. The sequence below is the same for both; only its cost differs.

### Per file

| Step | Where | Requests |
|---|---|---|
| Footer | `ParquetMetadataReader.readFooter` | Two reads: the trailing length and magic, then the footer body; the leading magic is not read ([FILE_METADATA.md](FILE_METADATA.md#footer-read)). On S3 the suffix read `S3InputFile.open` issues serves both when the footer lies inside it ([S3_STORAGE.md](S3_STORAGE.md)) |
| Row-group pruning on footer statistics | `RowGroupIterator.filterRowGroups` | None |
| Bloom filters | `RowGroupBloomFilterSource`, during the same planning | One read per row group footer statistics did not drop and per column whose `EQ`/`IN` leaf statistics left undecided (a second when a writer omitted `bloom_filter_length` and the filter outgrows the header probe). The reads are not merged with each other |

A file is planned when the first column enters the previous file's last row group, one row group ahead of the read ([READ_PIPELINE.md](READ_PIPELINE.md#incremental-planning)). Which leaves are probed is in [STATISTICS_PRUNING.md](STATISTICS_PRUNING.md#row-groups).

### Per row group

A row group's reads happen when the first decoded column reaches it, or earlier when the previous row group prefetches it ([next row group](#next-row-group-prefetch)). `computeFetchPlans` builds every decoded column's plans for a work item at once; caching and eviction are in [READ_PIPELINE.md](READ_PIPELINE.md#the-work-list). Planning reads metadata only; data pages are read later, on demand. A fast `tail` runs steps 1 to 3 before any column reaches a row group: `canFastSkipAllRowGroups` requests each kept row group's shared metadata on the thread building the reader, row group by row group until one closes the [mask gate](#the-mask-capability-gate).

| # | Step | Where | Requests |
|---|---|---|---|
| 1 | Dictionaries a filter can prune with: fully dictionary-encoded filter columns whose `EQ`/`IN` leaf statistics and bloom filters left undecided | `computeSharedMetadata` → `RowGroupDictionaryFilterSource` | One per such column, sized by the gap to the first data page or by a bounded probe, and a second only when the page header declares more than that read held |
| 2 | Page index | `IndexWindow.buffersFor` | The OffsetIndex of every decoded column the row group reads and, under page filtering, the ColumnIndex and OffsetIndex of every filter column. Fetched for the row group's [index window](#page-index-windows) by the first of its members to ask: at most one request per structure when the window's slices lie within the gap limit of each other, none when an earlier member fetched the window, none for a structure no needed chunk has |
| 3 | Mask-capability probe | `PageFormatProbe` | At most one page-header probe per decoded nested column without an OffsetIndex (re-read with a larger window while the header is truncated, up to a bound that marks the file corrupt), none after the first column that closes the gate, and only when the row group's ranges are not all rows or a fast `tail` asks ([page masking](#page-masking)) |
| 4 | Fetch plans | `computeFetchPlans` | None |
| 5 | Data | the plans' `ChunkHandle`s and `SharedRegion`s | As few as the [gap policy](#gap-policy) and the plan shapes allow, issued as the column's retriever advances |

A row group its dictionaries drop gets `FetchPlan.EMPTY` for every column, and steps 3 to 5 read nothing for it; step 2 reads nothing for it unless another member of its window asks. The dictionaries read in step 1 stay in the row group's `SharedRowGroupMetadata` until every column has released the row group, and the fetch plans decode with them, so a dictionary page pruning read is not fetched again. How the dictionaries refine the decisions planning recorded is in [STATISTICS_PRUNING.md](STATISTICS_PRUNING.md#row-groups).

Page filtering applies when the read has a filter and metadata filtering is on, in a row group statistics proved to match as well: a ColumnIndex that disagrees with the chunk statistics still narrows the rows. `RowGroupIndexBuffers.forColumn` fails for a column outside the needed set, so a column the read forgot to ask for is not mistaken for one without an index.

### Page-index windows

Planning a file splits its work items into windows of consecutive row groups (`IndexWindow.plan`), and step 2 fetches a window's slices together. A window takes as many row groups as fit the bytes it fetches, gaps included, within a budget, and at least one, so a file whose page index fits the budget is read in one window. The budget bounds what a window holds, since a large file's page index reaches hundreds of megabytes; a read that stops early has fetched at most one budget of index it does not use. A window is fetched only when one of its members asks, never at planning.

Writers commonly lay the page index out by structure, then row group, then column: all ColumnIndexes, then all OffsetIndexes. A window merges its ColumnIndex slices with each other and its OffsetIndex slices with each other under the [gap policy](#gap-policy), never one structure's with the other's: the gap between a window's last ColumnIndex slice and its first OffsetIndex slice holds the ColumnIndexes of every later row group, which the following windows fetch themselves. Consecutive windows therefore fetch disjoint bytes, and a read fetches no byte of the page index twice. Within a structure one column's slices are strided by the columns the read does not use, and a gap wider than the gap limit splits the request.

A window fetches under one monitor: the first member to ask fetches, the others wait for it, and a failed fetch stores nothing, so the next member to ask fetches again. The fetch runs inside the shared-metadata computation, after the dictionaries, so it holds the lock of that row group's cache entry and takes no further lock but the window's monitor. A window drops its bytes once every column has released all its members, and every window drops them when the iterator closes. The windows holding bytes therefore run from the one the slowest column is in to the one the fastest column's next-row-group prefetch opened.

**Data coalescing never crosses a row group.** Every merge of data ranges is computed in step 4 from one row group's plans, so the data bytes a row group holds are released with its plans and do not keep a neighbour's alive. Untested. Page-index windows cross row groups and hold their bytes until their last member is released.

Tests: `RowGroupIndexBuffersTest`, `IndexWindowTest`, `IndexWindowIoTest`, `IndexWindowLifecycleTest`, `DictionaryPushDownIoTest`, `PageFormatProbeTest`, `S3SelectiveReadJfrIT` (s3).

## Gap policy

Two byte ranges are fetched as one request when the gap between them is at most `CoalescingPolicy.GAP_BYTES` and the merged span at most `CoalescingPolicy.MAX_SPAN_BYTES`. `CoalescingPolicy.merges` holds the rule, and every merge on the data path calls it:

| Merge | Where |
|---|---|
| Needed pages within a column into page groups | `RowGroupIterator.coalescePages` |
| The dictionary page into the first page group | `RowGroupIterator.foldsDictionary` |
| First reads of different columns into a shared region | `RowGroupIterator.coalesceAcrossColumns` |
| Page-index slices of one structure | `CoalescedRanges` |

A merge bridges the gap between two needed ranges. It never extends a request before the first needed byte or past the last one. A range longer than the span limit is a request of its own.

The gap limit is the serial break-even of a remote request: the bytes that take as long to transfer as one round trip (latency × bandwidth), so fetching a gap costs no more than issuing a second request. The span limit keeps each `readRange` bounded, which lets prefetch overlap decode and lets a read that stops early leave the rest unfetched. The best gap depends on the storage and on how many requests run in parallel; the rule lives in one class so that a storage-dependent value replaces one constant. The span limit has a tuning override (`hardwood.internal.maxCoalescedBytes`), which is not a public option.

Tests: `CoalescingPolicyTest`, `CoalescedRangesTest`, `PageRangeIoTest`, `DictionaryPrefixFetchTest`.

## Fetch plans

A `FetchPlan` is one decoded column's view of one row group: `isEmpty()`, a `PageIterator` over `PageInfo`s in file order, and `prefetch()`. `PageSource` drains whichever plan it is given without knowing its type, and the only choice between the two page-reading plans is whether the column chunk has an OffsetIndex.

| Plan | Chosen when | Pages | I/O at plan time |
|---|---|---|---|
| `IndexedFetchPlan` | the chunk has an OffsetIndex | Listed from the OffsetIndex at plan time, filtered and truncated | None |
| `SequentialFetchPlan` | the chunk has no OffsetIndex | Discovered by walking page headers from fixed-size chunk handles | None |
| `FetchPlan.EMPTY` | the filter's row ranges touch no page of an indexed column, or the row group's dictionaries dropped it | None | None |
| `SkippedColumnFetchPlan` | a filter-only column in a row group statistics proved to match in full ([RECORD_FILTERING.md](RECORD_FILTERING.md#filter-only-column-skip)) | One boundary marker | None |

**A `PageInfo` carries resolved bytes.** The plan's iterator resolves a page's bytes (which fetches its chunk if needed) before it hands out the `PageInfo`, so a decode task does no I/O; every read of page bytes happens on the retriever or on a speculative task. A `PageInfo` holds no chunk handle. Its page slice keeps the fetched buffer alive after the plan is evicted, until the decode task is done with it. Untested.

### Indexed plans

`computeFetchPlans` builds an indexed plan in four steps:

1. **Needed pages.** Every page, each with `PageRowMask.ALL`, when the row ranges are all rows. Otherwise the pages whose rows `[firstRowIndex, next page's firstRowIndex)` overlap the matching `RowRanges`, each paired with the mask `RowRanges.maskForPage` gives it. No needed page yields `FetchPlan.EMPTY`, and nothing is fetched for the column. An OffsetIndex listing no page for a chunk with values is rejected at parse ([FILE_METADATA.md](FILE_METADATA.md#page-index)), so `FetchPlan.EMPTY` means only that the filter or the dictionaries left no needed page.
2. **Row limit.** On an unfiltered read the list is truncated to the pages starting before the row group's share of `maxRows` ([row limits](#row-limits-and-chunk-sizing)).
3. **Page groups.** The needed pages are merged under the gap policy into page groups, one `ChunkHandle` per group, each chained to the next.
4. **Dictionary.** The dictionary page spans from the chunk's first page to the first data page the OffsetIndex lists. It joins the first page group when the gap policy would merge the two (`foldsDictionary`); otherwise it gets its own handle, chained to the first group's, so reading the dictionary prefetches the first group. A dictionary pruning has read is not fetched at all.

An OffsetIndex page location's size includes the page header, so a page's slice is its header and body and the indexed path parses no header to find pages. The chunk's first page is at `dictionary_page_offset`, or at `data_page_offset` when the file declares none, as parquet-mr 1.12 does; a dictionary page declared after the first data page is rejected as malformed (`DictionaryParser.firstPageOffset`).

The plan parses its dictionary on the first advance of its iterator, from the dictionary handle or the first group, and then slices each page from its group's handle.

### Sequential plans

A sequential plan knows the chunk's byte range, and discovers its pages as its iterator walks the headers. The range starts at the chunk's first page, or where a dictionary pruning has read ends. Its bytes come through chunk handles of a fixed size (the [chunk size](#row-limits-and-chunk-sizing)): entering a chunk creates the next chunk's handle and chains it for one-ahead prefetch, and a page that straddles two chunks is assembled from both into one direct buffer (`assembleFromChunks`). A header is read with a bounded peek that grows while the header is truncated, up to a limit that marks the file corrupt.

The walk skips non-data pages. For each data page it decides, in order: drop the page when its row mask is empty, without reading its body; emit a null placeholder when inline page statistics prove no row matches ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md#inline-page-statistics)); otherwise emit the page with its mask. It stops when the row limit is covered ([row limits](#row-limits-and-chunk-sizing)) or the cursor has passed the last matching row.

**Counters.** `valuesRead` accumulates every data page's `num_values` and, under masks, `recordsRead` every data page's record count, whether the page was kept, dropped or replaced. A walk that reaches the end of the chunk checks `valuesRead` against the chunk's `num_values` and, under masks, `recordsRead` against the row group's row count, and raises `ParquetReadException` on a mismatch. A walk that stops early at the row limit or past the last matching row skips both checks.

Tests: `PageRangeIoTest`, `DictionaryPrefixFetchTest`, `SequentialFetchPlanEarlyExitTest`, `S3SelectiveReadJfrIT` (s3).

## Chunk handles and prefetch

A `ChunkHandle` is a lazy handle on one contiguous byte range. Its first `ensureFetched()` (or `slice()`) reads the range, double-checked under the handle's lock, and every later call returns the cached buffer. The first access that finds a next handle chained starts an asynchronous fetch of that handle, whether the access fetched its own bytes or found them already fetched by a prefetch. The prefetch fetches only its own handle and starts none, so prefetch stays one handle ahead of the read and never cascades down the chain. A handle backed by a `SharedRegion` slices the region instead of reading, and leaves prefetch to the region's own chain (`SharedRegion.nextRegion`), which follows the same rule one region ahead. The prefetches run as the iterator's speculative tasks, which its close awaits ([READ_PIPELINE.md](READ_PIPELINE.md#cancellation-and-close)).

A failed prefetch leaves the handle unfetched, and the demand path fetches it again ([EXCEPTION_MODEL.md](EXCEPTION_MODEL.md#crossing-a-thread-boundary)). A demand fetch that fails raises an `IOException` naming the file, offset and length.

Each column fetches independently, and no column waits for another's data, except that the first read of coalesce-safe plans may be shared through a `SharedRegion`.

Tests: `ChunkPrefetchChainTest`. The prefetch failure paths are untested.

### Next-row-group prefetch

Which column triggers the next row group's prefetch, on which thread, and when the task plans nothing is in [READ_PIPELINE.md](READ_PIPELINE.md#row-group-and-file-transitions). On the fetch side, `prefetch()` reads on the task's own thread, so the iterator's close, which awaits the task, also awaits that read. `IndexedFetchPlan.prefetch` fetches the dictionary handle when there is one and the first page group otherwise; as a demand fetch would, that fetch prefetches the handle chained after it (the first page group, the second one, or the next shared region). `SequentialFetchPlan.prefetch` fetches its first chunk handle, or the shared region that replaced it; the plan keeps that handle, so its walk starts on the prefetched bytes instead of fetching them again. Computing the plans ahead also performs that row group's planning reads (dictionaries, page index, and the mask probe where it is needed).

Tests: `NextRowGroupPrefetchTest`, `SequentialNextRowGroupPrefetchTest`, `DictionaryPushDownIoTest`, `S3SelectiveReadJfrIT` (s3).

## Cross-column coalescing

After building a row group's plans, `coalesceAcrossColumns` merges the first reads of several columns into `SharedRegion`s, one `readRange` each:

1. Collect the first read (`firstChunkOffset`, `firstChunkLength`) of every plan that is coalesce-safe, and the whole byte extent of every plan that is not.
2. Sort the first reads by offset and walk them greedily: a read joins the current region when the gap policy merges it and the gap it bridges holds no byte of an unsafe plan's extent; otherwise it starts a new region.
3. Regions of one column are dropped; when none holds two, nothing changes. Each remaining region is chained to the next for one-ahead prefetch, and each member plan's first handle is replaced by one backed by the region.

**The gate.** A plan is coalesce-safe only when its first chunk is everything it will read (`CoalescableFirstChunk.isCoalesceSafe`):

| Plan | Safe when |
|---|---|
| `IndexedFetchPlan` | exactly one page group and no separate dictionary handle |
| `SequentialFetchPlan` | its chunk size equals the chunk's length |

A plan with more than one read has gaps of its own (pages a filter dropped, a dictionary fetched apart, chunks a row limit left for later). Bridging such a plan into a region would fetch its dropped bytes, and its later handles would then fetch bytes the region already holds. The rule that a region never bridges an unsafe plan's extent prevents the same double fetch from the other side.

A region lives as long as any handle attached to it: evicting the row group's plans drops the handles, and the buffer goes with them. No reference count is kept.

Tests: `CrossColumnCoalesceTest`.

## Page masking

A `PageRowMask` selects the records of one page to keep, as sorted, non-overlapping `[start, end)` intervals relative to the page's first row, counted in values for a flat column and in top-level records (repetition level 0) for a nested one. `PageRowMask.ALL` keeps the page whole; a page with no matching row gets no mask and is not read. Masks come from the row group's `RowRanges`, which are the column-index result under a filter ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md#column-index)) or the range `[residue, numRows)` a fast `tail` sets on the first work item ([ROW_READER.md](ROW_READER.md#tail)). How workers apply a mask is in [NESTED_DECODE.md](NESTED_DECODE.md) and [READ_PIPELINE.md](READ_PIPELINE.md).

### The mask-capability gate

**Per-page masks apply to every decoded column of a row group or to none.** Columns have their own page boundaries; a column that ignored the masks its siblings applied would hand the worker rows its siblings dropped, and the batches would misalign. A column is mask-capable when it:

- has an OffsetIndex (its plan is indexed, and the OffsetIndex gives each page's first row); or
- is flat, so a sequential plan takes the record count from the page header's `num_values`; or
- is nested with `DATA_PAGE_V2` pages, whose repetition levels sit uncompressed ahead of the body and are counted without the codec (`PageRecordCounter`).

A nested column without an OffsetIndex whose pages are `DATA_PAGE` (v1) holds its repetition levels inside the compressed body, so counting its records would mean decompressing pages the mask exists to skip. Such a column closes the gate for the whole row group: `computeFetchPlans` promotes the row group's ranges to all rows, so no plan applies a mask, and the filter falls to row-group statistics and per-row evaluation. A fast `tail` is not taken for the read ([ROW_READER.md](ROW_READER.md#tail)).

`RowGroupIterator.masksApplicableForRowGroup` decides the gate at most once per row group. `SharedRowGroupMetadata` holds it as a `LazyMaskCapability`, which computes it on first request and caches it; a failed probe caches nothing. `computeFetchPlans` requests it only when the row group's ranges are not all rows, and the tail path (`canFastSkipAllRowGroups`) requests it for each row group in turn, stopping at the first that closes the gate, before the tail range exists, so an unfiltered read without a tail never probes. The v1/v2 question is answered by `PageFormatProbe`, which reads the page header at `data_page_offset` and accepts it only if it is a data page ending within the chunk; otherwise (some writers understate the offset or point it at the dictionary page) it walks from the chunk start past the dictionary page. A chunk with consistent offsets costs one request. Writers do not mix page versions within a chunk, so one header decides the chunk. A row group its dictionaries drop needs no gate, and a file whose chunks all have an OffsetIndex pays nothing.

### Sequential plans under a mask

With masks active, the sequential walk counts each data page's records (from `num_values` for a flat column, from the repetition-level prefix for a nested v2 one) and asks `RowRanges.maskForPage` for the page's mask. A page with no match is skipped without reading its body; its bytes may have been fetched as part of the chunk, so the saving is decompression and decode. A null placeholder under a partial mask stands for the masked record count, so sibling columns agree on row counts. Once `recordsRead` passes `RowRanges.endRow()`, every remaining page would get no mask, and the walk stops without reading further headers. Reaching a v1 page on a nested column under a mask raises `IllegalStateException`, since the gate should have prevented it.

Tests: `MisalignedPageBoundariesTest`, `MisalignedPageBoundariesNoIndexTest`, `NestedV2NoIndexMaskingTest`, `NestedV1NoIndexFallbackTest`, `SequentialFetchPlanEarlyExitTest`, `PageFormatProbeTest`, `MaskProbeIoTest`.

## Row limits and chunk sizing

`maxRows` narrows fetches only on an unfiltered read. With a filter, `head(n)` counts matching rows, which a scan may find anywhere, so every surviving page stays fetchable and the cap is enforced downstream ([READ_PIPELINE.md](READ_PIPELINE.md#row-limits)). Without one, each row group gets the remainder of the budget after the rows planned before it (`perRgMaxRows`, plus the skip residue on the first work item), since page row indices restart at 0 in every row group; earlier row groups are covered whole and only the last one is cut.

| Plan | Effect of the row group's share `m` |
|---|---|
| `IndexedFetchPlan` | Keeps the needed pages whose first row is below `m`; the page groups cover only those |
| `SequentialFetchPlan` | Sizes its chunks from `m`. A flat column stops yielding pages once their values cover `m` rows; a nested column, whose record count is inside the compressed page, yields its pages until the worker's drain reaches the cap |

A page is the smallest fetch and decode unit: a page that straddles the limit is fetched and decoded whole.

**Sequential chunk size** (`SequentialFetchPlan.computeChunkSize`, capped at the chunk's length):

| Case | Chunk size |
|---|---|
| No row limit | a default ceiling large enough that most chunks are one fetch; overridable through `hardwood.internal.sequentialChunkSize`, whose unparseable value is rejected with `IllegalArgumentException` rather than ignored |
| Row limit, small chunk (below a fixed threshold) | the whole chunk, which keeps the plan coalesce-safe |
| Row limit, larger chunk | average compressed bytes per value × `m` × a safety factor, between a fixed floor and the ceiling |

The estimate trades a possible second request for not fetching a large chunk whose head is all the read needs.

Tests: `SequentialFetchPlanChunkSizeTest`, `S3SelectiveReadJfrIT` (s3).

## I/O budget

`IoBudget` (core tests) computes, from the footer and the OffsetIndex alone and independently of the reader, the bytes a read of given columns and rows has to fetch: for every row group holding a requested row, each column's dictionary page and the data pages whose rows intersect the requested rows, or the whole chunk where the column has no OffsetIndex. A read is within budget when every data byte it fetches lies in a needed range or in a gap of at most `CoalescingPolicy.GAP_BYTES` between two, and no data byte is fetched twice. Data bytes are those inside a column chunk. Reads issued under a `pruning` fetch reason (dictionaries and bloom filters read to decide whether to read a chunk) are left out; the pruning tests pin their cost.

`CountingInputFile` wraps an `InputFile` and records every `readRange` with its offset, length and the `FetchReason` in effect. A change to the fetch path asserts its effect against the budget.

**Fetch reasons.** `FetchReason` is a per-thread tag each read runs under, for S3 fetch logs and for the budget. Chunk and plan prefetches carry the caller's tag across the thread hand-off (`FetchReason.bind`); the next-row-group task sets its own `prefetch rg=N`. A chunk handle composes its purpose under the tag in effect as `outer | purpose`, while planning reads replace the tag with their own.

| Reason | Read |
|---|---|
| `footer-info`, `footer-body` | footer |
| `rg=N pruning` | bloom filters at planning, dictionaries on entry |
| `rg=N indexes`, `rg=A-B indexes` | a page-index window over row groups `A` to `B`, and the mask probe of row group `N` |
| `rg=N col=C pageGroup=g/G`, `rg=N col=C dictionary` | indexed plan handles |
| `rg=N col='name' seqChunk@K` | sequential plan handles |
| `rg=N region=A..B` | a shared region whose first and last members in file order are plans `A` and `B` |
| `prefetch rg=N` | outer tag of the next-row-group prefetch |

Tests: `CrossColumnCoalesceTest`, `PageRangeIoTest`, `DictionaryPrefixFetchTest`.

## Boundaries

- **Bloom-filter slices (#735).** Bloom filters are read one request each at planning, not merged under the gap policy and not windowed with the page index.
- **Sequential chunks ignore row ranges (#1025).** A sequential plan under a mask fetches the chunk and skips unmatched page bodies after the fact.
- **Page-level skip for the `skip` residue (#381).** The residue of a physical `skip` is fetched and decoded, although a page mask could drop its leading pages as it does for `tail`.
- **Fast `tail` on nested v1 without an OffsetIndex (#1306).** Such a column closes the mask gate, so `tail` decodes and discards the residue.
- **Mask probe on bytes already fetched (#1354).** The mask-capability probe issues its own read, although the page headers it needs lie in bytes the read fetches anyway.
- **A storage-dependent gap (#763, #827).** The gap limit is one constant for every backend.
