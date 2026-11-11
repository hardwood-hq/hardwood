# Fetch planning

Describes which bytes a read requests from its `InputFile`, and in which requests: the order of a read's requests per file and per row group, the gap policy every merge applies, how a column's pages are found in the read's ranges, the rules by which the read starts, retries and releases its requests, per-page row masks and the gate that allows them, row limits on the fetch side, and the I/O budget tests hold reads to.

Related documents:

- [INPUT_FILES.md](INPUT_FILES.md): the `InputFile` contract and what a `readRange` costs on the mapped and in-memory backends
- [S3_STORAGE.md](S3_STORAGE.md): the S3 backend and what a `readRange` costs there
- [FILE_METADATA.md](FILE_METADATA.md): parsing the footer and the page index
- [READ_PIPELINE.md](READ_PIPELINE.md): the read that runs the fetches (`RowGroupIterator`, its parts, the priority and bounds of its jobs, multi-file reads, the failure model)
- [STATISTICS_PRUNING.md](STATISTICS_PRUNING.md): the CANNOT/MIGHT/ALWAYS decisions that choose rows and pages
- [THREADING.md](THREADING.md): the pool threads that run the read's plans and scans, and the virtual threads its requests run on

## Fetch sequence

On a local file up to 2 GB a `readRange` is a zero-copy slice of the whole-file mapping, so a fetch is a slice and reading ahead costs nothing; above 2 GB it is one `FileChannel.map` call ([INPUT_FILES.md](INPUT_FILES.md#local-files-memory-mapping)). On a remote file each `readRange` that is not served locally is a round trip. The order below is the same for both; only its cost differs.

### Per file

| Step | Where | Requests |
|---|---|---|
| Footer | `ParquetMetadataReader.readFooter` | One read of the file's tail, its last 256 KiB or the whole file where it is shorter, which holds the trailing length and magic and usually the footer; a footer longer than the tail costs a second read of the part before it. The leading magic is not read ([FILE_METADATA.md](FILE_METADATA.md#footer-read)). A footer a `MetadataSource` supplies costs the tail's read too, which checks the trailer ([FILE_METADATA.md](FILE_METADATA.md#supplied-footers)). On S3 the suffix `S3InputFile.open` fetches is the tail, so its read costs no request ([S3_STORAGE.md](S3_STORAGE.md)). A file whose bytes are in memory takes no tail ([The file's tail](#the-files-tail)) |
| Row-group pruning on footer statistics | `RowGroupPruning.filterRowGroups` | None. Records, per kept row group, the columns whose bloom filter a leaf would probe |

A file is listed once its footer is loaded and the files before it are listed ([READ_PIPELINE.md](READ_PIPELINE.md#units-and-files)); its footer is read ahead of the read's data within the files-ahead bound ([opening files ahead](READ_PIPELINE.md#units-and-files)). Listing reads no Bloom filter; which leaves are probed is in [STATISTICS_PRUNING.md](STATISTICS_PRUNING.md#row-groups).

### The file's tail

The footer's read of a file's tail (`TailedInputFile`) serves every later read that lies inside it as a slice, without a request: the footer, the page index and the Bloom filters that sit before it, and data. A file up to the tail's size costs that one request. Coalescing cuts a unit's ranges at the tail's start, so no data request crosses it: the part before the start is requested and the part after it is sliced. A page-index window merges no slices across the tail's start either, so of its slices only one that straddles the start is requested across it (`CoalescedRanges`, the one merge both use). The read holds a file's tail until the consumer passes the file, and drops it then, with the file's footer, or when it closes; a read of the file from then on is a request again. A footer read ahead is charged its tail's size against F and R until then. The tail's size is an internal property (`hardwood.internal.tailBytes`, 256 KiB by default), read for each file; core's tests set it to the eight bytes of the trailer, so a fixture of a few kilobytes reads as a file larger than its tail.

A file in memory holds every byte while it is open, and a file whose tail the read took holds the tail until it is dropped; the read asks a file what it holds through `HeldBytes` (from which offset, whether for as long as the file is open, a slice without I/O), never by its class, so the tail cut in coalescing, the ranges sliced in place and the dropping of a tail treat both alike.

Tests: `FileTailTest`.

### Per row group

A unit's reads follow one another in the order Parquet forces; each is a fetch keyed at the unit's first row, started by the read's rules ([Requests](#requests)), and each step runs once the bytes it reads have arrived. A range inside the file's tail while the read holds it, or of a file in memory, is sliced in place.

| # | Step | Where | Requests |
|---|---|---|---|
| 1 | Bloom filters of the columns listing recorded for the row group, in a row group statistics left undecided | the unit's [index window](#page-index-windows), then `RowGroupPruning.probeOnArrival` on a plan job | Fetched for the window by its first member: at most one request when the window's filters lie within the gap limit of each other. A filter whose footer entry gives no `bloom_filter_length` is left out of the window and read by `RowGroupBloomFilterSource` when the probe reaches it |
| 2 | Dictionaries a filter can prune with, in a row group statistics and Bloom filters left undecided: fully dictionary-encoded filter columns whose `EQ`/`IN` leaf can be decided | `RowGroupDictionaryFilterSource`, in the same probe | One per such column, as the filter's fold reaches a leaf it can decide, sized by the gap to the first data page or by a bounded probe, and a second only when the page header declares more than that read held |
| 3 | Page index | the unit's index window | The OffsetIndex of every decoded column the row group reads and, under page filtering, the ColumnIndex of every filter column. Fetched for the window once its first member is kept by its Bloom filters and dictionaries: at most one request per structure when the window's slices lie within the gap limit of each other |
| 4 | Plan | `PagePlanner`, on the unit's plan job, which `RowGroupPlanner` schedules | None: each decoded column's pages from its OffsetIndex, kept by the rows the filter matches and the row limit, and the unit's ranges |
| 5 | Data | the unit's ranges, once it is installed (`PageFetcher`) | Every needed range of every decoded column coalesced at once under the [gap policy](#gap-policy) ([Coalescing a unit](#coalescing-a-unit)) |

A probe reads over the bytes present: a filter or dictionary not present counts as absent for that pass, so a drop it reaches stands, and a keep is evaluated again once the ranges it lacked have arrived; the fold keeps each leaf's decision once final, so it reads the same dictionaries, in the same order, as a probe that waited for each. A row group its Bloom filters drop reads no dictionary. A row group its Bloom filters or dictionaries drop is planned with no page and no range, and steps 3 to 5 read nothing for it, unless another member of its window needs the page index. The dictionaries a probe read are decoded with the unit's pages, so a dictionary page pruning read is not fetched again. How the Bloom filters and dictionaries refine the decisions listing recorded is in [STATISTICS_PRUNING.md](STATISTICS_PRUNING.md#row-groups).

Before the first step that reads anything the row group's footer entries address, planning checks once that its chunks are stored in this file ([FILE_METADATA.md](FILE_METADATA.md)), and then takes the extents of the decoded columns' chunks from the footer and checks them against the file and against each other (`PagePlanner.checkedExtents`): a chunk outside the file, or two that overlap, fail the unit with a `ParquetReadException` naming the column. The same-file check comes first, since another file's chunk would otherwise fail as lying outside this one.

Page filtering applies when the read has a filter and metadata filtering is on, in a row group statistics left undecided. In a row group they proved to match in full every value of every page matches, so no page bound can rule a page out; its ColumnIndex is neither fetched nor evaluated, and its skipped filter-only columns need no index at all.

### Page-index windows

Listing a file splits its units into windows of consecutive row groups (`IndexWindow.plan`). A window holds three structures: the ColumnIndex slices, the OffsetIndex slices and the Bloom-filter slices its members need. A window takes as many row groups as fit the bytes it fetches, gaps included, within a budget (`hardwood.internal.indexWindowBytes`), and at least one, so a file whose page index and Bloom filters fit the budget is read in one window. The budget bounds what a window holds, since a large file's page index reaches hundreds of megabytes. Before it forms any window, listing checks every slice its units need against the file: a ColumnIndex, OffsetIndex or Bloom filter the footer places outside the file fails the read with a `ParquetReadException` naming the column and the structure.

Writers commonly lay the page index out by structure, then row group, then column: all ColumnIndexes, then all OffsetIndexes. A window merges the slices of each structure with each other under the [gap policy](#gap-policy), never one structure's with another's: the gap between a window's last ColumnIndex slice and its first OffsetIndex slice holds the ColumnIndexes of every later row group, which the following windows fetch themselves. Writers commonly place the Bloom filters of all row groups together too, row group by row group, so a window's filters of one column are strided by the other columns' filters and merge within the gap limit. Consecutive windows therefore fetch disjoint bytes, and a read fetches no byte of the page index twice.

A window's Bloom filters are fetched first, keyed at its first member, and its page index once its first member survives them, so a window whose members are all dropped by their Bloom filters or dictionaries fetches no page index. Each structure's ranges are held by every member, and returned once the last member is planned.

**Data coalescing never crosses a row group.** Every merge of data ranges is computed from one row group's columns, so a unit's ranges are returned with its pages and do not keep a neighbour's alive. Untested. Page-index windows cross row groups.

Tests: `RowGroupIndexBuffersTest`, `IndexWindowTest`, `OutsideFileTest`, `IndexWindowIoTest`, `BloomFilterWindowIoTest`, `DictionaryPushDownIoTest`, `S3SelectiveReadJfrIT` (s3).

## Gap policy

Two byte ranges are fetched as one request when the gap between them is at most G (`CoalescingPolicy.GAP_BYTES`) and the merged span at most S (`CoalescingPolicy.maxSpanBytes`). `CoalescingPolicy.merges` holds the rule, and every merge calls it:

| Merge | Where |
|---|---|
| A row group's needed data ranges, of every column at once, into its data requests | `CoalescingPolicy.coalesce` |
| Page-index and bloom-filter slices of one structure | `CoalescedRanges` |

A merge bridges the gap between two needed ranges. It never extends a request before the first needed byte or past the last one. A range longer than the span limit is a request of its own.

The gap limit is the serial break-even of a remote request: the bytes that take as long to transfer as one round trip (latency × bandwidth), so fetching a gap costs no more than issuing a second request. The span limit keeps each `readRange` bounded, which lets read-ahead overlap decode and lets a read that stops early leave the rest unfetched. The best gap depends on the storage and on how many requests run in parallel; the rule lives in one class so that a storage-dependent value replaces one constant. The span limit has a tuning override (`hardwood.internal.maxCoalescedBytes`), read once per read, which is not a public option; a value that is not a positive byte count is rejected with `IllegalArgumentException`.

Tests: `CoalescingPolicyTest`, `CoalescedRangesTest`, `PageRangeIoTest`, `DictionaryPrefixFetchTest`.

## Pages

A column chunk's pages reach its placement in one of two ways, and the only choice between them is whether the chunk has an OffsetIndex.

| Chunk | Pages | Bytes |
|---|---|---|
| With an OffsetIndex | listed from it when the unit is planned (`PagePlanner.plan`), kept by the rows the filter matches and truncated by the row limit | the unit's ranges holding each page, planned with them |
| Without one | found by a scan of the chunk's headers once the unit is installed (`HeaderScanner`), as the ranges holding them arrive | the chunk's ranges, keyed by the row as far into the chunk as each lies until the scan finds a page in it, then at that page's row |
| Filter-only, in a row group statistics proved to match in full ([RECORD_FILTERING.md](RECORD_FILTERING.md#filter-only-column-skip)) | none | none |

**A page's bytes come from the ranges it lies in.** A page that lies in one range is a zero-copy slice of it. A page that straddles the cut between two ranges is copied from both into one buffer once both have arrived. A page's decode starts only once every range holding it, and its chunk's dictionary, have arrived, so a decode does no I/O and parses no header again. The ranges stay held by the pages in them until each is decoded and every fill that reads the page's [stored values](VALUE_DECODE.md#page-decode) in place is done with it.

**OffsetIndex pages.** With all rows read, every page is needed, each with `PageRowMask.ALL`. Otherwise the pages whose rows `[firstRowIndex, next page's firstRowIndex)` overlap the matching `RowRanges` are needed, each paired with the mask `RowRanges.maskForPage` gives it; no needed page reads nothing for the column. An OffsetIndex listing no page for a chunk with values is rejected at parse ([FILE_METADATA.md](FILE_METADATA.md#page-index)). On an unfiltered read the list is truncated to the pages starting before the unit's share of the row limit ([row limits](#row-limits)). An OffsetIndex page location's size includes the page header, so a page's slice is its header and body. The dictionary page spans from the chunk's first page to the first data page the OffsetIndex lists, and is a range of its own unless the unit's probe read the dictionary. The chunk's first page is at `dictionary_page_offset`, or at `data_page_offset` when the file declares none, as parquet-mr 1.12 does; a dictionary page declared after the first data page is rejected as malformed (`DictionaryParser.dictionaryPageStart`).

**Scans.** A scan knows its chunk's byte range and finds its pages as it reads their headers, on a job at a time, each going as far as the ranges present reach. A header is read with a bounded peek that grows while the header is truncated, up to a limit that marks the file corrupt (`PageFormatProbe.readHeader`). The scan holds each range of its chunk until it has passed it, and its dictionary until it ends. For each data page it decides, in order: drop the page when its row mask is empty, without reading its body; emit a null placeholder when inline page statistics prove no row matches ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md#inline-page-statistics)); otherwise emit the page with its mask. A scan that reaches the end of its chunk checks the values its pages hold against the chunk's `num_values`, and fails on a mismatch with a `ParquetReadException`; its failure is placed at the first row it has not passed.

**Empty row ranges read no page.** When no row of the row group survives page filtering (a leaf's column index rules out every page, or an `AND` intersects disjoint ranges), its `RowRanges` are empty, and no decoded column reads a page: an indexed column after reporting its skipped pages, a column without an OffsetIndex without a scan.

### Coalescing a unit

A unit's ranges are built from every decoded column's needed ranges at once (`CoalescingPolicy.coalesce`): its kept pages and dictionary pages where a filter or the row limit leaves pages out, otherwise each chunk's extent from the footer cut into pieces of at most S, and a chunk without an OffsetIndex whole, or under a row limit its first part. Sorted by offset, a range joins the request before it when the gap policy merges it, so one request may serve the pages of several columns across a gap of at most G, and is at most S long unless one range alone is longer. Its fetch reason names every column whose range it holds ([fetch reasons](#io-budget)).

**Validation.** Before coalescing, each needed range is checked against its column chunk's extent and each chunk's extent against the file's length, and the extents of the columns the read reads must not overlap. A range or extent that fails is a `ParquetReadException` naming the column, so a request two columns share never fails for a column whose ranges were valid.

Tests: `CoalescingPolicyTest`, `CrossColumnCoalesceTest`, `PageRangeIoTest`, `DictionaryPrefixFetchTest`, `InlineNullPageDropTest`, `ReadRequestsTest`.

## Requests

Every read a read makes of a file whose bytes are not in memory is a fetch job on a virtual thread of the context's I/O executor: a unit's ranges, its window's Bloom filters and page index, a dictionary a probe reads, and the footer reads ([READ_PIPELINE.md](READ_PIPELINE.md#units-and-files)). A read issues no other request. A fetch starts by the read's priority and bounds ([READ_PIPELINE.md](READ_PIPELINE.md#scheduling)): lowest key first, while less than F is fetched ahead of the consumer's need and R has room for its bytes; the fetch the consumer's next batch needs starts whatever F and R hold, uncharged to R, while no other work of that batch is in flight. A fetch is charged to R from its start until its last reader is done ([lifetimes](READ_PIPELINE.md#scheduling)).

A fetch that fails with an exception is run once more inside its job; a second failure is final, an `IOException` naming the file, offset and length ([EXCEPTION_MODEL.md](EXCEPTION_MODEL.md#crossing-a-thread-boundary)), placed at the first row the fetch served ([READ_PIPELINE.md](READ_PIPELINE.md#failures)). An `Error` is not read again. The read's close returns the charges of every fetch and waits for the fetches in flight without interrupting them ([READ_PIPELINE.md](READ_PIPELINE.md#cancellation-and-close)).

Tests: `ReadYieldTest`, `ReadAheadTest`, `SpeculativeErrorTest`, `FilesAheadTest`, `FooterReadRetryTest`.

## Page masking

A `PageRowMask` selects the records of one page to keep, as sorted, non-overlapping `[start, end)` intervals relative to the page's first row, counted in values for a flat column and in top-level records (repetition level 0) for a nested one. `PageRowMask.ALL` keeps the page whole; a page with no matching row gets no mask and is not read. Masks come from the row group's `RowRanges`, which are the column-index result under a filter ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md#column-index)) or the range `[residue, numRows)` a fast `tail` sets on the first work item ([ROW_READER.md](ROW_READER.md#tail)). A nested column's decode trims the page to its kept records before measuring it, so nested placement and filling see only kept records ([NESTED_DECODE.md](NESTED_DECODE.md#masked-pages)); a flat column's fill reads each kept interval.

### The mask-capability gate

**Per-page masks apply to every decoded column of a row group or to none.** Columns have their own page boundaries; a column that ignored the masks its siblings applied would place rows its siblings dropped, and the batches would misalign. A flat column takes a mask whether or not it has an OffsetIndex: its OffsetIndex gives each page's first row, and a scan counts its rows from each page header's `num_values`. A nested column takes one only with an OffsetIndex, since only a page's repetition levels say where its records start.

So a row group takes page masks when every nested decoded column has an OffsetIndex; otherwise its ranges are promoted to all rows, no page takes a mask, and the filter falls to row-group statistics and per-row evaluation. The gate needs no bytes: the OffsetIndex offsets in the footer decide it. A fast `tail` is taken only where every row group passes it (`canFastSkipAllRowGroups`, [ROW_READER.md](ROW_READER.md#tail)).

Under a mask a scan asks `RowRanges.maskForPage` for each flat page's mask from the rows counted so far; a page with no match is skipped without reading its body, though its bytes may have been fetched with the chunk, so the saving is decompression and decode. A null placeholder under a partial mask stands for the masked record count, so sibling columns agree on row counts.

Tests: `MisalignedPageBoundariesTest`, `MisalignedPageBoundariesNoIndexTest`, `NestedV2NoIndexMaskingTest`, `NestedV1NoIndexFallbackTest`, `EmptyPageRangesUnindexedColumnTest`, `MaskProbeIoTest`.

## Row limits

`maxRows` narrows fetches only on an unfiltered read. With a filter, `head(n)` counts matching rows, which a scan may find anywhere, so every surviving page stays fetchable and the cap is enforced downstream ([READ_PIPELINE.md](READ_PIPELINE.md#row-limits)); listing stops only once row groups statistics prove to match cover it, before the first they leave undecided. Without one, listing stops once the limit is covered, and each unit gets the remainder of the budget after the rows listed before it, plus the skip residue on the first unit, since page row indices restart at 0 in every row group; earlier row groups are covered whole and only the last one is cut.

| Chunk | Effect of the unit's share `m` |
|---|---|
| With an OffsetIndex | Keeps the needed pages whose first row is below `m`; its ranges cover only those |
| Without one | Needs the chunk's first part, sized from `m`, and fetches the rest as the scan reaches it. A flat column's scan stops once its pages cover `m` rows; a nested column's, whose record count is inside the compressed page, goes on until the column's placement has placed its last row |

A page is the smallest fetch and decode unit: a page that straddles the limit is fetched and decoded whole.

**First part of a chunk without an OffsetIndex** (`CoalescingPolicy.computeChunkSize`, capped at the chunk's length):

| Case | First part |
|---|---|
| No row limit | the whole chunk, in pieces of at most S |
| Row limit, small chunk (below a fixed threshold) | the whole chunk |
| Row limit, larger chunk | average compressed bytes per value × `m` × a safety factor, between a fixed floor and S |

The estimate trades a possible second request for not fetching a large chunk whose head is all the read needs.

Tests: `ChunkSizeTest`, `ReadRequestsTest`, `S3SelectiveReadJfrIT` (s3).

## I/O budget

`IoBudget` (core tests) computes, from the footer and the OffsetIndex alone and independently of the reader, the bytes a read of given columns and rows has to fetch: for every row group holding a requested row, each column's dictionary page and the data pages whose rows intersect the requested rows, or the whole chunk where the column has no OffsetIndex. A read is within budget when every data byte it fetches lies in a needed range or in a gap of at most `CoalescingPolicy.GAP_BYTES` between two, and no data byte is fetched twice. Data bytes are those inside a column chunk. Reads issued under a `pruning` fetch reason (dictionaries and bloom filters read to decide whether to read a chunk) are left out; the pruning tests pin their cost.

`CountingInputFile` wraps an `InputFile` and records every `readRange` with its offset, length and the `FetchReason` in effect. A change to the fetch path asserts its effect against the budget.

**Fetch reasons.** `FetchReason` is a per-thread tag each read runs under, for S3 fetch logs and for the budget. A fetch runs under its own reason.

| Reason | Read |
|---|---|
| `footer-info`, `footer-body` | footer |
| `rg=N pruning`, `rg=A-B pruning` | the bloom filters of a window over row groups `A` to `B`; a row group's dictionaries, and a bloom filter its window did not fetch |
| `rg=N indexes`, `rg=A-B indexes` | a page-index window over row groups `A` to `B` |
| `rg=N col=a col=b` | a data request of row group `N`: one `col=` per column whose ranges it holds, by name, in file order |

Tests: `CrossColumnCoalesceTest`, `PageRangeIoTest`, `DictionaryPrefixFetchTest`, `ReadRequestsTest`.

## Boundaries

- **Scanned chunks ignore row ranges (#1025).** A chunk without an OffsetIndex under a mask is fetched whole, and its unmatched page bodies are skipped after the fact.
- **Page-level skip for a residue past the first file (#381).** The residue of a physical `skip` that lands in a later file is fetched and decoded, although a page mask could drop its leading pages as it does in the first file.
- **Nested columns without an OffsetIndex take no mask (#1306).** A row group with one reads every page of every column and filters per record, and `tail` decodes and discards the residue.
- **No overlap of data with the page index.** A unit's data ranges start once its plan has read its page index, also where every row of the row group is read.
- **A storage-dependent gap (#763, #827).** The gap limit is one constant for every backend.
