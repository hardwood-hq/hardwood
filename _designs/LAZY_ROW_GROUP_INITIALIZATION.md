# Lazy Row-Group Initialization

**Status: Implemented (Phases 1-3)**

## Problem

`SingleFileRowReader.initialize()` eagerly fetches column chunk data for **all** row groups before returning the first row. For local files this is invisible (zero-copy slices off a memory-mapped buffer), but for S3 and other remote backends each row group triggers `readRange()` calls that perform HTTP GETs, downloading the full column data for every row group upfront.

This means `print -n 10` on a multi-row-group S3 file downloads the entire file before printing a single row, even though the first row group alone contains far more than 10 rows.

The same pattern exists in `FileManager.scanAllProjectedColumns()` (the multi-file path) and in `ColumnReader.create()` (the columnar API).

### What is and isn't eager

The reader pipeline has two phases with different eagerness characteristics:

1. **Fetching and page scanning** (in `initialize()`) — **eager, no back-pressure.** For each row group, `initialize()` fetches index buffers (`RowGroupIndexBuffers.fetch()`), fetches all column chunk data (`inputFile.readRange()`), and scans page headers (`PageScanner.scanPages()`), creating `PageInfo` objects that hold `ByteBuffer` slices of the raw compressed data. This happens for ALL row groups before the first row is returned.

2. **Page decoding and batch assembly** (in `PageCursor` / `ColumnAssemblyBuffer`) — **lazy, with back-pressure.** `PageCursor` decodes pages via an adaptive prefetch queue capped at 4–8 futures. `ColumnAssemblyBuffer` publishes assembled batches via an `ArrayBlockingQueue` with capacity 2 — the assembly thread blocks when the consumer hasn't consumed previous batches. This pipeline is well-bounded and works correctly.

The back-pressure in phase 2 was always the intended design. The problem is that phase 1 has no equivalent gating: it downloads all raw column chunk data upfront. Before the `InputFile` abstraction was introduced (#98), `SingleFileRowReader` operated on a `MappedByteBuffer` directly, so phase 1 was just pointer arithmetic — no actual I/O. When S3 support was added, `readRange()` became a real HTTP GET, but the eager loop structure was preserved unchanged, turning phase 1 into a full-file download.

## Phase 1: Lazy row-group fetching (implemented)

Row-group fetching is now demand-driven: `initialize()` scans only the first row group, and `PageCursor` lazily fetches subsequent row groups when the assembly thread exhausts the current one.

### How back-pressure gates row-group fetching

Row groups are **not** proactively prefetched in `fillPrefetchQueue()` — unlike pages (which are small and decoded async), row groups involve full I/O on remote backends. Instead, row groups are fetched on demand via `tryLoadNextRowGroupBlocking()` when the assembly thread runs out of pages in the current row group.

The `ColumnAssemblyBuffer` back-pressure chain naturally limits how far ahead the assembly thread gets:

1. `readyBatches` queue (capacity 2) blocks the assembly thread on `put()` when full
2. `arrayPool` (initially 3 arrays, 1 taken by constructor) blocks on `take()` when empty
3. Combined, the assembly thread can produce at most **3 batches** before blocking — 2 in the queue + 1 being filled when the pool runs dry

With batch size determined by `computeOptimalBatchSize()` (targets 6 MB of L2 cache across all projected columns), the number of row groups fetched before back-pressure engages depends on the ratio of rows-per-row-group to batch size. For typical production files with large row groups (50K–1M+ rows), the first row group alone fills multiple batches, and the assembly thread never reaches the second row group before blocking.

### Concurrency

`SingleFileRowReader.getPagesForRowGroup()` is called by multiple assembly threads concurrently (one per column). Shared per-row-group state (index buffers, column chunk data) is cached and access is `synchronized` to prevent data races. The cache is keyed by row group index — when any column requests a new row group, the shared data is fetched once and reused by all columns.

`FileManager.scanRowGroupForColumn()` uses the same pattern with a `synchronized` block on the per-file `PerFileRowGroupContext`.

### What was implemented

- **`RowGroupPageSource`** — functional interface for lazy row-group page fetching
- **`PageCursor`** — added `tryLoadNextRowGroupBlocking()`, `hasMoreRowGroups()`, `fetchRowGroupPages()` that work with both `RowGroupPageSource` (single-file) and `FileManager` (multi-file). Row-group state resets on file boundary crossings.
- **`SingleFileRowReader`** — `initialize()` scans only the first row group; `getPagesForRowGroup()` extracted with synchronized per-row-group caching
- **`FileManager`** — `scanFirstRowGroup()` replaces `scanAllProjectedColumns()`; `PerFileRowGroupContext` stores per-file state for lazy access; `getRowGroupPages()` / `getRowGroupCount()` methods for `PageCursor`
- **`ColumnReader`** — `create()` scans only the first row group; `scanRowGroup()` extracted as `RowGroupPageSource`
- **`TestParquetGenerator`** — pure Java Parquet file generator (Thrift compact protocol writer) for generating test files on the fly, avoiding large committed binaries
- **`S3SelectiveReadJfrTest`** — 4 new tests: partial read for RowReader and ColumnReader (assert fewer RowGroupScanned events), full read correctness, and incremental scanning verification

## Phase 2: Lazy page fetching within a row group

Phase 1 fetches one row group at a time instead of all at once, but still fetches the **entire** row group's column chunk data upfront via `readRange()`. For production Parquet files where a single row group is 128 MB+ (the Spark/Hive default), `print -n 10` still downloads the full row group to decode just the first page.

### Where Phase 2 matters most

Phase 1 is sufficient when row groups are small relative to the data needed. Phase 2 matters when:

- **Row groups are large** (128 MB+, typical in Spark/Hive output) and the consumer only needs a few pages
- **The file has few row groups** (often just 1-2 for files written with default settings), so Phase 1's row-group skipping provides no benefit
- **S3 or remote backends**, where each `readRange()` is an HTTP GET with real latency and bandwidth cost

For local files (mmap), Phase 2 has no benefit — `readRange()` is a zero-copy slice regardless of size.

### Making page scanning incremental

`PageScanner.scanPagesSequential()` already reads page headers one at a time in a `while` loop. Each page header declares `compressedPageSize`, which gives the offset of the next page. The scanner discovers page boundaries as it goes — there is no need to have the full column chunk in memory to find the next page. Today it collects all `PageInfo` objects into a list and returns them; instead, it can yield them incrementally.

This means lazy page fetching works **without an OffsetIndex**. The sequential scan itself provides the page boundaries on demand. When an OffsetIndex is available, it enables additional optimizations (skipping directly to specific pages, knowing row counts per page for smarter read-ahead sizing), but it is not a prerequisite.

### Integration with Phase 1's RowGroupPageSource

In Phase 1, `RowGroupPageSource.getPages()` fetches the full column chunk data and scans all pages for a row group, returning the complete `List<PageInfo>`. In Phase 2, this changes: the source returns a lazy `PageScanner` iterator instead of a materialized list, and `PageCursor` pulls pages from it on demand.

The key change is in how `PageCursor` obtains pages. Today, `tryLoadNextRowGroupBlocking()` calls `fetchRowGroupPages()` which returns a `List<PageInfo>` and appends them all to `pageInfos`. In Phase 2, instead of appending a pre-scanned list, `PageCursor` holds a reference to the `PageScanner` iterator and pulls pages from it in `fillPrefetchQueue()`:

```
fillPrefetchQueue():
    while prefetchQueue.size < targetPrefetchDepth:
        if no more pages in pageInfos and scanner has more:
            PageInfo page = scanner.nextPage()   // may trigger readRange() for next window
            pageInfos.add(page)
        if nextPageIndex >= pageInfos.size:
            break
        // ... existing prefetch logic
```

This naturally integrates with the existing back-pressure: `fillPrefetchQueue()` is called from `nextPage()`, which is called by the assembly thread, which is gated by `ColumnAssemblyBuffer`. Pages are only pulled from the scanner when the assembly thread needs them.

### Incremental fetch model

Instead of fetching the full column chunk via a single `readRange()`, fetch a sliding window of raw bytes and advance it as the page scanner progresses:

1. Fetch an initial window of the column chunk (e.g. the first 256 KB, or a configurable size)
2. `PageScanner` reads page headers sequentially from this window, yielding `PageInfo` objects one at a time
3. `PageCursor` consumes them via its prefetch queue, applying back-pressure — the scanner only advances when the cursor needs more pages
4. When the scanner reaches the end of the fetched window, it fetches the next window via another `readRange()` call
5. Previous windows become unreachable once all their pages have been decoded

The window size can adapt based on page sizes observed so far: if pages are large, use larger windows to amortize `readRange()` overhead; if pages are small, smaller windows suffice.

### PageScanner as an iterator

Transform `PageScanner` from a batch method (`List<PageInfo> scanPages()`) into an iterator-like interface:

```java
boolean hasNextPage()
PageInfo nextPage()
```

The existing `scanPages()` method can remain as a convenience that drains the iterator into a list, preserving compatibility with code paths that don't need lazy fetching (e.g. local files where the full chunk is already memory-mapped).

### OffsetIndex as an accelerator

When an OffsetIndex is available, additional optimizations become possible:

- **Skip to specific pages.** The `OffsetIndex` provides exact file offsets per page, enabling direct seeks instead of sequential header scanning. This is useful when combining lazy fetching with page-level filter pushdown: only the matching pages need to be fetched.
- **Row-count-aware read-ahead.** The `OffsetIndex` includes `firstRowIndex` per page, enabling the read-ahead window to be sized in terms of rows rather than bytes.
- **Existing `PageRange`/`PageRangeData` infrastructure.** The selective I/O path already used for filter pushdown can be reused for demand-driven page fetching when an OffsetIndex is present.

### Scope

- Transform `PageScanner` into an incremental iterator with windowed fetching
- Modify `PageCursor` to pull pages from the scanner on demand instead of consuming a pre-populated list
- Update `RowGroupPageSource` (and `FileManager.scanRowGroupForColumn()`, `SingleFileRowReader.getPagesForRowGroup()`, `ColumnReader.scanRowGroup()`) to return a lazy scanner rather than a materialized page list
- Add OffsetIndex-based optimizations as an enhancement on top of the sequential path
- Adapt read-ahead window size based on observed page sizes

## Phase 3: Row limit in the reader API

Phases 1 and 2 make fetching lazy — the reader only pulls data when the consumer asks for it. But the reader still doesn't know *when to stop*. Today, the row limit lives in the CLI layer (`PrintCommand.prepareSampling()` applies `.limit(rowLimit)` on a stream), so the reader has no opportunity to act on it. The reader continues fetching, scanning, and decoding until the consumer stops calling `hasNext()`, at which point in-flight prefetch work is wasted.

Passing a `maxRows` parameter into the reader enables three optimizations that phases 1 and 2 cannot achieve on their own. This phase applies to `RowReader` only — `ColumnReader` exposes batch-level arrays rather than individual rows, so a row limit is not meaningful for its API. `ColumnReader` benefits from phases 1 and 2 (lazy row-group and page fetching) but not from phase 3.

### Why Phase 3 matters even with back-pressure

Phase 1 demonstrated that back-pressure from `ColumnAssemblyBuffer` limits how far ahead the assembly thread gets: it can produce at most 3 batches before blocking. However, each batch can hold tens or hundreds of thousands of rows (e.g. ~98K rows for 8 INT64 columns). For `print -n 10`, the reader still fetches and decodes enough data for ~300K rows before back-pressure engages — orders of magnitude more than needed.

With `maxRows`, the reader can:
- Skip row groups entirely based on metadata row counts (no I/O at all)
- Stop the assembly thread immediately after enough rows are emitted (no wasted decode work)
- Cancel in-flight prefetch futures (no wasted I/O)

### Row-group skipping

With `maxRows` known upfront and row-group row counts available from metadata, the reader can compute exactly which row groups are needed. For `maxRows = 10` on a file where the first row group has 100K rows, the reader knows at initialization time that only one row group will ever be needed — it never even registers the remaining row groups with `RowGroupPageSource`.

### Early termination of prefetch and assembly

When `AbstractRowReader` has emitted `maxRows` rows, it can set `exhausted = true` immediately instead of waiting for iterators to drain. More importantly, it can signal `PageCursor` to stop — cancelling in-flight prefetch futures and terminating the assembly thread. Without `maxRows`, the reader doesn't know the consumer is done until `close()` is called, by which time background threads may have decoded pages that will never be consumed.

### Interaction with record-level filtering

When a record-level filter is active, `maxRows` is an upper bound on *output* rows, not *input* rows. The reader must continue scanning until either `maxRows` matching rows have been emitted or all data is exhausted. The row limit is checked after filter evaluation, in `hasNextMatch()`, not before.

### API surface

Add a `maxRows` parameter to the `createRowReader` overloads:

```java
RowReader createRowReader(long maxRows)
RowReader createRowReader(ColumnProjection projection, long maxRows)
RowReader createRowReader(FilterPredicate filter, long maxRows)
RowReader createRowReader(ColumnProjection projection, FilterPredicate filter, long maxRows)
```

`maxRows <= 0` means unlimited (the current behavior). The parameter is propagated to `AbstractRowReader`, which counts emitted rows and stops iteration when the limit is reached.

For the CLI, `PrintCommand` passes its parsed `-n` value directly to `createRowReader()` instead of applying it as a stream operation.

### Scope

- Add `maxRows` field to `AbstractRowReader`, check in `hasNext()`
- Add `createRowReader` overloads to `ParquetFileReader` and multi-file readers
- Signal `PageCursor` to stop on early termination (cancel prefetch futures, finish assembly buffer)
- Update `PrintCommand` to pass the row limit to the reader
- Update usage documentation

## Test plan

### Phase 1 tests (implemented)

Test data is generated on the fly by `TestParquetGenerator` (pure Java, Thrift compact protocol), producing a file with 20 row groups × 50K rows × 8 INT64 columns (1M rows). This avoids committing a large binary to git.

**`partialReadDoesNotScanAllRowGroups`** — reads 10 rows via `RowReader`, asserts ≤50 `RowGroupScanned` events (out of 160 for a full read). Prints per-column breakdown for diagnostics. Actual results show ~33 events with back-pressure limiting to ~3–6 row groups per column.

**`columnReaderPartialReadDoesNotScanAllRowGroups`** — reads one batch via `ColumnReader`, asserts ≤15 `RowGroupScanned` events (out of 20 for a full read).

**`lazyInitScansRowGroupsIncrementally`** — full read via `RowReader`, asserts all 160 events, confirming lazy scanning produces correct results across all row groups.

**`fullReadReturnsAllRows`** — reads all 1M rows, asserts correct count and last value.

### Phase 2 tests

**S3 JFR test: `partialRowGroupReadTransfersProportionalBytes`** — use a single-row-group file with many pages (larger than 64 KB). Read a small number of rows and assert that bytes transferred are proportional to the rows read, not the full row group size.

### Phase 3 tests

**Unit test: `maxRowsLimitsOutput`** — read with `maxRows = 10` from a file with 10K+ rows and assert exactly 10 rows are returned.

**Unit test: `maxRowsWithFilterLimitsMatchingRows`** — combine `maxRows` with a `FilterPredicate` and assert that the limit applies to matching (output) rows, not scanned (input) rows.

**S3 JFR test: `maxRowsStopsEarly`** — read with `maxRows = 10` from a large multi-row-group S3 file and assert that bytes transferred are comparable to the phase 1/2 lazy tests — confirming that the reader doesn't fetch beyond what is needed and that prefetch work is cancelled.

### Existing tests

All existing reader tests (flat, nested, filtered, projected, multi-file, S3) must continue to pass, validating that lazy row-group, page-level, and row-limit transitions produce identical results.

## Files to modify

### Phase 1 (implemented)

- `core/src/main/java/dev/hardwood/internal/reader/RowGroupPageSource.java` — new functional interface
- `core/src/main/java/dev/hardwood/internal/reader/PageCursor.java` — row-group boundary tracking, `tryLoadNextRowGroupBlocking()`, `hasMoreRowGroups()`, `fetchRowGroupPages()`; row-group state reset on file boundary crossings
- `core/src/main/java/dev/hardwood/reader/SingleFileRowReader.java` — `initialize()` scans first row group only; `getPagesForRowGroup()` with synchronized caching
- `core/src/main/java/dev/hardwood/internal/reader/FileManager.java` — `scanFirstRowGroup()`, `PerFileRowGroupContext`, `getRowGroupPages()`, `getRowGroupCount()`, `scanRowGroupForColumn()`
- `core/src/main/java/dev/hardwood/reader/ColumnReader.java` — `create()` scans first row group only; `scanRowGroup()` as `RowGroupPageSource`
- `s3/src/test/java/dev/hardwood/s3/TestParquetGenerator.java` — pure Java Parquet file generator
- `s3/src/test/java/dev/hardwood/s3/S3SelectiveReadJfrTest.java` — 4 new tests

### Phase 2

- `core/src/main/java/dev/hardwood/internal/reader/PageScanner.java` — transform from batch (`scanPages()`) to iterator (`hasNextPage()`/`nextPage()`) with windowed fetching
- `core/src/main/java/dev/hardwood/internal/reader/PageCursor.java` — pull pages from scanner on demand in `fillPrefetchQueue()`
- `core/src/main/java/dev/hardwood/reader/SingleFileRowReader.java` — `getPagesForRowGroup()` returns lazy scanner
- `core/src/main/java/dev/hardwood/internal/reader/FileManager.java` — `scanRowGroupForColumn()` returns lazy scanner
- `core/src/main/java/dev/hardwood/reader/ColumnReader.java` — `scanRowGroup()` returns lazy scanner
- `s3/src/test/java/dev/hardwood/s3/S3SelectiveReadJfrTest.java` — add intra-row-group lazy fetch test

### Phase 3

- `core/src/main/java/dev/hardwood/reader/AbstractRowReader.java` — add `maxRows` field, check in `hasNext()`, signal early termination
- `core/src/main/java/dev/hardwood/reader/ParquetFileReader.java` — add `createRowReader` overloads with `maxRows`
- `core/src/main/java/dev/hardwood/reader/SingleFileRowReader.java` — propagate `maxRows`, use for row-group skipping
- `core/src/main/java/dev/hardwood/internal/reader/PageCursor.java` — support cancellation of in-flight prefetch futures
- `cli/src/main/java/dev/hardwood/cli/command/PrintCommand.java` — pass `-n` to `createRowReader()` instead of stream `.limit()`
- `docs/content/usage.md` — document `maxRows` parameter
