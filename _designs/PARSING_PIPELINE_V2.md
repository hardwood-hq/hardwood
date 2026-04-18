# Parsing Pipeline v2

**Status:** Completed

## Goals

1. **Maximize CPU utilization.** Every available core should be doing decode work whenever there is data to process.
2. **Structural deadlock prevention.** The design makes deadlock impossible by construction, not by workaround.
3. **Safe array hand-off.** Users never observe mutated data. Internal array reuse is fine but the boundary is explicit and enforced.
4. **No slowdown at file transitions.** Transitioning from one file to the next does not stall the pipeline. The next file's data is already in flight by the time the current file is exhausted.
5. **Minimize requests to remote object store.** Within-column page coalescing merges nearby matching pages into fewer `readRange()` calls. Pages beyond the consumer's needs are not fetched.
6. **Unified flat and nested paths.** One pipeline structure (`RowGroupIterator → PageSource → ColumnWorker → BatchExchange`) handles both flat and nested schemas. Assembly is specialized per schema type via `FlatColumnWorker` and `NestedColumnWorker`.
7. **Unified single-file and multi-file paths.** A single-file read is a multi-file read with one file. No separate reader classes.
8. **Clean architecture.** Each component has one job. Components are independently testable. The pipeline structure is explicit and visible.
9. **More resources for slower columns.** Columns can be differently complex to decompress and decode. More resources are given to slower columns so they don't slow down fast columns.
10. **I/O-decode overlap for remote files.** Fetching chunk N+1 overlaps with decoding pages from chunk N. The consumer sees the first decoded rows while later chunks are still downloading.
11. **Minimize over-fetching.** When `maxRows` is set, byte ranges are narrowed to only the needed pages before fetching. When the consumer stops advancing the cursor, back-pressure and cancellation ensure unfetched chunks are never downloaded.
12. **Simplicity.** One thread pool. No cross-pool scheduling. Minimal synchronization primitives.

## Architecture

### Single Pool

All virtual threads use the JVM's default virtual thread scheduler, which carries them on an internal `ForkJoinPool`. This pool is shared with other virtual threads in the process. Since virtual threads unmount from carriers during blocking operations (I/O, queue waits, `LockSupport.park()`), carrier sharing is cooperative — Hardwood's decode work does not monopolize carriers even under heavy load. No separate executor is needed.

### Components

There are four components in the pipeline.

#### 1. RowGroupIterator

Shared across all columns. Manages the ordered sequence of `(InputFile, RowGroup)` pairs that the pipeline processes.

- Opens files, reads footers (`ParquetMetadataReader`), validates schema compatibility against the reference schema from the first file.
- Filters row groups by statistics (`RowGroupFilterEvaluator`).
- Applies `maxRows` limiting at the row-group level: computes how many row groups are needed based on metadata row counts. Row groups beyond the budget are excluded.
- Prefetches the next file asynchronously via `CompletableFuture`: while the current file is being read, the next file is opened and its metadata is parsed in the background.
- Caches per-row-group shared metadata (index buffers, matching rows) in a `ConcurrentHashMap`. The first column to enter a row group populates the cache; subsequent columns reuse it.
- Plans I/O for each row group: produces a `FetchPlan` per projected column. See [I/O Planning and Fetching](#io-planning-and-fetching).
- Pre-plans the next row group asynchronously when the current row group's plans are first computed.

Each `PageSource` maintains its own cursor into the `RowGroupIterator`'s work list.

#### 2. PageSource

Iterates through row groups and yields `PageInfo` objects for a single column. One per column. This is the only interface the `ColumnWorker` sees — a simple `PageInfo next()` iterator.

For each row group, obtains a `FetchPlan` from `RowGroupIterator.getColumnPlan()` and drains its page iterator. `PageSource` is fully agnostic of whether pages were pre-computed (OffsetIndex) or are being discovered lazily (sequential scan) — both are hidden behind the `FetchPlan` iterator.

#### 3. ColumnWorker

Two long-lived virtual threads per column. Drives the per-column pipeline from page retrieval through decode to batch assembly.

**Retriever VThread:**
- Pulls `PageInfo` objects from `PageSource`.
- Submits decode tasks to a shared executor. Each task decodes one page: `PageDecoder.decodePage(pageData, dictionary) → Page`.
- Throttles itself when the gap between submitted and drained pages reaches `MAX_INFLIGHT_PAGES` (default 8, configurable via `hardwood.internal.maxOutstanding`). Parking via `LockSupport.park()`; unparked when the drain advances `consumePosition`.

**Drain VThread:**
- Reads decoded pages from a circular reorder buffer (`AtomicReferenceArray<Page>`, indexed by `seqNum % MAX_INFLIGHT_PAGES`) in sequence order.
- Assembles pages into batches via subclass-specific logic and publishes to the `BatchExchange`.
- Parks when no ready pages are available; unparked by decode tasks storing results.

The `AtomicReferenceArray` avoids the GC pressure of `ConcurrentHashMap` (no integer boxing, no `Node` allocations). Decode tasks store via `set()` and unpark the drain thread. The drain consumes via `getAndSet(slot, null)`.

On exhaustion, the retriever stores an `EMPTY_SENTINEL` in the reorder buffer. The drain publishes any partial batch and marks the pipeline as finished.

Two concrete subclasses specialize assembly:

- **`FlatColumnWorker`:** Assembles flat pages via `System.arraycopy` into pre-allocated fixed-size arrays. Tracks nulls via `BitSet` from definition levels. Batch boundary: `rowsInCurrentBatch >= batchCapacity`.
- **`NestedColumnWorker`:** Assembles nested pages into growing arrays for values, definition levels, repetition levels, and record offsets. Record boundary detection: `repLevel == 0` at a non-first position starts a new record. Batch boundary: `recordsInCurrentBatch >= batchCapacity`. Before publishing, computes index structures on the batch (element nulls, multi-level offsets, level nulls via `NestedLevelComputer`) so the consumer thread does no expensive computation.

Both enforce `maxRows` in the drain: assembly stops and the pipeline finishes once the budget is exhausted.

#### 4. BatchExchange

Generic exchange buffer (`BatchExchange<B>`) between the `ColumnWorker` drain thread and the consumer. One per column.

Internally, two `ArrayBlockingQueue`s:

- `readyQueue` (capacity 2): drain → consumer. Holds completed batches.
- `freeQueue` (capacity 3 in recycling mode): consumer → drain. Holds returned batch holders for reuse.

Two consumption modes:

- **Recycling mode** (`BatchExchange.recycling()`): Pre-allocates 3 batch holders (`READY_QUEUE_CAPACITY + 1`) that cycle between drain and consumer. No per-batch allocation. Used by `FlatRowReader` and `NestedRowReader`.
- **Detaching mode** (`BatchExchange.detaching()`): Allocates a fresh batch via a factory each time the drain needs one. The consumer keeps ownership of each batch permanently. Back-pressure comes from the bounded `readyQueue` only. Used by `ColumnReader` where the caller retains arrays between batches.

**Consumer polling:** `BatchExchange.poll()` encapsulates the consumer-side protocol. A non-blocking poll is attempted first; if the queue is empty, a timed poll loop follows. The `finished` flag is only checked after a timed poll returns null, guaranteeing that any batch published before `finish()` is visible. A JFR `BatchWaitEvent` is emitted when the consumer enters the timed poll loop (deferred past the first 10ms to avoid allocation on the fast path).

Two batch types:

- **`BatchExchange.Batch`** (flat): `Object values`, `BitSet nulls`, `int recordCount`.
- **`NestedBatch`** (nested): Raw arrays (`Object values`, `int recordCount`, `int valueCount`, `int[] definitionLevels`, `int[] repetitionLevels`, `int[] recordOffsets`) plus pre-computed index structures (`BitSet elementNulls`, `int[][] multiLevelOffsets`, `BitSet[] levelNulls`). The index fields are computed by the drain before publishing.

### Per-Column Pipeline

```
ColumnWorker                                                      BatchExchange        Consumer
├── Retriever VThread           Decode Tasks                                           (user thread)
│   loop:                       (on shared executor)
│     pageInfo = PageSource.next()
│     submit decode(pageInfo) ──────→ reorderBuffer[seq % N]
│     if gap >= maxOutstanding: park                                    ↓
│        unpark ←── drain advances                                readyQueue
│                                                                      ↓
├── Drain VThread                                                   Consumer
│   loop:                                                              │
│     reorderBuffer[pos % N] ──→ assemblePage ──→ publishBatch ──→ readyQueue
│     consumePosition++                                                │
│     unpark retriever                                                 │
│        unpark ←── decode task stores result            freeQueue ←── return batch
```

### Readers

Two independent `final` reader classes consume from the pipeline:

- **`FlatRowReader`:** Direct primitive array access (`((double[]) flatValueArrays[col])[rowIndex]`). No intermediate `DataView` indirection. JIT-friendly monomorphic call sites.
- **`NestedRowReader`:** Delegates accessors to `NestedBatchDataView`, which manages multi-level offset navigation and nested type traversal via `NestedBatchIndex`. Index structures (element nulls, multi-level offsets, level nulls) are pre-computed by the `NestedColumnWorker` drain thread before publishing, so the reader's batch transition is trivial — just assemble the cross-column `NestedBatchIndex` from per-column `NestedBatch` fields.

Both are intentionally separate classes despite similar iteration structure. The flat path avoids all indirection for JIT inlining; the nested path requires delegation for struct/list/map traversal. Sharing a base class would pollute the flat fast path.

**`FilteredRowReader`** wraps any `RowReader` (flat or nested) for record-level predicate filtering. Advances the delegate row-by-row via `RecordFilterEvaluator.matchesRow()`, which navigates nested struct paths automatically using the `FileSchema` field paths, so predicates on nested leaf columns (e.g. `address.zip`) are supported. Implemented as a separate wrapper so the unfiltered path pays zero overhead.

**`RowReader.create()`** is a static factory on the `RowReader` interface that dispatches to `FlatRowReader.create()` or `NestedRowReader.create()` based on `schema.isFlatSchema()`, then wraps with `FilteredRowReader` if a filter is present. Both `ParquetFileReader` and `MultiFileParquetReader` delegate to this factory.

**`ColumnReader`** (column-oriented API) also uses the v2 pipeline with `BatchExchange` in detaching mode, so callers retain array ownership between batches. For nested columns, index structures are pre-computed by the drain and read directly from the `NestedBatch`.

### Full Pipeline Visualization

```
                                     Column 0                 Column 1                 Column 2

RowGroupIterator                     PageSource               PageSource               PageSource
┌──────────────┐                     │ next()                 │ next()                 │ next()
│ open file    │                     ↓                        ↓                        ↓
│ read footer  │──→ shared state     ColumnWorker             ColumnWorker             ColumnWorker
│ validate     │    (index bufs,     ├─ Retriever VT          ├─ Retriever VT          ├─ Retriever VT
│ filter RGs   │     matching rows,  ├─ Drain VT              ├─ Drain VT              ├─ Drain VT
│ prefetch N+1 │     fetch plans)    │ Decode Tasks           │ Decode Tasks           │ Decode Tasks
│ maxRows      │    InputFile        │ ┌──┬──┬──┐            │ ┌──┬──┬──┐            │ ┌──┬──┬──┐
└──────────────┘    (per file)       │ │  │  │  │..          │ │  │  │  │..          │ │  │  │  │..
                                     │ └──┴──┴──┘            │ └──┴──┴──┘            │ └──┴──┴──┘
                                     │    ↓ reorder           │    ↓ reorder           │    ↓ reorder
                                     │    ↓ drain              │    ↓ drain              │    ↓ drain
                                     BatchExchange            BatchExchange            BatchExchange
                                     ┌────────────┐          ┌────────────┐          ┌────────────┐
                                     │ drain→ready│          │ drain→ready│          │ drain→ready│
                                     │ free←reader│          │ free←reader│          │ free←reader│
                                     └─────┬──────┘          └─────┬──────┘          └─────┬──────┘
                                           │                       │                       │
                                           └───────────┬───────────┘───────────┬───────────┘
                                                       │                       │
                                                Consumer (user thread)

All virtual threads run on the JVM's default virtual thread scheduler.
```

### Data Flow

```
RowGroupIterator ──→ PageSource ──→ ColumnWorker ──→ Decode Tasks ──→ Drain ──→ BatchExchange ──→ Consumer
  (shared)           (per column)   (per column)      (short-lived)             (per column)
```

### Thread Inventory

| Thread type | Count | Lifetime | What it does |
|-------------|-------|----------|-------------|
| Retriever VThread | 1 per column | Long (entire read) | Pulls PageInfos, submits decode tasks, throttles |
| Drain VThread | 1 per column | Long (entire read) | Assembles decoded pages into batches, publishes |
| Decode task | up to K per column | Short (one page) | Decodes one page, stores result, unparks drain |
| Consumer | 1 (user's thread) | User-managed | Takes batches from BatchExchanges |

For 3 columns: 6 long-lived VThreads (3 retriever + 3 drain) + short-lived decode tasks. All VThreads share `availableProcessors()` carrier threads.

### Slow/Fast Column Balancing

All columns share `ForkJoinPool.commonPool()` carriers with work-stealing.

When column A is slow (2ms/page decode):
- A's decode tasks hold carriers for 2ms each.
- A's retriever is ahead — it has submitted pages that are waiting for carriers.

When column B is fast (0.1ms/page decode):
- B's decode tasks complete quickly, freeing carriers.
- B's drain processes pages fast, batches fill up, the BatchExchange fills.
- B's drain blocks on `readyQueue.offer()`. The VThread unmounts from its carrier.
- B's retriever hits the `MAX_INFLIGHT_PAGES` threshold and parks, freeing its carrier.
- Carriers freed by B become available for A's decode work.

When the consumer consumes B's batch, B's drain unblocks, processes accumulated pages in a burst, and the pipeline resumes.

Net effect: slow columns hold carriers longer, naturally claiming more CPU. Fast columns self-limit through BatchExchange back-pressure. The pool's work-stealing ensures no carrier sits idle when there's work available. No explicit balancing logic.

### Back-Pressure Chain

```
Consumer doesn't take from readyQueue
  → BatchExchange full, readyQueue.offer() blocks with timeout
  → Drain blocks on publish (VThread unmounts from carrier)
  → Decoded pages accumulate in reorder buffer
  → Retriever hits MAX_INFLIGHT_PAGES threshold, parks
  → PageSource stops yielding (retriever not pulling)
  → Carriers freed by parked/blocked VThreads become available for other columns

Consumer takes from readyQueue, returns batch to freeQueue
  → Drain unblocks, publishes, takes next free batch
  → Drain processes accumulated pages, advances consumePosition
  → Retriever unparks (consumePosition advanced), resumes pulling from PageSource
```

Back-pressure propagates all the way from the consumer to the PageSource. The reorder buffer is bounded to `MAX_INFLIGHT_PAGES` (default 8) decoded pages by the retriever throttle.

### I/O Planning and Fetching

I/O is split into two phases: **planning** (pure metadata work, no network I/O) and **fetching** (actual `readRange()` calls, triggered by the retriever or by one-ahead pre-fetch).

#### Planning

`RowGroupIterator` owns I/O planning. When the first `PageSource` enters a row group, `getColumnPlan()` computes and caches a `FetchPlan` per projected column. The plan determines which pages exist and where their bytes live, but does not fetch any data.

For each projected column with an OffsetIndex:

1. Parse the column's OffsetIndex → get all page locations.
2. Apply filter: if `RowRanges` is not ALL, keep only pages overlapping matching rows. If no pages match → `FetchPlan.EMPTY`.
3. Apply `maxRows`: truncate to pages covering the needed row prefix.
4. Coalesce needed pages within the column into page groups (1 MB gap tolerance, 128 MB max group size), creating one `ChunkHandle` per group. Include dictionary prefix in the first group.
5. Link chunk handles for one-ahead pre-fetch: fetching chunk N triggers async fetch of chunk N+1 (but not further — the chain does not cascade).
6. Create an `IndexedFetchPlan` with the page list and chunk handles.

For columns without an OffsetIndex:

1. Create a `SequentialFetchPlan` that discovers pages lazily from fixed-size `ChunkHandle`s.
2. Chunk size: `min(chunkLength, 4 MB)` with `maxRows`, `min(chunkLength, 128 MB)` without.

After computing plans for a row group, async pre-planning of the next row group is triggered. This computes the next row group's plans (pure metadata) and pre-fetches the first chunk handle, overlapping with decode of the current row group's pages.

#### FetchPlan

A `FetchPlan` exposes an iterator of `PageInfo` for a single projected column in a row group:

```java
interface FetchPlan {
    boolean isEmpty();
    Iterator<PageInfo> pages();
    default void prefetch() {}
}
```

Both implementations resolve page bytes eagerly during iteration. `PageInfo` is a simple data holder — a resolved `ByteBuffer` plus column metadata and dictionary.

- **`IndexedFetchPlan`**: pages pre-computed from OffsetIndex at plan time. The iterator parses the dictionary on first access, then resolves each page's bytes via `ChunkHandle.slice()` (which triggers lazy chunk fetching and one-ahead pre-fetch). The resolved `ByteBuffer` is stored in the `PageInfo`.
- **`SequentialFetchPlan`**: pages discovered lazily by scanning headers from fixed-size `ChunkHandle`s. Dictionary parsed on first access. Each chunk is chained for one-ahead pre-fetch. When a page fits within the current chunk, its bytes are a zero-copy slice. When a page straddles multiple chunks, bytes are assembled by reading from each chunk and concatenating.

`PageSource` is fully agnostic — it just drains `plan.pages()`.

#### ChunkHandle

`ChunkHandle` is a lazy fetch handle for a contiguous byte range within a column. Multiple pages share a `ChunkHandle` when their byte ranges are coalesced (indexed path) or fall within the same fixed-size chunk (sequential path).

When `ensureFetched()` is called:

1. If data is already cached (volatile read), return immediately.
2. Otherwise, call `inputFile.readRange()` to fetch the chunk (synchronized, double-checked).
3. After fetching, trigger async pre-fetch of the next `ChunkHandle` in the chain via `fetchData()`. Pre-fetch is strictly one-ahead — `fetchData()` fetches the data but does not chain further, preventing recursive cascade.

Each column fetches independently — no column blocks waiting for another column's data. All columns' retrievers run concurrently on separate VThreads.

For local files, `readRange()` returns zero-copy mmap slices — no actual I/O, and pre-fetch is effectively a no-op.

#### I/O-Decode Overlap

```
Chunk 1: ──fetch──→ pages A, B, C → decode A ──→ decode B ──→ decode C
Chunk 2:              ──prefetch──→              pages D, E → decode D → decode E
Chunk 3:                                          ──prefetch──→         ...
```

When the retriever enters chunk 1 (via header scanning or page resolution), `ensureFetched()` fetches chunk 1 and pre-fetches chunk 2. While pages A/B/C are decoded, chunk 2 downloads in the background. By the time the retriever reaches chunk 2 (already fetched), chunk 3 is pre-fetched.

#### I/O Scenarios

| # | OffsetIndex | Filter | maxRows | I/O per column per row group |
|---|-------------|--------|---------|------------------------------|
| 1 | Yes | No | No | One or more `ChunkHandle`s per column (128 MB max per handle). Fetched on first page access. |
| 2 | Yes | No | Yes | One or more `ChunkHandle`s per column = dict + pages up to maxRows. |
| 3 | Yes | Yes (selective) | No | 1-N `ChunkHandle`s per column covering matching page groups (1 MB gap coalescing, 128 MB max). Non-matching pages' bytes never fetched. |
| 4 | Yes | Yes (selective) | Yes | Same as #3, further truncated to maxRows prefix. |
| 5 | Yes | Yes (none match) | Any | `FetchPlan.EMPTY`. No I/O. |
| 6 | No | Any | No | `SequentialFetchPlan` with 128 MB chunks. Full column chunk fetched on first access for most columns. |
| 7 | No | Any | Yes | `SequentialFetchPlan` with 4 MB chunks. Only needed chunks fetched. |

All OffsetIndex scenarios (#1-5) go through the same code path in `RowGroupIterator.computeFetchPlans()`. The only branch is OffsetIndex present vs absent. Files without an OffsetIndex also lack a Column Index (the Parquet spec requires OffsetIndex when Column Index is present), so no page-level filtering is possible — the filter is applied at the row-group level only.

### Row Limits (`maxRows`)

When the consumer requests only N rows, waste is minimized at four levels:

**Row-group level:** The `RowGroupIterator` computes how many row groups are needed based on `maxRows` and row-group metadata row counts. Row groups beyond the budget are excluded.

**I/O level:** When planning byte ranges, `RowGroupIterator` truncates each column's page list to pages covering the needed row prefix. Only those pages' bytes are included in chunk handles. For the sequential path, 4 MB chunks limit per-request over-fetch.

**Page level:** `SequentialFetchPlan` tracks `valuesRead` and stops yielding pages once enough rows are covered.

**Drain level:** The `ColumnWorker` drain counts assembled rows. Once `totalRowsAssembled >= maxRows`, it publishes the partial batch and finishes the pipeline.

**Decode granularity:** Pages are the smallest decode unit. If `maxRows` falls within a page, the full page is decoded and a partial batch is assembled.

### Row Group and File Transitions

#### Local Files

For local files, `RowGroupIterator` resolves all page locations and byte ranges at row-group entry. Since `MappedInputFile.readRange()` returns zero-copy mmap slices, the chunk "fetch" is instant — no actual I/O, just buffer slicing.

File transitions use async preflight via `RowGroupIterator`: while the current file is being read, the next file is opened and its metadata is parsed in the background. By the time the `PageSource` exhausts the current file's pages, the next file's metadata is ready.

#### Remote Files

For remote files with OffsetIndex, `RowGroupIterator` plans narrowed per-column byte ranges and creates `ChunkHandle`s. Fetching happens when the retriever resolves page bytes during iteration, with one-ahead pre-fetch overlapping with decode of the current chunk's pages. Each column fetches independently — no cross-column blocking.

For remote files without OffsetIndex, `SequentialFetchPlan` discovers pages lazily by scanning headers from fixed-size `ChunkHandle`s with one-ahead pre-fetch. Chunk size is `min(chunkLength, 128 MB)` without `maxRows`, or `min(chunkLength, 4 MB)` with `maxRows`. Pages that straddle chunk boundaries are assembled by reading from adjacent chunks.

File transitions work the same as for local files: the `RowGroupIterator` prefetches the next file asynchronously. Additionally, entering a row group triggers async pre-planning of the next row group's fetch plans.

### Error Propagation

A `volatile Throwable error` field in `ColumnWorker` captures the first error from any stage.

**Decode task errors:** A decode task that throws calls `signalError(t)`, which sets `finished = true`, propagates the error to the `BatchExchange`, and unparks both threads. Subsequent decode tasks check `error` before decoding and exit early if set.

**PageSource errors:** The retriever catches exceptions from `PageSource.next()`, calls `signalError(t)`.

**Consumer visibility:** `BatchExchange.checkError()` surfaces the error when the consumer detects an empty or finished state. Any batches that were successfully queued before the error are delivered first — the consumer sees the error only after draining completed work.

### Cancellation and Early Close

When the consumer calls `close()`:

1. `closed` is set to `true` on each `ColumnWorker`.
2. `exchange.finish()` is called to signal the BatchExchange's timeout loops to exit.
3. Both threads (retriever and drain) are unparked so they can observe the flag and exit.
4. The retriever stops pulling from `PageSource`. The drain stops processing pages.
5. Decode tasks check `closed` before decoding — unfetched chunks are never accessed.

### Component Responsibilities

| Component | Responsibility |
|-----------|---------------|
| `RowGroupIterator` | Opens files, reads footers, validates schemas, filters row groups, prefetches next file, plans per-RG I/O (per-column page groups → `ChunkHandle`s). Async pre-plans next row group. Shared across columns. |
| `FetchPlan` | Iterator of `PageInfo` for one column in one row group. `IndexedFetchPlan` (OffsetIndex: pages pre-computed, coalesced `ChunkHandle`s) or `SequentialFetchPlan` (no OffsetIndex: pages discovered lazily from fixed-size `ChunkHandle`s with one-ahead pre-fetch). |
| `ChunkHandle` | Lazy fetch handle for a contiguous byte range within a column. Defers `readRange()` until first access. Triggers one-ahead async pre-fetch of next handle (no cascade). |
| `DictionaryParser` | Parses dictionary pages from column chunk data. Used by both `IndexedFetchPlan` and `SequentialFetchPlan`. |
| `PageInfo` | Resolved page data (`ByteBuffer`) plus column metadata and dictionary. Simple data holder — no lazy I/O. |
| `PageSource` | Drains `FetchPlan.pages()` iterator. Fully agnostic of indexed vs sequential. Yields `PageInfo`. One per column. |
| `FlatColumnWorker` | Flat assembly: `System.arraycopy` into fixed-size arrays, null tracking via `BitSet`. |
| `NestedColumnWorker` | Nested assembly: growing arrays, record boundary detection via `repLevel == 0`. Pre-computes index structures before publishing. |
| `BatchExchange` | Generic exchange buffer between drain and consumer. Recycling mode (RowReader) or detaching mode (ColumnReader). `poll()` encapsulates the consumer-side protocol with correct `finished`-flag ordering. One per column. |
| `FlatRowReader` | Direct primitive array access, monomorphic JIT path. |
| `NestedRowReader` | Assembles `NestedBatchIndex` from pre-computed `NestedBatch` fields, delegates to `NestedBatchDataView`. |
| `FilteredRowReader` | Record-level predicate filtering via `RecordFilterEvaluator.matchesRow()`. Wraps any `RowReader`. |
| `RowReader.create()` | Static factory dispatching flat vs nested based on `schema.isFlatSchema()`. |
| `ColumnReader` | Column-oriented API, detaching mode, reads pre-computed index from `NestedBatch`. |

### Sizing

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| BatchExchange ready queue | 2 | Two queued batches + one being filled |
| BatchExchange free pool (recycling) | 3 | `READY_QUEUE_CAPACITY + 1` — one filling, up to two queued |
| MAX_INFLIGHT_PAGES | 8 (default) | Bounds decoded page retention and GC pressure. Configurable via `hardwood.internal.maxOutstanding` |
| Carrier threads | `availableProcessors()` | One per core, managed by the JVM's virtual thread scheduler |
| Batch size | L2-cache-adaptive | `6 MB / bytesPerRow`, clamped to [16K, 512K] rows |
| Within-column page coalescing gap | 1 MB | Matching pages within 1 MB are merged into a single `ChunkHandle` |
| Maximum coalesced group size | 128 MB (configurable via `hardwood.internal.maxCoalescedBytes`) | Coalesced groups exceeding this are split for bounded `readRange()` calls |
| Sequential chunk size (no maxRows) | 128 MB (configurable via `hardwood.internal.sequentialChunkSize`) | Full column chunk in one fetch for most columns |
| Sequential chunk size (with maxRows) | 4 MB | Limits over-fetch for partial reads |
