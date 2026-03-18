# Plan: Coalesced Reads for Remote Backends

## Context

On local files, `InputFile.readRange()` returns a zero-copy mmap slice — each call
costs nanoseconds regardless of how many calls are made. On remote backends (S3,
HTTP), each `readRange()` is a network round-trip (~20ms on S3). The reader's I/O
pattern — many small, independent reads — must be restructured into fewer, larger
requests.

### Problems in the baseline read pattern

Three layers of fine-grained reads compound on remote backends:

**1. Per-column offset index reads.** `PageScanner` is instantiated per-column
per-row-group. Each instance independently reads its column's offset index from the
footer area. For 20 projected columns, that's 20 small `readRange()` calls to a
contiguous region — 20 round-trips that could be 1.

**2. Per-page data reads.** When using the offset index path,
`scanPagesFromIndex()` issues one `readRange()` per data page. A column chunk with
50 pages costs 50 round-trips. The sequential path reads the entire chunk in 1
call, but the offset index path trades I/O granularity for page-level precision.

**3. Per-column chunk reads.** Each projected column's chunk is fetched
independently. For 20 columns, that's 20 reads — even though the chunks are stored
contiguously in the file and could be fetched in 1.

For 20 columns, 3 row groups, and 50 pages per chunk, the baseline produces
~3,180 `readRange()` calls.

### Design decisions

**Index metadata:** Pre-fetch all offset/column indexes for a row group in a
single `readRange()` call. The indexes are stored contiguously in the footer area.

**Column chunk data:** Coalesce projected column chunks within each row group into
minimal range requests using a gap tolerance. Adjacent chunks merge; non-projected
columns between projected ones create gaps that are bridged if within the
tolerance (1 MB). Coalescing is scoped to a single row group to bound memory.

**Page slicing:** With chunk data pre-fetched, `PageScanner` slices pages directly
from the buffer — no per-page `readRange()` calls needed, regardless of whether
the sequential or offset index path is used.

**`PageRange` for future page-level filtering.** The `PageRange` type (page-level
coalescing) is not used on the current read path — chunk-level coalescing
supersedes it. It is retained for page-level predicate pushdown (#118): when
Column Index filtering skips pages within a chunk, only matching page groups
should be fetched rather than the entire chunk.

---

## Step 1: Pre-fetch offset/column indexes per row group

### New domain types

`ColumnIndexBuffers` — raw offset/column index buffers for a single column chunk.
`RowGroupIndexBuffers` — fetches all indexes for a row group in one `readRange()`,
provides per-column access via `forColumn(int)`.

```java
record ColumnIndexBuffers(ByteBuffer offsetIndex, ByteBuffer columnIndex) {}

class RowGroupIndexBuffers {
    ColumnIndexBuffers forColumn(int columnIndex) { ... }

    static RowGroupIndexBuffers fetch(InputFile inputFile,
            RowGroup rowGroup) throws IOException {
        // Compute spanning range across all columns' index entries
        // Single readRange() for the entire region
        // Slice per-column buffers
    }
}
```

### Callers

`SingleFileRowReader`, `ColumnReader`, and `FileManager` call
`RowGroupIndexBuffers.fetch()` once per row group before dispatching per-column
scanning, and pass `ColumnIndexBuffers` to each `PageScanner`.

**Result:** C index reads per row group → **1 read** per row group.

**Files:**
- `core/.../internal/reader/ColumnIndexBuffers.java` (new)
- `core/.../internal/reader/RowGroupIndexBuffers.java` (new)
- `core/.../internal/reader/PageScanner.java` (accept `ColumnIndexBuffers`)
- `core/.../reader/SingleFileRowReader.java` (call `fetch()`)
- `core/.../reader/ColumnReader.java` (same)
- `core/.../internal/reader/FileManager.java` (same)

---

## Step 2: Coalesce column chunk reads within a row group

### New domain type: `ChunkRange`

```java
record ChunkRange(long offset, int length, List<ChunkEntry> entries) {

    record ChunkEntry(int columnIndex, long chunkOffset, int chunkLength) {}

    static List<ChunkRange> coalesce(List<ColumnChunk> columns,
            int[] projectedColumns, int maxGapBytes) { ... }
}
```

For each row group, the caller collects projected column chunks, sorts by file
offset, and coalesces adjacent/nearby ones. Each `ChunkRange` is fetched in one
`readRange()` call and sliced per-column.

### Gap tolerance

`MAX_GAP_BYTES = 1 MB`. Non-projected columns between two projected ones create
gaps. If within tolerance, the gap is fetched and discarded — one round-trip
instead of two. For local files the tolerance is harmless (zero-copy slicing).

### Behavior by scenario

| Scenario | Chunks per RG | Coalesced ranges per RG |
|----------|---------------|-------------------------|
| All 20 columns projected | 20 contiguous | **1 range** |
| Columns 0, 1, 2, 10 | 3 + gap + 1 | **1-2 ranges** |
| 1 column | 1 | **1 range** |

### Row group boundary

Coalescing does **not** cross row group boundaries. Merging row groups would hold
the entire data region in memory, preventing earlier row groups' buffers from
being GC'd. Each row group is coalesced independently, bounding memory to one row
group's data.

### No maximum range size (future consideration)

Currently there is no upper limit on the size of a coalesced range. If all 20
columns are projected and contiguous, the entire row group (potentially gigabytes)
is fetched in a single `readRange()`. This is fine for local files (zero-copy) and
for typical row groups (128 MB–1 GB), but could become a concern for very large
row groups on remote backends: the entire row group must be held in heap, and
column 0 can't be processed until the full range has been downloaded.

Splitting into parallel GETs would reduce download latency but not peak memory —
the full row group still ends up in heap. A better approach would be to **stream
the coalesced GET response**: issue a single request for the full range but
consume the response as an `InputStream`, handing off each column chunk's bytes
as soon as they arrive in file order. Column 0's `PageScanner` can be dispatched
while column 1's bytes are still downloading, giving 1 round-trip, pipelined
processing, and bounded memory (release each chunk buffer after scanning). This
would require either a streaming API on `InputFile` or caller-level logic that
reads the S3 response stream incrementally. Not needed for typical workloads but
the right direction for multi-GB row groups.

### Impact on `PageScanner`

`PageScanner` no longer takes an `InputFile`. It receives a pre-fetched
`ByteBuffer` for its column chunk data, plus `ColumnIndexBuffers` for its indexes:

```java
public PageScanner(ColumnSchema columnSchema, ColumnChunk columnChunk,
        HardwoodContextImpl context, ByteBuffer chunkData,
        long chunkDataFileOffset, ColumnIndexBuffers indexBuffers,
        int rowGroupIndex, String fileName)
```

- `scanPagesSequential()` works directly from the buffer
- `scanPagesFromIndex()` parses the offset index from `ColumnIndexBuffers` and
  slices pages from the buffer

### Interaction with row group filtering

Pruned row groups are excluded from the coalescing loop. Only kept row groups
contribute chunks. Filtering and coalescing compose naturally.

### Interaction with page-level filtering (future, #118)

The pre-fetched chunk buffer contains the full chunk. When page filtering is
implemented, `scanPagesFromIndex()` will only slice matching pages — unmatched
bytes are unused memory. For scenarios where skipping large sections of a chunk
matters, `PageRange` can be reintroduced at the `readRange()` level to fetch only
matching page groups.

**Files:**
- `core/.../internal/reader/ChunkRange.java` (new)
- `core/.../internal/reader/PageRange.java` (new — retained for future #118)
- `core/.../internal/reader/PageScanner.java` (takes `ByteBuffer` + `ColumnIndexBuffers`)
- `core/.../reader/SingleFileRowReader.java` (coalesce + pre-fetch per RG)
- `core/.../reader/ColumnReader.java` (same)
- `core/.../internal/reader/FileManager.java` (same)

---

## Step 3: Tests

Three layers: pure-logic unit tests, I/O call-count tests against real Parquet
files, and end-to-end S3 integration tests.

Test file: `page_index_test.parquet` — 3 columns (`id`, `value`, `category`),
small page size (4096 bytes → many pages), offset indexes enabled, uncompressed.

### `PageRange` unit tests

`PageRangeTest` — tests `coalesce()` and `extendStart()` with synthetic
`PageLocation` lists: contiguous merge, distant split, small gap merge, single
page, zero tolerance, dictionary prefix extension.

### `ChunkRange` unit tests

`ChunkRangeTest` — tests `coalesce()` with synthetic `ChunkEntry` lists:
adjacent merge, distant split, sparse projection with gaps, single chunk, zero
tolerance, overlapping chunks.

### `RowGroupIndexBuffers` call-count tests

`RowGroupIndexBuffersTest` — wraps `page_index_test.parquet` in a
`CountingInputFile` and asserts that `fetch()` issues exactly 1 `readRange()`
call for all columns' indexes. Also tests the no-index case with
`plain_uncompressed.parquet`.

### Existing `PageScannerTest`

The existing identity test (`scanPagesFromIndex()` vs `scanPagesSequential()`
produce identical decoded values) validates correctness automatically — the I/O
pattern changes but the results stay the same.

### S3 integration tests (`S3SelectiveReadJfrTest`)

- Projection byte comparison using `page_index_test.parquet` (170 KB > 64 KB
  tail cache, so socket-level differences are observable via `jdk.SocketRead`)
- Row group filtering using `RowGroupFilter` and `RowGroupScanned` JFR events

**Files:**
- `core/.../internal/reader/PageRangeTest.java` (new)
- `core/.../internal/reader/ChunkRangeTest.java` (new)
- `core/.../internal/reader/RowGroupIndexBuffersTest.java` (new)
- `core/.../internal/reader/CountingInputFile.java` (new — shared test utility)

---

## Summary of read pattern improvements

For a file with C projected columns, R row groups, and N pages per column chunk:

### Examples (20 columns, 3 row groups, 50 pages per chunk)

| Scenario | Before | After |
|----------|--------|-------|
| All columns projected | ~3180 | **~6** (3 index + 3 × 1 data range per RG) |
| 5 adjacent columns | ~795 | **~6** (3 index + 3 × 1 data range per RG) |
| 1 column | ~159 | **~6** (3 index + 3 × 1 data range) |
| All columns, filter keeps 1 RG | ~1060 | **~2** (1 index + 1 data range) |

---

## Verification

1. `./mvnw verify -pl core` — all tests pass
2. `./mvnw verify -pl s3` — S3 integration tests pass
3. All with 180s timeout to detect deadlocks
