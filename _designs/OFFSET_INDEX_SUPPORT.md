# Offset Index Support for Page Location Lookup

**Status: Implemented**

## Context

`PageScanner.scanPages()` currently discovers page boundaries by **sequentially reading every Thrift-encoded page header** through the entire column chunk. The Parquet format defines an **Offset Index** structure (per column chunk, stored near the file footer) that provides `(offset, size, firstRowIndex)` for each data page. When present, this eliminates the sequential scan — pages can be located by direct lookup.

Scope: **Offset Index only** (not Column Index, which is for predicate pushdown).

## Implementation

### Step 1: New metadata records

**`core/.../metadata/PageLocation.java`** (new file)
```java
public record PageLocation(long offset, int compressedPageSize, long firstRowIndex) {}
```
- `offset`: absolute file offset of the page
- `compressedPageSize`: total page size in file **including header** (per Thrift spec field 2 comment: "Size of the page, including header")
- `firstRowIndex`: index of the first row in this page within the row group

**`core/.../metadata/OffsetIndex.java`** (new file)
```java
public record OffsetIndex(List<PageLocation> pageLocations) {}
```

### Step 2: New Thrift readers

**`core/.../internal/thrift/PageLocationReader.java`** (new file)
Follow existing patterns (e.g., `DataPageHeaderReader`):
- Field 1 (i64): offset
- Field 2 (i32): compressedPageSize
- Field 3 (i64): firstRowIndex

**`core/.../internal/thrift/OffsetIndexReader.java`** (new file)
- Field 1 (list\<struct\>): pageLocations → delegate to `PageLocationReader`

### Step 3: Update `ColumnChunk` record

**`core/.../metadata/ColumnChunk.java`** — add two optional fields:
```java
public record ColumnChunk(
    ColumnMetaData metaData,
    Long offsetIndexOffset,
    Integer offsetIndexLength) {}
```

### Step 4: Update `ColumnChunkReader`

**`core/.../internal/thrift/ColumnChunkReader.java`** — parse fields 4 and 5:
- Field 4 (i64): `offset_index_offset`
- Field 5 (i32): `offset_index_length`

Update the constructor call: `new ColumnChunk(metaData, offsetIndexOffset, offsetIndexLength)`

### Step 5: Update `PageScanner` — extract strategies and add index-based scan

**`core/.../internal/reader/PageScanner.java`** — refactor into two package-private methods:

**`scanPagesSequential()`** — extract the current sequential scan logic (no behavior change)

**`scanPagesFromIndex()`** — new index-based path:
1. Read the OffsetIndex blob from `fileMapping` at `(offsetIndexOffset, offsetIndexLength)`
2. Parse via `OffsetIndexReader`
3. Validate that the offset index is non-empty (throw `IOException` if no page locations)
4. **Dictionary page**: if `dictionaryPageOffset` is set, parse the dictionary page at that offset. If `dictionaryPageOffset` is null/zero (as with parquet-mr <= 1.12), probe the page at `dataPageOffset` — if it is a dictionary page, parse it so data pages can reference it.
5. **Data pages**: for each `PageLocation`, create a `PageInfo` with buffer slice at `fileMapping.slice(offset - fileMappingBaseOffset, compressedPageSize)` — this slice contains header + compressed data, exactly as `PageReader.decodePage()` expects. No page headers are parsed here — the offset index provides all the information needed to locate pages.
6. Return `List<PageInfo>` (same API as today)

**`scanPages()`** — chooses automatically:
- If `columnChunk.offsetIndexOffset() != null` → call `scanPagesFromIndex()`
- Otherwise → call `scanPagesSequential()`

Both strategy methods are package-private so tests can call them directly. The scan strategy used (`"sequential"` or `"offset-index"`) is recorded in the `RowGroupScannedEvent` JFR event via a `scanStrategy` field.

### Step 6: Test data generation

**`simple-datagen.py`** — add a section generating a Parquet file **with page index enabled**:
```python
pq.write_table(table, 'core/src/test/resources/page_index_test.parquet',
               write_page_index=True, ...)
```
Use enough rows/small page size to produce multiple data pages, so the index is meaningful.

### Step 7: Dual-path correctness test

**`core/.../internal/reader/PageScannerTest.java`** (new test class):

1. `testOffsetIndexProducesIdenticalResultsToSequentialScan()` — open `page_index_test.parquet`, verify `offsetIndexOffset()` is non-null, call both `scanPagesSequential()` and `scanPagesFromIndex()`, assert same page count, decode all pages and verify identical values (type-specific comparison for all page types)
2. `testScanPagesAutoSelectsIndexPath()` — verify `scanPages()` auto-selects the index path when offset index is available and produces the same page count
3. `testSequentialFallbackForFilesWithoutIndex()` — verify files without offset index (e.g. `plain_uncompressed.parquet`) fall back to sequential scan

### Step 8: Performance testing

**`performance-testing/micro-benchmarks/.../benchmarks/PageScanBenchmark.java`** (new file)
Follow the pattern in `PageHandlingBenchmark.java`:

- `@Setup`: Open a multi-page Parquet file, memory-map it, prepare column chunks
- Single `scanPages()` benchmark method using `@Param` to select between two generated files: `page_scan_with_index.parquet` and `page_scan_no_index.parquet` — JMH cross-products the parameter so both strategies are benchmarked
- Consumes returned `List<PageInfo>` via `Blackhole`

**`performance-testing/generate_benchmark_data.py`** (new file)
Generates the two benchmark data files with a taxi-like 18-column schema (~9M rows). One file is written with `write_page_index=True`, the other without.

**`performance-testing/end-to-end/.../perf/PageScanPerformanceTest.java`** (new file)
Full end-to-end performance comparison reading files with and without offset index through the `Hardwood` API, verifying correctness (identical aggregated results) and reporting timing.

## Files to modify

| File | Action |
|------|--------|
| `core/.../metadata/PageLocation.java` | **New** — record |
| `core/.../metadata/OffsetIndex.java` | **New** — record |
| `core/.../internal/thrift/PageLocationReader.java` | **New** — Thrift reader |
| `core/.../internal/thrift/OffsetIndexReader.java` | **New** — Thrift reader |
| `core/.../metadata/ColumnChunk.java` | **Modify** — add offset index fields |
| `core/.../internal/thrift/ColumnChunkReader.java` | **Modify** — parse fields 4-5 |
| `core/.../internal/reader/PageScanner.java` | **Modify** — extract strategies + index path |
| `core/.../internal/reader/event/RowGroupScannedEvent.java` | **Modify** — add `scanStrategy` field |
| `core/.../internal/reader/PageScannerTest.java` | **New** — dual-path correctness test |
| `performance-testing/.../PageScanBenchmark.java` | **New** — JMH benchmark |
| `performance-testing/.../PageScanPerformanceTest.java` | **New** — end-to-end perf test |
| `performance-testing/generate_benchmark_data.py` | **New** — benchmark data generator |
| `simple-datagen.py` | **Modify** — generate test file with page index |

## Verification

```bash
# Generate test data
source .docker-venv/bin/activate && python simple-datagen.py

# Build and run all tests (180s timeout to detect deadlocks)
timeout 180 ./mvnw verify

# Run the JMH benchmark (requires -Pperformance-test and test data)
timeout 180 ./mvnw verify -Pperformance-test
# Then: java -jar performance-testing/micro-benchmarks/target/benchmarks.jar PageScanBenchmark
```

- All existing tests pass (files without offset indexes exercise the sequential fallback)
- New `PageScannerTest` verifies both paths produce identical results
- JMH benchmark quantifies the performance difference
