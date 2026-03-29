# Plan: Page-Level Predicate Push-Down via Column Index (#118)

**Status: Implemented**

## Context

[Predicate push-down](PREDICATE_PUSHDOWN.md) currently operates at the row-group level: row groups whose statistics prove no rows can match are skipped entirely. This is effective for coarse-grained filtering, but within a surviving row group there may be hundreds of data pages, many of which are also irrelevant to the predicate.

Parquet v2 files can include a **Column Index** — per-page min/max statistics stored alongside the existing Offset Index. PR #116 added the metadata infrastructure for Column Index (parsing, buffering, pre-fetching) but does not yet consume it for filtering. This plan wires up page-level evaluation so that irrelevant pages within surviving row groups are never decompressed or decoded.

### What already exists (from PR #116)

- `ColumnIndex` record — per-page `minValues`, `maxValues`, `nullPages`, `boundaryOrder`, `nullCounts`
- `ColumnIndexReader` — Thrift parser
- `ColumnIndexBuffers` — raw `ByteBuffer` slices for both Offset Index and Column Index, pre-fetched in a single read via `RowGroupIndexBuffers.fetch()`
- `PageRange.coalesce()` — coalesces non-contiguous page locations (currently unused, retained for this feature)
- `StatisticsDecoder` — decodes min/max bytes to typed primitives (directly reusable — Column Index uses the same encoding)
- `RowGroupFilterEvaluator` — `canDrop` logic for all operator/type combinations

### Design challenge: cross-column page alignment

Different columns have independent page boundaries. A filter on column A produces a set of matching pages in A, but columns B and C have their own, differently-sized pages. To skip pages across all projected columns, we work in **row-range space** rather than page-index space:

1. Evaluate the filter against the filter column's Column Index → determine which pages of that column *might* match
2. Convert matching pages to **row ranges** using `PageLocation.firstRowIndex()`
3. For each projected column, use its Offset Index to identify which of *its* pages overlap with the matching row ranges

This is what the `RowRanges` abstraction provides.

---

## Step 1: RowRanges Utility

A sorted, non-overlapping set of `[startRow, endRow)` intervals representing rows that might match the filter. Stored as a flat `long[]` array of interleaved `[start, end)` pairs.

### New file: `internal/reader/RowRanges.java`

- `RowRanges.all(rowGroupRowCount)` — conservative fallback when Column Index is absent
- `RowRanges.fromPages(pages, keep, rowGroupRowCount)` — builds intervals from page locations and a keep bitmap, merging adjacent kept pages
- `intersect(other)` — interval intersection for AND predicates
- `union(other)` — interval union for OR predicates
- `overlapsPage(pageFirstRow, pageLastRow)` — checks if a page's row range overlaps any matching interval
- `isAll()` — optimization to skip per-page checks when everything matches
- `intervalCount()` — number of intervals (used for testing)

**Files:**
- `core/src/main/java/dev/hardwood/internal/reader/RowRanges.java` (new)

---

## Step 2: PageFilterEvaluator

Evaluates a `FilterPredicate` against per-page statistics from the Column Index to produce `RowRanges` for a row group.

### New file: `internal/reader/PageFilterEvaluator.java`

Entry point: `PageFilterEvaluator.computeMatchingRows(predicate, rowGroup, schema, indexBuffers)`

**Evaluation logic** (recursive over the sealed predicate tree via pattern matching):

- **Leaf predicate** (e.g., `IntColumnPredicate`): resolves the filter column via shared helpers from `RowGroupFilterEvaluator` (`findColumnIndex`, `findColumnIndexByPath`). Parses the `ColumnIndex` and `OffsetIndex` from pre-fetched buffers. Evaluates each page using a `PageCanDropTest` functional interface that decodes min/max via `StatisticsDecoder` and delegates to the shared `canDrop`/`canDropFloat`/`canDropDouble`/`canDropCompared` methods. Null-only pages are always skipped. Produces `RowRanges` via `fromPages()`.
- **And**: computes `RowRanges` for each child, intersects them (starting from `RowRanges.all`)
- **Or**: computes `RowRanges` for each child, unions them
- **Not**: returns `RowRanges.all()` (conservative)

Falls back to `RowRanges.all()` when Column Index is absent, column is not found, or buffers are null.

### Modify: `internal/reader/RowGroupFilterEvaluator.java`

Widened `canDrop`, `canDropFloat`, `canDropDouble`, `canDropCompared`, `findColumnIndex`, and `findColumnIndexByPath` from `private` to package-private so both evaluators share the same comparison and column resolution logic.

**Files:**
- `core/src/main/java/dev/hardwood/internal/reader/PageFilterEvaluator.java` (new)
- `core/src/main/java/dev/hardwood/internal/reader/RowGroupFilterEvaluator.java` (modify)

---

## Step 3: Modify PageScanner to Accept Row Ranges

### Modify: `internal/reader/PageScanner.java`

Added an overloaded constructor accepting an optional `RowRanges matchingRows` parameter. The original constructor delegates with `null` for backwards compatibility.

In `scanPagesFromIndex()`, after parsing the Offset Index and before slicing pages, a `filterPageLocations()` method filters the page locations:
- If `matchingRows` is null or `isAll()`, all pages pass through unchanged
- Otherwise, each page's row range is checked via `overlapsPage()`. The last page uses `Long.MAX_VALUE` as its end sentinel to avoid needing the row group row count.
- A `PageFilterEvent` is emitted when pages are actually skipped

The sequential scan path (`scanPagesSequential`) is unaffected — it has no per-page statistics.

**Files:**
- `core/src/main/java/dev/hardwood/internal/reader/PageScanner.java` (modify)

---

## Step 4: Thread Filter Through the Read Pipeline

The `FilterPredicate` is passed through to `PageFilterEvaluator` at the point where `PageScanner` instances are created. `RowRanges` are computed once per row group and shared across all projected columns.

### Modify: `reader/ParquetFileReader.java`

Filter-accepting `createColumnReader()` and `createRowReader()` methods now pass the `FilterPredicate` through to `ColumnReader.create()` and `SingleFileRowReader`.

### Modify: `reader/SingleFileRowReader.java`

New constructor accepting `FilterPredicate`. In `initialize()`, computes `RowRanges` via `PageFilterEvaluator.computeMatchingRows()` once per row group, captures it as a final local, and passes to all `PageScanner` instances in the parallel scan lambdas.

### Modify: `reader/ColumnReader.java`

New filter-accepting `create()` overloads for both name and index lookups. The private `create()` computes `RowRanges` per row group and passes to `PageScanner`. No-filter overloads delegate with `null`.

### Modify: `internal/reader/FileManager.java`

In `scanAllProjectedColumns()`, computes `RowRanges` per row group using the existing `filterPredicate` field and the file's schema, then passes to each `PageScanner`.

**Files:**
- `core/src/main/java/dev/hardwood/reader/ParquetFileReader.java` (modify)
- `core/src/main/java/dev/hardwood/reader/SingleFileRowReader.java` (modify)
- `core/src/main/java/dev/hardwood/reader/ColumnReader.java` (modify)
- `core/src/main/java/dev/hardwood/internal/reader/FileManager.java` (modify)

---

## Step 5: JFR Observability

### New file: `jfr/PageFilterEvent.java`

Emitted once per column per row group when pages are actually skipped (not emitted when all pages pass through). Reports file, rowGroupIndex, column, totalPages, pagesKept, pagesSkipped.

**Files:**
- `core/src/main/java/dev/hardwood/jfr/PageFilterEvent.java` (new)

---

## Step 6: Tests

### `core/src/test/java/dev/hardwood/internal/reader/RowRangesTest.java` (new)

14 unit tests covering:
- `all()`, `isAll()`, `intervalCount()`
- `fromPages()` with keep all/none/first/last/edges, adjacent page merging
- `overlapsPage()` partial overlap and boundary exclusivity
- `intersect()` with all, overlapping ranges, disjoint ranges
- `union()` with all, disjoint ranges, overlapping merging, adjacent merging

### `core/src/test/java/dev/hardwood/internal/reader/PageFilterEvaluatorTest.java` (new)

67 parameterized unit tests covering:
- All 6 operators × int type (16 cases with 3-page layout)
- All 6 operators × long, float, double, binary types (12 cases each with 2-page layout)
- Edge cases: all pages match, no pages match, null-only pages skipped

Uses `@ParameterizedTest` with `@MethodSource` providers, following the codebase convention.

### `performance-testing/end-to-end/src/test/java/dev/hardwood/perf/PageFilterBenchmarkTest.java` (new)

End-to-end benchmark that lazily generates a 50M-row synthetic Parquet file (via parquet-java, no Python dependency) with sorted id column, small 4KB pages, and Parquet v2 Column Index. Compares unfiltered reads against filtered reads (`id < 10%` of range) to validate page-skipping throughput impact.

**Files:**
- `core/src/test/java/dev/hardwood/internal/reader/RowRangesTest.java` (new)
- `core/src/test/java/dev/hardwood/internal/reader/PageFilterEvaluatorTest.java` (new)
- `performance-testing/end-to-end/src/test/java/dev/hardwood/perf/PageFilterBenchmarkTest.java` (new)

---

## Summary of changes

| Type | File | Change |
|------|------|--------|
| New | `internal/reader/RowRanges.java` | Row range interval set with intersection/union |
| New | `internal/reader/PageFilterEvaluator.java` | Per-page predicate evaluation → RowRanges |
| New | `jfr/PageFilterEvent.java` | JFR event for page-level filtering |
| Modify | `internal/reader/RowGroupFilterEvaluator.java` | Widened shared comparison and column resolution helpers |
| Modify | `internal/reader/PageScanner.java` | Accept RowRanges, filter page locations, emit JFR event |
| Modify | `reader/ParquetFileReader.java` | Pass filter to ColumnReader/RowReader |
| Modify | `reader/SingleFileRowReader.java` | Accept filter, compute RowRanges per row group |
| Modify | `reader/ColumnReader.java` | Accept filter in factory methods |
| Modify | `internal/reader/FileManager.java` | Compute RowRanges per row group |
| New | test: `RowRangesTest.java` | 14 unit tests |
| New | test: `PageFilterEvaluatorTest.java` | 67 parameterized unit tests |
| New | test: `PageFilterBenchmarkTest.java` | Performance benchmark with synthetic data |

## Step 7: Page-Range I/O for Remote Backends

When page-level filtering skips pages, fetch only matching pages instead of entire column chunks. This reduces network I/O on S3 and other remote backends.

### New file: `internal/reader/PageRangeData.java`

Record holding fetched page-range buffers with `slicePage()` and `sliceRegion()` for page and dictionary lookup. Static `fetch()` method issues one `readRange()` per coalesced `PageRange`.

### Modify: `internal/reader/PageRange.java`

Added `forColumn()` factory that filters pages by `RowRanges`, coalesces matching pages, and extends the first range for dictionary prefix.

### Modify: `internal/reader/PageScanner.java`

New constructor accepting `PageRangeData` instead of full chunk buffer. Bifurcated page slicing and dictionary parsing in `scanPagesFromIndex()`.

### Modify: `reader/ColumnReader.java`, `reader/SingleFileRowReader.java`, `internal/reader/FileManager.java`

Each call site attempts page-range I/O per column when filtering is active. Falls back to full chunk fetch for columns that don't benefit or lack an Offset Index.

**Files:**
- `core/src/main/java/dev/hardwood/internal/reader/PageRangeData.java` (new)
- `core/src/main/java/dev/hardwood/internal/reader/PageRange.java` (modify)
- `core/src/main/java/dev/hardwood/internal/reader/PageScanner.java` (modify)
- `core/src/main/java/dev/hardwood/reader/ColumnReader.java` (modify)
- `core/src/main/java/dev/hardwood/reader/SingleFileRowReader.java` (modify)
- `core/src/main/java/dev/hardwood/internal/reader/FileManager.java` (modify)
- `core/src/test/java/dev/hardwood/internal/reader/PageRangeIoTest.java` (new)
- `core/src/test/java/dev/hardwood/internal/reader/PageRangeDataTest.java` (new)
- `s3/src/test/java/dev/hardwood/s3/S3InputFileTest.java` (modify)

---

## Updated summary of changes

| Type | File | Change |
|------|------|--------|
| New | `internal/reader/RowRanges.java` | Row range interval set with intersection/union |
| New | `internal/reader/PageFilterEvaluator.java` | Per-page predicate evaluation → RowRanges |
| New | `internal/reader/PageRangeData.java` | Fetched page-range buffers with page/dictionary slicing |
| New | `jfr/PageFilterEvent.java` | JFR event for page-level filtering |
| Modify | `internal/reader/PageRange.java` | Added `forColumn()` factory for page-range computation |
| Modify | `internal/reader/RowGroupFilterEvaluator.java` | Widened shared comparison and column resolution helpers |
| Modify | `internal/reader/PageScanner.java` | Accept RowRanges and PageRangeData, bifurcated page slicing |
| Modify | `reader/ParquetFileReader.java` | Pass filter to ColumnReader/RowReader |
| Modify | `reader/SingleFileRowReader.java` | Accept filter, page-range I/O path |
| Modify | `reader/ColumnReader.java` | Accept filter, page-range I/O path |
| Modify | `internal/reader/FileManager.java` | Page-range I/O path |
| New | test: `RowRangesTest.java` | 16 unit tests |
| New | test: `PageFilterEvaluatorTest.java` | 77 parameterized unit tests |
| New | test: `PageRangeDataTest.java` | 5 unit tests |
| New | test: `PageRangeIoTest.java` | 4 integration tests (byte counting) |
| New | test: `PageFilterBenchmarkTest.java` | Performance benchmark with synthetic data |
| Modify | test: `S3InputFileTest.java` | S3 byte counting assertion |

## Future optimizations (out of scope)

- **Boundary order binary search**: when `boundaryOrder` is `ASCENDING` or `DESCENDING`, binary search for the first/last matching page instead of linear scan.

## Verification

1. `./mvnw verify -pl core` — all tests pass
2. `./mvnw test -pl s3 -Dtest=S3InputFileTest` — S3 byte counting assertion confirms 88% I/O reduction
3. `./mvnw test -Pperformance-test -pl performance-testing/end-to-end -Dtest="PageFilterBenchmarkTest" -Dperf.runs=5` — benchmark validates page skipping under load
4. Existing predicate push-down tests still pass (no regressions)
