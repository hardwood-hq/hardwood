# Design: ColumnReader Batch API with Multi-Level Offsets

## Context

Benchmarking showed that per-row `hasNext()/next()/getDouble()` access is the primary consumer bottleneck — 2.5x slower than direct array iteration. Replacing per-row access with vectorized `for (int i = 0; i < count; i++)` loops over batch arrays gives a **2x speedup** (330M → 600M rec/s) with the same pipeline.

The old `ColumnReader` exposed values one at a time (`readNext()` → `Object`). It was reworked to expose batch-oriented typed arrays with multi-level offsets for nested/repeated columns, matching the Arrow-style offset pattern.

## API

### Full API surface

```java
public class ColumnReader implements AutoCloseable {
    // Batch iteration
    boolean nextBatch();
    int getRecordCount();        // rows in current batch
    int getValueCount();         // total leaf values in current batch

    // Typed value arrays (flattened for repeated columns)
    int[] getInts();
    long[] getLongs();
    float[] getFloats();
    double[] getDoubles();
    boolean[] getBooleans();
    byte[][] getBinaries();
    String[] getStrings();       // convenience: converts byte arrays to UTF-8

    // Null handling — per-level bitmaps
    BitSet getElementNulls();           // leaf value nulls (null if no nulls possible)
    BitSet getLevelNulls(int level);    // group-level nulls at offset level k

    // Offsets for repeated columns
    int getNestingDepth();              // 0 for flat, maxRepetitionLevel for nested
    int[] getOffsets(int level);        // offset array for given level (0-indexed)

    // Metadata
    ColumnSchema getColumnSchema();

    void close();
}
```

### Consumer code by column type

**Flat column** (required or optional):
```java
try (ColumnReader reader = fileReader.createColumnReader("fare_amount")) {
    while (reader.nextBatch()) {
        int count = reader.getRecordCount();    // == getValueCount() for flat
        double[] values = reader.getDoubles();
        BitSet nulls = reader.getElementNulls(); // null if column is required

        for (int i = 0; i < count; i++) {
            if (nulls == null || !nulls.get(i)) sum += values[i];
        }
    }
}
```

**Simple list** (`list<double>`, nestingDepth=1):
```java
try (ColumnReader reader = fileReader.createColumnReader("fare_components")) {
    while (reader.nextBatch()) {
        int recordCount = reader.getRecordCount();
        int valueCount = reader.getValueCount();
        double[] values = reader.getDoubles();
        int[] offsets = reader.getOffsets(0);         // record → value position
        BitSet recordNulls = reader.getLevelNulls(0); // null list records
        BitSet elementNulls = reader.getElementNulls(); // null elements within lists

        for (int r = 0; r < recordCount; r++) {
            if (recordNulls != null && recordNulls.get(r)) continue;
            int start = offsets[r];
            int end = (r + 1 < recordCount) ? offsets[r + 1] : valueCount;
            for (int i = start; i < end; i++) {
                if (elementNulls == null || !elementNulls.get(i)) sum += values[i];
            }
        }
    }
}
```

**Nested list** (`list<list<double>>`, nestingDepth=2):
```java
int[] outerOff = reader.getOffsets(0);  // record → inner list index
int[] innerOff = reader.getOffsets(1);  // inner list → value position
BitSet recordNulls = reader.getLevelNulls(0);    // null records
BitSet innerListNulls = reader.getLevelNulls(1); // null inner lists

for (int r = 0; r < recordCount; r++) {
    if (recordNulls != null && recordNulls.get(r)) continue;
    int listStart = outerOff[r];
    int listEnd = (r + 1 < recordCount) ? outerOff[r + 1] : innerListCount;
    for (int j = listStart; j < listEnd; j++) {
        if (innerListNulls != null && innerListNulls.get(j)) continue;
        int valStart = innerOff[j];
        int valEnd = innerOff[j + 1];
        // process values[valStart..valEnd)
    }
}
```

### Offset model

`maxRepetitionLevel` determines offset array count:
- **Flat** (maxRep=0): no offsets, `getRecordCount() == getValueCount()`
- **Simple list** (maxRep=1): 1 offset array
- **Nested list/map** (maxRep=N): N offset arrays, chained

Level k boundary: positions where `repLevel[i] <= k`.

### Null model

- `getElementNulls()`: BitSet over leaf values. For flat columns this doubles as record-level nulls (since records = values). Null if all elements are required.
- `getLevelNulls(int level)`: BitSet over items at offset level k. Null if that schema level is required. Only valid for `0 <= level < getNestingDepth()`.
- Computed from definition levels during batch assembly. Each schema nesting level has a known def level threshold; values below that threshold at a group boundary indicate null.

## Implementation

### ColumnReader internals

Changed from per-column-chunk to per-file (spanning all row groups). Reuses existing infrastructure:

- **Page scanning**: Same pattern as `SingleFileRowReader.initialize()` — scans pages for one column across all row groups using `PageScanner`, in parallel via `CompletableFuture` on `context.executor()`.
- **Flat columns**: Uses `ColumnAssemblyBuffer` + `PageCursor` with eager assembly virtual thread. `nextBatch()` calls `assemblyBuffer.awaitNextBatch()` returning `FlatColumnData`.
- **Nested columns**: Uses `ColumnValueIterator.readBatch(batchSize)` returning `NestedColumnData` with values + rep/def levels + recordOffsets.

### Multi-level offset computation

Static method `computeMultiLevelOffsets()` on `ColumnReader`. Given `int[] repLevels`, `int valueCount`, `int recordCount`, `int maxRepLevel`, computes `int[][] offsets`. Simple list (maxRep=1) has a fast path; general case does a two-pass approach (count items, then fill offsets).

### Per-level null bitmap computation

Private method `computeNullBitmaps()` on `ColumnReader`. Computes element-level nulls from definition levels (values where `defLevel < maxDefLevel`), and per-nesting-level nulls using a def-level threshold derived from the schema: `defThreshold = maxDefLevel - maxRepLevel + k + 1`.

### Factory methods on ParquetFileReader

```java
public ColumnReader createColumnReader(String columnName)
public ColumnReader createColumnReader(int columnIndex)
```

Both scan pages across all row groups in parallel, then create a `ColumnReader` with the collected `PageInfo` list.

## Key files

| File | Role |
|------|------|
| `core/.../reader/ColumnReader.java` | Batch API, offset/null computation, factory methods |
| `core/.../reader/ParquetFileReader.java` | `createColumnReader()` factory methods |
| `core/.../internal/reader/ColumnAssemblyBuffer.java` | Flat column batch assembly |
| `core/.../internal/reader/ColumnValueIterator.java` | Nested column batch assembly |
| `core/.../internal/reader/PageCursor.java` | Page iteration (single and multi-file) |
| `core/.../internal/reader/FlatColumnData.java` | Typed batch data for flat columns |
| `core/.../internal/reader/NestedColumnData.java` | Typed batch data for nested columns |

## Tests

- **`ParquetReaderTest`**: Tests batch API for flat required columns (`getLongs()`), optional columns with nulls (`getBinaries()` + `getElementNulls()`), compressed columns (Snappy), and column-by-index access.
- **`ParquetTestingRepoTest`**: Reads every column of every test file via the batch API, verifying `getRecordCount()` totals.
- **`ParquetComparisonTest`**: Validates batch API results against Apache parquet-java reference reads for all test files.

## Performance

`HARDWOOD_COLUMN_READER` contender in `SimplePerformanceTest` uses three column readers (`passenger_count`, `trip_distance`, `fare_amount`) with the batch loop pattern. Approaches ~600M rec/s for flat columns, matching the vectorized baseline.
