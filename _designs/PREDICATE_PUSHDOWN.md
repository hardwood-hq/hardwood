# Plan: Support Predicate Push-Down (#59)

**Status: Implemented**

## Context

Parquet files contain rich metadata for pruning during processing — row group statistics (min/max values per column) and bloom filters. Currently, all row groups are scanned unconditionally. This feature adds a public filter API so consumers can specify predicates, enabling the reader to skip irrelevant row groups entirely. This is especially important for object storage (#31) where skipping row groups avoids downloading unnecessary data.

**Scope**: This plan covers statistics-based row group filtering. Bloom filters and page-level (ColumnIndex) filtering are deferred to follow-up issues. ColumnIndex metadata parsing infrastructure has been added but page-level evaluation is not yet wired up.

---

## Step 1: Parse Statistics from Thrift

`ColumnMetaDataReader` parses the Statistics struct (Thrift field 12).

### New file: `metadata/Statistics.java`
```java
public record Statistics(
    byte[] minValue,          // field 6 (min_value) or fallback field 2 (min)
    byte[] maxValue,          // field 5 (max_value) or fallback field 1 (max)
    Long nullCount,           // field 3
    Long distinctCount,       // field 4
    boolean isMinMaxDeprecated // true if only legacy fields 1/2 were present
) {}
```

### New file: `internal/thrift/StatisticsReader.java`
Parses the Thrift Statistics struct (fields 1-6). Prefers fields 5/6 (correct sort order per PARQUET-1025) over deprecated fields 1/2. Sets `isMinMaxDeprecated` when only legacy fields are present.

### Modify: `metadata/ColumnMetaData.java`
Added `Statistics statistics` parameter to the record (nullable).

### Modify: `internal/thrift/ColumnMetaDataReader.java`
Parses the Statistics struct (Thrift field 12) via `StatisticsReader.read(reader)`.

**Files:**
- `core/src/main/java/dev/hardwood/metadata/Statistics.java` (new)
- `core/src/main/java/dev/hardwood/internal/thrift/StatisticsReader.java` (new)
- `core/src/main/java/dev/hardwood/metadata/ColumnMetaData.java` (modify — add statistics param)
- `core/src/main/java/dev/hardwood/internal/thrift/ColumnMetaDataReader.java` (modify — parse Statistics struct)

---

## Step 2: Create FilterPredicate API

A sealed interface hierarchy for type-safe predicates. Follows the `ColumnProjection` pattern (factory methods, immutable, simple).

### New file: `reader/FilterPredicate.java`

```java
public sealed interface FilterPredicate {
    // Comparison predicates (typed overloads for int, long, float, double, boolean, String)
    static FilterPredicate eq(String column, int value);
    static FilterPredicate eq(String column, long value);
    static FilterPredicate eq(String column, float value);
    static FilterPredicate eq(String column, double value);
    static FilterPredicate eq(String column, boolean value);
    static FilterPredicate eq(String column, String value);   // UTF-8 BYTE_ARRAY

    static FilterPredicate notEq(String column, int value);
    // ... same overloads

    static FilterPredicate lt(String column, int value);
    static FilterPredicate ltEq(String column, int value);
    static FilterPredicate gt(String column, int value);
    static FilterPredicate gtEq(String column, int value);
    // ... same overloads for long, float, double, String

    // Logical combinators
    static FilterPredicate and(FilterPredicate left, FilterPredicate right);
    static FilterPredicate and(FilterPredicate... filters);
    static FilterPredicate or(FilterPredicate left, FilterPredicate right);
    static FilterPredicate or(FilterPredicate... filters);
    static FilterPredicate not(FilterPredicate filter);
}
```

Sealed permits: `IntColumnPredicate`, `LongColumnPredicate`, `FloatColumnPredicate`, `DoubleColumnPredicate`, `BooleanColumnPredicate`, `BinaryColumnPredicate`, `And`, `Or`, `Not`.

Each leaf predicate stores: column name, operator enum (`EQ`, `NOT_EQ`, `LT`, `LT_EQ`, `GT`, `GT_EQ`), and the typed value. `And` and `Or` hold a `List<FilterPredicate>` to support varargs composition. `BinaryColumnPredicate` overrides `equals`/`hashCode` for correct `byte[]` semantics.

**Files:**
- `core/src/main/java/dev/hardwood/reader/FilterPredicate.java` (new)

---

## Step 3: Statistics Decoder Utility

Decodes raw statistics bytes to typed values for comparison.

### New file: `internal/reader/StatisticsDecoder.java`

```java
static int decodeInt(byte[] bytes);       // 4 bytes LE
static long decodeLong(byte[] bytes);     // 8 bytes LE
static float decodeFloat(byte[] bytes);   // 4 bytes IEEE 754 LE
static double decodeDouble(byte[] bytes); // 8 bytes IEEE 754 LE
static boolean decodeBoolean(byte[] bytes); // single byte
static int compareBinary(byte[] a, byte[] b); // unsigned lexicographic
```

**Files:**
- `core/src/main/java/dev/hardwood/internal/reader/StatisticsDecoder.java` (new)

---

## Step 4: Row Group Filter Evaluation

### New file: `internal/reader/RowGroupFilterEvaluator.java`

Given a `FilterPredicate` and a `RowGroup` + `FileSchema`, determines whether the row group can be skipped.

```java
static boolean canDropRowGroup(FilterPredicate predicate, RowGroup rowGroup, FileSchema schema);
```

Logic per operator:
- `EQ(v)`: drop if `v < min` or `v > max`
- `LT(v)`: drop if `min >= v`
- `LT_EQ(v)`: drop if `min > v`
- `GT(v)`: drop if `max <= v`
- `GT_EQ(v)`: drop if `max < v`
- `NOT_EQ(v)`: drop if `min == max == v`
- `AND(filters)`: drop if any child can drop
- `OR(filters)`: drop if all children can drop
- `NOT(delegate)`: never drop (conservative — cannot safely negate statistics)
- If statistics are null/absent for a column, never drop (conservative)

Float/double comparisons use `Float.compare()` / `Double.compare()` for correct NaN and -0.0 handling.

Column lookup uses `schema.getColumn(name).columnIndex()` (O(1) via name-to-index map) with a fallback to path-based matching for nested columns. Supports dotted paths (e.g., `"address.zip"` matches path `["address", "zip"]`) and top-level names for repeated columns (e.g., `"scores"` matches path `["scores", "list", "element"]`).

**Files:**
- `core/src/main/java/dev/hardwood/internal/reader/RowGroupFilterEvaluator.java` (new)

---

## Step 5: Integrate Filtering into Reader Pipeline

### Modify: `reader/ParquetFileReader.java`
Added overloads:
```java
public RowReader createRowReader(FilterPredicate filter);
public RowReader createRowReader(ColumnProjection projection, FilterPredicate filter);
public ColumnReader createColumnReader(String columnName, FilterPredicate filter);
public ColumnReader createColumnReader(int columnIndex, FilterPredicate filter);
```

These methods filter `fileMetaData.rowGroups()` via a private helper before passing to `SingleFileRowReader`/`ColumnReader.create()`:
```java
private List<RowGroup> filterRowGroups(FileSchema schema, FilterPredicate filter) {
    // Filters row groups, emits JFR event with skip counts
}
```

### Modify: `reader/MultiFileParquetReader.java`
Added overloads:
```java
public MultiFileRowReader createRowReader(FilterPredicate filter);
public MultiFileRowReader createRowReader(ColumnProjection projection, FilterPredicate filter);
public MultiFileColumnReaders createColumnReaders(ColumnProjection projection, FilterPredicate filter);
```

### Modify: `internal/reader/FileManager.java`
- Stores `FilterPredicate` (set via `initialize(projection, filter)`)
- In `scanAllProjectedColumns()`, filters row groups before iterating
- In `loadFile()`, applies same filtering to subsequently loaded files

**Files:**
- `core/src/main/java/dev/hardwood/reader/ParquetFileReader.java` (modify)
- `core/src/main/java/dev/hardwood/reader/MultiFileParquetReader.java` (modify)
- `core/src/main/java/dev/hardwood/internal/reader/FileManager.java` (modify)

---

## Step 6: JFR Observability

### New file: `jfr/RowGroupFilterEvent.java`

Dedicated JFR event emitted once per file when filtering is applied:
```java
public class RowGroupFilterEvent extends Event {
    public String file;
    public int totalRowGroups;
    public int rowGroupsKept;
    public int rowGroupsSkipped;
}
```

**Files:**
- `core/src/main/java/dev/hardwood/jfr/RowGroupFilterEvent.java` (new)

---

## Step 7: ColumnIndex Metadata Infrastructure

Provides the metadata foundation for future page-level filtering.

### New file: `metadata/ColumnIndex.java`
```java
public record ColumnIndex(
    List<Boolean> nullPages,
    List<byte[]> minValues,
    List<byte[]> maxValues,
    BoundaryOrder boundaryOrder,
    List<Long> nullCounts
) {}
```

### New file: `internal/thrift/ColumnIndexReader.java`
Parses the Thrift ColumnIndex struct (fields 1-5).

### Modify: `metadata/ColumnChunk.java`
Added `Long columnIndexOffset` and `Integer columnIndexLength` fields.

### Modify: `internal/thrift/ColumnChunkReader.java`
Parses fields 6 (column_index_offset) and 7 (column_index_length).

**Files:**
- `core/src/main/java/dev/hardwood/metadata/ColumnIndex.java` (new)
- `core/src/main/java/dev/hardwood/internal/thrift/ColumnIndexReader.java` (new)
- `core/src/main/java/dev/hardwood/metadata/ColumnChunk.java` (modify)
- `core/src/main/java/dev/hardwood/internal/thrift/ColumnChunkReader.java` (modify)

---

## Step 8: Tests

### New file: `core/src/test/java/dev/hardwood/StatisticsTest.java`
- Verifies statistics are correctly parsed from test Parquet files
- Tests StatisticsDecoder for all supported types

### New file: `core/src/test/java/dev/hardwood/FilterPredicateTest.java`
- Unit tests for each predicate type and operator
- Tests for And/Or/Not composition
- Tests for row group filter evaluation (canDropRowGroup) with constructed RowGroup/Statistics objects
- Edge cases: negative ranges, boundary values, missing statistics, single-value ranges

### New file: `core/src/test/java/dev/hardwood/PredicatePushDownTest.java`
- Integration tests against multi-row-group Parquet files with known statistics
- Single-file ColumnReader with filter (by name and by index)
- Single-file RowReader with filter (with and without projection)
- Multi-file RowReader with filter (with and without projection)
- Multi-file ColumnReaders with filter
- Filter matching no row groups returns zero rows
- Filter matching all row groups returns all rows
- Filter on different column than read column
- Mixed types: int, long, float, double, boolean, string filters
- Nested column filters via dotted paths (e.g., `address.zip`)
- Repeated/list column filters
- OR filter selecting multiple row groups

### Generate test data
Extended `tools/simple-datagen.py` to create four multi-row-group files with known statistics:
- `filter_pushdown_int.parquet` — 3 row groups, INT64 id/value 1-100, 101-200, 201-300
- `filter_pushdown_mixed.parquet` — 3 row groups, mixed types (int32, float64, float32, string, bool)
- `filter_pushdown_list.parquet` — 3 row groups, list\<int32\> column
- `filter_pushdown_nested.parquet` — 3 row groups, struct column (city, zip)

**Files:**
- `core/src/test/java/dev/hardwood/StatisticsTest.java` (new)
- `core/src/test/java/dev/hardwood/FilterPredicateTest.java` (new)
- `core/src/test/java/dev/hardwood/PredicatePushDownTest.java` (new)
- `tools/simple-datagen.py` (modify — add test data generation)

---

## Verification

1. `source .docker-venv/bin/activate && python tools/simple-datagen.py` — generate test Parquet files
2. `./mvnw verify -pl core` (with 180s timeout) — run all tests including new ones
3. Verify existing tests still pass (no regressions from Statistics field addition to ColumnMetaData)
4. Verify with a multi-row-group file that row groups are actually skipped (visible in DEBUG logs or JFR events)
