# Design: Multi-File ColumnReader

## Context

The single-file `ColumnReader` scans pages across all row groups within one file but cannot span multiple files. The row-based path already has `MultiFileRowReader` which uses `FileManager` + `PageCursor` cross-file prefetching to stream across files. The multi-file column reader provides the equivalent for the columnar API.

## API

### Architecture

The multi-file column reader follows the same two-step pattern as the row reader:

```
Hardwood.openAll(files)
  → MultiFileParquetReader        (reads schema, defers page scanning)
      → createColumnReaders(ColumnProjection)
          → MultiFileColumnReaders (shared FileManager, cross-file prefetching)
              → getColumnReader(name/index)
                  → ColumnReader   (batch-oriented API)
```

### MultiFileParquetReader (intermediate)

```java
public class MultiFileParquetReader implements AutoCloseable {
    FileSchema getFileSchema();
    MultiFileRowReader createRowReader();
    MultiFileRowReader createRowReader(ColumnProjection projection);
    MultiFileColumnReaders createColumnReaders(ColumnProjection projection);
}
```

`MultiFileParquetReader` reads the schema eagerly from the first file via `FileManager.openFirst()`, but defers page scanning until a reader is created. This lets consumers inspect `getFileSchema()` before deciding which columns to project.

### MultiFileColumnReaders

```java
public class MultiFileColumnReaders implements AutoCloseable {
    int getColumnCount();
    ColumnReader getColumnReader(String columnName);
    ColumnReader getColumnReader(int index);   // index within the requested columns
    void close();
}
```

### Usage

```java
try (Hardwood hardwood = Hardwood.create();
     MultiFileParquetReader parquet = hardwood.openAll(files);
     MultiFileColumnReaders columns = parquet.createColumnReaders(
         ColumnProjection.columns("passenger_count", "trip_distance", "fare_amount"))) {

    ColumnReader col0 = columns.getColumnReader("passenger_count");
    ColumnReader col1 = columns.getColumnReader("trip_distance");
    ColumnReader col2 = columns.getColumnReader("fare_amount");

    while (col0.nextBatch() & col1.nextBatch() & col2.nextBatch()) {
        int count = col0.getRecordCount();
        double[] v0 = col0.getDoubles();
        double[] v1 = col1.getDoubles();
        double[] v2 = col2.getDoubles();
        for (int i = 0; i < count; i++) {
            passengerCount += (long) v0[i];
            tripDistance += v1[i];
            fareAmount += v2[i];
        }
    }
}
```

A varargs overload is also available:
```java
MultiFileParquetReader parquet = hardwood.openAll(
    Path.of("data_01.parquet"), Path.of("data_02.parquet"));
```

## Implementation

### MultiFileParquetReader

- Constructor takes `List<Path> files` and `HardwoodContext context`
- Creates a `FileManager` and calls `openFirst()` to read the schema from the first file
- `createColumnReaders(ColumnProjection)` calls `fileManager.initialize(projection)` to scan pages, then passes the `FileManager` and `InitResult` to the `MultiFileColumnReaders` constructor
- Owns the `FileManager` lifecycle — closes it on `close()`

### MultiFileColumnReaders

- Constructor receives `HardwoodContext`, `FileManager`, and `FileManager.InitResult`
- For each projected column, creates a `ColumnReader` using the multi-file constructor with the `FileManager` for cross-file prefetching
- Stores readers in both a `LinkedHashMap<String, ColumnReader>` (by name) and a `ColumnReader[]` (by index)
- Does **not** close the `FileManager` — that's owned by the parent `MultiFileParquetReader`

### ColumnReader multi-file constructor

Package-private constructor that accepts an additional `FileManager`, `projectedColumnIndex`, and `fileName`:

```java
ColumnReader(ColumnSchema column, List<PageInfo> pageInfos, HardwoodContext context,
             int batchSize, FileManager fileManager, int projectedColumnIndex, String fileName)
```

The `PageCursor` is created with the `FileManager` for cross-file prefetching — the same pattern used by `MultiFileRowReader`.

### FileManager two-phase initialization

`FileManager` was split into two phases to support the `MultiFileParquetReader` intermediate:

1. `openFirst()` — maps and reads metadata from the first file, returns `FileSchema`
2. `initialize(ColumnProjection)` — scans pages for projected columns across all files, triggers prefetch

This split allows `MultiFileParquetReader` to expose the schema before committing to a column projection.

## Key files

| File | Role |
|------|------|
| `core/.../reader/MultiFileColumnReaders.java` | Holds multiple ColumnReaders with shared FileManager |
| `core/.../reader/MultiFileParquetReader.java` | Intermediate factory: schema access + reader creation |
| `core/.../reader/ColumnReader.java` | Multi-file constructor delegates to PageCursor with FileManager |
| `core/.../Hardwood.java` | `openAll(List<Path>)` and `openAll(Path, Path...)` entry points |
| `core/.../internal/reader/FileManager.java` | Two-phase init: `openFirst()` + `initialize(projection)` |

## Tests

- **`MultiFileColumnReadersTest`** (11 tests): Covers single-file, multi-file, column-by-index, unknown column errors, nulls, data integrity across file boundaries, multiple typed columns (int, long, float, double, boolean, string), single-column projection, and nesting depth metadata.
- **`SimplePerformanceTest`**: `HARDWOOD_COLUMN_READER_MULTIFILE` contender exercises the full multi-file column reader path with cross-file prefetching.

## Performance

`HARDWOOD_COLUMN_READER_MULTIFILE` eliminates file-boundary latency compared to `HARDWOOD_COLUMN_READER` (which reopens a `ParquetFileReader` per file). Cross-file prefetching ensures pages from file N+1 are ready when file N is exhausted.
