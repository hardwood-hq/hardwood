<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Usage

For detailed class-level documentation, see the [JavaDoc](/api/latest/).

## Choosing a Reader

Hardwood provides two reader APIs:

- **`RowReader`** — row-oriented access with typed getters, including nested structs, lists, and maps. Best for general-purpose reading where you process one row at a time.
- **`ColumnReader`** — batch-oriented columnar access with typed primitive arrays. Best for analytical workloads where you process columns independently (e.g. summing a column, computing statistics).

Both support column projection and predicate pushdown. For reading multiple files as a single dataset, use `MultiFileRowReader` or `MultiFileColumnReaders` via the `Hardwood` class.

## Row-Oriented Reading

The `RowReader` provides a convenient row-oriented interface for reading Parquet files with typed accessor methods for type-safe field access.

```java
import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqIntList;
import dev.hardwood.row.PqMap;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.math.BigDecimal;
import java.util.UUID;

try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
    RowReader rowReader = fileReader.createRowReader()) {

    while (rowReader.hasNext()) {
        rowReader.next();

        // Access columns by name with typed accessors
        long id = rowReader.getLong("id");
        String name = rowReader.getString("name");

        // Logical types are automatically converted
        LocalDate birthDate = rowReader.getDate("birth_date");
        Instant createdAt = rowReader.getTimestamp("created_at");
        LocalTime wakeTime = rowReader.getTime("wake_time");
        BigDecimal balance = rowReader.getDecimal("balance");
        UUID accountId = rowReader.getUuid("account_id");

        // Check for null values
        if (!rowReader.isNull("age")) {
            int age = rowReader.getInt("age");
            System.out.println("ID: " + id + ", Name: " + name + ", Age: " + age);
        }

        // Access nested structs
        PqStruct address = rowReader.getStruct("address");
        if (address != null) {
            String city = address.getString("city");
            int zip = address.getInt("zip");
        }

        // Access lists and iterate with typed accessors
        PqList tags = rowReader.getList("tags");
        if (tags != null) {
            for (String tag : tags.strings()) {
                System.out.println("Tag: " + tag);
            }
        }
    }
}
```

??? note "Advanced: nested lists, maps, and list-of-structs"

    ```java
            // Access list of structs
            PqList contacts = rowReader.getList("contacts");
            if (contacts != null) {
                for (PqStruct contact : contacts.structs()) {
                    String contactName = contact.getString("name");
                    String phone = contact.getString("phone");
                }
            }

            // Access nested lists (list<list<int>>) using primitive int lists
            PqList matrix = rowReader.getList("matrix");
            if (matrix != null) {
                for (PqIntList innerList : matrix.intLists()) {
                    for (var it = innerList.iterator(); it.hasNext(); ) {
                        int val = it.nextInt();
                        System.out.println("Value: " + val);
                    }
                }
            }

            // Access maps (map<string, int>)
            PqMap attributes = rowReader.getMap("attributes");
            if (attributes != null) {
                for (PqMap.Entry entry : attributes.getEntries()) {
                    String key = entry.getStringKey();
                    int value = entry.getIntValue();
                    System.out.println(key + " = " + value);
                }
            }

            // Access maps with struct values (map<string, struct>)
            PqMap people = rowReader.getMap("people");
            if (people != null) {
                for (PqMap.Entry entry : people.getEntries()) {
                    String personId = entry.getStringKey();
                    PqStruct person = entry.getStructValue();
                    String personName = person.getString("name");
                    int personAge = person.getInt("age");
                }
            }
    ```

### Typed Accessor Methods

All accessor methods are available in two forms:

- **Name-based** (e.g., `getInt("column_name")`) — convenient for ad-hoc access
- **Index-based** (e.g., `getInt(columnIndex)`) — faster for performance-critical loops

| Method | Physical Type | Logical Type | Java Type |
|--------|--------------|-------------|-----------|
| `getBoolean` | BOOLEAN | | `boolean` |
| `getInt` | INT32 | | `int` |
| `getLong` | INT64 | | `long` |
| `getFloat` | FLOAT | | `float` |
| `getDouble` | DOUBLE | | `double` |
| `getBinary` | BYTE_ARRAY | | `byte[]` |
| `getString` | BYTE_ARRAY | STRING | `String` |
| `getDate` | INT32 | DATE | `LocalDate` |
| `getTime` | INT32 or INT64 | TIME | `LocalTime` |
| `getTimestamp` | INT64, or legacy INT96 | TIMESTAMP | `Instant` |
| `getDecimal` | INT32, INT64, or FIXED_LEN_BYTE_ARRAY | DECIMAL | `BigDecimal` |
| `getUuid` | FIXED_LEN_BYTE_ARRAY | UUID | `UUID` |
| `getStruct` | | | `PqStruct` |
| `getList` | | LIST | `PqList` |
| `getMap` | | MAP | `PqMap` |
| `isNull` | Any | Any | `boolean` |

All methods are available as both `method(name)` and `method(index)`, except `getStruct`, `getList`, and `getMap` which are name-based only.

**Legacy INT96 timestamps:** Parquet files written by older versions of Apache Spark and Hive store timestamps in the deprecated INT96 physical type without a TIMESTAMP logical type annotation. `getTimestamp` detects INT96 automatically and decodes it to an `Instant`; no caller-side handling is required.

**Index-based access example:**

```java
// Get column indices once (before the loop)
int idIndex = fileReader.getFileSchema().getColumn("id").columnIndex();
int nameIndex = fileReader.getFileSchema().getColumn("name").columnIndex();

while (rowReader.hasNext()) {
    rowReader.next();
    if (!rowReader.isNull(idIndex)) {
        long id = rowReader.getLong(idIndex);      // No name lookup per row
        String name = rowReader.getString(nameIndex);
    }
}
```

**Null handling:** Primitive accessors (`getInt`, `getLong`, `getFloat`, `getDouble`, `getBoolean`) throw `NullPointerException` if the field is null — always check with `isNull()` first. Object accessors (`getString`, `getDate`, `getTimestamp`, `getDecimal`, `getUuid`, `getStruct`, `getList`, `getMap`) return `null` for null fields.

**Type validation:** The API validates at runtime that the requested type matches the schema. Mismatches throw `IllegalArgumentException` with a descriptive message.

## Predicate Pushdown (Filter)

Filter predicates enable three levels of predicate pushdown. At the row-group level, entire row groups whose statistics prove no rows can match are skipped. Within surviving row groups, the Column Index (per-page min/max statistics) is used to skip individual pages, avoiding unnecessary decompression and decoding. On remote backends like S3, only the matching pages are fetched, reducing network I/O. Finally, record-level filtering evaluates the predicate against each decoded row, so `createRowReader(filter)` returns only rows that actually match.

```java
import dev.hardwood.reader.FilterPredicate;

// Simple filter
FilterPredicate filter = FilterPredicate.gt("age", 21);

// Compound filter
FilterPredicate filter = FilterPredicate.and(
    FilterPredicate.gtEq("salary", 50000L),
    FilterPredicate.lt("age", 65)
);

// IN filter
FilterPredicate filter = FilterPredicate.in("department_id", 1, 3, 7);
FilterPredicate filter = FilterPredicate.inStrings("city", "NYC", "LA", "Chicago");

// NULL checks
FilterPredicate filter = FilterPredicate.isNull("middle_name");
FilterPredicate filter = FilterPredicate.isNotNull("email");

try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
     RowReader rowReader = fileReader.createRowReader(filter)) {

    while (rowReader.hasNext()) {
        rowReader.next();
        // Only rows from non-skipped row groups are returned
    }
}
```

Supported operators: `eq`, `notEq`, `lt`, `ltEq`, `gt`, `gtEq`, `in`, `inStrings`, `isNull`, `isNotNull`.
Supported physical types: `int`, `long`, `float`, `double`, `boolean`, `String` (comparison operators); `int`, `long`, `String` (`in`/`inStrings`); any type (`isNull`/`isNotNull`).
Supported logical types: `LocalDate`, `Instant`, `LocalTime`, `BigDecimal`, `UUID` (comparison operators).
Logical combinators: `and`, `or`, `not`; the `and` and `or` combinators also accept varargs for three or more conditions. All predicates, including those wrapped in `not`, are pushed down to the statistics level for row group and page skipping.

### Null handling

Comparison predicates (`eq`, `notEq`, `lt`, `ltEq`, `gt`, `gtEq`, `in`, `inStrings`) follow SQL three-valued logic: any comparison against a null column value yields UNKNOWN, and rows whose predicate is UNKNOWN are not returned. Put differently, **rows where the tested column is null are never returned by a comparison predicate** — including `notEq`.

`not(p)` preserves this behavior: rows where `p` is UNKNOWN stay UNKNOWN under negation and are dropped. The SQL identity `not(gt(x, v)) ≡ ltEq(x, v)` holds on all rows, including null ones.

To include null rows explicitly, combine with `isNull`:

```java
// rows with age > 30, plus rows where age is null
FilterPredicate filter = FilterPredicate.or(
    FilterPredicate.gt("age", 30),
    FilterPredicate.isNull("age")
);
```

!!! note "Divergence from parquet-java"
    parquet-java's `notEq` treats `null <> v` as true and therefore includes null rows, which breaks the SQL identity above. Hardwood applies uniform SQL three-valued-logic semantics across all comparison operators. To reproduce parquet-java's behavior, make the null-inclusion explicit: `or(notEq("x", v), isNull("x"))`.

### Logical Type Support

Factory methods are provided for common Parquet logical types, handling the physical
encoding automatically:

```java
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

// DATE columns
FilterPredicate filter = FilterPredicate.gt("birth_date", LocalDate.of(2000, 1, 1));

// TIMESTAMP columns — time unit is resolved from the column schema
FilterPredicate filter = FilterPredicate.gtEq("created_at",
    Instant.parse("2025-01-01T00:00:00Z"));

// TIME columns
FilterPredicate filter = FilterPredicate.lt("start_time", LocalTime.of(9, 0));

// DECIMAL columns — scale and physical type are resolved from the column schema
FilterPredicate filter = FilterPredicate.gtEq("amount", new BigDecimal("99.99"));

// UUID columns
FilterPredicate filter = FilterPredicate.eq("request_id",
    UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
```

Raw physical-type predicates (`int`, `long`, etc.) remain available for columns without logical types or for filtering on the underlying physical value directly.

Filters work with all reader types: `RowReader`, `ColumnReader`, `AvroRowReader`, and across multi-file readers.

### Limitations

- **Record-level filtering only applies to flat schemas
  ([#207](https://github.com/hardwood-hq/hardwood/issues/207)).** When the schema contains
  nested columns (structs, lists, or maps), record-level filtering is not active. Row-group
  and page-level statistics pushdown still apply, but non-matching rows within surviving pages
  will not be filtered out. A warning is logged when this occurs.
- **Bloom filter pushdown is not supported
  ([#180](https://github.com/hardwood-hq/hardwood/issues/180)).** Parquet files may contain
  Bloom filters for high-cardinality columns, but Hardwood does not currently use them for
  filter evaluation.
- **Dictionary-based filtering is not supported
  ([#196](https://github.com/hardwood-hq/hardwood/issues/196)).** Dictionary-encoded columns
  are not checked for predicate matches before decoding.

## Row Limit

A row limit instructs the reader to stop after the specified number of rows, avoiding unnecessary I/O and decoding. On remote backends like S3, this can reduce network transfers significantly — only the row groups and pages needed to satisfy the limit are fetched.

```java
// Read at most 100 rows
try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
     RowReader rowReader = fileReader.createRowReader(100)) {

    while (rowReader.hasNext()) {
        rowReader.next();
        // At most 100 rows will be returned
    }
}
```

The row limit can be combined with column projection and filters:

```java
try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
     RowReader rowReader = fileReader.createRowReader(
         ColumnProjection.columns("id", "name"),
         FilterPredicate.gt("age", 21),
         100)) {

    while (rowReader.hasNext()) {
        rowReader.next();
        // At most 100 matching rows with only id and name columns
    }
}
```

When combined with a filter, the limit applies to the number of **matching** rows, not the total number of scanned rows.

To read without a row limit, use the `createRowReader` overloads without `maxRows`.

## Column Projection

Column projection allows reading only a subset of columns from a Parquet file, improving performance by skipping I/O, decoding, and memory allocation for unneeded columns.

```java
import dev.hardwood.InputFile;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
     RowReader rowReader = fileReader.createRowReader(
         ColumnProjection.columns("id", "name", "created_at"))) {

    while (rowReader.hasNext()) {
        rowReader.next();

        // Access projected columns normally
        long id = rowReader.getLong("id");
        String name = rowReader.getString("name");
        Instant createdAt = rowReader.getTimestamp("created_at");

        // Accessing non-projected columns throws IllegalArgumentException
        // rowReader.getInt("age");  // throws "Column not in projection: age"
    }
}
```

**Projection options:**

- `ColumnProjection.all()` — read all columns (default)
- `ColumnProjection.columns("id", "name")` — read specific columns by name
- `ColumnProjection.columns("address")` — select an entire struct and all its children
- `ColumnProjection.columns("address.city")` — select a specific nested field (dot notation)

### Combining Projection and Filters

Column projection and predicate pushdown can be used together:

```java
try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
     RowReader rowReader = fileReader.createRowReader(
         ColumnProjection.columns("id", "name", "salary"),
         FilterPredicate.gtEq("salary", 50000L))) {

    while (rowReader.hasNext()) {
        rowReader.next();
        long id = rowReader.getLong("id");
        String name = rowReader.getString("name");
        long salary = rowReader.getLong("salary");
    }
}
```

The filter column does not need to be in the projection — Hardwood reads the filter column's statistics for pushdown regardless.

## Reading Multiple Files

When processing multiple Parquet files, use the `Hardwood` class to share a thread pool across readers.

### Unified Multi-File Reader

For reading multiple files as a single logical dataset, use `openAll()` which returns a `MultiFileParquetReader`. From there, create a row reader or column readers:

```java
import dev.hardwood.Hardwood;
import dev.hardwood.InputFile;
import dev.hardwood.reader.MultiFileParquetReader;
import dev.hardwood.reader.MultiFileRowReader;

List<InputFile> files = InputFile.ofPaths(
    Path.of("data_2024_01.parquet"),
    Path.of("data_2024_02.parquet"),
    Path.of("data_2024_03.parquet")
);

try (Hardwood hardwood = Hardwood.create();
     MultiFileParquetReader parquet = hardwood.openAll(files);
     MultiFileRowReader reader = parquet.createRowReader()) {

    while (reader.hasNext()) {
        reader.next();
        // Access data using the same API as single-file RowReader
        long id = reader.getLong("id");
        String name = reader.getString("name");
    }
}
```

The `MultiFileRowReader` provides cross-file prefetching: when pages from file N are running low, pages from file N+1 are already being prefetched. This eliminates I/O stalls at file boundaries.

By default, `Hardwood.create()` sizes the thread pool to the number of available processors. For custom thread pool sizing, use `HardwoodContext` directly:

```java
import dev.hardwood.HardwoodContext;

try (HardwoodContext context = HardwoodContext.create(4);  // 4 threads
     ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path), context);
     RowReader rowReader = reader.createRowReader()) {
    // ...
}
```

**With column projection:**

```java
try (Hardwood hardwood = Hardwood.create();
     MultiFileParquetReader parquet = hardwood.openAll(files);
     MultiFileRowReader reader = parquet.createRowReader(
         ColumnProjection.columns("id", "name", "amount"))) {

    while (reader.hasNext()) {
        reader.next();
        // Only projected columns are read
    }
}
```

## Column-Oriented Reading (ColumnReader)

The `ColumnReader` provides batch-oriented columnar access with typed primitive arrays, avoiding per-row method calls and boxing. This is the fastest way to consume Parquet data when you process columns independently.

### Single-File Column Reading

```java
import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ColumnReader;

try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) {
    // Create a column reader by name (spans all row groups automatically)
    try (ColumnReader fare = reader.createColumnReader("fare_amount")) {
        double sum = 0;
        while (fare.nextBatch()) {
            int count = fare.getRecordCount();
            double[] values = fare.getDoubles();
            BitSet nulls = fare.getElementNulls(); // null if column is required

            for (int i = 0; i < count; i++) {
                if (nulls == null || !nulls.get(i)) {
                    sum += values[i];
                }
            }
        }
    }
}
```

Typed accessors are available for each physical type: `getInts()`, `getLongs()`, `getFloats()`, `getDoubles()`, `getBooleans()`, `getBinaries()`, and `getStrings()`. Column readers can also be created by index via `createColumnReader(int columnIndex)`.

### Multi-File Column Reading

For reading columns across multiple files with cross-file prefetching, use `MultiFileColumnReaders`:

```java
import dev.hardwood.Hardwood;
import dev.hardwood.reader.MultiFileParquetReader;
import dev.hardwood.reader.MultiFileColumnReaders;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.schema.ColumnProjection;

try (Hardwood hardwood = Hardwood.create();
     MultiFileParquetReader parquet = hardwood.openAll(files);
     MultiFileColumnReaders columns = parquet.createColumnReaders(
         ColumnProjection.columns("passenger_count", "trip_distance", "fare_amount"))) {

    ColumnReader col0 = columns.getColumnReader("passenger_count");
    ColumnReader col1 = columns.getColumnReader("trip_distance");
    ColumnReader col2 = columns.getColumnReader("fare_amount");

    long passengerCount = 0;
    double tripDistance = 0, fareAmount = 0;

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

### Nested and Repeated Columns

For nested columns (lists, maps), `ColumnReader` provides multi-level offsets and per-level null bitmaps:

```java
try (ColumnReader reader = fileReader.createColumnReader("tags")) {
    while (reader.nextBatch()) {
        int recordCount = reader.getRecordCount();
        int valueCount = reader.getValueCount();
        byte[][] values = reader.getBinaries();
        int[] offsets = reader.getOffsets(0);            // record -> value position
        BitSet recordNulls = reader.getLevelNulls(0);    // null list records
        BitSet elementNulls = reader.getElementNulls();  // null elements within lists

        for (int r = 0; r < recordCount; r++) {
            if (recordNulls != null && recordNulls.get(r)) continue;
            int start = offsets[r];
            int end = (r + 1 < recordCount) ? offsets[r + 1] : valueCount;
            for (int i = start; i < end; i++) {
                if (elementNulls == null || !elementNulls.get(i)) {
                    // process values[i]
                }
            }
        }
    }
}
```

`getNestingDepth()` returns 0 for flat columns, or the number of offset levels for nested columns.

## Accessing File Metadata

Inspecting metadata before reading is useful for understanding file structure, choosing which columns to project, validating files in a pipeline, or building tooling. Hardwood exposes the full Parquet metadata hierarchy without reading any row data.

A Parquet file is organized as follows:

- **FileMetaData** — top-level: row count, schema, key-value metadata (e.g. Spark schema, pandas metadata), and the writer that produced the file (`createdBy`)
- **RowGroup** — a horizontal partition of the data; each row group contains all columns for a subset of rows
- **ColumnChunk** — one column within a row group; holds compression codec, byte sizes, and optional statistics (min/max values, null count) used for predicate pushdown

```java
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ColumnSchema;

try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) {
    FileMetaData metadata = reader.getFileMetaData();

    System.out.println("Version: " + metadata.version());
    System.out.println("Total rows: " + metadata.numRows());
    System.out.println("Created by: " + metadata.createdBy());

    // Access application-defined key-value metadata (e.g. Spark schema, pandas metadata, Avro schema)
    Map<String, String> kvMetadata = metadata.keyValueMetadata();
    for (Map.Entry<String, String> entry : kvMetadata.entrySet()) {
        System.out.println("  " + entry.getKey() + " = " + entry.getValue());
    }

    // Schema inspection
    FileSchema schema = reader.getFileSchema();
    for (int i = 0; i < schema.getColumnCount(); i++) {
        ColumnSchema column = schema.getColumn(i);
        System.out.println("Column " + i + ": " + column.name()
            + " (" + column.type() + ", " + column.repetitionType()
            + (column.logicalType() != null ? ", " + column.logicalType() : "")
            + ")");
    }

    // Row group and column chunk details
    for (int rg = 0; rg < metadata.rowGroups().size(); rg++) {
        RowGroup rowGroup = metadata.rowGroups().get(rg);
        System.out.println("Row group " + rg + ": "
            + rowGroup.numRows() + " rows, "
            + rowGroup.totalByteSize() + " bytes");

        for (ColumnChunk chunk : rowGroup.columns()) {
            ColumnMetaData col = chunk.metaData();
            System.out.println("  " + col.pathInSchema()
                + " [" + col.codec() + "]"
                + " compressed=" + col.totalCompressedSize()
                + " uncompressed=" + col.totalUncompressedSize());

            // Column statistics (if available)
            Statistics stats = col.statistics();
            if (stats != null && stats.nullCount() != null) {
                System.out.println("    nulls: " + stats.nullCount());
            }
        }
    }
}
```

## Error Handling

Hardwood throws specific exceptions for common error conditions:

| Exception | When |
|-----------|------|
| `IOException` | File is not a valid Parquet file (invalid magic number, corrupt footer) |
| `UnsupportedOperationException` | Compression codec library not on classpath — the message names the required dependency |
| `IllegalArgumentException` | Accessing a column not in the projection, type mismatch on accessor, or invalid column name |
| `NullPointerException` | Calling a primitive accessor (`getInt`, `getLong`, etc.) on a null field without checking `isNull()` first |
| `NoSuchElementException` | Calling `next()` on a `RowReader` when `hasNext()` returns `false` |
| `IllegalStateException` | Calling `ColumnReader` accessors before `nextBatch()`, or calling nested-column methods on a flat column |

