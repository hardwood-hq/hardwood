<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Usage

## Row-Oriented Reading

The `RowReader` provides a convenient row-oriented interface for reading Parquet files with typed accessor methods for type-safe field access.

```java
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

try (ParquetFileReader fileReader = ParquetFileReader.open(path);
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

| Method | Physical/Logical Type | Java Type |
|--------|----------------------|-----------|
| `getBoolean(name)` / `getBoolean(index)` | BOOLEAN | `boolean` |
| `getInt(name)` / `getInt(index)` | INT32 | `int` |
| `getLong(name)` / `getLong(index)` | INT64 | `long` |
| `getFloat(name)` / `getFloat(index)` | FLOAT | `float` |
| `getDouble(name)` / `getDouble(index)` | DOUBLE | `double` |
| `getBinary(name)` / `getBinary(index)` | BYTE_ARRAY | `byte[]` |
| `getString(name)` / `getString(index)` | STRING logical type | `String` |
| `getDate(name)` / `getDate(index)` | DATE logical type | `LocalDate` |
| `getTime(name)` / `getTime(index)` | TIME logical type | `LocalTime` |
| `getTimestamp(name)` / `getTimestamp(index)` | TIMESTAMP logical type | `Instant` |
| `getDecimal(name)` / `getDecimal(index)` | DECIMAL logical type | `BigDecimal` |
| `getUuid(name)` / `getUuid(index)` | UUID logical type | `UUID` |
| `getStruct(name)` | Nested struct | `PqStruct` |
| `getList(name)` | LIST logical type | `PqList` |
| `getMap(name)` | MAP logical type | `PqMap` |
| `isNull(name)` / `isNull(index)` | Any | `boolean` |

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

**Type validation:** The API validates at runtime that the requested type matches the schema. Mismatches throw `IllegalArgumentException` with a descriptive message.

## Column Projection

Column projection allows reading only a subset of columns from a Parquet file, improving performance by skipping I/O, decoding, and memory allocation for unneeded columns.

```java
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

try (ParquetFileReader fileReader = ParquetFileReader.open(path);
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

## Reading Multiple Files

When processing multiple Parquet files, use the `Hardwood` class to share a thread pool across readers.

### Unified Multi-File Reader

For reading multiple files as a single logical dataset, use `openAll()` which returns a `MultiFileParquetReader`. From there, create a row reader or column readers:

```java
import dev.hardwood.Hardwood;
import dev.hardwood.reader.MultiFileParquetReader;
import dev.hardwood.reader.MultiFileRowReader;

List<Path> files = List.of(
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
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ColumnReader;

try (ParquetFileReader reader = ParquetFileReader.open(path)) {
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

File metadata can be inspected without reading any row data:

```java
try (ParquetFileReader reader = ParquetFileReader.open(path)) {
    FileMetaData metadata = reader.getFileMetaData();

    System.out.println("Version: " + metadata.version());
    System.out.println("Total rows: " + metadata.numRows());
    System.out.println("Row groups: " + metadata.rowGroups().size());

    FileSchema schema = reader.getFileSchema();
    for (int i = 0; i < schema.getColumnCount(); i++) {
        ColumnSchema column = schema.getColumn(i);
        System.out.print("Column " + i + ": " + column.name() +
                         " (" + column.type() + ", " + column.repetitionType());

        // Display logical type if present
        if (column.logicalType() != null) {
            System.out.print(", " + column.logicalType());
        }
        System.out.println(")");
    }
}
```
