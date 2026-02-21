# Hardwood

A minimal dependency parser for the Apache Parquet file format.

## Project Vision

Now:

* Be light-weight: Implement the Parquet file format avoiding any 3rd party dependencies other than for compression algorithms (e.g. Snappy)
* Be correct: Support all Parquet files which are supported by the canonical [parquet-java](https://github.com/apache/parquet-java) library

In the future:

* Be fast: As fast or faster as parquet-java
* Be complete: Add a Parquet file writer

## Set-Up

### Using the BOM (Bill of Materials)

The `hardwood-bom` manages versions for all Hardwood modules and their optional runtime dependencies.
Import it in your dependency management so you can declare Hardwood dependencies without specifying versions:

**Maven:**

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>dev.hardwood</groupId>
            <artifactId>hardwood-bom</artifactId>
            <version>1.0.0-SNAPSHOT</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
```

**Gradle:**

```groovy
dependencies {
    implementation platform('dev.hardwood:hardwood-bom:1.0.0-SNAPSHOT')
}
```

Then declare dependencies without versions:

```xml
<dependency>
    <groupId>dev.hardwood</groupId>
    <artifactId>hardwood-core</artifactId>
</dependency>
```

### Adding the Core Dependency

If you prefer not to use the BOM, you can specify the version directly:

**Maven:**

```xml
<dependency>
    <groupId>dev.hardwood</groupId>
    <artifactId>hardwood-core</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

**Gradle:**

```groovy
implementation 'dev.hardwood:hardwood-core:1.0.0-SNAPSHOT'
```

### Compression Libraries

Hardwood supports reading Parquet files compressed with GZIP (built into Java), Snappy, ZSTD, LZ4, and Brotli. The compression libraries are optional dependencies—add only the ones you need.
When using the BOM, no version is required:

**Maven:**

```xml
<!-- Snappy compression -->
<dependency>
    <groupId>org.xerial.snappy</groupId>
    <artifactId>snappy-java</artifactId>
</dependency>

<!-- ZSTD compression -->
<dependency>
    <groupId>com.github.luben</groupId>
    <artifactId>zstd-jni</artifactId>
</dependency>

<!-- LZ4 compression -->
<dependency>
    <groupId>org.lz4</groupId>
    <artifactId>lz4-java</artifactId>
</dependency>

<!-- Brotli compression -->
<dependency>
    <groupId>com.aayushatharva.brotli4j</groupId>
    <artifactId>brotli4j</artifactId>
</dependency>
```

**Gradle:**

```groovy
// Snappy compression
implementation 'org.xerial.snappy:snappy-java'

// ZSTD compression
implementation 'com.github.luben:zstd-jni'

// LZ4 compression
implementation 'org.lz4:lz4-java'

// Brotli compression
implementation 'com.aayushatharva.brotli4j:brotli4j'
```

<details>
<summary>Without the BOM (explicit versions)</summary>

**Maven:**

```xml
<dependency>
    <groupId>org.xerial.snappy</groupId>
    <artifactId>snappy-java</artifactId>
    <version>1.1.10.8</version>
</dependency>
<dependency>
    <groupId>com.github.luben</groupId>
    <artifactId>zstd-jni</artifactId>
    <version>1.5.7-6</version>
</dependency>
<dependency>
    <groupId>org.lz4</groupId>
    <artifactId>lz4-java</artifactId>
    <version>1.8.1</version>
</dependency>
<dependency>
    <groupId>com.aayushatharva.brotli4j</groupId>
    <artifactId>brotli4j</artifactId>
    <version>1.20.0</version>
</dependency>
```

**Gradle:**

```groovy
implementation 'org.xerial.snappy:snappy-java:1.1.10.8'
implementation 'com.github.luben:zstd-jni:1.5.7-6'
implementation 'org.lz4:lz4-java:1.8.1'
implementation 'com.aayushatharva.brotli4j:brotli4j:1.20.0'
```

</details>

If you attempt to read a file using a compression codec whose library is not on the classpath, Hardwood will throw an exception with a message indicating which dependency to add.

#### Optional: Faster GZIP with libdeflate (Java 22+)

Hardwood can use [libdeflate](https://github.com/ebiggers/libdeflate) for GZIP decompression, which is significantly faster than the built-in Java implementation. This feature requires **Java 22 or newer** (it uses the Foreign Function & Memory API which became stable in Java 22).

Allow native access in order to use libdeflate:

```bash
--enable-native-access=ALL-UNNAMED
```

libdeflate is a native library that must be installed on your system:

**macOS:**
```bash
brew install libdeflate
```

**Linux (Debian/Ubuntu):**
```bash
apt install libdeflate-dev
```

**Linux (Fedora):**
```bash
dnf install libdeflate-devel
```

**Windows:**
```bash
vcpkg install libdeflate
```

Or download from [GitHub releases](https://github.com/ebiggers/libdeflate/releases).

When libdeflate is installed and available on the library path, Hardwood will automatically use it for GZIP decompression. To disable libdeflate and use the built-in Java implementation instead, set the system property:

```bash
-Dhardwood.uselibdeflate=false
```

#### Optional: SIMD Acceleration with Vector API (Java 22+)

Hardwood can use the Java Vector API (SIMD) to accelerate certain decoding operations like counting non-null values, marking nulls, and dictionary lookups. This feature requires **Java 22 or newer** and is enabled automatically when available.

To enable the Vector API incubator module, add this JVM argument:

```bash
--add-modules jdk.incubator.vector
```

When SIMD is available and enabled, you'll see an INFO log message at startup:
```
SIMD support: enabled (256-bit vectors)
```

The vector width depends on your CPU (128-bit for SSE/NEON, 256-bit for AVX2, 512-bit for AVX-512).

To disable SIMD and force scalar operations (for debugging or comparison), set the system property:

```bash
-Dhardwood.simd.disabled=true
```

---

## Usage

### Row-Oriented Reading

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
    }
}
```

**Typed accessor methods:**

All accessor methods are available in two forms:
- **Name-based** (e.g., `getInt("column_name")`) - convenient for ad-hoc access
- **Index-based** (e.g., `getInt(columnIndex)`) - faster for performance-critical loops

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

### Column Projection

Column projection allows reading only a subset of columns from a Parquet file, improving performance by skipping I/O, decoding, and memory allocation for unneeded columns.

```java
import dev.hardwood.reader.ColumnProjection;
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

```java
// Read all columns (default behavior)
ColumnProjection.all()

// Read specific columns by name
ColumnProjection.columns("id", "name", "address")

// For nested schemas - select entire struct and all its children
ColumnProjection.columns("address")  // includes address.street, address.city, etc.

// For nested schemas - select specific nested field (dot notation)
ColumnProjection.columns("address.city")  // only the city field
```

**With index-based access:**

When using column projection, the index-based accessors use *projected* indices (0, 1, 2, ...) rather than the original schema indices:

```java
try (ParquetFileReader fileReader = ParquetFileReader.open(path);
     RowReader rowReader = fileReader.createRowReader(
         ColumnProjection.columns("name", "created_at"))) {  // 2 columns projected

    while (rowReader.hasNext()) {
        rowReader.next();

        // Projected index 0 = "name", projected index 1 = "created_at"
        String name = rowReader.getString(0);
        Instant createdAt = rowReader.getTimestamp(1);

        // getFieldCount() returns 2 (projected count)
        // getFieldName(0) returns "name"
        // getFieldName(1) returns "created_at"
    }
}
```

### Reading Multiple Files

When processing multiple Parquet files, use the `Hardwood` class to share a thread pool across readers.

#### Unified Multi-File Reader

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

A varargs overload is available when the paths are known at compile time:

```java
MultiFileParquetReader parquet = hardwood.openAll(
    Path.of("data_2024_01.parquet"),
    Path.of("data_2024_02.parquet"));
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

The `MultiFileParquetReader` reads the schema eagerly (from the first file), but defers page scanning until a reader is created. This lets you inspect `parquet.getFileSchema()` before deciding which columns to project.

#### Individual File Processing

When you need to process files independently (e.g., different handling per file), use individual readers:

```java
import dev.hardwood.Hardwood;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

try (Hardwood hardwood = Hardwood.create()) {
    for (Path file : parquetFiles) {
        try (ParquetFileReader reader = hardwood.open(file);
                RowReader rowReader = reader.createRowReader()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                // Process rows...
            }
        }
    }
}
```

This avoids the overhead of creating and destroying thread pools for each file. The thread pool is sized to the number of available processors by default, or you can specify a custom size:

```java
try (Hardwood hardwood = Hardwood.create(4)) {  // 4 threads
    // ...
}
```

For single-file usage, `ParquetFileReader.open(path)` remains the simplest option—it manages its own thread pool internally.

### Column-Oriented Reading (ColumnReader)

The `ColumnReader` provides batch-oriented columnar access with typed primitive arrays, avoiding per-row method calls and boxing. This is the fastest way to consume Parquet data when you process columns independently.

#### Single-File Column Reading

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

#### Multi-File Column Reading

For reading columns across multiple files with cross-file prefetching, use `MultiFileColumnReaders`:

```java
import dev.hardwood.Hardwood;
import dev.hardwood.reader.MultiFileParquetReader;
import dev.hardwood.reader.MultiFileColumnReaders;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnProjection;

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

#### Nested and Repeated Columns

For nested columns (lists, maps), `ColumnReader` provides multi-level offsets and per-level null bitmaps:

```java
try (ColumnReader reader = fileReader.createColumnReader("tags")) {
    while (reader.nextBatch()) {
        int recordCount = reader.getRecordCount();
        int valueCount = reader.getValueCount();
        byte[][] values = reader.getBinaries();
        int[] offsets = reader.getOffsets(0);            // record → value position
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

### Accessing File Metadata

Both approaches allow you to inspect file metadata before reading:

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

### Hadoop Compatibility (hardwood-hadoop-compat)

The `hardwood-hadoop-compat` module provides a drop-in replacement for parquet-java's `ParquetReader<Group>` API. This allows users migrating from parquet-java to use Hardwood with minimal code changes.

**Features:**
- Provides `org.apache.parquet.*` namespace classes compatible with parquet-java
- Includes Hadoop shims (`Path`, `Configuration`) that wrap Java NIO - no Hadoop dependency required
- Supports the familiar builder pattern and Group-based record reading

**Usage:**

```java
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.GroupReadSupport;
import org.apache.parquet.hadoop.ParquetReader;

Path path = new Path("data.parquet");

try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path).build()) {
    Group record;
    while ((record = reader.read()) != null) {
        // Read primitive fields
        long id = record.getLong("id", 0);
        String name = record.getString("name", 0);
        int age = record.getInteger("age", 0);

        // Read nested groups (structs)
        Group address = record.getGroup("address", 0);
        String city = address.getString("city", 0);
        int zip = address.getInteger("zip", 0);

        // Check for null/optional fields
        int count = record.getFieldRepetitionCount("optional_field");
        if (count > 0) {
            String value = record.getString("optional_field", 0);
        }
    }
}
```

**Note:** This module provides its own interface copies in the `org.apache.parquet.*` namespace. It cannot be used alongside parquet-java on the same classpath.

---

## Status

This is Alpha quality software, resulting from a handful of coding sessions with Claude Code.
Note that while this project welcomes the usage of LLMs,
vibe coding (i.e. blindly accepting AI-generated changes without understanding them) is not accepted.
While there's currently a focus on quick iteration (closing feature gaps),
the aspiration is to build a high quality code base which is maintainable, extensible, performant, and safe.

## Performance Testing

These are the results from parsing files of the NYC Yellow Taxi Trip data set (2025-01 to 2025-11),
running on a Macbook Pro M3 Max.
The test parses all files and adds up three columns.

```
=== Performance Test Results (2026-01-17) ===
Files processed: 11
Total rows: 44,417,596

Contender            |   Time (s) |   passenger_count |     trip_distance |       fare_amount
---------------------+------------+-------------------+-------------------+------------------
Hardwood             |      23.53 |        43,991,423 |    303,521,661.96 |    801,793,105.07
parquet-java         |      84.31 |        43,991,423 |    303,521,661.96 |    801,793,105.07
```

Note that Hardwood parallelizes processing of column batches across CPU cores; hence, the absolute result is better
than parquet-java, but per-core performance is worse. No optimizations have been made at this point.

```
=== Performance Test Results (2026-01-18, avoiding some copying) ===
Contender            |   Time (s) |   passenger_count |     trip_distance |       fare_amount
---------------------+------------+-------------------+-------------------+------------------
Hardwood             |      18.73 |        43,991,423 |    303,521,540.36 |    801,792,434.75
```

```
=== Performance Test Results (2026-01-18, avoiding copying during decoding) ===
  Contender                Time (s)     Records/sec   Records/sec/core       MB/sec
  -------------------------------------------------------------------------------------
  Hardwood                    14.97       2,966,315            185,395         48.2
```

```
=== Performance Test Results (2026-01-18, fast path for simple columns) ===
  Contender                Time (s)     Records/sec   Records/sec/core       MB/sec
  -------------------------------------------------------------------------------------
  Hardwood                    10.82       4,105,897            256,619         66.7
```

```
=== Performance Test Results (2026-01-19, avoiding boxing and struct materialization for flat schemas) ===
  Contender                Time (s)     Records/sec   Records/sec/core       MB/sec
  -------------------------------------------------------------------------------------
  Hardwood                     3.30      13,463,958            841,497        218.6
```

```
=== Performance Test Results (2026-01-22, avoiding more boxing) ===
  Contender                Time (s)     Records/sec   Records/sec/core       MB/sec
  -------------------------------------------------------------------------------------
  Hardwood                     2.94      15,118,310            944,894        245.5
```

```
=== Performance Test Results (2026-01-22, prefetching) ===
  Contender                Time (s)     Records/sec   Records/sec/core       MB/sec
  -------------------------------------------------------------------------------------
  Hardwood                     2.57      17,310,053          1,081,878        281.1
```

```
=== Performance Test Results (2026-01-22, RLE decoder tweaks) ===
Performance:
  Contender                Time (s)     Records/sec   Records/sec/core       MB/sec
  -------------------------------------------------------------------------------------
  Hardwood                     2.24      19,793,938          1,237,121        321.4
```

```
Performance Test Results (2026-01-26, Adaptive page pre-fetching) ===
  Contender                Time (s)     Records/sec   Records/sec/core       MB/sec
  -------------------------------------------------------------------------------------
  Hardwood                     1.87      23,790,892          1,486,931        386.3
```

## Implementation Status & Roadmap

A from-scratch implementation of Apache Parquet reader/writer in Java with no dependencies except compression libraries.

### Phase 1: Foundation & Format Understanding

#### 1.1 Core Data Structures
- [x] Define physical types enum: `BOOLEAN`, `INT32`, `INT64`, `INT96`, `FLOAT`, `DOUBLE`, `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY`
- [x] Define logical types as sealed interface with implementations:
  - [x] STRING, ENUM, UUID, DATE, TIME, TIMESTAMP, DECIMAL, JSON, BSON
  - [x] INT_8, INT_16, INT_32, INT_64 (signed integers)
  - [x] UINT_8, UINT_16, UINT_32, UINT_64 (unsigned integers)
  - [x] LIST (nested list support with arbitrary depth)
  - [x] MAP (map<key, value> with nested maps and struct values)
  - [ ] INTERVAL (not implemented)
- [x] Define repetition types: `REQUIRED`, `OPTIONAL`, `REPEATED`

#### 1.2 Schema Representation
- [x] Implement `SchemaElement` class (name, type, repetition, logicalType, children, fieldId, typeLength)
- [x] Implement `MessageType` as root schema container
- [x] Schema traversal utilities (getColumn, getMaxDefinitionLevel, getMaxRepetitionLevel)

#### 1.3 Thrift Compact Protocol (Manual Implementation)
- [x] Implement `ThriftCompactReader`
  - [x] Varint decoding
  - [x] Zigzag decoding
  - [x] Field header parsing
  - [x] Struct reading
  - [x] List/Map container reading
  - [x] String/Binary reading
- [x] Separate Thrift readers into dedicated classes (in `internal.thrift` package)
  - [x] `FileMetaDataReader`
  - [x] `RowGroupReader`
  - [x] `ColumnChunkReader`
  - [x] `ColumnMetaDataReader`
  - [x] `PageHeaderReader`
  - [x] `DataPageHeaderReader`
  - [x] `DataPageHeaderV2Reader`
  - [x] `DictionaryPageHeaderReader`
  - [x] `SchemaElementReader`
  - [x] `LogicalTypeReader` (union deserialization with nested structs)
- [ ] Implement `ThriftCompactWriter`
  - [ ] Varint encoding
  - [ ] Zigzag encoding
  - [ ] Field header writing
  - [ ] Struct writing
  - [ ] List/Map container writing
  - [ ] String/Binary writing

---

### Phase 2: Encoding Implementations

#### 2.1 Plain Encoding (PLAIN)
- [x] Little-endian integer encoding/decoding (INT32, INT64)
- [x] Little-endian float encoding/decoding (FLOAT, DOUBLE)
- [x] INT96 encoding/decoding
- [x] Length-prefixed byte array encoding/decoding
- [x] Fixed-length byte array encoding/decoding
- [x] Bit-packed boolean encoding/decoding

#### 2.2 Dictionary Encoding (RLE_DICTIONARY)
- [ ] Implement `DictionaryEncoder<T>` (valueToIndex map, indexToValue list)
- [x] Implement `DictionaryDecoder<T>`
- [ ] Dictionary page serialization
- [x] Dictionary page deserialization
- [ ] Fallback to plain encoding when dictionary grows too large

#### 2.3 RLE/Bit-Packing Hybrid
- [ ] Implement `RleBitPackingHybridEncoder`
  - [ ] Bit width calculation
  - [ ] RLE encoding (repeated values)
  - [ ] Bit-packing encoding (groups of 8)
  - [ ] Automatic mode switching
- [x] Implement `RleBitPackingHybridDecoder`
  - [x] Header byte parsing (RLE vs bit-packed)
  - [x] RLE decoding
  - [x] Bit-packing decoding

#### 2.4 Delta Encodings
- [x] DELTA_BINARY_PACKED
  - [x] Block/miniblock structure
  - [x] Min delta calculation per block
  - [x] Bit width calculation per miniblock
  - [ ] Encoder implementation
  - [x] Decoder implementation
- [x] DELTA_LENGTH_BYTE_ARRAY
  - [x] Length encoding with DELTA_BINARY_PACKED
  - [x] Raw byte concatenation
  - [ ] Encoder implementation
  - [x] Decoder implementation
- [x] DELTA_BYTE_ARRAY
  - [x] Prefix length calculation
  - [x] Suffix extraction
  - [ ] Encoder implementation
  - [x] Decoder implementation

#### 2.5 Byte Stream Split (BYTE_STREAM_SPLIT)
- [x] Float byte separation/interleaving
- [x] Double byte separation/interleaving
- [x] FIXED_LEN_BYTE_ARRAY support
- [ ] Encoder implementation
- [x] Decoder implementation

---

### Phase 3: Page Structure

#### 3.1 Page Types
- [x] Implement `DataPageV1` structure
- [x] Implement `DataPageV2` structure
- [x] Implement `DictionaryPage` structure

#### 3.2 Page Header (Thrift)
- [x] Define `PageHeader` Thrift structure
- [x] Define `DataPageHeader` Thrift structure
- [x] Define `DataPageHeaderV2` Thrift structure
- [x] Define `DictionaryPageHeader` Thrift structure
- [ ] Page header serialization
- [x] Page header deserialization
- [ ] CRC32 calculation and validation

#### 3.3 Definition & Repetition Levels
- [ ] Implement `LevelEncoder` using RLE/bit-packing hybrid
- [x] Implement `LevelDecoder`
- [x] Max level calculation from schema
- [x] Null detection from definition levels

---

### Phase 4: Column Chunk & Row Group

#### 4.1 Column Chunk Structure
- [x] Implement `ColumnChunk` class
- [x] Implement `ColumnMetaData` Thrift structure
  - [x] Type, encodings, path in schema
  - [x] Codec, num values, sizes
  - [x] Page offsets (data, index, dictionary)
  - [x] Statistics
- [ ] Column chunk serialization
- [x] Column chunk deserialization

#### 4.2 Row Group
- [x] Implement `RowGroup` class
- [ ] Row group metadata serialization
- [x] Row group metadata deserialization
- [ ] Sorting column tracking (optional)

---

### Phase 5: File Structure

#### 5.1 File Layout
- [x] Magic number validation ("PAR1")
- [x] Footer location calculation (last 8 bytes)
- [x] Row group offset tracking

#### 5.2 FileMetaData (Thrift)
- [x] Implement `FileMetaData` Thrift structure
  - [x] Version
  - [x] Schema elements
  - [x] Num rows
  - [x] Row groups
  - [x] Key-value metadata
  - [x] Created by string
  - [x] Column orders
- [ ] FileMetaData serialization
- [x] FileMetaData deserialization

---

### Phase 6: Writer Implementation

#### 6.1 Writer Architecture
- [ ] Implement `ParquetWriter<T>` main class
- [ ] Implement `WriterConfig` (row group size, page size, dictionary size, codec, version)
- [ ] Implement `RowGroupWriter`
- [ ] Implement `ColumnWriter`
- [ ] Implement `PageWriter`

#### 6.2 Write Flow
- [ ] Record buffering
- [ ] Row group size tracking
- [ ] Automatic row group flushing
- [ ] Dictionary page writing
- [ ] Data page encoding and writing
- [ ] Page compression
- [ ] Footer writing
- [ ] File finalization

#### 6.3 Record Shredding (Dremel Algorithm)
- [ ] Implement schema traversal for shredding
- [ ] Definition level calculation
- [ ] Repetition level calculation
- [ ] Primitive value emission
- [ ] Nested structure handling
- [ ] Repeated field handling
- [ ] Optional field handling

---

### Phase 7: Reader Implementation

#### 7.1 Reader Architecture
- [ ] Implement `ParquetReader<T>` main class
- [x] Implement `ParquetFileReader` (low-level)
- [x] Implement `RowReader` (row-oriented API with parallel batch fetching)
- [x] Implement `ColumnReader`
- [x] Implement `PageReader`
- [x] Separate Thrift readers from metadata types (moved to `internal.thrift` package)

#### 7.2 Read Flow
- [x] Footer reading and parsing
- [x] Schema reconstruction from schema elements
- [x] Row group iteration
- [x] Column chunk seeking
- [x] Dictionary page reading
- [x] Data page reading and decoding
- [x] Page decompression
- [x] Parallel column batch fetching

#### 7.3 Record Assembly (Inverse Dremel)
- [x] Column reader synchronization (via RowReader)
- [x] Definition level interpretation
- [x] Repetition level interpretation
- [x] Null value handling
- [x] Nested structure reconstruction (structs within structs, arbitrary depth)
- [x] List assembly from repeated fields (simple lists, list of structs)
- [x] Nested list assembly (list<list<T>>, list<list<list<T>>>, arbitrary depth)
- [x] Record completion detection

#### 7.4 Logical Type Support
- [x] Logical type metadata parsing from Thrift
  - [x] `LogicalTypeReader` - union deserialization with nested struct handling
  - [x] Parameterized types (DECIMAL, TIMESTAMP, TIME, INT)
  - [x] Boolean field handling in Thrift Compact Protocol (0x01/0x02 type codes)
  - [x] Nested struct reading with field ID context management (push/pop)
- [x] Logical type conversions in Row API
  - [x] `LogicalTypeConverter` - centralized conversion logic
  - [x] STRING (BYTE_ARRAY → String with UTF-8 decoding)
  - [x] DATE (INT32 → LocalDate, days since epoch)
  - [x] TIMESTAMP (INT64 → Instant with MILLIS/MICROS/NANOS units)
  - [x] TIME (INT32/INT64 → LocalTime with MILLIS/MICROS/NANOS units)
  - [x] DECIMAL (FIXED_LEN_BYTE_ARRAY → BigDecimal with scale/precision)
  - [x] INT_8, INT_16 (INT32 → narrowed int with validation)
  - [x] INT_32, INT_64 (INT32/INT64 → int/long)
  - [x] UINT_8, UINT_16, UINT_32, UINT_64 (unsigned integers)
  - [x] Generic getObject() with automatic conversion based on logical type
- [x] Logical type implementations (code exists, partial test coverage)
  - [x] ENUM (no test coverage - PyArrow doesn't write ENUM logical type)
  - [x] UUID (tested with PyArrow 21+ which writes UUID logical type)
  - [x] JSON (no test coverage - PyArrow doesn't write JSON logical type)
  - [x] BSON (no test coverage - PyArrow doesn't write BSON logical type)
- [x] Nested types (tested)
  - [x] Nested structs (arbitrary depth, e.g., Customer → Account → Organization → Address)
  - [x] Lists of primitives (list<int>, list<string>, etc.)
  - [x] Lists of structs (list<struct>)
  - [x] Nested lists (list<list<T>>, list<list<list<T>>>, arbitrary depth)
  - [x] Logical type conversion within nested lists (e.g., list<list<timestamp>>)
  - [x] Maps (map<string, int>, map<string, struct>, etc.)
  - [x] Nested maps (map<string, map<string, int>>)
  - [x] List of maps (list<map<string, int>>)
- [ ] Not implemented (future)
  - [ ] INTERVAL

---

### Phase 8: Compression Integration

#### 8.1 Compression Interface
- [x] Define `CompressionCodec` interface (compress, decompress, getName)
- [x] Implement codec registry

#### 8.2 Codec Implementations
- [x] UNCOMPRESSED (passthrough)
- [x] GZIP (java.util.zip, no external dependency)
- [x] SNAPPY (snappy-java)
- [x] LZ4 (lz4-java) - supports both Hadoop and raw LZ4 formats
- [x] ZSTD (zstd-jni)
- [x] BROTLI (brotli4j)
- [ ] LZO (lzo-java, optional)

---

### Phase 9: Advanced Features

#### 9.1 Statistics
- [ ] Implement `Statistics<T>` class (min, max, nullCount, distinctCount)
- [ ] Statistics collection during writing
- [ ] Binary min/max truncation for efficiency
- [ ] Statistics serialization/deserialization
- [ ] Type-specific comparators

#### 9.2 Page Index (Column Index & Offset Index)
- [ ] Implement `ColumnIndex` structure
  - [ ] Null pages tracking
  - [ ] Min/max values per page
  - [ ] Boundary order
  - [ ] Null counts
- [ ] Implement `OffsetIndex` structure
  - [ ] Page locations (offset, size, first row)
- [ ] Page index writing
- [ ] Page index reading
- [ ] Page skipping based on index

#### 9.3 Bloom Filters
- [ ] Implement split block bloom filter
- [ ] XXHASH implementation (or integration)
- [ ] Bloom filter serialization
- [ ] Bloom filter deserialization
- [ ] Bloom filter checking during reads

#### 9.4 Predicate Pushdown
- [ ] Implement `FilterPredicate` hierarchy
  - [ ] Eq, NotEq
  - [ ] Lt, LtEq, Gt, GtEq
  - [ ] In
  - [ ] And, Or, Not
- [ ] Statistics-based row group filtering
- [ ] Page index-based page filtering
- [ ] Bloom filter-based filtering
- [ ] Filter evaluation engine

---

### Phase 10: Public API Design

#### 10.1 Schema Builder API
- [ ] Implement fluent `Types.buildMessage()` API
- [ ] Primitive type builders with logical type support
- [ ] Group builders for nested structures
- [ ] List and Map convenience builders

#### 10.2 Writer API
- [ ] Implement `ParquetWriter.builder(path)` fluent API
- [ ] Configuration methods (schema, codec, sizes, etc.)
- [ ] GenericRecord support
- [ ] Custom record materializer support

#### 10.3 Reader API
- [ ] Implement `ParquetReader.builder(path)` fluent API
- [x] Column projection (select subset of columns to read)
- [ ] Filter predicate support
- [ ] GenericRecord support
- [ ] Custom record materializer support

#### 10.4 Low-Level API
- [ ] Direct column chunk access
- [ ] Page-level iteration
- [ ] Raw value reading with levels

---

## Milestones

### Milestone 1: Minimal Viable Reader ✓
- [x] Thrift compact protocol reader
- [x] Footer parsing
- [x] Schema reconstruction
- [x] PLAIN encoding decoder
- [x] UNCOMPRESSED pages only
- [x] Flat schemas only (no nesting)
- [x] **Validate**: Read simple files from parquet-testing

### Milestone 2: Minimal Viable Writer
- [ ] Thrift compact protocol writer
- [ ] PLAIN encoding encoder
- [ ] Footer serialization
- [ ] Flat schema writing
- [ ] **Validate**: Round-trip flat records

### Milestone 3: Core Encodings & Logical Types ✓
- [x] RLE/bit-packing hybrid
- [x] Dictionary encoding
- [x] Definition/repetition levels
- [x] Logical type parsing and conversion (STRING, DATE, TIMESTAMP, TIME, DECIMAL, INT types)
- [x] Nested schema support (Dremel algorithm for reading)
  - [x] Nested structs (arbitrary depth)
  - [x] Lists of primitives and structs
  - [x] Nested lists (arbitrary depth)
- [x] **Validate**: Read nested structures (AddressBook example from Dremel paper)

### Milestone 4: Compression ✓
- [x] GZIP integration
- [x] Snappy integration
- [x] ZSTD integration
- [x] LZ4 integration (both Hadoop and raw formats)
- [x] **Validate**: Read files with various codecs from parquet-testing

### Milestone 5: Advanced Encodings
- [x] DELTA_BINARY_PACKED
- [x] DELTA_LENGTH_BYTE_ARRAY
- [x] DELTA_BYTE_ARRAY
- [x] BYTE_STREAM_SPLIT
- [x] **Validate**: Read files using these encodings

### Milestone 6: Optimization Features
- [ ] Statistics collection and usage
- [ ] Page indexes
- [ ] Bloom filters
- [ ] Predicate pushdown
- [ ] **Validate**: Performance improvement with filtering

### Milestone 7: Production Ready
- [ ] Comprehensive error handling
- [ ] Input validation
- [ ] Memory management optimization
- [x] Parallel reading support (parallel batch fetching in RowReader)
- [ ] Parallel writing support
- [ ] **Validate**: Full compatibility with parquet-java and PyArrow

---

## Testing

### Test Data Sources
- [x] Clone parquet-testing repository
- [ ] Clone arrow-testing repository
- [x] Generate test files with PyArrow (various configs)
- [ ] Generate test files with DuckDB

### Test Summary

**Current Pass Rate: 207/215 (96.3%) parquet-testing, 39 unit tests**

Progress:
- Started (first column only): 163/215 (75.8%)
- After Dictionary Encoding (first column only): 187/220 (85.0%)
- After fixing tests to read ALL columns: 177/215 (82.3%)
- After fixing field ID bugs (ColumnMetaData): 178/215 (82.8%)
- After boolean bit-packing fix: 182/215 (84.7%)
- After DATA_PAGE_V2 support: 184/215 (85.6%)
- After FIXED_LEN_BYTE_ARRAY support: 188/215 (87.4%)
- After GZIP compression support: 189/215 (87.9%)
- After ZSTD compression support: 190/215 (88.4%)
- After nested types support: 25 unit tests (nested structs, lists, nested lists)
- After DELTA_BINARY_PACKED/DELTA_LENGTH_BYTE_ARRAY: 189/215 (87.9%), 28 unit tests
- After DELTA_BYTE_ARRAY: 193/215 (89.8%), 29 unit tests
- After LZ4 compression: 198/215 (92.1%), 29 unit tests
- After DATA_PAGE_V2 decompression fix + RLE boolean: 202/215 (94.0%), 29 unit tests
- After BYTE_STREAM_SPLIT encoding: 204/215 (94.9%), 29 unit tests
- After Snappy DATA_PAGE_V2 fixes: 206/215 (95.8%), 29 unit tests
- After dict-page-offset-zero fix: 207/215 (96.3%), 29 unit tests
- After MAP support: 207/215 (96.3%), 39 unit tests

Remaining Failures by Category (8 total):
- Bad data files (intentionally malformed): 6 files (includes fixed_length_byte_array which has truncated page data - PyArrow also fails)
- Java array size limit: 1 file (large_string_map.brotli - column chunk exceeds 2GB, parquet-java has same limitation)
- Other edge cases: 1 file (case-046)

### Test Categories
- [ ] Round-trip tests (write → read → compare)
- [x] Compatibility tests (read files from other implementations)
- [x] Logical type tests (comprehensive coverage for all implemented types)
  - [x] STRING, DATE, TIMESTAMP, TIME, DECIMAL, UUID conversions
  - [x] Signed integers (INT_8, INT_16) with narrowing
  - [x] Unsigned integers (UINT_8, UINT_16, UINT_32, UINT_64)
  - [x] Parameterized type metadata (scale/precision, time units, bit widths)
- [x] Nested type tests
  - [x] Nested structs (4-level deep: Customer → Account → Organization → Address)
  - [x] Lists of primitives (int, string)
  - [x] Lists of structs
  - [x] Nested lists (list<list<int>>, list<list<list<int>>>)
  - [x] Logical types in nested lists (list<list<timestamp>>)
  - [x] AddressBook example from Dremel paper
  - [x] Maps (map<string, int>)
  - [x] Nested maps (map<string, map<string, int>>)
  - [x] Map with struct values (map<string, struct>)
  - [x] List of maps (list<map<string, int>>)
- [ ] Cross-compatibility tests (write files, read with other implementations)
- [ ] Fuzz testing (random schemas and data)
- [ ] Edge cases (empty files, single values, max nesting)
- [ ] Performance benchmarks vs parquet-java

### Tools for Validation
- [ ] Set up parquet-cli for metadata inspection
- [x] PyArrow scripts for file inspection
- [ ] DuckDB for quick validation queries

---

## Resources

- [Parquet Format Specification](https://github.com/apache/parquet-format)
- [Thrift Compact Protocol Spec](https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md)
- [Dremel Paper](https://research.google/pubs/pub36632/)
- [parquet-java Reference](https://github.com/apache/parquet-java)
- [parquet-testing Files](https://github.com/apache/parquet-testing)
- [arrow-testing Files](https://github.com/apache/arrow-testing)

---

## Build

This project requires **Java 25 or newer for building** (to create the multi-release JAR with Java 22+ FFM support). The resulting JAR runs on **Java 21+** (libdeflate support requires Java 22+).

It comes with the Apache [Maven wrapper](https://github.com/takari/maven-wrapper),
i.e. a Maven distribution will be downloaded automatically, if needed.

Run the following command to build this project:

```shell
./mvnw clean verify
```

On Windows, run the following command:

```shell
mvnw.cmd clean verify
```

Pass the `-Dquick` option to skip all non-essential plug-ins and create the output artifact as quickly as possible:

```shell
./mvnw clean verify -Dquick
```

Run the following command to format the source code and organize the imports as per the project's conventions:

```shell
./mvnw process-sources
```

### Running Performance Tests

The performance testing modules are not included in the default build. Enable them with `-Pperformance-test`.

#### End-to-End Performance Tests

Compare Hardwood against parquet-java using NYC Yellow Taxi Trip data:

```shell
./mvnw test -Pperformance-test
```

This will download test data on the first run (up to ~4GB for the full 2016-2025 dataset).

**Configuration options:**

| Property | Default | Description |
|----------|---------|-------------|
| `perf.contenders` | `hardwood` | Which implementations to benchmark: `hardwood`, `parquet-java`, or `all` |
| `perf.start` | `2016-01` | Start year-month for data range |
| `perf.end` | `2025-11` | End year-month for data range |

**Examples:**

```shell
# Run only Hardwood (default)
./mvnw test -Pperformance-test

# Compare Hardwood against parquet-java
./mvnw test -Pperformance-test -Dperf.contenders=all

# Run only for 2025 data
./mvnw test -Pperformance-test -Dperf.start=2025-01

# Run for a specific year
./mvnw test -Pperformance-test -Dperf.start=2024-01 -Dperf.end=2024-12

# Run for a single month
./mvnw test -Pperformance-test -Dperf.start=2025-06 -Dperf.end=2025-06
```

#### JMH Micro-Benchmarks

For detailed micro-benchmarks, build the JMH benchmark JAR and run it directly:

```shell
# Build the benchmark JAR
./mvnw package -Pperformance-test -pl performance-testing/micro-benchmarks -am -DskipTests

# Run all benchmarks (with Vector API for SIMD support)
java --add-modules jdk.incubator.vector \
  -jar performance-testing/micro-benchmarks/target/benchmarks.jar \
  -p dataDir=performance-testing/test-data-setup/target/tlc-trip-record-data

# Run a specific benchmark
java --add-modules jdk.incubator.vector \
  -jar performance-testing/micro-benchmarks/target/benchmarks.jar \
  "PageHandlingBenchmark.decodePages" \
  -p dataDir=performance-testing/test-data-setup/target/tlc-trip-record-data

# Run SIMD benchmark comparing scalar vs vectorized operations
java --add-modules jdk.incubator.vector \
  -jar performance-testing/micro-benchmarks/target/benchmarks.jar SimdBenchmark \
  -p size=1024,8192,65536 -p implementation=scalar,auto

# List available benchmarks
java --add-modules jdk.incubator.vector \
  -jar performance-testing/micro-benchmarks/target/benchmarks.jar -l
```

**Available benchmarks:**

| Benchmark | Description |
|-----------|-------------|
| `MemoryMapBenchmark.memoryMapToByteArray` | Memory map a file and copy to byte array |
| `PageHandlingBenchmark.a_decompressPages` | Scan and decompress all pages |
| `PageHandlingBenchmark.b_decodePages` | Scan, decompress, and decode all pages |
| `PipelineBenchmark.a_assembleColumns` | Synchronous page decoding + column assembly |
| `PipelineBenchmark.b_consumeRows` | Full pipeline with row-oriented access |
| `SimdBenchmark.*` | SIMD operations (countNonNulls, markNulls, dictionary, bit unpacking) |

**JMH options:**

| Option | Description |
|--------|-------------|
| `-wi <n>` | Number of warmup iterations (default: 3) |
| `-i <n>` | Number of measurement iterations (default: 5) |
| `-f <n>` | Number of forks (default: 2) |
| `-p param=value` | Set benchmark parameter |
| `-l` | List available benchmarks |
| `-h` | Show help |

**Note:** The taxi data files use GZIP compression (2016-01 to 2023-01) and ZSTD compression (2023-02 onwards). The default benchmark file is `yellow_tripdata_2025-05.parquet` (ZSTD, 75MB).

## License

This code base is available under the Apache License, version 2.
