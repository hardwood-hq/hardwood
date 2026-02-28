# Hardwood

_A parser for the Apache Parquet file format, optimized for minimal dependencies and great performance._

Goals of the project are:

* Be light-weight: Implement the Parquet file format avoiding any 3rd party dependencies other than for compression algorithms (e.g. Snappy)
* Be correct: Support all Parquet files which are supported by the canonical [parquet-java](https://github.com/apache/parquet-java) library
* Be fast: As fast or faster as parquet-java
* Be complete: Add a Parquet file writer (after 1.0)

Latest version: 1.0.0.Alpha1, 2026-02-26

## Set-Up

Hardwood runs on Java 21 or newer; Java 25 is recommended for best performance.

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
            <version>1.0.0.Alpha1</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
```

**Gradle:**

```groovy
dependencies {
    implementation platform('dev.hardwood:hardwood-bom:1.0.0.Alpha1')
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
    <version>1.0.0.Alpha1</version>
</dependency>
```

**Gradle:**

```groovy
implementation 'dev.hardwood:hardwood-core:1.0.0.Alpha1'
```

### Logging

Hardwood uses the Java Platform Logging API (`System.Logger`).
Bindings are available for all popular logger implementations, for instance for [log4j 2](https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-jpl).

### Compression Libraries

Hardwood supports reading Parquet files compressed with GZIP (built into Java), Snappy, ZSTD, LZ4, and Brotli. The compression libraries are optional dependencies—add only the ones you need:

| Codec | Group ID | Artifact ID |
|-------|----------|-------------|
| Snappy | `org.xerial.snappy` | `snappy-java` |
| ZSTD | `com.github.luben` | `zstd-jni` |
| LZ4 | `at.yawk.lz4` | `lz4-java` |
| Brotli | `com.aayushatharva.brotli4j` | `brotli4j` |

When using the BOM, declare without a version — for example, to add Snappy:

**Maven:**

```xml
<dependency>
    <groupId>org.xerial.snappy</groupId>
    <artifactId>snappy-java</artifactId>
</dependency>
```

**Gradle:**

```groovy
implementation 'org.xerial.snappy:snappy-java'
```

<details>
<summary>Without the BOM (explicit versions)</summary>

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
    <groupId>at.yawk.lz4</groupId>
    <artifactId>lz4-java</artifactId>
    <version>1.8.1</version>
</dependency>
<dependency>
    <groupId>com.aayushatharva.brotli4j</groupId>
    <artifactId>brotli4j</artifactId>
    <version>1.20.0</version>
</dependency>
```

```groovy
implementation 'org.xerial.snappy:snappy-java:1.1.10.8'
implementation 'com.github.luben:zstd-jni:1.5.7-6'
implementation 'at.yawk.lz4:lz4-java:1.8.1'
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
    }
}
```

<details>
<summary>Advanced: nested lists, maps, and list-of-structs</summary>

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

</details>

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

### Parquet-Java Compatibility (hardwood-parquet-java-compat)

The `hardwood-parquet-java-compat` module provides a drop-in replacement for parquet-java's `ParquetReader<Group>` API. This allows users migrating from parquet-java to use Hardwood with minimal code changes.

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

## Package Structure

The library is organized into public API packages and internal implementation packages:

| Package | Visibility | Purpose |
|---------|-----------|---------|
| `dev.hardwood` | **Public API** | Entry point for creating readers and managing shared resources (thread pool, decompressor pool). |
| `dev.hardwood.reader` | **Public API** | Single-file and multi-file readers for row-oriented and column-oriented access. |
| `dev.hardwood.metadata` | **Public API** | Parquet file metadata: row groups, column chunks, physical/logical types, and compression codecs. |
| `dev.hardwood.schema` | **Public API** | Schema representation: file schema, column schemas, and column projection. |
| `dev.hardwood.row` | **Public API** | Value types for nested data access: structs, lists, and maps. |
| `dev.hardwood.internal.*` | **Internal** | Implementation details — not part of the public API and may change without notice. |

---

## Status

This is Alpha quality software, under active development.

Currently, individual Parquet files must be **at most 2 GB**.
Larger datasets should be split across multiple files and read via `MultiFileParquetReader`.

Note that while this project welcomes the usage of LLMs,
vibe coding (i.e. blindly accepting AI-generated changes without understanding them) is not accepted.
While there's currently a focus on quick iteration (closing feature gaps),
the aspiration is to build a high quality code base which is maintainable, extensible, performant, and safe.

See [ROADMAP.md](ROADMAP.md) for the detailed implementation status, roadmap, and milestones.

## Performance Testing

### Flat Files

These are the results from parsing files of the NYC Yellow Taxi Trip data set (subset 2016-01 to 2025-11, ~9.2GB overall, ~650M rows),
running on a Macbook Pro M3 Max.
The test (`FlatPerformanceTest`) parses all files and adds up the values of three columns (out of 20).
The results shown are for:

* The row reader API, using indexed access (mapping field names to indexes once upfront)
* The columnar reader API, using indexed access

```
====================================================================================================
PERFORMANCE TEST RESULTS
====================================================================================================

Environment:
  CPU cores:       16
  Java version:    25
  OS:              Mac OS X aarch64

Data:
  Files processed: 119
  Total rows:      651,209,003
  Total size:      9,241.1 MB
  Runs per contender: 5

Correctness Verification:
                              passenger_count     trip_distance       fare_amount
  Hardwood (multifile indexed)       972,078,547  2,701,223,013.48  9,166,943,759.83
  Hardwood (column reader multifile)       972,078,547  2,701,223,013.48  9,166,943,759.83

Performance (all runs):
  Contender                          Time (s)     Records/sec   Records/sec/core       MB/sec
  -----------------------------------------------------------------------------------------------
  Hardwood (multifile indexed) [1]         2.75     236,975,620         14,810,976       3362.8
  Hardwood (multifile indexed) [2]         2.78     234,669,911         14,666,869       3330.1
  Hardwood (multifile indexed) [3]         2.70     240,831,732         15,051,983       3417.6
  Hardwood (multifile indexed) [4]         2.70     240,831,732         15,051,983       3417.6
  Hardwood (multifile indexed) [5]         2.68     242,897,800         15,181,113       3446.9
  Hardwood (multifile indexed) [AVG]         2.72     239,239,163         14,952,448       3395.0
                                   min: 2.68s, max: 2.78s, spread: 0.09s

  Hardwood (column reader multifile) [1]         1.30     502,476,083         31,404,755       7130.5
  Hardwood (column reader multifile) [2]         1.11     584,568,225         36,535,514       8295.4
  Hardwood (column reader multifile) [3]         1.06     614,348,116         38,396,757       8718.0
  Hardwood (column reader multifile) [4]         1.06     616,091,772         38,505,736       8742.8
  Hardwood (column reader multifile) [5]         1.08     603,530,123         37,720,633       8564.5
  Hardwood (column reader multifile) [AVG]         1.12     580,917,933         36,307,371       8243.6
                                   min: 1.06s, max: 1.30s, spread: 0.24s

====================================================================================================
```

### Nested Files

These are the results from parsing a file with points of interest from the Overture Maps data set
(~900 MB, ~9M rows), running on a Macbook Pro M3 Max.
The test (`NestedPerformanceTest`) parses all columns of the file and determines min/max values, max array lengths, etc.
As above, the results shown are for the row reader API and the columnar API with indexed access.

```
====================================================================================================
NESTED SCHEMA PERFORMANCE TEST RESULTS
====================================================================================================

Environment:
  CPU cores:       16
  Java version:    25
  OS:              Mac OS X aarch64

Data:
  Total rows:      9,152,540
  File size:       882.2 MB
  Runs per contender: 5

Correctness Verification:
                               min_ver    max_ver       rows     websites      sources  addresses
  Hardwood (indexed)                 1          9  9,152,540    3,687,576   18,305,080  9,152,540
  Hardwood (columnar)                1          9  9,152,540    3,687,576   18,305,080  9,152,540

Performance (all runs):
  Contender                          Time (s)     Records/sec   Records/sec/core       MB/sec
  -----------------------------------------------------------------------------------------------
  Hardwood (indexed) [1]                 2.22       4,120,910            257,557        397.2
  Hardwood (indexed) [2]                 1.92       4,759,511            297,469        458.8
  Hardwood (indexed) [3]                 1.89       4,855,459            303,466        468.0
  Hardwood (indexed) [4]                 1.88       4,876,153            304,760        470.0
  Hardwood (indexed) [5]                 1.88       4,858,036            303,627        468.3
  Hardwood (indexed) [AVG]               1.96       4,674,433            292,152        450.6
                                   min: 1.88s, max: 2.22s, spread: 0.34s

  Hardwood (columnar) [1]                1.34       6,830,254            426,891        658.4
  Hardwood (columnar) [2]                1.32       6,918,020            432,376        666.8
  Hardwood (columnar) [3]                1.24       7,363,266            460,204        709.8
  Hardwood (columnar) [4]                1.24       7,404,968            462,810        713.8
  Hardwood (columnar) [5]                1.22       7,477,565            467,348        720.8
  Hardwood (columnar) [AVG]              1.27       7,189,741            449,359        693.0
                                   min: 1.22s, max: 1.34s, spread: 0.12s

====================================================================================================
```

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

### Creating a Release

See [RELEASING.md](RELEASING.md).

### Running Performance Tests

The performance testing modules are not included in the default build. Enable them with `-Pperformance-test`.

#### End-to-End Performance Tests

There are two end-to-end performance tests: one for flat schemas (NYC Yellow Taxi Trip data) and one for nested schemas (Overture Maps POI data). Test data is downloaded automatically on the first run.

```shell
./mvnw test -Pperformance-test
```

**Flat schema test** (`FlatPerformanceTest`) — reads ~9GB of taxi trip data (2016-2025, ~650M rows) and sums three columns.

| Property | Default | Description |
|----------|---------|-------------|
| `perf.contenders` | `HARDWOOD_MULTIFILE_INDEXED` | Comma-separated list of contenders, or `all` |
| `perf.start` | `2016-01` | Start year-month for data range |
| `perf.end` | `2025-11` | End year-month for data range |
| `perf.runs` | `10` | Number of timed runs per contender |

Available contenders: `HARDWOOD_INDEXED`, `HARDWOOD_NAMED`, `HARDWOOD_PROJECTION`, `HARDWOOD_MULTIFILE_INDEXED`, `HARDWOOD_MULTIFILE_NAMED`, `HARDWOOD_COLUMN_READER`, `HARDWOOD_COLUMN_READER_MULTIFILE`, `PARQUET_JAVA_INDEXED`, `PARQUET_JAVA_NAMED`.

**Nested schema test** (`NestedPerformanceTest`) — reads ~900MB of Overture Maps POI data (~9M rows) with deeply nested columns.

| Property | Default | Description |
|----------|---------|-------------|
| `perf.contenders` | `HARDWOOD_NAMED` | Comma-separated list of contenders, or `all` |
| `perf.runs` | `5` | Number of timed runs per contender |

Available contenders: `HARDWOOD_INDEXED`, `HARDWOOD_NAMED`, `HARDWOOD_COLUMNAR`, `PARQUET_JAVA`.

**Examples:**

```shell
# Run all contenders for the flat test, limited to 2025 data
./mvnw test -Pperformance-test -Dtest=FlatPerformanceTest -Dperf.contenders=all -Dperf.start=2025-01

# Compare multifile indexed vs named access
./mvnw test -Pperformance-test -Dperf.contenders=HARDWOOD_MULTIFILE_INDEXED,HARDWOOD_MULTIFILE_NAMED

# Run nested test only
./mvnw test -Pperformance-test -Dtest=NestedPerformanceTest -Dperf.contenders=all
```

#### PyArrow Comparison Tests

Python counterparts of the Java performance tests using PyArrow, for cross-platform comparison.
These scripts require a Python environment with PyArrow installed (use the `.venv` venv).

**Flat schema** (`flat_performance_test.py`) — counterpart of `FlatPerformanceTest.java`:

```shell
cd performance-testing/end-to-end

# Run all contenders (single-threaded and multi-threaded), 5 runs each
python flat_performance_test.py

# Single-threaded only
python flat_performance_test.py -c single_threaded

# Multi-threaded, 10 runs
python flat_performance_test.py -c multi_threaded -r 10
```

**Nested schema** (`nested_performance_test.py`) — counterpart of `NestedPerformanceTest.java`:

```shell
cd performance-testing/end-to-end

# Run all contenders, 5 runs each
python nested_performance_test.py

# Single-threaded only, 3 runs
python nested_performance_test.py -c single_threaded -r 3
```

**Options:**

| Flag | Default | Description |
|------|---------|-------------|
| `-c`, `--contenders` | `all` | Contenders to run: `single_threaded`, `multi_threaded`, or `all` |
| `-r`, `--runs` | `5` | Number of timed runs per contender |

**Notes on comparability:**

- The flat test uses column projection (reads only the 3 summed columns), matching the Hardwood projection and column-reader contenders. The parquet-java contenders in `FlatPerformanceTest.java` read all columns without projection, so direct comparison against parquet-java is not apples-to-apples.
- PyArrow uses vectorized columnar operations (C++ engine) rather than row-by-row iteration.
- The `single_threaded` contender (`use_threads=False`) is most comparable to single-threaded parquet-java; `multi_threaded` is comparable to Hardwood's parallel reading.

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

---

## License

This code base is available under the Apache License, version 2.

---

## Resources

- [Parquet Format Specification](https://github.com/apache/parquet-format)
- [Thrift Compact Protocol Spec](https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md)
- [Dremel Paper](https://research.google/pubs/pub36632/)
- [parquet-java Reference](https://github.com/apache/parquet-java)
- [parquet-testing Files](https://github.com/apache/parquet-testing)
- [arrow-testing Files](https://github.com/apache/arrow-testing)
