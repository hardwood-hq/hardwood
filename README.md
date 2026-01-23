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

### Adding the Core Dependency

**Maven:**

```xml
<dependency>
    <groupId>dev.hardwoodhq</groupId>
    <artifactId>hardwood-core</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

**Gradle:**

```groovy
implementation 'dev.hardwoodhq:hardwood-core:1.0.0-SNAPSHOT'
```

### Compression Libraries

Hardwood supports reading Parquet files compressed with GZIP (built into Java), Snappy, ZSTD, LZ4, and Brotli. The compression libraries are optional dependencies—add only the ones you need:

**Maven:**

```xml
<!-- Snappy compression -->
<dependency>
    <groupId>org.xerial.snappy</groupId>
    <artifactId>snappy-java</artifactId>
    <version>1.1.10.8</version>
</dependency>

<!-- ZSTD compression -->
<dependency>
    <groupId>com.github.luben</groupId>
    <artifactId>zstd-jni</artifactId>
    <version>1.5.7-6</version>
</dependency>

<!-- LZ4 compression -->
<dependency>
    <groupId>org.lz4</groupId>
    <artifactId>lz4-java</artifactId>
    <version>1.8.1</version>
</dependency>

<!-- Brotli compression -->
<dependency>
    <groupId>com.aayushatharva.brotli4j</groupId>
    <artifactId>brotli4j</artifactId>
    <version>1.20.0</version>
</dependency>
```

**Gradle:**

```groovy
// Snappy compression
implementation 'org.xerial.snappy:snappy-java:1.1.10.8'

// ZSTD compression
implementation 'com.github.luben:zstd-jni:1.5.7-6'

// LZ4 compression
implementation 'org.lz4:lz4-java:1.8.1'

// Brotli compression
implementation 'com.aayushatharva.brotli4j:brotli4j:1.20.0'
```

If you attempt to read a file using a compression codec whose library is not on the classpath, Hardwood will throw an exception with a message indicating which dependency to add.

---

## Usage

### Row-Oriented Reading (PqRow API)

The `RowReader` provides a convenient row-oriented interface for reading Parquet files. The `PqRow` API provides typed accessor methods for type-safe field access.

```java
import dev.morling.hardwood.reader.ParquetFileReader;
import dev.morling.hardwood.reader.RowReader;
import dev.morling.hardwood.row.PqRow;
import dev.morling.hardwood.row.PqList;
import dev.morling.hardwood.row.PqIntList;
import dev.morling.hardwood.row.PqMap;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.math.BigDecimal;
import java.util.UUID;

try (ParquetFileReader fileReader = ParquetFileReader.open(path);
    RowReader rowReader = fileReader.createRowReader()) {

    for (PqRow row : rowReader) {
        // Access columns by name with typed accessors
        long id = row.getLong("id");
        String name = row.getString("name");

        // Logical types are automatically converted
        LocalDate birthDate = row.getDate("birth_date");
        Instant createdAt = row.getTimestamp("created_at");
        LocalTime wakeTime = row.getTime("wake_time");
        BigDecimal balance = row.getDecimal("balance");
        UUID accountId = row.getUuid("account_id");

        // Check for null values
        if (!row.isNull("age")) {
            int age = row.getInt("age");
            System.out.println("ID: " + id + ", Name: " + name + ", Age: " + age);
        }

        // Access nested structs
        PqRow address = row.getRow("address");
        if (address != null) {
            String city = address.getString("city");
            int zip = address.getInt("zip");
        }

        // Access lists and iterate with typed accessors
        PqList tags = row.getList("tags");
        if (tags != null) {
            for (String tag : tags.strings()) {
                System.out.println("Tag: " + tag);
            }
        }

        // Access list of structs
        PqList contacts = row.getList("contacts");
        if (contacts != null) {
            for (PqRow contact : contacts.rows()) {
                String contactName = contact.getString("name");
                String phone = contact.getString("phone");
            }
        }

        // Access nested lists (list<list<int>>) using primitive int lists
        PqList matrix = row.getList("matrix");
        if (matrix != null) {
            for (PqIntList innerList : matrix.intLists()) {
                for (var it = innerList.iterator(); it.hasNext(); ) {
                    int val = it.nextInt();
                    System.out.println("Value: " + val);
                }
            }
        }

        // Access maps (map<string, int>)
        PqMap attributes = row.getMap("attributes");
        if (attributes != null) {
            for (PqMap.Entry entry : attributes.getEntries()) {
                String key = entry.getStringKey();
                int value = entry.getIntValue();
                System.out.println(key + " = " + value);
            }
        }

        // Access maps with struct values (map<string, struct>)
        PqMap people = row.getMap("people");
        if (people != null) {
            for (PqMap.Entry entry : people.getEntries()) {
                String personId = entry.getStringKey();
                PqRow person = entry.getRowValue();
                String personName = person.getString("name");
                int personAge = person.getInt("age");
            }
        }
    }
}
```

**Typed accessor methods:**

| Method | Physical/Logical Type | Java Type |
|--------|----------------------|-----------|
| `getBoolean(name)` | BOOLEAN | `boolean` |
| `getInt(name)` | INT32 | `int` |
| `getLong(name)` | INT64 | `long` |
| `getFloat(name)` | FLOAT | `float` |
| `getDouble(name)` | DOUBLE | `double` |
| `getBinary(name)` | BYTE_ARRAY | `byte[]` |
| `getString(name)` | STRING logical type | `String` |
| `getDate(name)` | DATE logical type | `LocalDate` |
| `getTime(name)` | TIME logical type | `LocalTime` |
| `getTimestamp(name)` | TIMESTAMP logical type | `Instant` |
| `getDecimal(name)` | DECIMAL logical type | `BigDecimal` |
| `getUuid(name)` | UUID logical type | `UUID` |
| `getRow(name)` | Nested struct | `PqRow` |
| `getList(name)` | LIST logical type | `PqList` |
| `getMap(name)` | MAP logical type | `PqMap` |

**Type validation:** The API validates at runtime that the requested type matches the schema. Mismatches throw `IllegalArgumentException` with a descriptive message.

### Column-Oriented Reading (ColumnReader)

The `ColumnReader` provides lower-level columnar access, useful when you need to process specific columns independently or when working with the columnar nature of Parquet directly.

```java
import dev.morling.hardwood.reader.ParquetFileReader;
import dev.morling.hardwood.reader.ColumnReader;
import dev.morling.hardwood.schema.FileSchema;
import dev.morling.hardwood.schema.ColumnSchema;
import dev.morling.hardwood.metadata.RowGroup;
import dev.morling.hardwood.metadata.ColumnChunk;

try (ParquetFileReader reader = ParquetFileReader.open(path)) {
    // Get schema information
    FileSchema schema = reader.getFileSchema();
    System.out.println("Columns: " + schema.getColumnCount());

    // Access first row group
    RowGroup rowGroup = reader.getFileMetaData().rowGroups().get(0);

    // Read a specific column
    ColumnSchema idColumn = schema.getColumn("id");
    ColumnChunk idColumnChunk = rowGroup.columns().get(idColumn.columnIndex());

    ColumnReader idReader = reader.getColumnReader(idColumn, idColumnChunk);
    List<Object> idValues = idReader.readAll();

    // Process column values
    for (Object value : idValues) {
        if (value != null) {
            Long id = (Long) value;
            System.out.println("ID: " + id);
        } else {
            System.out.println("ID: null");
        }
    }
}
```

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
- [ ] Projection pushdown
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

This project requires Java 21 or newer for building.
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

The performance test module is not included in the default build. To run performance tests against NYC Yellow Taxi Trip data:

```shell
./mvnw test -Pperformance-test
```

This will download test data on the first run (up to ~4GB for the full 2020-2025 dataset).

**Configuration options:**

| Property | Default | Description |
|----------|---------|-------------|
| `perf.contenders` | `hardwood` | Which implementations to benchmark: `hardwood`, `parquet-java`, or `all` |
| `perf.start` | `2020-01` | Start year-month for data range |
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

## License

This code base is available under the Apache License, version 2.
