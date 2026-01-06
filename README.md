# Hardwood

A minimal dependency parser for the Apache Parquet file format.

## Project Vision

Now:

* Be light-weight: Implement the Parquet file format avoiding any 3rd party dependencies other than for compression algorithms (e.g. Snappy)
* Be correct: Support all Parquet files which are supported by the canonical [parquet-java](https://github.com/apache/parquet-java) library

In the future:

* Be fast: As fast or faster as parquet-java
* Be complete: Add a Parquet file writer

## Usage

### Row-Oriented Reading (RowReader)

The `RowReader` provides a convenient row-oriented interface for reading Parquet files. It handles parallel batch fetching from all columns and automatically processes all row groups.

```java
import dev.morling.hardwood.reader.ParquetFileReader;
import dev.morling.hardwood.row.Row;
import dev.morling.hardwood.reader.RowReader;
import java.time.Instant;
import java.time.LocalDate;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

try (ParquetFileReader fileReader = ParquetFileReader.open(path)) {
    try (RowReader rowReader = fileReader.createRowReader()) {
        for (Row row : rowReader) {
            // Access columns by name with type-safe accessors
            long id = row.getLong("id");
            String name = row.getString("name");

            // Logical types are automatically converted
            LocalDate birthDate = row.getDate("birth_date");        // DATE → LocalDate
            Instant createdAt = row.getTimestamp("created_at");    // TIMESTAMP → Instant
            BigDecimal balance = row.getDecimal("balance");        // DECIMAL → BigDecimal

            // Check for null values
            if (!row.isNull("age")) {
                int age = row.getInt("age");
                System.out.println("ID: " + id + ", Name: " + name + ", Age: " + age);
            } else {
                System.out.println("ID: " + id + ", Name: " + name + ", Age: null");
            }

            // Can also access by position or use generic getObject()
            long idByIndex = row.getLong(0);
            Object genericValue = row.getObject("created_at");  // Returns Instant for TIMESTAMP

            // Access nested structs
            Row address = row.getStruct("address");
            if (address != null) {
                String city = address.getString("city");
                int zip = address.getInt("zip");
            }

            // Access lists
            List<String> tags = row.getStringList("tags");
            List<Row> contacts = row.getStructList("contacts");  // List of structs

            // Access nested lists (list<list<T>>)
            List<?> matrix = row.getList("matrix");  // Returns List<List<Integer>> for list<list<int>>
        }
    }
}
```

**Supported physical types:**
- `getBoolean()` - BOOLEAN
- `getInt()` - INT32
- `getLong()` - INT64
- `getFloat()` - FLOAT
- `getDouble()` - DOUBLE
- `getByteArray()` - BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY
- `getString()` - BYTE_ARRAY with STRING logical type

**Supported logical types (automatic conversion):**
- `getString()` - STRING logical type → String
- `getDate()` - DATE logical type → LocalDate
- `getTimestamp()` - TIMESTAMP logical type → Instant
- `getTime()` - TIME logical type → LocalTime
- `getDecimal()` - DECIMAL logical type → BigDecimal
- `getUuid()` - UUID logical type → UUID
- `getInt()` / `getLong()` - INT_8/16/32/64, UINT_8/16/32/64 logical types → int/long
- `getObject()` - Generic accessor with automatic logical type conversion

**Nested types:**
- `getStruct()` - Nested struct → Row (recursive access)
- `getIntList()`, `getLongList()`, `getStringList()` - List of primitives
- `getStructList()` - List of structs → List<Row>
- `getList()` - Generic list access for nested lists (list<list<T>>, etc.)

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

---

## Status

This is Alpha quality software, resulting from a handful of coding sessions with Claude Code.
Note that while this project welcomes the usage of LLMs,
vibe coding (i.e. blindly accepting AI-generated changes without understanding them) is not accepted.
While there's currently a focus on quick iteration (closing feature gaps),
the aspiration is to build a high quality code base which is maintainable, extensible, performant, and safe.

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
  - [ ] MAP (not implemented)
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
- [ ] DELTA_BINARY_PACKED
  - [ ] Block/miniblock structure
  - [ ] Min delta calculation per block
  - [ ] Bit width calculation per miniblock
  - [ ] Encoder implementation
  - [ ] Decoder implementation
- [ ] DELTA_LENGTH_BYTE_ARRAY
  - [ ] Length encoding with DELTA_BINARY_PACKED
  - [ ] Raw byte concatenation
  - [ ] Encoder implementation
  - [ ] Decoder implementation
- [ ] DELTA_BYTE_ARRAY
  - [ ] Prefix length calculation
  - [ ] Suffix extraction
  - [ ] Encoder implementation
  - [ ] Decoder implementation

#### 2.5 Byte Stream Split (BYTE_STREAM_SPLIT)
- [ ] Float byte separation/interleaving
- [ ] Double byte separation/interleaving
- [ ] Encoder implementation
- [ ] Decoder implementation

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
- [ ] Not implemented (future)
  - [ ] INTERVAL
  - [ ] MAP

---

### Phase 8: Compression Integration

#### 8.1 Compression Interface
- [x] Define `CompressionCodec` interface (compress, decompress, getName)
- [x] Implement codec registry

#### 8.2 Codec Implementations
- [x] UNCOMPRESSED (passthrough)
- [x] GZIP (java.util.zip, no external dependency)
- [x] SNAPPY (snappy-java)
- [ ] LZ4 (lz4-java)
- [x] ZSTD (zstd-jni)
- [ ] LZO (lzo-java, optional)
- [ ] BROTLI (brotli4j, optional)

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
- [ ] LZ4 integration
- [x] **Validate**: Read files with various codecs from parquet-testing

### Milestone 5: Advanced Encodings
- [ ] DELTA_BINARY_PACKED
- [ ] DELTA_LENGTH_BYTE_ARRAY
- [ ] DELTA_BYTE_ARRAY
- [ ] BYTE_STREAM_SPLIT
- [ ] **Validate**: Read files using these encodings

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

**Current Pass Rate: 190/215 (88.4%) parquet-testing, 25/25 unit tests**

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

Remaining Failures by Category (25 total):
- ZSTD compression: 3 files (edge cases with unusual frame descriptors)
- LZ4 compression: 3 files
- LZ4_RAW compression: 2 files
- Special GZIP formats: 2 files (concatenated members, non-standard variant)
- Delta encoding issues: 4 files (DELTA_BINARY_PACKED, DELTA_BYTE_ARRAY)
- BYTE_STREAM_SPLIT encoding: 2 files
- Snappy decompression failures: 2 files
- Other edge cases: 7 files (EOF errors, malformed data, unknown types, etc.)

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

This project requires Java 17 or newer for building.
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

## License

This code base is available under the Apache License, version 2.
