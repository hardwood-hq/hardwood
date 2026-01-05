# Parquet Implementation TODO

A from-scratch implementation of Apache Parquet reader/writer in Java with no dependencies except compression libraries.

---

## Phase 1: Foundation & Format Understanding

### 1.1 Core Data Structures
- [x] Define physical types enum: `BOOLEAN`, `INT32`, `INT64`, `INT96`, `FLOAT`, `DOUBLE`, `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY`
- [x] Define logical types: `STRING`, `ENUM`, `UUID`, `DATE`, `TIME`, `TIMESTAMP`, `DECIMAL`, `LIST`, `MAP`, etc.
- [x] Define repetition types: `REQUIRED`, `OPTIONAL`, `REPEATED`

### 1.2 Schema Representation
- [x] Implement `SchemaElement` class (name, type, repetition, logicalType, children, fieldId, typeLength)
- [x] Implement `MessageType` as root schema container
- [x] Schema traversal utilities (getColumn, getMaxDefinitionLevel, getMaxRepetitionLevel)

### 1.3 Thrift Compact Protocol (Manual Implementation)
- [x] Implement `ThriftCompactReader`
  - [x] Varint decoding
  - [x] Zigzag decoding
  - [x] Field header parsing
  - [x] Struct reading
  - [x] List/Map container reading
  - [x] String/Binary reading
- [ ] Implement `ThriftCompactWriter`
  - [ ] Varint encoding
  - [ ] Zigzag encoding
  - [ ] Field header writing
  - [ ] Struct writing
  - [ ] List/Map container writing
  - [ ] String/Binary writing

---

## Phase 2: Encoding Implementations

### 2.1 Plain Encoding (PLAIN)
- [x] Little-endian integer encoding/decoding (INT32, INT64)
- [x] Little-endian float encoding/decoding (FLOAT, DOUBLE)
- [x] INT96 encoding/decoding
- [x] Length-prefixed byte array encoding/decoding
- [x] Fixed-length byte array encoding/decoding
- [x] Bit-packed boolean encoding/decoding

### 2.2 Dictionary Encoding (RLE_DICTIONARY)
- [ ] Implement `DictionaryEncoder<T>` (valueToIndex map, indexToValue list)
- [x] Implement `DictionaryDecoder<T>`
- [ ] Dictionary page serialization
- [x] Dictionary page deserialization
- [ ] Fallback to plain encoding when dictionary grows too large

### 2.3 RLE/Bit-Packing Hybrid
- [ ] Implement `RleBitPackingHybridEncoder`
  - [ ] Bit width calculation
  - [ ] RLE encoding (repeated values)
  - [ ] Bit-packing encoding (groups of 8)
  - [ ] Automatic mode switching
- [x] Implement `RleBitPackingHybridDecoder`
  - [x] Header byte parsing (RLE vs bit-packed)
  - [x] RLE decoding
  - [x] Bit-packing decoding

### 2.4 Delta Encodings
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

### 2.5 Byte Stream Split (BYTE_STREAM_SPLIT)
- [ ] Float byte separation/interleaving
- [ ] Double byte separation/interleaving
- [ ] Encoder implementation
- [ ] Decoder implementation

---

## Phase 3: Page Structure

### 3.1 Page Types
- [x] Implement `DataPageV1` structure
- [ ] Implement `DataPageV2` structure
- [x] Implement `DictionaryPage` structure

### 3.2 Page Header (Thrift)
- [x] Define `PageHeader` Thrift structure
- [x] Define `DataPageHeader` Thrift structure
- [ ] Define `DataPageHeaderV2` Thrift structure
- [x] Define `DictionaryPageHeader` Thrift structure
- [ ] Page header serialization
- [x] Page header deserialization
- [ ] CRC32 calculation and validation

### 3.3 Definition & Repetition Levels
- [ ] Implement `LevelEncoder` using RLE/bit-packing hybrid
- [x] Implement `LevelDecoder`
- [x] Max level calculation from schema
- [x] Null detection from definition levels

---

## Phase 4: Column Chunk & Row Group

### 4.1 Column Chunk Structure
- [x] Implement `ColumnChunk` class
- [x] Implement `ColumnMetaData` Thrift structure
  - [x] Type, encodings, path in schema
  - [x] Codec, num values, sizes
  - [x] Page offsets (data, index, dictionary)
  - [x] Statistics
- [ ] Column chunk serialization
- [x] Column chunk deserialization

### 4.2 Row Group
- [x] Implement `RowGroup` class
- [ ] Row group metadata serialization
- [x] Row group metadata deserialization
- [ ] Sorting column tracking (optional)

---

## Phase 5: File Structure

### 5.1 File Layout
- [x] Magic number validation ("PAR1")
- [x] Footer location calculation (last 8 bytes)
- [x] Row group offset tracking

### 5.2 FileMetaData (Thrift)
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

## Phase 6: Writer Implementation

### 6.1 Writer Architecture
- [ ] Implement `ParquetWriter<T>` main class
- [ ] Implement `WriterConfig` (row group size, page size, dictionary size, codec, version)
- [ ] Implement `RowGroupWriter`
- [ ] Implement `ColumnWriter`
- [ ] Implement `PageWriter`

### 6.2 Write Flow
- [ ] Record buffering
- [ ] Row group size tracking
- [ ] Automatic row group flushing
- [ ] Dictionary page writing
- [ ] Data page encoding and writing
- [ ] Page compression
- [ ] Footer writing
- [ ] File finalization

### 6.3 Record Shredding (Dremel Algorithm)
- [ ] Implement schema traversal for shredding
- [ ] Definition level calculation
- [ ] Repetition level calculation
- [ ] Primitive value emission
- [ ] Nested structure handling
- [ ] Repeated field handling
- [ ] Optional field handling

---

## Phase 7: Reader Implementation

### 7.1 Reader Architecture
- [ ] Implement `ParquetReader<T>` main class
- [x] Implement `ParquetFileReader` (low-level)
- [ ] Implement `RowGroupReader`
- [x] Implement `ColumnReader`
- [x] Implement `PageReader`

### 7.2 Read Flow
- [x] Footer reading and parsing
- [x] Schema reconstruction from schema elements
- [x] Row group iteration
- [x] Column chunk seeking
- [x] Dictionary page reading
- [x] Data page reading and decoding
- [x] Page decompression

### 7.3 Record Assembly (Inverse Dremel)
- [ ] Column reader synchronization
- [x] Definition level interpretation
- [ ] Repetition level interpretation
- [x] Null value handling
- [ ] Nested structure reconstruction
- [ ] List assembly from repeated fields
- [ ] Record completion detection

---

## Phase 8: Compression Integration

### 8.1 Compression Interface
- [x] Define `CompressionCodec` interface (compress, decompress, getName)
- [x] Implement codec registry

### 8.2 Codec Implementations
- [x] UNCOMPRESSED (passthrough)
- [ ] GZIP (java.util.zip, no external dependency)
- [ ] SNAPPY (snappy-java)
- [ ] LZ4 (lz4-java)
- [ ] ZSTD (zstd-jni)
- [ ] LZO (lzo-java, optional)
- [ ] BROTLI (brotli4j, optional)

---

## Phase 9: Advanced Features

### 9.1 Statistics
- [ ] Implement `Statistics<T>` class (min, max, nullCount, distinctCount)
- [ ] Statistics collection during writing
- [ ] Binary min/max truncation for efficiency
- [ ] Statistics serialization/deserialization
- [ ] Type-specific comparators

### 9.2 Page Index (Column Index & Offset Index)
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

### 9.3 Bloom Filters
- [ ] Implement split block bloom filter
- [ ] XXHASH implementation (or integration)
- [ ] Bloom filter serialization
- [ ] Bloom filter deserialization
- [ ] Bloom filter checking during reads

### 9.4 Predicate Pushdown
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

## Phase 10: Public API Design

### 10.1 Schema Builder API
- [ ] Implement fluent `Types.buildMessage()` API
- [ ] Primitive type builders with logical type support
- [ ] Group builders for nested structures
- [ ] List and Map convenience builders

### 10.2 Writer API
- [ ] Implement `ParquetWriter.builder(path)` fluent API
- [ ] Configuration methods (schema, codec, sizes, etc.)
- [ ] GenericRecord support
- [ ] Custom record materializer support

### 10.3 Reader API
- [ ] Implement `ParquetReader.builder(path)` fluent API
- [ ] Projection pushdown
- [ ] Filter predicate support
- [ ] GenericRecord support
- [ ] Custom record materializer support

### 10.4 Low-Level API
- [ ] Direct column chunk access
- [ ] Page-level iteration
- [ ] Raw value reading with levels

---

## Milestones

### Milestone 1: Minimal Viable Reader
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

### Milestone 3: Core Encodings
- [x] RLE/bit-packing hybrid
- [x] Dictionary encoding
- [x] Definition/repetition levels
- [ ] Nested schema support (Dremel algorithm)
- [ ] **Validate**: Read/write nested structures

### Milestone 4: Compression
- [ ] GZIP integration
- [ ] Snappy integration
- [ ] ZSTD integration
- [ ] LZ4 integration
- [ ] **Validate**: Read files with various codecs from parquet-testing

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
- [ ] Parallel reading support
- [ ] Parallel writing support
- [ ] **Validate**: Full compatibility with parquet-java and PyArrow

---

## Testing

### Test Data Sources
- [x] Clone parquet-testing repository
- [ ] Clone arrow-testing repository
- [x] Generate test files with PyArrow (various configs)
- [ ] Generate test files with DuckDB

### Parquet-Testing Repository Files

#### Bad Data Files (Intentionally Malformed)
- [ ] bad_data/ARROW-GH-41317.parquet
- [ ] bad_data/ARROW-GH-41321.parquet
- [ ] bad_data/ARROW-GH-43605.parquet
- [x] bad_data/ARROW-GH-45185.parquet
- [ ] bad_data/ARROW-RS-GH-6229-DICTHEADER.parquet
- [x] bad_data/ARROW-RS-GH-6229-LEVELS.parquet
- [ ] bad_data/PARQUET-1481.parquet

#### Data Files (Valid Test Data)
- [ ] data/alltypes_dictionary.parquet (Boolean EOF)
- [ ] data/alltypes_plain.parquet (Boolean EOF)
- [ ] data/alltypes_plain.snappy.parquet (Boolean EOF)
- [ ] data/alltypes_tiny_pages.parquet (Boolean EOF)
- [ ] data/alltypes_tiny_pages_plain.parquet (Boolean EOF)
- [x] data/binary.parquet
- [x] data/binary_truncated_min_max.parquet
- [x] data/byte_array_decimal.parquet
- [ ] data/byte_stream_split.zstd.parquet (ZSTD)
- [ ] data/byte_stream_split_extended.gzip.parquet (GZIP)
- [x] data/column_chunk_key_value_metadata.parquet
- [ ] data/concatenated_gzip_members.parquet (GZIP)
- [ ] data/data_index_bloom_encoding_stats.parquet (Boolean EOF)
- [x] data/data_index_bloom_encoding_with_length.parquet
- [x] data/datapage_v1-corrupt-checksum.parquet
- [x] data/datapage_v1-snappy-compressed-checksum.parquet
- [x] data/datapage_v1-uncompressed-checksum.parquet
- [ ] data/datapage_v2.snappy.parquet (DATA_PAGE_V2)
- [ ] data/datapage_v2_empty_datapage.snappy.parquet (DATA_PAGE_V2)
- [ ] data/delta_binary_packed.parquet (Boolean EOF in some columns)
- [ ] data/delta_byte_array.parquet (Boolean EOF in some columns)
- [ ] data/delta_encoding_optional_column.parquet (Boolean EOF in some columns)
- [ ] data/delta_encoding_required_column.parquet (Boolean EOF in some columns)
- [ ] data/delta_length_byte_array.parquet (Boolean EOF in some columns)
- [ ] data/dict-page-offset-zero.parquet (Boolean EOF)
- [ ] data/fixed_length_byte_array.parquet (FIXED_LEN_BYTE_ARRAY)
- [ ] data/fixed_length_decimal.parquet (FIXED_LEN_BYTE_ARRAY)
- [ ] data/fixed_length_decimal_legacy.parquet (FIXED_LEN_BYTE_ARRAY)
- [ ] data/float16_nonzeros_and_nans.parquet (Boolean EOF)
- [ ] data/float16_zeros_and_nans.parquet (Boolean EOF)
- [x] data/geospatial/crs-arbitrary-value.parquet
- [x] data/geospatial/crs-default.parquet
- [x] data/geospatial/crs-geography.parquet
- [x] data/geospatial/crs-projjson.parquet
- [x] data/geospatial/crs-srid.parquet
- [x] data/geospatial/geospatial-with-nan.parquet
- [x] data/geospatial/geospatial.parquet
- [ ] data/hadoop_lz4_compressed.parquet (LZ4)
- [ ] data/hadoop_lz4_compressed_larger.parquet (LZ4)
- [x] data/incorrect_map_schema.parquet
- [x] data/int32_decimal.parquet
- [x] data/int32_with_null_pages.parquet
- [x] data/int64_decimal.parquet
- [x] data/int96_from_spark.parquet
- [x] data/large_string_map.brotli.parquet
- [x] data/list_columns.parquet
- [ ] data/lz4_raw_compressed.parquet (LZ4_RAW)
- [ ] data/lz4_raw_compressed_larger.parquet (LZ4_RAW)
- [x] data/map_no_value.parquet
- [ ] data/nan_in_stats.parquet (NegativeArraySize)
- [x] data/nation.dict-malformed.parquet
- [x] data/nested_lists.snappy.parquet
- [x] data/nested_maps.snappy.parquet
- [ ] data/nested_structs.rust.parquet (Boolean EOF)
- [ ] data/non_hadoop_lz4_compressed.parquet (LZ4)
- [x] data/nonnullable.impala.parquet
- [x] data/null_list.parquet
- [x] data/nullable.impala.parquet
- [x] data/nulls.snappy.parquet
- [x] data/old_list_structure.parquet
- [x] data/overflow_i16_page_cnt.parquet
- [ ] data/page_v2_empty_compressed.parquet (DATA_PAGE_V2)
- [x] data/plain-dict-uncompressed-checksum.parquet
- [x] data/repeated_no_annotation.parquet
- [x] data/repeated_primitive_no_list.parquet
- [ ] data/rle-dict-snappy-checksum.parquet (Boolean EOF)
- [ ] data/rle-dict-uncompressed-corrupt-checksum.parquet (Boolean EOF)
- [ ] data/rle_boolean_encoding.parquet (Boolean EOF)
- [x] data/single_nan.parquet
- [x] data/sort_columns.parquet
- [x] data/unknown-logical-type.parquet

#### Shredded Variant Files (Nested Structure Tests)
- [ ] shredded_variant/case-001.parquet
- [ ] shredded_variant/case-002.parquet
- [ ] shredded_variant/case-004.parquet
- [ ] shredded_variant/case-005.parquet
- [ ] shredded_variant/case-006.parquet
- [ ] shredded_variant/case-007.parquet
- [ ] shredded_variant/case-008.parquet
- [ ] shredded_variant/case-009.parquet
- [ ] shredded_variant/case-010.parquet
- [ ] shredded_variant/case-011.parquet
- [ ] shredded_variant/case-012.parquet
- [ ] shredded_variant/case-013.parquet
- [ ] shredded_variant/case-014.parquet
- [ ] shredded_variant/case-015.parquet
- [ ] shredded_variant/case-016.parquet
- [ ] shredded_variant/case-017.parquet
- [ ] shredded_variant/case-018.parquet
- [ ] shredded_variant/case-019.parquet
- [ ] shredded_variant/case-020.parquet
- [ ] shredded_variant/case-021.parquet
- [ ] shredded_variant/case-022.parquet
- [ ] shredded_variant/case-023.parquet
- [ ] shredded_variant/case-024.parquet
- [ ] shredded_variant/case-025.parquet
- [ ] shredded_variant/case-026.parquet
- [ ] shredded_variant/case-027.parquet
- [ ] shredded_variant/case-028.parquet
- [ ] shredded_variant/case-029.parquet
- [ ] shredded_variant/case-030.parquet
- [ ] shredded_variant/case-031.parquet
- [ ] shredded_variant/case-032.parquet
- [ ] shredded_variant/case-033.parquet
- [ ] shredded_variant/case-034.parquet
- [ ] shredded_variant/case-035.parquet
- [ ] shredded_variant/case-036.parquet
- [ ] shredded_variant/case-037.parquet
- [ ] shredded_variant/case-038.parquet
- [ ] shredded_variant/case-039.parquet
- [ ] shredded_variant/case-040.parquet
- [ ] shredded_variant/case-041.parquet
- [ ] shredded_variant/case-042.parquet
- [ ] shredded_variant/case-043-INVALID.parquet
- [ ] shredded_variant/case-044.parquet
- [ ] shredded_variant/case-045.parquet
- [ ] shredded_variant/case-046.parquet
- [ ] shredded_variant/case-047.parquet
- [ ] shredded_variant/case-048.parquet
- [ ] shredded_variant/case-049.parquet
- [ ] shredded_variant/case-050.parquet
- [ ] shredded_variant/case-051.parquet
- [ ] shredded_variant/case-052.parquet
- [ ] shredded_variant/case-053.parquet
- [ ] shredded_variant/case-054.parquet
- [ ] shredded_variant/case-055.parquet
- [ ] shredded_variant/case-056.parquet
- [ ] shredded_variant/case-057.parquet
- [ ] shredded_variant/case-058.parquet
- [ ] shredded_variant/case-059.parquet
- [ ] shredded_variant/case-060.parquet
- [ ] shredded_variant/case-061.parquet
- [ ] shredded_variant/case-062.parquet
- [ ] shredded_variant/case-063.parquet
- [ ] shredded_variant/case-064.parquet
- [ ] shredded_variant/case-065.parquet
- [ ] shredded_variant/case-066.parquet
- [ ] shredded_variant/case-067.parquet
- [ ] shredded_variant/case-068.parquet
- [ ] shredded_variant/case-069.parquet
- [ ] shredded_variant/case-070.parquet
- [ ] shredded_variant/case-071.parquet
- [ ] shredded_variant/case-072.parquet
- [ ] shredded_variant/case-073.parquet
- [ ] shredded_variant/case-074.parquet
- [ ] shredded_variant/case-075.parquet
- [ ] shredded_variant/case-076.parquet
- [ ] shredded_variant/case-077.parquet
- [ ] shredded_variant/case-078.parquet
- [ ] shredded_variant/case-079.parquet
- [ ] shredded_variant/case-080.parquet
- [ ] shredded_variant/case-081.parquet
- [ ] shredded_variant/case-082.parquet
- [ ] shredded_variant/case-083.parquet
- [ ] shredded_variant/case-084-INVALID.parquet
- [ ] shredded_variant/case-085.parquet
- [ ] shredded_variant/case-086.parquet
- [ ] shredded_variant/case-087.parquet
- [ ] shredded_variant/case-088.parquet
- [ ] shredded_variant/case-089.parquet
- [ ] shredded_variant/case-090.parquet
- [ ] shredded_variant/case-091.parquet
- [ ] shredded_variant/case-092.parquet
- [ ] shredded_variant/case-093.parquet
- [ ] shredded_variant/case-094.parquet
- [ ] shredded_variant/case-095.parquet
- [ ] shredded_variant/case-096.parquet
- [ ] shredded_variant/case-097.parquet
- [ ] shredded_variant/case-098.parquet
- [ ] shredded_variant/case-099.parquet
- [ ] shredded_variant/case-100.parquet
- [ ] shredded_variant/case-101.parquet
- [ ] shredded_variant/case-102.parquet
- [ ] shredded_variant/case-103.parquet
- [ ] shredded_variant/case-104.parquet
- [ ] shredded_variant/case-105.parquet
- [ ] shredded_variant/case-106.parquet
- [ ] shredded_variant/case-107.parquet
- [ ] shredded_variant/case-108.parquet
- [ ] shredded_variant/case-109.parquet
- [ ] shredded_variant/case-110.parquet
- [ ] shredded_variant/case-111.parquet
- [ ] shredded_variant/case-112.parquet
- [ ] shredded_variant/case-113.parquet
- [ ] shredded_variant/case-114.parquet
- [ ] shredded_variant/case-115.parquet
- [ ] shredded_variant/case-116.parquet
- [ ] shredded_variant/case-117.parquet
- [ ] shredded_variant/case-118.parquet
- [ ] shredded_variant/case-119.parquet
- [ ] shredded_variant/case-120.parquet
- [ ] shredded_variant/case-121.parquet
- [ ] shredded_variant/case-122.parquet
- [ ] shredded_variant/case-123.parquet
- [ ] shredded_variant/case-124.parquet
- [ ] shredded_variant/case-125-INVALID.parquet
- [ ] shredded_variant/case-126.parquet
- [ ] shredded_variant/case-127.parquet
- [ ] shredded_variant/case-128.parquet
- [ ] shredded_variant/case-129.parquet
- [ ] shredded_variant/case-130.parquet
- [ ] shredded_variant/case-131.parquet
- [ ] shredded_variant/case-132.parquet
- [ ] shredded_variant/case-133.parquet
- [ ] shredded_variant/case-134.parquet
- [ ] shredded_variant/case-135.parquet
- [ ] shredded_variant/case-136.parquet
- [ ] shredded_variant/case-137.parquet
- [ ] shredded_variant/case-138.parquet

### Test Summary

**Current Pass Rate: 177/215 (82.3%)** - Reading ALL columns from ALL row groups

Progress:
- Started (first column only): 163/215 (75.8%)
- After Dictionary Encoding (first column only): 187/220 (85.0%)
- After fixing tests to read ALL columns: 177/215 (82.3%)

**Note:** Test was updated to read from ALL columns, not just first column. This exposed additional issues:
- Boolean column reading issues (EOF errors)
- Delta encoding issues in some files
- Float16 handling issues

Remaining Failures by Category (38 total):
- Boolean EOF errors: ~15 files (alltypes_*, float16_*, rle_boolean, etc.)
- Delta encoding issues: ~5 files (delta_*)
- ZSTD compression: ~3 files
- GZIP compression: ~3 files
- LZ4/LZ4_RAW compression: ~5 files
- DATA_PAGE_V2: ~3 files
- FIXED_LEN_BYTE_ARRAY: ~3 files
- Other issues: ~1 file

### Test Categories
- [ ] Round-trip tests (write → read → compare)
- [x] Compatibility tests (read files from other implementations)
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
