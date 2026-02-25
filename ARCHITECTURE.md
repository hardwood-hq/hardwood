# Hardwood Architecture

A minimal-dependency Apache Parquet reader in Java.

## What This Is

Hardwood reads Parquet files without requiring Hadoop or parquet-java. It implements the Parquet format specification from scratch, including custom Thrift Compact Protocol parsing, all standard encodings, and the Dremel record assembly algorithm.

## Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Parquet File                                                               │
│  ┌──────────┬──────────────────────────────────────────────────┬─────────┐  │
│  │  PAR1    │  Row Groups → Column Chunks → Pages              │ Footer  │  │
│  │ (magic)  │                                                  │ + PAR1  │  │
│  └──────────┴──────────────────────────────────────────────────┴─────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  ParquetFileReader.open()                                                   │
│  - Validates magic bytes (PAR1)                                             │
│  - Reads footer (Thrift-encoded FileMetaData)                               │
│  - Builds FileSchema from schema elements                                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                          ┌───────────┴───────────┐
                          ▼                       ▼
              ┌───────────────────┐    ┌───────────────────┐
              │  FlatRowReader    │    │  NestedRowReader  │
              │  (no nesting)     │    │  (structs/lists)  │
              └───────────────────┘    └───────────────────┘
                          │                       │
                          ▼                       ▼
              ┌───────────────────┐    ┌───────────────────┐
              │  FlatColumnData   │    │  NestedColumnData │
              │  (values only)    │    │  (+ rep/def lvls) │
              └───────────────────┘    └───────────────────┘
                          │                       │
                          │                       ▼
                          │            ┌───────────────────┐
                          │            │  RecordAssembler  │
                          │            │  (Dremel inverse) │
                          │            └───────────────────┘
                          │                       │
                          └───────────┬───────────┘
                                      ▼
                              ┌───────────────┐
                              │  Row values   │
                              │  (primitives, │
                              │   PqStruct,   │
                              │   PqList...)  │
                              └───────────────┘
```

## Package Structure

```
dev.hardwood.hardwood/
├── reader/              # PUBLIC API - Entry points
│   ├── ParquetFileReader    Main entry: open(), createRowReader()
│   ├── RowReader            Row iteration interface
│   ├── FlatRowReader        Optimized for flat schemas
│   ├── NestedRowReader      Handles structs, lists, maps
│   └── ColumnReader         Low-level columnar access
│
├── row/                 # PUBLIC API - Result types
│   ├── PqStruct             Nested struct access
│   ├── PqList               List access (strings(), structs()...)
│   ├── PqIntList, PqLongList, PqDoubleList   Primitive lists (unboxed)
│   └── PqMap                Map access
│
├── metadata/            # PUBLIC - File/column metadata records
│   ├── FileMetaData         Root metadata (schema, row groups)
│   ├── RowGroup             Row group boundaries
│   ├── ColumnChunk          Column location in file
│   └── PageHeader, Encoding, CompressionCodec...
│
├── schema/              # PUBLIC - Schema representation
│   ├── FileSchema           Complete file schema
│   ├── ColumnSchema         Per-column schema (type, levels)
│   └── FieldPath            Navigation path to leaf columns
│
└── internal/            # PRIVATE - Implementation details
    ├── thrift/              Custom Thrift Compact Protocol parser
    │   ├── ThriftCompactReader   Low-level varint/zigzag decoding
    │   ├── FileMetaDataReader    Footer parsing
    │   └── PageHeaderReader      Page header parsing
    │
    ├── encoding/            Page encoding decoders
    │   ├── PlainDecoder          PLAIN encoding
    │   ├── RleBitPackingHybridDecoder   RLE/bit-packing
    │   ├── DeltaBinaryPackedDecoder     DELTA_BINARY_PACKED
    │   ├── DeltaByteArrayDecoder        DELTA_BYTE_ARRAY
    │   └── ByteStreamSplitDecoder       BYTE_STREAM_SPLIT
    │
    ├── compression/         Decompressor registry
    │   └── DecompressorFactory   GZIP, Snappy, ZSTD, LZ4, Brotli
    │
    ├── reader/              Core reading logic
    │   ├── PageReader            Single page decoding
    │   ├── PageScanner           Page iteration + dictionary
    │   ├── RecordAssembler       Dremel algorithm (rep/def → records)
    │   ├── FlatColumnData        Flat column value storage
    │   ├── NestedColumnData      Nested column with levels
    │   └── Dictionary            Dictionary-encoded values
    │
    └── conversion/          Logical type conversions
        └── LogicalTypeConverter   INT32→LocalDate, BYTE_ARRAY→String...
```

## Start Reading Here

1. **`ParquetFileReader.open()`** (`reader/ParquetFileReader.java:55`)
   - File validation, footer reading, schema construction

2. **`PageReader.decodePage()`** (`internal/reader/PageReader.java:68`)
   - Where bytes become values: decompression → level decoding → value decoding

3. **`RecordAssembler.assembleRow()`** (`internal/reader/RecordAssembler.java:66`)
   - Dremel algorithm: repetition/definition levels → nested records

4. **`FlatRowReader` vs `NestedRowReader`** (`reader/`)
   - Flat: skips level tracking, direct primitive access
   - Nested: full Dremel assembly with rep/def levels

## Key Design Decisions

### Custom Thrift Parser
We implement Thrift Compact Protocol from scratch (`internal/thrift/`) instead of using the Thrift library. This eliminates a heavyweight dependency and gives us control over memory allocation.

### Flat vs Nested Split
Schemas without nesting (just primitive columns) take a fast path that skips repetition/definition level tracking entirely. This is a significant performance win for flat data like the NYC taxi dataset.

### Primitive-First API
The public API (`PqIntList`, `getLong()`, etc.) avoids boxing wherever possible. Internal code uses primitive arrays (`int[]`, `long[]`) rather than `List<Integer>`.

### Virtual Threads for Parallelism
`ParquetFileReader` uses virtual threads (`Executors.newVirtualThreadPerTaskExecutor()`) for parallel column batch fetching, avoiding fixed thread pool sizing issues.

### Minimal Dependencies
Only compression libraries are external dependencies. Everything else (Thrift parsing, encodings, assembly) is implemented from scratch.

## Module Overview

| Module | Purpose |
|--------|---------|
| `core` | The Parquet reader library |
| `parquet-java-compat` | Drop-in parquet-java API compatibility layer |
| `integration-test` | Cross-implementation compatibility tests |
| `parquet-testing-runner` | Test runner for parquet-testing files |
| `performance-testing` | Benchmarks (enable with `-Pperformance-test`) |

