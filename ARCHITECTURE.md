# Hardwood Architecture

A minimal-dependency Apache Parquet reader and writer in Java.

## What This Is

Hardwood reads and writes Parquet files without Hadoop or parquet-java. It implements the Parquet format specification from scratch: the Thrift Compact Protocol for the footer and page headers, the standard encodings, the page index and bloom filters, and the Dremel shredding and assembly of nested data. The design of each subsystem is in `_designs/`; this page is the map.

## Design Documents

| Area | Documents |
|---|---|
| Read path | [READ_PIPELINE.md](_designs/READ_PIPELINE.md) (row groups to published batches, threading, batch sizing, multi-file planning), [ROW_READER.md](_designs/ROW_READER.md), [COLUMN_READER.md](_designs/COLUMN_READER.md), [NESTED_DECODE.md](_designs/NESTED_DECODE.md) (levels to nested batches), [VALUE_DECODE.md](_designs/VALUE_DECODE.md) (page values to Java values) |
| Filtering | [PREDICATE_MODEL.md](_designs/PREDICATE_MODEL.md) (the `FilterPredicate` API and its resolution), [STATISTICS_PRUNING.md](_designs/STATISTICS_PRUNING.md) (pruning row groups and pages from metadata), [RECORD_FILTERING.md](_designs/RECORD_FILTERING.md) (exact row filtering on both readers) |
| I/O and metadata | [INPUT_FILES.md](_designs/INPUT_FILES.md), [S3_STORAGE.md](_designs/S3_STORAGE.md), [FETCH_PLANNING.md](_designs/FETCH_PLANNING.md) (which bytes a read requests), [FILE_METADATA.md](_designs/FILE_METADATA.md) (footer, Thrift parsing, page index), [EXCEPTION_MODEL.md](_designs/EXCEPTION_MODEL.md) |
| Writer | [WRITER.md](_designs/WRITER.md) (write model, row-group lifecycle, memory), [WRITER_INPUT.md](_designs/WRITER_INPUT.md) (schema builder, `ColumnBatch`, shredding, `RowWriter`), [WRITER_ENCODING.md](_designs/WRITER_ENCODING.md) (page layout, dictionary selection, codecs, statistics), [WRITER_VALIDATION.md](_designs/WRITER_VALIDATION.md) (interop gate) |
| Types | [LOGICAL_TYPES.md](_designs/LOGICAL_TYPES.md) (annotations, timestamps, Variant, geospatial), [AVRO_BINDING.md](_designs/AVRO_BINDING.md) (`hardwood-avro`) |
| CLI | [DIVE_ARCHITECTURE.md](_designs/DIVE_ARCHITECTURE.md), [DIVE_UI_RULES.md](_designs/DIVE_UI_RULES.md), [CLI_VALUE_RENDERING.md](_designs/CLI_VALUE_RENDERING.md) |
| Project | [DOCUMENTATION.md](_designs/DOCUMENTATION.md), [BUILD_INFRASTRUCTURE.md](_designs/BUILD_INFRASTRUCTURE.md), [TESTING.md](TESTING.md), [PERFORMANCE.md](PERFORMANCE.md), [NATIVE_BUILD.md](NATIVE_BUILD.md), [FORMAT_COVERAGE.md](FORMAT_COVERAGE.md) |

Implementation plans for work spanning several PRs are in `_plans/`.

## Key Design Decisions

### Custom Thrift Parser
The Thrift Compact Protocol is implemented from scratch (`internal/thrift/`) instead of using the Thrift library. This removes a heavyweight dependency and gives control over parsing policy and memory allocation ([FILE_METADATA.md](_designs/FILE_METADATA.md)).

### Flat vs Nested Split
A file whose top-level fields are all primitive and whose columns have no repetition is read by `FlatRowReader`, which serves accessors straight from each column's typed value array and validity bitmap. Any other schema is read by `NestedRowReader`, which navigates offsets derived from the repetition and definition levels ([ROW_READER.md](_designs/ROW_READER.md)).

### Primitive-First API
The public API (`PqIntList`, `getLong()`, `ColumnReader`'s typed arrays) avoids boxing wherever possible. Internal code uses primitive arrays (`int[]`, `long[]`) rather than `List<Integer>`.

### Per-Column Pipelines on Virtual Threads
Each decoded column (the projection plus any filter-only predicate columns) has two virtual threads of its own: a retriever that fetches its pages and a drain that assembles and publishes its batches, with back-pressure towards the consumer. Pages are decompressed and decoded in parallel on a fixed platform-thread pool owned by the `HardwoodContext` ([READ_PIPELINE.md](_designs/READ_PIPELINE.md)).

### Minimal Dependencies
`hardwood-core` has no required runtime dependency. GZIP goes through the JDK; the other codecs use compression libraries that are optional, needed only for the codecs a file uses. LZO is not supported. `hardwood-s3` implements its S3 client and SigV4 signing without the AWS SDK.

## Module Overview

| Module | Purpose |
|--------|---------|
| `core` | The Parquet reader and writer library |
| `s3` | Reading from S3-compatible object storage |
| `aws-auth` | Bridges the AWS SDK credential chain to Hardwood's credential types |
| `avro` | Reading rows as Avro `GenericRecord`s |
| `cli` | The `hardwood` command-line tool, including the `dive` TUI |
| `parquet-java-compat` | parquet-java API compatibility layer without parquet-java or Hadoop dependencies |
| `integration-test` | Tests of `hardwood-core` consumed as a packaged dependency, on the Java 21 baseline |
| `parquet-testing-runner` | Tests against the apache/parquet-testing files, compared with parquet-java |
| `performance-testing` | Benchmarks (enable with `-Pperformance-test`) |
| `bom`, `test-bom` | Bills of materials for Hardwood and its test dependencies |
| `test-support` | Test helpers shared across modules |
| `error-prone-checks` | The project's Error Prone checks |
| `tools/predicate-audit` | Audit of the predicate literal rule against an oracle, parquet-java and DuckDB (enable with `-Ppredicate-audit`) |
