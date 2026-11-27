# Arrow binding

Describes how `hardwood-arrow` converts Parquet schemas to Arrow and serves Parquet columns as Arrow `VectorSchemaRoot` batches.

Related documents:

- [COLUMN_READER.md](COLUMN_READER.md): batch model whose validity, offsets, and binary views match Arrow's layout
- [READ_PIPELINE.md](READ_PIPELINE.md): batch sizing and row-group boundaries
- [LOGICAL_TYPES.md](LOGICAL_TYPES.md): logical annotations consulted by the mapping
- [AVRO_BINDING.md](AVRO_BINDING.md): sibling module sharing the plan-before-reader shape
- [docs/content/how-to/arrow.md](../docs/content/how-to/arrow.md): user-facing guide

## Module surface

`hardwood-arrow` depends on `hardwood-core`, `org.apache.arrow:arrow-vector`, and `arrow-memory-core`. It declares no memory backend (`arrow-memory-unsafe` or `arrow-memory-netty` is chosen by the caller). `hardwood-core` has no Arrow dependency; one internal bridge in `dev.hardwood.internal.reader.batch` (`BinaryDictionaryLayout`) unwraps a `BinaryDictionary`'s backing `ByteArrayDictionary` for first-party bindings without expanding `BinaryDictionary`'s public API. Because Arrow Java opens `java.nio` reflectively, JVMs loading the module pass `--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED`.

Public package `dev.hardwood.arrow` exposes two `@Experimental` types:

| Type | Role |
|---|---|
| `ArrowReaders` | Static entry points over `ParquetFileReader`: `schema(...)` for standalone schema conversion; `batchReader(...)` and `buildBatchReader(...)` with `projection`, `filter(FilterPredicate)`, `filter(RowGroupPredicate)`, `batchSize`, `dictionaryEncoding`, and `dictionaryColumns` |
| `ArrowBatchReader` | Extends Arrow Java's `ArrowReader` (`DictionaryProvider`): `loadNextBatch`, reused `VectorSchemaRoot`, dictionary vectors, `bytesRead`, `close` |

Internals live in `dev.hardwood.arrow.internal`: `ArrowSchemaConverter`, `ArrowPlan`, `ScalarKind`, and `BatchLoader`.

**Arrow's reader contract.** `ArrowBatchReader` subclasses `org.apache.arrow.vector.ipc.ArrowReader`, calling `ensureInitialized()` upfront (zero bytes) so `getDictionaryIds()` and `lookup(long)` are usable before `getVectorSchemaRoot()`. `ArrowStreamWriter`, `ArrowFileWriter`, and `arrow-c-data`'s `Data.exportArrayStream` consume it without adapter code. `bytesRead()` counts Arrow dictionary and record-batch buffer bytes loaded so far.

**Conversion precedes the reader.** `BatchReaderBuilder.build()` runs `ArrowSchemaConverter.plan(...)` before calling `ParquetFileReader.buildColumnReaders(...)` so rejected schemas never start and orphan column worker threads.

Tests: `ArrowBatchReaderTest`, `ArrowDictionaryTest`, `ArrowIpcRoundTripTest`, `ArrowCDataDuckDbTest` (hardwood-arrow).

## Scope and schema mapping

`ArrowSchemaConverter.plan(...)` resolves the projection via core's `ProjectedSchema` and builds `ArrowPlan` (Arrow `Schema` + per-root `Node` tree) in one pass. The mapping matches Arrow C++'s metadata-free Parquet conversion (no `ARROW:schema` restoration, no extension types):

| Parquet | Annotation | Arrow type | `ScalarKind` |
|---|---|---|---|
| `BOOLEAN` | — | `Bool` | `BOOLEAN` |
| `INT32` | —, `INT(32, *)` | `Int(32, signed)` | `INT32` |
| `INT32` | `INT(8 / 16, signed)` | `Int(8 / 16, true)` | `INT8` / `INT16` |
| `INT32` | `INT(8 / 16, unsigned)` | `Int(8 / 16, false)` | `UINT8` / `UINT16` |
| `INT32` | `DATE` | `Date(DAY)` | `INT32` |
| `INT32` | `TIME(MILLIS)` | `Time(MILLISECOND, 32)` | `INT32` |
| `INT32`, `INT64` | `DECIMAL(p, s)` | `Decimal(p, s, 128)` | `DECIMAL_INT32` / `DECIMAL_INT64` |
| `INT64` | —, `INT(64, *)` | `Int(64, signed)` | `INT64` |
| `INT64` | `TIME(MICROS / NANOS)` | `Time(unit, 64)` | `INT64` |
| `INT64` | `TIMESTAMP(unit, adjusted)` | `Timestamp(unit, "UTC")` when adjusted, `Timestamp(unit, null)` otherwise | `INT64` |
| `INT96` | — | `Timestamp(NANOSECOND, null)` | `INT96_TIMESTAMP` |
| `FLOAT`, `DOUBLE` | — | `FloatingPoint(SINGLE / DOUBLE)` | `FLOAT` / `DOUBLE` |
| `BYTE_ARRAY` | —, `ENUM`, `BSON`, `GEOMETRY`, `GEOGRAPHY` | `Binary` | `BINARY` |
| `BYTE_ARRAY` | `STRING`, `JSON` | `Utf8` | `BINARY` |
| `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` | `DECIMAL(p, s)` | `Decimal(p, s, 128)` (`p ≤ 38`), `Decimal(p, s, 256)` (`p ≤ 76`) | `DECIMAL_BYTES` |
| `FIXED_LEN_BYTE_ARRAY(n)` | —, `UUID`, `INTERVAL`, `TIMESTAMP` | `FixedSizeBinary(n)` | `FIXED_BINARY` |
| `FIXED_LEN_BYTE_ARRAY(2)` | `FLOAT16` | `FloatingPoint(HALF)` | `FIXED_BINARY` |
| any | `UNKNOWN` | `Null` | `NULL` |

Groups:

| Parquet | Arrow | Nullable |
|---|---|---|
| unannotated group, `FILE` group, `VARIANT` group | `Struct` of projected children | when `OPTIONAL` |
| `LIST` group (2- or 3-level) | `List` of `GroupNode.getListElement` | when `OPTIONAL`; element by its repetition |
| `MAP` group | `Map(keysSorted=false)` over `entries` struct (`key`, `value`) | when `OPTIONAL`; `entries` and `key` never; `value` by its repetition |
| bare `repeated` primitive or group | `List` of element | never, element never |

Non-negative `SchemaNode.fieldId()` values become `PARQUET:field_id` field metadata (omitted on map `entries` and bare repeated primitive elements, matching Arrow C++). File key-value metadata becomes schema metadata minus `ARROW:schema`.

**Layers.** Each group reads its validity and offsets from one layer of its first projected leaf (the anchor), matching `NestedLevelComputer.walkLayers` and verified at every leaf against `NestedLevelComputer.computeLayers(FileSchema)`.

**Rejections.** Missing `FIXED_LEN_BYTE_ARRAY` width, `LIST` without element, `MAP` without key, or `MAP` with `OPTIONAL` key throw `SchemaIncompatibleException`. Partial `MAP` projection, `MAP` without value or with an `UNKNOWN` key, `DECIMAL` with precision > 76 or declared wider than its Arrow decimal, and unmapped physical/logical pairings throw `UnsupportedOperationException`. Bare repeated `VARIANT` groups are refused by core during `build()`.

Tests: `ArrowSchemaConverterTest`, `ArrowBatchReaderTest` (hardwood-arrow).

## Dictionary encoding interop

`ColumnReader` exposes `getDictionaryIds()` and an identity-stable `BinaryDictionary` per row group for `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY`, and `INT96` columns. `.dictionaryEncoding(true)` selects projected eligible columns whose footer metadata uses dictionary encoding; `.dictionaryColumns(...)` selects named leaf columns explicitly. Selected fields receive `DictionaryEncoding(id, false, Int(32, true))` in `readSchema()`.

- When `getBinaryDictionary()` matches the loaded instance by identity (`==`), only `int[]` indices are copied (bulk copy when non-null, mapping `-1` to `0` when nullable/parent-null).
- When the dictionary changes, `BatchLoader` builds a replacement `ArrowDictionaryBatch` (`isDelta = false`) from `BinaryDictionaryLayout` (single-chunk bulk copy or multi-chunk concatenation for `BINARY` / `FIXED_BINARY`, one conversion pass for `DECIMAL_BYTES` / `INT96_TIMESTAMP`).
- On plain-page fallback (`getDictionaryIds() == null`), `BatchLoader` emits the batch's values as a replacement dictionary with identity indices `0..count-1` and clears the cached dictionary reference.
- If a non-nullable dictionary child inside an all-null `OPTIONAL` struct has a 0-entry dictionary, a 1-entry placeholder dictionary keeps placeholder index `0` in bounds.

Tests: `ArrowDictionaryTest`, `ArrowCDataDuckDbTest` (hardwood-arrow).

## Loading a batch

`ArrowBatchReader.loadNextBatch()` calls `prepareLoadNextBatch()`, advances `ColumnReaders`, builds an `ArrowRecordBatch` via `BatchLoader.load(...)`, and hands it to `loadRecordBatch(...)`.

**Non-nullable children beneath nullable structs.** In `ColumnReader`, a `REQUIRED` child of an `OPTIONAL` struct has `sourceValidity` unset on parent-null rows. Arrow requires `nullCount == 0` and an all-set bitmap on non-nullable fields, so `BatchLoader` uses `sourceValidity` to guard value conversion and `arrowValidity` (`field.isNullable() ? sourceValidity : Validity.NO_NULLS`) for the Arrow field node and bitmap.

| Kind | Buffers | Fill |
|---|---|---|
| `NULL` (`UNKNOWN`) | none | `count` nulls after verifying no present values exist |
| `BOOLEAN` | validity, data | packed 8 bits per byte directly into off-heap memory |
| `INT32`, `INT64`, `FLOAT`, `DOUBLE` | validity, data | bulk copy via little-endian `nio` view |
| `INT8`, `INT16`, `UINT8`, `UINT16` | validity, data | range-checked per present value, narrowed directly into view |
| `DECIMAL_INT32`, `DECIMAL_INT64` | validity, data | sign-extended to 16 little-endian bytes per present value |
| `DECIMAL_BYTES` | validity, data | big-endian payload reversed and sign-extended to 16 or 32 bytes |
| `INT96_TIMESTAMP` | validity, data | Julian day + nanos of day converted to epoch nanos with overflow check |
| `FIXED_BINARY` | validity, data | gathered per present value (serves `FLOAT16` directly as 2 little-endian bytes) |
| `BINARY` | validity, offsets, data | single pass checks total length (`<= Integer.MAX_VALUE`) and contiguity; contiguous batches bulk-copy, otherwise gather per present value |

**Validity.** Core's bitmap layout matches Arrow's; full 64-bit words bulk-copy via `LongBuffer.put`, tail bytes are written individually, and trailing bits past `count` are masked off. Allocating all-set bitmaps upfront keeps every allocation inside `BatchLoader.load(...)`, so an OOM releases all batch buffers before `VectorLoader` mutates the root.

**Failure.** If any allocation or validation throws, `BatchLoader.load(...)` closes every buffer it allocated, and `ArrowBatchReader` marks itself failed, closes `ColumnReaders`, and rethrows; subsequent `loadNextBatch()` calls throw `IllegalStateException`.

Tests: `ArrowBatchReaderTest`, `BatchLoaderTest`, `ArrowIpcRoundTripTest` (hardwood-arrow).

## Memory, fixtures, and oracle

The caller owns the `BufferAllocator`; `ArrowBatchReader` never creates or closes one. Every test asserts zero allocated bytes after teardown.

`ArrowFixtures` writes its Parquet fixtures at test time under `arrow/target/arrow-fixtures/<pid>` through Hardwood's `ColumnWriter`. `ArrowDatasetOracleTest` scans those fixtures and core's checked-in fixtures through Arrow C++'s Parquet reader (`arrow-dataset`, test-only in `hardwood-test-bom`) and asserts schema, nullability, null counts, and vector equality. `ArrowCDataDuckDbTest` exports streams via `Data.exportArrayStream` into DuckDB and compares aggregates against `read_parquet`.

Tests: `ArrowDatasetOracleTest`, `ArrowCDataDuckDbTest`, `ArrowBatchReaderTest`, `ArrowIpcRoundTripTest` (hardwood-arrow).

## Boundaries

- **Numeric dictionaries** (`INT32`, `INT64`, `FLOAT`, `DOUBLE`) decode to typed arrays on core's decode thread and emit plain Arrow vectors.
- **Shredded Variant values** are served as stored (`metadata`, `value`, `typed_value`) without canonical reassembly or Arrow's Variant extension type.
- **`ARROW:schema`** is neither read nor forwarded.
- **`Utf8View` / `BinaryView`**, **CLI IPC output**, and **Arrow-to-Parquet writing** are separate follow-ups under #153.
- **Zero-copy decode** directly into caller-provided off-heap buffers depends on #737.
