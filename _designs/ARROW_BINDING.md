# Arrow binding

Describes how the `hardwood-arrow` module serves the primitive columns of a Parquet file as Arrow `VectorSchemaRoot` batches: its public surface, the columns it accepts, the mapping from a Parquet column to an Arrow field, how a `ColumnReaders` batch becomes Arrow buffers, and the ownership of the memory involved.

Related documents:

- [COLUMN_READER.md](COLUMN_READER.md): the batch model the module reads, whose validity, offsets and byte views are already Arrow's layout
- [READ_PIPELINE.md](READ_PIPELINE.md): batch sizing and where a batch ends
- [LOGICAL_TYPES.md](LOGICAL_TYPES.md): the meaning of each annotation the mapping consults
- [AVRO_BINDING.md](AVRO_BINDING.md): the sibling module, whose plan-before-reader shape this one shares
- [docs/content/how-to/arrow.md](../docs/content/how-to/arrow.md): the user-facing API

## Module surface

`hardwood-arrow` depends on `hardwood-core`, `org.apache.arrow:arrow-vector` and `arrow-memory-core`. It declares no memory backend: the caller adds `arrow-memory-unsafe` or `arrow-memory-netty`, as a reader of core adds the codecs it needs. Core has no Arrow dependency. Arrow Java opens `java.nio` through reflection, so every JVM that loads the module runs with `--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED`; the module's Surefire configuration appends it to the parent's `test.argLine`.

Its public package `dev.hardwood.arrow` holds two top-level types:

| Type | Role |
|---|---|
| `ArrowReaders` | Static entry point over a `ParquetFileReader` and a `BufferAllocator`: `batchReader` for every column, `buildBatchReader` for a builder with `projection`, `filter` and `batchSize` |
| `ArrowBatchReader` | Extends Arrow Java's `ArrowReader`: `loadNextBatch`, the reused root, `bytesRead`, `close` |

Everything else lives in `dev.hardwood.arrow.internal`: `ArrowSchemaConverter` (schema conversion and plan building), `ArrowPlan` (the plan), `ScalarKind` (how a column's values reach its buffers) and `BatchLoader` (the buffer fill).

**Arrow's reader contract, not one of its own.** `ArrowBatchReader` extends `org.apache.arrow.vector.ipc.ArrowReader` and implements its two abstract reads: `readSchema` returns the converted schema, `loadNextBatch` advances the wrapped `ColumnReaders` and loads the batch through the base class's `loadRecordBatch`. Everything written against `ArrowReader` (`ArrowStreamWriter`, `ArrowFileWriter`, `arrow-c-data`'s `Data.exportArrayStream`, Flight) therefore consumes Hardwood output with no code in this module, and the lifecycle a consumer has to know (one root, refreshed on every load; transfer what is kept) is the one Arrow documents for every reader. `bytesRead`, which the contract defines as bytes taken from a channel, counts the bytes of the Arrow buffers loaded so far, there being no channel beneath this reader.

**Conversion precedes the reader.** The builder converts the schema before it builds the underlying `ColumnReaders`, for the reason [AVRO_BINDING.md](AVRO_BINDING.md#module-surface) gives: building the column readers starts a worker per column, and a rejection after that would orphan them. Every rejection below surfaces from `build()` with nothing to release.

Tests: `ArrowBatchReaderTest`, `ArrowIpcRoundTripTest`, `ArrowCDataDuckDbTest` (hardwood-arrow).

## Scope: primitive top-level columns

The reader serves a column that is a non-repeated primitive directly below the root: `REQUIRED` or `OPTIONAL`, with a one-element field path. `ArrowSchemaConverter.plan` resolves the projection through core's `ProjectedSchema`, exactly as `ColumnReaders` does, and checks every leaf it selects. A leaf whose path has more than one element, which includes a leaf below a `REQUIRED` group that contributes no layer, and a `REPEATED` primitive are refused with `UnsupportedOperationException` naming the leaf's path. The check covers the output projection only: a filter may name any column, and the rest of a file with nested columns is readable through a projection that leaves them out.

The restriction is what keeps the loader to one leaf slot per record: with no layer above any leaf, `getValueCount()` equals the record count for every column, and no offsets or ancestor validity need translating. Nested output is a later milestone and has no half-implemented form here.

Tests: `ArrowSchemaConverterTest`, `ArrowBatchReaderTest` (hardwood-arrow).

## Schema mapping and plan

`ArrowSchemaConverter.plan(FileSchema, List<SchemaElement>, ColumnProjection)` is the single conversion entry point. It returns an `ArrowPlan`: the Arrow `Schema` and, in field order, one `Entry` pairing a field's `ColumnSchema`, its `Field` and its `ScalarKind`; an entry's index is the field's position among the requested columns, the index `ColumnReaders.getColumnReader(int)` serves it at. A column the projection names twice has two entries and two fields, as it has two reader positions. Schema and plan are built in one traversal, so loading a batch never re-derives from the Arrow type how a column is filled.

The mapping is Arrow C++'s metadata-free conversion with extension inference off, which is what PyArrow applies to a file carrying no `ARROW:schema` entry:

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
| `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` | `DECIMAL(p, s)` | `Decimal(p, s, 128)` for `p ≤ 38`, `Decimal(p, s, 256)` above | `DECIMAL_BYTES` |
| `FIXED_LEN_BYTE_ARRAY(n)` | —, `UUID`, `INTERVAL`, `TIMESTAMP` | `FixedSizeBinary(n)` | `FIXED_BINARY` |
| `FIXED_LEN_BYTE_ARRAY(2)` | `FLOAT16` | `FloatingPoint(HALF)` | `FIXED_BINARY` |
| any | `NULL` | `Null` | `NULL` |

A `ScalarKind` names the fill, not the type: `DATE` and bare `INT32` are both a bulk copy of the reader's `int[]`. The `FIXED_BINARY` kind serves `FLOAT16` too, because the two little-endian bytes Parquet stores are Arrow's half-float layout. Nullability follows repetition. A top-level column with a field id of zero or more carries it as `PARQUET:field_id` field metadata, as Arrow C++ records it, and a negative id is none, as Arrow C++ reads it; the ids come from the footer's `SchemaElement`s, which the builder takes from `ParquetFileReader.getFileMetaData().schema()` (the first file's, on a multi-file reader), since `FileSchema` does not keep them. No other metadata is emitted, and `ARROW:schema` is never consulted.

**Rejections.** A `FIXED_LEN_BYTE_ARRAY` without a positive width fails through core's `FixedWidthValidator` (`SchemaIncompatibleException`); a `DECIMAL` with precision above 76, a fixed-width `DECIMAL` declared over more bytes than the Arrow decimal of its precision holds (as Arrow C++ refuses it), and any pairing of physical type and annotation the table does not list, fail with `UnsupportedOperationException` naming the column. An unknown projected name fails as core's resolution fails it.

Tests: `ArrowSchemaConverterTest`, `ArrowBatchReaderTest` (hardwood-arrow).

## Loading a batch

`ArrowBatchReader.loadNextBatch` calls the base class's `prepareLoadNextBatch`, advances the `ColumnReaders`, and has `BatchLoader.load` build an `ArrowRecordBatch` from the batch: one `ArrowFieldNode` and the Arrow buffers of each field, in field order, each buffer allocated at its exact size from the caller's allocator and filled once. The base class's `loadRecordBatch` then loads it into the root through Arrow's `VectorLoader`, which retains the buffers in the vectors and closes the batch. The loader asserts that every column's value count is the batch's record count, which the scope guarantees.

| Kind | Buffers | Fill |
|---|---|---|
| `NULL` | none | a node of `count` nulls, after checking that the batch holds no value; one that does fails the batch, as the row path refuses it |
| `BOOLEAN` | validity, data | `boolean[]` packed into bits, present values only |
| `INT32`, `INT64`, `FLOAT`, `DOUBLE` | validity, data | one bulk copy of the typed array through a little-endian `nio` view of the buffer |
| `INT8`, `INT16`, `UINT8`, `UINT16` | validity, data | per present value, range-checked against the annotation, then narrowed |
| `DECIMAL_INT32`, `DECIMAL_INT64` | validity, data | per present value, the unscaled value and its sign extension as 16 little-endian bytes |
| `DECIMAL_BYTES` | validity, data | per present value, the big-endian payload reversed and sign-extended to 16 or 32 bytes; a `BYTE_ARRAY` payload longer than that is refused |
| `INT96_TIMESTAMP` | validity, data | per present value, Julian day and nanoseconds of day to nanoseconds since the epoch, with overflow refused |
| `FIXED_BINARY` | validity, data | per present value, the view's bytes gathered at `index * width` |
| `BINARY` | validity, offsets, data | see below |

**Validity.** The reader's bitmap is Arrow's: set bit means present, bit `i` of word `i / 64`, which written little-endian is bit `i` of byte `i / 8`. The words are copied whole, the tail bytes one by one, and the bits at or past the item count cleared, since the reader leaves them undefined. `Validity.NO_NULLS` becomes an all-set bitmap allocated by the loader, although Arrow would accept an empty one and synthesize the bitmap: `VectorLoader` releases a vector's old bitmap before allocating that replacement, so an allocation failing there would leave the vector over a released buffer, whereas every allocation the loader makes is released as one if any of them fails. The node's null count is `Validity.nullCount(count)`.

**Binary values.** `BatchLoader.views` measures the present values' views in one pass: their total length as a `long`, since the views of a dictionary-backed batch may name the same bytes many times over, and whether every present value starts where the previous one ended. A contiguous batch, which a column no dictionary page reached produces, is one bulk copy of the run and a running sum for the offsets; any other batch is gathered value by value. The decision reads the views, never the page encoding. The total is checked before any allocation: Arrow's `Binary` and `Utf8` carry 32-bit offsets, so a batch whose values exceed `Integer.MAX_VALUE` bytes is refused with `IllegalStateException` naming the column and advising a smaller batch size. The reader does not split a batch or switch to a large type.

**Positions no value is written at.** A data buffer filled per value is zeroed first, so a null position reads as zero and the buffer's bytes are a function of the batch; a bulk-copied buffer carries whatever the reader's array held at null positions, which the format leaves unspecified.

**Failure.** `BatchLoader.load` closes every buffer it allocated if any step throws, so a failed load leaves nothing of the batch allocated; the empty root's own buffers, which Arrow's `setRowCount(0)` allocates, remain the root's and go with `close()`. That preparation runs inside the same failure path as the load, since it allocates too. `ArrowBatchReader` then marks itself failed, closes the `ColumnReaders`, and rethrows; a later `loadNextBatch` throws `IllegalStateException`, because the read cannot resume from a batch the scan has consumed. A value that contradicts its annotation (an `INT(8)` holding 300, a decimal payload wider than its precision) is `ParquetReadException`, the file being at fault; an `INT96` instant outside a 64-bit nanosecond count is `UnsupportedOperationException`, the file being valid and Arrow's type the limit. Both name the column and the row.

Tests: `ArrowBatchReaderTest`, `BatchLoaderTest` (the view measurement, the size limit on synthetic views, the `INT96` arithmetic at both ends of its range, the `NULL` and narrowing checks), `ArrowIpcRoundTripTest` (hardwood-arrow). The refusal of a `BYTE_ARRAY` decimal payload longer than its Arrow width, and an all-null `OPTIONAL` column of another type, are untested: no writer in the build produces the first, and no fixture holds the second.

## Memory and lifecycle

The caller owns the `BufferAllocator`; the module never creates a `RootAllocator` and never closes the one it is given. Every batch is loaded into the one root the base class creates, and `VectorLoader` releases the previous batch's buffers as it loads the next. A consumer that keeps a batch transfers it (`TransferPair`, `VectorUnloader`) and releases what it transferred. `close()` is `ArrowReader`'s: it releases the root, then `closeReadSource` closes the `ColumnReaders`, which the reader owns; the `ParquetFileReader` stays the caller's. Close is idempotent, and `loadNextBatch` after it throws `IllegalStateException`. With the reader and everything transferred out of it closed, the allocator holds nothing from the module; every test asserts this on its allocator.

Through `arrow-c-data`, `Data.exportArrayStream` wraps the reader in an `ArrowArrayStream`, whose release by the consumer closes the reader. The test registers one in DuckDB's JDBC driver and queries it, releasing the stream's Java handle after the connection.

Tests: `ArrowBatchReaderTest`, `ArrowCDataDuckDbTest` (hardwood-arrow).

## Fixtures and the Arrow C++ oracle

The module checks in no fixture. `ArrowFixtures` (a test helper) writes its files under `arrow/target/arrow-fixtures/<pid>` once per test JVM, through Hardwood's own `ColumnWriter`, from one declaration of seven rows carrying every mapped type with a different null pattern per optional column: once with `PLAIN` pages, once with `AUTO` encoding in row groups of three so that dictionaries change between batches and nulls fall on batch edges, and once with a real `ARROW:schema` entry naming other types, whose output must equal the bare file's; plus an empty file. The expected table of each is built from the same declaration through Arrow's typed setters (`DecimalVector.setSafe(BigDecimal)`, `Float2Vector`, `TimeStampNanoTZVector` and the rest), with the Arrow schema stated by hand, so every value is encoded twice by independent code and the schema mapping is pinned rather than derived. The tests append every batch into one root and compare schema, null counts and vectors (`VectorEqualsVisitor`) column by column.

Arrow C++'s own Parquet reader is the independent oracle, reached in-process through `arrow-dataset` (Arrow Java's JNI binding, a test dependency of about 80 MB that bundles the native library for Linux, macOS and Windows on x86_64 and aarch64). `ArrowDatasetOracleTest` scans the same file through a `FileSystemDatasetFactory`, takes the batches over the C Data Interface, and holds the module to the same strict comparison as the hand-built tables: schema, nullability, null counts and values. Since that reader is the specification the mapping follows, every mapped type is covered, half floats, UUIDs, intervals, 256-bit decimals and the `NULL` type included. The shapes Hardwood's writer cannot produce come from elsewhere through the same comparison: legacy `INT96` and a dictionary that falls back to plain pages mid-chunk or changes between row groups from core's checked-in fixtures, and a file with field ids, zero among them, written at test time by Arrow C++ itself through `arrow-dataset`'s `DatasetFileWriter`, since Hardwood's writer writes no field ids. The file with `ARROW:schema` stored is left out, since Arrow C++ honours that entry and the module does not. `INTERVAL` is written in its converted-type-only form, which Arrow C++ reads as `FixedSizeBinary(12)`; parquet-java's form, which pairs the converted type with the union's `UNKNOWN` member, reads as the `NULL` logical type.

Tests: `ArrowBatchReaderTest`, `ArrowDatasetOracleTest`, `ArrowIpcRoundTripTest` (hardwood-arrow).

## Boundaries

- **Nested columns.** Structs, lists, maps, bare repeated fields and Variant are refused at `build()`; serving them needs a group-oriented plan over the per-leaf layer model.
- **Dictionary output.** Dictionary-encoded Parquet columns are read into plain vectors. A dictionary-typed Arrow field would need the type fixed at `build()` from the footer's encodings and replacement dictionaries per row group.
- **`ARROW:schema`.** Not read: large offsets, the original zone of an unadjusted timestamp, extension types and field metadata PyArrow recorded are not restored. The CLI's rendering of the entry is #1007.
- **View types.** `Utf8View` and `BinaryView`, whose layout the reader's views already have, are not produced; a dictionary-backed batch is gathered instead.
- **CLI IPC output** and **Arrow-to-Parquet writing** are separate proposals under #153.
- **API stability.** `ArrowReaders` and `ArrowBatchReader` are `@Experimental`, as `ColumnReader` is: every follow-up above touches what the reader serves or produces.
- **Zero copy.** Decoded values are heap arrays and Arrow Java vectors own off-heap memory, so every batch is one bulk copy per buffer, with a converting loop where the layouts differ.
