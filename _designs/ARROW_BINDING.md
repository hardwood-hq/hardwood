# Arrow binding

Describes how the `hardwood-arrow` module converts Parquet schemas to Arrow and serves the columns of a Parquet file as Arrow `VectorSchemaRoot` batches: its public surface, the columns it accepts, the mapping from a Parquet column or group to an Arrow field, dictionary-encoded Arrow output, how a `ColumnReaders` batch becomes Arrow buffers, nested groups included, and the ownership of the memory involved.

Related documents:

- [COLUMN_READER.md](COLUMN_READER.md): the batch model the module reads, whose validity, offsets, byte views and binary dictionaries are already Arrow's layout
- [READ_PIPELINE.md](READ_PIPELINE.md): batch sizing and where a batch ends
- [LOGICAL_TYPES.md](LOGICAL_TYPES.md): the meaning of each annotation the mapping consults
- [AVRO_BINDING.md](AVRO_BINDING.md): the sibling module, whose plan-before-reader shape this one shares
- [docs/content/how-to/arrow.md](../docs/content/how-to/arrow.md): the user-facing API

## Module surface

`hardwood-arrow` depends on `hardwood-core`, `org.apache.arrow:arrow-vector` and `arrow-memory-core`. It declares no memory backend: the caller adds `arrow-memory-unsafe` or `arrow-memory-netty`, as a reader of core adds the codecs it needs. Core has no Arrow dependency; one internal bridge in `dev.hardwood.internal.reader.batch` (`BinaryDictionaryLayout`) unwraps a `BinaryDictionary`'s backing `ByteArrayDictionary` for first-party bindings without adding methods or internal interfaces to `BinaryDictionary`'s public API. Arrow Java opens `java.nio` through reflection, so every JVM that loads the module runs with `--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED`; the module's Surefire configuration appends it to the parent's `test.argLine`.

Its public package `dev.hardwood.arrow` holds two top-level types:

| Type | Role |
|---|---|
| `ArrowReaders` | Static entry point over a `ParquetFileReader`: `schema` for standalone Parquet-to-Arrow schema conversion without starting workers or passing an allocator; `batchReader` for every column; `buildBatchReader` for a builder with `projection`, `filter(FilterPredicate)`, `filter(RowGroupPredicate)`, `batchSize`, `dictionaryEncoding` and `dictionaryColumns` |
| `ArrowBatchReader` | Extends Arrow Java's `ArrowReader` (implementing `DictionaryProvider`): `loadNextBatch`, the reused root, dictionary vectors, `bytesRead`, `close` |

Everything else lives in `dev.hardwood.arrow.internal`: `ArrowSchemaConverter` (schema conversion and plan building), `ArrowPlan` (the plan), `ScalarKind` (how a column's values reach its buffers) and `BatchLoader` (the dictionary and record-batch buffer fill).

**Arrow's reader contract, not one of its own.** `ArrowBatchReader` extends `org.apache.arrow.vector.ipc.ArrowReader` and implements its two abstract reads: `readSchema` returns the converted message-format schema (carrying value types and any `DictionaryEncoding`), and `loadNextBatch` advances the wrapped `ColumnReaders`, loads any new or changed dictionaries through `loadDictionary`, and loads the batch through `loadRecordBatch`. The constructor calls `ensureInitialized()` (allocating zero bytes) so `getDictionaryIds()` and `lookup(long)` are usable immediately after `build()`. Everything written against `ArrowReader` (`ArrowStreamWriter`, `ArrowFileWriter`, `arrow-c-data`'s `Data.exportArrayStream`, Flight) therefore consumes Hardwood output with no code in this module, and the lifecycle a consumer has to know (one root, refreshed on every load; transfer what is kept) is the one Arrow documents for every reader. `bytesRead`, which the contract defines as bytes taken from a channel, counts the bytes of the Arrow dictionary and record-batch buffers loaded so far, there being no channel beneath this reader.

**Conversion precedes the reader.** The builder converts the schema and validates every dictionary column selection before it builds the underlying `ColumnReaders`, for the reason [AVRO_BINDING.md](AVRO_BINDING.md#module-surface) gives: building the column readers starts a worker per column, and a rejection after that would orphan them. Every rejection below surfaces from `build()` with nothing to release.

Tests: `ArrowBatchReaderTest`, `ArrowDictionaryTest`, `ArrowIpcRoundTripTest`, `ArrowCDataDuckDbTest` (hardwood-arrow).

## Scope

The reader serves primitive columns and the groups around them: structs, `LIST` and `MAP` groups in the three-level and the legacy two-level encodings, through core's `GroupNode.getListElement` and `getMapKey` / `getMapValue` rules, `repeated` fields outside a `LIST` or `MAP` scaffold, which read as lists of present elements, and Variant groups, which read as the struct of the fields they store. A map of which the projection selects only the key or only the value is refused at `build()` with `UnsupportedOperationException` naming the map, since an Arrow map holds both, and a map with no value field or an `UNKNOWN`-annotated key has no Arrow counterpart. A `LIST` group with no element, a `MAP` group with no key, or a `MAP` group whose key is `OPTIONAL` fails with `SchemaIncompatibleException` (consistent with [AVRO_BINDING.md](AVRO_BINDING.md#schema-mapping) and Arrow's non-nullable map key rule). The checks cover the output projection only: a filter may name any column. A `repeated` Variant group outside a `LIST` or `MAP` scaffold is refused by core when the column readers are built ([LOGICAL_TYPES.md](LOGICAL_TYPES.md#variant)), and the refusal surfaces from `build()` as core's.

**Variant.** A Variant group is served as stored, like any other group: `metadata` and `value`, and for a shredded Variant the `typed_value` subtree as its own struct, list or leaf. That is the storage layout of Arrow's Variant extension type; the type itself is not applied, as no extension type is, and Arrow C++ with extension inference off reads the group the same way. The reassembly of a shredded value into canonical bytes is the row path's (`VariantShredReassembler` works on the row reader's batch view), so a consumer that needs canonical bytes from a shredded file reads it through `RowReader.getVariant`.

`ArrowSchemaConverter.plan` resolves the projection through core's `ProjectedSchema`, exactly as `ColumnReaders` does, and walks the projected children of each group in request order, so a projection into a struct keeps the struct around the children it names and a name listed twice yields one field.

Tests: `ArrowSchemaConverterTest`, `ArrowBatchReaderTest` (hardwood-arrow).

## Schema mapping and plan

`ArrowSchemaConverter.plan(...)` is the single conversion entry point. It returns an `ArrowPlan`: the Arrow `Schema`, one tree of `Node`s per top-level field, and the flat list of dictionary-encoded leaf nodes. A node pairs its `Shape` (`LEAF`, `STRUCT`, `LIST`, `MAP`), its `Field`, for a leaf its `ScalarKind`, its position among the requested columns (the index `ColumnReaders.getColumnReader(int)` serves it at), its Arrow dictionary id and slot (`-1` when plain), and for a group the index of the layer, on the leaf readers beneath it, that carries the group's validity and offsets. Schema and plan are built in one traversal, so loading a batch never re-derives from the Arrow type how a node is filled.

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
| any | `UNKNOWN` | `Null` | `NULL` |

Groups:

| Parquet | Arrow | Nullable |
|---|---|---|
| unannotated group, `FILE` group, any other group that is neither list, map nor Variant | `Struct` of the projected children, in request order | when `OPTIONAL` |
| `LIST` group | `List` of the element `GroupNode.getListElement` finds, named after the element node | when `OPTIONAL`; the element by its own repetition |
| `MAP` group | `Map(keysSorted=false)` whose child is a struct named `entries` holding the key and the value | when `OPTIONAL`; `entries` and the key never, the value by its own repetition |
| `repeated` primitive or group outside a `LIST` or `MAP` scaffold | `List` of the field's own type, the element named after the field | never, and the element never |
| `VARIANT` group | `Struct` of `metadata`, `value` and, when shredded, `typed_value`, as stored | when `OPTIONAL` |

A `ScalarKind` names the fill, not the type: `DATE` and bare `INT32` are both a bulk copy of the reader's `int[]`. The `FIXED_BINARY` kind serves `FLOAT16` too, because the two little-endian bytes Parquet stores are Arrow's half-float layout. A node with a field id of zero or more carries it as `PARQUET:field_id` field metadata, as Arrow C++ records it, and a negative id is none, as Arrow C++ reads it. Two nodes carry none: the `entries` struct of a map, whatever id the `key_value` group has, and the synthetic element of a bare repeated primitive, whose id sits on the list alone, while the struct element of a bare repeated group keeps the group's id beside the list. The ids come from `SchemaNode.fieldId()`, which core populates from the footer's `SchemaElement`s. The file's key-value metadata becomes the schema's metadata, as Arrow C++ records it, except the `ARROW:schema` entry, which Arrow C++ consumes and drops and this module leaves unread.

**Layers.** A group's validity and offsets are a layer of the leaf readers beneath it ([COLUMN_READER.md](COLUMN_READER.md#batch-model-and-layers)), reported identically by every leaf the group encloses, so the plan reads them from one of them, the group's first projected leaf. Which layer a group owns follows the rule `NestedLevelComputer.walkLayers` applies per leaf, carried down the tree as the converter walks it: a node contributes a `REPEATED` layer when it is repeated outside a `LIST` or `MAP` scaffold, another when it is itself a `LIST` or `MAP`, and a `STRUCT` layer when it is an optional group outside a scaffold; a required group, and the repeated child of a `LIST` or `MAP` group (the synthetic wrapper, or the element itself in the legacy two-level encoding), contributes none. The derivation is checked at every leaf against `NestedLevelComputer.computeLayers`, and a difference fails the conversion as a defect, since it would misalign every group above the leaf.

**Rejections.** A `FIXED_LEN_BYTE_ARRAY` without a positive width fails through core's `FixedWidthValidator` (`SchemaIncompatibleException`), as do a `LIST` group with no element, a `MAP` group with no key, and a `MAP` group with an `OPTIONAL` key. A `DECIMAL` with precision above 76, a fixed-width `DECIMAL` declared over more bytes than the Arrow decimal of its precision holds (as Arrow C++ refuses it), a `MAP` with no value or an `UNKNOWN` key, an explicit dictionary column of an ineligible physical type, and any pairing of physical type and annotation the table does not list fail with `UnsupportedOperationException` naming the column. An unknown projected name or an explicit dictionary column not in the projection fails with `IllegalArgumentException`.

Tests: `ArrowSchemaConverterTest`, `ArrowBatchReaderTest`, `ArrowDictionaryTest` (hardwood-arrow).

## Dictionary encoding interop

`ColumnReader` exposes `getDictionaryIds()` and `getBinaryDictionary()` for `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` and `INT96` columns, with one identity-stable `BinaryDictionary` per row group and deferred view building ([COLUMN_READER.md](COLUMN_READER.md#dictionary-ids)). Calling `.dictionaryEncoding(true)` on `ArrowReaders.BatchReaderBuilder` selects every projected eligible column whose footer metadata uses `RLE_DICTIONARY` or `PLAIN_DICTIONARY` (or carries a `dictionaryPageOffset`); `.dictionaryColumns(...)` selects named leaf columns explicitly. Each selected field receives a `DictionaryEncoding(id, false, Int(32, true))` in `readSchema()`, which `ArrowReader.initialize()` turns into an `IntVector` in the root paired with a value vector in `dictionaries`. On each batch:

- When `getDictionaryIds()` is non-null and `getBinaryDictionary()` matches the already-loaded instance by identity (`==`), no dictionary batch is built or loaded; the loader only copies the `int[]` indices (bulk copy when all present, mapping `-1` null sentinels to `0` when nullable/parent-null). Deferred views in core are never materialized.
- When the dictionary changed (first batch or a new row group), `BatchLoader` builds a replacement `ArrowDictionaryBatch` (`isDelta = false`) directly from `BinaryDictionaryLayout` — two bulk copies (`chunkOffsets(0)` and `chunkBytes(0)`) for single-chunk `BINARY` (or chunk concatenation when a dictionary spans multiple chunks), one bulk copy (or chunk concatenation) for `FIXED_BINARY`, or one conversion pass over `dict.size()` entries for `DECIMAL_BYTES` and `INT96_TIMESTAMP`.
- When a batch falls back to plain pages (`getDictionaryIds() == null`), `BatchLoader` reuses the plain leaf loader to emit the batch's values as a replacement dictionary paired with identity indices `0..count-1` and clears the cached dictionary reference so the next dictionary batch reloads cleanly.
- If a non-nullable dictionary child sits inside an all-null `OPTIONAL` struct and its dictionary has zero entries, a 1-entry placeholder dictionary ensures placeholder index `0` stays in bounds.

Because Arrow IPC file format (`ArrowFileWriter`) writes each dictionary id only once, multi-row-group files with dictionary output enabled use `ArrowStreamWriter` (which emits replacement `ArrowDictionaryBatch` messages whenever a dictionary changes) or the C Data Interface (`Data.exportArrayStream`).

Tests: `ArrowDictionaryTest`, `ArrowCDataDuckDbTest` (hardwood-arrow).

## Loading a batch

`ArrowBatchReader.loadNextBatch` calls the base class's `prepareLoadNextBatch`, advances the `ColumnReaders`, and has `BatchLoader.load` build a `LoadedBatch` (all replacement `ArrowDictionaryBatch`es plus the `ArrowRecordBatch`): one `ArrowFieldNode` and the Arrow buffers of each node of the plan, in the depth-first order Arrow's `VectorLoader` consumes them, each buffer allocated at its exact size from the caller's allocator and filled once. Every replacement dictionary and the record batch are allocated and validated before `loadDictionary` or `loadRecordBatch` touches the reader's state.

The walk carries the number of items at each scope: the batch's records at the top, and below a list or map the entries its offsets end at. A struct contributes its validity (its layer's, or all-present for a required struct) and passes the count down unchanged; a list or map contributes its layer's validity and offsets (`count + 1` of them) and passes the last offset down; a map's `entries` struct is always present; a leaf contributes its validity and the data buffers its kind (or dictionary index type) names, and must hold exactly the count its scope says, which is where a leaf disagreeing with the offsets of its group's first leaf fails, before any buffer is filled with it.

**Non-nullable children beneath nullable structs.** In `ColumnReader`, a `REQUIRED` leaf, `LIST`, or `MAP` inside an `OPTIONAL` struct has `sourceValidity` unset on rows where the enclosing struct is null. In Arrow, a field with `nullable = false` must report `nullCount = 0` and an all-set validity bitmap; the enclosing struct's validity bitmap masks those rows. `BatchLoader` therefore separates `sourceValidity` (which guards data conversion and width/range checks) from `arrowValidity` (`field.isNullable() ? sourceValidity : Validity.NO_NULLS`), which sizes the Arrow node's null count and validity buffer.

| Kind | Buffers | Fill |
|---|---|---|
| `NULL` (`UNKNOWN`) | none | a node of `count` nulls, after checking that the batch holds no value; one that does fails the batch, as the row path refuses it |
| dictionary leaf | validity, indices | `int[]` dictionary indices bulk-copied (or `-1 -> 0` when nullable/parent-null); dictionary loaded once per row group via `BinaryDictionaryLayout` |
| `BOOLEAN` | validity, data | `boolean[]` packed directly into the off-heap buffer 8 bits at a time, with `hasNulls()` hoisted |
| `INT32`, `INT64`, `FLOAT`, `DOUBLE` | validity, data | one bulk copy of the typed array through a little-endian `nio` view of the buffer |
| `INT8`, `INT16`, `UINT8`, `UINT16` | validity, data | per present value, range-checked against the annotation, then narrowed directly into a little-endian view |
| `DECIMAL_INT32`, `DECIMAL_INT64` | validity, data | per present value, unscaled `int`/`long` and sign extension written directly as 16 little-endian bytes (no intermediate `long[]`) |
| `DECIMAL_BYTES` | validity, data | per present value (or once per dictionary entry per row group), big-endian payload reversed and sign-extended to 16 or 32 bytes; a `BYTE_ARRAY` payload longer than that is refused |
| `INT96_TIMESTAMP` | validity, data | per present value (or once per dictionary entry per row group), Julian day and nanoseconds of day to nanoseconds since the epoch, with overflow refused |
| `FIXED_BINARY` | validity, data | gathered per present value into a `ByteBuffer` view, or bulk-copied once per row group from `BinaryDictionaryLayout` when dictionary-encoded |
| `BINARY` | validity, offsets, data | see below |

**Validity.** The reader's bitmap is Arrow's: set bit means present, bit `i` of word `i / 64`, which written little-endian is bit `i` of byte `i / 8`. Full words bulk-copy via `LongBuffer.put`, tail bytes are written individually, and bits at or past `count` are cleared. `Validity.NO_NULLS` fills `-1L` words directly into the off-heap buffer with no intermediate heap `byte[]`. Allocating the all-set bitmap upfront keeps every allocation inside `BatchLoader.load`, avoiding `VectorLoader` releasing an existing validity buffer before a failing replacement allocation.

**Binary values.** `BatchLoader.views` measures the present values' views in one pass: their total length as a `long` and whether every present value starts where the previous one ended. A contiguous batch is one bulk copy (`ByteBuffer.put(int, byte[], int, int)`) and a running sum for the offsets; any other batch gathers value by value. Total length is checked before allocation: Arrow's `Binary` and `Utf8` carry 32-bit offsets, so a batch exceeding `Integer.MAX_VALUE` bytes fails with `IllegalStateException` naming the column and advising a smaller batch size.

**Positions no value is written at.** A data buffer filled per value is zeroed first, so null positions read as zero; a bulk-copied buffer carries whatever the reader's array held at null positions, which the format leaves unspecified.

**Failure.** `BatchLoader.load` closes every buffer and `ArrowDictionaryBatch` it allocated if any step throws, so a failed load leaves nothing of the batch allocated; the empty root's own buffers remain the root's and go with `close()`. `ArrowBatchReader` marks itself failed, closes `ColumnReaders`, and rethrows; a later `loadNextBatch` throws `IllegalStateException`. A value contradicting its annotation or stored under `UNKNOWN` raises `ParquetReadException`; an out-of-range `INT96` raises `UnsupportedOperationException`.

Tests: `ArrowBatchReaderTest`, `ArrowDictionaryTest`, `BatchLoaderTest`, `ArrowIpcRoundTripTest` (hardwood-arrow).

## Memory and lifecycle

The caller owns the `BufferAllocator`; the module never creates a `RootAllocator` and never closes the one it is given. Every batch is loaded into the one root the base class creates, and `VectorLoader` releases the previous batch's buffers as it loads the next (and replaces a dictionary vector's buffers only when its dictionary changes). A consumer that keeps a batch transfers it (`TransferPair`, `VectorUnloader`) and releases what it transferred. `close()` is `ArrowReader`'s: it releases the root and all dictionary vectors, then `closeReadSource` closes the `ColumnReaders`, which the reader owns; the `ParquetFileReader` stays the caller's. Close is idempotent, and `loadNextBatch` after it throws `IllegalStateException`. With the reader and everything transferred out of it closed, the allocator holds nothing from the module; every test asserts this on its allocator.

Through `arrow-c-data`, `Data.exportArrayStream` wraps the reader in an `ArrowArrayStream`, whose release by the consumer closes the reader. `ArrowCDataDuckDbTest` registers plain, dictionary-encoded (with plain fallback and cross-chunk dictionaries) and nested streams in DuckDB's JDBC driver and verifies aggregates against DuckDB's own `read_parquet`.

Tests: `ArrowBatchReaderTest`, `ArrowDictionaryTest`, `ArrowCDataDuckDbTest` (hardwood-arrow).

## Fixtures and the Arrow C++ oracle

The module checks in no fixture. `ArrowFixtures` writes its files under `arrow/target/arrow-fixtures/<pid>` once per test JVM through Hardwood's own `ColumnWriter`, and builds expected tables independently through Arrow's typed setters. `ArrowTestSupport` recursively validates Arrow structural invariants on every loaded batch: non-nullable fields (at every depth) hold zero nulls, offset buffers are monotonic and in bounds, and dictionary indices lie within `[0, dictSize)`.

Arrow C++'s own Parquet reader is the independent oracle, reached in-process through `arrow-dataset` (managed in `hardwood-test-bom`, a test-only dependency). `ArrowDatasetOracleTest` compares schema, nullability, null counts and values across every mapped type, nested shapes, legacy encodings, field ids and Variant fixtures.

Tests: `ArrowBatchReaderTest`, `ArrowDictionaryTest`, `ArrowDatasetOracleTest`, `ArrowIpcRoundTripTest` (hardwood-arrow).

## Boundaries

- **Shredded Variant values.** A shredded Variant is served as stored; its canonical bytes are not reassembled, and Arrow's Variant extension type is not applied.
- **Primitive dictionaries.** Dictionary output covers `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` and `INT96` columns, which `ColumnReader.getDictionaryIds()` exposes. Numeric dictionaries (`INT32`, `INT64`, `FLOAT`, `DOUBLE`) are decoded to typed arrays on core's decode thread and emit plain vectors.
- **`ARROW:schema`.** Not read: large offsets, the original zone of an unadjusted timestamp, extension types, the names of list elements and field metadata PyArrow recorded are not restored. The CLI's rendering of the entry is #1007.
- **View types.** `Utf8View` and `BinaryView` are not produced; dictionary-encoded columns can instead be read as Arrow dictionaries via `dictionaryEncoding(true)` / `dictionaryColumns(...)`.
- **CLI IPC output** and **Arrow-to-Parquet writing** are separate proposals under #153 (`hardwood-cli` ships as a GraalVM native image and stays free of Arrow Java's `Unsafe`/`java.nio` reflection requirements).
- **API stability.** `ArrowReaders` and `ArrowBatchReader` are `@Experimental`, as `ColumnReader` is.
- **Off-heap / zero copy.** Parquet pages on disk or in S3 off-heap `ByteBuffer`s are compressed and encoded, so they cannot be handed directly to Arrow without decoding. Decoded values in `ColumnReader` are heap arrays and Arrow Java vectors own off-heap memory, so every batch is one bulk copy per buffer (and for dictionary-encoded columns, one bulk copy of the dictionary per row group plus `int[]` indices per batch). Eliminating the heap-to-off-heap copy requires decoding directly into caller-provided off-heap buffers in core (#737).
