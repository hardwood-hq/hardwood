# Plan: primitive-column Arrow integration (`hardwood-arrow`)

Tracking issue: [#153](https://github.com/hardwood-hq/hardwood/issues/153).
Stage issues are to be agreed with the maintainer before filing.
Related work: #514 (logical-type decoders for external columnar consumers), #737
(adopting heap batch arrays), #1007 (`ARROW:schema` rendering), and #626 (64-bit offsets).

A new optional module, `hardwood-arrow`, reads flat primitive columns through
`ColumnReaders` and exposes batches as Arrow Java `VectorSchemaRoot`s through
`ArrowReader`. Core remains free of Arrow dependencies.

## Scope

**Primitive means a non-repeated primitive Parquet field directly below the file root.**
Both `REQUIRED` and `OPTIONAL` fields are supported, including the scalar logical
annotations in the mapping table below: strings, dates, times, timestamps, decimals,
unsigned integers and fixed-width binary values.

This plan does not produce structs, lists, maps, Variant, extension vectors or
dictionary-encoded Arrow fields. It does not flatten nested leaves into root columns.
A repeated primitive is a list in Parquet's data model and is also outside this scope.

The restriction applies to the output projection, not the whole file. A file containing
both `id` and a nested `address` can be read with a projection selecting only `id`.
Selecting `address`, `address.city`, a map value, or either child of a Variant fails
at `build()` with `UnsupportedOperationException` naming the unsupported path, before
column workers start. A reader requesting all columns rejects any such output field.
Predicates retain core's semantics; filter-only columns are not Arrow output fields.

### Relationship to #153

This is the issue's starting milestone: a primitive-column reader, schema conversion,
and a tested handoff to IPC and a C Data consumer. Completion does not claim all of
#153's exploratory integration points are implemented:

- Parquet dictionary encoding is accepted as input but materialized into plain vectors.
  Dictionary-preserving output is follow-up work.
- Heap arrays cannot be adopted as Arrow Java's allocator-owned off-heap buffers. The
  bridge copies values, with additional conversion where their layouts differ.
- S3 buffers hold encoded page bytes, not the decoded values required by Arrow vectors.
- CLI IPC output requires a separate native-image feasibility check and agreed scope.
- Arrow-to-Parquet writing is a separate proposal, not a prerequisite for this reader.

## Design

### Module and public surface

`hardwood-arrow` depends on `hardwood-core`, `org.apache.arrow:arrow-vector` and
`arrow-memory-core`, pinned to Arrow Java 19.0.0. The caller supplies a memory backend
(`arrow-memory-netty` or `arrow-memory-unsafe`); tests use `arrow-memory-unsafe`.
The module adds no `arrow-variant` dependency.

Document Arrow's required JVM opening:
`--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED`.
Append it to the existing test JVM arguments rather than replacing them. Keep plugin
versions in the parent POM's `pluginManagement`; Arrow's dependency version is a
module-local property, following `avro.version`. Add the artifact to `hardwood-bom`.

Public package `dev.hardwood.arrow`:

| Type | Role |
|---|---|
| `ArrowReaders` | `batchReader(reader, allocator)` for all columns; `buildBatchReader(reader, allocator)` for a builder with `projection`, `filter` and `batchSize` |
| `ArrowBatchReader` | Extends Arrow Java's `ArrowReader`; exposes the reusable root, `loadNextBatch()`, `getSchema()` and `close()` |

Schema conversion, the flat decode plan and vector loaders live in
`dev.hardwood.arrow.internal`. There is no nesting plan, group-layer derivation or
new core group-wise API in this milestone.

Resolve and validate the output projection before constructing `ColumnReaders`.
Forward the validated projection, filters and batch size to core. Preserve its
requested leaf order, including repeated requests, with an explicit source-reader
index on every plan entry. Multi-file reads retain core's lazy metadata loading and
schema validation; this adapter does not preflight every file's encodings.

`ArrowBatchReader` owns its `ColumnReaders` and root, but not the `ParquetFileReader`
or caller's allocator. It emits no Arrow dictionaries, so both `ArrowStreamWriter`
and `ArrowFileWriter` can write its plain vectors without dictionary replacement or
index remapping. Test those integrations rather than assuming interface compatibility
is sufficient. `arrow-c-data` is a test dependency for exporting an `ArrowArrayStream`
and querying it through the existing DuckDB JDBC test dependency.

### Schema conversion

`ArrowSchemaConverter.plan(FileSchema, ColumnProjection)` returns a flat plan with one
entry per output field. Each entry carries the source primitive, its source-reader
index, the Arrow `Field` and a conversion kind. Build schema and plan together, once;
loading a value does not repeat logical-type dispatch.

| Parquet physical type | Annotation | Arrow type / conversion |
|---|---|---|
| `BOOLEAN` | — | `Bool`; pack values into bits |
| `INT32` | —, `INT(32, signed)` | `Int32` |
| `INT32` | `INT(8/16, signed)` | `Int8` / `Int16`; checked narrowing |
| `INT32` | `INT(8/16/32, unsigned)` | `UInt8` / `UInt16` / `UInt32`; checked narrowing for 8/16, preserve bits for 32 |
| `INT32` | `DATE` | `Date32(DAY)` |
| `INT32` | `TIME(MILLIS)` | `Time32(MILLISECOND)` |
| `INT32`, `INT64` | `DECIMAL(p, s)` | `Decimal128(p, s)`; sign-extend to 16 little-endian bytes |
| `INT64` | —, `INT(64, signed)` | `Int64` |
| `INT64` | `INT(64, unsigned)` | `UInt64`; preserve bits |
| `INT64` | `TIME(MICROS/NANOS)` | `Time64(unit)` |
| `INT64` | `TIMESTAMP(unit, adjusted)` | `Timestamp(unit, "UTC")` when adjusted, `Timestamp(unit, null)` otherwise |
| `INT96` | — | `Timestamp(NANOSECOND, null)`; checked Julian-day/nanoseconds conversion |
| `FLOAT`, `DOUBLE` | — | `Float32`, `Float64` |
| `BYTE_ARRAY` | —, `ENUM`, `BSON`, `GEOMETRY`, `GEOGRAPHY` | `Binary` |
| `BYTE_ARRAY` | `STRING`, `JSON` | `Utf8` |
| `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` | `DECIMAL(p, s)` | `Decimal128` for p ≤ 38, otherwise `Decimal256`; convert big-endian two's complement to little-endian with checked width/sign extension |
| `FIXED_LEN_BYTE_ARRAY(n)` | —, `INTERVAL`, `TIMESTAMP` | `FixedSizeBinary(n)` |
| `FIXED_LEN_BYTE_ARRAY(16)` | `UUID` | `FixedSizeBinary(16)`, no extension type |
| `FIXED_LEN_BYTE_ARRAY(2)` | `FLOAT16` | `Float16` (`Float2Vector`) |
| any supported primitive | `NULL` | `Null` |

Field nullability follows `OPTIONAL` versus `REQUIRED`. Preserve declared Parquet field
ids as `PARQUET:field_id`, obtained from the footer's `FileMetaData.schema()` elements,
since `FileSchema` does not keep them.
`ARROW:schema` is not interpreted, and extension metadata is not copied onto storage
vectors. There is no schema-restoration option in this milestone.

The baseline is Arrow C++ 24's metadata-free conversion with extension inference
disabled. `INT96` has no inferred timezone and `ENUM` maps to binary. JSON, UUID and
geospatial columns retain their storage types without extensions. Core's accepted
`TIMESTAMP` annotation on fixed-length binary is a separate compatibility case:
test its stored bytes directly rather than promising PyArrow accepts that schema.

Reject invalid fixed widths through core's validator and decimal precision above 76
before constructing column readers. Validate conversion ranges before narrowing a
present value; reject an `INT96` value outside the chosen nanosecond range instead of
wrapping it. Conversion errors name the source column and do not publish a partial
batch. Null positions are not converted.

Reuse or extract allocation-free helpers from `LogicalTypeConverter` where needed.
Internal sharing does **not** complete #514: its external public decoder API and
columnar-consumer documentation remain separate work.

### Loading a batch

`loadNextBatch()` advances the owned `ColumnReaders`. Every supported output column
has one leaf slot per root record, so there are no layer offsets or ancestor validity
masks to translate.

- **Validity:** copy present-bit words into an Arrow validity buffer. For `NO_NULLS`,
  materialize an all-present bitmap: Arrow Java's vector getters require it even though
  the interchange format permits an omitted bitmap. Handle unused tail bits and empty
  batches explicitly.
- **Fixed-width values:** copy into the vector's off-heap buffer, preserving Arrow's
  little-endian layout; use a conversion loop only where the mapping requires it.
  Boolean values need bit packing. Fixed-size binary values are gathered by their views.
- **Variable-width values:** inspect the binary views, not the page encoding. Contiguous
  views allow a data-buffer bulk copy and rebased sentinel offsets; overlapping or
  out-of-order views require gathering. Null values contribute no bytes. An empty
  vector still has a zero offset sentinel.
- **Size checks:** sum the expanded binary payload using checked `long` arithmetic
  before allocation or filling offsets. A small dictionary-backed source buffer can
  expand past `Integer.MAX_VALUE`. Reject that output with a defined size error naming
  the column and advising a smaller batch size. This milestone does not split batches
  or switch their Arrow types dynamically. Exercise the sizing check without allocating
  gigabytes.
- **Publication:** set vector value counts and the root row count consistently after
  successful loading. On failure, release partially allocated output and stop the owned
  scan; the reader cannot continue from a partially consumed batch.

A single root is reused. Advancing or closing releases the reader's references to the
previous buffers. A consumer retaining data must transfer or retain it using Arrow's
ownership APIs and release those references itself. Retaining data must not make a
later load overwrite it. Exporting through the C Stream Interface also needs an
explicit recipe for stream release, reader ownership and allocator lifetime.

The adapter never creates or closes a `RootAllocator`. Allocator-limit failures,
conversion failures, early close and end-of-stream are part of the lifecycle tests.
After all reader and consumer references are closed, the test allocator must hold zero
bytes; intentionally retained consumer buffers are not reader leaks.

## Stages

| # | Stage | Depends on | Gate |
|---|---|---|---|
| 1 | **Module, scope validation and schema conversion.** POM/BOM/module name, JVM flags, flat plan, primitive mapping table, nested/repeated rejection tests, initial design and usage documentation. | — | Supported schemas match the explicit oracle profile; unsupported output is rejected before workers start. |
| 2 | **Physical primitive batches.** Boolean, int, long, float, double, variable/fixed binary; validity, projection, filtering and multi-file reads. Include plain IPC stream/file round trips and an early C Data/DuckDB smoke test. Reject logical conversions not yet implemented rather than exposing physical values under the wrong Arrow type. | 1 | Value parity, reordered/repeated projection coverage and clean allocator on normal/error paths. |
| 3 | **Scalar logical conversions.** Narrow/unsigned integers, temporal types, decimals over all carriers, `INT96`, `FLOAT16`, UUID storage and null type. Shared internal conversion helpers where justified. | 2 | Mapping-table coverage, range failures and exact exception messages. |
| 4 | **Interoperability and performance gate.** Complete the supported scalar IPC matrix, exercise supported C Data types through DuckDB, test retention across batches and dictionary changes in the input, and benchmark against a raw `ColumnReaders` scan in `performance-testing`. Finish documentation. | 3 | End-to-end value parity and measured copy/conversion cost by scalar kind. |

Order: 1 → 2 → 3 → 4. No stage depends on nested readers, dictionary-preserving output,
`ARROW:schema` restoration, CLI native-image support or a Parquet writer adapter.

## Testing

- **Fixtures:** extend `tools/simple-datagen.py` using the pinned PyArrow environment.
  Maintain an explicit supported-fixture list, not a promise to read every core fixture.
  Generate metadata-free baseline Parquet fixtures with `store_schema=False`; read the
  reference with `arrow_extensions_enabled=False` and no dictionary-output request.
  Store their IPC streams under `arrow/src/test/resources/`. Do not strip arbitrary
  types or metadata from comparison results to make them pass.
- **Logical mapping:** compare schema and values for each supported table entry. Keep
  core-specific fixed-binary timestamp acceptance and out-of-range conversion tests
  separate from the PyArrow oracle. Test that `ARROW:schema` does not change this
  milestone's output, using paired files with the same physical values and schema.
- **Projection boundary:** reject structs, lists, maps, Variant, nested primitive paths
  and bare repeated primitives, including a required struct whose leaf has no layers.
  Accept a scalar-only projection from the same mixed file. Check rejection occurs
  before workers start, and preserve core behavior for filters on unselected columns.
- **Values:** all-present, all-null and mixed validity; empty files, short final batches,
  null/empty strings, numeric limits, temporal ranges and signed decimal byte layouts.
  Check Java getters and null counts as well as IPC output. Compare across different
  batch boundaries with `VectorEqualsVisitor` without losing null/value distinctions.
- **Dictionary input:** plain, dictionary-only and dictionary-to-plain fallback pages;
  changed/reversed dictionaries between row groups and files; absent encoding stats.
  Output is always plain, including in IPC files. No footer eligibility gate is needed.
- **Lifecycle:** close before first load, close mid-read, repeated close, exhaustion,
  retained buffers across advance/close, allocator limits and conversion failure.
  Release the C stream and all consumer-owned buffers before asserting zero allocation.
- **Benchmarks:** flat fixed-width columns, plain strings, dictionary-backed strings and
  conversion-heavy scalar columns. Record time and heap/off-heap allocation against
  the raw scan; use PyArrow on the same data as a separate external reference. Do not
  assume copying is negligible relative to decoding.

## Documentation and completion

Stage 1 adds `_designs/ARROW_BINDING.md`, its node and table row in `ARCHITECTURE.md`,
`docs/content/how-to/arrow.md`, navigation/index entries and the public package entry
in `reference/packages.md`. Each stage updates usage docs with the public API it adds.
Document the scalar projection boundary, mapping table, JVM flag, memory backend,
allocator ownership, reusable-root lifecycle, ignored `ARROW:schema`, plain dictionary
output and size-limit errors. Stage 2 adds the IPC and C Data usage recipes.

Add a primitive-column Arrow milestone under `ROADMAP.md` Phase 11 and tick stages as
they ship. Once stage 4 completes, fold the end state into the design document and
remove this plan. Record the reduced milestone and outstanding explorations on #153
rather than closing the issue as if every integration point were implemented.

## Follow-ups, not implementation stages

- **Nested output:** structs, lists, maps, bare repeated fields and Variant. Requires a
  group-oriented plan, structural projection policy and compatible extension handling.
- **Dictionary output:** requires supported-type and missing-statistics policies, plus
  a stream-versus-file contract for changing dictionaries. It is not required to read
  dictionary-encoded Parquet into the plain scalar vectors in this plan.
- **Schema restoration and extension types:** `ARROW:schema`, large-offset layouts,
  original timezone metadata and canonical extensions. #1007 remains separate CLI work.
- **View vectors:** may avoid expanding repeated values, but still require copying
  source heap bytes into off-heap storage and constructing views. Benchmark that tradeoff
  before adding an option.
- **CLI IPC:** first test native-image construction and runtime conversion with a small
  scalar fixture. Agree a separate CLI stage only after feasibility is established;
  this plan does not introduce a hand-written IPC implementation as a fallback.
- **Arrow-to-Parquet:** a separate writer plan, including dictionary inputs, schema
  compatibility and validity normalization. Not part of #153's reader milestone.
