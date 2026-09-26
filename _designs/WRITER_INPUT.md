# Writer input

How values enter the writer: the schema builder and which schema shapes the writer can produce, the `ColumnBatch` contract (column-by-column input, nulls, nested layers and the rules a batch must satisfy), the shredding of that input into repetition and definition levels, which logical-type annotations are legal on which physical types and the value ranges they impose, and the row-oriented `RowWriter` layered over the columnar path. The write model, `OutputFile`, row-group lifecycle and threading are in [WRITER.md](WRITER.md); page and chunk layout, dictionary selection, codecs and statistics are in [WRITER_ENCODING.md](WRITER_ENCODING.md); the interop gate and coverage assertion are in [WRITER_VALIDATION.md](WRITER_VALIDATION.md). The annotation model, including how an annotation is emitted as both `LogicalType` and `converted_type`, is in [LOGICAL_TYPES.md](LOGICAL_TYPES.md); the read-side inverse of shredding is in [NESTED_DECODE.md](NESTED_DECODE.md). User-facing semantics are in [docs/content/how-to/write-column-by-column.md](../docs/content/how-to/write-column-by-column.md), [docs/content/how-to/write-row-by-row.md](../docs/content/how-to/write-row-by-row.md) and [docs/content/reference/writer.md](../docs/content/reference/writer.md).

## Schema construction

The writer reuses the immutable `FileSchema` / `SchemaNode` model the reader builds, so a file it writes is read back through the same model. `FileSchema.Builder` constructs that model programmatically: `build()` flattens the declared tree into the `SchemaElement` list the footer carries and parses it back through `FileSchema.fromSchemaElements`, so the max definition and repetition levels, the leaf-column indices and the column paths are computed by the reader's own code, and a declared schema is exactly the schema a reader of the file sees.

The builder emits the **canonical** physical layout only: a `LIST` expands to `group (LIST) { repeated group list { element } }`, a `MAP` to `group (MAP) { repeated group key_value { key, value } }`. The builder verbs are `struct` / `list` / `map`, not a bare `group`. In the schema model `group` is the abstract super-type, a `SchemaNode.GroupNode` carrying an annotation (`LIST` / `MAP` / none) from which the reader resolves the concrete shape. Construction cannot leave that abstract: the concrete shape dictates the physical bytes emitted, so there is no byte layout for an unspecialized group to build. The builder offers only the concrete shapes, and they share the `struct` / `list` / `map` vocabulary with the batch setters.

| Verb | Declares |
|---|---|
| `addColumn(name, type, repetition[, typeLength][, logicalType])` | a primitive leaf |
| `struct(name, repetition, filler)` | a plain group; children declared on a `StructBuilder` |
| `list(name, repetition, element)` | a `LIST`; the element declared on an `ElementBuilder` via `primitive`, `struct`, `list` or `map` |
| `map(name, repetition, keyType[, keyTypeLength][, keyLogicalType], value)` | a `MAP` with a `REQUIRED` primitive key; the value declared like a list element |

Declaration rules, all enforced where the field is declared:

- `repetition` is `REQUIRED` or `OPTIONAL`. `REPEATED` is rejected on every verb: repetition is what `list` and `map` express.
- A `typeLength` is required and positive for `FIXED_LEN_BYTE_ARRAY` and rejected for every other type. A map key is built through the same `leaf` helper as any other primitive, so its type length and annotation are validated identically.
- A `struct` needs at least one field, a `list` its element, a `map` its value; `build()` rejects a schema with no fields.
- An annotation rides on the leaf and is checked against the physical type and type length by `LogicalTypeValidator` (see [Physical × logical legality](#physical--logical-legality)). Groups take no logical-type parameter: `LIST` and `MAP` groups are annotated by their verbs, and a plain `struct` carries no annotation.
- `build()` also checks every leaf against `LeafAnnotation.dropFault`, the rule by which the reader drops an annotation its physical type cannot carry, so a declared schema never carries an annotation the reader of its own file would discard. Untested.

Because the scaffolding is named canonically (`list` / `element`, `key_value` / `key` / `value`), the leaf and layer paths the batch contract addresses are fully determined by the schema shape.

### Producible shapes

A schema the caller did not build, one read from an existing file through `FileSchema.fromSchemaElements`, may carry shapes the builder never emits. `ParquetFileWriter.create` settles producibility once, through `WriterSchemaShape`, before the destination is opened and before either write view exists, so an unproducible schema is one rejection at one moment with one wording, and never leaves a file at the destination.

- **At least one column.** A schema with no leaf has no column chunk to write. Hardwood reads such a file (as a relation of no rows) and declines to produce one.
- **No `INT96`.** The type is deprecated and is not written.
- **Repetition is producible exactly where a `LIST` or `MAP` annotation accounts for it.** The annotated group is what gives the shredder a repetition layer and `ColumnBatch` a path to address its entry offsets under. An annotated group is `REQUIRED` or `OPTIONAL` and holds exactly one `REPEATED` field, which for a `MAP` is a group of `key` and `value`. Both legacy two-level lists satisfy that and are written: `LIST { repeated element }`, whose entry is the element, and `LIST { repeated group element { … } }`, whose entry is an element struct.

Everything else repeated is refused with `UnsupportedOperationException`:

- a `repeated` leaf or group with no annotated parent, because nothing could supply its entry offsets and shredding it would emit one entry per record, every value its own one-element list;
- an annotated group that repeats on its own behalf, holds more than its entry, holds an entry that does not repeat, or (for a `MAP`) holds a leaf entry, because the shredder derives one layer from the annotation while the schema declares a different number of repetition levels, and the file's levels and schema then disagree.

A nullable struct enclosing a `LIST` or `MAP` is producible and is not a schema rule. What that shape adds is a rule about the batch, enforced where the masks that decide it exist (see [Batch validation](#batch-validation)). Rules about *addressing* a shape rather than producing it belong to the row layer alone (see [One API per file, and row-layer shapes](#one-api-per-file-and-row-layer-shapes)).

Tests: `FileSchemaFlattenTest`, `WriterSchemaShapeTest`, `WriterNestedRoundTripTest`.

## Column batches

Data arrives as `ColumnBatch` objects, an aligned slice carrying one typed array per leaf column. `ColumnWriter.writeBatch(Consumer<ColumnBatch>)` takes a filler: the writer creates the batch bound to the schema, passes it to the filler, then submits it, so there is no separate build or submit step to forget. Because the batch is schema-bound, every identifier is validated as values are added, and every rule below fails at fill time rather than at write time:

| Rule | Failure |
|---|---|
| A column is addressed by leaf-column index or by name / dotted path | unknown name: `IllegalArgumentException`; index outside `[0, columnCount)`: `IndexOutOfBoundsException` |
| The setter fits the column's physical type | `IllegalArgumentException` naming both types |
| Each column, struct, list or map is set at most once, by either index or name | `IllegalArgumentException` |
| Every leaf column is set | `IllegalArgumentException` at submit (`completedSources`) |
| Every flat leaf (max repetition level `0`) has the same length, the batch's row count | ragged batch: `IllegalArgumentException` |
| A binary column holds no Java `null` at a present row; a `FIXED_LEN_BYTE_ARRAY` value is exactly the declared length | `IllegalArgumentException` naming the row |
| A value lies in the range its annotation declares | see [Annotation ranges](#annotation-ranges) |

A leaf under a `LIST` or `MAP` is exempt from the row-count agreement: its array holds the concatenated entries of every record, and its length is what the offsets account for. The record count of a nested batch is derived and cross-checked by the shredder when it binds the batch (see [Batch validation](#batch-validation)).

Arrays are referenced, not copied, and must not be mutated until the batch has been written. The batch is single-use: `writeBatch` marks it consumed, so a filler that stashes a reference and mutates it after `writeBatch` returns fails loudly. `writeBatch` consumes its sources synchronously: the batch is shredded, encoded and buffered within the call, which is what lets a caller, and the row layer, refill the arrays as soon as it returns. The batch is an arrival unit only and leaves no trace in the file; how its records are banded into row groups is in [WRITER.md](WRITER.md).

The columnar API takes physical values and converts nothing: a `STRING` column is written through `bytes(...)` as UTF-8, a `DATE` column through `ints(...)` as epoch days. The bytes of a binary column are written as given; the writer does not check UTF-8 validity or the well-formedness of `JSON`, `BSON`, `VARIANT`, `GEOMETRY` or `GEOGRAPHY` payloads.

### Null representation

An `OPTIONAL` column carries its nulls as a `Validity`, the same type the reader returns from `getLeafValidity()`, living in the neutral `dev.hardwood` package as shared read/write vocabulary. Because `Validity` is an interface, the caller picks the representation through its factory: `NO_NULLS` (a singleton), `of(long[])` for a packed present-bitmap, `ofNulls(boolean[])` to bridge a plain mask. The writer consumes any of them the same way (`isNull`, `nextNull(from, end)`), so a new representation is a drop-in with no change to the write API. The polarity is null-centric (`Validity.isNull`), matching the reader, so a value read back as null is written by marking that row null.

The values array is full length, one slot per row, and the entry at a null row is ignored: it is never encoded and never checked. The mask-less setter is the all-present form for both `REQUIRED` and `OPTIONAL` columns. A `boolean[]` overload is convenience sugar over `Validity.ofNulls` (`nulls[i] == true` ⇒ null); it is the only null form whose length is validated against the values, since a `Validity` has no intrinsic length. A null mask is rejected on a `REQUIRED` column. `Validity` and every overload taking it are `@Experimental`.

How a mask becomes a definition-level stream is in [Shredding](#shredding).

### Value sources

Behind the public setters, each column's array sits behind a bulk value-source seam: `ColumnSource` carries `size()`, all the shredder needs, and a typed sub-interface per storage family (`IntColumnSource`, `LongColumnSource`, `FloatColumnSource`, `DoubleColumnSource`, `BooleanColumnSource`, `BinaryColumnSource`) carries a `copyInto` that fills a reused page-sized primitive buffer. `BYTE_ARRAY` and `FIXED_LEN_BYTE_ARRAY` share `BinaryColumnSource`. Encoders, statistics and dictionary building consume page-sized ranges through this seam, so no value is boxed and intermediate memory stays bounded. The primitive-array setters are sugar over the seam, so a public source SPI over a caller's own container would be additive and would not change the `writeBatch` signature.

Tests: `WriterBatchContractTest`, `WriterRoundTripTest`, `WriterFixedWidthTypeRoundTripTest`, `WriterVariableWidthTypeRoundTripTest`.

## Nested input and shredding

The reader takes level streams off disk and reconstructs a per-layer view (`getLayerValidity`, `getLayerOffsets`, `getLeafValidity`, leaf value arrays). The writer takes that same per-layer view from the caller and computes the level streams. The two contracts are mirror images, so a column written at a given path reads back at the same path with the same values and nulls.

### Path addressing and layers

The reader addresses a nested leaf by its **full physical path**, scaffolding included: a `LIST`-annotated `int_list` column is read at `int_list.list.element`. The write contract uses the same physical paths, so a column written at `people.list.element.name` reads back at `people.list.element.name`. A path segment is a schema field name; the `LIST` and `MAP` scaffolding contributes its literal segments. A layer's path is always a prefix of the leaf paths beneath it.

The schema chain collapses into **layers**, one per `OPTIONAL` plain group (`STRUCT`) and one per `LIST`/`MAP`-annotated group (`REPEATED`); `REQUIRED` plain groups and the repeated scaffolding inside a `LIST`/`MAP` contribute no layer, only definition and repetition depth. This is the reader's layer rule ([COLUMN_READER.md](COLUMN_READER.md)), and the write contract addresses exactly that set, plus the leaves:

| Facet | Keyed on | Setter | Mandatory | Reader counterpart |
|---|---|---|---|---|
| Leaf values | the leaf's full path, or its column index | `ints`, `longs`, …, `fixed` | values yes; `Validity` optional | leaf value array + `getLeafValidity` |
| `STRUCT` layer | the `OPTIONAL` group's path | `struct(path, validity)` | no; omitted ⇒ all present | `getLayerValidity(k)`, kind `STRUCT` |
| `LIST` layer | the annotated group's path | `list(path, offsets[, validity])` | offsets yes | `getLayerValidity(k)` + `getLayerOffsets(k)`, kind `REPEATED` |
| `MAP` layer | the annotated group's path | `map(path, offsets[, validity])` | offsets yes | as `LIST` |

A `LIST` is physically two nodes, an outer group and an inner `repeated` one, which the reader presents as one `REPEATED` layer carrying both the list-itself-null validity and the entry offsets. One `list(...)` call keyed on the outer group's path supplies both facets; the inner node is never addressed. `map` shares the `list` storage and validates that the node is `MAP`-annotated; its `key` and `value` leaves share the one offsets array. `list` on a `MAP`, `map` on a `LIST`, `struct` on anything but an `OPTIONAL` plain group, and a layer `Validity` on a `REQUIRED` list or map are rejected eagerly. The layer setters take `Validity` only, with no `boolean[]` overload.

### Leaf slots

A `REPEATED` layer re-indexes what lies beneath it: the leaf array of a list's element holds one slot per entry that exists, the concatenated entries of every record. A null or empty list contributes no entries and so no slots; the shredder synthesizes its phantom from the offsets and validity. A `STRUCT` layer does not re-index: every leaf beneath an `OPTIONAL` struct keeps one slot per struct instance, absent instances included, and the value at a slot beneath an absent instance is ignored. This matches the reader, whose leaf array also keeps a slot, marked absent in `getLeafValidity()`, beneath an absent struct.

### Offsets, null and empty

A `REPEATED` layer's offsets follow the reader's convention: length `parentCount + 1`, `offsets[i+1] - offsets[i]` the number of entries in the `i`-th parent scope, `offsets[parentCount]` the number of items in the layer below. A zero delta is an **empty** list or map; the layer's `Validity` carries the **null** bit. Both are expressible independently, so empty and null never collide. Offsets cannot be defaulted, since list boundaries are not inferable, so every repeated layer's offsets are mandatory; every `Validity` is optional and means all-present when omitted.

### Batch validation

`RecordShredder.bind` validates the batch and derives its record count. For each leaf it walks the leaf's layers from leaf to root, starting from the leaf's value count:

- A `REPEATED` layer's offsets must start at `0`, be non-decreasing, and end at the current count; the count then becomes `offsets.length - 1`, the layer's parent count.
- A `STRUCT` layer leaves the count unchanged, nullable or not.
- A **null list or map** (its `Validity` marks index `i` null) must carry a zero delta at `i`. The shredder takes the null branch and never reads those offsets, so entries it spanned would be dropped from the file without error.
- An **absent struct** directly above a `REPEATED` layer (any `STRUCT` layer between that layer and the next `REPEATED` one above it) must also leave a zero delta at every index it marks absent. The shredder stops at the absent struct and never descends into the offsets below it, so the same silent loss arrives through the struct's `Validity` instead of the list's, and is rejected the same way at the same point. A struct above the next `REPEATED` layer is checked against that layer's offsets in its turn.

Column 0 sets the record count; every other column must imply the same one, so a short or long column, flat or nested, is rejected naming the column. Because the count is threaded through the chain, `offsets[count(k)] == count(k+1)` holds for every layer by construction. A `Validity`'s length is not checked (it has none); only its null positions within the offsets' range are examined.

### Shredding

`RecordShredder` turns the per-layer input into each leaf's repetition and definition levels. It runs per leaf column, walking that leaf's layers, and is the write-side inverse of the reader's `NestedLevelComputer`.

**Levels from the schema.** Along a leaf's physical path, each `OPTIONAL` or `REPEATED` node adds one to the max definition level and each `REPEATED` node one to the max repetition level, the values `FileSchema` stores on `ColumnSchema`. Each `REPEATED` layer is assigned a repetition depth equal to the number of `REPEATED` layers up to and including it; that is the level emitted when it repeats.

**Emitting entries.** The shredder produces one `(rep, def)` entry per leaf slot, **including phantom slots** for null and empty ancestors; the page header's `num_values` counts all of them. Descending a record driven by the caller's validity and offsets:

- a null `STRUCT` instance emits one phantom at the definition level of its present ancestors, excluding itself;
- a null list or map emits one phantom before the outer group's contribution;
- a present but **empty** list or map counts the outer group as defined and the inner `repeated` node as not, emitting one phantom;
- a null leaf counts every ancestor as defined but not itself: `def = maxDefinitionLevel - 1`;
- a present leaf emits `def = maxDefinitionLevel` with its value.

The first entry of a list scope inherits the repetition level of the enclosing scope, and each later entry carries that scope's own depth. A value therefore repeats at the depth of the innermost list it extends, and takes level `0` only when it opens a record. Empty and null scopes each emit exactly one phantom, so the record boundary survives on disk and the reader can distinguish an empty list, a null list, and a present list of nulls.

The shredder is value-type-agnostic. Its `LevelSink` receives `(repetitionLevel, definitionLevel, valueIndex)`, where `valueIndex` is the position of a present value in the column's source and a negative index marks an absent slot; the per-type value writer behind the column chunk reads the typed value itself. Present positions arrive in increasing source order, so the typed read stays a forward-only bulk copy, and the nested recursion is written once for every type. Shredding streams: a record range is shredded on demand into the column chunk's level and value stores, and `leafRange` composes one offsets lookup per repeated layer to map a record range to its leaf range without walking records. How the level streams are framed in a page is in [WRITER_ENCODING.md](WRITER_ENCODING.md).

**Worked trace.** Schema `m: [[int?]]`, canonical layout, `maxDef = 5`, `maxRep = 2`:

```
optional group m (LIST)            def +1
  repeated group list              def +1, rep depth 1
    optional group element (LIST)  def +1
      repeated group list          def +1, rep depth 2
        optional int32 element     def +1
```

Five records `[[1,2],[3]]`, `[]`, `null`, `[[]]`, `[null]`, supplied as:

- `list("m", [0,2,2,2,3,4], nulls at 2)`: outer offsets over inner lists; record 2 is a null outer list.
- `list("m.list.element", [0,2,3,3,3], nulls at 3)`: inner offsets over ints; the fourth inner list (record 4's) is null.
- `ints("m.list.element.list.element", [1,2,3])`: three leaves, all present.

| Record | Entries `(rep, def, value)` |
|---|---|
| `[[1,2],[3]]` | `(0,5,1) (2,5,2) (1,5,3)` |
| `[]` | `(0,1,·)`: outer list present but empty |
| `null` | `(0,0,·)`: outer list null |
| `[[]]` | `(0,3,·)`: inner list present but empty |
| `[null]` | `(0,2,·)`: inner list null |

Seven entries on disk, three values. Reading the page back reconstructs the five records.

Tests: `WriterNestedRoundTripTest`, `WriterBatchContractTest`, `ColumnarNestedCopyTest`, `NestedListStructFixtureCopyTest`, `WriterNestedInteropTest` (parquet-testing-runner).

## Physical × logical legality

An annotation is legal only on the physical types parquet-format permits. `LogicalTypeValidator` (`internal.schema`) is the single table, applied by `FileSchema.Builder` when a leaf or map key is declared, so an illegal pairing never reaches a file, where no reader could honour it. The reader applies the same pairings leniently, dropping an annotation that fails them and reading the column as its physical type (see [EXCEPTION_MODEL.md](EXCEPTION_MODEL.md#an-annotation-the-reader-cannot-use-is-dropped-not-raised)); only the writer refuses.

| Logical type | Physical type | Further constraints |
|---|---|---|
| `STRING`, `ENUM`, `JSON`, `BSON`, `GEOMETRY`, `GEOGRAPHY` | `BYTE_ARRAY` | — |
| `UUID` | `FIXED_LEN_BYTE_ARRAY` | type length 16 |
| `FLOAT16` | `FIXED_LEN_BYTE_ARRAY` | type length 2 |
| `INTERVAL` | `FIXED_LEN_BYTE_ARRAY` | type length 12 |
| `DATE` | `INT32` | — |
| `INT(8\|16\|32, *)` | `INT32` | — |
| `INT(64, *)` | `INT64` | — |
| `TIME(*, MILLIS)` | `INT32` | — |
| `TIME(*, MICROS\|NANOS)` | `INT64` | — |
| `TIMESTAMP` | `INT64`, or `FIXED_LEN_BYTE_ARRAY` | every unit; type length 12 |
| `DECIMAL` | `INT32` | `precision <= 9` |
| `DECIMAL` | `INT64` | `precision <= 18` |
| `DECIMAL` | `FIXED_LEN_BYTE_ARRAY` | `precision <= floor(log10(2^(8·len − 1) − 1))` |
| `DECIMAL` | `BYTE_ARRAY` | — |
| `UNKNOWN` | any | not `REQUIRED` |
| `LIST`, `MAP` | — | rejected on a primitive; declared by the `list` / `map` verbs |
| `VARIANT` | — | rejected; the writer does not build the Variant group |

`DECIMAL`'s `scale <= precision` and positive precision are enforced by the `LogicalType.DecimalType` record itself. `UNKNOWN` is the one annotation constrained by repetition rather than by physical type: it describes a column whose every value is null, which a `REQUIRED` column can never be, and the reader throws on a non-null value under it.

Emitting a declared annotation as both the `LogicalType` union and the legacy `converted_type` / `scale` / `precision` is part of the annotation model in [LOGICAL_TYPES.md](LOGICAL_TYPES.md). How an annotation changes the column's statistics order is in [WRITER_ENCODING.md](WRITER_ENCODING.md).

Tests: `LogicalTypeValidatorTest`, `WriterLogicalTypeRoundTripTest`, `WriterFlba12TimestampTest`.

## Annotation ranges

An annotation narrows what its physical type may hold: an `INT32` annotated `INT(8)` carries 256 of its 2^32 bit patterns, one annotated `DECIMAL(9, 2)` the unscaled values of at most nine digits. A consumer reads the column through the annotation, so a `uint8` reader returns 44 for a stored 300, and the `min`/`max` in the chunk's statistics would describe values the annotation says cannot exist. Neither write API produces a value outside the range its column's annotation declares. The check is a property of the column, resolved from its declared type, and both APIs apply the same one.

The bound is on the *stored* value, the physical value that lands in the page, which is what both APIs' physical setters take.

| Column | Bound on the stored value |
|---|---|
| `INT32` annotated `INT(8)` / `INT(16)` | `[-2^(w-1), 2^(w-1) - 1]` |
| `INT32` annotated `UINT_8` / `UINT_16` | `[0, 2^w - 1]` |
| `INT32` / `INT64` annotated `TIME(unit)` | `[0, one day of unit)` |
| `INT32` / `INT64` annotated `DECIMAL(p, s)` | `[-(10^p - 1), 10^p - 1]` |
| `BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY` annotated `DECIMAL(p, s)` | a non-empty two's complement value of magnitude at most `10^p - 1` |
| any column annotated `UNKNOWN` | no value at all: every row is null |
| anything else | none |

`DECIMAL` bounds the unscaled value; the scale is a fixed divisor the annotation carries, so `p` digits of unscaled value is the whole constraint.

Deliberately unbounded:

- **`UINT_32`, `UINT_64`, `INT(32)`, `INT(64)`.** Every bit pattern of the underlying `int` / `long` is a valid value of the column, and spelling an unsigned one above `Integer.MAX_VALUE` / `Long.MAX_VALUE` as a negative is the only way to reach it, which is also how the reader returns it. A bound here would cost expressiveness rather than buy conformance.
- **`DATE` and `TIMESTAMP`.** parquet-format states that every `int64` represents a valid timestamp, and every `int32` day count is a date `LocalDate` can hold.
- **Fixed byte lengths.** `FIXED_LEN_BYTE_ARRAY` values are checked against the declared type length on both APIs independently of any annotation.

**`TIME`** is the one annotation whose bound the format states only by implication: it defines the value as the time elapsed after midnight without naming a range. The bound is enforced because the alternative is a file this project's own reader cannot read through the annotation: `LogicalTypeConverter` materializes a `LocalTime`, which rejects a value of a day or more. A full day is outside the range, matching `LocalTime` and rejecting the `24:00:00` spelling some producers emit.

Two annotations admit fewer values than a range expresses:

- **An empty binary `DECIMAL` value.** Two's complement has no zero-byte encoding, so an empty value denotes no unscaled value and the reader raises `Zero length BigInteger` on it. It is rejected wherever it is handed over.
- **`UNKNOWN`.** The column holds only nulls. The validator already refuses a `REQUIRED` `UNKNOWN` column; on an `OPTIONAL` one every row must be null, so `ColumnBatch` requires a null mask marking every row, and the row layer refuses every value setter.

### Where the check lives

`LogicalTypeValueRange` (`internal.writer`) resolves a column's bound once from its `ColumnSchema` and answers `contains(long)` for an integral column and `containsUnscaled(byte[])` for a binary `DECIMAL`. An unbounded column resolves to a shared instance reporting `isBounded() == false`, so the caller skips the per-value scan entirely. `UNKNOWN` has no value predicate to answer; it reports `holdsNoValue()`, and each API enforces it on the shape it holds, `ColumnBatch` on the column's nulls and the row layer by refusing the setters. The derivation lives in one place, which keeps the two APIs from disagreeing about which values a column may hold.

- **`ColumnBatch`** takes the ranges `ParquetFileWriter` resolves once per file and scans the values a setter is handed, skipping the rows the column's `Validity` marks null, in the same pass that checks fixed byte lengths. The rejection names the column and the row (`Column 3 (zip) has value 300 at row 17, out of range for a UINT_8 column`).
- **`RowWriter`** resolves the range per leaf when its plan is built and checks in the setter, before anything is staged. The rejection names the field the caller set. The batch scan then runs again over the staged values as a backstop.

A slot an absent ancestor makes unreachable is not a value either API checks. The row layer marks that slot null when the leaf is `OPTIONAL`; only a `REQUIRED` leaf, which has no null bit to set, keeps a placeholder, and its placeholder is a value the column can hold: the declared width for a `FIXED_LEN_BYTE_ARRAY` and a decodable zero (`{0}`) under a binary `DECIMAL`.

An unannotated or unbounded column pays one `isBounded()` test per setter call and nothing per value. A bounded integral column pays two compares per value. A binary `DECIMAL` constructs a `BigInteger` only for a value long enough to possibly exceed the precision: a value of `L` bytes cannot exceed `10^p - 1` when `2^(8L-1) <= 10^p - 1`, so the longest always-safe length is derived once per column from the bound's bit length.

Tests: `WriterAnnotationRangeTest`, `WriterReaderSymmetryTest`.

## RowWriter

`ColumnWriter.writeBatch` suits a caller that already holds columns. A caller that holds records would otherwise allocate an array per column, track a fill cursor, maintain a null mask per optional column, and spell the synthetic `list.element` and `key_value` path segments. The row layer closes that gap, mirroring the read side, where `ParquetFileReader` offers both `columnReader()` (performance-first) and `rowReader()` (ergonomics-first) over the same file.

### Shape

`ParquetFileWriter.rowWriter()` returns a `RowWriter` beside the `ColumnWriter` that `columnWriter()` returns. `writeRow(Consumer<StructBuilder>)` follows the `writeBatch` idiom: the layer creates the builder bound to the schema, hands it to the filler, then stages the record, so there is no build or submit step to forget and no builder instance that outlives the record it describes.

A row is a struct, so the top-level builder is a `StructBuilder`, the same type used for a nested struct, as `RowReader extends StructAccessor` on the read side. The public types in `dev.hardwood.writer`, all `@Experimental`:

| Type | Role |
|---|---|
| `RowWriter` | `writeRow(Consumer<StructBuilder>)`; a final class |
| `StructBuilder` | sets the fields of one struct instance: the row, a nested struct, or a map entry |
| `ListBuilder` | appends the entries of one `LIST` instance |
| `MapBuilder` | appends the entries of one `MAP` instance through `addEntry(Consumer<StructBuilder>)` |
| `PrecisionLossPolicy` | `REJECT` / `TRUNCATE`, selected through `WriterConfig` |

The builders are interfaces implemented by the plan nodes in `internal.writer` (`RowStructNode`, `RowListNode`, `RowMapNode`), so the public surface is a contract with no visible construction. A builder is valid only for the duration of the filler it is passed to; using a retained one later, or re-entering a scope that is still open, throws `IllegalStateException` rather than corrupting a later record.

A `MAP`'s entries are a repeated struct of `key` and `value`, so `MapBuilder` reuses `StructBuilder` rather than a key/value vocabulary. That composes with every key and value type, including struct, list and map values, at the cost of naming the two fields as strings; typed `put` methods would be combinatorial in key type × value type.

### Field addressing

Fields are addressed by their user-visible name, never by a leaf path: `setList("phones", …)` and, inside it, `addString(…)` produce the values of the `phones.list.element` column. Nested structs are entered through `setStruct(name, …)` rather than a dotted path, so a name is always resolved against exactly one group.

Every setter has an index-taking mirror (`setLong(int, long)` beside `setLong(String, long)`) addressing a field by its position in schema declaration order within the struct that declares it: a nested struct's indices are its own, and a `MAP` entry declares `key` at `0` and `value` at `1`. `getFieldCount()` and `getFieldName(int)` report those positions, the pair the reader's `FieldAccessor` exposes. The two forms address the same field and are interchangeable within one record; an out-of-range index takes the place of an unknown name (`IndexOutOfBoundsException`), and every other rule holds unchanged. The by-name surface resolves to the index through a primitive-valued open-addressed map, once per field per record, and then does what the by-index form does.

The index form makes the write side symmetric with the read side: `StructAccessor` addresses a row's fields by position, so code that walks a row generically (a copy, a projection, a format bridge) can read at position `i` and write at position `i` without a name in the loop, and a hot loop over a fixed schema pays no name lookup. The two positions agree only where the write schema is what was read; the reader's index is a position among the projected children, the writer's a position in the declared schema, and where the fields that then line up share a physical type a mismatch lands values in the wrong fields rather than failing. `getFieldName` on both sides is the check that turns it into a failure, which is why the writer exposes it. The user-facing rule is in [docs/content/concepts/write-model.md](../docs/content/concepts/write-model.md#index-addressing-on-the-two-sides).

### Nulls, completeness and rollback

Within a struct scope, a field that is never set is written as null if it is `OPTIONAL` and fails the record if it is `REQUIRED`. `setNull(name)` states the same thing explicitly, and a `null` handed to any object-typed setter does too, so a caller writing `setString("name", person.name())` over a nullable field need not branch. This is checked when the scope's filler returns: an unset `REQUIRED` field throws from inside `writeRow`, naming the field. Nothing is silently defaulted, and a name not in the schema throws from the setter itself, so a typo cannot silently write nulls. Only present subtrees are checked: a `REQUIRED` field inside an `OPTIONAL` struct the record left null is not required of that record. Setting the same field twice within one scope throws, mirroring the columnar already-set rule.

A list or map entry is nullable only where the schema says so: `ListBuilder.addNull()` requires an `OPTIONAL` element, and a map's `key` is always `REQUIRED`, so a null key throws. An absent list (`setNull("phones")`) and an empty one (`setList("phones", phones -> {})`) are distinct.

A record is staged in full or not at all. `RowPlan` checkpoints every node's staging before the filler runs and rolls it back if it throws, whether the writer rejected a value or the caller's own code failed, so a rejected record leaves the staged batch exactly as it was and its columns still agree on their record count. The exception still fails the writer, as every exception out of a write call does ([WRITER.md](WRITER.md)).

### Value conversion

The setters mirror the reader's accessors one for one, so a value read back through `FieldAccessor` is written by the setter of the same name, by name or by index. The setter-to-type table is in [docs/content/reference/writer.md](../docs/content/reference/writer.md#logical-types-and-row-setters). Conversion is performed by `internal.conversion.PhysicalValueConverter`, the inverse of `LogicalTypeConverter` and its neighbour in the same package. The strategy is fixed per column when the plan is built; a setter needing no conversion (`setInt` on a bare `INT32`) goes straight to the staging array.

**Magnitude is never negotiable.** A value the column cannot represent at all (a date beyond the `INT32` day range, an instant outside the span its `TIMESTAMP` unit covers in an `INT64`, an unscaled decimal wider than the declared precision, an `INT(8)` out of range, a `FIXED_LEN_BYTE_ARRAY` of the wrong length, an `INTERVAL` component outside the unsigned 32-bit range) throws `IllegalArgumentException` naming the field. No configuration relaxes this, because no narrowing would preserve what the caller handed over. Magnitude is checked before precision, so a value that is both too large and too precise fails the same way under either policy.

**Precision is configurable** through `WriterConfig.precisionLossPolicy(...)`:

- `REJECT` (the default) throws when a value carries digits the column's unit or scale cannot hold. A value exact at that unit or scale is written normally.
- `TRUNCATE` drops those digits. `TIME` and `TIMESTAMP` floor the sub-unit fraction, the result `Instant.toEpochMilli()` produces. `DECIMAL` rescales with `RoundingMode.DOWN`.

`REJECT` is the default because relaxing a rejection later is a compatible change, while tightening one silently changes what past files meant; PyArrow's `write_table` likewise raises on a lossy timestamp cast unless `allow_truncated_timestamps` is set. The cost is that `Instant.now()` carries microseconds on a modern JDK and `setTimestamp` on a `TIMESTAMP(MILLIS)` column rejects it, so the rejection message names all three ways out: truncate at the call site, declare a finer unit, or select `TRUNCATE`. The policy is a property of the row layer: `ColumnBatch` takes physical values, so nothing there can lose precision.

- **Annotation match.** A logical setter whose type does not match the column's annotation throws. `setTimestamp` requires `isAdjustedToUTC = true` and `setLocalTimestamp` requires `false`, mirroring the reader's split between `Instant` and `LocalDateTime`; calling the wrong one is a time-zone bug and is reported as one.
- **Physical setters apply to annotated columns.** `setInt` on a `DATE` column writes the stored `int`, mirroring the reader's `getInt`, which returns it whatever the annotation. That makes the physical setters the escape hatch beside the logical ones, and is why the [annotation range](#annotation-ranges) check sits on the physical setters too. `setString` takes a `BYTE_ARRAY` column annotated `STRING`, `ENUM` or `JSON`, or unannotated, and encodes UTF-8. A physical-type mismatch (`setLong` on an `INT32` column) throws.

Tests: `RowWriterConversionTest`, `RowWriterRoundTripTest`, `WriterFlba12TimestampTest`, `RowWriterLogicalTypeInteropTest` (parquet-testing-runner).

### Transposition

The row writer is an adapter, not a second write path. It owns no shredding, paging, statistics or dictionary logic: `RowPlan` accumulates records into column-shaped staging and submits it through the same batch path `writeBatch` uses. Every guarantee of the columnar path therefore holds for files written through it, and the same data written both ways produces byte-identical files.

Staging, reused across batches:

- per leaf column, a growable typed array and a null mask (`LeafStage`);
- per `OPTIONAL` struct, a null mask indexed by struct instance (`NullMaskStage`);
- per `LIST` / `MAP`, an offsets array and, if `OPTIONAL`, a null mask (`RowRepeatedNode`).

The builders append in document order, which is the order the shredder consumes, so transposition reorders nothing. A leaf appends one entry per instance of its enclosing scope, a placeholder included beneath an absent struct, matching the columnar [leaf slots](#leaf-slots); every nested group beneath an absent ancestor still advances its own scope so later records stay aligned. A `BYTE_ARRAY` placeholder is empty except under `DECIMAL`, where it is `{0}`; a `FIXED_LEN_BYTE_ARRAY` placeholder has the declared width.

A batch is submitted when either trigger fires:

- **1024 staged records.** A fixed internal constant, not a `WriterConfig` option: the batch is an arrival unit with no effect on the produced file.
- **Staged variable-width payload ≥ `WriterConfig.rowGroupBufferTargetBytes()`.** A record of large `BYTE_ARRAY` values would otherwise let 1024 records hold arbitrarily much; this bounds staging at one row group's worth.

`ParquetFileWriter.close()` submits the pending partial batch before the final row group is flushed and the footer written. The staging arrays are refilled as soon as the submission returns, which relies on the batch being consumed synchronously ([Column batches](#column-batches)); a write path that consumed batches asynchronously would have to give the row writer a second staging generation. A staged array is handed over directly when its fill count equals its capacity and trimmed with `Arrays.copyOf` otherwise, because `ColumnBatch` derives a column's value count from the array length; the columnar API is the zero-copy path.

Tests: `RowWriterEquivalenceTest`, `RowWriterFieldIndexTest`, `RowWriterRulesTest`, `RowWriterRoundTripTest`.

### One API per file, and row-layer shapes

A file writer serves one API or the other, never both: rows and batches would otherwise interleave two independent staging states into one row group, with a submission order that depends on when the row writer happens to flush. `ParquetFileWriter` carries a mode latch: `rowWriter()` latches row mode and `columnWriter()` batch mode, on the call that obtains the view rather than on the first write, and either throws `IllegalStateException` naming both APIs if the other mode is latched. Each returns the same instance on every call, so two views cannot stage against one file.

`RowWriter` is not `Closeable`. The file writer is the resource; a second closeable over the same file invites a double close that either discards a valid file or writes a second footer. `writeRow` after close, or after a failed write, throws through the same check the columnar path uses.

Producibility is settled by `ParquetFileWriter.create` before either view exists. `RowPlan`, built when `rowWriter()` is first called, additionally rejects the shapes this layer alone cannot *address*: two sibling fields sharing a name, which leaves the by-name setters ambiguous, and the legacy two-level lists, whose entry is the element itself where the builders reach a list's values through an element node below the entry. The columnar API addresses by index and dotted path and writes both. A group carrying an annotation the row layer does not interpret, such as a `VARIANT` group read from an existing file, is written field by field like a struct (untested).

### Reserved surface

Two extensions are foreseen, and the surface is shaped so they can be added without a breaking change or a boxing penalty: bulk primitive entries (`setList("scores", int[])` and the other primitive arrays, distinct erasures that compose with the filler form) and string-keyed map sugar (`MapBuilder.putString(String, String)`, `putLong(String, long)`, …, linear rather than combinatorial).

One rule keeps both open: **no unqualified `Object`-typed setter.** There is no `set(String, Object)`, no `ListBuilder.add(Object)`, and no `Collection<?>` overload. Any of them would make the boxing path the one that binds by default (`addInt(1)` and `add(1)` are not the same program) and would create overload ambiguity against primitive forms added later. The rule bites on the name, an unqualified `set` or `add` competing with typed siblings, not on the parameter type: a distinctly named `setValue(String | int, Object)` mirroring the reader's `getValue` is outside it, since no call to `setInt` can resolve to it by accident.

## Boundaries

- **`INT96`** is not writable through either API.
- **`VARIANT`** annotations are rejected on a declared leaf, and the builder cannot declare a Variant group; a Variant group read from a file is written field by field through the binary setters, since the writer has no Variant encoder.
- **Typed binding** (writing a POJO or record directly) is the write side of the typed view epic (#940), not part of this layer.
- **A rejected record fails the writer** although its staging is rolled back; skipping it and continuing is #1253.
- **Open input-vocabulary gaps**, tracked in #1291: `StructBuilder.setValue` and `setVariant` (the two reader accessors with no setter); `ColumnBatch.strings(…)` and a blob-plus-offsets binary form matching the reader's `getBinaryValues()` / `getBinaryOffsets()`; and group-oriented nesting for columnar copies, including a `REQUIRED` leaf under an `OPTIONAL` struct, whose reader-side leaf validity reports nulls the writer's leaf setter refuses on a `REQUIRED` column.
