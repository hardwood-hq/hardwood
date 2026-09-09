# Logical-type write support (#9, stage 13)

**Status: Complete.** Tracking issue: #9. Delivery stage 13 (Breadth) of
[WRITER_SUPPORT.md](WRITER_SUPPORT.md), delivered in increments 13a (annotations) and 13b
(order-correct statistics). This document is the reference the logical-type increments
implement against.

## Context

Through stage 12 the writer produces every primitive physical type Parquet defines, in every
column shape the reader supports, with paging, nulls, nesting, dictionary encoding,
compression, and statistics. What it cannot express is what those bytes *mean*: a `BYTE_ARRAY`
column is only ever a bag of bytes, never a string; an `INT32` is only ever a signed 32-bit
integer, never a date; an unscaled `INT64` is never a decimal.

Parquet carries that meaning as a **schema annotation** on the `SchemaElement`, in two
representations that coexist:

- `converted_type` (field 6) — the legacy `ConvertedType` enum. Being an enum, it cannot carry
  parameters, so `DECIMAL` spills its `scale` and `precision` into sibling `SchemaElement`
  fields 7 and 8, and there is no way at all to express a non-UTC-normalized timestamp, a
  nanosecond unit, or a UUID.
- `logicalType` (field 10) — the modern `LogicalType` union of parameterized structs, which
  supersedes it.

The reader already models both: `LogicalType` is a public sealed interface with 18 variants,
`FileSchema.effectiveLogicalType` collapses a legacy `converted_type` into the equivalent
`LogicalType`, and `LogicalTypeConverter` turns the physical bytes into the annotated Java
value. The writer models neither — `SchemaElementWriter` serializes fields 1–6 and throws
`UnsupportedOperationException` on any element carrying a logical type, decimal scale /
precision, or field id, so the annotation cannot be dropped silently.

This stage makes the writer emit annotations, so a column written as `STRING`, `DATE`,
`DECIMAL`, `TIMESTAMP`, `UUID` or any other logical type reads back as that type — in Hardwood,
and in DuckDB, Spark, and pandas.

## Scope

1. **Declaring an annotation** — `FileSchema.Builder` overloads that attach a `LogicalType` to a
   primitive column, with fail-early validation that the annotation is legal for the column's
   physical type and type length.
2. **Emitting both representations** — every annotation is written as the `LogicalType` union
   *and*, where a legacy equivalent exists, as the `converted_type` (with `scale` / `precision`
   for `DECIMAL`), per the parquet-format compatibility rule.
3. **Thrift serialization** — a `LogicalTypeWriter` for the union and its nested structs, the
   `i8` and boolean field primitives the union needs, and `SchemaElement` fields 7–10.
4. **Sort order** — an annotation redefines a column's `min` / `max` ordering. Statistics are
   made order-correct for every annotation whose order is defined, and suppressed for those
   whose order the spec leaves undefined.
5. **`column_orders`** — the footer's per-leaf `ColumnOrder` list, required by the format
   whenever `Statistics.min_value` / `max_value` are written.

Value conversion — accepting a `LocalDate` or a `BigDecimal` and lowering it to the physical
representation — is **not** in scope. This stage annotates; the caller still supplies physical
values (days-since-epoch, unscaled bytes). The inverse of `LogicalTypeConverter` arrives with
the row-oriented `RowWriter` in stage 16.

### Increment split

**13a — annotations.** Items 1–3, plus the *suppression* half of item 4: a column whose
annotation implies an ordering the stage 11/12 collectors do not implement writes `null_count`
only, with no `min` / `max`. Omitting a bound is always sound — a reader that finds no bound
prunes nothing — so 13a never produces a wrong file, only a less prunable one.

**13b — order-correct statistics.** The remaining half of item 4 (unsigned integer, signed
big-endian decimal, and half-precision float ordering) plus item 5, un-suppressing what 13a
suppressed and making every bound the file carries well-defined.

## Declaring an annotation

`FileSchema.Builder`, `StructBuilder`, and `ElementBuilder` each gain a `LogicalType` overload
of their primitive verb, alongside the existing plain and type-length forms:

```java
Builder       addColumn(String name, PhysicalType type, RepetitionType rep, LogicalType logicalType)
Builder       addColumn(String name, PhysicalType type, RepetitionType rep, int typeLength, LogicalType logicalType)
StructBuilder addColumn(String name, PhysicalType type, RepetitionType rep, LogicalType logicalType)
StructBuilder addColumn(String name, PhysicalType type, RepetitionType rep, int typeLength, LogicalType logicalType)
void          primitive(PhysicalType type, RepetitionType rep, LogicalType logicalType)
void          primitive(PhysicalType type, RepetitionType rep, int typeLength, LogicalType logicalType)
```

A `MAP`'s key is a primitive the caller never names a verb for — it is declared positionally on
the `map` verb — so each `map` verb gains the same pair of overloads for it. A `MAP<STRING, …>`
whose key is an unannotated `BYTE_ARRAY` is a map of opaque blobs to every other engine:

```java
Builder       map(String name, RepetitionType rep, PhysicalType keyType, LogicalType keyLogicalType,
                  Consumer<ElementBuilder> value)
Builder       map(String name, RepetitionType rep, PhysicalType keyType, int keyTypeLength,
                  LogicalType keyLogicalType, Consumer<ElementBuilder> value)
StructBuilder map(…)   // the same two forms
void          map(…)   // the same two forms, on ElementBuilder
```

A key takes no length-only form, so the primitive verbs' four shapes become three here: a
`FIXED_LEN_BYTE_ARRAY` key with no annotation is declared by passing a `null` one. A raw
fixed-width key is rare enough not to justify a fourth overload on each of the three builders.

The annotation rides on the private `BuilderLeaf` node and is lowered by `flatten` onto the
leaf's `SchemaElement`; a map key is a `BuilderLeaf` too, so it is built and validated through
the same `leaf` helper as any other primitive. Groups take no logical-type parameter: `LIST` and
`MAP` groups are annotated by the `list` / `map` verbs themselves, and a plain `struct` group
carries no annotation.

```java
FileSchema schema = FileSchema.builder("schema")
        .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, LogicalType.string())
        .addColumn("birthday", PhysicalType.INT32, RepetitionType.REQUIRED, LogicalType.date())
        .addColumn("balance", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 8,
                LogicalType.decimal(18, 2))
        .addColumn("created", PhysicalType.INT64, RepetitionType.REQUIRED,
                LogicalType.timestamp(true, LogicalType.TimeUnit.MICROS))
        .map("counts", RepetitionType.OPTIONAL, PhysicalType.BYTE_ARRAY, LogicalType.string(),
                value -> value.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
        .build();
```

### Validation

An annotation is legal only on the physical types parquet-format permits, and the writer
rejects an illegal pairing at declaration time rather than producing a file no reader can
interpret. `LogicalTypeValidator` (in `internal.schema`) is the single table:

| Logical type | Physical type | Further constraints |
|---|---|---|
| `StringType`, `EnumType`, `JsonType`, `BsonType` | `BYTE_ARRAY` | — |
| `GeometryType`, `GeographyType` | `BYTE_ARRAY` | — |
| `UuidType` | `FIXED_LEN_BYTE_ARRAY` | type length 16 |
| `Float16Type` | `FIXED_LEN_BYTE_ARRAY` | type length 2 |
| `IntervalType` | `FIXED_LEN_BYTE_ARRAY` | type length 12 |
| `DateType` | `INT32` | — |
| `IntType(8\|16\|32, *)` | `INT32` | — |
| `IntType(64, *)` | `INT64` | — |
| `TimeType(*, MILLIS)` | `INT32` | — |
| `TimeType(*, MICROS\|NANOS)` | `INT64` | — |
| `TimestampType` | `INT64` | every unit |
| `DecimalType` | `INT32` | `precision <= 9` |
| `DecimalType` | `INT64` | `precision <= 18` |
| `DecimalType` | `FIXED_LEN_BYTE_ARRAY` | `precision <= floor(log10(2^(8·len − 1) − 1))` |
| `DecimalType` | `BYTE_ARRAY` | — |
| `NullType` | any | not `REQUIRED` |
| `ListType`, `MapType`, `VariantType` | — | rejected on a primitive |

`DecimalType` additionally requires `scale <= precision`, which the record itself does not
enforce (it must stay lenient enough to model the files the reader encounters).

`NullType` is the one annotation constrained by repetition rather than by physical type:
`UNKNOWN` describes a column whose every value is null, which a `REQUIRED` column can never be.
Nothing it could legally hold matches the annotation, and `LogicalTypeConverter` throws on the
non-null value such a column would carry, so the pairing is rejected where it is declared.

`ListType` and `MapType` are group annotations that the `list` / `map` verbs emit; declaring
either on a leaf is a caller error. `VariantType` annotates a group of a prescribed shape and is
out of scope for this stage — declaring it is rejected with a message naming that limitation.

## Emitting both representations

parquet-format is explicit: *"Parquet writers must always write `LogicalType` annotations where
applicable, but must also write the corresponding `ConvertedType` annotations (if any) to
maintain compatibility with old readers."* Hardwood emits both, from a single declared
`LogicalType`, in `FileSchema.toSchemaElements`.

That is the right seam because the schema model is already one-sided: `PrimitiveNode` and
`ColumnSchema` carry only a `LogicalType`, since `fromSchemaElements` collapses any legacy
`converted_type` it reads into one. Deriving the legacy form back out during the lowering to
`SchemaElement` therefore both serves the builder and repairs the read-then-rewrite path, which
until now silently dropped a legacy annotation's `scale` / `precision`. The thrift `*Writer`
classes stay pure struct serializers — they write whatever the record holds.

`LogicalTypeAnnotations.of(LogicalType)` (in `internal.schema`) is the inverse of
`FileSchema.effectiveLogicalType`. Its `ofGroup` companion covers the group case, where the
model may hold either representation:

| Logical type | `converted_type` | Union member |
|---|---|---|
| `StringType` | `UTF8` | 1 |
| `MapType` | `MAP` | 2 |
| `ListType` | `LIST` | 3 |
| `EnumType` | `ENUM` | 4 |
| `DecimalType(s, p)` | `DECIMAL` + `scale = s`, `precision = p` | 5 |
| `DateType` | `DATE` | 6 |
| `TimeType(*, MILLIS)` | `TIME_MILLIS` | 7 |
| `TimeType(*, MICROS)` | `TIME_MICROS` | 7 |
| `TimeType(*, NANOS)` | — | 7 |
| `TimestampType(*, MILLIS)` | `TIMESTAMP_MILLIS` | 8 |
| `TimestampType(*, MICROS)` | `TIMESTAMP_MICROS` | 8 |
| `TimestampType(*, NANOS)` | — | 8 |
| `IntType(w, signed)` | `INT_w` / `UINT_w` | 10 |
| `NullType` | — | 11 |
| `JsonType` | `JSON` | 12 |
| `BsonType` | `BSON` | 13 |
| `UuidType` | — | 14 |
| `Float16Type` | — | 15 |
| `GeometryType` | — | 17 |
| `GeographyType` | — | 18 |
| `IntervalType` | `INTERVAL` | — |

Two rows carry the subtleties:

- **`TIME` / `TIMESTAMP` ignore `isAdjustedToUTC` when deriving the legacy form.** The legacy
  annotations denoted UTC-normalized values only, but parquet-format nonetheless requires
  writers to annotate local time and local timestamps with them too, for forward compatibility
  with the libraries that did so before the union existed. A pre-union reader therefore reads a
  local timestamp as UTC-normalized; a union-aware reader takes field 10 and is exact. `NANOS`
  has no legacy counterpart and is union-only.
- **`INTERVAL` is `converted_type`-only.** Union field 9 is *reserved* for `INTERVAL` in
  parquet.thrift — the member was never defined — so an interval column is annotated by the
  legacy enum alone. (Hardwood's `LogicalTypeReader` accepts field 9 leniently when some other
  writer emits it; the writer does not produce it.)

`VariantType` is not in the table because it is rejected before reaching the lowering.

## Thrift serialization

### `SchemaElementWriter`

The `rejectUnsupported` guard is removed and fields 7–10 are appended in ascending id order,
each still conditional on the record component being present:

| id | field | wire type |
|---|---|---|
| 7 | `scale` | `I32` |
| 8 | `precision` | `I32` |
| 9 | `field_id` | `I32` |
| 10 | `logicalType` | `STRUCT` → `LogicalTypeWriter` |

### `LogicalTypeWriter`

The inverse of `LogicalTypeReader`: a union is a struct with exactly one field set, whose id
selects the member and whose value is the member struct — empty for the unparameterized types.
The nested structs and their field ids:

| Struct | Fields |
|---|---|
| `DecimalType` | 1 `scale` (`i32`), 2 `precision` (`i32`) |
| `TimeType`, `TimestampType` | 1 `isAdjustedToUTC` (`bool`), 2 `unit` (`TimeUnit`) |
| `TimeUnit` (union) | 1 `MILLIS`, 2 `MICROS`, 3 `NANOS`, each an empty struct |
| `IntType` | 1 `bitWidth` (`i8`), 2 `isSigned` (`bool`) |
| `VariantType` | 1 `specification_version` (`i8`, optional) |
| `GeometryType` | 1 `crs` (`string`, optional) |
| `GeographyType` | 1 `crs` (`string`, optional), 2 `algorithm` (`EdgeInterpolationAlgorithm`) |
`EdgeInterpolationAlgorithm` is a Thrift **enum**, not a union like `TimeUnit`, so `algorithm` is
an `i32` of the enum's value (`SPHERICAL` = 0 … `KARNEY` = 4) rather than a nested struct. The
reader's `UNKNOWN` — an algorithm added to the format after this release — has no value to write,
so a schema carrying one is rejected rather than written as some other algorithm.

Each nested struct brackets its fields with `pushFieldIdContext` / `popFieldIdContext`, since
compact-protocol field ids are delta-encoded per struct.

`GeometryType` and `GeographyType` omit `crs` when it is `null`, matching the reader, which
substitutes the `"OGC:CRS84"` default for an absent value.

### `ThriftCompactWriter`

The union needs two primitives the writer does not have:

- `writeBool(int fieldId, boolean value)` — in the compact protocol a boolean *field* carries no
  payload; the value is the field header's type code (`BOOLEAN_TRUE` / `BOOLEAN_FALSE`). The
  helper keeps that encoding in one place instead of leaving callers to pick the type code.
- `writeByte(byte value)` — an `i8` is a single plain byte, not a zigzag varint, so `writeI32`
  cannot stand in for it.

## Sort order and statistics

`ColumnOrder`'s type-defined order is defined by a column's **logical** type where it has one,
falling back to the physical type otherwise. Annotating a column can therefore change the
ordering its `min` / `max` must be computed in, and the stage 11/12 collectors are selected by
physical type alone. Left unaddressed, annotating an `INT32` column as `UINT_32` would keep the
signed collector and write bounds that prune away live rows.

| Order | Columns | Collector |
|---|---|---|
| Signed integer | unannotated `INT32` / `INT64`; `INT_8/16/32/64`; `DATE`; `TIME`; `TIMESTAMP`; `DECIMAL` on `INT32` / `INT64` | `IntStatisticsCollector`, `LongStatisticsCollector` (existing) |
| Unsigned integer | `UINT_8/16/32/64` | 13b: unsigned comparison |
| Unsigned lexicographic | unannotated `BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY`; `STRING`; `ENUM`; `JSON`; `BSON`; `UUID` | `BinaryStatisticsCollector` (existing) |
| Signed big-endian two's complement | `DECIMAL` on `BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY` | 13b: signed binary comparison |
| Represented value | unannotated `FLOAT` / `DOUBLE` | `FloatStatisticsCollector`, `DoubleStatisticsCollector` (existing) |
| Represented value | `FLOAT16` | 13b: half-precision comparison |
| Boolean | unannotated `BOOLEAN` | `BooleanStatisticsCollector` (existing) |
| **Undefined** | `INTERVAL`; `UNKNOWN`; `VARIANT`; `GEOMETRY`; `GEOGRAPHY`; `LIST`; `MAP` | none — no `min` / `max` written |

`StatisticsOrder` (in `internal.writer`) is the single table of which columns have well-defined
bounds. `ColumnChunkBuffer` consults it once per chunk and drops the bounds where they do not,
so the decision costs nothing per value. 13b extends the same seam to *select* the comparator:
`ValueEncoder.forColumn` already receives the `ColumnSchema`, which carries `logicalType()`.

A binary column whose order is undefined does not accumulate bounds at all — `BinaryStatistics`
gives it a `NullCountStatistics` that counts nulls and ignores values, rather than a collector
that would copy a `GEOMETRY` blob out on every bound extension only for the flush to discard it.
The fixed-width collectors accumulate in primitive fields, so there is nothing to save there.

Two consequences beyond the comparator itself:

- **Undefined-order columns write `null_count` only.** parquet-format states it directly for
  `INTERVAL`: *"When writing data, no min/max statistics should be saved for this type."* The
  same holds for every other undefined-order annotation. `Statistics` is still written, so the
  null count remains available for pushdown.
- **`BYTE_ARRAY` bound truncation applies only to the lexicographic order.** Truncating a `min`
  to a prefix, and a `max` to an incremented prefix, is order-preserving under unsigned
  byte-wise comparison and *not* under signed big-endian comparison of a decimal's represented
  value — a shorter byte string is a numerically different value there, in either direction.
  A binary `DECIMAL` column's bounds are therefore never truncated.

13a writes `null_count` alone for every row marked "13b" above and every undefined-order row;
13b adds the marked collectors, leaving only the undefined-order rows without bounds.

## `column_orders`

parquet-format requires the footer's `column_orders` (field 7) whenever statistics bounds are
written: *"Without `column_orders`, the meaning of the `min_value` and `max_value` fields in the
`Statistics` object and the `ColumnIndex` object is undefined. To ensure well-defined behaviour,
if these fields are written to a Parquet file, `column_orders` must be written as well."* The
writer has emitted bounds since stage 11 without it.

`ColumnOrderWriter` serializes the union, and `FileMetaDataWriter` writes field 7 as a
`list<ColumnOrder>` with one entry per **leaf** column in schema order. Every entry is
`TypeDefinedOrder` (union field 1):

- It is the order every collector implements, including the float and double collectors, whose
  NaN exclusion and signed-zero normalization are the type-defined convention.
- The spec's recommendation to prefer `IEEE754TotalOrder` for floating-point columns does not
  apply to bounds computed this way. Under the total order NaN is an ordinary value that sorts
  beyond the infinities, so excluding it from the bounds — which the type-defined order requires
  — would let a total-order reader drop a page that contains one.

## Testing

- **Thrift symmetry** — `LogicalTypeWriterTest` writes each of the 17 defined union members with
  `LogicalTypeWriter` and reads it back with `LogicalTypeReader`, asserting equality; the
  parameterized members are covered across their parameter space (each `TimeUnit`, both
  `isAdjustedToUTC` values, each `bitWidth` × signedness, present and absent `crs`). This is the
  established pattern for the codec, following `LogicalTypeReaderTest`.
- **Schema round trip** — `WriterLogicalTypeRoundTripTest` writes a column per annotation with
  `ParquetFileWriter`, reads the file back with `ParquetFileReader`, and asserts both the
  recovered `LogicalType` and the on-disk `converted_type` / `scale` / `precision`, on flat,
  nullable, and nested columns.
- **Validation** — every rejected physical/logical pairing in the table above fails at
  `FileSchema.Builder` with a message naming the column, the annotation, and the constraint.
- **Statistics** — bounds are asserted per ordering, on both `BYTE_ARRAY` and
  `FIXED_LEN_BYTE_ARRAY` for the signed big-endian order, and undefined-order columns are
  asserted to carry a null count with no bounds. A binary `DECIMAL` column with values longer
  than the truncation length is asserted to keep untruncated, exact bounds.
- **Differential** — `WriterDifferentialTest` gains annotated columns and asserts DuckDB reads
  them as `VARCHAR`, `DATE`, `TIMESTAMP`, `DECIMAL`, and `UUID` rather than as raw physical
  types, and reads an annotated map key as `MAP(VARCHAR, INTEGER)`.
- **Legacy readers** — a schema built with annotations and then stripped of field 10, as a
  pre-union reader observes it, resolves every annotation that has a legacy equivalent from the
  `converted_type` alone. A local `TIMESTAMP` is asserted to resolve as UTC-normalized, the
  documented limit of what the legacy form can express.
