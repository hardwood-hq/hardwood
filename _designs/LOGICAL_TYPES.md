# Logical types

Describes how Hardwood models Parquet's logical-type annotations on read and write: how `FileSchema` derives the one `LogicalType` a column carries from the footer, which annotations it drops, how each annotation decodes to a Java value and which accessors a column admits, the timestamp kinds and carriers, Variant groups and their shredded form, the geospatial types and their statistics, and how the writer emits an annotation.

Related documents:

- [FILE_METADATA.md](FILE_METADATA.md): the Thrift parse of the `LogicalType` union and its forward-compatibility policy
- [VALUE_DECODE.md](VALUE_DECODE.md): the leaf decode path and `LeafKind`
- [ROW_READER.md](ROW_READER.md): accessor addressing
- [PREDICATE_MODEL.md](PREDICATE_MODEL.md): the filter literal each annotation takes
- [STATISTICS_PRUNING.md](STATISTICS_PRUNING.md): bound readability and sort orders on read
- [WRITER_INPUT.md](WRITER_INPUT.md): the physical × logical legality table and value ranges on write
- [WRITER_ENCODING.md](WRITER_ENCODING.md): statistics order on write
- [AVRO_BINDING.md](AVRO_BINDING.md): the Avro mapping
- [CLI_VALUE_RENDERING.md](CLI_VALUE_RENDERING.md): CLI rendering
- [docs/content/reference/accessors.md](../docs/content/reference/accessors.md) and [docs/content/concepts/timestamps.md](../docs/content/concepts/timestamps.md): user-facing semantics

## Annotation model

`LogicalType` (`dev.hardwood.metadata`) is a public sealed interface with one record per annotation parquet-format defines: `STRING`, `ENUM`, `UUID`, `INT(bitWidth, signed)`, `DECIMAL(precision, scale)`, `DATE`, `TIME(isAdjustedToUTC, unit)`, `TIMESTAMP(isAdjustedToUTC, unit)`, `INTERVAL`, `JSON`, `BSON`, `LIST`, `MAP`, `VARIANT(specVersion)`, `GEOMETRY(crs)`, `GEOGRAPHY(crs, edgeInterpolation)`, `FLOAT16` and `NULL` (the format's `UNKNOWN`). All but `INTERVAL`, which exists only as a `converted_type`, are members of the Thrift union.

`AnnotationKind` (`internal.schema`) holds what parquet-format states about each annotation, one constant per member: its union field id, whether it annotates a group, whether it holds text, its order and byte order, the physical types and widths it is defined over, its legacy `converted_type`, and the Java type of its filter literal. Facts that depend on a member's parameters, such as a `TIME`'s unit or a `DECIMAL`'s digits, are functions of the record. `AnnotationKind.of` is the one switch from a member to its constant, so a new member does not compile until its constant states every fact. `AnnotationPairings` holds the rules that combine an annotation with a physical type, a width or a converted type, and `ByteColumnOrder` is the one representation of a byte order. The remaining switches over `LogicalType` decide what a consumer does with a value; decoding (`LogicalTypeConverter.convert`) and the writer's value ranges (`LogicalTypeValueRange.of`) are exhaustive.

A column carries at most one annotation. A leaf in the schema model holds only the `LogicalType`; the footer's legacy `converted_type` is folded into it when the schema is built and derived back out when it is written ([Writing annotations](#writing-annotations)).

### Deriving a leaf's annotation

`LeafAnnotation.effective` (`internal.schema`) answers for a primitive `SchemaElement`:

- The modern `logicalType` wins when present.
- Where it is absent, the `converted_type` is promoted: `UTF8` → `STRING`; `ENUM`, `JSON`, `BSON`, `DATE`, `INTERVAL` → the member of the same name; `DECIMAL` → `DECIMAL(precision, scale)` read off the element (a missing scale is `0`; a missing precision, a negative scale, a precision that is not positive or a scale above the precision is refused with `ParquetReadException` where the footer is parsed, as the union's `DecimalType` is ([FILE_METADATA.md](FILE_METADATA.md#footer-read))); `TIME_MILLIS` / `TIME_MICROS` / `TIMESTAMP_MILLIS` / `TIMESTAMP_MICROS` → the member with that unit and `isAdjustedToUTC = true`, as the format's backward-compatibility rule defines them; `INT_n` / `UINT_n` → `INT(n, signed)`.
- `UNKNOWN` beside a `converted_type` yields to the converted type. parquet-java writes `INTERVAL` as `converted_type = INTERVAL` beside `logicalType = UNKNOWN`, since the union has no `INTERVAL` member. `LeafAnnotation.convertedTypeDecides` states when the converted type decides: where the union is absent, unrecognized or `UNKNOWN`.
- The group-level `LIST`, `MAP` and `MAP_KEY_VALUE` give no leaf annotation.

A group's annotation is its `logicalType` when present; otherwise a group whose single child is a repeated `MAP_KEY_VALUE` group (the legacy map form) is given `MAP`, so the rest of the reader treats both forms alike. `SchemaNode.GroupNode` carries both the `convertedType` and the `logicalType`; `isList` and `isMap` read both, `isVariant` the `logicalType` alone. A group keeps only the annotations of a structure (`LIST`, `MAP`, `VARIANT`; converted `LIST`, `MAP`, `MAP_KEY_VALUE`), and a converted type naming another structure than the logical type is dropped, the logical type deciding as on a leaf, so `isList` and `isMap` never both hold. `LogicalTypeAnnotations.ofGroup` refuses any other annotation on the write side.

### Dropping an annotation

An annotation the reader cannot use is dropped and the column reads as its physical type (a group as an unannotated group), per parquet-format's rule for unsupported logical types. The contract for callers (physical accessors work, a logical accessor fails as on any unannotated column) is in [EXCEPTION_MODEL.md](EXCEPTION_MODEL.md#an-annotation-the-reader-cannot-use-is-dropped-not-raised). Four cases, dropped in three places:

| Case | Where | Warning |
|---|---|---|
| A union arm this build does not know | `LogicalTypeReader`, while parsing ([FILE_METADATA.md](FILE_METADATA.md)); `FileMetaDataReader.ReadFooter` records the leaf | one per annotation, naming the field id |
| A known annotation the column's physical type or width cannot carry | `FileSchema.fromSchemaElements`, through `LeafAnnotation.dropFault` | one per file, listing every dropped column and its fault |
| An annotation only a primitive carries on a group, or a group's converted type naming another structure than its logical type | `FileSchema.fromSchemaElements`, through `AnnotationKind.annotatesGroup` and `AnnotationPairings.convertedAgrees` | in the same per-file warning |
| An annotation other than `VARIANT` on a repeated group outside a `LIST` or `MAP` group | `BareRepeatedGroups.dropAnnotations`, on the footer's elements before the schema is built ([NESTED_DECODE.md](NESTED_DECODE.md#unannotated-repeated-fields)) | one per file, listing every group and its annotation |

`dropFault` asks `LogicalTypeConverter.conversionFault(physicalType, typeLength, annotation)`, which reads the answer from `AnnotationPairings.check`: the one grid of which pairings of physical type, annotation and width the format defines. The writer's `LogicalTypeValidator` reads the same grid and refuses what it calls illegal, so the two sides cannot disagree about a pairing; each words its own message, because a reader describes a file that exists while the writer addresses a schema being declared, and both name the annotation by the record's `toString()`. The grid also holds the one pairing a legacy converted type adds to its logical counterpart (`AnnotationPairings.checkConverted`): a `TIMESTAMP_MILLIS` / `TIMESTAMP_MICROS` standing alone on anything but an `INT64` is faulted, although the `TIMESTAMP` it promotes to is also carried by a `FIXED_LEN_BYTE_ARRAY(12)`, and the writer asks the same question to leave the legacy annotation off such a column. `conversionFault` imposes nothing on `NULL`, which is legal over any type, and it leaves a `FIXED_LEN_BYTE_ARRAY` with no declared width, or a width that is not positive, to `FixedWidthValidator`, which refuses that column by name because it cannot be decoded at all. `LIST`, `MAP` and `VARIANT` on a primitive leaf are faulted as structural annotations.

The writer applies the same pairings strictly (`LogicalTypeValidator`, [WRITER_INPUT.md](WRITER_INPUT.md#physical--logical-legality)). `FileSchema`'s builder path re-checks its flattened elements against `dropFault` (`requireNoAnnotationDropped`) and throws `IllegalStateException` on a hit, since that means the two rule sets have drifted apart. Untested.

A consumer that needs the footer's own view asks `LeafAnnotation.dropFault` and `ReadFooter.logicalTypeUnread`, since the dropped annotation no longer appears in the schema: `BoundsReadability` reads no bounds for a leaf whose annotation was dropped in either of the first two places, because its writer ordered them by that annotation; the third drops group annotations only, and a group has no bounds ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md#bounds-readability)).

Tests: `FileSchemaConvertedTypeTest`, `LegacyConvertedTypesReadTest`, `LogicalTypeReaderTest`, `AnnotatedBareRepeatedGroupTest`, `IncompatibleLogicalTypeReadTest` (parquet-testing-runner), `UnknownLogicalTypeReadTest` (parquet-testing-runner).

## Decoding per annotation

`LogicalTypeConverter` (`internal.conversion`) is the decode table: primitive entry points per type for typed accessors, and `convert(Object, PhysicalType, LogicalType)` for the generic route, an exhaustive switch over the annotation. Which of them a leaf reaches, and the no-boxing rule for typed accessors, are in [VALUE_DECODE.md](VALUE_DECODE.md#leaf-kinds-and-nested-primitive-leaves).

| Annotation | Carriers | Typed accessor → Java type | Rule |
|---|---|---|---|
| `STRING`, `ENUM`, `JSON` | `BYTE_ARRAY` | `getString` → `String` | UTF-8 decode; `ENUM` decodes exactly like a string, as the format tells readers without a native enum type to do |
| `BSON` | `BYTE_ARRAY` | `getBinary` → `byte[]` | not text; the generic route returns the bytes |
| `DATE` | `INT32` | `getDate` → `LocalDate` | days since the epoch |
| `TIME` | `INT32` (`MILLIS`), `INT64` (`MICROS`, `NANOS`) | `getTime` → `LocalTime` | `isAdjustedToUTC` is informational: `LocalTime` has no zone |
| `TIMESTAMP` | `INT64`, `FIXED_LEN_BYTE_ARRAY(12)` | `getTimestamp` → `Instant`, `getLocalTimestamp` → `LocalDateTime` | see [Timestamps](#timestamps) |
| `DECIMAL` | `INT32`, `INT64`, `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` | `getDecimal` → `BigDecimal` | the stored unscaled value at the annotation's scale; byte payloads are big-endian two's complement and an empty payload is zero |
| `INT(8 / 16, signed)` | `INT32` | `getInt` | the generic route narrows to `Byte` / `Short` |
| `INT(n, unsigned)` | `INT32`, `INT64` | `getInt`, `getLong` | the stored bit pattern, unchanged; the caller reads the magnitude with `Integer.toUnsignedLong` / `Long.toUnsignedString` |
| `UUID` | `FIXED_LEN_BYTE_ARRAY(16)` | `getUuid` → `UUID` | big-endian most then least significant half |
| `INTERVAL` | `FIXED_LEN_BYTE_ARRAY(12)` | `getInterval` → `PqInterval` | three little-endian unsigned 32-bit fields (months, days, milliseconds), not normalized |
| `FLOAT16` | `FIXED_LEN_BYTE_ARRAY(2)` | `getFloat` → `float` | little-endian IEEE half widened losslessly, `NaN` payload kept |
| `GEOMETRY`, `GEOGRAPHY` | `BYTE_ARRAY` | `getBinary` → `byte[]` | the WKB payload, undecoded |
| `NULL` | any | none | every value is null; a stored value raises `ParquetReadException` on the generic route |
| `LIST`, `MAP`, `VARIANT` | groups | `getList`, `getMap`, `getVariant` | reaching the leaf conversion is an internal error (`IllegalStateException`) |

Text is decided in one place: `TextColumns.holdsText` accepts a `BYTE_ARRAY` that is unannotated or annotated `STRING`, `ENUM` or `JSON`, the annotations `AnnotationKind` calls text. `getString`, `PqList.strings()` and `ColumnReader.getStrings()` read exactly those columns, and a `String` filter literal applies to exactly those, so a value a string accessor cannot read out of a column is never one a `String` can filter it by ([PREDICATE_MODEL.md](PREDICATE_MODEL.md)).

`ColumnReader` returns physical values only; annotations are the caller's to apply ([COLUMN_READER.md](COLUMN_READER.md)). `getStrings()` is the one decoded accessor there, under the same text rule.

Tests: `LogicalTypeConverterTest`, `LogicalTypesMetadataTest`, `EnumLogicalTypeReadTest`, `JsonLogicalTypeTest`, `BsonLogicalTypeTest`, `IntervalLogicalTypeTest`, `Float16LogicalTypeTest`, `NullLogicalTypeTest`, `TextAccessorTest`.

## Accessor guards

An accessor asked for a type its column does not hold fails; how it fails depends on whether a cast on the way to the value can catch the mismatch. Validating ahead of every accessor call would cost every call, so only the accessors no cast can catch check first ([EXCEPTION_MODEL.md](EXCEPTION_MODEL.md)).

**Caught by a cast.** An accessor that decodes an annotation casts it to the one it expects, as in `((LogicalType.TimeType) leaf.logicalType()).unit()`. Another annotation fails there with `ClassCastException`, no annotation with `NullPointerException`, and a physical-type mismatch surfaces as the storage array's `ClassCastException` (`(int[])` on a `long[]`). `getTime`, `getDecimal` and the physical accessors are in this population.

**Checked first.** Five accessors read nothing from the annotation, so every cast on their way succeeds on the wrong column: a `DATE`, a bare `INT32` and a `TIME(MILLIS)` share one `int[]`; every 12-byte `FIXED_LEN_BYTE_ARRAY` (and every `INT96`) is one `BinaryBatchValues`; every byte column decodes to some text. They ask `LogicalAccessorKind` first:

| Accessor | Guard | Admits |
|---|---|---|
| `getDate` | `requireDate` | `DATE` |
| `getUuid` | `requireUuid` | `UUID` |
| `getInterval` | `requireInterval` | `INTERVAL` |
| `getString` | `requireText` | `TextColumns.holdsText` |
| `getFloat` on a non-`FLOAT` column | `requireFloat16` | `FLOAT16` |

Each rejection is an `IllegalArgumentException` carrying the file name and naming the column, its physical type and its annotation.

**Timestamp kind.** `getTimestamp` and `getLocalTimestamp` ask `TimestampAccessorKind.require`, which rejects the other `isAdjustedToUTC` kind with `IllegalStateException` naming the column and the flag it has. It treats an unannotated leaf as the UTC-adjusted legacy `INT96` kind and lets a non-`TIMESTAMP` annotation through to the cast. `FlatRowReader`, `NestedBatchDataView`, `PqStructImpl`, `PqMapImpl` and `PqListImpl` all go through it, so the message is the same everywhere; the filter resolver uses its `describe` wording for the same columns.

**Group elements.** A list element or map value that is a group has no leaf of its own; a leaf accessor over it would read the group's first leaf column and decode whatever it holds. `PqListImpl.requirePrimitiveElement` and `PqMapImpl.requirePrimitiveValue` cast the element's schema node to a primitive, so the call fails there instead.

**Typed views.** A typed view over a whole list column (`PqList.dates()`, `timestamps()`, `decimals()` and siblings) resolves the annotation, its unit or scale, and the guard once, when the view is built. A column the view does not fit is rejected when the view is asked for, before any element is read.

Tests: `NestedLogicalAccessorsTest`, `TextAccessorTest`, `Float16LogicalTypeTest`, `LocalTimestampTest`.

## Timestamps

Users' view of the two kinds is in [docs/content/concepts/timestamps.md](../docs/content/concepts/timestamps.md).

**Two kinds.** `TIMESTAMP(isAdjustedToUTC = true)` is an instant and reads as `Instant`; `isAdjustedToUTC = false` is a wall-clock value with no zone and reads as `LocalDateTime`. The accessors are split along the flag (`getTimestamp` / `getLocalTimestamp` on `FieldAccessor`, `getTimestampValue` / `getLocalTimestampValue` on `PqMap.Entry`, `timestamps()` / `localTimestamps()` on `PqList`), and `TimestampAccessorKind` rejects the wrong one. `getValue` returns whichever type the flag names (`LogicalTypeConverter.longToTemporal`, or `Flba12Timestamps.toTemporal` for the 12-byte carrier); `getRawValue` returns the stored value. The split converter entry points `longToTimestamp` and `longToLocalTimestamp` take the stored `long` and a unit and check no kind, since a primitive signature carries no annotation to check; the check lives at the accessor, where it can name the column. A legacy `TIMESTAMP_*` converted type promotes to the UTC-adjusted kind ([Annotation model](#annotation-model)). `TIME` has one accessor, `getTime`, for both flag values.

**Units.** `MILLIS`, `MICROS` and `NANOS` count from the Unix epoch in both kinds; a negative count is a value before the epoch.

**INT96.** The legacy `INT96` carries no annotation: 8 little-endian bytes of nanoseconds of the day and a 4-byte Julian day. By the Spark / Hive convention an unannotated `INT96` reads as a UTC `Instant` (`LogicalTypeConverter.int96ToInstant`) through `getTimestamp`, `getValue` and the flyweights at every depth; `getLocalTimestamp` rejects it. The convention keys on the annotation's absence (`LeafKind.INT96_TIMESTAMP`), which is sound because `FileSchema` drops every annotation `INT96` cannot carry, so an annotation that survives on one decides its decode as on any other column. One instant has several `INT96` encodings, which is why min/max bounds are never read for the type ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md#int96-and-stored-byte-comparisons)). The writer does not produce `INT96`.

**`FIXED_LEN_BYTE_ARRAY(12)`.** parquet-format after 2.14.0 lets `TIMESTAMP` sit on 12 bytes holding a signed two's complement little-endian 96-bit count of the unit since the epoch. The annotation is the ordinary `TimestampType`, with all three units and both flags; the legacy converted types annotate an `INT64` only, so this carrier is union-only. It is unrelated to `INT96`, which is also 12 bytes.

- `Flba12Timestamps` (`internal.conversion`) decodes and encodes the layout for the row readers, `getValue`, the writer's `PhysicalValueConverter` and the CLI. The filter resolver encodes its range-checked literal itself (`FilterPredicateResolver.fixedTimestampBytes`), as the two's complement of the count reversed to little-endian.
- The parquet-java compat layer gives the column no `OriginalType`, since the legacy timestamp types annotate an `INT64` only, and a `Group` receives the 12 stored bytes as `Binary`.
- A value is held as two words, `hi` (top 32 bits, signed) and `lo` (low 64 bits, unsigned). Splitting it into seconds and the sub-second remainder is a floor division by the unit's per-second count, done over 32-bit limbs in integer arithmetic, never `BigInteger`, because for nanoseconds the carrier exists for values beyond a `long`. Encoding is the inverse, computing `epochSecond × d` with `Math.multiplyHigh` and adding with carry.
- `Instant` and `LocalDateTime` span about a billion years either side of the epoch, which contains the SQL range years 0001–9999 but not the 96-bit space. A stored value outside it raises `DateTimeException` naming the stored bytes, whether its seconds overflow a `long` or fit but lie past the Java type. The stored bytes stay reachable through `getBinary`, `getRawValue` and `ColumnReader`. The writer has no range to check: every `Instant` and `LocalDateTime` in nanoseconds fits 96 bits.
- The typed accessors decode in place from the batch's packed bytes (`BinaryBatchValues.flba12InstantAt` / `flba12LocalDateTimeAt`); the readers branch on the physical type where they read the stored value.
- Order is signed over the represented value: pre-epoch values are negative and their bytes do not order as unsigned strings. The writer collects bounds in `ByteColumnOrder.SIGNED_LITTLE_ENDIAN`, as `AnnotationPairings.byteColumnOrder` picks it, the reader compares with `BinaryComparator.compareSignedLittleEndian` under `Comparison.FIXED_TIMESTAMP`, and bounds are never truncated. Filter literals are in [PREDICATE_MODEL.md](PREDICATE_MODEL.md#per-column-type).

**Variant timestamps.** The Variant encoding has four timestamp tags. `PqVariant.asTimestamp` reads `TIMESTAMP` and `TIMESTAMP_NANOS` as `Instant`, `asLocalTimestamp` reads `TIMESTAMP_NTZ` and `TIMESTAMP_NTZ_NANOS` as `LocalDateTime`, and each rejects the other set through `VariantErrors.expectedOneOf`.

Tests: `LocalTimestampTest`, `NestedInt96TimestampTest`, `Int96TimestampTest` (parquet-testing-runner), `Flba12TimestampsTest`, `Flba12TimestampReadTest`, `Flba12TimestampTest` (parquet-testing-runner), `WriterFlba12TimestampTest`.

## Variant

A Variant column is a group annotated `VARIANT(specVersion)` whose children hold the self-describing Variant binary encoding: `metadata` (the field-name dictionary) and `value` (the payload), and in the shredded form a `typed_value` sibling holding a typed projection of the payload. The user-facing API and its limitations are in [docs/content/how-to/variant.md](../docs/content/how-to/variant.md).

**Schema.** `LogicalTypeReader` reads union field 16 with an optional `specification_version` defaulting to `1`; a version below `1` raises `ParquetReadException`. `FileSchema` validates each Variant group when it is built: two or three children, named `metadata`, `value` and optionally `typed_value` in that order, the first two `BYTE_ARRAY` primitives. A group of another shape raises `IllegalArgumentException` naming the group, which opening the file surfaces as `ParquetReadException`, so a malformed file never yields empty Variants downstream. Repetition of the children and the shape of `typed_value` are not checked there; the carrier check is part of shredded reassembly below.

**Public surface.** `PqVariant` exposes the raw `metadata()` and `value()` bytes, `type()` (the `VariantType` enum of the encoding's type tags), typed `as*` extraction, and `asObject()` / `asArray()`. A mismatched `as*` call raises `VariantTypeException`. `PqVariantObject` implements `FieldAccessor`, the primitive getter surface it shares with `PqStruct`; the schema-typed `getStruct`, `getList` and `getMap` stay on `StructAccessor extends FieldAccessor`, which a Variant object does not implement, since Variant has no schema-typed sub-structures. Variant-side nesting uses the encoding's terms, `getObject` and `getArray`, so a call site distinguishes a Parquet struct from a Variant object. `getVariant` lives on `FieldAccessor`, reaching a Variant from a Parquet struct or from inside a Variant object. Whether a field is a Variant is read from the schema (`SchemaNode.GroupNode.isVariant()`); `FieldAccessor` has no per-field Variant test.

**Decoder.** `internal.variant` holds the byte-level codec (`VariantBinary`, `VariantMetadata`, `VariantValueDecoder`, `VariantValueEncoder`) and the flyweights `PqVariantImpl`, `PqVariantObjectImpl`, `PqVariantArrayImpl`. Navigation into an object field or array element returns a flyweight over the same buffers at another offset, without copying. Every offset read from the encoding is checked against its buffer before it is dereferenced, and nesting depth is capped (`VariantValueDecoder.descend`), so a malformed value fails with an exception at the offending offset or depth. A field lookup by name binary-searches the dictionary when its header declares it sorted and scans it otherwise. `metadata()` and `value()` return copies; `value()` of a nested flyweight walks the encoding for its extent.

**Reading.** Variant accessors run on the nested row reader (`NestedBatchDataView`, `PqStructImpl`, `PqListImpl`, `PqMapImpl`); `TopLevelFieldMap` describes a Variant field with the projected indices of its children and a `ShredLevel` tree built once from the schema. Projecting any part of a Variant pulls in all its leaves ([ROW_READER.md](ROW_READER.md)). An unshredded Variant wraps the row's `metadata` and `value` bytes directly. A repeated `VARIANT` group outside a `LIST` or `MAP` group is a list of variants in the bare form, which the reader does not serve: it keeps its annotation, and a read touching its columns is refused with `UnsupportedOperationException` when the reader is built ([NESTED_DECODE.md](NESTED_DECODE.md#unannotated-repeated-fields)).

**Shredded reassembly.** When `typed_value` is present, `VariantShredReassembler` (`internal.reader`) rebuilds the canonical `value` bytes for the row, so a caller sees the same encoding however the file was written, and `metadata` passes through unchanged. It walks the `ShredLevel` tree, whose levels are a `Primitive`, an `Array` (a `LIST` whose element is itself a shredded level) or an `Object` (a group of named shredded levels):

| Level | `typed_value` present | only `value` present | neither |
|---|---|---|---|
| primitive | encode the typed value with its Variant tag | pass `value` through | missing |
| array | reassemble each element, a missing element as Variant `NULL`, emit an array | pass `value` through | missing |
| object | reassemble each field, merge the fields of a partial `value` object, emit an object | pass `value` through | missing |

- The carrier of each primitive `typed_value` is resolved once, when the `ShredLevel` tree is built (`ShredLevel.Carrier`), and must be one the Variant shredding specification lists: `BOOLEAN`; `INT32` unannotated or `INT(8/16/32, signed)`, `DECIMAL`, `DATE`; `INT64` unannotated or `INT(64, signed)`, `DECIMAL`, `TIME(MICROS)` local, `TIMESTAMP(MICROS / NANOS)`; `FLOAT`; `DOUBLE`; `BYTE_ARRAY` unannotated, `STRING`, `DECIMAL`; `FIXED_LEN_BYTE_ARRAY` `UUID`, `DECIMAL`. Any other carrier, `INT96` included, makes the file invalid and raises `ParquetReadException` naming the file and the type when a row reader that projects or filters on the Variant column, or a column reader filtered on it, is built; a read that does not touch the Variant builds no `ShredLevel` tree and is not refused. A rejection on a row with a null `typed_value` would never fire, so the check is per file, not per value; it runs before any column worker starts, or closes the started ones, so a rejected build leaves nothing running.
- A typed value is encoded with the Variant type its carrier names: `INT(8/16)` as `INT8` / `INT16`; `DECIMAL` as `DECIMAL4` on `INT32`, `DECIMAL8` on `INT64`, `DECIMAL16` on `BYTE_ARRAY`, and at the narrowest width its precision fits on `FIXED_LEN_BYTE_ARRAY`; `TIME` as micros; unannotated bytes as binary.
- An object's fields are emitted sorted by name, with ids resolved against the row's metadata. A field present in both the shredded struct and the partial `value` object is a malformed file and raises `ParquetReadException`, as does a field id or name the metadata does not hold.
- A missing field is omitted from its object. A present Variant group with neither side populated reads as a Variant `NULL`, top-level and as a struct field alike (the reassembler produces it); a null Variant group reads as Java `null`.
- Every call returns the reassembled value in a fresh array, which `PqVariantImpl` wraps.

Shredded reassembly is checked byte for byte against the `shredded_variant/*.variant.bin` expectations of apache/parquet-testing.

**Not read as values.** The leaves under a Variant group hold an encoded payload; a comparison or set predicate on one is refused at resolution, and `isNull` / `isNotNull` apply ([PREDICATE_MODEL.md](PREDICATE_MODEL.md)). The Avro binding materializes a Variant as a record of its two byte fields ([AVRO_BINDING.md](AVRO_BINDING.md)). The writer does not build Variant groups: `LogicalTypeValidator` rejects `VARIANT` on a declared leaf ([WRITER_INPUT.md](WRITER_INPUT.md#boundaries)).

Tests: `VariantSchemaTest`, `VariantLogicalTypeTest`, `VariantMetadataTest`, `VariantValueDecoderTest`, `VariantValueEncoderTest`, `PqVariantInvalidInputTest`, `ShredLevelTest`, `VariantShredReassemblerTest`, `VariantShredReassemblerTest` (parquet-testing-runner), `VariantInRepeatedAccessorsTest`, `FieldAccessorSplitTest`.

## Geospatial

`GEOMETRY` (union field 17) is planar geometry and `GEOGRAPHY` (field 18) geodesic geometry on an ellipsoid; both annotate a `BYTE_ARRAY` of WKB. The user-facing API is in [docs/content/how-to/geospatial.md](../docs/content/how-to/geospatial.md).

**Annotations.** `GeometryType(crs)` and `GeographyType(crs, edgeInterpolation)`. An absent CRS is materialized as the format's default `OGC:CRS84` by the factories and by `LogicalTypeReader`, so a read annotation never carries a `null` CRS. `algorithm` is a Thrift enum (an `i32`), not a union; `LogicalType.EdgeInterpolationAlgorithm` has `SPHERICAL` (the default when absent), `VINCENTY`, `THOMAS`, `ANDOYER`, `KARNEY`, and `UNKNOWN` for a value added to the format after this build. The payload is never decoded: `getBinary` and `getValue` return the WKB bytes.

**Statistics.** `ColumnMetaData.geospatialStatistics()` (field 17) holds a `GeospatialStatistics` of an optional `BoundingBox` (`xmin`, `xmax`, `ymin`, `ymax` required; `z` and `m` ranges optional) and the list of WKB geometry type codes present, empty when unknown. For a `GEOGRAPHY`, x is longitude and y latitude. `xmin > xmax` denotes a box that wraps across the antimeridian; the format defines it for `GEOGRAPHY`, and pruning treats a stored box that way on either type. The format defines them per column chunk only; the column index has no geospatial field.

**`intersects`.** `FilterPredicate.intersects(column, xmin, ymin, xmax, ymax)` resolves only on a `GEOMETRY` or `GEOGRAPHY` column, otherwise raising `IllegalArgumentException` at reader creation; it has no inverse, so `not` over it is refused ([PREDICATE_MODEL.md](PREDICATE_MODEL.md)). It decides whole row groups:

- `RowGroupFilterEvaluator.geospatialDecision` returns `CANNOT_MATCH` when the chunk's box and the query box are disjoint, and `MIGHT_MATCH` otherwise, including when the chunk carries no statistics or no box.
- The x test goes through `Geospatial.xAxisOverlaps`, which splits a wrapping range into its two halves; two wrapping ranges always overlap, since both contain the antimeridian. The y test is a plain interval test.
- Pages are not pruned by it (`PageFilterEvaluator` yields every row; `UnitStats` answers `MIGHT_MATCH`), and the record matcher accepts every row, so a surviving row group returns all its rows, nulls included.

The decision joins the row-group fold described in [STATISTICS_PRUNING.md](STATISTICS_PRUNING.md#row-groups).

**Writing.** The writer accepts both annotations on `BYTE_ARRAY` and emits them union-only ([Writing annotations](#writing-annotations)). It writes no `min` / `max` for them, since the format defines no order ([WRITER_ENCODING.md](WRITER_ENCODING.md#sort-order)), and no `GeospatialStatistics`, so `intersects` prunes nothing in a Hardwood-written file.

Tests: `GeospatialStatisticsTest`, `GeospatialEndToEndTest`, `GeospatialNaNFilterTest` (parquet-testing-runner), `LogicalTypeWriterTest`.

## Writing annotations

A leaf declared through `FileSchema.Builder` with a `LogicalType` is validated by `LogicalTypeValidator` ([WRITER_INPUT.md](WRITER_INPUT.md#physical--logical-legality)). The writer then emits it as both the `LogicalType` union and, where one exists, the legacy `converted_type` (with `scale` / `precision` for `DECIMAL`), because parquet-format requires writers to write the union and the corresponding converted type for older readers. Both are derived from the one declared annotation in `FileSchema.toSchemaElements`, through `LogicalTypeAnnotations.of(physicalType, logicalType)` for a leaf and `ofGroup(convertedType, logicalType)` for a group: these are the inverse of `LeafAnnotation.effective`. Deriving at lowering time means a read-then-rewrite also writes both forms, so a legacy `DECIMAL`'s `scale` and `precision` survive.

| Annotation | `converted_type` | Union member |
|---|---|---|
| `STRING` | `UTF8` | 1 |
| `MAP`, `LIST` | `MAP`, `LIST` | 2, 3 |
| `ENUM` | `ENUM` | 4 |
| `DECIMAL(p, s)` | `DECIMAL`, `scale = s`, `precision = p` | 5 |
| `DATE` | `DATE` | 6 |
| `TIME(*, MILLIS / MICROS)` | `TIME_MILLIS` / `TIME_MICROS` | 7 |
| `TIMESTAMP(*, MILLIS / MICROS)` on `INT64` | `TIMESTAMP_MILLIS` / `TIMESTAMP_MICROS` | 8 |
| `INT(w, signed)` | `INT_w` / `UINT_w` | 10 |
| `JSON`, `BSON` | `JSON`, `BSON` | 12, 13 |
| `INTERVAL` | `INTERVAL` | none |
| `TIME` / `TIMESTAMP` at `NANOS`, `TIMESTAMP` on `FIXED_LEN_BYTE_ARRAY(12)`, `NULL`, `UUID`, `FLOAT16`, `VARIANT`, `GEOMETRY`, `GEOGRAPHY` | none | 7 / 8, 8, 11, 14, 15, 16, 17, 18 |

- **The legacy `TIME` / `TIMESTAMP` form ignores `isAdjustedToUTC`.** The legacy annotations denoted UTC-normalized values, but parquet-format requires writers to annotate local values with them too, for compatibility with libraries that did so before the union existed. A pre-union reader therefore reads a local timestamp as UTC; a union-aware reader takes the union and is exact.
- **`INTERVAL` is `converted_type` only.** Union field 9 is reserved for it and never defined, so `LogicalTypeWriter` refuses to write it, and `LogicalTypeReader` treats field 9 as an unrecognized member: it promises no layout, and a column that is an `INTERVAL` says so through its `converted_type`.
- **A legacy `MAP_KEY_VALUE`** has no union member and passes through `ofGroup` unchanged.

`LogicalTypeWriter` (`internal.thrift`) is the inverse of `LogicalTypeReader`: a union is a struct with one field set, whose id selects the member. Both take a member's field id from `AnnotationKind`. `TIME` / `TIMESTAMP` write `isAdjustedToUTC` as a compact-protocol boolean field and the unit as a nested `TimeUnit` union; `INT` writes `bitWidth` as an `i8`. `GEOMETRY` and `GEOGRAPHY` omit a `null` CRS, which the reader reads back as `OGC:CRS84`, and `GEOGRAPHY` writes `algorithm` as an `i32` of the enum's value. An `UNKNOWN` algorithm has no value to write: `LogicalTypeValidator` refuses it where the column is declared, and on any other schema when the writer is created, before data is written, and `LogicalTypeWriter` refuses to write it.

An annotation also selects the column's statistics order on write ([WRITER_ENCODING.md](WRITER_ENCODING.md#sort-order)) and the value ranges the write APIs enforce ([WRITER_INPUT.md](WRITER_INPUT.md#annotation-ranges)).

Tests: `FileSchemaLogicalTypeTest`, `LogicalTypeWriterTest`, `LogicalTypeReferenceCodecTest` (each union member against parquet-format-structures, both directions), `WriterLogicalTypeRoundTripTest`, `WriterLogicalTypeInteropTest` (parquet-testing-runner).

## Boundaries

- **Cast failures carry no message** (#971). A typed accessor outside the five guarded ones fails with a bare `ClassCastException` or `NullPointerException` on a mismatched column.
- **Shredded Variants in repeated contexts** (#467). A shredded Variant as a list element or map value raises `UnsupportedOperationException`; the unshredded form reads.
- **Shredded Variant bytes are materialized eagerly** (#308): every `getVariant` on a shredded column re-encodes and copies the whole value, even for a caller that reads one field.
- **Bare repeated Variant groups** (#1378). A repeated `VARIANT` group outside a `LIST` or `MAP` group is refused rather than read as a list of variants.
- **Variant spec version** (#777). Any `specVersion >= 1` is accepted and parsed as version 1.
- **Shredding invariants** (#315, #812). The reader does not reject every shredded layout parquet-testing classifies as invalid, such as a non-object level with both `value` and `typed_value` populated, where the typed side wins.
- **No Variant-aware filtering or path projection** (#309, #700), and no typed primitive-array access on `PqVariant` (#314).
- **`intersects` is row-group coarse** (#414). No row-level geometry test; a surviving row group returns every row.
- **`ColumnReader` exposes physical values only** (#514).
