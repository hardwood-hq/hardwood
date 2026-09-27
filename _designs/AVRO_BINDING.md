# Avro binding

How the `hardwood-avro` module reads Parquet rows as Avro `GenericRecord`s: its public surface, the mapping from a Parquet schema to an Avro schema, the decode plan that carries every per-value decision from the Parquet schema to materialization, and how Parquet names become legal, unique Avro names.

Related documents:

- [ROW_READER.md](ROW_READER.md): the row reader it wraps
- [RECORD_FILTERING.md](RECORD_FILTERING.md): filtering
- [NESTED_DECODE.md](NESTED_DECODE.md): how nested values are assembled
- [VALUE_DECODE.md](VALUE_DECODE.md): leaf decoding
- [LOGICAL_TYPES.md](LOGICAL_TYPES.md): the meaning of each Parquet annotation (timestamps, Variant, geospatial)
- [docs/content/how-to/avro.md](../docs/content/how-to/avro.md): the user-facing API

## Module surface

`hardwood-avro` depends on `hardwood-core` and `org.apache.avro:avro`. Core has no Avro dependency, so a reader that does not produce `GenericRecord`s does not carry Avro and the Jackson it pulls in. Its public package `dev.hardwood.avro` holds two top-level types:

| Type | Role |
|---|---|
| `AvroReaders` | Static entry point over a `ParquetFileReader`: `rowReader(reader)` for all rows and columns, `buildRowReader(reader)` for a `RowReaderBuilder` with `projection`, `filter`, `head` and `tail` |
| `AvroRowReader` | `hasNext`, `next` returning a `GenericRecord`, `getSchema`, `close` |

Everything else lives in `dev.hardwood.avro.internal`: `AvroSchemaConverter` (schema conversion and plan building), `AvroPlanNode` (the decode plan) and `AvroNames` (name resolution). A caller obtains the converted schema only from an open reader, through `AvroRowReader.getSchema()`. The two schema property keys, `hardwood.parquetName` and `hardwood.unsignedInt32`, are constants on the internal converter; users read them by their string value.

`RowReaderBuilder` forwards every setting to `ParquetFileReader.RowReaderBuilder` unchanged, so projection, filter pushdown, record filtering, `head` and `tail` have the core row reader's semantics, including over a multi-file `ParquetFileReader`. Projection paths name Parquet columns, never the rewritten Avro names. `AvroRowReader` owns the `RowReader` it wraps and closes it; it does not own the `ParquetFileReader`.

**Conversion precedes the reader.** `build()` converts the schema before it builds the underlying `RowReader`. Building a `RowReader` starts a column worker per projected column, and a conversion that failed afterwards would leave those workers running with no `AvroRowReader` to close them. Every conversion rejection in this document therefore surfaces from `build()` with nothing to release. The ordering is untested.

Tests: `AvroRowReaderTest` (hardwood-avro).

## Schema mapping

`AvroSchemaConverter.plan(FileSchema, ColumnProjection)` is the single conversion entry point. It returns the root of the decode plan, whose `avro()` is the converted record schema. There is no entry point that returns a bare `Schema`, so no caller holds a converted schema without the plan that reads it; `AvroPlanNode` is constructed only by the converter. The mapping follows parquet-java's `AvroSchemaConverter` and diverges from it in accepting more: a map key annotated `ENUM` or `JSON` converts, where parquet-java requires `STRING`.

### Primitives

| Parquet physical | Annotation | Avro schema | Kind | Java value |
|---|---|---|---|---|
| `BOOLEAN` | — | `boolean` | `BOOLEAN` | `Boolean` |
| `INT32` | —, `INT(8/16/32, signed)`, `INT(8/16, unsigned)` | `int` | `INT` | `Integer` |
| `INT32` | `INT(32, unsigned)` | `long` + `hardwood.unsignedInt32` | `UNSIGNED_INT32` | `Long`, widened unsigned |
| `INT32` | `DATE` | `int` + `date` | `INT` | `Integer`, days since epoch |
| `INT32` | `TIME(MILLIS)` | `int` + `time-millis` | `INT` | `Integer` |
| `INT64` | —, `INT(64, signed or unsigned)` | `long` | `LONG` | `Long`; an unsigned value at or above 2^63 is negative |
| `INT64` | `TIME(MICROS)` | `long` + `time-micros` | `LONG` | `Long` |
| `INT64` | `TIME(NANOS)` | `long` | `LONG` | `Long` |
| `INT64` | `TIMESTAMP(MILLIS/MICROS)` | `long` + `timestamp-*` when adjusted to UTC, `local-timestamp-*` otherwise | `LONG` | `Long` |
| `INT64` | `TIMESTAMP(NANOS)` | `long` | `LONG` | `Long` |
| `INT32`, `INT64`, `BYTE_ARRAY` | `DECIMAL(p, s)` | `bytes` + `decimal(p, s)` | `DECIMAL` | `ByteBuffer` of the unscaled two's-complement big-endian bytes |
| `FIXED_LEN_BYTE_ARRAY(n)` | `DECIMAL(p, s)` | `fixed(n)` + `decimal(p, s)` | `FIXED` | `GenericData.Fixed` of the stored bytes |
| `FLOAT` | — | `float` | `FLOAT` | `Float` |
| `DOUBLE` | — | `double` | `DOUBLE` | `Double` |
| `BYTE_ARRAY` | —, `BSON`, `GEOMETRY`, `GEOGRAPHY` | `bytes` | `BINARY` | `ByteBuffer`; geospatial values are the WKB payload |
| `BYTE_ARRAY` | `STRING`, `ENUM`, `JSON` | `string` | `STRING` | `String` |
| `FIXED_LEN_BYTE_ARRAY(16)` | `UUID` | `string` + `uuid` | `UUID` | canonical UUID `String` |
| `FIXED_LEN_BYTE_ARRAY(n)` | — | `fixed(n)` | `FIXED` | `GenericData.Fixed` |
| `FIXED_LEN_BYTE_ARRAY(12)` | `TIMESTAMP` | `fixed(12)` | `FIXED` | `GenericData.Fixed` of the stored bytes |
| `FIXED_LEN_BYTE_ARRAY(12)` | `INTERVAL` | `fixed` named `interval`, size 12 | `FIXED` | `GenericData.Fixed` |
| `FIXED_LEN_BYTE_ARRAY(2)` | `FLOAT16` | `fixed` named `float16`, size 2 | `FIXED` | `GenericData.Fixed` |
| `INT96` | — | `fixed(12)` | `FIXED` | `GenericData.Fixed` of the stored bytes |
| any | `NULL` | `null` | `NULL` | always `null` |

Values keep Avro's raw representation: temporal values are the stored number, and no Avro logical-type conversion is applied. Avro has no nanosecond logical types in the Avro version the module builds against, and no timestamp wider than a long, so `NANOS` columns carry a plain `long` and a 96-bit timestamp its stored bytes. A `fixed` other than `interval` and `float16` is a named type called after its column (see [Name resolution](#name-resolution)). The width of a `FIXED_LEN_BYTE_ARRAY` is its declared `type_length`; conversion rejects a column that declares none.

### Groups and repetition

| Parquet | Avro |
|---|---|
| Group with neither annotation nor converted type | `record`, one field per retained child |
| `LIST` group | `array` of the element schema; the element follows the format's backward-compatibility rules (`GroupNode.getListElement`, see [NESTED_DECODE.md](NESTED_DECODE.md#legacy-list-element-rules)) |
| `MAP` group | `map` of the value schema; the key must convert to `STRING` |
| `MAP` group whose `key_value` has no value field | `map` of `null` |
| `VARIANT` group, shredded or not | `record { metadata: bytes, value: bytes }` named after the group, carrying the canonical Variant bytes; `typed_value` does not appear |
| `OPTIONAL` field, list element or map value | `[null, T]`, or bare `null` when `T` is `null` (Avro unions reject duplicate branches) |
| `REQUIRED` | `T` |
| `repeated` field outside a `LIST` or `MAP` group ([NESTED_DECODE.md](NESTED_DECODE.md#unannotated-repeated-fields)) | non-null `array` of the required element: the element's own type for a primitive, a `map` for a group that reads as a legacy map, a `record` for any other group |

Synthetic levels (`list`, `key_value`) are collapsed during conversion and appear in no schema or name. Error messages name value paths without them, except the two rejections core raises: the `FIXED_LEN_BYTE_ARRAY` width rejection (`FixedWidthValidator`) and the bare repeated `VARIANT` refusal, which name the full Parquet path.

A group that is neither a list, a map nor a Variant is a record, as the row reader serves it as a struct; that includes a group carrying an annotation other than `LIST`, `MAP` or `VARIANT`. Conversion also rejects a `LIST` group with no discoverable element and a `MAP` group with no discoverable key.

Core drops any annotation other than `VARIANT` from a bare repeated group when the file is opened, so such a group converts as an unannotated one ([NESTED_DECODE.md](NESTED_DECODE.md#unannotated-repeated-fields)). A projected bare repeated `VARIANT` group is refused with core's own `UnsupportedOperationException` (`BareRepeatedGroups.refuseTouchedVariants`), since conversion meets it before the row reader does; a projection that excludes it converts.

**Map keys.** An Avro map fixes its keys as strings, and materialization reads each key through `PqMap.Entry.getStringKey`. A map whose key is not a `BYTE_ARRAY` annotated `STRING`, `ENUM` or `JSON` is rejected, naming the map's path and the key type. A `UUID` key converts to an Avro string but is rejected too: `getStringKey` refuses a key that is not text (`LogicalAccessorKind.requireText`), so the map would fail on its first row instead of at `build()`. The check covers the projected schema only, so a file with such a map reads through a projection that excludes it.

**Projection.** With a projection, a record retains only the children that contain a projected leaf, recursively through structs, list elements and map values, in the order the projection names them (the order a `RowReader` indexes them; see [ROW_READER.md](ROW_READER.md#index-space)). A map's key is always retained.

**Rejections.** Every conversion rejection surfaces from `build()` and follows [EXCEPTION_MODEL.md](EXCEPTION_MODEL.md); unlike the core readers' messages, a conversion message names the column or group path but not the file.

| Rejection | Exception |
|---|---|
| `FIXED_LEN_BYTE_ARRAY` with no or a non-positive `type_length` (through core's `FixedWidthValidator`), `LIST` group with no element, `MAP` group with no key | `SchemaIncompatibleException` |
| Map key that is not a string, projected bare repeated `VARIANT` group, root named after a canonical `fixed` type in use, two siblings with one raw name | `UnsupportedOperationException` |
| A node the name resolver did not name | `IllegalStateException` (a converter defect) |

Tests: `AvroSchemaConverterTest`, `AvroRowReaderTest` (hardwood-avro).

## Decode plan

The Avro schema is a lossy projection of the Parquet schema. `UINT_32` becomes a plain `long`, `DECIMAL` over `FIXED_LEN_BYTE_ARRAY` and over `INT64` both become "decimal", and a Variant group converts to the same record as an ordinary group of two `bytes` fields named `metadata` and `value`. A reader that chose its accessor from the Avro schema would guess, and for the Variant case guess wrong. Materialization therefore takes every per-value decision from the Parquet schema, once per value position, while the schema is converted.

The decision is recorded in a tree of `AvroPlanNode`, one node per value position of the converted schema. Each node pairs the `SchemaNode` the value is read from, the Avro `Schema` it materializes into, and a `Kind`, the closed set of ways a value is read:

| Kind | Read through | Avro value |
|---|---|---|
| `BOOLEAN`, `INT`, `LONG`, `FLOAT`, `DOUBLE` | raw accessor | boxed primitive |
| `UNSIGNED_INT32` | raw accessor, `Integer.toUnsignedLong` | `Long` |
| `BINARY` | raw accessor | `ByteBuffer` |
| `FIXED` | raw accessor | `GenericData.Fixed` of the declared size |
| `STRING` | string accessor | `String` |
| `UUID` | UUID accessor | canonical `String` |
| `DECIMAL` | decimal accessor | `ByteBuffer` of the unscaled bytes |
| `STRUCT` | struct accessor | nested `GenericRecord` |
| `VARIANT` | Variant accessor | two-field `GenericRecord` of the canonical bytes |
| `LIST` | list accessor | `java.util.List` |
| `MAP` | map accessor | `java.util.Map` with `String` keys |
| `NULL` | none | a non-null value is an invariant failure |

Children are positional. A `STRUCT` node has one child per field of its record, at the field's Avro position; a `LIST` node has the element plan (`listElement()`); a `MAP` node has the value plan (`mapValue()`), a `NULL` leaf for a key-only map. Every other node, a `VARIANT` node included, is a leaf.

**Invariants.**

- The plan and the schema are built in one traversal, because the pairing is only knowable where both trees are walked together. A projection prunes both in the same step. `AvroPlanNode.record` rejects a record node whose child count differs from its field count: a plan that drifted from its record would read every later field through a neighbour's accessor.
- A node's `avro()` is the resolved schema, the `T` of an optional position's `[null, T]`. The union stays on the enclosing field, list or map schema, so materialization never resolves a union.
- No logical-type lookup, schema property read or union resolution happens per value.
- Where Avro cannot carry a Parquet distinction, the converted schema is annotated from the node's `Kind`: an `UNSIGNED_INT32` schema gets `hardwood.unsignedInt32`. The annotation is output for consumers holding the schema alone; nothing reads it back.

Tests: `AvroSchemaConverterTest`, `AvroRowReaderTest` (hardwood-avro).

## Materialization

`AvroRowReader.next()` advances the `RowReader` and walks the plan. `RowReader` and `PqStruct` are both `StructAccessor`, so the row and a nested struct are one traversal (`materializeRecord`), differing only in the plan node. A record field is read by its Parquet name, `node.source().name()`, never by its Avro field name, which the name resolution may have rewritten. A null field, list element or map value is put as `null` before its `Kind` is consulted.

List elements and map values switch on the same `Kind` through their own accessors, since `PqList` and `PqMap.Entry` are addressed by position and entry rather than by name:

| Kinds | Field and map value | List element |
|---|---|---|
| `BOOLEAN`, `INT`, `LONG`, `FLOAT`, `DOUBLE`, `UNSIGNED_INT32`, `BINARY`, `FIXED` (the raw kinds) | `getRawValue` | `getRaw(index)` |
| `STRING`, `UUID`, `DECIMAL`, `STRUCT`, `VARIANT`, `LIST`, `MAP` | the typed accessor | `get(index)`, checked against the class the kind requires |

The raw kinds share one conversion (`rawToAvro`) at all three positions. A temporal column is therefore the stored number wherever it sits; one that materialized as a number in one position and a `java.time` object in another would produce records that contradict their own schema.

**Validation.** Every value is checked against the representation its `Kind` requires before conversion: a raw or generic value for its Java class, a typed accessor's result for an unexpected null, a `fixed` payload for its declared width. A mismatch throws `IllegalArgumentException` naming the position (`field`, `struct field`, `list element`, or `map value for key`), the Avro type, and the actual Java type. The Avro type is qualified with the `Kind` where the two differ, so a `UINT_32` column reads as `Avro LONG (UNSIGNED_INT32)`. The message carries the `[fileName] ` prefix of the core readers ([EXCEPTION_MODEL.md](EXCEPTION_MODEL.md#error-messages)), taken from the underlying reader through the internal `FileAwareRowReader`, so a multi-file read names the file of the current row. The location is flat: a nested failure names the innermost position only. A mismatch means the plan and the row reader disagree, which is a defect, not a property of the file.

`ENUM` materializes through `STRING`. Parquet declares no symbol set, so there is no membership to validate.

Tests: `AvroRowReaderTest` (hardwood-avro).

## Name resolution

Parquet permits any UTF-8 name; Avro names match `[A-Za-z_][A-Za-z0-9_]*`, and every record and `fixed` needs a full name unique within the schema. `AvroNames` resolves every name once per conversion, over the complete unprojected tree and keyed by schema-node identity, and the converter reads the resolved names from it. The grammar check and the rewrite are `SchemaNames` in core.

**Sanitization.** A legal name is kept. Otherwise every UTF-16 code unit outside `[A-Za-z0-9_]` becomes `_` (a supplementary character becomes `__`), an initial digit gets a leading `_`, and an empty name becomes `_`.

**Sibling scopes.** The fields of one record form a scope; a list element, a map key and a map value each form a scope of their own, so a map key, which appears in no Avro schema, never affects the value's name. Within a scope:

1. Two members with the same raw name are rejected, naming the name and its value path.
2. Members whose candidates collide after sanitization form a group. The member whose raw name is already legal keeps the bare candidate; if none is legal, the member with the smallest raw name in natural `String` order keeps it. Declaration order plays no part, so reordering columns cannot swap two names.
3. Every bare candidate in the scope is reserved before any suffix is handed out. The other members of a group take `_2`, `_3`, … in raw-name order, skipping reserved names.

A suffix is therefore a function of every name in its scope. Siblings `a-b` and `a.b` resolve to `a_b` and `a_b_2`; beside a third sibling literally named `a_b_2` they resolve to `a_b` and `a_b_3`. A legal raw name is never rewritten, so no suffix scheme avoids this.

**Root.** The root's Parquet name is split at the last dot: the final segment is the local name, the earlier segments the namespace, each sanitized on its own.

**Namespaces.** A named type below the root takes as its namespace the root's full name followed by the resolved local names of the value path above it. A `LIST` or `MAP` field contributes its own name; the synthetic `list` and `key_value` levels contribute nothing. `home.address` below root `schema` is `schema.home.address`, the element record of list `items` is `schema.items.element`, and a child named `root` below root `root` is `root.root`. A descendant's full name extends the root's, so it cannot collide with the root.

**Canonical `fixed` types.** `INTERVAL` and `FLOAT16` convert to `fixed` types named exactly `interval` and `float16`, with no namespace, however often they occur. A root named `interval` or `float16` has the same full name as that type, so conversion rejects a schema whose root takes one of these names while some column carries the matching annotation. The check reads the unprojected schema, so a file either converts under every projection or under none.

**Recovering Parquet names.** A record, `fixed` or field named after a Parquet node whose emitted local name differs from its raw Parquet name carries the raw name in the `hardwood.parquetName` property. The root compares its local name with its whole Parquet name, so a dotted root carries the property even when no segment was rewritten (`acme.row` has local name `row` and carries `acme.row`). The property is absent when the local name equals the raw name, is never set on the canonical `interval` and `float16` types, and survives Avro's schema serialization and parsing.

**Invariants.**

- Every emitted record and `fixed` full name is legal and unique. `AvroNames` checks each scope's result and throws `IllegalStateException` on a violation, which would be a resolver defect.
- A legal Parquet name is never rewritten.
- A descendant cannot collide with the root or with the canonical `interval` and `float16` types: its namespace is never empty, and theirs is, so the canonical-type check tests the root alone.
- A descendant's name is stable under projection, and under changes to siblings outside the suffix space its collision group draws from.
- The resolver and the converter classify groups in the same order (Variant, list, map, struct). A node the converter reaches that the resolver did not name fails conversion.

Tests: `AvroNamesTest`, `AvroSchemaConverterTest` (hardwood-avro).

## Boundaries

- **No reader schema.** The Avro schema is always derived from the Parquet schema; a caller-supplied reader schema, and with it Avro schema resolution, is not supported.
- **Bare repeated Variant groups** (#1378). A projected repeated `VARIANT` group outside a `LIST` or `MAP` group is refused instead of converting to an array of Variant records.
- **`SpecificRecord`.** Only `GenericRecord` is materialized (#559).
- **Two conversions.** `hardwood-cli`'s `schema -F AVRO` has its own Parquet-to-Avro conversion with different output (#956).
- **parquet-avro compatibility.** Names follow the rules above, not parquet-avro's occurrence counter (#925); the `AvroParquetReader` shim is #130.
