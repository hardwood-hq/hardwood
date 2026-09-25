# Design: Avro decode plan

**Status: Implemented.** Tracking issue: #896.

## Goal

Materialization of a Parquet row into an Avro `GenericRecord` must take every
per-value decision from the Parquet schema, and must take it once per column
rather than once per value.

The Avro schema is a lossy projection of the Parquet schema: the `VARIANT`
annotation disappears, `UINT_32` becomes a plain `LONG`, and a `FIXED_LEN_BYTE_ARRAY`
decimal and a `BYTES` decimal both arrive as "decimal". A reader that decides how to
read a value by inspecting the converted Avro schema is therefore guessing, and for
Variant it guesses wrong: an ordinary `{metadata, value}` struct of two `bytes`
fields is indistinguishable from a canonical Variant once converted.

## Structure

`AvroSchemaConverter` produces a **decode plan** — a tree of `AvroPlanNode`,
one node per value position in the converted schema. Each node pairs

- the Hardwood `SchemaNode` the value is read from,
- the Avro `Schema` the value is materialized into, and
- a `Kind`: the accessor decision that pairing implies.

```java
AvroPlanNode plan = AvroSchemaConverter.plan(fileSchema, projection);
plan.avro();        // the Avro record schema, as before
plan.kind();        // STRUCT
plan.child(0);      // plan for the first field of the record
```

`Kind` is the closed set of ways a value is read and represented:

| Kind | Accessor | Avro value |
|---|---|---|
| `BOOLEAN` / `INT` / `LONG` / `FLOAT` / `DOUBLE` | `getBoolean` / `getInt` / `getLong` / `getFloat` / `getDouble` | boxed primitive |
| `UNSIGNED_INT32` | `getInt`, widened by `Integer.toUnsignedLong` | `Long` |
| `STRING` | `getString` | `String` |
| `UUID` | `getUuid` | canonical UUID `String` |
| `BINARY` | `getBinary` | `ByteBuffer` |
| `DECIMAL` | `getDecimal` | `ByteBuffer` of the unscaled two's-complement bytes |
| `FIXED` | `getBinary` | `GenericData.Fixed` of the declared width |
| `STRUCT` | `getStruct` | nested `GenericRecord` |
| `VARIANT` | `getVariant` | two-field `GenericRecord` of the canonical Variant bytes |
| `LIST` | `getList` | `java.util.List` |
| `MAP` | `getMap` | `java.util.Map` |
| `NULL` | no non-null value is valid | a non-null value is an invariant failure |

Children are positional:

- a `STRUCT` node has one child per field of its record, at the field's Avro position;
- a `LIST` node has one child, the element plan, reached through `listElement()`;
- a `MAP` node has one child, the value plan, reached through `mapValue()`; a
  key-only map uses a `NULL` value plan because every entry has a null value;
- every other node is a leaf.

Building the plan and building the schema is one traversal, because the pairing is
only knowable where both trees are walked together. Parquet's synthetic levels —
`list.list.element` and `key_value` — are collapsed there, so nothing downstream
re-derives them. A projection prunes the plan and the schema in the same step, which
keeps a `STRUCT` node's children aligned with the fields its record actually has;
`AvroPlanNode` rejects a record node whose child count disagrees with its field count.

Groups are classified the way the row reader classifies them: a group is a struct only
when `GroupNode.isStruct()` says so, not merely because it is neither Variant, list nor
map. A group carrying an annotation conversion does not recognise is rejected there and
then, naming the group and the annotation — converting it to an Avro record the reader
cannot fill would surface as a wrong value rather than an error.

A node's `avro()` is the *resolved* schema — the non-null branch of an `OPTIONAL`
field's `[null, T]` union. The union survives in the enclosing record's field, so the
schema handed to users is unchanged, while materialization never re-resolves a union.

## Materialization

`AvroRowReader` walks the plan, not the schema:

```java
private Object materializeField(StructAccessor accessor, String name, AvroPlanNode node) {
    return switch (node.kind()) {
        case BOOLEAN -> accessor.getBoolean(name);
        ...
        case VARIANT -> materializeVariant(accessor.getVariant(name), node.avro());
        case STRUCT  -> materializeRecord(accessor.getStruct(name), node);
    };
}
```

`RowReader` and `PqStruct` are both `StructAccessor`, so the top-level row and a
nested struct are the same traversal; the plan node supplies what used to differ.
List elements and map entries keep their own switch — `PqList` and `PqMap.Entry`
expose positional and value accessors rather than name-based ones — but they switch
on the same `Kind`. Values obtained through generic `Object` accessors are validated
against the representation their `Kind` requires before conversion; typed object
accessors are checked for an unexpected null after the enclosing null guard. Field and
map positions use typed accessors for logical and group kinds, while list elements use
`PqList.get(index)` and validate its returned object. Physical kinds use raw accessors
at all three positions and share the same type and fixed-width checks. Every mismatch
throws an `IllegalArgumentException` naming the flat materialization position, the Avro
type, and the actual Java value type. The Avro type is qualified with the plan's `Kind`
where the two differ, so a `UINT_32` column reads as `Avro LONG (UNSIGNED_INT32)` rather
than a `LONG` that inexplicably requires an `Integer`. The message carries the
`[fileName] ` prefix the core readers use, read from the underlying reader through
`FileAwareRowReader` so a multi-file read attributes the failure to one file. The
location model is intentionally flat: nested failures name the innermost field, list
element, or map value.

Parquet `ENUM` materializes through the `STRING` kind. The Parquet annotation carries
no declared symbol set, so materialization validates the Java representation as a
non-null `String`; membership validation is not defined for this schema shape.

The `NULL` kind represents a Parquet NULL logical type. A null value is consumed by
the enclosing null guard; reaching a `NULL` node with a non-null value is an invariant
failure. A key-only MAP produces a `map<null>` schema and `NULL` value plan. LIST groups
without a discoverable element and MAP groups without a discoverable key are malformed
and rejected instead of receiving an untyped child plan.

No logical-type lookup, property read, or union resolution happens per value.

The complete plan is built before the underlying `RowReader` is acquired. This keeps
planning failures from opening and then stranding a row-reader resource.

Where Avro's type system cannot carry a Parquet distinction, the converted schema is
annotated from the node's `Kind` — an unsigned `INT32` widens to Avro `LONG` and its
schema gets the `hardwood.unsignedInt32` marker. The annotation is output, describing
the schema for consumers that hold it without the plan; nothing reads it back.

## Public surface

`AvroSchemaConverter.plan(FileSchema, ColumnProjection)` is the only conversion entry
point; a caller that wants nothing but the type mapping takes `plan(...).avro()`. There
is no second entry point returning a bare `Schema`, so no caller can hold a converted
schema whose plan has been discarded. `AvroPlanNode` lives in `dev.hardwood.avro.internal`
and is constructed only by the converter.
