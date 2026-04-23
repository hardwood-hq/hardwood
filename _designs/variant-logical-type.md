# Design: Parquet Variant logical type support

**Status: Phase 1 implemented. Phase 2 (shredded reassembly) pending.**

**Issues:** #74 (Variant logical type recognition + read API), #286 (Shredded Variant reassembly)

## Goal

Recognize and expose Parquet's `VARIANT` logical type as a first-class value in Hardwood's
reader, so callers can navigate the self-describing binary encoding (field lookup by name,
type introspection, nested access) instead of seeing two opaque binary columns.

The work lands in two phases:

- **Phase 1 (#74)** — Recognize the `VARIANT(…)` annotation on group nodes, decode the
  `{metadata, value}` binary encoding, and expose it through a new `PqVariant` row API.
  Covers the non-shredded form produced e.g. by Spark, DuckDB, or PyArrow.
- **Phase 2 (#286)** — Reassemble the shredded form (`{metadata, value, typed_value}`)
  into a canonical `{metadata, value}` binary at read time, so consumers see the same
  representation regardless of how the file was written. Unblocks row-level comparison of
  the 137 `shredded_variant/case-*.parquet` fixtures in `ParquetComparisonTest`.

Both phases ship inside `hardwood-core`. No new module is introduced: the Variant binary
encoding is part of the Parquet spec, and exposing it does not require a third-party
dependency.

## Background — Variant on the wire

A Variant column is encoded as a Parquet group annotated with the `VARIANT` logical type:

```
optional group v (VARIANT(specVersion=1)) {
  required binary metadata;
  required binary value;
}
```

- `metadata` holds the field-name dictionary and a spec version byte.
- `value` is a self-describing byte sequence carrying the actual payload (a primitive,
  object, or array). Objects reference field names by dictionary id.

In the **shredded** form, the group gains an optional `typed_value` sibling holding a
typed projection of the payload. At write time the writer places the real payload in
whichever side fits best:

```
optional group v (VARIANT(specVersion=1)) {
  required binary metadata;
  optional binary value;           // present when the payload is "untyped"
  optional <T>    typed_value;     // present when the payload matches the shredded type T
}
```

`typed_value` itself may be a primitive, a list, or a struct — including nested shredded
substructures with their own `value` / `typed_value` fields.

## User-facing API (end state after both phases)

```java
try (RowReader reader = fileReader.createRowReader()) {
    while (reader.hasNext()) {
        reader.next();
        PqVariant v = reader.getVariant("v");

        // Raw canonical bytes (post-reassembly in Phase 2)
        byte[] metadata = v.metadata();
        byte[] value = v.value();

        // Type introspection
        VariantType type = v.type();   // OBJECT, ARRAY, STRING, INT8, ... NULL

        // Primitive accessors (fail-early if type mismatches)
        if (type == VariantType.STRING) {
            String s = v.asString();
        }

        // Object navigation
        if (v.type() == VariantType.OBJECT) {
            PqVariantObject obj = v.asObject();
            String title = obj.getString("title");              // typed primitive access
            Instant ts   = obj.getTimestamp("ts");
            PqVariantObject addr = obj.getObject("address");    // nested Variant OBJECT
            PqVariantArray  tags = obj.getArray("tags");        // nested Variant ARRAY
            PqVariant       raw  = obj.getVariant("meta");      // escape hatch
        }

        // Array navigation
        if (v.type() == VariantType.ARRAY) {
            PqVariantArray arr = v.asArray();
            for (PqVariant e : arr) { ... }
            PqVariant first = arr.get(0);
        }
    }
}
```

Struct-nested Variants work identically via `PqStruct.getVariant(name)`.

### Consistency with the existing Pq API

`PqVariantObject` shares the primitive getter surface (`getInt` / `getString` /
`getTimestamp` / …) with `PqStruct` by implementing a shared `FieldAccessor`
interface. The Parquet-only complex getters (`getStruct` / `getList` / `getMap`)
stay on `StructAccessor extends FieldAccessor` and do **not** appear on
`PqVariantObject` — Variant has no schema-typed sub-structures to expose, so
exposing those methods would be a dead surface.

Variant-specific complex access uses the spec's own terminology — `getObject(...)`
for nested OBJECT, `getArray(...)` for nested ARRAY — which also makes
Parquet-vs-Variant nesting unambiguous at the call site: `getStruct` means
"schema-typed Parquet struct"; `getObject` means "tag-typed Variant object".

## Phase 1 — Variant recognition and read API (#74)

### 1.1 Thrift-level recognition

`LogicalType` gains a new permitted record:

```java
/// Self-describing semi-structured value per the Parquet Variant spec.
///
/// @param specVersion spec version declared by the writer (currently always `1`)
record VariantType(int specVersion) implements LogicalType {
    public VariantType {
        if (specVersion < 1) {
            throw new IllegalArgumentException("specVersion must be >= 1: " + specVersion);
        }
    }
}
```

`LogicalTypeReader` (`core/src/main/java/dev/hardwood/internal/thrift/LogicalTypeReader.java:40`)
adds a branch for the Variant union tag (field id **16** per
`apache/parquet-format`'s `parquet.thrift`). The variant struct carries a single
optional `specification_version` i8 field; when absent, default to `1` per the spec.
The shape mirrors `readTimeType` / `readTimestampType` (push/pop field id context, read
nested struct, validate).

### 1.2 Schema representation — annotating group nodes

`SchemaNode.GroupNode` (`core/src/main/java/dev/hardwood/schema/SchemaNode.java:65`)
currently tracks only legacy `ConvertedType` on groups. Variant is the first
*modern* `LogicalType` that annotates a group, so `GroupNode` is extended:

```java
record GroupNode(
        String name,
        RepetitionType repetitionType,
        ConvertedType convertedType,
        LogicalType logicalType,           // NEW — null for plain structs
        List<SchemaNode> children,
        int maxDefinitionLevel,
        int maxRepetitionLevel) implements SchemaNode {

    public boolean isVariant() {
        return logicalType instanceof LogicalType.VariantType;
    }
    // isList / isMap / isStruct remain unchanged (driven by convertedType)
}
```

`FileSchema.buildChildren` (`core/src/main/java/dev/hardwood/schema/FileSchema.java:219`)
passes `element.logicalType()` into the new parameter. All existing callers that
construct `GroupNode` (tests, compat shim) are updated to pass `null` explicitly.

Validation (fail-early): when a group is annotated as Variant, its children must be
exactly the required `metadata` binary column and at least a `value` binary column. A
`typed_value` sibling is allowed (Phase 2). Other shapes raise at schema build time —
this prevents corrupt files from silently producing empty Variants downstream.

### 1.3 Variant binary decoder (internal)

A new internal package `dev.hardwood.internal.variant` hosts the byte-level decoder.
It never appears in the public API; `PqVariant` is the only user-visible surface.

```
core/src/main/java/dev/hardwood/internal/variant/
    VariantBinary.java         // header byte constants, type-tag parsing
    VariantMetadata.java       // parses metadata: version, sorted flag, dict
    VariantValueDecoder.java   // lazy decoder over a (metadata, value) pair
```

Key points:

- **Zero-copy by default.** The decoder operates over a `byte[] value` + `int offset`
  + `int length` triple. Navigation into objects / arrays returns new decoder views
  referencing the same backing array — no allocation of sub-arrays.
- **Primitive access avoids boxing.** `asInt()` / `asLong()` / `asDouble()` return
  primitives; boxed entry points (`getObject()`-style) live on `PqVariant`, not the
  decoder.
- **Dictionary lookup is O(log n).** The metadata parser builds two views over the
  metadata buffer: offset table + raw strings. Field lookup by name uses binary search
  when the "sorted" flag is set in the metadata header, linear scan otherwise.
- **Bounds-checked reads.** Every offset decoded from the encoded stream is validated
  against the containing buffer length before dereference (fail-early, never silent
  wrap-around).

`VariantType` enum enumerates the spec's logical types:

```
NULL, BOOLEAN_TRUE, BOOLEAN_FALSE,
INT8, INT16, INT32, INT64,
FLOAT, DOUBLE, DECIMAL4, DECIMAL8, DECIMAL16,
DATE, TIMESTAMP, TIMESTAMP_NTZ, TIME_NTZ,
STRING, BINARY, UUID,
OBJECT, ARRAY
```

### 1.4 Public row API — `PqVariant`, `PqVariantObject`, `PqVariantArray`

New types in the existing `dev.hardwood.row` package, siblings of `PqStruct` /
`PqList` / `PqMap`.

#### `PqVariant` — top-level Variant value

```java
public interface PqVariant {

    // Raw canonical bytes
    byte[] metadata();
    byte[] value();

    // Type introspection
    VariantType type();
    boolean isNull();

    // Primitive extraction — throws VariantTypeException on mismatch
    boolean asBoolean();
    int asInt();
    long asLong();
    float asFloat();
    double asDouble();
    String asString();
    byte[] asBinary();
    java.math.BigDecimal asDecimal();
    java.time.LocalDate asDate();
    java.time.Instant asTimestamp();
    java.util.UUID asUuid();

    // Complex unwrapping — throws VariantTypeException on mismatch
    PqVariantObject asObject();
    PqVariantArray  asArray();
}
```

#### `PqVariantObject` — Variant OBJECT view

`PqVariantObject` implements the shared `FieldAccessor` interface (see below), so
its primitive getters match `PqStruct` exactly. Variant-specific complex access is
named after the spec — `getObject` / `getArray` — to distinguish it from Parquet's
schema-typed `getStruct` / `getList` / `getMap`.

```java
public interface PqVariantObject extends FieldAccessor {

    // (inherited from FieldAccessor)
    //   int getInt(String name);
    //   long getLong(String name);
    //   float getFloat(String name);
    //   double getDouble(String name);
    //   boolean getBoolean(String name);
    //   String getString(String name);
    //   byte[] getBinary(String name);
    //   BigDecimal getDecimal(String name);
    //   LocalDate getDate(String name);
    //   LocalTime getTime(String name);
    //   Instant getTimestamp(String name);
    //   UUID getUuid(String name);
    //   boolean isNull(String name);
    //   int getFieldCount();
    //   String getFieldName(int index);
    //   PqVariant getVariant(String name);

    // Variant-specific complex access
    PqVariantObject getObject(String name);   // nested OBJECT
    PqVariantArray  getArray(String name);    // nested ARRAY
}
```

#### `PqVariantArray` — Variant ARRAY view

```java
public interface PqVariantArray extends Iterable<PqVariant> {
    int size();
    PqVariant get(int index);   // decoded lazily on access
}
```

Elements are always `PqVariant` (heterogeneous); callers inspect `type()` and
unwrap. No primitive-specialized iterators — the Variant binary layout can't
deliver the zero-boxing win those bring on the columnar `PqList`, so adding them
would be ceremony without benefit.

All three types are implemented by internal classes under
`internal.variant` (`PqVariantImpl`, `PqVariantObjectImpl`, `PqVariantArrayImpl`),
each a thin wrapper over a decoder view.

### 1.5 `StructAccessor` split and Variant integration

`StructAccessor` today combines primitive field access (shared with Variant
objects) with Parquet-specific complex access (not applicable to Variant). Split
it into two interfaces:

```java
/// Name-based access to fields whose values are primitives, Variants,
/// or absent. Implemented by any record-shaped accessor — Parquet structs
/// AND Variant objects.
public interface FieldAccessor {
    // primitive getters: getInt, getLong, getFloat, getDouble, getBoolean,
    //                    getString, getBinary, getDecimal, getDate, getTime,
    //                    getTimestamp, getUuid
    // metadata:          isNull(name), isNull(index), getFieldCount,
    //                    getFieldName(index)
    // variant escape:    getVariant(name), isVariant(name)
}

/// Parquet-struct access, adding schema-typed complex accessors that have no
/// Variant analog.
public interface StructAccessor extends FieldAccessor {
    PqStruct getStruct(String name);
    PqList   getList(String name);
    PqMap    getMap(String name);
}
```

`RowReader` and `PqStruct` continue to implement `StructAccessor` (no change for
existing callers). `PqVariantObject` implements only `FieldAccessor`.

The new `getVariant(String)` / `isVariant(String)` pair lives on `FieldAccessor`,
not `StructAccessor`, so Variant fields are reachable from both Parquet structs
(a Parquet struct whose field is Variant-annotated) and Variant objects (a
Variant object whose field is itself a Variant — which is always, since every
Variant field value IS a Variant).

The `RowReader` / `PqStruct` `getVariant` implementation reads the two child
binary columns (`metadata`, `value`) and constructs a `PqVariantImpl` pointing
at the row's current slices. The `PqVariantObject` `getVariant` implementation
returns a view into the object's field table. Phase 1 does not consult
`typed_value`; if the schema includes it, the columns are still read but only
`metadata` + `value` feed the accessor.

### 1.6 Documentation

New section in `docs/content/usage.md` titled "Variant columns", covering the
accessor methods, `VariantType` enum, and a short example. One-line entry added to
the logical-types table. `docs/content/compat.md` is left untouched for Phase 1 —
the compat shim does not expose Variant directly.

### 1.7 Tests (Phase 1)

- `VariantMetadataTest` — parse every metadata fixture under
  `parquet-testing/variant/*.metadata`, assert dictionary contents against
  `data_dictionary.json`.
- `VariantValueDecoderTest` — parse every `parquet-testing/variant/*.value` fixture,
  assert decoded type and value against the filename convention (e.g.
  `primitive_int8.value` decodes as `INT8`) and against `data_dictionary.json` where
  applicable.
- `VariantLogicalTypeTest` — mirrors `JsonLogicalTypeTest`: read a generated
  Parquet file containing a Variant column, assert the column schema reports
  `LogicalType.VariantType` and that `getVariant()` returns a `PqVariant` matching
  the expected shape.
- `VariantRowReaderTest` — generate (via `simple-datagen.py`) a file with flat and
  nested Variant columns and exercise `PqVariantObject` / `PqVariantArray`
  navigation, including primitive accessors inherited from `FieldAccessor` and
  the Variant-specific `getObject` / `getArray`.
- `FieldAccessorSplitTest` — assert `PqStruct` and `RowReader` still implement
  `StructAccessor`, and that `PqVariantObject` implements `FieldAccessor` but
  not `StructAccessor` (compile-time + reflective check).
- `SchemaNodeTest` additions — assert `GroupNode.logicalType` round-trips through
  schema construction, and that malformed Variant groups (missing `metadata` or
  `value`) throw at schema build time.

Generation: `simple-datagen.py` gains a `--variant` mode. PyArrow 21+ supports
writing Variant via `pa.variant()` with no post-processing required.

## Phase 2 — Shredded Variant reassembly (#286)

### 2.1 What changes at read time

When `GroupNode.isVariant()` and a `typed_value` sibling is present in the Variant
group (or anywhere inside the typed_value subtree), the row reader reassembles the
logical Variant value before handing it to `PqVariant`.

The canonical output is a `(metadata, value)` pair where `value` is a freshly encoded
Variant binary representing the merged payload. `metadata` is passed through from the
stored column unchanged; a shredded file's metadata already contains every field name
that the reassembled value will reference.

### 2.2 The reassembly algorithm

Implemented as `VariantShredReassembler` in the internal variant package. For a
Variant group at schema position P with stored columns `metadata`, `value`,
`typed_value`:

1. **Shredded primitive.** `typed_value` is a primitive column.
   - If `typed_value` is non-null: encode the primitive using the Variant binary
     encoding for its type (e.g. `INT32` → `0x07` + little-endian 4 bytes).
   - Else if `value` is non-null: pass through.
   - Else: emit `NULL` (0x00 header) — the row's variant is SQL NULL.

2. **Shredded array.** `typed_value` is a `LIST` group whose element is itself a
   shredded Variant (i.e. has its own `{value, typed_value}` substructure).
   - If `typed_value` is non-null: recursively reassemble each element, then emit an
     ARRAY header followed by the element offset table and concatenated element
     bytes.
   - Else if `value` is non-null: pass through.
   - Else: `NULL`.

3. **Shredded object.** `typed_value` is a group whose children are themselves
   shredded Variants keyed by field name.
   - If `typed_value` is non-null: recursively reassemble each field, emit an OBJECT
     header + key id table + value offset table + concatenated bytes. Missing child
     fields (all child def levels < def level of `typed_value`) are merged with any
     fields present in the partial `value` column per the spec's merge rules.
   - Else if `value` is non-null: pass through.
   - Else: `NULL`.

4. **Nested shredding.** Recursion terminates at non-shredded leaves (`typed_value`
   absent), falling back to rule (1)-(3)'s "pass through `value`" branch.

Encoding helpers live alongside the decoder:

```
core/src/main/java/dev/hardwood/internal/variant/
    VariantValueEncoder.java   // emits the binary value from primitive inputs
    VariantShredReassembler.java
```

`VariantValueEncoder` is the minimum encoder needed by reassembly — it does **not**
turn Hardwood into a Variant writer in the general sense; it only composes bytes for
types produced by shredding.

### 2.3 Where reassembly is invoked

Reassembly happens inside the Variant accessor path, not inside the low-level
`ColumnReader`. Concretely:

- `RowReader.getVariant(name)` (and `PqStruct.getVariant(name)`) detect a shredded
  Variant group (`GroupNode.isVariant()` + `typed_value` child present), invoke
  `VariantShredReassembler.reassemble(...)` over the current row's slices, and wrap
  the result in `PqVariantImpl`.
- The reassembler writes into a per-row scratch `byte[]` held on the row reader,
  reused across rows to stay on the zero-allocation steady-state path. `PqVariant`
  copies out of scratch only when the caller invokes `value()` / `metadata()`.

Non-shredded Variant files take the identical code path as in Phase 1 — the
reassembler is a no-op when `typed_value` is absent.

### 2.4 Schema validation additions

`FileSchema` validates the Variant group shape more strictly in Phase 2:

- `metadata` must be `required binary` (as Phase 1 required).
- `value` may be `optional binary` when `typed_value` is present (Phase 1 requires
  it to be present; here it becomes optional iff `typed_value` is present to carry
  the payload).
- `typed_value`, when present, must be the third child in the group. Its type may be
  any of: primitive, `LIST`-annotated group whose element is a shredded Variant, or
  a plain group whose children are themselves shredded Variant structs.

Violations throw at schema build time with a message naming the offending field path.

### 2.5 Avro and compat implications

- **`hardwood-avro`** — `AvroSchemaConverter` gains a branch for
  `LogicalType.VariantType`: emit a two-field Avro RECORD `{metadata: bytes, value:
  bytes}`. `AvroRowReader` materializes a Variant column via `getVariant()` and
  places the canonical `metadata` / `value` bytes into the record. This matches
  parquet-java's `AvroParquetReader` behavior, so the `shredded_variant/` fixtures
  round-trip through the comparison test identically.
- **`hardwood-parquet-java-compat`** — Group type shims are extended to carry a
  `LogicalTypeAnnotation.Variant` stub, and `Group.getBinary("metadata")` /
  `getBinary("value")` return the reassembled bytes. Behavior matches parquet-java's
  group reader over shredded files.

### 2.6 Re-enabling the comparison suite

The `shredded_variant/` comparison skip is already narrowed in Phase 1:
`isShreddedVariantFile` now inspects the Parquet schema via Hardwood and marks
a file shredded only when the `var` group carries a `typed_value` sibling.
Unshredded files (`{metadata, value}` only) are compared today via
`Utils.compareField`'s Variant dispatch, which byte-compares `PqVariant#metadata`
and `PqVariant#value` against the parquet-java reference record's binaries.

Phase 2 removes the remaining shredded-only skip entirely: once reassembly is
wired through `getVariant`, Hardwood's `PqVariant.value()` returns canonical
bytes matching parquet-java's reassembled output, so the same
`compareVariant` helper handles shredded files without changes.

### 2.7 Tests (Phase 2)

- `VariantShredReassemblerTest` — for each `shredded_variant/case-NNN.parquet` with
  a matching `case-NNN_row-M.variant.bin`: read the Parquet file via Hardwood,
  assert the reassembled `(metadata + value)` bytes match the expected `.variant.bin`
  byte-for-byte. This is the authoritative cross-impl oracle.
- `VariantValueEncoderTest` — round-trip: decode every `variant/*.value` fixture,
  re-encode via `VariantValueEncoder`, assert the re-encoded bytes equal the input.
- `ParquetComparisonTest` — delta-verify: the suite now runs row-level comparison
  over the `shredded_variant/` files against parquet-java's `AvroParquetReader`.
- `SchemaNodeTest` extensions — reject malformed shredded groups (wrong child
  count, wrong child order, `typed_value` of an unsupported type).

## Public API impact

New user-visible symbols:

| Symbol | Module | Phase |
|---|---|---|
| `LogicalType.VariantType` | `hardwood-core` | 1 |
| `SchemaNode.GroupNode#logicalType()` + `isVariant()` | `hardwood-core` | 1 |
| `PqVariant` (interface) | `hardwood-core` | 1 |
| `PqVariantObject` (interface) | `hardwood-core` | 1 |
| `PqVariantArray` (interface) | `hardwood-core` | 1 |
| `VariantType` (enum) | `hardwood-core` | 1 |
| `VariantTypeException` | `hardwood-core` | 1 |
| `FieldAccessor` (interface, extracted from `StructAccessor`) | `hardwood-core` | 1 |
| `FieldAccessor#getVariant(String)` + `isVariant(String)` | `hardwood-core` | 1 |

Internal-only (no public API):

| Symbol | Module | Phase |
|---|---|---|
| `internal.variant.VariantBinary` | `hardwood-core` | 1 |
| `internal.variant.VariantMetadata` | `hardwood-core` | 1 |
| `internal.variant.VariantValueDecoder` | `hardwood-core` | 1 |
| `internal.variant.PqVariantImpl` | `hardwood-core` | 1 |
| `internal.variant.PqVariantObjectImpl` | `hardwood-core` | 1 |
| `internal.variant.PqVariantArrayImpl` | `hardwood-core` | 1 |
| `internal.variant.VariantValueEncoder` | `hardwood-core` | 2 |
| `internal.variant.VariantShredReassembler` | `hardwood-core` | 2 |

## File summary

### Phase 1

| File | Action |
|---|---|
| `core/.../metadata/LogicalType.java` | Add `VariantType` to `permits` + new record |
| `core/.../internal/thrift/LogicalTypeReader.java` | Add case 16, `readVariantType()` |
| `core/.../schema/SchemaNode.java` | Add `logicalType` to `GroupNode`, add `isVariant()` |
| `core/.../schema/FileSchema.java` | Pass `element.logicalType()` into `GroupNode`, validate Variant group shape |
| `core/.../row/PqVariant.java` | New |
| `core/.../row/PqVariantObject.java` | New |
| `core/.../row/PqVariantArray.java` | New |
| `core/.../row/VariantType.java` | New enum |
| `core/.../row/VariantTypeException.java` | New |
| `core/.../row/FieldAccessor.java` | New — extracted from `StructAccessor` |
| `core/.../row/StructAccessor.java` | Reduced to complex Parquet getters; `extends FieldAccessor` |
| `core/.../row/PqStruct.java` | Unchanged (still implements `StructAccessor`); inherits `getVariant` |
| `core/.../reader/RowReader.java` | Implement `getVariant` / `isVariant` (inherited from `FieldAccessor`) |
| `core/.../internal/variant/VariantBinary.java` | New |
| `core/.../internal/variant/VariantMetadata.java` | New |
| `core/.../internal/variant/VariantValueDecoder.java` | New |
| `core/.../internal/variant/PqVariantImpl.java` | New |
| `core/.../internal/variant/PqVariantObjectImpl.java` | New |
| `core/.../internal/variant/PqVariantArrayImpl.java` | New |
| `core/src/test/.../VariantMetadataTest.java` | New |
| `core/src/test/.../VariantValueDecoderTest.java` | New |
| `core/src/test/.../VariantLogicalTypeTest.java` | New |
| `core/src/test/.../VariantRowReaderTest.java` | New |
| `core/src/test/.../FieldAccessorSplitTest.java` | New |
| `simple-datagen.py` | Add `--variant` mode |
| `docs/content/usage.md` | Add Variant section + logical-type table row |

### Phase 2

| File | Action |
|---|---|
| `core/.../schema/FileSchema.java` | Extend Variant group validation to allow `typed_value` |
| `core/.../row/PqStruct.java` | Wire reassembler into `getVariant` |
| `core/.../reader/RowReader.java` | Wire reassembler into `getVariant`, allocate scratch buffer per reader |
| `core/.../internal/variant/VariantValueEncoder.java` | New |
| `core/.../internal/variant/VariantShredReassembler.java` | New |
| `avro/.../AvroSchemaConverter.java` | Variant → RECORD `{metadata, value}` branch |
| `avro/.../AvroRowReader.java` | Materialize Variant records from `PqVariant` |
| `parquet-java-compat/.../schema/…` | Expose `LogicalTypeAnnotation.Variant` shim + `Group` binary accessors over reassembled bytes |
| `parquet-testing-runner/.../Utils.java` | Drop blanket `shredded_variant` skip |
| `core/src/test/.../VariantShredReassemblerTest.java` | New |
| `core/src/test/.../VariantValueEncoderTest.java` | New |
| `parquet-testing-runner/src/test/.../ParquetComparisonTest.java` | Assert `shredded_variant/` cases now pass row-level comparison |
| `docs/content/usage.md` | Note reassembly transparency in Variant section |
| `ROADMAP.md` | Mark Variant entries complete under Phase 7.4 |
