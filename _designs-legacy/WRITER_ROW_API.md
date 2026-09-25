# Row-oriented write API (#9, stage 16)

**Status: Complete.** Tracking issue: #9. Delivery stage 16 (Layer) of
[WRITER_SUPPORT.md](../_plans/WRITER_SUPPORT.md).

## Context

Stages 1–15 produce a complete columnar write path: `ColumnWriter.writeBatch` takes an
aligned slice of typed arrays, shreds it, pages it, encodes it, and cuts row groups on a size
target. That API is the right one for a caller that already holds columns — a query engine, an
Arrow buffer, a bulk converter. It is the wrong one for a caller that holds records.

Producing a file of ten flat columns from a stream of objects currently means allocating ten
arrays, tracking a fill cursor, maintaining a parallel null mask per optional column, and
knowing that a list's elements live at `phones.list.element` and a map's entries at
`props.key_value.key`. The synthetic `list`/`element` and `key_value` path segments exist in the
Parquet schema and the columnar API exposes them, because the columnar API addresses leaf
columns. A record-shaped caller thinks in fields.

This layer closes that gap. It mirrors the read side, where `ParquetFileReader` offers both
`columnReader()` (performance-first) and `rowReader()` (ergonomics-first) over the same file.

## Shape

`ParquetFileWriter` gains `rowWriter()`, returning a `RowWriter` view over the same file, beside
the `columnWriter()` view returning the `ColumnWriter` that carries `writeBatch`. The
file writer keeps ownership of the lifecycle: `close()` writes the footer, and the row writer is
not itself closeable.

```java
try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
    RowWriter rows = writer.rowWriter();
    for (Person person : people) {
        rows.writeRow(row -> row
                .setLong("id", person.id())
                .setString("name", person.name())
                .setDate("hired", person.hired())
                .setStruct("address", address -> address
                        .setString("city", person.city())
                        .setString("zip", person.zip()))
                .setList("phones", phones -> {
                    for (String phone : person.phones()) {
                        phones.addString(phone);
                    }
                }));
    }
}
```

`writeRow(Consumer<StructBuilder>)` is the idiom `writeBatch(Consumer<ColumnBatch>)` already
established: the layer creates the builder bound to the schema, hands it to the caller to
populate, then submits it. There is no separate build or submit step to forget, and no builder
instance that can outlive the row it describes.

A row is a struct, so the top-level builder is a `StructBuilder` — the same type used for a
nested struct. This mirrors the read side, where `RowReader extends StructAccessor` and a nested
`PqStruct` offers the same accessors as the row itself. No distinct `RowBuilder` type is
introduced; it would carry no members of its own.

### Types

Five public types in `dev.hardwood.writer`, all `@Experimental` while the writer is:

| Type | Role |
| --- | --- |
| `RowWriter` | `void writeRow(Consumer<StructBuilder>)`. A final class, obtained from `ParquetFileWriter.rowWriter()`. |
| `PrecisionLossPolicy` | `REJECT` / `TRUNCATE`, selected through `WriterConfig`. See [Logical-type value conversion](#logical-type-value-conversion). |
| `StructBuilder` | Sets the fields of one struct instance — the row, or a nested struct. |
| `ListBuilder` | Appends the entries of one `LIST` instance. |
| `MapBuilder` | Appends the entries of one `MAP` instance. |

The three builders are interfaces, implemented by the plan nodes in
`dev.hardwood.internal.writer`, so the public surface stays a contract rather than a class with
visible construction. Builder instances are valid only
for the duration of the lambda they are passed to; retaining one and using it later throws
`IllegalStateException` rather than corrupting a later row.

### Field addressing

Fields are addressed by their user-visible name, never by a leaf path. `setList("phones", …)`
and, inside it, `addString(…)` together produce the values of the `phones.list.element` column;
the caller never spells the synthetic segments. Nested structs are entered through
`setStruct(name, …)` rather than a dotted path, so a name is always resolved against exactly one
group.

Every setter also has an index-taking mirror — `setLong(int fieldIndex, long)` alongside
`setLong(String, long)` — addressing a field by its position within the struct that declares it.
The position is the field's place in schema declaration order, scoped to one group: a nested
struct's indices are its own, and a `MAP` entry declares `key` at `0` and `value` at `1`.
`getFieldCount()` and `getFieldName(int)` report those positions, the same pair the reader's
`FieldAccessor` exposes.

The two forms address the same field and are interchangeable within one record; only the way a
field is named differs, so an out-of-range index takes the place of an unknown name and every
other rule — type check, range check, already-set, scope lifetime — holds unchanged. The scope
rule covers `getFieldCount` and `getFieldName` too, so a retained builder is not a usable view
of the schema either.

Two things make the index form worth its surface. A hot write loop over a fixed schema pays no
name lookup per field per row. More importantly, the index is what makes the write side
symmetric with the read side: `StructAccessor` addresses a row's fields by position, so code
that walks a row generically — a copy, a projection, a format bridge — can read at position `i`
and write at position `i` without a name in the loop. A write layer that only understood names
would force such code to route every field through a string it does not otherwise need.

The two positions are the same one only where the write schema mirrors what was read. The
reader's index is the position in *projected* schema order — `FlatRowReader.getFieldName` maps
it through `ProjectedSchema.toOriginalIndex` — while the writer's is declaration order in the
schema being written. Reading a whole file into its own schema makes them identical, which is
the copy case; a projection does not, and reading three of ten columns while writing the
ten-column schema shifts every position. Where the columns that then line up share a physical
type, the values land in the wrong fields instead of being rejected, because an index carries
no evidence of what it was supposed to name. `getFieldName` on both sides is the cheap check
that turns it into a failure, which is why the writer exposes it rather than leaving the
caller to consult the schema.

### Maps

A `MAP`'s entries are a repeated struct of `key` and `value`, so the map builder reuses
`StructBuilder` rather than inventing a key/value vocabulary:

```java
.setMap("props", props -> props
        .addEntry(entry -> entry
                .setString("key", "region")
                .setString("value", "eu-central-1")))
```

This composes with every key and value type, including struct, list and map values, at the cost
of naming the two fields as strings. The alternative — typed `put` methods — is combinatorial in
key type × value type. String-keyed sugar (`putString(String, String)`, `putLong(String, long)`,
…) is linear rather than combinatorial and remains available as an additive family if the
verbosity proves to bite; see [Reserved surface](#reserved-surface).

## Null and completeness rules

Within a struct scope, a field that is never set is written as null if it is `OPTIONAL`, and
fails the row if it is `REQUIRED`. `setNull(name)` states the same thing explicitly for a reader
of the calling code, and a `null` value handed to any object-typed setter does too — a caller
writing `setString("name", person.name())` over a nullable field should not have to branch, and
the reader returns `null` for exactly the fields written this way.

This is checked when the scope's lambda returns, not at file close: an unset `REQUIRED` field
throws from inside `writeRow`, naming the field. Nothing is silently defaulted. A typo cannot
silently write nulls, because a name that is not in the schema throws from the setter itself.

Only present subtrees are checked. A `REQUIRED` field inside an `OPTIONAL` struct that the row
left null is not required of that row.

Setting the same field twice within one scope throws, mirroring the columnar API's
already-set rule.

A record is staged in full or not at all. Every node's staging is checkpointed before the filler
runs and rolled back if it throws — whether the writer rejected a value or the caller's own code
failed — so a rejected record leaves the batch exactly as it was. Without this a validation
error would leave a half-populated batch whose columns no longer agree on their record count.
The exception still fails the writer, as every exception out of a write call does (see
`WRITER_SUPPORT.md`); the rollback keeps the staged batch consistent for skipping a rejected
record explicitly (#1253).

A list or map entry is nullable only where the schema says so: `ListBuilder.addNull()` requires
an `OPTIONAL` element, and a map's `key` is always `REQUIRED` by the Parquet `MAP` contract, so
a null key throws. An absent list (`setNull("phones")`) and an empty list (`setList("phones",
phones -> {})`) are distinct, and both are expressible.

## Logical-type value conversion

The setters mirror the reader's accessors one for one, so a value read back through
`FieldAccessor` is written by the setter of the same name. Each row below also stands for the
index-taking mirror described under [Field addressing](#field-addressing) — `setInt(int, int)`
against `getInt(int)`, and so on for every entry:

| Setter | Getter | Accepted physical types |
| --- | --- | --- |
| `setInt(String, int)` | `getInt` | `INT32` |
| `setLong(String, long)` | `getLong` | `INT64` |
| `setFloat(String, float)` | `getFloat` | `FLOAT` |
| `setDouble(String, double)` | `getDouble` | `DOUBLE` |
| `setBoolean(String, boolean)` | `getBoolean` | `BOOLEAN` |
| `setString(String, String)` | `getString` | `BYTE_ARRAY` with `STRING`/`ENUM`/`JSON` |
| `setBinary(String, byte[])` | `getBinary` | `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` |
| `setDate(String, LocalDate)` | `getDate` | `INT32` with `DATE` |
| `setTime(String, LocalTime)` | `getTime` | `INT32`/`INT64` with `TIME` |
| `setTimestamp(String, Instant)` | `getTimestamp` | `INT64` with `TIMESTAMP`, `isAdjustedToUTC = true` |
| `setLocalTimestamp(String, LocalDateTime)` | `getLocalTimestamp` | `INT64` with `TIMESTAMP`, `isAdjustedToUTC = false` |
| `setDecimal(String, BigDecimal)` | `getDecimal` | `INT32`/`INT64`/`BYTE_ARRAY`/`FIXED_LEN_BYTE_ARRAY` with `DECIMAL` |
| `setUuid(String, UUID)` | `getUuid` | `FIXED_LEN_BYTE_ARRAY(16)` with `UUID` |
| `setInterval(String, PqInterval)` | `getInterval` | `FIXED_LEN_BYTE_ARRAY(12)` with `INTERVAL` |

The conversion is performed by `dev.hardwood.internal.conversion.PhysicalValueConverter`, the
inverse of `LogicalTypeConverter` and its neighbour in the same package.

The rules below split in two, along a line worth stating explicitly.

**Magnitude is never negotiable.** A value the column cannot represent at all — a date beyond
the `INT32` day range, an instant outside the span its `TIMESTAMP` unit covers (a `NANOS` column
reaches only from about 1677 to 2262), an unscaled decimal wider than the declared precision, an
`INT(8)` out of range, a `FIXED_LEN_BYTE_ARRAY` of the wrong length, an `INTERVAL` component
outside the unsigned 32-bit range — throws `IllegalArgumentException` naming the field. No
configuration relaxes this, because no narrowing would preserve what the caller handed over.

Magnitude is therefore checked *before* precision, so a value that is both too large and too
precise fails the same way under either policy. A decimal whose precision exceeds what its
physical type can hold is caught even earlier, by `FileSchema.Builder`, which bounds a
`DECIMAL`'s precision by its physical width when the schema is declared.

**Precision is configurable**, through `WriterConfig.precisionLossPolicy(...)`:

- `REJECT` (the default) throws when a value carries digits the column's unit or scale cannot
  hold. A value that happens to be exact at that unit or scale is written normally.
- `TRUNCATE` drops those digits. `TIME` and `TIMESTAMP` drop the sub-unit fraction, which floors
  the value — the fraction is carried as a non-negative nanosecond-of-second, so this is the
  result `Instant.toEpochMilli()` produces and what every other Parquet writer does. `DECIMAL`
  rescales with `RoundingMode.DOWN`, dropping the digits beyond the declared scale rather than
  rounding to the nearest.

`REJECT` is the default for two reasons. It is the direction that keeps the option open:
relaxing a rejection later is a compatible change, while tightening one silently changes what
past files meant. And it matches the only comparable API — PyArrow's `write_table` raises
`Casting from timestamp[us] to timestamp[ms] would lose data` unless
`allow_truncated_timestamps=True`. The Java precedents that truncate silently (Avro's
conversions, Spark) sit above a Parquet API that takes a raw `long` and never faces the
question; the decision had already been made by the time it reached them.

The cost of the default is real and worth naming: `Instant.now()` carries microseconds on a
modern JDK, so `setTimestamp` on a `TIMESTAMP(MILLIS)` column rejects it. The rejection
message therefore names all three ways out — truncate at the call site with
`Instant.truncatedTo(ChronoUnit.MILLIS)`, declare the column at a finer unit, or select
`TRUNCATE` — and the physical setters (`setLong` with `instant.toEpochMilli()`) remain available
for a caller who wants the raw parquet-java contract. The policy is a property of the row layer:
`ColumnBatch` takes physical values and converts nothing, so nothing there can lose precision.

- **Annotation match.** A setter whose logical type does not match the column's annotation
  throws. `setTimestamp` requires `isAdjustedToUTC = true` and `setLocalTimestamp` requires
  `false`, mirroring the reader's split between `Instant` and `LocalDateTime`. Calling the
  wrong one is a bug about time-zone semantics and is reported as one.
- **Unit narrowing.** A `TIMESTAMP(MILLIS)` column rejects an `Instant` carrying sub-millisecond
  precision under `REJECT` and floors it under `TRUNCATE`; the same holds for `TIME` and for the
  `MICROS`/`NANOS` units. A value that happens to be exact at the column's unit is accepted
  either way.
- **Decimal scale.** A `BigDecimal` is rescaled to the column's declared scale when that is
  lossless; when it is not, `REJECT` throws and `TRUNCATE` drops the digits toward zero. Its
  unscaled value must fit the declared precision and the physical width under either policy.
- **Integer widths.** `setInt` range-checks the value against what the annotation can hold:
  `[-2^(n-1), 2^(n-1))` for a signed `INT(8)` / `INT(16)`, and `[0, 2^n)` for an unsigned
  `UINT_8` / `UINT_16`. All 256 valid `UINT_8` values fit inside that range, so the check costs
  no expressiveness and stops a file whose values fall outside the range its own annotation
  declares.
  `UINT_32` is the exception and keeps the raw two's-complement bits, which is what the reader
  returns for it: every bit pattern is a valid value of the column, and spelling one above
  `Integer.MAX_VALUE` as a negative `int` is the only way to reach it. The complete rule set,
  including the `DECIMAL` bounds the physical setters carry, is in
  [WRITER_ANNOTATION_RANGES.md](WRITER_ANNOTATION_RANGES.md).
- **Fixed widths.** `setBinary` on a `FIXED_LEN_BYTE_ARRAY` column requires exactly the declared
  length.

Setters that need no conversion — `setInt` on a bare `INT32`, `setBinary` on a bare
`BYTE_ARRAY` — go straight to the staging array with no per-value branch on the logical type;
the conversion strategy is resolved once per column at bind time, not per value.

Physical-type mismatch (`setLong` on an `INT32` column) throws from the setter, as it does in
the columnar API. A physical setter does, however, apply to an annotated column: `setInt` on a
`DATE` column writes the stored `int`, mirroring the reader's `getInt`, which returns it whatever
the annotation. That makes the physical setters the escape hatch alongside the logical ones, and
it is why the `INT(8)` / `INT(16)` range check lives on `setInt` rather than only on a logical
setter.

`ColumnBatch` keeps its "the caller hands over encoded physical arrays" contract, and gains no
logical setters: conversion is a property of the row layer. The range an annotation declares is
not — both APIs check the values they are handed against it.

## Transposition

The row writer is an adapter, not a second write path. It owns no shredding, paging, statistics
or dictionary logic: it accumulates rows into the column-shaped staging described below and
submits them through `writeBatch`. Every guarantee of stages 1–15 therefore holds unchanged for
files written through it, and the interop gate covers both APIs by construction.

Staging, all reused across batches:

- per leaf column, a growable typed array (`int[]`, `long[]`, …, `byte[][]`) and a null mask;
- per `OPTIONAL` struct group, a null mask indexed by group instance — which is the row index
  for a struct at the top level, and the enclosing repetition index for one nested under a list;
- per `LIST`/`MAP` group, an offsets `int[]` and a null mask.

The builders append in document order, which is exactly the order `RecordShredder` consumes, so
the transposition is bookkeeping rather than reordering.

A batch is submitted when either trigger fires:

- **1024 staged rows.** A fixed internal constant, not a `WriterConfig` knob: the batch is an
  arrival unit with no effect on the produced file, and the file's layout is already governed by
  the page and row-group targets.
- **staged variable-width payload ≥ `WriterConfig.rowGroupBufferTargetBytes()`.** A row of large
  `BYTE_ARRAY` values would otherwise let 1024 rows hold arbitrarily much; this bounds staging
  at one row group's worth, which is the memory bound the columnar path already promises.

The pending partial batch is submitted from `ParquetFileWriter.close()`, before the final row
group is flushed and the footer written.

Because `writeBatch` consumes its sources synchronously — `RecordShredder.bind` and
`appendRecords` both complete within the call — the staging arrays can be refilled as soon as it
returns. Stage 23 (parallel encode) must preserve that, or give the row writer a second staging
generation to write into; it is noted here so the coupling is not rediscovered.

A staged array is passed to `ColumnBatch` directly when its fill count equals its capacity, and
copied with `Arrays.copyOf` otherwise, because `ColumnBatch` derives the batch row count from
the array length. Nested leaves, whose element count is not the row count, therefore copy on
every flush. That is a known cost of the ergonomic layer and is not optimized here; the columnar
API remains the zero-copy path.

## Interleaving and lifecycle

A file writer serves one API or the other, never both: rows and batches would otherwise
interleave two independent staging states into one row group, with a submission order that
depends on when the row writer happens to flush.

`ParquetFileWriter` carries a mode latch, initially unset. `rowWriter()` latches row mode and
`columnWriter()` latches batch mode; either method throws `IllegalStateException` if the other
mode is already latched, naming both APIs in the message. Each latches on the call rather than
on the first `writeRow` or `writeBatch`, so obtaining the view is the declaration of intent, and
each returns the same instance on every call so two views cannot stage against one file.

`RowWriter` is not `Closeable`. The file writer is the resource; a second closeable over the
same file invites a double-close that either discards a valid file or writes a second footer.
`writeRow` after the file writer is closed throws, through the same `ensureOpen` check the
columnar path uses.

Whether a schema can be produced at all is settled by `ParquetFileWriter.create` before either
view exists, so a repeated field outside a `LIST` or `MAP` group never reaches this layer.

The plan is built when `rowWriter()` is called, and rejects there — naming the offending path —
the shapes this layer alone cannot *address*: two sibling fields sharing a name, which leaves
the by-name setters ambiguous, and the legacy two-level lists, whose entry is the element itself
where the builders reach a list's values through an element node below the entry. The columnar
API addresses by index and dotted path and writes those shapes.

## Reserved surface

The layer is deliberately narrow, but two extensions are foreseen and the surface is shaped so
they can be added without a breaking change or a boxing penalty:

- **Bulk primitive entries.** `setList("scores", int[])`, `long[]`, `double[]` and the rest,
  for a caller that already holds a primitive array. These are distinct erasures and compose
  with the lambda form.
- **String-keyed map sugar.** `MapBuilder.putString(String, String)`, `putLong(String, long)`
  and the rest.

One rule keeps both open: **no *unqualified* `Object`-typed setter anywhere in this API.** There
is no `set(String, Object)`, no `ListBuilder.add(Object)`, and no `Collection<?>` overload. Any
of them would make the boxing path the one that binds by default — `addInt(1)` and `add(1)` are
not the same program — and would create overload ambiguity against the primitive forms added
later.

The rule bites on the *name*, not on the parameter type: it is an unqualified `set` or `add`
competing with typed siblings that a call can silently resolve to the wrong way. A distinctly
named `setValue(String | int, Object)` mirroring the reader's `getValue` is outside it, because
no call to `setInt` can resolve to `setValue` by accident, and it is planned as stage 34 of
`_plans/WRITER_SUPPORT.md`. Its justification is narrower than `getValue`'s: a reader must
always be able to handle a column whose type it learns at runtime, whereas a writer usually
knows the schema it declared — except in the generic case, a copier or a format bridge, which
knows it only at runtime and otherwise has to switch on `PhysicalType` to pick a setter.

## Out of scope

- **Collection-shaped setters** taking a `List<String>` or a `Map<String, Long>`. Deferred until
  asked for, per the reserved-surface rule above.
- **Typed binding** — writing a POJO or record directly. That is the write side of the typed
  view epic (#940) and needs its schema reconciliation, not a second ad-hoc mapper here.
- **`VARIANT` values.** The writer has no Variant encoder; a shredded Variant group can still be
  written field by field through the binary setters. `setVariant` therefore waits on a Variant
  writer rather than on this layer, planned as stage 35 of `_plans/WRITER_SUPPORT.md`.
- **`INT96`.** Not writable at all.
- **Shapes the columnar path rejects.** The layer produces `ColumnBatch` inputs, so it inherits
  every limitation of the core: repetition no `LIST` or `MAP` annotation accounts for, which the
  shredder cannot level.

## Validation

- Round-trip tests per physical type, flat `REQUIRED`/`OPTIONAL`, mirroring the existing
  `Writer*RoundTripTest` structure but driven through `RowWriter`.
- Round-trip tests for every nested shape the columnar API supports: struct, list, map, list of
  list, list of struct, map of struct — read back through `RowReader` and asserted field by
  field, so the row API is validated against the row reader it mirrors.
- **Equivalence tests**: the same logical data written through `writeBatch` and through
  `writeRow` must produce byte-identical files. This is the strongest available assertion that
  the layer is an adapter and not a second write path, and it fails loudly if the row layer ever
  starts making its own paging or dictionary decisions.
- Conversion tests per logical type, both the accepted values and every rejection listed above —
  sub-millisecond `Instant` into `TIMESTAMP(MILLIS)`, `BigDecimal` needing a rounding rescale,
  out-of-range `INT(8)`, wrong-length `FIXED_LEN_BYTE_ARRAY`, `setTimestamp` on a local column.
- Rule tests: unset `REQUIRED` field, double set, retained builder used after its lambda
  returned, interleaved `writeBatch` and `rowWriter()` in both orders, `writeRow` after close.
- **By-index equivalence tests**: the same records written by name and by index must produce
  byte-identical files — every typed setter, `setNull`, and the struct/list/map verbs including
  a `MAP` entry's `key`/`value` — plus a record addressed both ways at once, since the two forms
  are interchangeable. Backed by a copy loop that reads a file's fields by index through
  `StructAccessor` and writes them forward through the same positions, asserted to reproduce the
  file it read: the shape by-index addressing exists to serve.
- By-index rule tests: an index outside the struct's fields (in the record and in a nested
  struct, whose indices are its own), a field set by name and then by index, a verb that does not
  fit the field at that index, and a retained builder used through the indexed surface —
  `getFieldCount` and `getFieldName` included.
- Batch-boundary tests: a row count that straddles the 1024-row trigger, and large `BYTE_ARRAY`
  values that fire the payload trigger, both asserted to read back intact.
- The stage 14 interop gate is extended with a row-written file, so parquet-java and PyArrow see
  this layer's output as well.
