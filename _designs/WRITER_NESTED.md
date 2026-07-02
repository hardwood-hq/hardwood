# Nested write support (#9, stage 5)

**Status: Settled (stage 5 complete); implemented in stages 6–8.** Tracking issue: #9. Delivery stage 5 (Spike) of
[WRITER_SUPPORT.md](WRITER_SUPPORT.md). This document is the reference the
shredding increments (stages 6–8: structs, lists, maps) implement against.

## Context

Through stage 4 the writer covers flat columns — `REQUIRED` / `OPTIONAL`, no
repetition — fed one page-aligned `int[]` per column through `ColumnBatch`. A flat
`OPTIONAL` column lowers a per-row null mask to a single definition-level stream at
page seal; there are no repetition levels and every column in a batch has the same
length.

Nested schemas break both of those simplifications. A struct adds definition-level
depth; a list or map adds repetition, so one top-level record can contribute zero,
one, or many leaf values, and sibling leaf columns no longer share a length. The
write path must accept per-record nesting structure from the caller and shred it into
the `[rep levels][def levels][values]` page body the reader already parses.

This is the exact inverse of the reader's Dremel assembly. The reader takes level
streams off disk and reconstructs a per-layer view — `getLayerValidity`,
`getLayerOffsets`, `getLeafValidity`, real-items-only value arrays. The writer takes
that same per-layer view from the caller and computes the level streams. The two
contracts are mirror images, and this document defines the write half so a column
written at a given path reads back at the same path with the same values and nulls.

## Scope

Stage 5 settles three things:

1. **The nested `ColumnBatch` input contract** — how a caller supplies per-layer
   validity, per-repeated-layer offsets, and real-items-only leaf values, addressed
   by physical path.
2. **The shredding algorithm** — how `RecordShredder` computes repetition and
   definition levels from that input, the write-side inverse of the reader's
   `NestedLevelComputer`.
3. **`FileSchema.Builder` nested support** — constructing struct, list, and map
   schemas that emit the canonical Parquet physical layout and compute max
   definition / repetition levels.

Leaf physical type stays `INT32` for the shredding increments; type breadth is stage
12 and rides unchanged on top of the level machinery. Dictionary encoding (stage 9),
compression (stage 10), and statistics (stage 11) layer on afterwards. The writer
emits the canonical 3-level `LIST` and 2-level `MAP` layouts only; the legacy
backward-compatibility list shapes the reader tolerates on read are not produced.

## Path addressing model

The reader addresses a nested leaf by its **full physical path**, scaffolding
included — a `LIST`-annotated `int_list` column is read at `int_list.list.element`,
and `getColumn` / `ColumnProjection` key on that dotted string. The flat writer
already resolves every column through the same `FileSchema#getColumn(String)`. The
nested write contract therefore uses **the same physical paths**: no separate logical
naming, no read/write asymmetry, and a column written at `people.list.element.name`
reads back at `people.list.element.name`.

A path segment is a schema field name. Descending into a struct field uses the field
name; the canonical `LIST` and `MAP` scaffolding contributes its literal segments
(`list`, `element`, `key_value`, `key`, `value`). Every leaf and every addressable
layer has a unique path at any nesting depth, and a layer's path is always a prefix
of the leaf paths beneath it.

### What is addressable

The reader collapses the schema chain into **layers** — one per user-authored
`OPTIONAL` group (`STRUCT`) and one per `LIST`/`MAP`-annotated group (`REPEATED`);
`REQUIRED` groups and the synthetic repeated scaffolding inside a `LIST`/`MAP`
contribute no layer. The write contract addresses exactly that set, plus the leaves:

| Addressable thing | Physical node it is keyed on | Reader counterpart |
|---|---|---|
| Leaf values | the primitive leaf's full path | typed value arrays + `getLeafValidity` |
| `STRUCT` layer | the `OPTIONAL` group's path | `getLayerValidity(k)`, kind `STRUCT` |
| `REPEATED` layer | the outer `LIST`/`MAP` group's path | `getLayerValidity(k)` + `getLayerOffsets(k)`, kind `REPEATED` |

A `LIST` is physically two nodes — an outer nullable group and an inner `repeated`
group — that the reader presents as one `REPEATED` layer carrying both the
list-itself-null validity and the entry offsets. The write contract mirrors that: one
`.list(...)` call keyed on the outer group's path supplies both facets. The inner
`repeated` node is never addressed directly. `REQUIRED` groups are invisible to the
caller exactly as they are to the reader — they only raise the leaves' max definition
level.

## Nested `ColumnBatch` input contract

`ColumnBatch` gains three path-addressed facet setters. They compose with the
existing flat `ints(...)` setters unchanged; a flat schema uses only the leaf setters
and never reaches for a layer verb.

| Facet | Setter | Mandatory | Input |
|---|---|---|---|
| Leaf values | `ints(path, values)` / `ints(path, values, nulls)` | values yes | real-items-only `int[]`; optional leaf `Validity` |
| `STRUCT` layer | `struct(path, validity)` | no — omit ⇒ all present | `Validity` over the group's instances |
| `REPEATED` layer | `list(path, offsets)` / `list(path, offsets, validity)` | offsets yes | `int[]` entry offsets; optional list-null `Validity` |
| `MAP` layer | `map(path, offsets)` / `map(path, offsets, validity)` | offsets yes | as `list`; addresses the `MAP` group |

`map` is a thin alias over `list` — identical offsets-plus-validity storage — that
validates the addressed node is `MAP`-annotated; its `key` and `value` leaves share
the one offsets array. Calling `list` on a `STRUCT` node, `struct` on a repeated
node, or `ints` on a group fails eagerly, the same posture as the flat setters'
`INT32`/repetition checks. Every path is validated against the schema as it is added,
so an unknown path, a wrong-kind verb, or a facet set twice fails at fill time rather
than at write time.

Every validity facet — leaf, `STRUCT`, and `REPEATED` — is supplied as a `Validity`,
with the one polarity that type carries: `isNull(i)` marks the absent item (a null
leaf, a null struct instance, a null list/map). The layer setters take **`Validity`
only**; they deliberately do not offer the `boolean[]` null-mask overload the flat
`ints` setter provides as convenience, keeping the nested surface minimal. The
`present=[…]` and `…Present` / `…Nulls` names in the traces and examples below are
illustrative of one `Validity`'s contents, not a second boolean-array form.

### Real-items-only values

Nested leaf arrays are **real-items-only**, matching the reader's real-view: the array
carries one slot per leaf position that actually exists, and null leaves occupy a slot
whose value is ignored (as in the flat contract, but at real-item rather than
per-record granularity). Phantom positions — leaf slots that would belong to a null or
empty parent — are not represented in the array; they are synthesized by the shredder
from the ancestor validity and offsets. This is the inverse of the reader excluding
phantom positions from `getValueCount()` and the typed arrays.

### Offsets and the null-vs-empty distinction

A `REPEATED` layer's offsets follow the reader's convention exactly: length
`count(layer) + 1`, with `offsets[i+1] - offsets[i]` the number of entries in the
`i`-th parent scope and `offsets[count] == count(innerLayer)`. A zero delta is an
**empty** list/map; the layer's `Validity` carries the **null** list/map bit. Both are
expressible independently, so empty and null never collide — the same split the reader
exposes.

### Record-count chaining replaces uniform length

A flat batch's row count is the single shared `int[]` length. A nested batch's row
count is the number of **top-level records**, and the leaf arrays legitimately differ
in length across columns. The batch is validated as a chain, threading a running
**derived count** `c` — the number of real (non-phantom) scopes entering the next
layer — down each column from its outermost layer to its leaf. `c` starts at the
record count `R`, and each layer both constrains its input against the current `c` and
produces the `c` the next-inner layer sees:

- The outermost layer of every top-level column agrees on the record count, so `c₀ ==
  R` there.
- **`STRUCT` layer**: `validity` length (when not the `NO_NULLS` sparse form) equals
  `c`. A null struct instance contributes only a phantom to everything beneath it, so
  its descendants shrink accordingly: `c ← c − validity.nullCount(c)` (unchanged for
  `NO_NULLS`).
- **`REPEATED` layer**: `offsets.length == c + 1` and offsets are non-decreasing; then
  `c ← offsets[c]` (the real entry count feeding the next layer). A **null** list/map —
  `validity.isNull(i)` — must have a **zero delta** at its slot (`offsets[i+1] ==
  offsets[i]`), the same shape an empty one has; a caller that marks a scope null yet
  supplies entries for it is contradictory input and is rejected. Null and empty both
  contribute no inner scopes, and validity is the only thing that separates them.
- **Leaf**: values length equals the final `c`; a leaf `Validity` (non-sparse) matches
  that length.

Because `c` is threaded rather than read per layer, `offsets[count(k)] == count(k+1)`
falls out automatically — the entry count a `REPEATED` layer produces *is* the `c` its
child is validated against. Any break is a ragged nested batch and is rejected eagerly
with the offending path, the same failure class as the flat ragged-batch check.

### Completeness and defaults

At submit, the batch is complete when **every leaf's values are set and every
`REPEATED` layer's offsets are set** — the nested analog of the flat
`completedSources()` check. Validity is always optional: an omitted `STRUCT`,
`REPEATED`, or leaf `Validity` means all-present at that scope, mirroring the flat
mask-less form. Offsets cannot be defaulted — list boundaries are not inferable — so
they are mandatory on every repeated layer.

### Array ownership

As in the flat contract, arrays are referenced, not copied, and must not be mutated
until the batch has been written. The batch is single-use; a filler that stashes a
reference and mutates it after `writeBatch` returns fails loudly via the existing
`consumed` guard.

## Shredding algorithm

`RecordShredder` turns the per-layer input into the `[rep levels][def levels][values]`
page body. It is the inverse of the reader's `NestedLevelComputer` and runs per leaf
column, walking that leaf's physical ancestor path.

### Levels from the schema

For a leaf, walk its physical path `n₁ … n_L` (root's child down to the leaf). Each
node contributes:

- `defInc(n) = 1` if `n` is `OPTIONAL` or `REPEATED`, else `0`.
- `repInc(n) = 1` if `n` is `REPEATED`, else `0`.

`maxDefinitionLevel = Σ defInc`, `maxRepetitionLevel = Σ repInc` — the same values
`FileSchema` already computes and stores on `ColumnSchema`. Each `REPEATED` node is
assigned a **repetition depth** equal to the running `Σ repInc` up to and including
it; that depth is the repetition level emitted when that node repeats.

### Emitting triples

The shredder produces one `(rep, def, value?)` triple per leaf slot, **including
phantom slots** for null and empty ancestors; `num_values` in the page header counts
all triples. It descends the record structure driven by the caller's validity and
offsets:

- **Definition level** is the number of `OPTIONAL`/`REPEATED` nodes on the path that
  are *defined* for this slot. Descent stops at the first node that is absent:
  - a `STRUCT` (optional group) marked null contributes its own `defInc` up to but not
    including itself, and emits a single phantom at that depth;
  - a `LIST`/`MAP` marked null (outer group absent) stops before the outer group's
    `defInc`;
  - a `LIST`/`MAP` present but **empty** counts the outer group as defined but the
    inner `repeated` node as not defined, emitting one phantom;
  - a null leaf counts every ancestor as defined but not the leaf, so `def =
    maxDefinitionLevel - 1`;
  - a fully present leaf emits `def = maxDefinitionLevel` with its value.
- **Repetition level** follows the standard recursive rule: the first entry of a
  list/map scope inherits the repetition level of the enclosing scope, and each
  subsequent entry carries that scope's own repetition depth. A value therefore repeats
  at the depth of the **deepest** (innermost) list it extends, and takes level `0` only
  when it opens a fresh chain of lists from the record root.

Empty and null parent scopes still emit exactly one phantom triple so the record
boundary survives on disk, which is what lets the reader distinguish an empty list, a
null list, and a present list of nulls on read-back.

### Worked trace — list of list of int

Schema `m: [[int?]]`, canonical layout, `maxDef = 5`, `maxRep = 2`:

```
optional group m (LIST)            defInc 1
  repeated group list              defInc 1, rep depth 1
    optional group element (LIST)  defInc 1
      repeated group list          defInc 1, rep depth 2
        optional int32 element     defInc 1
```

Five records: `[[1,2],[3]]`, `[]`, `null`, `[[]]`, `[null]`. The caller supplies:

- `.list("m", [0,2,2,2,3,4], present=[T,T,F,T,T])` — outer offsets over inner lists;
  record 2 is a null outer list.
- `.list("m.list.element", [0,2,3,3,3], present=[T,T,T,F])` — inner offsets over
  ints; the fourth inner list (record 4's) is null.
- `.ints("m.list.element.list.element", [1,2,3])` — three real leaves, all present.

Shredding emits:

| Record | Triples `(rep, def, value)` |
|---|---|
| `[[1,2],[3]]` | `(0,5,1) (2,5,2) (1,5,3)` |
| `[]` | `(0,1,·)` — outer list present but empty |
| `null` | `(0,0,·)` — outer list null |
| `[[]]` | `(0,3,·)` — inner list present but empty |
| `[null]` | `(0,2,·)` — inner list null |

Seven values on disk, three real. Reading this page back through the reader
reconstructs the five records exactly — the round-trip check the shredding increments
assert.

## `FileSchema.Builder` nested support

The builder constructs the immutable `FileSchema` / `SchemaNode` model the writer and
reader share, emitting the **canonical** physical layout (3-level `LIST`, 2-level
`MAP`) and computing max definition / repetition levels as it builds. A schema it
cannot yet produce — an unsupported shape or a leaf type outside the current increment
— is rejected at construction, never mid-write.

The builder verbs are `struct` / `list` / `map`, not a bare `group`. In the schema
model `group` is the abstract super-type — a `SchemaNode.GroupNode` carries an
annotation (`LIST` / `MAP` / none) and the reader resolves the concrete shape from it
at read time. Construction cannot leave that abstract: the concrete shape dictates the
physical bytes emitted — a struct is the bare group, a list expands to
`group(LIST){ repeated group { element } }`, a map to
`group(MAP){ repeated group key_value { key, value } }` — so there is no byte layout
for an unspecialized group to build. The builder therefore offers only the concrete
shapes, and they share the `struct` / `list` / `map` vocabulary with the batch setters.

The flat `addColumn(name, type, repetition)` stays. Nested shapes add:

- `struct(name, repetition, s -> …)` — a plain group; children added inside the lambda.
- `list(name, repetition, element -> …)` — a `LIST`; the element lambda declares the
  element via `primitive(type, repetition)`, `struct(repetition, …)`, `list(…)`, or
  `map(…)`, so lists of primitives, lists of structs, and lists of lists compose.
- `map(name, repetition, keyType, value -> …)` — a `MAP`; the key is a required
  primitive per the canonical layout, the value declared like a list element.

```java
FileSchema schema = FileSchema.builder("m")
        .addColumn("id", INT32, REQUIRED)
        .struct("address", OPTIONAL, s -> s
                .addColumn("street", INT32, REQUIRED)
                .addColumn("zip", INT32, OPTIONAL))
        .list("people", OPTIONAL, el -> el.struct(OPTIONAL, s -> s
                .addColumn("name", INT32, REQUIRED)
                .addColumn("age", INT32, OPTIONAL)))
        .list("m", OPTIONAL, el -> el.list(OPTIONAL, inner -> inner.primitive(INT32, OPTIONAL)))
        .build();
```

The builder names scaffolding canonically (`list` / `element`, `key_value` / `key` /
`value`), so the leaf and layer paths the write contract addresses are fully
determined by the schema shape. Stages 6–8 finalize the builder surface per shape
(struct, list, map) as each shredding increment lands.

Building a nested `FileSchema` is only half the canonical-layout work: the other half
is emitting it into the file's `SchemaElement` stream. Through stage 4
`FileSchema#toSchemaElements` rejects everything but flat schemas, so stage 6 also
lifts that restriction and extends the thrift schema serialization
(`SchemaElementWriter`) to write the group nodes with their `repetition`,
`num_children`, and `LIST`/`MAP` `converted_type` / `logicalType` annotations. The
canonical bytes come from the two halves together — the builder fixes the node tree,
the serializer writes it — so neither is complete without the other.

## Worked examples

**Struct with a nullable leaf** — `address: {street, zip?}`:

```java
writer.writeBatch(b -> b
        .ints("id", ids)
        .struct("address", addrPresent)              // omit ⇒ all structs present
        .ints("address.street", streets)
        .ints("address.zip", zips, zipNulls));
```

**List of nullable int** — `phones: [int?]`:

```java
writer.writeBatch(b -> b
        .list("phones", phoneOffsets, phonesPresent)     // list-null validity + offsets
        .ints("phones.list.element", phoneValues, elemNulls));
```

**Map of int to int** — `props: {int: int?}`:

```java
writer.writeBatch(b -> b
        .map("props", entryOffsets, propsPresent)
        .ints("props.key_value.key", keys)
        .ints("props.key_value.value", values, valueNulls));
```

**List of struct** — `people: [{name, age?}]`:

```java
writer.writeBatch(b -> b
        .list("people", offsets, peoplePresent)
        .struct("people.list.element", elemPresent)      // omit if the element is REQUIRED
        .ints("people.list.element.name", names)
        .ints("people.list.element.age", ages, ageNulls));
```

**List of list of int** — `m: [[int?]]`: see the shredding trace above.

## Component architecture

Nested support extends the writer packages already laid out in
[WRITER_SUPPORT.md](WRITER_SUPPORT.md); it introduces one new orchestration type and
extends two public ones.

| Layer | Package | Change |
|---|---|---|
| Public API | `dev.hardwood.writer` | `ColumnBatch` gains `struct` / `list` / `map` facet setters and path-addressed leaf setters |
| Public API | `dev.hardwood.schema` | `FileSchema.Builder` gains `struct` / `list` / `map` |
| Orchestration | `dev.hardwood.internal.writer` | `RecordShredder` — per-leaf rep/def-level computation from validity + offsets |
| Value encoding | `dev.hardwood.internal.encoding` | `LevelEncoder` (already present) is reused unchanged for the repetition-level stream |

`LevelEncoder` is a stateless utility — `encode(levels, offset, count, maxLevel)` —
already symmetric across level kinds, so the repetition stream needs no new encoder:
the page-body builder calls it a second time with the rep-level array and
`maxRepetitionLevel`. The resulting repetition-level stream sits ahead of the
definition-level stream in the DataPage V1 body — `[rep levels][def levels][values]` —
both RLE/bit-packed through `LevelEncoder` over the shared
`RleBitPackingHybridEncoder`. A `maxRepetitionLevel == 0` column (flat or struct-only)
writes no repetition stream, exactly the layout the reader expects and the shape
stages 1–4 already produce.

## Validation strategy

The nested increments run the same two-tier check the flat increments use, inverted
from [DIFFERENTIAL_TESTING.md](DIFFERENTIAL_TESTING.md):

1. **DuckDB differential (primary)** — hardwood writes a nested file; DuckDB reads it
   through `read_parquet` and the assembled structs / lists / maps are asserted to
   agree, ordered by a synthetic top-level index column. Boundary records — null
   list, empty list, present list of nulls, null struct, empty map — are written
   explicitly to stress the shredder, since these are exactly the phantom-slot cases.
2. **Round-trip** — write with hardwood, read back with the `ColumnReader` layer view,
   assert `getLayerValidity` / `getLayerOffsets` / `getLeafValidity` and the
   real-items-only value arrays equal what was submitted. Because the read and write
   contracts are mirror images, this pins the two halves against each other on the
   details DuckDB does not surface (exact level streams, empty-vs-null encoding).

Property/fuzz coverage — random in-scope nested schemas round-tripped through both
checks — arrives with the later shredding increments to surface repetition and
depth edge cases beyond the hand-written records.

## Delivery

This design is stage 5 of [WRITER_SUPPORT.md](WRITER_SUPPORT.md). The shredding
increments implement against it in order: **stage 6** struct shredding (definition
depth > 1, per-layer validity, no repetition), **stage 7** list shredding (the
repetition-level stream and offset-driven input), **stage 8** map shredding (key/value
repeated group, reusing the list machinery). Each lands with the DuckDB differential
and round-trip checks above, so `main` never holds nested write code that cannot
produce a readable file.
