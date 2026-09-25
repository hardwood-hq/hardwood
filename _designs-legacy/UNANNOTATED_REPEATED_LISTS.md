# Unannotated repeated fields as lists

Status: Completed

## Problem

The Parquet format spec defines a backward-compatibility rule for repeated
fields that carry no `LIST`/`MAP` annotation
([LogicalTypes – Nested Types](https://parquet.apache.org/docs/file-format/types/logicaltypes/#nested-types)):

> A repeated field that is neither contained by a LIST- or MAP-annotated group
> nor annotated by LIST or MAP should be interpreted as a required list of
> required elements where the element type is the type of the field.

The `RowReader` assembly path did not implement this rule. A schema such as

```
message record {
  REPEATED INT32 foo;
}
```

was surfaced as a single scalar (`foo = 42`) rather than a list
(`foo = [42, 42]`). The same applies to an unannotated repeated *group*, which
must be read as a list of structs whose element type is the group itself.

## Scope

The decision of list-vs-scalar for a top-level field or a struct child is made
in `TopLevelFieldMap`, based solely on the `LIST`/`MAP` annotation
(`SchemaNode.GroupNode#isList()` / `#isMap()`). A bare `REPEATED` node falls
through to the `Primitive`/`Struct` branch.

Such a file already routes through `NestedRowReader` /`NestedBatchDataView`
because the leaf column has `maxRepetitionLevel > 0` (so `FileSchema#isFlatSchema`
returns `false`). The definition/repetition levels and the per-row list offsets
are therefore already materialised — only the field-descriptor assembly treats
the column as a scalar.

## Approach

Treat a bare repeated field as a synthetic *required list whose element is the
field itself*, reusing the existing `FieldDesc.ListOf` machinery and
`PqListImpl`.

A `REPEATED` node reaching `TopLevelFieldMap` as a top-level field or as a struct
child is always a bare repeated field: the structural repeated layers of
`LIST`/`MAP` groups are consumed inside `buildListDesc` / `buildMapDesc` and the
unwrapped element is what gets handed onwards, so they never arrive here with
their repeated wrapper intact. This makes detection a simple
`repetitionType() == REPEATED` check at exactly two call sites (the top-level
field loop and the struct-child loop) — list elements and map values are
deliberately excluded.

For such a node a `ListOf` descriptor is built with:

- a synthetic `REQUIRED` `LIST`-annotated `GroupNode` wrapper whose single child
  is the original node, with `maxDefinitionLevel = node.maxDef - 1` and
  `maxRepetitionLevel = node.maxRep - 1` (the levels "outside" the list);
- `elementSchema` set to the original node itself — never unwrapped. A primitive
  element materialises as a leaf value; a group element materialises as a struct.
  This differs from the `LIST`-annotated backward-compatibility rules in
  `SchemaNode#getListElement`, which unwrap a single-field repeated group; for an
  unannotated repeated field the element type is always the field's own type;
- `nullDefLevel = node.maxDef - 1` (the list is null exactly when its enclosing
  parent is absent) and `elementDefLevel = node.maxDef` (an element exists at or
  above the field's own definition level).

These levels coincide with those of a legacy two-level list, so the existing
`PqListImpl` range/null/empty computation applies unchanged.

## Legacy two-level list-of-lists (LIST element rule)

The Parquet `LIST` backward-compatibility rules define five cases for resolving
the element of a repeated field. `SchemaNode#getListElement` was missing the
rule for *a repeated group with a single field that is itself `repeated`*: such
a group is a genuine element (a list whose element is a list), not a synthetic
single-field wrapper to be unwrapped. The rule is applied before the
`array`/`_tuple` naming check, so

```
optional group mylist (LIST) {
  repeated group bag { repeated int32 num }
}
```

resolves its element to `bag` and reads as `list<struct{num: list<int>}>` —
`num`, being an unannotated repeated field, is itself a list per the rule above.

Reading this shape also requires the leaf's repetition layers to be counted
correctly. `NestedLevelComputer.walkLayersToLeaf` derived one `REPEATED` layer
per `LIST`/`MAP` annotation (folding the synthetic `repeated group` scaffolding
into its parent) but emitted no layer for a bare `repeated` *primitive*. A
repeated primitive that is not the suppressed element of a `LIST`/`MAP` wrapper
now contributes its own `REPEATED` layer, with threshold `maxDef - 1` (the
container's definition level), so the layer count matches the leaf's maximum
repetition level. This makes a legacy two-level list-of-lists produce the same
layer descriptor — and therefore the same multi-level offsets — as the
equivalent fully-annotated form.

## Out of scope

For the unannotated top-level repeated field, `ColumnReader` already treats a
repeated leaf as nested (`maxRepetitionLevel > 0`); that part of the change is
confined to the `RowReader` / `PqRow` assembly layer. The `NestedLevelComputer`
layer-counting fix is shared by both reader paths.
