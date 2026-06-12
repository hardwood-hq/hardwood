<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# The Layer Model

`ColumnReader` hands you a column as flat, typed primitive arrays. For a nested column — a struct
field, a list, a map, or any combination — those flat arrays need extra structure to say which
leaf values belong to which record, and where the nulls and empty containers sit. The **layer
model** is how `ColumnReader` expresses that structure without boxing or per-row objects. This page
explains the model; for worked code against it, see
[Column-Oriented Reading](../how-to/column-reader.md).

> **A note on lineage.** The layer model — flat value arrays, offset buffers, and set-bit-present
> validity bitmaps — takes inspiration from [Apache Arrow](https://arrow.apache.org/)'s columnar
> representation, so the shapes feel familiar if you come from an Arrow-based engine. The
> resemblance is conceptual only: Hardwood implements no part of the Arrow specification, and the
> buffers are plain Java arrays rather than an Arrow-bit-compatible layout.

## Layers

`ColumnReader` exposes a column's schema chain as a sequence of **layers**. Each non-leaf node
along the chain contributes zero or one layer:

| Schema node | Contributes layer? | Layer kind |
|---|---|---|
| `REQUIRED` group | no | — |
| `OPTIONAL` group (struct) | yes | `STRUCT` |
| `LIST` / `MAP`-annotated group | yes — exactly one | `REPEATED` |
| synthetic `repeated group` inside a `LIST` / `MAP` | no | — |

Layers follow a column's *logical* structure rather than its physical schema nodes, so node count
and layer count diverge wherever a `LIST` or `MAP` appears: its stack of group nodes contributes a
single `REPEATED` layer. [How schema nodes map to layers](#how-schema-nodes-map-to-layers) works this
through on a concrete schema.

Layers are numbered `0..getLayerCount() - 1` outermost-to-innermost, and the leaf is queried
separately. A flat column reports `getLayerCount() == 0`.

For each layer, two buffers describe the items at that layer:

- `getLayerValidity(k)` — a [Validity](/api/latest/dev/hardwood/reader/Validity.html) indexed by
  item position; `isNull(i)` / `isNotNull(i)` answer the per-item question, `hasNulls()` is the
  O(1) fast-path gate. Returns the shared `Validity.NO_NULLS` singleton when no item at layer `k`
  is null in this batch.
- `getLayerOffsets(k)` — sentinel-suffixed offsets of length `count(k) + 1` into the next inner
  layer's items (or, for the innermost layer, into the leaf-value array). Only valid when
  `getLayerKind(k) == REPEATED`; throws on a `STRUCT` layer.

The leaf has its own `getLeafValidity()` (also a `Validity`).

## Empty versus null containers

The four states of a `LIST` / `MAP` container fall out cleanly from offsets and validity:

| Logical value | `getLayerValidity` | `offsets[r+1] - offsets[r]` |
|---|---|---|
| `null`           | `isNull(r)`    | `0` |
| `[]`             | `isNotNull(r)` | `0` |
| `[null]`         | `isNotNull(r)` | `1`, leaf validity says null |
| `[v]`            | `isNotNull(r)` | `1`, leaf validity says not null |

Empty-vs-null is the offsets diff; the validity bit picks out null. No empty-marker bitmap is
needed.

## STRUCT keeps cardinality, REPEATED expands it

Two rules govern how item counts flow down the chain:

1. **STRUCT keeps cardinality.** Items at layer `k+1` equal items at layer `k`. STRUCT layers
   carry validity, no offsets.
2. **REPEATED expands cardinality.** Items at layer `k+1` equal `getLayerOffsets(k)[count(k)]`.
   REPEATED layers carry both validity and offsets.

The leaf array and `getLayerOffsets` carry **real items only** — phantom slots from null/empty
parents at any `REPEATED` layer are excluded. `getValueCount()` returns the real leaf count.
`STRUCT` layers do not expand or contract the item stream; only `REPEATED` layers add cardinality,
via their offsets.

Those two rules generate the layer shape of any chain:

| Schema chain | `getLayerCount()` | Kinds (outer → inner) |
|---|---|---|
| `optional double x` | 0 | — |
| `optional struct { ... int x }` | 1 | STRUCT |
| `list<int>` | 1 | REPEATED |
| `map<string, int>` | 1 | REPEATED |
| `list<list<int>>` | 2 | REPEATED, REPEATED |
| `optional struct { list<int> }` | 2 | STRUCT, REPEATED |
| `list<optional struct { ... }>` | 2 | REPEATED, STRUCT |
| `optional struct { map<string, int> }` | 2 | STRUCT, REPEATED |

Maps report as `REPEATED` — the layer enum does not distinguish map-shape from list-shape; consult
`getColumnSchema()` if you need that distinction.

## How schema nodes map to layers

Because layers follow logical structure, the physical group nodes of a schema do not map one-to-one
onto layers: a `LIST` or `MAP` is encoded as an annotated outer group wrapping a synthetic `repeated
group`, and that pair contributes one `REPEATED` layer rather than one layer per node. The rules
above resolve any schema unambiguously. Take a `contacts` column whose schema prints as:

```
optional group contacts (ListType[]) {
  repeated group list {
    optional group element {
      required byte_array name (StringType[]);
      optional byte_array phoneNumber (StringType[]);
    }
  }
}
```

Walking the chain for `contacts.list.element.name`, the two `LIST` nodes fuse into one layer:

```
optional group contacts (ListType[])   ──┐
  repeated group list                  ──┴─►  REPEATED  (layer 0)
    optional group element             ────►  STRUCT    (layer 1)
      required byte_array name          ───►  leaf
```

`getLayerCount()` is `2`, with kinds `[REPEATED, STRUCT]`. The list's own nullability is not a
separate layer: a null `contacts` is recorded in `getLayerValidity(0)` of the `REPEATED` layer — the
same layer shape a `required` list would have, differing only in that validity. Layer 1 is the
`element` struct, which carries validity but no offsets, so `getLayerOffsets(1)` throws `Layer 1 is
STRUCT, not REPEATED`.

## Counts at each layer

Every per-layer buffer is sized to `count(k)`, defined recursively:

- `count(0) == getRecordCount()`
- For `k > 0`: `count(k)` equals `count(k-1)` if layer `k-1` is `STRUCT`, or
  `getLayerOffsets(k-1)[count(k-1)]` (the trailing sentinel) if layer `k-1` is `REPEATED`.

The leaf array itself follows the same rule one step past the innermost layer, so
`getValueCount()` matches `count(layerCount)`. Deeper nestings extend the same chain: at depth N you
walk `getLayerOffsets(0)` through `getLayerOffsets(N - 1)`, checking `getLayerValidity(k)` (and, for
`REPEATED` layers, the zero-length offsets diff that flags an empty container) at each step before
descending.
