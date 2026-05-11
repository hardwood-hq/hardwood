# Design: ColumnReader Layer Model

Tracking issue: #430. Subsumes #436.

## Context

`ColumnReader` exposes a column's batch as typed leaf values plus per-layer
metadata. A column's schema chain — every group between the root and the
projected leaf — surfaces to the consumer as a sequence of **layers**, with
two kinds:

- `STRUCT` — a user-authored `OPTIONAL` group. Carries validity but no
  offsets; its cardinality matches its parent's.
- `REPEATED` — a `LIST`- or `MAP`-annotated group. Carries both validity
  and offsets, and is the only way cardinality grows along the chain.

`REQUIRED` groups and synthetic LIST/MAP scaffolding (the inner `repeated
group` of the 3-level encoding, the canonical `element` / `list` /
`array_element` wrapper) contribute no layer.

This shape mirrors [Apache Arrow](https://arrow.apache.org/)'s
columnar format for nested data: one validity bitmap per nullable
layer plus one offsets buffer per repeated layer, with empty-vs-null
on lists falling out of offsets length and struct-null living on its
own validity bit. Arrow-aligned engines converge on the same model.
Hardwood follows it *in spirit* — same per-layer concept, same set-
bit-= -present polarity, same offsets-plus-values pattern for
varlength leaves — and lifts it to apply uniformly to every layer
between root and leaf, including `STRUCT` layers.

This is *not* an Arrow-bit-compatible representation. Validity is
`java.util.BitSet` behind a `Validity` wrapper, layer and binary offsets
are `int[]`, varlength leaf bytes are `byte[]`. The shared piece is the
model, not the bytes. Bit-compatible storage is a separate concern (see
Out of scope).

## Design properties

Each choice below ties to a concrete consumer-side benefit.

- **Per-layer validity for every `STRUCT` and `REPEATED` layer along
  the chain.** A leaf inside an `OPTIONAL` struct reports
  struct-presence and leaf-presence independently — the distinction
  #436 calls out. The same shape generalises to arbitrary depth, so
  `optional struct { optional struct { ... } }` chains and
  `list<optional struct>` work without special cases.
- **Set-bit-= -present storage polarity.** The `BitSet` backing
  `Validity` is laid out the Arrow way (set bit = present) so a future
  swap to a `ByteBuffer`-backed Arrow-bit-compatible storage (#153) is
  an internal change. Consumer-side predicates expose null-polarity
  (`isNull` / `isNotNull` / `hasNulls`) to match the rest of the API
  (`RowReader.isNull`).
- **Sentinel-suffixed offsets.** `getLayerOffsets(k)[count(k)] ==
  count(k+1)` removes the last-record special case from consumer
  loops: `int end = offsets[r + 1]` works for every record, no
  `r + 1 < count ? ... : valueCount` ternary.
- **Real-items-only offsets and leaf values.** Phantom slots from
  null/empty parents are excluded: `values[i]` is the `i`-th real
  leaf, not an arbitrary position in the raw def/rep stream that
  might or might not be meaningful. Empty-vs-null at a `REPEATED`
  layer falls out of `offsets[r+1] - offsets[r] == 0` plus the
  validity bit — no separate empty-marker bitmap.
- **`Validity` sealed type with `NO_NULLS` singleton.** The no-nulls
  batch is the common analytical-workload state and stays zero-
  allocation; `hasNulls()` answers the fast-path question in O(1) and
  returns `false` on the singleton. The wrapper eliminates the
  null-check bug class a nullable bitmap return would permit, while
  keeping `BitSet`'s intrinsics available behind `Validity.Backed` for
  advanced consumers via sealed-type pattern matching.
- **`LayerKind` explicit in the API.** `getLayerKind(k)` distinguishes
  `STRUCT` from `REPEATED` at the type level; consumers do not have
  to infer list-ness from the existence of an offsets array.
  `getLayerOffsets` throws on `STRUCT` layers rather than returning a
  no-op array.
- **`(byte[] bytes, int[] offsets)` for varlength leaves.** A 262K-row
  binary-column batch is one shared bytes buffer and one offsets
  array, not 262K per-value `byte[]` allocations. The convenience
  accessors `getBinaries()` / `getStrings()` allocate per row on top
  of the buffer for low-volume paths; hot loops read the buffer
  directly. Matches Arrow's variable-binary layout in spirit and
  makes future bit-compatible storage (#153) an internal swap rather
  than an API break.
- **Uniform per-layer accessor shape at the API level.** The same
  pattern — `getLayerValidity(k)` / `getLayerOffsets(k)` /
  `getLeafValidity()` — applies across flat columns, optional structs,
  lists, nested lists, and maps. Cold and medium-throughput consumers
  write one mental model from root to leaf. Hot loops do split: the
  standard pattern in vectorised engines built around Arrow-shaped
  columnar data is to hoist `hasNulls()` into a local boolean outside
  the per-element loop and short-circuit on it before calling
  `isNotNull(i)` / `isNull(i)`. This is a HotSpot-specific concern:
  C2's profile-guided null-check optimisation specialises local
  references far more aggressively than instance-field loads, so
  making the no-nulls signal legible to the JIT as a local boolean
  restores the inner loop's fast path. The usage doc spells the
  pattern out; the API itself stays the same shape.

## API

### Layer model

A column's schema chain is walked from root to leaf. Each non-leaf node
contributes **zero or one layer**:

| Schema node | Contributes layer? | Layer kind |
|---|---|---|
| `REQUIRED` group (struct) | no | — |
| `OPTIONAL` group (struct) | yes | `STRUCT` |
| `LIST` / `MAP` annotated group (any encoding) | yes — exactly one | `REPEATED` |

Layers are numbered `0..layerCount-1` outermost-to-innermost. The leaf
itself is queried through dedicated accessors, not as a layer.
`getLayerCount() == 0` denotes a flat column whose chain has no nullable
groups and no repetition.

`MAP` does not get its own `LayerKind`. It is reported as `REPEATED`;
consumers that need to distinguish map-shape from list-shape consult
`getColumnSchema()`. Arrow keeps the types distinct at the schema level;
Hardwood follows the schema node for that distinction rather than
duplicating it on the runtime layer enum.

**LIST/MAP scaffolding collapses to one layer.** The modern 3-level list
(`optional group list (LIST) { repeated group list { optional <element> } }`),
the legacy 2-level form (`optional group list (LIST) { repeated <element> }`),
and the canonical map
(`optional group map (MAP) { repeated group key_value { required key; optional value; } }`)
all contribute exactly one `REPEATED` layer for the list/map. The inner
`repeated group` of the 3-level form, and the canonical `element` /
`list` / `array_element` single-field wrapper, are part of the
scaffolding; their definition-level contribution is folded into the
layer's offsets and validity bitmap.

**User-authored `OPTIONAL` groups always contribute a `STRUCT` layer**,
including when they appear as the element type of a list or the value
type of a map. For `list<optional group customer { optional int32 age }>`
the layers are:

- layer 0: `REPEATED` (the `LIST`)
- layer 1: `STRUCT` (`customer` — the user's optional group, not
  synthetic)
- leaf: `age`, with its own leaf validity

so customer-null vs. age-null is preserved inside lists exactly as it is
at the top level. The detection rule is structural: only groups that are
part of a `LIST`- or `MAP`-annotated parent's canonical encoding (the
inner `repeated group` plus any single-field synthetic wrapper) are
omitted; everything else nullable the user wrote becomes a `STRUCT`
layer.

### Items at a layer

Every per-layer buffer is sized to the count of items at that layer,
defined recursively:

- `count(0) == getRecordCount()`
- For `k > 0`: `count(k) == count(k-1)` if layer `k-1` is `STRUCT`, or
  `getLayerOffsets(k-1)[count(k-1)]` (the trailing sentinel) if layer
  `k-1` is `REPEATED`.

At layer 0 an item is a top-level record. Inside a `REPEATED` layer's
offsets, items are individual list/map entries. `STRUCT` layers do not
expand or contract the item stream — they only carry a validity bitmap
of the same length as their parent. Only `REPEATED` layers add
cardinality, via their offsets.

The leaf array follows the same recurrence one step past the innermost
layer: `getValueCount() == count(layerCount)`.

### Validity

Validity is exposed through a sealed `Validity` type with two
implementations:

- `Validity.NO_NULLS` — singleton signalling "no item at this scope
  is null in the current batch." `hasNulls()` returns `false` in
  O(1); other accessors return their no-nulls constants.
- `Validity.Backed(BitSet bits)` — wrapper over the per-item null
  bitmap. Bit `i` is set iff item `i` is **not null** (set-bit-= -
  present storage polarity, matching Arrow's layout).

Consumer-facing accessors describe nullability, matching
`RowReader.isNull`'s vocabulary:

- `hasNulls()` — fast-path gate; `true` iff this is `Backed`.
- `isNull(int i)` / `isNotNull(int i)` — per-item predicates; both
  short-circuit correctly on the singleton without per-call null
  checks.
- `nullCount(int count)` — null-item count. Takes the layer's item
  count because the singleton has no intrinsic length.
- `nextNull(int from, int count)` / `nextNotNull(int from, int count)` —
  index helpers. Both take `count` because the singleton has no
  intrinsic length.

The singleton design eliminates the null-check bug class a nullable
`BitSet` return type would permit: there is no `null` to forget, and
`isNull` / `isNotNull` produce the correct answer for both the
no-nulls and backed cases without conditional logic on the caller's
side. The fast path stays zero-allocation — `Validity.of(null)` from
the internal pipeline returns the shared singleton.

### Real items only

Layer offsets and the leaf array carry **real items only** — phantom
slots from null/empty parents at any `REPEATED` layer are excluded.
`getValueCount()` returns the real leaf count. Two consequences:

- `offsets[r+1] - offsets[r] == 0` at a `REPEATED` layer means the
  container at index `r` has no entries. Whether that's a null container
  or a present-empty one is determined by `getLayerValidity(k).isNull(r)`.
- For chains containing at least one `REPEATED` layer, the
  fixed-width leaf array is compacted: `getInts()[i]` is the `i`-th real
  leaf, not the `i`-th raw def/rep position. For chains with only
  `STRUCT` layers above the leaf, the leaf array is sized to
  `getRecordCount()` (STRUCT preserves cardinality).

**Values at absent positions are undefined.** The leaf array always
carries `getValueCount()` entries and per-layer offsets always span the
full layer cardinality, but the contents at positions where the leaf or
any enclosing layer is absent (validity bit clear) carry undefined
values — typically whatever was last decoded into that slot, or zero.
Consumers must check validity before reading. This matches Arrow's
semantics for child arrays under a null parent.

### Full API surface

```java
public class ColumnReader implements AutoCloseable {
    // Batch iteration
    boolean nextBatch();
    int getRecordCount();
    int getValueCount();   // count(layerCount); for nested chains == getLayerOffsets(innermost)[count(innermost)]

    // Layer metadata — stable for the reader's lifetime; safe to cache once
    int getLayerCount();
    LayerKind getLayerKind(int layer);

    // Per-layer buffers
    /// Validity at `layer`. Returns Validity.NO_NULLS when no item at
    /// that layer is null in the current batch.
    Validity getLayerValidity(int layer);

    /// Offsets at `layer`. Length == count(layer) + 1, with
    /// offsets[count(layer)] == count(layer + 1) (or getValueCount() for
    /// the innermost layer). offsets[i+1] - offsets[i] == 0 denotes an
    /// empty list/map. Throws IllegalStateException if
    /// getLayerKind(layer) != REPEATED.
    int[] getLayerOffsets(int layer);

    // Leaf — fixed-width primitives
    /// Validity over the leaf-value array, indexed 0..getValueCount().
    /// Returns Validity.NO_NULLS when no leaf in the current batch is
    /// null.
    Validity getLeafValidity();
    int[]    getInts();
    long[]   getLongs();
    float[]  getFloats();
    double[] getDoubles();
    boolean[] getBooleans();

    // Leaf — varlength (BINARY / FIXED_LEN_BYTE_ARRAY / INT96).
    // Orthogonal to layer offsets: layer offsets walk which leaf values
    // belong to a record (across getValueCount()); binary offsets walk
    // byte spans within a single leaf value (across the byte axis).
    /// Backing byte buffer. Capacity-sized: only bytes in
    /// [0, getBinaryOffsets()[getValueCount()]) are valid; bytes beyond
    /// that position are unspecified.
    byte[] getBinaryValues();

    /// Sentinel-suffixed offsets into getBinaryValues(). Length ==
    /// getValueCount() + 1; byte length of value i is offsets[i+1] -
    /// offsets[i]. For FIXED_LEN_BYTE_ARRAY columns the offsets are
    /// trivially i * width.
    int[] getBinaryOffsets();

    // Leaf — convenience accessors (per-row allocation; intended for
    // low-volume / debug paths). Hot loops should read the buffers above.
    byte[][] getBinaries();
    String[] getStrings();

    // Low-level escape hatch
    int[] getDefinitionLevels();   // may return null for flat columns
    int[] getRepetitionLevels();   // null when maxRepetitionLevel == 0

    ColumnSchema getColumnSchema();
    void close();
}

public enum LayerKind { STRUCT, REPEATED }

public sealed interface Validity permits Validity.NoNulls, Validity.Backed {
    Validity NO_NULLS = new NoNulls();
    static Validity of(BitSet bits) { ... }
    boolean hasNulls();
    boolean isNull(int i);
    boolean isNotNull(int i);
    int nullCount(int count);
    int nextNull(int from, int count);
    int nextNotNull(int from, int count);
    record NoNulls() implements Validity { ... }
    record Backed(BitSet bits) implements Validity { ... }
}
```

### Consumer code by column shape

**Flat column** (`optional double fare_amount`):

```java
try (ColumnReader col = reader.columnReader("fare_amount")) {
    while (col.nextBatch()) {
        int count = col.getRecordCount();
        double[] values = col.getDoubles();
        Validity validity = col.getLeafValidity();
        for (int i = 0; i < count; i++) {
            if (validity.isNotNull(i)) sum += values[i];
        }
    }
}
```

**Optional struct + optional leaf** (`optional group customer { optional int32 age }`),
querying `age` — the #436 disambiguation:

```java
// layerCount == 1: one STRUCT layer for customer
Validity structValidity = col.getLayerValidity(0);
Validity leafValidity   = col.getLeafValidity();
int[]    ages           = col.getInts();
for (int r = 0; r < recordCount; r++) {
    if (structValidity.isNull(r)) {
        // customer == null
    } else if (leafValidity.isNull(r)) {
        // customer present, age == null
    } else {
        sumAge += ages[r];
    }
}
```

**Simple list** (`list<double> fare_components`):

```java
// layerCount == 1: one REPEATED layer for the list
int[]    offsets       = col.getLayerOffsets(0);   // length recordCount + 1
Validity listValidity  = col.getLayerValidity(0);
Validity leafValidity  = col.getLeafValidity();
double[] values        = col.getDoubles();

for (int r = 0; r < recordCount; r++) {
    if (listValidity.isNull(r)) continue;        // null list
    int start = offsets[r];
    int end   = offsets[r + 1];
    if (start == end) continue;                    // empty list
    for (int i = start; i < end; i++) {
        if (leafValidity.isNotNull(i)) sum += values[i];
    }
}
```

**Nested list** (`list<list<int>>`, `layerCount == 2`):

```java
Validity outerValidity  = col.getLayerValidity(0);
int[]    outerOffsets   = col.getLayerOffsets(0);
Validity innerValidity  = col.getLayerValidity(1);
int[]    innerOffsets   = col.getLayerOffsets(1);
Validity leafValidity   = col.getLeafValidity();
int[]    values         = col.getInts();

for (int r = 0; r < recordCount; r++) {
    if (outerValidity.isNull(r)) continue;
    for (int j = outerOffsets[r]; j < outerOffsets[r + 1]; j++) {
        if (innerValidity.isNull(j)) continue;
        for (int i = innerOffsets[j]; i < innerOffsets[j + 1]; i++) {
            if (leafValidity.isNotNull(i)) sum += values[i];
        }
    }
}
```

The chained-offsets pattern generalises to arbitrary depth: layer `k`'s
offsets index into layer `k+1`'s offsets, or into the leaf value array
at the innermost layer.

**List of strings** (`list<string> tags`) — the cross-product of layer
offsets and binary offsets:

```java
int[]    layerOffsets    = col.getLayerOffsets(0);
Validity listValidity    = col.getLayerValidity(0);
byte[]   bytes           = col.getBinaryValues();    // capacity-sized
int[]    binaryOffsets   = col.getBinaryOffsets();   // length valueCount + 1
Validity leafValidity    = col.getLeafValidity();

for (int r = 0; r < recordCount; r++) {
    if (listValidity.isNull(r)) continue;
    for (int i = layerOffsets[r]; i < layerOffsets[r + 1]; i++) {
        if (leafValidity.isNull(i)) continue;
        int byteStart = binaryOffsets[i];
        int byteLen   = binaryOffsets[i + 1] - byteStart;
        // bytes[byteStart..byteStart+byteLen) is the i-th string's UTF-8
    }
}
```

**Map** (`map<string, int> tags`) — open the key and value leaves and
drive them in lockstep on shared layer offsets:

```java
try (ColumnReader keys   = reader.columnReader("tags.key_value.key");
     ColumnReader values = reader.columnReader("tags.key_value.value")) {

    while (keys.nextBatch() & values.nextBatch()) {
        int[]    entryOffsets  = keys.getLayerOffsets(0);
        Validity mapValidity   = keys.getLayerValidity(0);
        byte[]   keyBytes      = keys.getBinaryValues();
        int[]    keyOffsets    = keys.getBinaryOffsets();
        int[]    valueInts     = values.getInts();
        Validity valueValidity = values.getLeafValidity();

        for (int r = 0; r < keys.getRecordCount(); r++) {
            if (mapValidity.isNull(r)) continue;
            for (int i = entryOffsets[r]; i < entryOffsets[r + 1]; i++) {
                String key = new String(keyBytes, keyOffsets[i],
                        keyOffsets[i + 1] - keyOffsets[i], UTF_8);
                if (valueValidity.isNull(i)) continue;
                process(key, valueInts[i]);
            }
        }
    }
}
```

## Implementation

### Layer extraction from schema

`NestedLevelComputer.computeLayers(rootGroup, columnIndex)` walks the
schema chain at reader-construction time and produces a `Layers`
descriptor with:

- `LayerKind[] kinds` — kind per layer slot (`STRUCT` or `REPEATED`).
- `int[] defThresholds` — for each layer, the def level at-or-above
  which the node at that layer is present. Drives validity computation.
- `int[] itemDefThresholds` (derived, cached) — minimum def-level at
  which a real item slot exists at layer `k`. Recurrence:
  `itemDefThresholds[0] = 0`; for `k > 0`,
  `itemDefThresholds[k] = itemDefThresholds[k-1]` if `kinds[k-1] ==
  STRUCT`, or `defThresholds[k-1] + 1` if `kinds[k-1] == REPEATED` (the
  inner LIST/MAP scaffolding bumps def by one to reach the next layer's
  items).
- `int[] itemRepThresholds` (derived, cached) — number of `REPEATED`
  layers strictly before `k`. A raw stream position with
  `repLevel <= itemRepThresholds[k]` is a candidate item-start at
  layer `k`.

The same `Layers` descriptor drives the worker's per-batch validity
computation and the `ColumnReader`-side real-items-only conversion.

### Validity computation

Workers produce per-batch validity bitmaps directly in present polarity
— no flip-at-boundary cost. For each layer `k`:

```
bit `i` is set iff defLevel[i] >= layerDefThresholds[k]
```

at the item positions that advance layer `k` (positions where
`repLevel[i] <= itemRepThresholds[k]`). The bitmap is allocated lazily:
the worker leaves it untouched while every item seen so far is present,
and on the first absent item backfills `[0, itemIdx)` and maintains
bits normally from there. When no absent item is seen in the batch, no
bitmap is materialised — `Validity.of(null)` resolves to the
`Validity.NO_NULLS` singleton at the API boundary.

`FlatColumnWorker` applies the same lazy-init pattern for the flat-leaf
no-absents fast path, gated by a `currentBatchHasAbsents` flag. This
matters because typical analytical scans see batches with no nulls; an
O(N) BitSet initialization per batch on that path costs measurably.

### Real-items-only translation

`NestedBatch` carries raw-counting offsets and a raw-counting leaf
array — every position in the def/rep stream maps to a slot, including
phantom positions for null/empty parents. `ColumnReader` translates to
the real-items-only public shape via `NestedLevelComputer.computeRealView`
on first access of any layer/leaf accessor and caches the result for
the batch. Invalidated in `nextBatch()`.

A single pass over the raw stream produces:

- Per-layer real-item counts.
- Sentinel-suffixed layer offsets keyed by real-item indices.
- Per-layer validity bitmaps over real-item indices.
- A real-leaf validity bitmap.
- A `realToRawLeaf` index mapping that lets fixed-width leaf arrays be
  compacted into typed primitive arrays on first access of `getInts()`
  / `getLongs()` / etc. For chains with no `REPEATED` layer (STRUCT-only
  or flat), the raw leaf array passes through without compaction.

Total cost: `O(rawValueCount)` per batch, paid only when a consumer
queries the new accessors.

### Offsets

Layer offsets are sentinel-suffixed: length `count(layer) + 1`, with the
final entry equal to `count(layer + 1)` (or `getValueCount()` for the
innermost layer). The sentinel removes the last-record special case from
consumer code and is the basis of the empty-list-via-equal-offsets rule.

Offsets are `int[]` (32-bit). The same int32 cap applies to binary
offsets for varlength leaves: the total byte length of all values in a
single batch caps at `Integer.MAX_VALUE` (~2 GB). Typical workloads sit
comfortably below this; columns with very wide values (~100 KB strings)
can hit it before the row count does. The worker detects overflow when
appending to the binary buffer and throws; the recovery is to reduce
batch size for that column.

For chains with at least one `REPEATED` layer the real-items-only
compaction performed at the `ColumnReader` surface allocates a fresh
`byte[]` for the compacted binary bytes and holds both the raw worker
buffer and the compacted copy until the next `nextBatch()` swap. Peak
binary memory per batch is therefore up to **2×** the raw-bytes count.
Consumers near the int32 ceiling on raw bytes should size batches so
the compacted copy fits as well.

Arrow distinguishes `LIST` (int32 offsets) from `LARGE_LIST` (int64);
Hardwood does not need the latter at column-reader granularity because
offsets are per-batch, not per-file. If batch sizes ever exceed
`Integer.MAX_VALUE` total values, a separate API decision is required.

### Varlength leaf storage

For `BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY` / `INT96` columns the per-
batch value buffer is a `(byte[] bytes, int[] offsets)` pair, allocated
by `BatchExchange.allocateArray`:

- `BYTE_ARRAY` / `INT96`: bytes buffer pre-sized to
  `BINARY_BYTES_PER_VALUE_HINT × capacity` (32 by default), grown on
  overflow via the worker's append path.
- `FIXED_LEN_BYTE_ARRAY`: bytes buffer sized exactly to `width ×
  capacity`; offsets pre-filled trivially as `i * width`.

The bytes buffer is **capacity-sized**, not exact-sized: only the prefix
`[0, offsets[valueCount])` is meaningful on read; any tail beyond is
unspecified scratch space. Consumers must read the buffer through
`getBinaryOffsets()[valueCount]`, not through `bytes.length`.

This shape lets the worker append directly into a single contiguous
buffer rather than allocating one `byte[]` per leaf, and matches the
Arrow variable-binary array layout in spirit. The convenience accessors
`getBinaries()` / `getStrings()` allocate per row on top of this buffer
for low-volume / debug paths; hot loops use the buffer directly.

### NestedBatch and BatchExchange.Batch

`NestedBatch` is the internal record produced by `NestedColumnWorker`
and consumed by both `ColumnReader` and the row API
(`NestedRowReader` / `NestedBatchIndex` / `NestedBatchDataView` read its
fields directly without going through `ColumnReader`). `BatchExchange
.Batch` is the flat-column equivalent. Both carry:

- `validity` (flat) / `elementValidity` (nested leaf) as `BitSet` with
  set-bit-= -present polarity; `null` means "all present in this batch."
  Per-layer validity is not pre-computed on the batch — `ColumnReader`
  derives it on demand via `RealView`.
- Sentinel-suffixed offset arrays per layer; `null` at `STRUCT` slots.
- Raw-counting (not real-items-only) — translation happens at the
  `ColumnReader` API boundary; internal consumers continue to read
  raw-counting offsets and leaf arrays.
- `(byte[] bytes, int[] offsets)` for varlength `values`, via
  `BinaryBatchValues`.

The internal pipeline uses raw `BitSet` throughout; `Validity` exists
only at the `ColumnReader` public surface.

### Routing

A column with at least one `STRUCT` or `REPEATED` layer goes through
`NestedColumnWorker` — def levels are required for per-layer validity,
which `FlatColumnWorker` does not produce. Pure-flat columns
(`layerCount == 0`) take the `FlatColumnWorker` fast path, which skips
def-level capture for `maxRepetitionLevel == 0 && maxDefinitionLevel
<= 1` workloads.

## Out of scope

- **`ByteBuffer`-backed buffers / bit-for-bit Arrow IPC interop**
  (#153). Validity is `BitSet`-backed, offsets and varlength bytes are
  Java arrays. Switching to `ByteBuffer` / `IntBuffer` so Arrow
  consumers can take the bytes without a copy is a pure storage swap
  invisible to the public API: `Validity.Backed` swaps its internal
  type, the byte and offset arrays become buffer views, the method
  signatures stay.
- **`RowReader` record-API changes.** Internal; not user-visible.
- **Decoder-side changes.** No change to page reading, decoding, or
  definition/repetition-level production. Only the public surface and
  the per-batch derivation step are affected.
- **Compressed leaf encodings** (run-end / REE / dictionary as a
  public-API shape). Fixed-width leaves are materialised into typed
  primitive arrays plus a validity bitmap; varlength leaves into
  `(bytes, offsets)`. Adopting a compressed leaf shape would be a
  separate API surface.

## Key files

| File | Role |
|------|------|
| `core/.../reader/ColumnReader.java` | Public API; layer accessors, real-items-only translation, varlength leaf buffers, convenience accessors |
| `core/.../reader/Validity.java` | Sealed `Validity` type with `NoNulls` singleton + `Backed(BitSet)` record |
| `core/.../reader/LayerKind.java` | `LayerKind` enum (`STRUCT`, `REPEATED`) |
| `core/.../internal/reader/NestedLevelComputer.java` | Schema-chain walk producing `Layers`; per-batch validity, layer offsets, and real-view computation |
| `core/.../internal/reader/NestedBatch.java` | Internal batch holder: leaf-level `BitSet` validity, layer-indexed sentinel-suffixed offsets, `BinaryBatchValues` for varlength leaves |
| `core/.../internal/reader/BatchExchange.java` | Flat batch holder (`Batch.validity` BitSet, `Batch.values` typed array or `BinaryBatchValues`); `allocateArray` per physical type |
| `core/.../internal/reader/BinaryBatchValues.java` | `(byte[] bytes, int[] offsets)` pair for varlength leaves; append-and-grow API |
| `core/.../internal/reader/FlatColumnWorker.java` | Flat-column drain; lazy validity bitmap, append-into-shared-buffer for varlength |
| `core/.../internal/reader/NestedColumnWorker.java` | Nested-column drain; leaf-level validity, layer-indexed sentinel-suffixed offsets, varlength buffer append |
| `core/.../internal/reader/NestedBatchIndex.java` | Row-API consumer of `NestedBatch`; rep-level-indexed `multiOffsets` view |
| `core/.../internal/reader/NestedBatchDataView.java` | Row-API materialisation; reads validity in present polarity, derives empty-list from offsets |
