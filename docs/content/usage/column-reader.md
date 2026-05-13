<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Column-Oriented Reading

The `ColumnReader` provides batch-oriented columnar access with typed primitive arrays, avoiding per-row method calls and boxing. This is the fastest way to consume Parquet data when you process columns independently.

!!! warning "Experimental API"
    The `ColumnReader` is under active development; the shape of the batch accessors and layer representation may change in future releases without prior deprecation.

### Reading a Single Column

```java
import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ColumnReader;

try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) {
    // Create a column reader by name (spans all row groups automatically)
    try (ColumnReader fare = reader.columnReader("fare_amount")) {
        double sum = 0;
        while (fare.nextBatch()) {
            int count = fare.getRecordCount();
            double[] values = fare.getDoubles();
            Validity validity = fare.getLeafValidity();
            boolean hasNulls = validity.hasNulls();

            for (int i = 0; i < count; i++) {
                if (!hasNulls || validity.isNotNull(i)) {
                    sum += values[i];
                }
            }
        }
    }
}
```

The [Validity](/api/latest/dev/hardwood/reader/Validity.html) type wraps the underlying null bitmap behind `isNull(i)` / `isNotNull(i)` / `hasNulls()`. When no item in a batch is null, `getLeafValidity()` (and `getLayerValidity(k)`) returns the shared `Validity.NO_NULLS` singleton — `hasNulls()` returns `false` in O(1) and gates the no-per-element-check fast path, no per-batch allocation. Hot inner loops should hoist `hasNulls()` into a local boolean before iterating; see [Hot loops](#hot-loops-hoist-hasnulls-outside-the-loop) for why.

Typed accessors are available for each fixed-width physical type: `getInts()`, `getLongs()`, `getFloats()`, `getDoubles()`, `getBooleans()`. For varlength leaves (`BINARY`, `FIXED_LEN_BYTE_ARRAY`, `INT96`) the primary accessors are `getBinaryValues()` (a `byte[]` buffer) plus `getBinaryOffsets()` (a sentinel-suffixed `int[]` of length `getValueCount() + 1`); the byte slice for value `i` is `[offsets[i], offsets[i+1])`. The convenience accessors `getBinaries()` and `getStrings()` materialise one `byte[]` or `String` per leaf — useful for low-volume / debug paths but allocate per-row, so hot loops should read the buffers directly.

Column readers can also be created by index via `columnReader(int columnIndex)`. To attach a filter, use the builder form: `reader.buildColumnReader("id").filter(predicate).build()`.

### Reading Multiple Columns

For reading multiple columns together, use `columnReaders(projection)` which returns a `ColumnReaders` collection. Drive every reader in lockstep with `ColumnReaders.nextBatch()`:

```java
import dev.hardwood.Hardwood;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.schema.ColumnProjection;

try (ParquetFileReader parquet = ParquetFileReader.open(InputFile.of(path));
     ColumnReaders columns = parquet.buildColumnReaders(
             ColumnProjection.columns("passenger_count", "trip_distance", "fare_amount"))
             .build()) {

    long passengerCount = 0;
    double tripDistance = 0, fareAmount = 0;

    while (columns.nextBatch()) {
        int count = columns.getRecordCount();
        double[] v0 = columns.getColumnReader("passenger_count").getDoubles();
        double[] v1 = columns.getColumnReader("trip_distance").getDoubles();
        double[] v2 = columns.getColumnReader("fare_amount").getDoubles();

        for (int i = 0; i < count; i++) {
            passengerCount += (long) v0[i];
            tripDistance += v1[i];
            fareAmount += v2[i];
        }
    }
}
```

`ColumnReaders.nextBatch()` advances every underlying reader once and returns `false` when any reader is exhausted — partial advancement isn't possible because all readers consume from a shared `RowGroupIterator`. The aligned record count is exposed via `ColumnReaders.getRecordCount()`. As a defensive guard, mismatched per-reader record counts throw `IllegalStateException`. Single-column consumers, or callers that need fine-grained per-reader cadence, can still call `ColumnReader.nextBatch()` directly on the readers returned by `getColumnReader(...)`.

### Nested and Repeated Columns

#### Navigating nested groups in the schema

Before opening a reader for a nested column, you usually need to walk the file's schema tree to find the leaf you want. `SchemaNode.GroupNode` exposes the structural primitives:

- `isList()` / `isMap()` / `isStruct()` — disambiguate which kind of group a node is.
- `getListElement()` — for LIST groups, returns the element node, applying Parquet's backward-compatibility rules for legacy 2-level encodings.
- `getMapKey()` / `getMapValue()` — for MAP groups, returns the key and value nodes from the standard `map.key_value.key` / `map.key_value.value` encoding.
- `children()` — for plain struct groups, iterate to get each field.

All three navigation methods return `null` when the group isn't of the expected kind or its encoding is malformed. Callers decide whether `null` is fatal at their layer.

#### Reading nested data: the layer model

`ColumnReader` exposes a column's schema chain as a sequence of **layers**. Each non-leaf node along the chain contributes zero or one layer:

| Schema node | Contributes layer? | Layer kind |
|---|---|---|
| `REQUIRED` group | no | — |
| `OPTIONAL` group (struct) | yes | `STRUCT` |
| `LIST` / `MAP`-annotated group | yes — exactly one | `REPEATED` |

Layers are numbered `0..getLayerCount() - 1` outermost-to-innermost, and the leaf is queried separately. A flat column reports `getLayerCount() == 0`.

For each layer, two buffers describe the items at that layer:

- `getLayerValidity(k)` — a [Validity](/api/latest/dev/hardwood/reader/Validity.html) indexed by item position; `isNull(i)` / `isNotNull(i)` answer the per-item question, `hasNulls()` is the O(1) fast-path gate. Returns the shared `Validity.NO_NULLS` singleton when no item at layer `k` is null in this batch.
- `getLayerOffsets(k)` — sentinel-suffixed offsets of length `count(k) + 1` into the next inner layer's items (or, for the innermost layer, into the leaf-value array). Only valid when `getLayerKind(k) == REPEATED`; throws on a `STRUCT` layer.

The leaf has its own `getLeafValidity()` (also a `Validity`), and the four states of a `LIST`/`MAP` container fall out cleanly from offsets and validity:

| Logical value | `getLayerValidity` | `offsets[r+1] - offsets[r]` |
|---|---|---|
| `null`           | `isNull(r)`    | `0` |
| `[]`             | `isNotNull(r)` | `0` |
| `[null]`         | `isNotNull(r)` | `1`, leaf validity says null |
| `[v]`            | `isNotNull(r)` | `1`, leaf validity says not null |

Empty-vs-null is the offsets diff; the validity bit picks out null. No empty-marker bitmap is needed.

A quick reference for what `getLayerCount()` / `getLayerKind(k)` report for common chains:

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

Two rules generate this table:

1. **STRUCT keeps cardinality.** Items at layer `k+1` equal items at layer `k`. STRUCT layers carry validity, no offsets.
2. **REPEATED expands cardinality.** Items at layer `k+1` equal `getLayerOffsets(k)[count(k)]`. REPEATED layers carry both validity and offsets.

#### Hot loops: hoist `hasNulls()` outside the loop

`Validity.NO_NULLS` is the common case on analytical workloads — most columns are non-null in most batches — and the API is designed to make checking for it O(1). In a per-element inner loop, **call `hasNulls()` once outside the loop and use a local boolean inside**, rather than calling `isNotNull(i)` directly per element:

```java
Validity validity = col.getLeafValidity();
boolean hasNulls = validity.hasNulls();
for (int i = 0; i < count; i++) {
    if (!hasNulls || validity.isNotNull(i)) {
        sum += values[i];
    }
}
```

Extracting the check once per batch is meaningfully faster than calling it per element on no-nulls data, which is the common analytical-workload case. Examples below show the hoist applied; for cold paths (small batches, schema introspection, debug code) the direct `isNull(i)` / `isNotNull(i)` form is fine and reads better.

#### Flat column

```java
try (ColumnReader fare = reader.columnReader("fare_amount")) {
    while (fare.nextBatch()) {
        int count = fare.getRecordCount();
        double[] values = fare.getDoubles();
        Validity validity = fare.getLeafValidity();
        boolean hasNulls = validity.hasNulls();
        for (int i = 0; i < count; i++) {
            if (!hasNulls || validity.isNotNull(i)) {
                sum += values[i];
            }
        }
    }
}
```

#### Optional struct above an optional leaf

For `optional group customer { optional int32 age }`, the leaf `age` has one `STRUCT` layer above it. The two sources of "absent" — `customer == null` versus `customer.age == null` — show up on different bitmaps:

```java
try (ColumnReader col = reader.columnReader("customer.age")) {
    while (col.nextBatch()) {
        int recordCount = col.getRecordCount();
        Validity structValidity = col.getLayerValidity(0);  // customer null?
        Validity leafValidity   = col.getLeafValidity();    // age null (within a present customer)?
        boolean structHasNulls = structValidity.hasNulls();
        boolean leafHasNulls   = leafValidity.hasNulls();
        int[] ages = col.getInts();

        for (int r = 0; r < recordCount; r++) {
            if (structHasNulls && structValidity.isNull(r)) {
                // customer == null
            } else if (leafHasNulls && leafValidity.isNull(r)) {
                // customer != null, age == null
            } else {
                sumAge += ages[r];
            }
        }
    }
}
```

#### Simple list

For `list<double> fare_components`:

```java
try (ColumnReader col = reader.columnReader("fare_components.list.element")) {
    while (col.nextBatch()) {
        int recordCount = col.getRecordCount();
        double[] values = col.getDoubles();
        int[] offsets = col.getLayerOffsets(0);          // length recordCount + 1
        Validity listValidity = col.getLayerValidity(0);
        Validity leafValidity = col.getLeafValidity();
        boolean listHasNulls = listValidity.hasNulls();
        boolean leafHasNulls = leafValidity.hasNulls();

        for (int r = 0; r < recordCount; r++) {
            if (listHasNulls && listValidity.isNull(r)) continue;        // null list
            int start = offsets[r];
            int end   = offsets[r + 1];
            if (start == end) continue;                                  // empty list
            for (int i = start; i < end; i++) {
                if (!leafHasNulls || leafValidity.isNotNull(i)) {
                    sum += values[i];
                }
            }
        }
    }
}
```

The sentinel suffix on `offsets` removes the last-record special case from the inner loop bounds.

#### Multi-Level Nesting

For `list<list<int>>` (`getLayerCount() == 2`, both layers `REPEATED`), layer 0's offsets index into layer 1's offsets, which in turn index into the leaf array. The pattern generalises to arbitrary depth:

```java
try (ColumnReader col = reader.columnReader("matrix.list.element.list.element")) {
    while (col.nextBatch()) {
        int recordCount = col.getRecordCount();
        int[] outerOffsets     = col.getLayerOffsets(0);   // length recordCount + 1
        int[] innerOffsets     = col.getLayerOffsets(1);
        Validity outerValidity = col.getLayerValidity(0);
        Validity innerValidity = col.getLayerValidity(1);
        Validity leafValidity  = col.getLeafValidity();
        boolean outerHasNulls = outerValidity.hasNulls();
        boolean innerHasNulls = innerValidity.hasNulls();
        boolean leafHasNulls  = leafValidity.hasNulls();
        int[] values           = col.getInts();

        for (int r = 0; r < recordCount; r++) {
            if (outerHasNulls && outerValidity.isNull(r)) continue;
            int innerStart = outerOffsets[r];
            int innerEnd   = outerOffsets[r + 1];
            for (int j = innerStart; j < innerEnd; j++) {
                if (innerHasNulls && innerValidity.isNull(j)) continue;
                int valStart = innerOffsets[j];
                int valEnd   = innerOffsets[j + 1];
                for (int i = valStart; i < valEnd; i++) {
                    if (!leafHasNulls || leafValidity.isNotNull(i)) {
                        sum += values[i];
                    }
                }
            }
        }
    }
}
```

#### List of strings

Layer offsets and binary offsets are orthogonal axes: layer offsets walk which leaf values belong to a record (across the `getValueCount()` axis); binary offsets walk byte spans within a single varlength leaf (across the byte axis of the values buffer).

```java
try (ColumnReader col = reader.columnReader("tags.list.element")) {
    while (col.nextBatch()) {
        int recordCount = col.getRecordCount();
        int[] layerOffsets     = col.getLayerOffsets(0);
        Validity listValidity  = col.getLayerValidity(0);
        byte[] bytes           = col.getBinaryValues();    // capacity-sized
        int[] binaryOffsets    = col.getBinaryOffsets();   // length valueCount + 1
        Validity leafValidity  = col.getLeafValidity();
        boolean listHasNulls = listValidity.hasNulls();
        boolean leafHasNulls = leafValidity.hasNulls();

        for (int r = 0; r < recordCount; r++) {
            if (listHasNulls && listValidity.isNull(r)) continue;
            int firstValue = layerOffsets[r];
            int lastValue  = layerOffsets[r + 1];
            for (int i = firstValue; i < lastValue; i++) {
                if (leafHasNulls && leafValidity.isNull(i)) continue;
                int byteStart = binaryOffsets[i];
                int byteLen   = binaryOffsets[i + 1] - byteStart;
                if (matches(bytes, byteStart, byteLen)) hits++;
            }
        }
    }
}
```

#### Map

Maps report as `REPEATED` (Hardwood does not distinguish map-shape from list-shape on the layer enum — consult `getColumnSchema()` if you need that distinction).

To pair keys with values, open both leaves and drive them in lockstep. The two columns share the same `map.key_value` parent, so their layer offsets agree — entry `i` of one is entry `i` of the other:

```java
try (ColumnReader keys   = reader.columnReader("tags.key_value.key");
     ColumnReader values = reader.columnReader("tags.key_value.value")) {

    while (keys.nextBatch() & values.nextBatch()) {
        int recordCount        = keys.getRecordCount();
        int[]    entryOffsets  = keys.getLayerOffsets(0);
        Validity mapValidity   = keys.getLayerValidity(0);
        byte[]   keyBytes      = keys.getBinaryValues();
        int[]    keyOffsets    = keys.getBinaryOffsets();
        int[]    valueInts     = values.getInts();
        Validity valueValidity = values.getLeafValidity();
        boolean  mapHasNulls   = mapValidity.hasNulls();
        boolean  valueHasNulls = valueValidity.hasNulls();

        for (int r = 0; r < recordCount; r++) {
            if (mapHasNulls && mapValidity.isNull(r)) continue;  // null map
            int start = entryOffsets[r];
            int end   = entryOffsets[r + 1];
            for (int i = start; i < end; i++) {
                int keyStart = keyOffsets[i];
                int keyLen   = keyOffsets[i + 1] - keyStart;
                String key   = new String(keyBytes, keyStart, keyLen, StandardCharsets.UTF_8);

                if (valueHasNulls && valueValidity.isNull(i)) {
                    // key present, value null
                } else {
                    process(key, valueInts[i]);
                }
            }
        }
    }
}
```

Two orthogonal offset axes show up here, as in `list<string>`: `entryOffsets` walks map entries within a record (across the `getValueCount()` axis), `keyOffsets` walks byte spans within a single key (across the byte axis of `getBinaryValues()`).

If the map sits under an `OPTIONAL` group — e.g. `optional group meta { map<string, int> tags }` — the chain gains a `STRUCT` layer on top. The same key/value lockstep walk applies, with `getLayerValidity(0)` for `meta`, `getLayerValidity(1)` plus `getLayerOffsets(1)` for the map, and `getLeafValidity()` for the value:

```java
Validity metaValidity  = reader.getLayerValidity(0);   // STRUCT layer for `meta`
Validity mapValidity   = reader.getLayerValidity(1);   // REPEATED layer for the map
int[]    entryOffsets  = reader.getLayerOffsets(1);
```

#### Real items only

The leaf array and `getLayerOffsets` carry **real items only** — phantom slots from null/empty parents at any `REPEATED` layer are excluded. `getValueCount()` returns the real leaf count. `STRUCT` layers do not expand or contract the item stream — they carry a validity bitmap of the same length as their parent. Only `REPEATED` layers add cardinality, via their offsets.

#### Counts at each layer

Every per-layer buffer is sized to `count(k)`, defined recursively:

- `count(0) == getRecordCount()`
- For `k > 0`: `count(k)` equals `count(k-1)` if layer `k-1` is `STRUCT`, or `getLayerOffsets(k-1)[count(k-1)]` (the trailing sentinel) if layer `k-1` is `REPEATED`.

The leaf array itself follows the same rule one step past the innermost layer, so `getValueCount()` matches `count(layerCount)`.

Deeper nestings extend the same chain: at depth N you walk `getLayerOffsets(0)` through `getLayerOffsets(N - 1)`, checking `getLayerValidity(k)` (and, for `REPEATED` layers, the zero-length offsets diff that flags an empty container) at each step before descending.

