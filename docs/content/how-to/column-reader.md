<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Column-Oriented Reading

The `ColumnReader` provides batch-oriented columnar access with typed primitive arrays, avoiding per-row method calls and boxing.

!!! warning "Experimental API"
    The `ColumnReader` is under active development; the shape of the batch accessors and layer representation may change in future releases without prior deprecation.

!!! example "Try it yourself"
    Want to run it or explore the capabilities yourself? [**Column Analytics**](https://github.com/hardwood-hq/hardwood-examples/tree/main/column-analytics) aggregates a column batch by batch, [**Layer Model**](https://github.com/hardwood-hq/hardwood-examples/tree/main/layer-model) reads nested columns from offsets, and [**Concurrent Column Consumer**](https://github.com/hardwood-hq/hardwood-examples/tree/main/concurrent-column-consumer) fans batch arrays across a thread pool.

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
            int count = fare.getValueCount();
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

The [Validity](/api/latest/dev/hardwood/Validity.html) type wraps the underlying null bitmap behind `isNull(i)` / `isNotNull(i)` / `hasNulls()`. When no item in a batch is null, `getLeafValidity()` (and `getLayerValidity(k)`) returns the shared `Validity.NO_NULLS` singleton, with no per-batch allocation. Its `hasNulls()` returns `false` in O(1); see [Hot loops](#hot-loops-hoist-hasnulls-outside-the-loop).

Typed accessors are available for each fixed-width physical type: `getInts()`, `getLongs()`, `getFloats()`, `getDoubles()`, `getBooleans()`. For varlength leaves (`BINARY`, `FIXED_LEN_BYTE_ARRAY`, `INT96`) the primary accessors are `getBinaryValues()` (a `byte[]` buffer) plus `getBinaryOffsets()` (a sentinel-suffixed `int[]` of length `getValueCount() + 1`); the byte slice for value `i` is `[offsets[i], offsets[i+1])`. The buffer is capacity-sized: bytes past `offsets[getValueCount()]` are unspecified. The convenience accessors `getBinaries()` and `getStrings()` materialise one `byte[]` or `String` per leaf; hot loops should read the buffers directly. `getBinaries()` reads all three physical types; `getStrings()` reads a text column and throws `IllegalArgumentException` on any other (see [Text columns](../reference/accessors.md#text-columns)). `getBinaries()` allocates a fresh `byte[]` per value; `getStrings()` likewise allocates per value, except on dictionary-encoded columns, where repeated values reuse a single interned `String` per dictionary entry.

Column readers can also be created by index via `columnReader(int columnIndex)`. To attach a filter or customize the batch size, use the builder form: `reader.buildColumnReader("id").filter(predicate).batchSize(1024).build()`.

A filtered column reader returns **only** the matching rows: each batch's `getRecordCount()` and typed arrays exclude non-matching rows. The predicate may reference the column being read, another column, or a column outside the projection; for `columnReaders(projection)` every column is filtered to the same row set and stays row-aligned. Predicate columns that are not part of the projection are decoded internally to evaluate the filter but are not exposed.

### Reading Multiple Columns

For reading multiple columns together, use `columnReaders(projection)` which returns a `ColumnReaders` collection. Drive every reader in lockstep with `ColumnReaders.nextBatch()`. Open the columns together rather than building one `ColumnReader` per column: separately built readers batch at different row boundaries, so pairing their values by position reads different rows from each (see [RowReader vs. ColumnReader](../concepts/reader-models.md#several-columns-means-one-columnreaders-not-several-columnreaders)).

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
        long[]   v0 = columns.getColumnReader("passenger_count").getLongs();
        double[] v1 = columns.getColumnReader("trip_distance").getDoubles();
        double[] v2 = columns.getColumnReader("fare_amount").getDoubles();

        for (int i = 0; i < count; i++) {
            passengerCount += v0[i];
            tripDistance += v1[i];
            fareAmount += v2[i];
        }
    }
}
```

By default the batch size is chosen adaptively from the projected columns' physical widths, so the per-batch arrays stay within the CPU cache regardless of how wide or how many columns you project. It is capped at the rows the read can produce, so a read shorter than one batch does not allocate arrays for rows that cannot arrive. To pin a specific record count instead, use `.batchSize(int)` on the builder:

```java
try (ColumnReaders columns = parquet.buildColumnReaders(
             ColumnProjection.columns("passenger_count", "trip_distance", "fare_amount"))
        .batchSize(2048)
        .build()) {
    // ...
}
```

The batch size caps the number of **records** per batch, never the number of leaf values. A batch boundary always falls between records: a record, including all the leaf values a repeated column holds for it, is never split across two batches. A consequence for repeated columns is that `getValueCount()` can exceed the configured batch size, since one record may carry many leaf values; size any per-value buffers off `getValueCount()`, not the batch size.

`ColumnReaders.nextBatch()` advances every underlying reader once and returns `false` when the readers are exhausted. The aligned record count is exposed via `ColumnReaders.getRecordCount()`. As a defensive guard, mismatched per-column record counts throw `IllegalStateException`.

`ColumnReaders.nextBatch()` advances the whole group in one call. Calling `ColumnReader.nextBatch()` on each reader from `getColumnReader(...)` in turn also moves the group once per turn: the first reader called advances the group, and each other reader takes up that same batch. A reader called twice before its siblings advances the group twice; a sibling then called would skip the batch in between, so its `nextBatch()` throws `IllegalStateException`. A reader that is never called does not hold the group back. Closing any reader of the group closes the whole group, after which `nextBatch()` on any of its readers throws `IllegalStateException`.

### Retaining and Handing Off Batch Arrays

The arrays and `Validity` objects returned by the accessors belong to the current batch and are freshly allocated on each `nextBatch()`. A later `nextBatch()` never reuses or overwrites an array returned for an earlier batch, so you can keep a returned array and process it after advancing, including by handing it to another thread:

```java
while (columns.nextBatch()) {
    int count = columns.getRecordCount();
    long[]   v0 = columns.getColumnReader("passenger_count").getLongs();
    double[] v1 = columns.getColumnReader("trip_distance").getDoubles();
    double[] v2 = columns.getColumnReader("fare_amount").getDoubles();
    Thread.ofPlatform().start(() -> process(count, v0, v1, v2));
}
```

The reader stays a single-threaded cursor: only the loop thread calls `nextBatch()`. The *returned arrays* are detached and safe to read on other threads. (This includes the `getBinaryValues()` buffer.)

### Nested and Repeated Columns

#### Navigating nested groups in the schema

Before opening a reader for a nested column, you usually need to walk the file's schema tree to find the leaf you want. `SchemaNode.GroupNode` exposes the structural primitives:

- `isList()` / `isMap()` / `isStruct()` — disambiguate which kind of group a node is.
- `getListElement()` — for LIST groups, returns the element node, applying Parquet's backward-compatibility rules for legacy 2-level encodings. Walk to the leaf uniformly by recursing while the result `isList()`; [Logical element versus physical encoding](../concepts/nested-columns.md#logical-element-versus-physical-encoding) describes the returned node under each encoding.
- `getMapKey()` / `getMapValue()` — for MAP groups, returns the key and value nodes from the standard `map.key_value.key` / `map.key_value.value` encoding.
- `children()` — for plain struct groups, iterate to get each field.

All three navigation methods return `null` when the group isn't of the expected kind or its encoding is malformed. Callers decide whether `null` is fatal at their layer.

#### Reading nested data: the layer model

`ColumnReader` exposes a nested column's schema chain as a sequence of **layers**, numbered
`0..getLayerCount() - 1` outermost-to-innermost, each with `getLayerValidity(k)` and, for lists
and maps, `getLayerOffsets(k)`; the leaf has `getLeafValidity()`. The model is described in
[The Layer Model](../concepts/nested-columns.md).

#### Picking a null-check loop shape

`Validity` supports three loop shapes against the same value. Pick by workload:

| Loop shape | Use when |
|---|---|
| `isNull(i)` / `isNotNull(i)` direct | Cold paths: debug output, schema introspection, small batches. Reads best. |
| Hoisted `hasNulls()` + `isNotNull(i)` | Default for hot inner loops on analytical data, where most batches hit the `NO_NULLS` fast path. |
| `words()` word-wise + `Long.numberOfTrailingZeros` | Null-dense regions where you want to skip whole runs of clear bits instead of scanning every position. |

#### Hot loops: hoist `hasNulls()` outside the loop

In a per-element inner loop, **call `hasNulls()` once outside the loop and use a local boolean inside**, rather than calling `isNotNull(i)` directly per element:

```java
Validity validity = col.getLeafValidity();
boolean hasNulls = validity.hasNulls();
for (int i = 0; i < count; i++) {
    if (!hasNulls || validity.isNotNull(i)) {
        sum += values[i];
    }
}
```

On no-nulls data, the common case for analytical workloads, this is faster than calling `isNotNull(i)` per element. The examples below apply the hoist.

#### Word-wise iteration via `Validity.words()`

For null-dense regions where most elements are null, scanning bit-by-bit via `isNotNull(i)` does work proportional to the count. `Validity.words()` exposes the backing `long[]` (set-bit = present polarity) so callers can iterate only the present positions via `Long.numberOfTrailingZeros`:

```java
Validity validity = col.getLeafValidity();
long[] words = validity.words();
if (words == null) {
    // Validity.NO_NULLS — tight loop over every position.
    for (int i = 0; i < count; i++) sum += values[i];
} else {
    int wordCount = (count + 63) >>> 6;
    for (int w = 0; w < wordCount; w++) {
        long present = words[w];
        while (present != 0L) {
            int bit = Long.numberOfTrailingZeros(present);
            sum += values[(w << 6) + bit];
            present &= present - 1L;
        }
    }
}
```

The returned array is the `Validity`'s backing storage, not a copy. Callers must not mutate it. Bits at indices `>= count` are undefined and must not be read.

#### Optional struct above an optional leaf

For `optional group customer { optional int32 age }`, the leaf `age` has one `STRUCT` layer above it. The two sources of "absent", `customer == null` and `customer.age == null`, show up on different bitmaps:

```java
try (ColumnReader col = reader.columnReader("customer.age")) {
    while (col.nextBatch()) {
        int recordCount = col.getRecordCount();
        Validity structValidity = col.getLayerValidity(0);  // customer null?
        Validity leafValidity   = col.getLeafValidity();    // age null (within a present customer)?
        boolean anyNullStruct = structValidity.hasNulls();
        boolean anyNullLeaf   = leafValidity.hasNulls();
        int[] ages = col.getInts();

        for (int r = 0; r < recordCount; r++) {
            if (anyNullStruct && structValidity.isNull(r)) {
                // customer == null
            } else if (anyNullLeaf && leafValidity.isNull(r)) {
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
        boolean anyNullList = listValidity.hasNulls();
        boolean anyNullLeaf = leafValidity.hasNulls();

        for (int r = 0; r < recordCount; r++) {
            if (anyNullList && listValidity.isNull(r)) continue;        // null list
            int start = offsets[r];
            int end   = offsets[r + 1];
            if (start == end) continue;                                  // empty list
            for (int i = start; i < end; i++) {
                if (!anyNullLeaf || leafValidity.isNotNull(i)) {
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
        boolean anyNullOuter = outerValidity.hasNulls();
        boolean anyNullInner = innerValidity.hasNulls();
        boolean anyNullLeaf  = leafValidity.hasNulls();
        int[] values           = col.getInts();

        for (int r = 0; r < recordCount; r++) {
            if (anyNullOuter && outerValidity.isNull(r)) continue;
            int innerStart = outerOffsets[r];
            int innerEnd   = outerOffsets[r + 1];
            for (int j = innerStart; j < innerEnd; j++) {
                if (anyNullInner && innerValidity.isNull(j)) continue;
                int valStart = innerOffsets[j];
                int valEnd   = innerOffsets[j + 1];
                for (int i = valStart; i < valEnd; i++) {
                    if (!anyNullLeaf || leafValidity.isNotNull(i)) {
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
        boolean anyNullList = listValidity.hasNulls();
        boolean anyNullLeaf = leafValidity.hasNulls();

        for (int r = 0; r < recordCount; r++) {
            if (anyNullList && listValidity.isNull(r)) continue;
            int firstValue = layerOffsets[r];
            int lastValue  = layerOffsets[r + 1];
            for (int i = firstValue; i < lastValue; i++) {
                if (anyNullLeaf && leafValidity.isNull(i)) continue;
                int byteStart = binaryOffsets[i];
                int byteLen   = binaryOffsets[i + 1] - byteStart;
                if (matches(bytes, byteStart, byteLen)) hits++;
            }
        }
    }
}
```

#### Map

Maps report as `REPEATED` (Hardwood does not distinguish map-shape from list-shape on the layer enum; consult `getColumnSchema()` if you need that distinction).

To pair keys with values, open both leaves through `columnReaders(...)` and drive them with `ColumnReaders.nextBatch()`. The two columns share the same `map.key_value` parent, so their layer offsets agree: entry `i` of one is entry `i` of the other.

```java
try (ColumnReaders columns = reader.columnReaders(
        ColumnProjection.columns("tags.key_value.key", "tags.key_value.value"))) {

    ColumnReader keys   = columns.getColumnReader("tags.key_value.key");
    ColumnReader values = columns.getColumnReader("tags.key_value.value");

    while (columns.nextBatch()) {
        int recordCount        = columns.getRecordCount();
        int[]    entryOffsets  = keys.getLayerOffsets(0);
        Validity mapValidity   = keys.getLayerValidity(0);
        byte[]   keyBytes      = keys.getBinaryValues();
        int[]    keyOffsets    = keys.getBinaryOffsets();
        int[]    valueInts     = values.getInts();
        Validity valueValidity = values.getLeafValidity();
        boolean  anyNullMap   = mapValidity.hasNulls();
        boolean  anyNullValue = valueValidity.hasNulls();

        for (int r = 0; r < recordCount; r++) {
            if (anyNullMap && mapValidity.isNull(r)) continue;  // null map
            int start = entryOffsets[r];
            int end   = entryOffsets[r + 1];
            for (int i = start; i < end; i++) {
                int keyStart = keyOffsets[i];
                int keyLen   = keyOffsets[i + 1] - keyStart;
                String key   = new String(keyBytes, keyStart, keyLen, StandardCharsets.UTF_8);

                if (anyNullValue && valueValidity.isNull(i)) {
                    // key present, value null
                } else {
                    process(key, valueInts[i]);
                }
            }
        }
    }
}
```

As in `list<string>`, `entryOffsets` walks map entries within a record and `keyOffsets` walks byte spans within a single key.

If the map sits under an `OPTIONAL` group (e.g. `optional group meta { map<string, int> tags }`), the chain gains a `STRUCT` layer on top. The same key/value walk applies, with `getLayerValidity(0)` for `meta`, `getLayerValidity(1)` plus `getLayerOffsets(1)` for the map, and `getLeafValidity()` for the value:

```java
Validity metaValidity  = values.getLayerValidity(0);   // STRUCT layer for `meta`
Validity mapValidity   = values.getLayerValidity(1);   // REPEATED layer for the map
int[]    entryOffsets  = values.getLayerOffsets(1);
```
