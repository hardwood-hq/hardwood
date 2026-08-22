<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Column-Oriented Writing

`ParquetFileWriter.writeBatch` takes an aligned slice of typed arrays — one per leaf column — through a `ColumnBatch`. It is the write-side mirror of `ColumnReader`, and the API to use when you already hold columns.

## Writing a File

Writing takes three objects: a `FileSchema` describing what the file holds, an `OutputFile` naming where it goes, and a `ParquetFileWriter` joining them.

```java
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

FileSchema schema = FileSchema.builder("measurement")
        .addColumn("station", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, new LogicalType.StringType())
        .addColumn("temperature", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
        .build();

byte[][] stations = { "Hamburg".getBytes(StandardCharsets.UTF_8), "Aarhus".getBytes(StandardCharsets.UTF_8) };
double[] temperatures = { 12.3, 9.8 };

try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(Path.of("measurements.parquet")), schema)) {
    writer.writeBatch(batch -> batch
            .bytes("station", stations)
            .doubles("temperature", temperatures));
}
```

The writer creates the batch — bound to the schema — hands it to the filler, then submits it, so there is no separate build or submit step. Call `writeBatch` as often as there is data; the writer bands the values into pages and row groups itself.

The file is produced front to back and the footer is written last, so **it becomes a valid Parquet file only when `close()` returns**. A writer abandoned before that leaves nothing readable at the destination.

## Batch Rules

A batch is an atomic, aligned slice, and every rule is checked as the values arrive rather than at write time:

- **Every leaf column of the schema must be set**, exactly once, by either index or name. A batch missing a column, or setting one twice, is rejected.
- **Every array must have the same length**, which is the batch's row count. A ragged batch is rejected.
- **The setter must fit the column's physical type.** `ints` on a `DOUBLE` column is rejected, as is an unknown column name or an out-of-range index.
- **Arrays are referenced, not copied.** Do not mutate an array until the batch has been written.

Columns can be addressed by name or by zero-based leaf-column index, which is the column's position in the schema as declared:

```java
writer.writeBatch(batch -> batch
        .bytes(0, stations)
        .doubles(1, temperatures));
```

## Nulls

The mask-less setter is the all-present form, for both `REQUIRED` and `OPTIONAL` columns. To write nulls, pass a `Validity` or a `boolean[]` mask:

```java
import dev.hardwood.Validity;

int[] ids = { 1, 2, 3 };
int[] scores = { 10, 0, 30 };          // the slot at a null row is ignored
boolean[] nulls = { false, true, false };

writer.writeBatch(batch -> batch
        .ints("id", ids)
        .ints("score", scores, nulls));
```

The values array stays full length — one slot per row — and the entry at a null row is never read, so there is no need to compact the values.

`Validity` is the same type the reader returns from `getLeafValidity()`, so a mask read from one file can be handed straight to the writer of another. Build one with `Validity.ofNulls(boolean[])` (`nulls[i] == true` marks row `i` null) or `Validity.of(long[])` for a packed bitmap with set-bit-means-present polarity; `Validity.NO_NULLS` is the all-present singleton.

```java
writer.writeBatch(batch -> batch
        .ints("id", ids)
        .ints("score", scores, Validity.ofNulls(nulls)));
```

A null mask on a `REQUIRED` column is rejected. A `boolean[]` mask is length-checked against the values; a `Validity` has no intrinsic length, so it is not.

!!! warning "Experimental API"
    The `Validity` overloads and the `struct` / `list` / `map` layer setters are annotated `@Experimental`: their shape may change in a future release.

## Binary and Fixed-Width Values

`bytes(...)` writes a `BYTE_ARRAY` column and `fixed(...)` a `FIXED_LEN_BYTE_ARRAY` column, both taking `byte[][]`. A `STRING` column is a `BYTE_ARRAY` column annotated `STRING`, so its values are written as UTF-8 bytes — the columnar API has no `String` overload, and the encoding is the caller's to perform:

```java
byte[][] names = new byte[people.size()][];
for (int i = 0; i < names.length; i++) {
    names[i] = people.get(i).name().getBytes(StandardCharsets.UTF_8);
}
```

Every present value of a `FIXED_LEN_BYTE_ARRAY` column must be exactly the length the column declares.

## Nested Columns

Nesting is described the way `ColumnReader` reports it: leaf values in one flat array, plus a per-layer description of how they group. Leaf columns are addressed by their dotted path through the schema, including the synthetic `list.element` and `key_value` segments Parquet's nested layout introduces.

**Structs** need a `struct(...)` call only when the group is `OPTIONAL`, to say which instances are absent. Leaves beneath an absent instance are ignored:

```java
// optional group address { required int32 street; optional int32 zip; }
writer.writeBatch(batch -> batch
        .struct("address", Validity.ofNulls(new boolean[] { false, true }))
        .ints("address.street", streets)
        .ints("address.zip", zips, zipNulls));
```

**Lists** carry an offsets array of length `rowCount + 1`, where `offsets[i + 1] - offsets[i]` is the number of entries of list `i` and a zero delta is an empty list. The element leaf holds the concatenated entries:

```java
// optional group phones (LIST) { repeated group list { optional int32 element } }
// row 0: [1, 2]   row 1: []   row 2: null   row 3: [3, null, 5]
int[] offsets = { 0, 2, 2, 2, 5 };
Validity listNulls = Validity.ofNulls(new boolean[] { false, false, true, false });
int[] elements = { 1, 2, 3, 0, 5 };
boolean[] elementNulls = { false, false, false, true, false };

writer.writeBatch(batch -> batch
        .list("phones", offsets, listNulls)
        .ints("phones.list.element", elements, elementNulls));
```

An empty list and an absent list are different values: the empty list carries a zero delta, the absent one is marked in the list's `Validity` and must carry a zero delta as well.

**Maps** work the same way — `map(...)` sets one offsets array shared by the two leaves under `key_value`:

```java
writer.writeBatch(batch -> batch
        .map("props", offsets, mapNulls)
        .ints("props.key_value.key", keys)
        .ints("props.key_value.value", values, valueNulls));
```

Layers compose to any depth: `list("m.list.element", innerOffsets)` describes the inner list of a list of lists, whose values sit at `m.list.element.list.element`.

Offsets are validated: they must start at `0`, be non-decreasing, and end at exactly the number of entries the element column holds.

!!! note "One nested shape is not writable"
    An `OPTIONAL` struct group directly enclosing a repeated field is rejected with an `UnsupportedOperationException` naming the shape, by both write APIs. Make the enclosing struct `REQUIRED`, or move the repeated field out of it.

## Batch Size

A batch is an *arrival* unit, not a layout unit. The writer distributes each batch's values into per-column page buffers, cuts pages at the page target, and flushes a row group once the buffered data reaches the row-group target — so batch boundaries leave no trace in the file, and a batch larger than the row-group target is split at the boundary rather than held whole.

Submit whole columns as one large batch, or stream many small ones and discard each after handing it over; the file is the same either way. What bounds memory is `rowGroupTargetBytes`, not the batch size — see [The Write Model](../concepts/write-model.md).

## Configuring the Writer

`WriterConfig` carries the page and row-group targets, the compression codec and the per-column encoding policy. Pass one to `create`:

```java
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.WriterConfig;

WriterConfig config = WriterConfig.builder()
        .codec(CompressionCodec.ZSTD)
        .rowGroupTargetBytes(64L << 20)
        .encoding("temperature", ColumnEncoding.BYTE_STREAM_SPLIT)
        .build();
```

Every option, its default and what it rejects: [Writer Reference](../reference/writer.md).

## One API per File

A file is written through one API or the other: calling both `writeBatch` and [`rowWriter()`](write-row-by-row.md) on the same `ParquetFileWriter` is rejected. The row-oriented layer stages records into batches and submits them through this same core, so it produces the same layout — see [Choosing a Write API](index.md#choosing-a-write-api).
