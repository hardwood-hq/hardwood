<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Column-Oriented Writing

`ColumnWriter` takes an aligned slice of typed arrays — one per leaf column — through a `ColumnBatch`. It is the write-side mirror of `ColumnReader`, and the API to use when you already hold columns. Obtain one from `ParquetFileWriter.columnWriter()`; the file writer keeps ownership, so the view is not closeable.

## Writing a File

Writing takes three objects: a `FileSchema` describing what the file holds, an `OutputFile` naming where it goes, and a `ParquetFileWriter` joining them.

```java
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnWriter;
import dev.hardwood.writer.ParquetFileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

FileSchema schema = FileSchema.builder("measurement")
        .addColumn("station", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, LogicalType.string())
        .addColumn("temperature", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
        .build();

byte[][] stations = { "Hamburg".getBytes(StandardCharsets.UTF_8), "Aarhus".getBytes(StandardCharsets.UTF_8) };
double[] temperatures = { 12.3, 9.8 };

try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(Path.of("measurements.parquet")), schema)) {
    ColumnWriter columns = writer.columnWriter();
    columns.writeBatch(batch -> batch
            .bytes("station", stations)
            .doubles("temperature", temperatures));
}
```

The writer creates the batch — bound to the schema — hands it to the filler, then submits it, so there is no separate build or submit step. `columnWriter()` returns the same view on every call, so it can be obtained once and kept; call `writeBatch` as often as there is data and the writer bands the values into pages and row groups itself. Batch boundaries leave no trace in the file, and what the writer holds follows `rowGroupBufferTargetBytes` rather than the batch size; see [The Write Model](../concepts/write-model.md#why-the-writer-chooses-the-boundaries). A file is written through one API: calling both `columnWriter()` and [`rowWriter()`](write-row-by-row.md) on the same `ParquetFileWriter` is rejected, on the call that obtains the second view.

The footer is written last, so **the file is valid only once `close()` returns**. After a failure, see [Handle Write Failures](write-failures.md).

## Batch Rules

A batch is an atomic, aligned slice, and every rule below is checked as the values arrive:

- **Every leaf column of the schema must be set**, exactly once, by either index or name. A batch missing a column, or setting one twice, is rejected.
- **Every flat column's array must have the same length**, which is the batch's row count. A ragged batch is rejected. A leaf under a `LIST` or `MAP` is exempt: its array holds the concatenated entries of every row, and its length is what the offsets account for (see [Nested Columns](#nested-columns)).
- **The setter must fit the column's physical type.** `ints` on a `DOUBLE` column is rejected, as is an unknown column name. A leaf-column index outside `[0, leaf column count)` raises `IndexOutOfBoundsException`.
- **Arrays are referenced, not copied.** Do not mutate an array until the batch has been written.

Columns can be addressed by name or by zero-based leaf-column index, which is the column's position in the schema as declared:

```java
columns.writeBatch(batch -> batch
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

columns.writeBatch(batch -> batch
        .ints("id", ids)
        .ints("score", scores, nulls));
```

`Validity` is the same type the reader returns from `getLeafValidity()`, so a mask read from one file can be handed straight to the writer of another. Build one with `Validity.ofNulls(boolean[])` (`nulls[i] == true` marks row `i` null) or `Validity.of(long[])` for a packed bitmap with set-bit-means-present polarity; `Validity.NO_NULLS` is the all-present singleton.

```java
columns.writeBatch(batch -> batch
        .ints("id", ids)
        .ints("score", scores, Validity.ofNulls(nulls)));
```

A null mask on a `REQUIRED` column is rejected. A `boolean[]` mask is length-checked against the values; a `Validity` has no intrinsic length, so it is not.

!!! warning "Experimental API"
    The null-mask overloads, taking a `Validity` or a `boolean[]`, and the `struct` / `list` / `map` layer setters are annotated `@Experimental`: their shape may change in a future release.

## Binary and Fixed-Width Values

`bytes(...)` writes a `BYTE_ARRAY` column and `fixed(...)` a `FIXED_LEN_BYTE_ARRAY` column, both taking `byte[][]`. A `STRING` column takes its values as UTF-8 bytes the caller encodes:

```java
byte[][] names = new byte[people.size()][];
for (int i = 0; i < names.length; i++) {
    names[i] = people.get(i).name().getBytes(StandardCharsets.UTF_8);
}
```

Every present value of a `FIXED_LEN_BYTE_ARRAY` column must be exactly the length the column declares. The writer does not validate binary content; see [Value Ranges](../reference/writer.md#value-ranges).

## Nested Columns

Nesting is described the way `ColumnReader` reports it: leaf values in one flat array, plus a per-layer description of how they group. Leaf columns are addressed by their dotted path through the schema, including the synthetic `list.element` and `key_value` segments Parquet's nested layout introduces.

**Structs** need a `struct(...)` call only when the group is `OPTIONAL`, to say which instances are absent. Leaves beneath an absent instance are ignored:

```java
// optional group address { required int32 street; optional int32 zip; }
columns.writeBatch(batch -> batch
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

columns.writeBatch(batch -> batch
        .list("phones", offsets, listNulls)
        .ints("phones.list.element", elements, elementNulls));
```

An empty list and an absent list are different values: the empty list carries a zero delta, the absent one is marked in the list's `Validity` and must carry a zero delta as well. The same holds one level up: where an enclosing struct is marked absent, a list or map beneath it has no entries, so its offsets must carry a zero delta at that row.

**Maps** work the same way — `map(...)` sets one offsets array shared by the two leaves under `key_value`:

```java
columns.writeBatch(batch -> batch
        .map("props", offsets, mapNulls)
        .ints("props.key_value.key", keys)
        .ints("props.key_value.value", values, valueNulls));
```

Layers compose to any depth: `list("m.list.element", innerOffsets)` describes the inner list of a list of lists, whose values sit at `m.list.element.list.element`.

Offsets are validated: they must start at `0`, be non-decreasing, and end at exactly the number of entries the element column holds.

`create` rejects a repeated field that no `LIST` or `MAP` annotation accounts for; see [Schema Shapes](../reference/writer.md#schema-shapes). The legacy two-level lists a schema read from an existing file may carry are writable here, addressed through the annotated group's path exactly as the three-level layout is.

## Configuring the Writer

Pass a `WriterConfig` to `ParquetFileWriter.create(out, schema, config)` to set the codec, the page and row-group targets and the per-column encoding policy; every option, its default and what it rejects is under [Writer Options](../reference/writer.md#writer-options). The footer's key-value metadata and `created_by` identifier are set on the `ParquetFileWriter` until `close()`; see [File Metadata](../reference/writer.md#file-metadata).
