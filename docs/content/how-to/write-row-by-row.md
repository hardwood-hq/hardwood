<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Row-Oriented Writing

`RowWriter` writes one record at a time, addressing fields by the names they carry in the schema and taking logical-type values as the Java types the reader returns for them. It is the write-side mirror of `RowReader`.

!!! warning "Experimental API"
    `RowWriter`, `StructBuilder`, `ListBuilder` and `MapBuilder` are annotated `@Experimental`: their shape may change in a future release.

## Writing a File

Writing takes three objects: a `FileSchema` describing what the file holds, an `OutputFile` naming where it goes, and a `ParquetFileWriter` joining them.

```java
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.RowWriter;
import java.nio.file.Path;
import java.time.LocalDate;

FileSchema schema = FileSchema.builder("person")
        .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
        .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, new LogicalType.StringType())
        .addColumn("hired", PhysicalType.INT32, RepetitionType.OPTIONAL, new LogicalType.DateType())
        .build();

try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(Path.of("people.parquet")), schema)) {
    RowWriter rows = writer.rowWriter();

    rows.writeRow(row -> row
            .setLong("id", 1)
            .setString("name", "Ada")
            .setDate("hired", LocalDate.of(2019, 3, 1)));

    rows.writeRow(row -> row
            .setLong("id", 2)
            .setString("name", "Alan")
            .setDate("hired", LocalDate.of(2021, 11, 15)));
}
```

The writer creates the `StructBuilder`, hands it to the filler, and stages the record when the filler returns — there is no separate build or submit step. `RowWriter` is not closeable: the `ParquetFileWriter` owns the file, and closing it writes the records still staged along with the footer.

The file is produced front to back and the footer is written last, so **it becomes a valid Parquet file only when `close()` returns**. A writer abandoned before that leaves nothing readable at the destination.

## Typed Setters

Each setter names the type of the field it writes, and a field's declared type decides which setter fits. `setBoolean`, `setInt`, `setLong`, `setFloat`, `setDouble` and `setBinary` write physical values; `setString`, `setDate`, `setTime`, `setTimestamp`, `setLocalTimestamp`, `setDecimal`, `setUuid` and `setInterval` write logical-type values, which the writer converts to the column's physical representation.

Given a schema that also declares a `DECIMAL` `salary`, a `TIMESTAMP` `created_at` and a `UUID` `external_id`:

```java
rows.writeRow(row -> row
        .setLong("id", 7)
        .setString("name", "Grace")
        .setDecimal("salary", new BigDecimal("81500.00"))
        .setTimestamp("created_at", Instant.now())
        .setUuid("external_id", UUID.randomUUID()));
```

A setter that does not fit the field's declared type is rejected, as is a value outside the range the field's annotation declares — an `INT(8)` column takes `[-128, 128)`, a `DECIMAL(9, 2)` column an unscaled value of at most nine digits. See [Writer Reference](../reference/writer.md) for the full setter map and the ranges.

A value carrying more precision than its column can hold — an `Instant` with microseconds written to a `TIMESTAMP(MILLIS)` column — is rejected by default. Configure `precisionLossPolicy(PrecisionLossPolicy.TRUNCATE)` to drop the digits that do not fit instead.

## Nulls and Unset Fields

An `OPTIONAL` field is null in three equivalent spellings: passing `null` to an object-typed setter, calling `setNull`, and leaving the field unset.

```java
rows.writeRow(row -> row
        .setLong("id", 3)
        .setString("name", "Katherine")
        .setNull("hired"));

// Same record: "hired" is never mentioned.
rows.writeRow(row -> row
        .setLong("id", 3)
        .setString("name", "Katherine"));
```

A `REQUIRED` field left unset fails the record. A failed record is staged in full or not at all: if the filler rejects a value or throws, everything it staged is discarded and the writer is left as it was, so the caller can handle the failure and carry on with the next record.

## Structs, Lists, and Maps

Nesting is entered with a filler per level. Fields are addressed by their user-visible names throughout — the synthetic `list.element` and `key_value` path segments Parquet's nested layout introduces never appear.

```java
FileSchema schema = FileSchema.builder("person")
        .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
        .struct("address", RepetitionType.OPTIONAL, address -> address
                .addColumn("city", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, new LogicalType.StringType())
                .addColumn("zip", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, new LogicalType.StringType()))
        .list("phones", RepetitionType.OPTIONAL,
                element -> element.primitive(PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                        new LogicalType.StringType()))
        .map("props", RepetitionType.OPTIONAL, PhysicalType.BYTE_ARRAY, new LogicalType.StringType(),
                value -> value.primitive(PhysicalType.INT64, RepetitionType.OPTIONAL))
        .build();

rows.writeRow(row -> row
        .setLong("id", 1)
        .setStruct("address", address -> address
                .setString("city", "Berlin")
                .setString("zip", "10115"))
        .setList("phones", phones -> phones
                .addString("+49 30 1234")
                .addString("+49 30 5678"))
        .setMap("props", props -> props
                .addEntry(entry -> entry.setString("key", "reads").setLong("value", 12))
                .addEntry(entry -> entry.setString("key", "writes").setNull("value"))));
```

A map entry is a struct with the two fields `key` and `value`, so `addEntry` takes a `StructBuilder` like any other struct.

Lists distinguish three states, and each has its own spelling:

| State | How to write it |
|---|---|
| A list with entries | `setList("phones", phones -> phones.addString("…"))` |
| An empty list | `setList("phones", phones -> { })` |
| An absent (null) list | `setNull("phones")`, or leave the field unset |

`addNull()` appends a null *entry* to a list whose element is `OPTIONAL`; on a list of `REQUIRED` elements it is rejected. Lists nest through `addList` and `addStruct`:

```java
rows.writeRow(row -> row
        .setList("people", people -> people
                .addStruct(person -> person.setString("name", "Ada"))
                .addStruct(person -> person.setString("name", "Alan")))
        .setList("grid", grid -> grid
                .addList(inner -> inner.addInt(1).addInt(2))
                .addList(inner -> inner.addInt(3))));
```

A builder is valid only inside the filler it was handed to. Retaining one and using it after its scope has ended is rejected rather than writing into a later record.

## Addressing Fields by Index

Every by-name setter has a by-index twin, addressing a field by its position in the struct being filled. `getFieldCount()` and `getFieldName(int)` report the positions, which lets code that walks a record's fields uniformly write them back without knowing their names.

```java
// person is the record being written; its fields are matched to the schema's by name.
rows.writeRow(row -> {
    for (int field = 0; field < row.getFieldCount(); field++) {
        switch (row.getFieldName(field)) {
            case "id" -> row.setLong(field, person.id());
            case "name" -> row.setString(field, person.name());
            default -> row.setNull(field);
        }
    }
});
```

Every rule of the by-name form holds unchanged — same type check, same range check, same rejection of a field set twice, same scope lifetime. Only the way the field is named differs, so an index outside `[0, getFieldCount())` takes the place of an unknown name.

!!! warning "The reader's index and the writer's index are not always the same position"
    On the reader, a field index is a position among an accessor's **projected** children. On the writer, it is a position in the struct as **declared** in the schema being written. The two agree when a whole file is read into its own schema — copying a record field by field then works — and diverge as soon as a projection drops or reorders fields. Resolve the name with `getFieldName(int)` on both sides when the two schemas are not identical. See [The Write Model](../concepts/write-model.md#index-addressing-on-the-two-sides).

## Configuring the Writer

`WriterConfig` carries the page and row-group targets, the compression codec, the encoding policy and the precision-loss policy. Pass one to `create`:

```java
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.writer.PrecisionLossPolicy;
import dev.hardwood.writer.WriterConfig;

WriterConfig config = WriterConfig.builder()
        .codec(CompressionCodec.ZSTD)
        .rowGroupTargetBytes(64L << 20)
        .precisionLossPolicy(PrecisionLossPolicy.TRUNCATE)
        .build();

try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
    // ...
}
```

Every option, its default and what it rejects: [Writer Reference](../reference/writer.md).

## One API per File

`RowWriter` stages records into batches and submits them through the columnar core, so the file it produces is laid out exactly like one written through [`writeBatch`](write-column-by-column.md) — same paging, same row-group cadence, same encoding decisions and statistics. A file is written through one API or the other, though: calling both on the same `ParquetFileWriter` is rejected.
