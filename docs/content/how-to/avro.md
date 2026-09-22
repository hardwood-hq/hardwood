<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Avro Support

If your application already works with Avro records, for instance in a Kafka or Spark pipeline, you can read Parquet files directly into `GenericRecord` instances instead of using Hardwood's own row API. The `hardwood-avro` module handles the schema conversion and record materialization, matching the behavior of parquet-java's `AvroReadSupport`. Add it alongside `hardwood-core`:

```xml
<dependency>
    <groupId>dev.hardwood</groupId>
    <artifactId>hardwood-avro</artifactId>
</dependency>
```

Read rows as `GenericRecord`:

```java
import dev.hardwood.avro.AvroReaders;
import dev.hardwood.avro.AvroRowReader;
import dev.hardwood.reader.ParquetFileReader;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
     AvroRowReader reader = AvroReaders.rowReader(fileReader)) {

    Schema avroSchema = reader.getSchema();

    while (reader.hasNext()) {
        GenericRecord record = reader.next();

        // Access fields by name
        long id = (Long) record.get("id");
        String name = (String) record.get("name");

        // Nested structs are nested GenericRecords
        GenericRecord address = (GenericRecord) record.get("address");
        if (address != null) {
            String city = (String) address.get("city");
        }

        // Lists and maps use standard Java collections
        @SuppressWarnings("unchecked")
        List<String> tags = (List<String>) record.get("tags");
    }
}
```

`AvroReaders` supports all reader options: column projection, predicate pushdown, and their combination:

```java
// With filter
AvroRowReader reader = AvroReaders.buildRowReader(fileReader)
    .filter(FilterPredicate.gt("id", 1000L))
    .build();

// With projection
AvroRowReader reader = AvroReaders.buildRowReader(fileReader)
    .projection(ColumnProjection.columns("id", "name"))
    .build();

// With both
AvroRowReader reader = AvroReaders.buildRowReader(fileReader)
    .projection(ColumnProjection.columns("id", "name"))
    .filter(FilterPredicate.gt("id", 1000L))
    .build();
```

A projected record's fields, and a projected struct's, follow the order the projection names them in, as a `RowReader`'s fields do (see [Index order](../reference/query-controls.md#index-order)), so `GenericRecord.get(int)` takes that position.

Values are stored in Avro's standard representations: timestamps as `Long` (millis, micros or nanos since epoch; a nanosecond timestamp is a plain `long` with no Avro logical type), dates as `Integer` (days since epoch), decimals over `INT32`, `INT64` or `BYTE_ARRAY` as `ByteBuffer`, decimals over `FIXED_LEN_BYTE_ARRAY` as `GenericData.Fixed`, binary data as `ByteBuffer`, and ENUM values as `String`. A `TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)` has no Avro counterpart and is a `fixed` of its 12 stored bytes, as an `INT96` is. parquet-java's `AvroReadSupport` reads the same representations, except that it reads decimals over `INT32` and `INT64` as `Integer` and `Long`, and reads an `INT96` as a 12-byte `fixed` only when `parquet.avro.readInt96AsFixed` is set.

Avro maps always have string keys, so a Parquet map key must be a `BYTE_ARRAY` annotated as `STRING`, `ENUM`, or `JSON`. Building an `AvroRowReader` whose projection contains a map with any other key type, including an unannotated `BYTE_ARRAY` key whose bytes are not necessarily text, fails with an error. The file's other columns are unaffected: narrow the projection to exclude the map, or read it through Hardwood's `RowReader`, which serves the key in its original type.

## Avro names

Avro names that already match `[A-Za-z_][A-Za-z0-9_]*` remain unchanged. Other Parquet names are rewritten to match the Avro grammar. The original name is available from the `hardwood.parquetName` property on the converted schema or field.

Nested records use their Parquet value path to form their Avro namespace, so records with the same name in different branches do not clash. Converted names do not change when you project away a branch, or add, remove, or reorder columns whose Avro names do not collide with each other.

Two Parquet names that differ only in characters the rewrite replaces, such as `a-b` and `a.b`, both become `a_b`, so one of them takes a `_2`, `_3`, … suffix. Suffixes skip names already taken by a sibling, so they depend on the whole set of sibling names: with siblings `a-b` and `a.b` alone the Avro names are `a_b` and `a_b_2`, and adding a third column literally named `a_b_2` shifts the second to `a_b_3`. Read `hardwood.parquetName` rather than assuming a suffix when a schema carries names that collide this way.

A rewrite gives a field two names, and each side of the API takes one of them:

| Use | Name to pass |
|---|---|
| `GenericRecord.get(name)` | the Avro field name |
| `ColumnProjection.columns(...)` | the Parquet name |

Projection paths use `.` as the nesting separator. A Parquet group or field name containing `.` cannot be addressed by `ColumnProjection.columns(...)`; read the file without projection or use a schema with addressable names.

`Schema.getProp("hardwood.parquetName")` recovers the Parquet name of a rewritten record or fixed type, and `Schema.Field.getProp("hardwood.parquetName")` that of a rewritten field. Both return `null` when the emitted local name equals the Parquet name and Hardwood kept it unchanged.

Two schema shapes have no valid Avro naming, and building a reader over either fails whatever projection is applied:

- A group whose two children carry the same Parquet name. Avro records cannot hold two fields of one name.
- A file whose root is named `interval` or `float16` and that carries a column of that logical type. Those two logical types convert to Avro `fixed` types with exactly those names and no namespace, which is also the root's full name. Excluding the column from the projection does not lift the rejection; read such a file through Hardwood's `RowReader`.

A Parquet column annotated with the `NULL` logical type (e.g. PyArrow's `pa.null()` columns) maps to a bare Avro `null` field. The usual `union [null, T]` nullable wrap is illegal when `T` is itself `null`. The same collapse applies inside lists and maps: a `list<null>` element or `map<string, null>` value position becomes a bare `null` in the corresponding Avro `array` / `map` schema.

A key-only Parquet MAP, whose repeated `key_value` group has no value column, also maps to an Avro `map` with bare `null` values. Each decoded key is present in the Java map with a `null` value.

A `repeated` field outside a `LIST` or `MAP` group is a required list of required elements, and maps to a non-nullable Avro `array` of the field's own type: `repeated int32 foo` becomes `array<int>`, a repeated group becomes an `array` of its record, and a repeated group whose only child is a repeated `MAP_KEY_VALUE` group becomes an `array` of Avro `map`s. Its values are `java.util.List` instances. Such a group converts as the row reader reads it, ignoring any `LIST`, `MAP` or `MAP_KEY_VALUE` annotation of its own (see [Legacy list encodings](../reference/accessors.md#legacy-list-encodings)). Building an `AvroRowReader` whose projection includes a column of such a group annotated `VARIANT` fails with an error; a projection without its columns reads the rest of the file.

A group carrying an annotation other than `LIST`, `MAP` or `VARIANT`, such as a `LIST` element group annotated `MAP_KEY_VALUE`, maps to an Avro record of its fields.

Building an `AvroRowReader` fails with an error when a projected `LIST` group has no element field, a projected `MAP` group has no key field, or a projected `FIXED_LEN_BYTE_ARRAY` column declares no positive type length.

## Lifecycle

`AvroRowReader` does **not** take ownership of the `ParquetFileReader` it wraps. Closing the `AvroRowReader` releases the inner readers and column workers, but the underlying `ParquetFileReader` must be closed separately by the caller.

## Schema overrides

`AvroReaders` derives the Avro schema from the Parquet schema. There is no equivalent of parquet-java's `AvroReadSupport.setRequestedProjection(...)` or `setAvroReadSchema(...)`: supplying an explicit Avro reader schema (for schema-evolution promotions, renames, or alias resolution) is not supported. Column projection (`ColumnProjection.columns(...)`) is the only way to narrow what is read; the Avro schema returned by `getSchema()` always matches the projected Parquet schema's converted form.
