<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Avro Support

The `hardwood-avro` module reads Parquet files into Avro `GenericRecord` instances, converting the schema and materializing records as parquet-java's `AvroReadSupport` does. Add it alongside `hardwood-core`:

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

`AvroReaders.buildRowReader` takes column projection and predicate pushdown, alone or combined:

```java
AvroRowReader reader = AvroReaders.buildRowReader(fileReader)
    .projection(ColumnProjection.columns("id", "name"))
    .filter(FilterPredicate.gt("id", 1000L))
    .build();
```

Values are stored in Avro's standard representations: timestamps as `Long` (millis/micros since epoch), dates as `Integer` (days since epoch), decimals as `ByteBuffer`, binary data as `ByteBuffer`, and ENUM values as `String`. A `TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)` has no Avro counterpart and is a `fixed` of its 12 stored bytes, as an `INT96` is.

Avro maps always have string keys, so a Parquet map key must be a `BYTE_ARRAY` annotated as `STRING`, `ENUM`, or `JSON`. Building an `AvroRowReader` whose projection contains a map with any other key type, including an unannotated `BYTE_ARRAY` key whose bytes are not necessarily text, fails with an error naming the map's path and its key type. The file's other columns are unaffected: narrow the projection to exclude the map, or read it through Hardwood's `RowReader`, which serves the key in its original type.

## Avro names

Parquet names that match `[A-Za-z_][A-Za-z0-9_]*` are used as they are. Other names are rewritten: each character outside `[A-Za-z0-9_]` becomes `_`, and a leading `_` is prepended to a name starting with a digit. `Schema.getProp("hardwood.parquetName")` returns the Parquet name of a rewritten record or fixed type, and `Schema.Field.getProp("hardwood.parquetName")` that of a rewritten field; both return `null` for a name used as it is.

Nested records use their Parquet value path to form their Avro namespace, so records with the same name in different branches do not clash. Converted names do not change when you project away a branch, or add, remove, or reorder columns whose Avro names do not collide with each other.

Two Parquet names that differ only in characters the rewrite replaces, such as `a-b` and `a.b`, both become `a_b`, so one of them takes a `_2`, `_3`, … suffix. Suffixes skip names a sibling already holds, so they depend on the whole set of sibling names; read `hardwood.parquetName` rather than assuming a suffix.

A rewrite gives a field two names, and each side of the API takes one of them:

| Use | Name to pass |
|---|---|
| `GenericRecord.get(name)` | the Avro field name |
| `ColumnProjection.columns(...)` | the Parquet name |

Projection paths use `.` as the nesting separator. A Parquet group or field name containing `.` cannot be addressed by `ColumnProjection.columns(...)`; read the file without projection or use a schema with addressable names.

Two schema shapes have no valid Avro naming, and building a reader over either fails whatever projection is applied:

- A group whose two children carry the same Parquet name. Avro records cannot hold two fields of one name, so the error names the duplicate and the value path it sits at.
- A file whose root is named `interval` or `float16` and that carries a column of that logical type. Those two logical types convert to Avro `fixed` types with exactly those names and no namespace, which is also the root's full name. Excluding the column from the projection does not lift the rejection; read such a file through Hardwood's `RowReader`.

A Parquet column annotated with the `NULL` logical type (e.g. PyArrow's `pa.null()` columns) maps to a bare Avro `null` field. The same collapse applies inside lists and maps: a `list<null>` element or `map<string, null>` value position becomes a bare `null` in the corresponding Avro `array` / `map` schema.

A key-only Parquet MAP, whose repeated `key_value` group has no value column, also maps to an Avro `map` with bare `null` values. Each decoded key is present in the Java map with a `null` value.

Building an `AvroRowReader` fails with an `IllegalArgumentException` naming the group's path when a projected `LIST` group has no element field, or a projected `MAP` group has no key field.

## Lifecycle

`AvroRowReader` does **not** take ownership of the `ParquetFileReader` it wraps. Closing the `AvroRowReader` releases the inner readers and column workers, but the underlying `ParquetFileReader` must be closed separately by the caller.

## Schema overrides

`AvroSchemaConverter` derives the Avro schema from the Parquet schema, and `getSchema()` returns the converted form of the projected schema. An explicit Avro reader schema, as set with parquet-java's `AvroReadSupport.setRequestedProjection(...)` or `setAvroReadSchema(...)` for schema-evolution promotions, renames or alias resolution, is not supported; `ColumnProjection.columns(...)` is the only way to narrow what is read.
