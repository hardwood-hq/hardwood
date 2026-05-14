<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Avro Support

If your application already works with Avro records — for instance in a Kafka or Spark pipeline — you can read Parquet files directly into `GenericRecord` instances instead of using Hardwood's own row API. The `hardwood-avro` module handles the schema conversion and record materialization, matching the behavior of parquet-java's `AvroReadSupport`. Add it alongside `hardwood-core`:

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

Values are stored in Avro's standard representations: timestamps as `Long` (millis/micros since epoch), dates as `Integer` (days since epoch), decimals as `ByteBuffer`, binary data as `ByteBuffer`. This matches the behavior of parquet-java's `AvroReadSupport`.

A Parquet column annotated with the `NULL` logical type (e.g. PyArrow's `pa.null()` columns) maps to a bare Avro `null` field — not the usual `union [null, T]` nullable wrap, which is illegal when `T` is itself `null`. The same collapse applies inside lists and maps: a `list<null>` element or `map<string, null>` value position becomes a bare `null` in the corresponding Avro `array` / `map` schema.
