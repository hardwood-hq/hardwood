<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Avro Support

The `hardwood-avro` module reads Parquet files into Avro `GenericRecord` instances, the most common record representation for Parquet data in the JVM ecosystem. Add it alongside `hardwood-core`:

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
     AvroRowReader reader = AvroReaders.createRowReader(fileReader)) {

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
AvroRowReader reader = AvroReaders.createRowReader(fileReader,
    FilterPredicate.gt("id", 1000L));

// With projection
AvroRowReader reader = AvroReaders.createRowReader(fileReader,
    ColumnProjection.columns("id", "name"));

// With both
AvroRowReader reader = AvroReaders.createRowReader(fileReader,
    ColumnProjection.columns("id", "name"),
    FilterPredicate.gt("id", 1000L));
```

Values are stored in Avro's standard representations: timestamps as `Long` (millis/micros since epoch), dates as `Integer` (days since epoch), decimals as `ByteBuffer`, binary data as `ByteBuffer`. This matches the behavior of parquet-java's `AvroReadSupport`.
