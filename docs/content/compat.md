<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Parquet-Java Compatibility

The `hardwood-parquet-java-compat` module provides a drop-in replacement for parquet-java's `ParquetReader<Group>` API. This allows users migrating from parquet-java to use Hardwood with minimal code changes.

**Features:**

- Provides `org.apache.parquet.*` namespace classes compatible with parquet-java
- Includes Hadoop shims (`Path`, `Configuration`) that wrap Java NIO — no Hadoop dependency required
- Supports the familiar builder pattern and Group-based record reading

## Usage

```java
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.GroupReadSupport;
import org.apache.parquet.hadoop.ParquetReader;

Path path = new Path("data.parquet");

try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path).build()) {
    Group record;
    while ((record = reader.read()) != null) {
        // Read primitive fields
        long id = record.getLong("id", 0);
        String name = record.getString("name", 0);
        int age = record.getInteger("age", 0);

        // Read nested groups (structs)
        Group address = record.getGroup("address", 0);
        String city = address.getString("city", 0);
        int zip = address.getInteger("zip", 0);

        // Check for null/optional fields
        int count = record.getFieldRepetitionCount("optional_field");
        if (count > 0) {
            String value = record.getString("optional_field", 0);
        }
    }
}
```

!!! warning
    This module provides its own interface copies in the `org.apache.parquet.*` namespace. It cannot be used alongside parquet-java on the same classpath.
