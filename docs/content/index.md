<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Hardwood

_A parser for the Apache Parquet file format, optimized for minimal dependencies and great performance._

Goals of the project are:

* **Light-weight:** Implement the Parquet file format avoiding any 3rd party dependencies other than for compression algorithms (e.g. Snappy)
* **Correct:** Support all Parquet files which are supported by the canonical [parquet-java](https://github.com/apache/parquet-java) library
* **Fast:** As fast or faster as parquet-java
* **Complete:** Add a Parquet file writer (after 1.0)

Latest version: 1.0.0.Alpha1, 2026-02-26

## Quick Example

```java
import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
    RowReader rowReader = fileReader.createRowReader()) {

    while (rowReader.hasNext()) {
        rowReader.next();

        long id = rowReader.getLong("id");
        String name = rowReader.getString("name");
        LocalDate birthDate = rowReader.getDate("birth_date");
        Instant createdAt = rowReader.getTimestamp("created_at");
    }
}
```

See [Getting Started](getting-started.md) for installation and setup.
