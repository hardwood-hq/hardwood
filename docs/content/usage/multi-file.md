<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Reading Multiple Files

When processing multiple Parquet files, use the `Hardwood` class to share a thread pool across readers.
`Hardwood.openAll(List<InputFile>)` returns a `ParquetFileReader` over many files. The same `RowReader`, `ColumnReader`, and `ColumnReaders` APIs apply.

```java
import dev.hardwood.Hardwood;
import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

List<InputFile> files = InputFile.ofPaths(
    Path.of("data_2024_01.parquet"),
    Path.of("data_2024_02.parquet"),
    Path.of("data_2024_03.parquet")
);

try (Hardwood hardwood = Hardwood.create();
     ParquetFileReader parquet = hardwood.openAll(files);
     RowReader reader = parquet.rowReader()) {

    while (reader.hasNext()) {
        reader.next();
        // Access data using the same API as a single-file RowReader
        long id = reader.getLong("id");
        String name = reader.getString("name");
    }
}
```

Cross-file prefetching is automatic: when pages from file N are running low, pages from file N+1 are already being prefetched. This eliminates I/O stalls at file boundaries.

The schema of the first file is the reference schema. Each subsequent file is validated against it as it is opened: every projected column must exist with a matching physical type, logical type, and repetition type, otherwise a `SchemaIncompatibleException` is thrown. Non-projected columns are not checked, so files may carry additional columns. With no explicit projection, all columns of the first file are projected and therefore required in every subsequent file.

By default, `Hardwood.create()` sizes the thread pool to the number of available processors. For custom thread pool sizing, use `HardwoodContext` directly:

```java
import dev.hardwood.HardwoodContext;

try (HardwoodContext context = HardwoodContext.create(4);  // 4 threads
     ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path), context);
     RowReader rowReader = reader.rowReader()) {
    // ...
}
```

