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

!!! example "Try it yourself"
    Want to run it or explore the capabilities yourself? [**Multi-File**](https://github.com/hardwood-hq/hardwood-examples/tree/main/multi-file) reads three months of data as one dataset, and [**Byte Buffer Source**](https://github.com/hardwood-hq/hardwood-examples/tree/main/byte-buffer-source) reads several in-memory `ByteBuffer`s as one dataset.

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

Cross-file prefetching is automatic: when pages from file N are running low, pages from file N+1 are already being prefetched.

The schema of the first file is the reference schema. Each later file a data reader reaches is
validated against it for that reader's projection and filter; inspecting a file's metadata does not
validate it. Columns are matched by field path, in any order. Every projected or filtered column
must exist with a matching physical type, logical type, repetition type, and fixed byte length, and
its enclosing groups must match in nullability and repeatedness, otherwise a
`SchemaIncompatibleException` is thrown. The check runs as the read reaches a file, so a mismatch
surfaces from the reading loop, before any row of that file is returned. Other columns are not
checked, so files may carry extra columns or omit unused ones. With no explicit projection, every
column of the first file is projected and therefore required.

By default, `Hardwood.create()` sizes the thread pool to the number of available processors. To control the decode parallelism, create a `HardwoodContext` of the desired size and pass it to `Hardwood.create(HardwoodContext)`:

```java
import dev.hardwood.HardwoodContext;

try (HardwoodContext context = HardwoodContext.create(4);  // 4 threads
     Hardwood hardwood = Hardwood.create(context);
     ParquetFileReader parquet = hardwood.openAll(files);
     RowReader reader = parquet.rowReader()) {
    // ...
}
```

The caller owns the supplied context: it is not closed when the `Hardwood` instance is closed, so the same context — and its thread pool — can be reused across later reads and shared with single-file reads via `ParquetFileReader.open(InputFile, HardwoodContext)`.
