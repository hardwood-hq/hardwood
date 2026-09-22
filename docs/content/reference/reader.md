<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Reader Reference

## Reader Options

Read-time behaviour is configured with an immutable `ReaderConfig`, passed to `ParquetFileReader.open(...)`. It is separate from `HardwoodContext`, which holds the shared runtime resources (the decode thread pool and native decompression pools): a single context can back reads with different `ReaderConfig`s.

```java
import dev.hardwood.HardwoodContext;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ReaderConfig;

ReaderConfig config = ReaderConfig.builder()
        .option("hardwood.fixed-list-fast-path", "true")
        .build();

try (HardwoodContext context = HardwoodContext.create();
     ParquetFileReader reader = ParquetFileReader.open(inputFile, context, config)) {
    // ...
}
```

Obtain the defaults with `ReaderConfig.defaults()`. The `open(inputFile)` and `open(inputFile, context)` overloads use the defaults.

Options are string-keyed and keys are matched case-sensitively; boolean option values are compared case-insensitively. An unrecognised key is ignored, so a retired option does not break callers, and is reported in the log.

| Option | Default | Description |
|--------|---------|-------------|
| `hardwood.fixed-list-fast-path` | `false` | Set to `"true"` to decode fixed-size `LIST` columns (e.g. embedding vectors, where every row holds the same number of non-null elements) without reconstructing per-row definition and repetition levels. |
| `hardwood.metadata-filtering` | `true` | Set to `"false"` to disable metadata-based filtering: no row groups or pages are skipped from min/max statistics, bloom filters, dictionary pages, or page indexes, and filter predicates are instead evaluated against every decoded row. Filtered results then depend only on the data pages, for files whose footer or page-index metadata is unreliable. |

## Limits

Each column chunk must be at most 2 GB of *compressed* data. The limit is per chunk, not per file: local memory-mapped files and S3-backed files may be arbitrarily large overall. The in-memory (`ByteBuffer`) backend additionally caps the *whole file* at 2 GB, and so does an S3 file opened with [`RangeBacking.SPARSE_TEMPFILE`](s3.md#range-backing), which the `dive` TUI uses for every file. For datasets that don't fit a single supported file, split the data into multiple files at write time and read them as one; see [Read Multiple Files as One Dataset](../how-to/multi-file.md).
