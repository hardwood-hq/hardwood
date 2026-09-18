<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Reader Reference

The `ReaderConfig` options, their defaults, and how an unrecognised key is handled. The accessor-to-type correspondence is in [Typed Accessors](accessors.md), predicates and projection in [Query Controls](query-controls.md), and the exceptions a read can throw in [Error Handling](error-handling.md). Runtime acceleration and JFR events are in [Configuration](configuration.md).

## Reader Options

Read-time behaviour is configured with an immutable `ReaderConfig`, passed to `ParquetFileReader.open(...)`. It is separate from `HardwoodContext`, which holds the shared runtime resources (the decode thread pool and native decompression pools): a single context can back reads with different `ReaderConfig`s, so a behaviour knob never forces a fresh thread pool.

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
| `hardwood.fixed-list-fast-path` | `false` | Set to `"true"` to decode fixed-size `LIST` columns (e.g. embedding vectors, where every row holds the same number of non-null elements) without reconstructing per-row definition and repetition levels. Off by default, so every column takes the general nested-decode path unless the option is enabled. |
| `hardwood.metadata-filtering` | `true` | Set to `"false"` to disable metadata-based filtering: no row groups or pages are skipped from min/max statistics, bloom filters, dictionary pages, or page indexes, and filter predicates are instead evaluated against every decoded row. Filtered results then depend only on the data pages, for files whose footer or page-index metadata is unreliable. |

