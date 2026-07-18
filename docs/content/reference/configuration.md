<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Configuration

## Faster GZIP with libdeflate (Java 22+)

Hardwood can use [libdeflate](https://github.com/ebiggers/libdeflate) for GZIP decompression, which is significantly faster than the built-in Java implementation. This feature requires **Java 22 or newer** (it uses the Foreign Function & Memory API which became stable in Java 22).

Enabling libdeflate is two steps: a JVM flag to allow native access, plus the native library installed on the system.

**1. JVM flag** (allow Hardwood to bind native functions):

```bash
--enable-native-access=ALL-UNNAMED
```

**2. Native library** — install on your system:

=== "macOS"

    ```bash
    brew install libdeflate
    ```

=== "Linux (Debian/Ubuntu)"

    ```bash
    apt install libdeflate-dev
    ```

=== "Linux (Fedora)"

    ```bash
    dnf install libdeflate-devel
    ```

=== "Windows"

    ```bash
    vcpkg install libdeflate
    ```

Or download from [GitHub releases](https://github.com/ebiggers/libdeflate/releases).

When libdeflate is installed and available on the library path, Hardwood will automatically use it for GZIP decompression. To disable libdeflate and use the built-in Java implementation instead, set the system property:

```bash
-Dhardwood.uselibdeflate=false
```

## SIMD Acceleration with Vector API (Java 22+)

Hardwood can use the Java Vector API (SIMD) to accelerate certain decoding operations like counting non-null values, marking nulls, and dictionary lookups. This feature requires **Java 22 or newer** and is enabled automatically when available.

To enable the Vector API incubator module, add this JVM argument:

```bash
--add-modules jdk.incubator.vector
```

When SIMD is available and enabled, you'll see an INFO log message at startup:
```
SIMD support: enabled (256-bit vectors)
```

The vector width depends on your CPU (128-bit for SSE/NEON, 256-bit for AVX2, 512-bit for AVX-512).

SIMD engages only when the incubator module is added, so to run scalar operations (for debugging or comparison) simply omit the `--add-modules jdk.incubator.vector` argument.

## JFR (Java Flight Recorder) Events

Hardwood emits JFR events during file reading, enabling detailed performance profiling with zero overhead when recording is off. Start a JFR recording to capture them:

```bash
java -XX:StartFlightRecording=filename=recording.jfr,settings=profile ...
```

Or attach dynamically via `jcmd <pid> JFR.start`.

**Available events:**

| Event | Category | Description |
|-------|----------|-------------|
| `dev.hardwood.FileOpened` | I/O | File opened and metadata read. Fields: file, fileSize, rowGroupCount, columnCount |
| `dev.hardwood.FileMapping` | I/O | Memory-mapping of a file region. Fields: file, offset, size |
| `dev.hardwood.RowGroupScanned` | Decode | Page boundaries scanned in a column chunk. Fields: file, rowGroupIndex, column, pageCount, scanStrategy (`sequential` or `offset-index`) |
| `dev.hardwood.PageDecoded` | Decode | Single data page decoded. Fields: column, compressedSize, uncompressedSize |
| `dev.hardwood.RowGroupFilter` | Filter | Row groups dropped by statistics/bloom predicate pushdown. Fields: file, totalRowGroups, rowGroupsKept, rowGroupsSkipped, rowGroupsFullyMatching |
| `dev.hardwood.RowGroupByteRangeFilter` | Filter | Row groups selected by a byte-range split predicate (split-aware reading). Fields: file, totalRowGroups, rowGroupsKept, rowGroupsSkipped |
| `dev.hardwood.PageFilter` | Filter | Pages filtered by Column Index predicate pushdown. Fields: file, rowGroupIndex, column, totalPages, pagesKept, pagesSkipped |
| `dev.hardwood.RecordFilter` | Filter | Records filtered by record-level predicate evaluation. Fields: totalRecords, recordsKept, recordsSkipped |
| `dev.hardwood.BatchWait` | Pipeline | Consumer blocked waiting for the assembly pipeline. Fields: column |
| `dev.hardwood.PrefetchMiss` | Pipeline | Prefetch queue miss requiring synchronous decode. Fields: file, column, newDepth, queueEmpty |

Events appear under the **Hardwood** category in JDK Mission Control (JMC) or any JFR analysis tool. Use them to identify:

- **I/O bottlenecks** — large `FileMapping` durations or frequent `PrefetchMiss` events
- **Filter effectiveness** — `RowGroupFilter` shows how many row groups were dropped by statistics/bloom pushdown and `RowGroupByteRangeFilter` how many were excluded by split selection; `PageFilter` shows how many pages were skipped within surviving row groups; `RecordFilter` shows how many individual records were filtered out
- **Decode hotspots** — `PageDecoded` events with large uncompressed sizes or high frequency
- **Pipeline stalls** — `BatchWait` events indicate the reader is waiting for decoded data

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

Options are string-keyed and keys are matched case-sensitively; boolean option values are compared case-insensitively. An unrecognised key is ignored (so a transitional flag can be retired without breaking callers) but logged at `WARNING`, so a typo in a live key surfaces rather than silently taking the default.

| Option | Default | Description |
|--------|---------|-------------|
| `hardwood.fixed-list-fast-path` | `false` | Set to `"true"` to decode fixed-size `LIST` columns (e.g. embedding vectors, where every row holds the same number of non-null elements) without reconstructing per-row definition and repetition levels. Off by default, so every column takes the general nested-decode path unless the option is enabled. |

## System Properties Reference

| Property | Default | Description |
|----------|---------|-------------|
| `hardwood.uselibdeflate` | `true` | Set to `false` to disable libdeflate for GZIP decompression |
