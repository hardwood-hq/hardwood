<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Configuration

## Faster GZIP with libdeflate (Java 22+)

Hardwood can use [libdeflate](https://github.com/ebiggers/libdeflate) for GZIP decompression instead of the built-in Java implementation. This feature requires **Java 22 or newer** (it uses the Foreign Function & Memory API).

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

Hardwood can use the Java Vector API (SIMD) to accelerate certain decoding operations like counting non-null values, marking nulls, and dictionary lookups. This feature requires **Java 22 or newer** and the Vector API incubator module, added with this JVM argument:

```bash
--add-modules jdk.incubator.vector
```

The vector width depends on your CPU (128-bit for SSE/NEON, 256-bit for AVX2, 512-bit for AVX-512).

To run scalar operations (for debugging or comparison), omit the argument.

## JFR (Java Flight Recorder) Events

Hardwood emits JFR events during file reading. Start a JFR recording to capture them:

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
| `dev.hardwood.RowGroupDictionaryFilter` | Filter | Row group dropped by dictionary predicate pushdown when the read reaches it, one event per row group dropped. `RowGroupFilter` counts that row group as kept. Fields: file, rowGroupIndex |
| `dev.hardwood.RowGroupByteRangeFilter` | Filter | Row groups selected by a byte-range split predicate (split-aware reading). Fields: file, totalRowGroups, rowGroupsKept, rowGroupsSkipped |
| `dev.hardwood.PageFilter` | Filter | Pages filtered by Column Index predicate pushdown within a kept row group, one event per column chunk the Column Index narrowed; a chunk without a Column Index has its pages dropped from page-header statistics, which no event reports. Fields: file, rowGroupIndex, column, totalPages, pagesKept, pagesSkipped |
| `dev.hardwood.RecordFilter` | Filter | Records filtered by record-level predicate evaluation, one event per file read. Fields: file, totalRecords, recordsKept, recordsSkipped |
| `dev.hardwood.BatchWait` | Pipeline | Consumer blocked waiting for the assembly pipeline; the event's duration is the stall. Fields: column |

Events appear under the **Hardwood** category in JDK Mission Control (JMC) or any JFR analysis tool. Use them to identify:

- **I/O bottlenecks** — large `FileMapping` durations
- **Filter effectiveness**: `RowGroupFilter`, `RowGroupDictionaryFilter`, `RowGroupByteRangeFilter`, `PageFilter` and `RecordFilter` counts
- **Decode hotspots** — `PageDecoded` events with large uncompressed sizes or high frequency
- **Pipeline stalls** — `BatchWait` events indicate the reader is waiting for decoded data

## Reader and Writer Options

Read-time behaviour is configured with a `ReaderConfig` passed to `ParquetFileReader.open(...)`; see the [Reader Reference](reader.md#reader-options). Write-time behaviour is configured with a `WriterConfig` passed to `ParquetFileWriter.create(...)`; see the [Writer Reference](writer.md#writer-options).

## System Properties Reference

| Property | Default | Description |
|----------|---------|-------------|
| `hardwood.uselibdeflate` | `true` | Set to `false` to disable libdeflate for GZIP decompression |
