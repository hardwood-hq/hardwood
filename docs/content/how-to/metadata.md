<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Accessing File Metadata

Hardwood exposes the full Parquet metadata hierarchy of a file without reading any row data.

!!! example "Try it yourself"
    Want to run it or explore the capabilities yourself? The [**Metadata Explorer**](https://github.com/hardwood-hq/hardwood-examples/tree/main/metadata-explorer) example describes a Parquet file from its footer alone: version, schema, and per-row-group column statistics.

A Parquet file is organized as follows:

- **FileMetaData** — top-level: row count, schema, key-value metadata (e.g. Spark schema, pandas metadata), the writer that produced the file (`createdBy`), and the per-column statistics ordering (`columnOrders`)
- **RowGroup** — a horizontal partition of the data; each row group contains all columns for a subset of rows
- **ColumnChunk** — one column within a row group; holds compression codec, byte sizes, and optional statistics (min/max values, null count) used for predicate pushdown. Per-chunk byte ranges for the column index and offset index (when present in the file) are exposed via `columnIndexOffset`/`columnIndexLength` and `offsetIndexOffset`/`offsetIndexLength` on `ColumnChunk`. The bloom-filter byte range (`bloomFilterOffset`/`bloomFilterLength`) is exposed on `ColumnMetaData`, matching its position in the Parquet Thrift schema. `ColumnMetaData.encodingStats()` returns the chunk's page counts per (page type, encoding) pair as a list of `PageEncodingStats`, empty when the file omits the field. A page type this version does not recognize is reported as `PageType.UNKNOWN`. An encoding this version does not recognize is reported as `Encoding.UNKNOWN`, in both `ColumnMetaData.encodings()` and `encodingStats()`; the metadata reads normally, and reading a page that uses such an encoding throws an `UnsupportedOperationException` naming the raw Thrift encoding value. `ColumnChunk.filePath()` is the file holding the chunk's data under the legacy split-file layout, and the empty string (never `null`) when the data sits in the file being read; `requireSameFile()` throws an `IOException` for the former, which is what reading such a chunk does.

```java
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.ColumnOrder;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.SizeStatistics;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import java.util.List;
import java.util.Map;

try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) {
    FileMetaData metadata = reader.getFileMetaData();

    System.out.println("Version: " + metadata.version());
    System.out.println("Total rows: " + metadata.numRows());
    System.out.println("Created by: " + metadata.createdBy());

    // Access application-defined key-value metadata (e.g. Spark schema, pandas metadata, Avro schema)
    Map<String, String> kvMetadata = metadata.keyValueMetadata();
    for (Map.Entry<String, String> entry : kvMetadata.entrySet()) {
        System.out.println("  " + entry.getKey() + " = " + entry.getValue());
    }

    // The statistics ordering for each leaf column, in schema order. Empty when the file omits
    // column_orders, in which case the type-defined ordering applies to every column. The value is
    // one of ColumnOrder.TYPE_DEFINED_ORDER, IEEE754_TOTAL_ORDER, or UNKNOWN.
    List<ColumnOrder> columnOrders = metadata.columnOrders();

    // Schema inspection
    FileSchema schema = reader.getFileSchema();
    for (int i = 0; i < schema.getColumnCount(); i++) {
        ColumnSchema column = schema.getColumn(i);
        System.out.println("Column " + i + ": " + column.name()
            + " (" + column.type() + ", " + column.repetitionType()
            + (column.logicalType() != null ? ", " + column.logicalType() : "")
            + ")");
    }

    // Row group and column chunk details
    for (int rg = 0; rg < metadata.rowGroups().size(); rg++) {
        RowGroup rowGroup = metadata.rowGroups().get(rg);
        System.out.println("Row group " + rg + ": "
            + rowGroup.numRows() + " rows, "
            + rowGroup.totalByteSize() + " bytes");

        for (ColumnChunk chunk : rowGroup.columns()) {
            ColumnMetaData col = chunk.metaData();
            System.out.println("  " + col.pathInSchema()
                + " [" + col.codec() + "]"
                + " compressed=" + col.totalCompressedSize()
                + " uncompressed=" + col.totalUncompressedSize());

            // Column statistics (if available)
            Statistics stats = col.statistics();
            if (stats != null && stats.nullCount() != null) {
                System.out.println("    nulls: " + stats.nullCount());
            }
        }
    }
}
```

The map `keyValueMetadata()` returns can be handed to `ParquetFileWriter.keyValueMetadata(Map)` to stamp the same entries on a file being written; see [File Metadata](../reference/writer.md#file-metadata).

## Reuse a parsed footer across readers

Opening a reader reads and parses the file's footer. For a file that does not change, install a
[`MetadataSource`][dev.hardwood.reader.MetadataSource] on a `HardwoodContext` so that every reader opened
against it receives the cached footer rather than re-reading and re-parsing it from disk:

```java
// A minimal cache backed by a ConcurrentHashMap. Real caches should also handle
// concurrent first-population (computeIfAbsent does that here) and expiry.
Map<String, ParsedFooter> cache = new ConcurrentHashMap<>();

// Wrap checked IOException in an unchecked exception for the lambda body.
MetadataSource source = file -> {
    try {
        return cache.computeIfAbsent(
                file.name(), k -> {
                    try {
                        return ParsedFooter.readFrom(file);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
    } catch (UncheckedIOException e) {
        throw e.getCause();
    }
};

try (HardwoodContext ctx = HardwoodContext.builder()
        .metadataSource(source)
        .build()) {
    // Footer is read once; every subsequent open against ctx uses the cache.
    try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path), ctx)) {
        // reads as usual
    }
}
```

`ParsedFooter.readFrom(InputFile)` reads and parses the footer once; the resulting `ParsedFooter` is a
value object that can be shared safely across any number of concurrent readers. The source is called on
every `open` and `openAll` against the context — hardwood does not cache the result itself.

!!! warning "The metadata must describe exactly the bytes being read"

    Chunk offsets and page locations come from the footer, so a cached footer from a different version
    of the file reads unrelated bytes rather than failing. If the cache key is a path alone, a replaced
    object-store file will silently corrupt reads. Supply a `sourceIdentity` in the `ParsedFooter` (e.g.
    an ETag, generation number, or `length:mtime` pair) so hardwood can detect the mismatch:

    ```java
    return cache.computeIfAbsent(file.name(), k -> {
        try { return ParsedFooter.readFrom(file); }
        catch (IOException e) { throw new UncheckedIOException(e); }
    });
    ```

    `ParsedFooter.readFrom` populates `sourceIdentity` from `InputFile.identity()` automatically.
    `MappedInputFile` returns `path:size:mtime` by default. When a staleness is detected, hardwood
    throws `StaleMetadataException` before reading any data; call `sourceIdentity()` on the exception
    to find the cache entry to invalidate, then retry.

    On a multi-file reader, `getFileMetaData()` returns only the *first* file's footer. Reusing it for any
    other file of that reader is a misread, not an error.

## Metadata for multiple files

For a multi-file reader, use `getFileCount()` and `getFileMetaData(int)` to inspect each
physical input file in order. The first file's footer is read when the reader is opened.
Later footers are read when indexed metadata access or data-reader prefetch first needs them.
In-progress, successful, and failed loads are cached for the lifetime of the parent reader, so
metadata, row, and column access all reuse the same parsed footer.

```java
try (Hardwood hardwood = Hardwood.create();
     ParquetFileReader reader = hardwood.openAll(files)) {
    long totalRows = 0;
    for (int fileIndex = 0; fileIndex < reader.getFileCount(); fileIndex++) {
        totalRows = Math.addExact(
            totalRows,
            reader.getFileMetaData(fileIndex).numRows());
    }
}
```

Indexed metadata access reports the physical file's footer; it does not validate that file against
the first file for a particular projection or filter. Cross-file schema validation happens when a
row or column reader is planned. Keep every input file unchanged until the parent reader is closed.
Close and reopen the parent reader to retry a failed footer load or inspect a changed file.

## Size statistics and level histograms

`ColumnMetaData.sizeStatistics()` reports how much data a column chunk holds, without reading any of it:

- `unencodedByteArrayDataBytes()` — the size the chunk's `BYTE_ARRAY` values would occupy unencoded and uncompressed, which the on-disk sizes do not tell you
- `definitionLevelHistogram()` — how many values sit at each definition level, `0` through the column's maximum. The entry at the maximum counts the non-null values; each lower entry counts the values that stop being present at that level: a null, or, on a repeated column, an empty list
- `repetitionLevelHistogram()` — how many values sit at each repetition level, `0` through the column's maximum. Entry `0` counts the values that start a new row, so it is the number of rows in the chunk; each higher entry counts the values that continue a repeated field at that level

Every field is optional. A writer that omits one reports `null`, which is distinct from a value the writer recorded as empty or zero:

```java
SizeStatistics sizeStats = col.sizeStatistics();
if (sizeStats != null && sizeStats.definitionLevelHistogram() != null) {
    long[] histogram = sizeStats.definitionLevelHistogram();
    long nonNull = histogram[histogram.length - 1];
    System.out.println("    non-null values: " + nonNull);
}
```

`Statistics.nanCount()` reports how many NaN values a `FLOAT`, `DOUBLE` or `FLOAT16` chunk holds. NaN sits outside the ordering of `minValue()`/`maxValue()`, so those bounds say nothing about it. A `null` count means the writer recorded none; only a recorded `0` establishes that a chunk holds no NaN.
