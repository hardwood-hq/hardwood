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

The metadata follows the [file layout](../concepts/parquet-layout.md#the-hierarchy). `FileMetaData` holds the row count, schema, key-value metadata (e.g. Spark schema, pandas metadata), the writer that produced the file (`createdBy`), and the per-column statistics ordering (`columnOrders`). Each `RowGroup` holds one `ColumnChunk` per column, with its compression codec, byte sizes, optional statistics (min/max values, null count), and the byte ranges of its column index, offset index and bloom filter when the file has them.

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

!!! warning "Experimental API"
    `MetadataSource`, `ParsedFooter`, `StaleMetadataException`, `HardwoodContext.builder()` and `InputFile.identity()` are annotated `@Experimental`: their shape may change in a future release.

A reader opened against a context with a `MetadataSource` takes the footer of each file it reads from the source instead of reading and parsing it from the file. This saves the footer parse on every open of a file whose footer the source holds, and on S3 the extra request a footer outside the object's initial tail read costs (see the [S3 reference](../reference/s3.md)): install one in a process that opens the same files repeatedly, such as a lookup service opening a reader per request, an engine opening a file once per split, or a service reading the same dataset with `openAll` on every refresh.

The source returns a `ParsedFooter`, and reads one with `ParsedFooter.readFrom(file)` for a file it holds none for:

```java
import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.MetadataSource;
import dev.hardwood.metadata.ParsedFooter;
import dev.hardwood.reader.ParquetFileReader;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

record FooterKey(String name, String identity) {}

Map<FooterKey, ParsedFooter> cache = new ConcurrentHashMap<>();

MetadataSource source = file -> {
    Optional<String> identity = file.identity();
    if (identity.isEmpty()) {
        return ParsedFooter.readFrom(file);
    }
    FooterKey key = new FooterKey(file.name(), identity.get());
    ParsedFooter footer = cache.get(key);
    if (footer == null) {
        footer = ParsedFooter.readFrom(file);
        cache.put(key, footer);
    }
    return footer;
};

try (HardwoodContext context = HardwoodContext.builder()
        .metadataSource(source)
        .build()) {
    try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path), context)) {
        // the first open of a file reads its footer; later opens take it from the cache
    }
}
```

The source serves every file of `openAll` as well, each when the reader first needs its footer.

Key the cache on the file's name and its identity. `InputFile.identity()` differs for different content, so a rewritten or replaced file is a cache miss: a local file's identity is its size, modification time and file key (its absolute path on a file system without file keys), and an S3 object's is its `ETag`. Together with the name, which for an S3 object is its full URI, the key tells apart both different files and different versions of one file. A file without an identity, such as an in-memory file, is read on every open.

The reader calls the source from several threads at once, one per file whose footer it needs, so the cache must be thread-safe. Each call holds up the read that made it; keep footers stored remotely behind an in-process cache.

The reader checks each supplied footer against the file before reading any of its data, and fails with `StaleMetadataException` when the footer does not describe the file (see [Error Handling](../reference/error-handling.md#the-read-failed)). Evict the footer the source served for that file and open a new reader:

```java
import dev.hardwood.reader.StaleMetadataException;

try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path), context)) {
    // read
}
catch (StaleMetadataException e) {
    e.sourceIdentity().ifPresent(id -> cache.remove(new FooterKey(e.fileName(), id)));
    // open a new reader for path
}
```

`ParsedFooter.metaData()` returns what `getFileMetaData()` returns on a reader that read the footer itself. A `ParsedFooter` holds no file resources, and one instance serves any number of readers on any threads.

## Metadata for multiple files

For a multi-file reader, use `getFileCount()` and `getFileMetaData(int)` to inspect each
physical input file in order. Each footer is read once, when first needed, and reused by
metadata, row and column access for the lifetime of the parent reader.

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

`getFileMetaData(int)` reports the physical file's footer without validating it against the first
file; cross-file schema validation happens when a row or column reader is planned. Leave every input
file unchanged until the parent reader is closed. A failed footer load is cached too: close and
reopen the parent reader to retry it or to inspect a changed file.

## Size statistics and level histograms

`ColumnMetaData.sizeStatistics()` reports how much data a column chunk holds, without reading any of it:

- `unencodedByteArrayDataBytes()` — the size the chunk's `BYTE_ARRAY` values would occupy unencoded and uncompressed
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
