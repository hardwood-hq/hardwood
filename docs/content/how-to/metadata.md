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
