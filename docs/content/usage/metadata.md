<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Accessing File Metadata

Inspecting metadata before reading is useful for understanding file structure, choosing which columns to project, validating files in a pipeline, or building tooling. Hardwood exposes the full Parquet metadata hierarchy without reading any row data.

A Parquet file is organized as follows:

- **FileMetaData** — top-level: row count, schema, key-value metadata (e.g. Spark schema, pandas metadata), and the writer that produced the file (`createdBy`)
- **RowGroup** — a horizontal partition of the data; each row group contains all columns for a subset of rows
- **ColumnChunk** — one column within a row group; holds compression codec, byte sizes, and optional statistics (min/max values, null count) used for predicate pushdown

```java
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

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

