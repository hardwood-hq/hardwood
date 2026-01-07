/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood;

import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import dev.morling.hardwood.metadata.LogicalType;
import dev.morling.hardwood.reader.ParquetFileReader;
import dev.morling.hardwood.reader.RowReader;
import dev.morling.hardwood.row.PqRow;
import dev.morling.hardwood.schema.ColumnSchema;
import dev.morling.hardwood.schema.FileSchema;

public class DebugParquetTest {

    public static void main(String[] args) throws Exception {
        String file = "src/test/resources/yellow_tripdata_2025-01.parquet";

        System.out.println("=== " + file + " ===");
        try (ParquetFileReader reader = ParquetFileReader.open(Paths.get(file))) {
            System.out.println("Version: " + reader.getFileMetaData().version());
            System.out.println("Num rows: " + reader.getFileMetaData().numRows());
            System.out.println("Row groups: " + reader.getFileMetaData().rowGroups().size());
            System.out.println();

            FileSchema schema = reader.getFileSchema();
            int colCount = schema.getColumnCount();

            // Calculate column widths (accounting for type names)
            int[] widths = new int[colCount];
            for (int i = 0; i < colCount; i++) {
                ColumnSchema col = schema.getColumn(i);
                int nameLen = col.name().length();
                int physicalLen = col.type().name().length();
                int logicalLen = formatLogicalType(col.logicalType()).length();
                widths[i] = Math.max(Math.max(Math.max(nameLen, physicalLen), logicalLen), 8);
            }

            // Print header with field names, physical types, and logical types
            StringBuilder header = new StringBuilder("| ");
            StringBuilder physicalRow = new StringBuilder("| ");
            StringBuilder logicalRow = new StringBuilder("| ");
            StringBuilder separator = new StringBuilder("+-");
            for (int i = 0; i < colCount; i++) {
                ColumnSchema col = schema.getColumn(i);
                header.append(padRight(col.name(), widths[i])).append(" | ");
                physicalRow.append(padRight(col.type().name(), widths[i])).append(" | ");
                logicalRow.append(padRight(formatLogicalType(col.logicalType()), widths[i])).append(" | ");
                separator.append("-".repeat(widths[i])).append("-+-");
            }
            System.out.println(separator);
            System.out.println(header);
            System.out.println(physicalRow);
            System.out.println(logicalRow);
            System.out.println(separator);

            // Print rows
            try (RowReader rowReader = reader.createRowReader()) {
                int rowNum = 0;
                for (PqRow row : rowReader) {
                    StringBuilder line = new StringBuilder("| ");
                    for (int i = 0; i < colCount; i++) {
                        String value = formatValue(row, i, schema.getColumn(i));
                        // Adjust width if value is longer
                        if (value.length() > widths[i]) {
                            value = value.substring(0, widths[i] - 2) + "..";
                        }
                        line.append(padRight(value, widths[i])).append(" | ");
                    }
                    System.out.println(line);
                    if (++rowNum >= 5) {
                        break;
                    }
                }
            }
            System.out.println(separator);
        }
    }

    private static String formatValue(PqRow row, int col, ColumnSchema schema) {
        if (row.isNull(col)) {
            return "null";
        }

        Object value = getValue(row, col, schema);

        if (value instanceof Instant instant) {
            // Format timestamp as local datetime for readability
            return LocalDateTime.ofInstant(instant, ZoneOffset.UTC)
                    .toString().replace("T", " ");
        }
        if (value instanceof Double d) {
            return String.format("%.2f", d);
        }
        return String.valueOf(value);
    }

    private static Object getValue(PqRow row, int col, ColumnSchema schema) {
        return row.getValue(schema.toPqType(), col);
    }

    private static String formatLogicalType(LogicalType logicalType) {
        if (logicalType == null) {
            return "-";
        }
        // Extract the simple type name from the record class
        String name = logicalType.getClass().getSimpleName();
        // Remove "Type" suffix if present (e.g., "TimestampType" -> "TIMESTAMP")
        if (name.endsWith("Type")) {
            name = name.substring(0, name.length() - 4);
        }
        // Add parameters for parameterized types
        if (logicalType instanceof LogicalType.TimestampType ts) {
            return name.toUpperCase() + "(" + ts.unit() + ")";
        }
        if (logicalType instanceof LogicalType.TimeType t) {
            return name.toUpperCase() + "(" + t.unit() + ")";
        }
        if (logicalType instanceof LogicalType.DecimalType d) {
            return name.toUpperCase() + "(" + d.precision() + "," + d.scale() + ")";
        }
        if (logicalType instanceof LogicalType.IntType i) {
            return (i.isSigned() ? "INT" : "UINT") + "_" + i.bitWidth();
        }
        return name.toUpperCase();
    }

    private static String padRight(String s, int width) {
        if (s.length() >= width) {
            return s;
        }
        return s + " ".repeat(width - s.length());
    }
}
