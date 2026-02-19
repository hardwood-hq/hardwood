/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

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
                while (rowReader.hasNext() && rowNum < 5) {
                    rowReader.next();
                    rowNum++;
                    StringBuilder line = new StringBuilder("| ");
                    for (int i = 0; i < colCount; i++) {
                        String value = formatValue(rowReader, i, schema);
                        // Adjust width if value is longer
                        if (value.length() > widths[i]) {
                            value = value.substring(0, widths[i] - 2) + "..";
                        }
                        line.append(padRight(value, widths[i])).append(" | ");
                    }
                    System.out.println(line);
                }
            }
            System.out.println(separator);
        }
    }

    private static String formatValue(RowReader rowReader, int col, FileSchema schema) {
        ColumnSchema colSchema = schema.getColumn(col);
        String fieldName = colSchema.name();
        if (rowReader.isNull(fieldName)) {
            return "null";
        }

        // Check logical type first for timestamps
        LogicalType logicalType = colSchema.logicalType();
        if (logicalType instanceof LogicalType.TimestampType) {
            Instant instant = rowReader.getTimestamp(fieldName);
            return LocalDateTime.ofInstant(instant, ZoneOffset.UTC)
                    .toString().replace("T", " ");
        }

        // Fall back to physical type
        return switch (colSchema.type()) {
            case INT32 -> String.valueOf(rowReader.getInt(fieldName));
            case INT64 -> String.valueOf(rowReader.getLong(fieldName));
            case FLOAT -> String.format("%.2f", rowReader.getFloat(fieldName));
            case DOUBLE -> String.format("%.2f", rowReader.getDouble(fieldName));
            case BOOLEAN -> String.valueOf(rowReader.getBoolean(fieldName));
            case BYTE_ARRAY -> rowReader.getString(fieldName);
            default -> String.valueOf(rowReader.getValue(fieldName));
        };
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
