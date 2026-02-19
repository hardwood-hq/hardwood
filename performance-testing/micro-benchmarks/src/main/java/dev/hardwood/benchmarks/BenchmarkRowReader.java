/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;

import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.internal.reader.FlatColumnData;
import dev.hardwood.internal.reader.TypedColumnData;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/**
 * A minimal row reader for benchmarking purposes.
 * <p>
 * Unlike FlatRowReader, this class operates in "manual mode" where column data
 * is explicitly loaded via {@link #loadBatch(TypedColumnData[])} rather than
 * read from a file. This allows benchmarking the row access layer in isolation
 * without file I/O overhead.
 * </p>
 */
final class BenchmarkRowReader {

    private final FileSchema schema;

    private FlatColumnData[] columnData;
    private BitSet[] nulls;
    private int recordCount = 0;

    private int rowIndex = -1;

    BenchmarkRowReader(FileSchema schema) {
        this.schema = schema;
    }

    /**
     * Load a batch of column data for row-oriented access.
     */
    void loadBatch(TypedColumnData[] newColumnData) {
        this.columnData = new FlatColumnData[newColumnData.length];
        this.nulls = new BitSet[newColumnData.length];
        this.rowIndex = -1;
        this.recordCount = newColumnData.length > 0 ? newColumnData[0].recordCount() : 0;

        for (int i = 0; i < newColumnData.length; i++) {
            FlatColumnData flat = (FlatColumnData) newColumnData[i];
            this.columnData[i] = flat;
            this.nulls[i] = flat.nulls();
        }
    }

    boolean hasNext() {
        return rowIndex + 1 < recordCount;
    }

    void next() {
        rowIndex++;
    }

    // ==================== Primitive Type Accessors ====================

    int getInt(int columnIndex) {
        TypedColumnData data = columnData[columnIndex];
        if (isNull(columnIndex)) {
            throw new NullPointerException("Column " + columnIndex + " is null");
        }
        return ((FlatColumnData.IntColumn) data).get(rowIndex);
    }

    long getLong(int columnIndex) {
        TypedColumnData data = columnData[columnIndex];
        if (isNull(columnIndex)) {
            throw new NullPointerException("Column " + columnIndex + " is null");
        }
        return ((FlatColumnData.LongColumn) data).get(rowIndex);
    }

    float getFloat(int columnIndex) {
        TypedColumnData data = columnData[columnIndex];
        if (isNull(columnIndex)) {
            throw new NullPointerException("Column " + columnIndex + " is null");
        }
        return ((FlatColumnData.FloatColumn) data).get(rowIndex);
    }

    double getDouble(int columnIndex) {
        TypedColumnData data = columnData[columnIndex];
        if (isNull(columnIndex)) {
            throw new NullPointerException("Column " + columnIndex + " is null");
        }
        return ((FlatColumnData.DoubleColumn) data).get(rowIndex);
    }

    boolean getBoolean(int columnIndex) {
        TypedColumnData data = columnData[columnIndex];
        if (isNull(columnIndex)) {
            throw new NullPointerException("Column " + columnIndex + " is null");
        }
        return ((FlatColumnData.BooleanColumn) data).get(rowIndex);
    }

    // ==================== Object Type Accessors ====================

    String getString(int columnIndex) {
        TypedColumnData data = columnData[columnIndex];
        if (isNull(columnIndex)) {
            return null;
        }
        return new String(((FlatColumnData.ByteArrayColumn) data).get(rowIndex), StandardCharsets.UTF_8);
    }

    byte[] getBinary(int columnIndex) {
        TypedColumnData data = columnData[columnIndex];
        if (isNull(columnIndex)) {
            return null;
        }
        return ((FlatColumnData.ByteArrayColumn) data).get(rowIndex);
    }

    LocalDate getDate(int columnIndex) {
        TypedColumnData data = columnData[columnIndex];
        if (isNull(columnIndex)) {
            return null;
        }
        ColumnSchema col = schema.getColumn(columnIndex);
        int rawValue = ((FlatColumnData.IntColumn) data).get(rowIndex);
        return LogicalTypeConverter.convertToDate(rawValue, col.type());
    }

    LocalTime getTime(int columnIndex) {
        TypedColumnData data = columnData[columnIndex];
        if (isNull(columnIndex)) {
            return null;
        }
        ColumnSchema col = schema.getColumn(columnIndex);
        Object rawValue;
        if (col.type() == dev.hardwood.metadata.PhysicalType.INT32) {
            rawValue = ((FlatColumnData.IntColumn) data).get(rowIndex);
        }
        else {
            rawValue = ((FlatColumnData.LongColumn) data).get(rowIndex);
        }
        return LogicalTypeConverter.convertToTime(rawValue, col.type(), (LogicalType.TimeType) col.logicalType());
    }

    Instant getTimestamp(int columnIndex) {
        TypedColumnData data = columnData[columnIndex];
        if (isNull(columnIndex)) {
            return null;
        }
        ColumnSchema col = schema.getColumn(columnIndex);
        long rawValue = ((FlatColumnData.LongColumn) data).get(rowIndex);
        return LogicalTypeConverter.convertToTimestamp(rawValue, col.type(), (LogicalType.TimestampType) col.logicalType());
    }

    BigDecimal getDecimal(int columnIndex) {
        TypedColumnData data = columnData[columnIndex];
        if (isNull(columnIndex)) {
            return null;
        }
        ColumnSchema col = schema.getColumn(columnIndex);
        Object rawValue = switch (col.type()) {
            case INT32 -> ((FlatColumnData.IntColumn) data).get(rowIndex);
            case INT64 -> ((FlatColumnData.LongColumn) data).get(rowIndex);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> ((FlatColumnData.ByteArrayColumn) data).get(rowIndex);
            default -> throw new IllegalArgumentException("Unexpected physical type for DECIMAL: " + col.type());
        };
        return LogicalTypeConverter.convertToDecimal(rawValue, col.type(), (LogicalType.DecimalType) col.logicalType());
    }

    UUID getUuid(int columnIndex) {
        TypedColumnData data = columnData[columnIndex];
        if (isNull(columnIndex)) {
            return null;
        }
        ColumnSchema col = schema.getColumn(columnIndex);
        return LogicalTypeConverter.convertToUuid(((FlatColumnData.ByteArrayColumn) data).get(rowIndex), col.type());
    }

    // ==================== Metadata ====================

    boolean isNull(int columnIndex) {
        BitSet columnNulls = nulls[columnIndex];
        return columnNulls != null && columnNulls.get(rowIndex) == true;
    }
}
