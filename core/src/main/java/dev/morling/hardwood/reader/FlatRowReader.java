/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.reader;

import java.math.BigDecimal;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import dev.morling.hardwood.internal.conversion.LogicalTypeConverter;
import dev.morling.hardwood.internal.reader.FlatColumnData;
import dev.morling.hardwood.internal.reader.TypedColumnData;
import dev.morling.hardwood.metadata.LogicalType;
import dev.morling.hardwood.metadata.PhysicalType;
import dev.morling.hardwood.metadata.RowGroup;
import dev.morling.hardwood.row.PqDoubleList;
import dev.morling.hardwood.row.PqIntList;
import dev.morling.hardwood.row.PqList;
import dev.morling.hardwood.row.PqLongList;
import dev.morling.hardwood.row.PqMap;
import dev.morling.hardwood.row.PqStruct;
import dev.morling.hardwood.schema.ColumnSchema;
import dev.morling.hardwood.schema.FileSchema;

/**
 * RowReader implementation for flat schemas (no nested structures).
 * Directly accesses column data without record assembly.
 */
final class FlatRowReader extends AbstractRowReader {

    private FlatColumnData[] columnData;
    // Pre-extracted null BitSets to avoid megamorphic FlatColumnData::nulls() calls
    private BitSet[] nulls;

    FlatRowReader(FileSchema schema, FileChannel channel, List<RowGroup> rowGroups,
                  ExecutorService executor, String fileName) {
        super(schema, channel, rowGroups, executor, fileName);
    }

    @Override
    protected void onBatchLoaded(TypedColumnData[] newColumnData) {
        this.columnData = new FlatColumnData[newColumnData.length];
        this.nulls = new BitSet[newColumnData.length];
        for (int i = 0; i < newColumnData.length; i++) {
            FlatColumnData flat = (FlatColumnData) newColumnData[i];
            this.columnData[i] = flat;
            this.nulls[i] = flat.nulls();
        }
    }

    private boolean isNullInternal(int columnIndex) {
        BitSet columnNulls = nulls[columnIndex];
        return columnNulls != null && columnNulls.get(rowIndex);
    }

    // ==================== Primitive Type Accessors ====================

    @Override
    public int getInt(String name) {
        ColumnSchema col = schema.getColumn(name);
        validatePhysicalType(col, PhysicalType.INT32);
        return getInt(col.columnIndex());
    }

    @Override
    public int getInt(int columnIndex) {
        if (isNullInternal(columnIndex)) {
            throw new NullPointerException("Column " + columnIndex + " is null");
        }
        return ((FlatColumnData.IntColumn) columnData[columnIndex]).get(rowIndex);
    }

    @Override
    public long getLong(String name) {
        ColumnSchema col = schema.getColumn(name);
        validatePhysicalType(col, PhysicalType.INT64);
        return getLong(col.columnIndex());
    }

    @Override
    public long getLong(int columnIndex) {
        if (isNullInternal(columnIndex)) {
            throw new NullPointerException("Column " + columnIndex + " is null");
        }
        return ((FlatColumnData.LongColumn) columnData[columnIndex]).get(rowIndex);
    }

    @Override
    public float getFloat(String name) {
        ColumnSchema col = schema.getColumn(name);
        validatePhysicalType(col, PhysicalType.FLOAT);
        return getFloat(col.columnIndex());
    }

    @Override
    public float getFloat(int columnIndex) {
        if (isNullInternal(columnIndex)) {
            throw new NullPointerException("Column " + columnIndex + " is null");
        }
        return ((FlatColumnData.FloatColumn) columnData[columnIndex]).get(rowIndex);
    }

    @Override
    public double getDouble(String name) {
        ColumnSchema col = schema.getColumn(name);
        validatePhysicalType(col, PhysicalType.DOUBLE);
        return getDouble(col.columnIndex());
    }

    @Override
    public double getDouble(int columnIndex) {
        if (isNullInternal(columnIndex)) {
            throw new NullPointerException("Column " + columnIndex + " is null");
        }
        return ((FlatColumnData.DoubleColumn) columnData[columnIndex]).get(rowIndex);
    }

    @Override
    public boolean getBoolean(String name) {
        ColumnSchema col = schema.getColumn(name);
        validatePhysicalType(col, PhysicalType.BOOLEAN);
        return getBoolean(col.columnIndex());
    }

    @Override
    public boolean getBoolean(int columnIndex) {
        if (isNullInternal(columnIndex)) {
            throw new NullPointerException("Column " + columnIndex + " is null");
        }
        return ((FlatColumnData.BooleanColumn) columnData[columnIndex]).get(rowIndex);
    }

    // ==================== Object Type Accessors ====================

    @Override
    public String getString(String name) {
        return getString(schema.getColumn(name).columnIndex());
    }

    @Override
    public String getString(int columnIndex) {
        if (isNullInternal(columnIndex)) {
            return null;
        }
        return new String(((FlatColumnData.ByteArrayColumn) columnData[columnIndex]).get(rowIndex), StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getBinary(String name) {
        return getBinary(schema.getColumn(name).columnIndex());
    }

    @Override
    public byte[] getBinary(int columnIndex) {
        if (isNullInternal(columnIndex)) {
            return null;
        }
        return ((FlatColumnData.ByteArrayColumn) columnData[columnIndex]).get(rowIndex);
    }

    @Override
    public LocalDate getDate(String name) {
        return getDate(schema.getColumn(name).columnIndex());
    }

    @Override
    public LocalDate getDate(int columnIndex) {
        if (isNullInternal(columnIndex)) {
            return null;
        }
        ColumnSchema col = schema.getColumn(columnIndex);
        int rawValue = ((FlatColumnData.IntColumn) columnData[columnIndex]).get(rowIndex);
        return LogicalTypeConverter.convertToDate(rawValue, col.type());
    }

    @Override
    public LocalTime getTime(String name) {
        return getTime(schema.getColumn(name).columnIndex());
    }

    @Override
    public LocalTime getTime(int columnIndex) {
        if (isNullInternal(columnIndex)) {
            return null;
        }
        ColumnSchema col = schema.getColumn(columnIndex);
        Object rawValue;
        if (col.type() == PhysicalType.INT32) {
            rawValue = ((FlatColumnData.IntColumn) columnData[columnIndex]).get(rowIndex);
        }
        else {
            rawValue = ((FlatColumnData.LongColumn) columnData[columnIndex]).get(rowIndex);
        }
        return LogicalTypeConverter.convertToTime(rawValue, col.type(), (LogicalType.TimeType) col.logicalType());
    }

    @Override
    public Instant getTimestamp(String name) {
        return getTimestamp(schema.getColumn(name).columnIndex());
    }

    @Override
    public Instant getTimestamp(int columnIndex) {
        if (isNullInternal(columnIndex)) {
            return null;
        }
        ColumnSchema col = schema.getColumn(columnIndex);
        long rawValue = ((FlatColumnData.LongColumn) columnData[columnIndex]).get(rowIndex);
        return LogicalTypeConverter.convertToTimestamp(rawValue, col.type(), (LogicalType.TimestampType) col.logicalType());
    }

    @Override
    public BigDecimal getDecimal(String name) {
        return getDecimal(schema.getColumn(name).columnIndex());
    }

    @Override
    public BigDecimal getDecimal(int columnIndex) {
        if (isNullInternal(columnIndex)) {
            return null;
        }
        ColumnSchema col = schema.getColumn(columnIndex);
        FlatColumnData data = columnData[columnIndex];
        Object rawValue = switch (col.type()) {
            case INT32 -> ((FlatColumnData.IntColumn) data).get(rowIndex);
            case INT64 -> ((FlatColumnData.LongColumn) data).get(rowIndex);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> ((FlatColumnData.ByteArrayColumn) data).get(rowIndex);
            default -> throw new IllegalArgumentException("Unexpected physical type for DECIMAL: " + col.type());
        };
        return LogicalTypeConverter.convertToDecimal(rawValue, col.type(), (LogicalType.DecimalType) col.logicalType());
    }

    @Override
    public UUID getUuid(String name) {
        return getUuid(schema.getColumn(name).columnIndex());
    }

    @Override
    public UUID getUuid(int columnIndex) {
        if (isNullInternal(columnIndex)) {
            return null;
        }
        ColumnSchema col = schema.getColumn(columnIndex);
        return LogicalTypeConverter.convertToUuid(((FlatColumnData.ByteArrayColumn) columnData[columnIndex]).get(rowIndex), col.type());
    }

    // ==================== Nested Type Accessors ====================

    @Override
    public PqStruct getStruct(String name) {
        throw new UnsupportedOperationException("Nested struct access not supported for flat schemas.");
    }

    @Override
    public PqIntList getListOfInts(String name) {
        throw new UnsupportedOperationException("List access not supported for flat schemas.");
    }

    @Override
    public PqLongList getListOfLongs(String name) {
        throw new UnsupportedOperationException("List access not supported for flat schemas.");
    }

    @Override
    public PqDoubleList getListOfDoubles(String name) {
        throw new UnsupportedOperationException("List access not supported for flat schemas.");
    }

    @Override
    public PqList getList(String name) {
        throw new UnsupportedOperationException("List access not supported for flat schemas.");
    }

    @Override
    public PqMap getMap(String name) {
        throw new UnsupportedOperationException("Map access not supported for flat schemas.");
    }

    // ==================== Generic Fallback ====================

    @Override
    public Object getValue(String name) {
        ColumnSchema col = schema.getColumn(name);
        int idx = col.columnIndex();
        if (isNullInternal(idx)) {
            return null;
        }
        return columnData[idx].getValue(rowIndex);
    }

    // ==================== Metadata ====================

    @Override
    public boolean isNull(String name) {
        return isNull(schema.getColumn(name).columnIndex());
    }

    @Override
    public boolean isNull(int columnIndex) {
        return isNullInternal(columnIndex);
    }

    @Override
    public int getFieldCount() {
        return schema.getColumnCount();
    }

    @Override
    public String getFieldName(int index) {
        return schema.getColumn(index).name();
    }

    // ==================== Internal Helpers ====================

    private void validatePhysicalType(ColumnSchema col, PhysicalType... expectedTypes) {
        for (PhysicalType expected : expectedTypes) {
            if (col.type() == expected) {
                return;
            }
        }
        String expectedStr = expectedTypes.length == 1
                ? expectedTypes[0].toString()
                : java.util.Arrays.toString(expectedTypes);
        throw new IllegalArgumentException(
                "Field '" + col.name() + "' has physical type " + col.type() + ", expected " + expectedStr);
    }
}
