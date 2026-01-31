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
import dev.morling.hardwood.schema.ProjectedSchema;

/**
 * RowReader implementation for flat schemas (no nested structures).
 * Directly accesses column data without record assembly.
 */
final class FlatRowReader extends AbstractRowReader {

    private FlatColumnData[] columnData;
    // Pre-extracted null BitSets to avoid megamorphic FlatColumnData::nulls() calls
    private BitSet[] nulls;

    FlatRowReader(FileSchema schema, ProjectedSchema projectedSchema, FileChannel channel,
                  List<RowGroup> rowGroups, HardwoodContext context, String fileName) {
        super(schema, projectedSchema, channel, rowGroups, context, fileName);
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

    /**
     * Translates an original column index to a projected index.
     * Throws IllegalArgumentException if the column is not in the projection.
     */
    private int requireProjectedIndex(int originalIndex, String columnName) {
        int projectedIndex = projectedSchema.toProjectedIndex(originalIndex);
        if (projectedIndex < 0) {
            throw new IllegalArgumentException("Column not in projection: " + columnName);
        }
        return projectedIndex;
    }

    private boolean isNullInternal(int projectedIndex) {
        BitSet columnNulls = nulls[projectedIndex];
        return columnNulls != null && columnNulls.get(rowIndex);
    }

    // ==================== Primitive Type Accessors ====================

    @Override
    public int getInt(String name) {
        ColumnSchema col = schema.getColumn(name);
        validatePhysicalType(col, PhysicalType.INT32);
        int projectedIndex = requireProjectedIndex(col.columnIndex(), name);
        return getIntInternal(projectedIndex);
    }

    @Override
    public int getInt(int projectedIndex) {
        return getIntInternal(projectedIndex);
    }

    private int getIntInternal(int projectedIndex) {
        if (isNullInternal(projectedIndex)) {
            throw new NullPointerException("Column " + projectedIndex + " is null");
        }
        return ((FlatColumnData.IntColumn) columnData[projectedIndex]).get(rowIndex);
    }

    @Override
    public long getLong(String name) {
        ColumnSchema col = schema.getColumn(name);
        validatePhysicalType(col, PhysicalType.INT64);
        int projectedIndex = requireProjectedIndex(col.columnIndex(), name);
        return getLongInternal(projectedIndex);
    }

    @Override
    public long getLong(int projectedIndex) {
        return getLongInternal(projectedIndex);
    }

    private long getLongInternal(int projectedIndex) {
        if (isNullInternal(projectedIndex)) {
            throw new NullPointerException("Column " + projectedIndex + " is null");
        }
        return ((FlatColumnData.LongColumn) columnData[projectedIndex]).get(rowIndex);
    }

    @Override
    public float getFloat(String name) {
        ColumnSchema col = schema.getColumn(name);
        validatePhysicalType(col, PhysicalType.FLOAT);
        int projectedIndex = requireProjectedIndex(col.columnIndex(), name);
        return getFloatInternal(projectedIndex);
    }

    @Override
    public float getFloat(int projectedIndex) {
        return getFloatInternal(projectedIndex);
    }

    private float getFloatInternal(int projectedIndex) {
        if (isNullInternal(projectedIndex)) {
            throw new NullPointerException("Column " + projectedIndex + " is null");
        }
        return ((FlatColumnData.FloatColumn) columnData[projectedIndex]).get(rowIndex);
    }

    @Override
    public double getDouble(String name) {
        ColumnSchema col = schema.getColumn(name);
        validatePhysicalType(col, PhysicalType.DOUBLE);
        int projectedIndex = requireProjectedIndex(col.columnIndex(), name);
        return getDoubleInternal(projectedIndex);
    }

    @Override
    public double getDouble(int projectedIndex) {
        return getDoubleInternal(projectedIndex);
    }

    private double getDoubleInternal(int projectedIndex) {
        if (isNullInternal(projectedIndex)) {
            throw new NullPointerException("Column " + projectedIndex + " is null");
        }
        return ((FlatColumnData.DoubleColumn) columnData[projectedIndex]).get(rowIndex);
    }

    @Override
    public boolean getBoolean(String name) {
        ColumnSchema col = schema.getColumn(name);
        validatePhysicalType(col, PhysicalType.BOOLEAN);
        int projectedIndex = requireProjectedIndex(col.columnIndex(), name);
        return getBooleanInternal(projectedIndex);
    }

    @Override
    public boolean getBoolean(int projectedIndex) {
        return getBooleanInternal(projectedIndex);
    }

    private boolean getBooleanInternal(int projectedIndex) {
        if (isNullInternal(projectedIndex)) {
            throw new NullPointerException("Column " + projectedIndex + " is null");
        }
        return ((FlatColumnData.BooleanColumn) columnData[projectedIndex]).get(rowIndex);
    }

    // ==================== Object Type Accessors ====================

    @Override
    public String getString(String name) {
        int originalIndex = schema.getColumn(name).columnIndex();
        int projectedIndex = requireProjectedIndex(originalIndex, name);
        return getStringInternal(projectedIndex);
    }

    @Override
    public String getString(int projectedIndex) {
        return getStringInternal(projectedIndex);
    }

    private String getStringInternal(int projectedIndex) {
        if (isNullInternal(projectedIndex)) {
            return null;
        }
        return new String(((FlatColumnData.ByteArrayColumn) columnData[projectedIndex]).get(rowIndex), StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getBinary(String name) {
        int originalIndex = schema.getColumn(name).columnIndex();
        int projectedIndex = requireProjectedIndex(originalIndex, name);
        return getBinaryInternal(projectedIndex);
    }

    @Override
    public byte[] getBinary(int projectedIndex) {
        return getBinaryInternal(projectedIndex);
    }

    private byte[] getBinaryInternal(int projectedIndex) {
        if (isNullInternal(projectedIndex)) {
            return null;
        }
        return ((FlatColumnData.ByteArrayColumn) columnData[projectedIndex]).get(rowIndex);
    }

    @Override
    public LocalDate getDate(String name) {
        int originalIndex = schema.getColumn(name).columnIndex();
        int projectedIndex = requireProjectedIndex(originalIndex, name);
        return getDateInternal(projectedIndex, originalIndex);
    }

    @Override
    public LocalDate getDate(int projectedIndex) {
        int originalIndex = projectedSchema.toOriginalIndex(projectedIndex);
        return getDateInternal(projectedIndex, originalIndex);
    }

    private LocalDate getDateInternal(int projectedIndex, int originalIndex) {
        if (isNullInternal(projectedIndex)) {
            return null;
        }
        ColumnSchema col = schema.getColumn(originalIndex);
        int rawValue = ((FlatColumnData.IntColumn) columnData[projectedIndex]).get(rowIndex);
        return LogicalTypeConverter.convertToDate(rawValue, col.type());
    }

    @Override
    public LocalTime getTime(String name) {
        int originalIndex = schema.getColumn(name).columnIndex();
        int projectedIndex = requireProjectedIndex(originalIndex, name);
        return getTimeInternal(projectedIndex, originalIndex);
    }

    @Override
    public LocalTime getTime(int projectedIndex) {
        int originalIndex = projectedSchema.toOriginalIndex(projectedIndex);
        return getTimeInternal(projectedIndex, originalIndex);
    }

    private LocalTime getTimeInternal(int projectedIndex, int originalIndex) {
        if (isNullInternal(projectedIndex)) {
            return null;
        }
        ColumnSchema col = schema.getColumn(originalIndex);
        Object rawValue;
        if (col.type() == PhysicalType.INT32) {
            rawValue = ((FlatColumnData.IntColumn) columnData[projectedIndex]).get(rowIndex);
        }
        else {
            rawValue = ((FlatColumnData.LongColumn) columnData[projectedIndex]).get(rowIndex);
        }
        return LogicalTypeConverter.convertToTime(rawValue, col.type(), (LogicalType.TimeType) col.logicalType());
    }

    @Override
    public Instant getTimestamp(String name) {
        int originalIndex = schema.getColumn(name).columnIndex();
        int projectedIndex = requireProjectedIndex(originalIndex, name);
        return getTimestampInternal(projectedIndex, originalIndex);
    }

    @Override
    public Instant getTimestamp(int projectedIndex) {
        int originalIndex = projectedSchema.toOriginalIndex(projectedIndex);
        return getTimestampInternal(projectedIndex, originalIndex);
    }

    private Instant getTimestampInternal(int projectedIndex, int originalIndex) {
        if (isNullInternal(projectedIndex)) {
            return null;
        }
        ColumnSchema col = schema.getColumn(originalIndex);
        long rawValue = ((FlatColumnData.LongColumn) columnData[projectedIndex]).get(rowIndex);
        return LogicalTypeConverter.convertToTimestamp(rawValue, col.type(), (LogicalType.TimestampType) col.logicalType());
    }

    @Override
    public BigDecimal getDecimal(String name) {
        int originalIndex = schema.getColumn(name).columnIndex();
        int projectedIndex = requireProjectedIndex(originalIndex, name);
        return getDecimalInternal(projectedIndex, originalIndex);
    }

    @Override
    public BigDecimal getDecimal(int projectedIndex) {
        int originalIndex = projectedSchema.toOriginalIndex(projectedIndex);
        return getDecimalInternal(projectedIndex, originalIndex);
    }

    private BigDecimal getDecimalInternal(int projectedIndex, int originalIndex) {
        if (isNullInternal(projectedIndex)) {
            return null;
        }
        ColumnSchema col = schema.getColumn(originalIndex);
        FlatColumnData data = columnData[projectedIndex];
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
        int originalIndex = schema.getColumn(name).columnIndex();
        int projectedIndex = requireProjectedIndex(originalIndex, name);
        return getUuidInternal(projectedIndex, originalIndex);
    }

    @Override
    public UUID getUuid(int projectedIndex) {
        int originalIndex = projectedSchema.toOriginalIndex(projectedIndex);
        return getUuidInternal(projectedIndex, originalIndex);
    }

    private UUID getUuidInternal(int projectedIndex, int originalIndex) {
        if (isNullInternal(projectedIndex)) {
            return null;
        }
        ColumnSchema col = schema.getColumn(originalIndex);
        return LogicalTypeConverter.convertToUuid(((FlatColumnData.ByteArrayColumn) columnData[projectedIndex]).get(rowIndex), col.type());
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
        int projectedIndex = requireProjectedIndex(col.columnIndex(), name);
        if (isNullInternal(projectedIndex)) {
            return null;
        }
        return columnData[projectedIndex].getValue(rowIndex);
    }

    // ==================== Metadata ====================

    @Override
    public boolean isNull(String name) {
        int originalIndex = schema.getColumn(name).columnIndex();
        int projectedIndex = requireProjectedIndex(originalIndex, name);
        return isNullInternal(projectedIndex);
    }

    @Override
    public boolean isNull(int projectedIndex) {
        return isNullInternal(projectedIndex);
    }

    @Override
    public int getFieldCount() {
        return projectedSchema.getProjectedColumnCount();
    }

    @Override
    public String getFieldName(int projectedIndex) {
        int originalIndex = projectedSchema.toOriginalIndex(projectedIndex);
        return schema.getColumn(originalIndex).name();
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
