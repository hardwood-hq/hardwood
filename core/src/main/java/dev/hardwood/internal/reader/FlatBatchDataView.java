/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;

import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.internal.util.StringToIntMap;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.row.PqDoubleList;
import dev.hardwood.row.PqIntList;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqLongList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

/**
 * BatchDataView implementation for flat schemas (no nested structures).
 * Directly accesses column data arrays for optimal performance.
 */
public final class FlatBatchDataView implements BatchDataView {

    private final FileSchema schema;
    private final ProjectedSchema projectedSchema;
    // Precomputed for fast named lookups: maps column name directly to projected index
    private final StringToIntMap nameToProjectedIndex;
    private final PhysicalType[] projectedTypes;

    private FlatColumnData[] columnData;
    // Pre-extracted null BitSets to avoid megamorphic FlatColumnData::nulls() calls
    private BitSet[] nulls;
    private int rowIndex;

    public FlatBatchDataView(FileSchema schema, ProjectedSchema projectedSchema) {
        this.schema = schema;
        this.projectedSchema = projectedSchema;

        int projectedCount = projectedSchema.getProjectedColumnCount();
        this.nameToProjectedIndex = new StringToIntMap(projectedCount);
        this.projectedTypes = new PhysicalType[projectedCount];
        for (int pi = 0; pi < projectedCount; pi++) {
            int originalIndex = projectedSchema.toOriginalIndex(pi);
            ColumnSchema col = schema.getColumn(originalIndex);
            nameToProjectedIndex.put(col.name(), pi);
            projectedTypes[pi] = col.type();
        }
    }

    @Override
    public void setBatchData(TypedColumnData[] newColumnData) {
        this.columnData = new FlatColumnData[newColumnData.length];
        this.nulls = new BitSet[newColumnData.length];
        for (int i = 0; i < newColumnData.length; i++) {
            FlatColumnData flat = (FlatColumnData) newColumnData[i];
            this.columnData[i] = flat;
            this.nulls[i] = flat.nulls();
        }
    }

    @Override
    public void setRowIndex(int rowIndex) {
        this.rowIndex = rowIndex;
    }

    // ==================== Index Lookup ====================

    private int lookupProjectedIndex(String name) {
        int projectedIndex = nameToProjectedIndex.get(name);
        if (projectedIndex < 0) {
            throw new IllegalArgumentException("Column not in projection: " + name);
        }
        return projectedIndex;
    }

    private int lookupAndValidate(String name, PhysicalType expectedType) {
        int projectedIndex = lookupProjectedIndex(name);
        if (projectedTypes[projectedIndex] != expectedType) {
            throw new IllegalArgumentException(
                    "Field '" + name + "' has physical type " + projectedTypes[projectedIndex]
                            + ", expected " + expectedType);
        }
        return projectedIndex;
    }

    @Override
    public boolean isNull(String name) {
        return isNull(lookupProjectedIndex(name));
    }

    @Override
    public boolean isNull(int projectedIndex) {
        BitSet columnNulls = nulls[projectedIndex];
        return columnNulls != null && columnNulls.get(rowIndex);
    }

    // ==================== Primitive Type Accessors (by name) ====================

    @Override
    public int getInt(String name) {
        return getInt(lookupAndValidate(name, PhysicalType.INT32));
    }

    @Override
    public long getLong(String name) {
        return getLong(lookupAndValidate(name, PhysicalType.INT64));
    }

    @Override
    public float getFloat(String name) {
        return getFloat(lookupAndValidate(name, PhysicalType.FLOAT));
    }

    @Override
    public double getDouble(String name) {
        return getDouble(lookupAndValidate(name, PhysicalType.DOUBLE));
    }

    @Override
    public boolean getBoolean(String name) {
        return getBoolean(lookupAndValidate(name, PhysicalType.BOOLEAN));
    }

    // ==================== Primitive Type Accessors (by index) ====================

    @Override
    public int getInt(int projectedIndex) {
        if (isNull(projectedIndex)) {
            throw new NullPointerException("Column " + projectedIndex + " is null");
        }
        return ((FlatColumnData.IntColumn) columnData[projectedIndex]).get(rowIndex);
    }

    @Override
    public long getLong(int projectedIndex) {
        if (isNull(projectedIndex)) {
            throw new NullPointerException("Column " + projectedIndex + " is null");
        }
        return ((FlatColumnData.LongColumn) columnData[projectedIndex]).get(rowIndex);
    }

    @Override
    public float getFloat(int projectedIndex) {
        if (isNull(projectedIndex)) {
            throw new NullPointerException("Column " + projectedIndex + " is null");
        }
        return ((FlatColumnData.FloatColumn) columnData[projectedIndex]).get(rowIndex);
    }

    @Override
    public double getDouble(int projectedIndex) {
        if (isNull(projectedIndex)) {
            throw new NullPointerException("Column " + projectedIndex + " is null");
        }
        return ((FlatColumnData.DoubleColumn) columnData[projectedIndex]).get(rowIndex);
    }

    @Override
    public boolean getBoolean(int projectedIndex) {
        if (isNull(projectedIndex)) {
            throw new NullPointerException("Column " + projectedIndex + " is null");
        }
        return ((FlatColumnData.BooleanColumn) columnData[projectedIndex]).get(rowIndex);
    }

    // ==================== Object Type Accessors (by name) ====================

    @Override
    public String getString(String name) {
        return getString(lookupProjectedIndex(name));
    }

    @Override
    public byte[] getBinary(String name) {
        return getBinary(lookupProjectedIndex(name));
    }

    @Override
    public LocalDate getDate(String name) {
        return getDate(lookupProjectedIndex(name));
    }

    @Override
    public LocalTime getTime(String name) {
        return getTime(lookupProjectedIndex(name));
    }

    @Override
    public Instant getTimestamp(String name) {
        return getTimestamp(lookupProjectedIndex(name));
    }

    @Override
    public BigDecimal getDecimal(String name) {
        return getDecimal(lookupProjectedIndex(name));
    }

    @Override
    public UUID getUuid(String name) {
        return getUuid(lookupProjectedIndex(name));
    }

    // ==================== Object Type Accessors (by index) ====================

    @Override
    public String getString(int projectedIndex) {
        if (isNull(projectedIndex)) {
            return null;
        }
        return new String(((FlatColumnData.ByteArrayColumn) columnData[projectedIndex]).get(rowIndex),
                StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getBinary(int projectedIndex) {
        if (isNull(projectedIndex)) {
            return null;
        }
        return ((FlatColumnData.ByteArrayColumn) columnData[projectedIndex]).get(rowIndex);
    }

    @Override
    public LocalDate getDate(int projectedIndex) {
        if (isNull(projectedIndex)) {
            return null;
        }
        int originalIndex = projectedSchema.toOriginalIndex(projectedIndex);
        ColumnSchema col = schema.getColumn(originalIndex);
        int rawValue = ((FlatColumnData.IntColumn) columnData[projectedIndex]).get(rowIndex);
        return LogicalTypeConverter.convertToDate(rawValue, col.type());
    }

    @Override
    public LocalTime getTime(int projectedIndex) {
        if (isNull(projectedIndex)) {
            return null;
        }
        int originalIndex = projectedSchema.toOriginalIndex(projectedIndex);
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
    public Instant getTimestamp(int projectedIndex) {
        if (isNull(projectedIndex)) {
            return null;
        }
        int originalIndex = projectedSchema.toOriginalIndex(projectedIndex);
        ColumnSchema col = schema.getColumn(originalIndex);
        long rawValue = ((FlatColumnData.LongColumn) columnData[projectedIndex]).get(rowIndex);
        return LogicalTypeConverter.convertToTimestamp(rawValue, col.type(),
                (LogicalType.TimestampType) col.logicalType());
    }

    @Override
    public BigDecimal getDecimal(int projectedIndex) {
        if (isNull(projectedIndex)) {
            return null;
        }
        int originalIndex = projectedSchema.toOriginalIndex(projectedIndex);
        ColumnSchema col = schema.getColumn(originalIndex);
        FlatColumnData data = columnData[projectedIndex];
        Object rawValue = switch (col.type()) {
            case INT32 -> ((FlatColumnData.IntColumn) data).get(rowIndex);
            case INT64 -> ((FlatColumnData.LongColumn) data).get(rowIndex);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> ((FlatColumnData.ByteArrayColumn) data).get(rowIndex);
            default -> throw new IllegalArgumentException("Unexpected physical type for DECIMAL: " + col.type());
        };
        return LogicalTypeConverter.convertToDecimal(rawValue, col.type(),
                (LogicalType.DecimalType) col.logicalType());
    }

    @Override
    public UUID getUuid(int projectedIndex) {
        if (isNull(projectedIndex)) {
            return null;
        }
        int originalIndex = projectedSchema.toOriginalIndex(projectedIndex);
        ColumnSchema col = schema.getColumn(originalIndex);
        return LogicalTypeConverter.convertToUuid(
                ((FlatColumnData.ByteArrayColumn) columnData[projectedIndex]).get(rowIndex), col.type());
    }

    // ==================== Nested Type Accessors (by name) ====================

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

    // ==================== Nested Type Accessors (by index) ====================

    @Override
    public PqStruct getStruct(int projectedIndex) {
        throw new UnsupportedOperationException("Nested struct access not supported for flat schemas.");
    }

    @Override
    public PqIntList getListOfInts(int projectedIndex) {
        throw new UnsupportedOperationException("List access not supported for flat schemas.");
    }

    @Override
    public PqLongList getListOfLongs(int projectedIndex) {
        throw new UnsupportedOperationException("List access not supported for flat schemas.");
    }

    @Override
    public PqDoubleList getListOfDoubles(int projectedIndex) {
        throw new UnsupportedOperationException("List access not supported for flat schemas.");
    }

    @Override
    public PqList getList(int projectedIndex) {
        throw new UnsupportedOperationException("List access not supported for flat schemas.");
    }

    @Override
    public PqMap getMap(int projectedIndex) {
        throw new UnsupportedOperationException("Map access not supported for flat schemas.");
    }

    // ==================== Generic Value Access ====================

    @Override
    public Object getValue(String name) {
        return getValue(lookupProjectedIndex(name));
    }

    @Override
    public Object getValue(int projectedIndex) {
        if (isNull(projectedIndex)) {
            return null;
        }
        return columnData[projectedIndex].getValue(rowIndex);
    }

    // ==================== Metadata ====================

    @Override
    public int getFieldCount() {
        return projectedSchema.getProjectedColumnCount();
    }

    @Override
    public String getFieldName(int projectedIndex) {
        int originalIndex = projectedSchema.toOriginalIndex(projectedIndex);
        return schema.getColumn(originalIndex).name();
    }

    @Override
    public FlatColumnData[] getFlatColumnData() {
        return columnData;
    }
}
