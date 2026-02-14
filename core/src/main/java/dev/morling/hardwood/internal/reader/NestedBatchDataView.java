/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.UUID;

import dev.morling.hardwood.row.PqDoubleList;
import dev.morling.hardwood.row.PqIntList;
import dev.morling.hardwood.row.PqList;
import dev.morling.hardwood.row.PqLongList;
import dev.morling.hardwood.row.PqMap;
import dev.morling.hardwood.row.PqStruct;
import dev.morling.hardwood.schema.FileSchema;
import dev.morling.hardwood.schema.ProjectedSchema;
import dev.morling.hardwood.schema.SchemaNode;

/**
 * BatchDataView implementation for nested schemas.
 * Uses RecordAssembler to build hierarchical row structures.
 */
public final class NestedBatchDataView implements BatchDataView {

    private final FileSchema schema;
    private final ProjectedSchema projectedSchema;
    private final RecordAssembler assembler;

    // Maps projected field index -> original field index in root children
    private final int[] projectedFieldToOriginal;
    // Maps original field index -> projected field index (-1 if not projected)
    private final int[] originalFieldToProjected;

    private List<NestedColumnData> columnData;
    private MutableStruct currentRow;

    public NestedBatchDataView(FileSchema schema, ProjectedSchema projectedSchema) {
        this.schema = schema;
        this.projectedSchema = projectedSchema;
        this.assembler = new RecordAssembler(schema, projectedSchema);
        this.projectedFieldToOriginal = projectedSchema.getProjectedFieldIndices().clone();

        // Build reverse mapping
        int totalFields = schema.getRootNode().children().size();
        this.originalFieldToProjected = new int[totalFields];
        for (int i = 0; i < totalFields; i++) {
            originalFieldToProjected[i] = -1;
        }
        for (int projIdx = 0; projIdx < projectedFieldToOriginal.length; projIdx++) {
            originalFieldToProjected[projectedFieldToOriginal[projIdx]] = projIdx;
        }
    }

    @Override
    public void setBatchData(TypedColumnData[] newColumnData) {
        NestedColumnData[] nested = new NestedColumnData[newColumnData.length];
        for (int i = 0; i < newColumnData.length; i++) {
            nested[i] = (NestedColumnData) newColumnData[i];
        }
        this.columnData = List.of(nested);
    }

    @Override
    public void setRowIndex(int rowIndex) {
        this.currentRow = assembler.assembleRow(columnData, rowIndex);
    }

    // ==================== Index Lookup ====================

    private int lookupProjectedIndex(String name) {
        List<SchemaNode> children = schema.getRootNode().children();
        for (int i = 0; i < children.size(); i++) {
            if (children.get(i).name().equals(name)) {
                int projectedIdx = originalFieldToProjected[i];
                if (projectedIdx < 0) {
                    throw new IllegalArgumentException("Column not in projection: " + name);
                }
                return projectedIdx;
            }
        }
        throw new IllegalArgumentException("Field not found: " + name);
    }

    private int toOriginalFieldIndex(int projectedIndex) {
        return projectedFieldToOriginal[projectedIndex];
    }

    private SchemaNode getFieldSchema(int originalFieldIndex) {
        return schema.getRootNode().children().get(originalFieldIndex);
    }

    @Override
    public boolean isNull(String name) {
        return isNull(lookupProjectedIndex(name));
    }

    @Override
    public boolean isNull(int projectedIndex) {
        int originalFieldIndex = toOriginalFieldIndex(projectedIndex);
        return currentRow.getChild(originalFieldIndex) == null;
    }

    // ==================== Primitive Type Accessors (by name) ====================

    @Override
    public int getInt(String name) {
        return getInt(lookupProjectedIndex(name));
    }

    @Override
    public long getLong(String name) {
        return getLong(lookupProjectedIndex(name));
    }

    @Override
    public float getFloat(String name) {
        return getFloat(lookupProjectedIndex(name));
    }

    @Override
    public double getDouble(String name) {
        return getDouble(lookupProjectedIndex(name));
    }

    @Override
    public boolean getBoolean(String name) {
        return getBoolean(lookupProjectedIndex(name));
    }

    // ==================== Primitive Type Accessors (by index) ====================

    @Override
    public int getInt(int projectedIndex) {
        int originalFieldIndex = toOriginalFieldIndex(projectedIndex);
        SchemaNode fieldSchema = getFieldSchema(originalFieldIndex);
        Integer val = ValueConverter.convertToInt(currentRow.getChild(originalFieldIndex), fieldSchema);
        if (val == null) {
            throw new NullPointerException("Column " + originalFieldIndex + " is null");
        }
        return val;
    }

    @Override
    public long getLong(int projectedIndex) {
        int originalFieldIndex = toOriginalFieldIndex(projectedIndex);
        SchemaNode fieldSchema = getFieldSchema(originalFieldIndex);
        Long val = ValueConverter.convertToLong(currentRow.getChild(originalFieldIndex), fieldSchema);
        if (val == null) {
            throw new NullPointerException("Column " + originalFieldIndex + " is null");
        }
        return val;
    }

    @Override
    public float getFloat(int projectedIndex) {
        int originalFieldIndex = toOriginalFieldIndex(projectedIndex);
        SchemaNode fieldSchema = getFieldSchema(originalFieldIndex);
        Float val = ValueConverter.convertToFloat(currentRow.getChild(originalFieldIndex), fieldSchema);
        if (val == null) {
            throw new NullPointerException("Column " + originalFieldIndex + " is null");
        }
        return val;
    }

    @Override
    public double getDouble(int projectedIndex) {
        int originalFieldIndex = toOriginalFieldIndex(projectedIndex);
        SchemaNode fieldSchema = getFieldSchema(originalFieldIndex);
        Double val = ValueConverter.convertToDouble(currentRow.getChild(originalFieldIndex), fieldSchema);
        if (val == null) {
            throw new NullPointerException("Column " + originalFieldIndex + " is null");
        }
        return val;
    }

    @Override
    public boolean getBoolean(int projectedIndex) {
        int originalFieldIndex = toOriginalFieldIndex(projectedIndex);
        SchemaNode fieldSchema = getFieldSchema(originalFieldIndex);
        Boolean val = ValueConverter.convertToBoolean(currentRow.getChild(originalFieldIndex), fieldSchema);
        if (val == null) {
            throw new NullPointerException("Column " + originalFieldIndex + " is null");
        }
        return val;
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
        int originalFieldIndex = toOriginalFieldIndex(projectedIndex);
        SchemaNode fieldSchema = getFieldSchema(originalFieldIndex);
        return ValueConverter.convertToString(currentRow.getChild(originalFieldIndex), fieldSchema);
    }

    @Override
    public byte[] getBinary(int projectedIndex) {
        int originalFieldIndex = toOriginalFieldIndex(projectedIndex);
        SchemaNode fieldSchema = getFieldSchema(originalFieldIndex);
        return ValueConverter.convertToBinary(currentRow.getChild(originalFieldIndex), fieldSchema);
    }

    @Override
    public LocalDate getDate(int projectedIndex) {
        int originalFieldIndex = toOriginalFieldIndex(projectedIndex);
        SchemaNode fieldSchema = getFieldSchema(originalFieldIndex);
        return ValueConverter.convertToDate(currentRow.getChild(originalFieldIndex), fieldSchema);
    }

    @Override
    public LocalTime getTime(int projectedIndex) {
        int originalFieldIndex = toOriginalFieldIndex(projectedIndex);
        SchemaNode fieldSchema = getFieldSchema(originalFieldIndex);
        return ValueConverter.convertToTime(currentRow.getChild(originalFieldIndex), fieldSchema);
    }

    @Override
    public Instant getTimestamp(int projectedIndex) {
        int originalFieldIndex = toOriginalFieldIndex(projectedIndex);
        SchemaNode fieldSchema = getFieldSchema(originalFieldIndex);
        return ValueConverter.convertToTimestamp(currentRow.getChild(originalFieldIndex), fieldSchema);
    }

    @Override
    public BigDecimal getDecimal(int projectedIndex) {
        int originalFieldIndex = toOriginalFieldIndex(projectedIndex);
        SchemaNode fieldSchema = getFieldSchema(originalFieldIndex);
        return ValueConverter.convertToDecimal(currentRow.getChild(originalFieldIndex), fieldSchema);
    }

    @Override
    public UUID getUuid(int projectedIndex) {
        int originalFieldIndex = toOriginalFieldIndex(projectedIndex);
        SchemaNode fieldSchema = getFieldSchema(originalFieldIndex);
        return ValueConverter.convertToUuid(currentRow.getChild(originalFieldIndex), fieldSchema);
    }

    // ==================== Nested Type Accessors (by name) ====================

    @Override
    public PqStruct getStruct(String name) {
        return getStruct(lookupProjectedIndex(name));
    }

    @Override
    public PqIntList getListOfInts(String name) {
        return getListOfInts(lookupProjectedIndex(name));
    }

    @Override
    public PqLongList getListOfLongs(String name) {
        return getListOfLongs(lookupProjectedIndex(name));
    }

    @Override
    public PqDoubleList getListOfDoubles(String name) {
        return getListOfDoubles(lookupProjectedIndex(name));
    }

    @Override
    public PqList getList(String name) {
        return getList(lookupProjectedIndex(name));
    }

    @Override
    public PqMap getMap(String name) {
        return getMap(lookupProjectedIndex(name));
    }

    // ==================== Nested Type Accessors (by index) ====================

    @Override
    public PqStruct getStruct(int projectedIndex) {
        int originalFieldIndex = toOriginalFieldIndex(projectedIndex);
        SchemaNode fieldSchema = getFieldSchema(originalFieldIndex);
        return ValueConverter.convertToStruct(currentRow.getChild(originalFieldIndex), fieldSchema);
    }

    @Override
    public PqIntList getListOfInts(int projectedIndex) {
        int originalFieldIndex = toOriginalFieldIndex(projectedIndex);
        SchemaNode fieldSchema = getFieldSchema(originalFieldIndex);
        return ValueConverter.convertToIntList(currentRow.getChild(originalFieldIndex), fieldSchema);
    }

    @Override
    public PqLongList getListOfLongs(int projectedIndex) {
        int originalFieldIndex = toOriginalFieldIndex(projectedIndex);
        SchemaNode fieldSchema = getFieldSchema(originalFieldIndex);
        return ValueConverter.convertToLongList(currentRow.getChild(originalFieldIndex), fieldSchema);
    }

    @Override
    public PqDoubleList getListOfDoubles(int projectedIndex) {
        int originalFieldIndex = toOriginalFieldIndex(projectedIndex);
        SchemaNode fieldSchema = getFieldSchema(originalFieldIndex);
        return ValueConverter.convertToDoubleList(currentRow.getChild(originalFieldIndex), fieldSchema);
    }

    @Override
    public PqList getList(int projectedIndex) {
        int originalFieldIndex = toOriginalFieldIndex(projectedIndex);
        SchemaNode fieldSchema = getFieldSchema(originalFieldIndex);
        return ValueConverter.convertToList(currentRow.getChild(originalFieldIndex), fieldSchema);
    }

    @Override
    public PqMap getMap(int projectedIndex) {
        int originalFieldIndex = toOriginalFieldIndex(projectedIndex);
        SchemaNode fieldSchema = getFieldSchema(originalFieldIndex);
        return ValueConverter.convertToMap(currentRow.getChild(originalFieldIndex), fieldSchema);
    }

    // ==================== Generic Value Access ====================

    @Override
    public Object getValue(String name) {
        return getValue(lookupProjectedIndex(name));
    }

    @Override
    public Object getValue(int projectedIndex) {
        int originalFieldIndex = toOriginalFieldIndex(projectedIndex);
        return currentRow.getChild(originalFieldIndex);
    }

    // ==================== Metadata ====================

    @Override
    public int getFieldCount() {
        return projectedFieldToOriginal.length;
    }

    @Override
    public String getFieldName(int projectedIndex) {
        int originalFieldIndex = projectedFieldToOriginal[projectedIndex];
        return schema.getRootNode().children().get(originalFieldIndex).name();
    }

    @Override
    public FlatColumnData[] getFlatColumnData() {
        return null;
    }
}
