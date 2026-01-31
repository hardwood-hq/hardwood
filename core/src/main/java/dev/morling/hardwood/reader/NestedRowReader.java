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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.UUID;

import dev.morling.hardwood.internal.reader.MutableStruct;
import dev.morling.hardwood.internal.reader.NestedColumnData;
import dev.morling.hardwood.internal.reader.RecordAssembler;
import dev.morling.hardwood.internal.reader.TypedColumnData;
import dev.morling.hardwood.internal.reader.ValueConverter;
import dev.morling.hardwood.metadata.RowGroup;
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
 * RowReader implementation for nested schemas.
 * Uses RecordAssembler to build hierarchical row structures.
 */
final class NestedRowReader extends AbstractRowReader {

    private RecordAssembler assembler;
    private List<NestedColumnData> columnData;
    private MutableStruct currentRow;

    // Maps projected field index -> original field index in root children
    private int[] projectedFieldToOriginal;
    // Maps original field index -> projected field index (-1 if not projected)
    private int[] originalFieldToProjected;

    NestedRowReader(FileSchema schema, ProjectedSchema projectedSchema, FileChannel channel,
                    List<RowGroup> rowGroups, HardwoodContext context, String fileName) {
        super(schema, projectedSchema, channel, rowGroups, context, fileName);
    }

    @Override
    protected void onInitialize() {
        assembler = new RecordAssembler(schema, projectedSchema);

        // Build field index mapping
        int[] projectedFieldIndices = projectedSchema.getProjectedFieldIndices();
        int totalFields = schema.getRootNode().children().size();

        projectedFieldToOriginal = projectedFieldIndices.clone();
        originalFieldToProjected = new int[totalFields];
        for (int i = 0; i < totalFields; i++) {
            originalFieldToProjected[i] = -1;
        }
        for (int projIdx = 0; projIdx < projectedFieldIndices.length; projIdx++) {
            originalFieldToProjected[projectedFieldIndices[projIdx]] = projIdx;
        }
    }

    @Override
    protected void onBatchLoaded(TypedColumnData[] newColumnData) {
        NestedColumnData[] nested = new NestedColumnData[newColumnData.length];
        for (int i = 0; i < newColumnData.length; i++) {
            nested[i] = (NestedColumnData) newColumnData[i];
        }
        this.columnData = List.of(nested);
    }

    @Override
    protected void onNext() {
        currentRow = assembler.assembleRow(columnData, rowIndex);
    }

    /**
     * Gets the field index in the schema from a field name.
     * Throws IllegalArgumentException if the field is not in the projection.
     */
    private int getFieldIndex(String name) {
        List<SchemaNode> children = schema.getRootNode().children();
        for (int i = 0; i < children.size(); i++) {
            if (children.get(i).name().equals(name)) {
                int projectedIdx = originalFieldToProjected[i];
                if (projectedIdx < 0) {
                    throw new IllegalArgumentException("Column not in projection: " + name);
                }
                return i; // Return original index for accessing schema
            }
        }
        throw new IllegalArgumentException("Field not found: " + name);
    }

    /**
     * Validates that the given original field index is in the projection.
     */
    private void requireProjectedField(int originalFieldIndex) {
        if (originalFieldToProjected[originalFieldIndex] < 0) {
            String name = schema.getRootNode().children().get(originalFieldIndex).name();
            throw new IllegalArgumentException("Column not in projection: " + name);
        }
    }

    // ==================== Primitive Type Accessors ====================

    @Override
    public int getInt(String name) {
        int fieldIndex = getFieldIndex(name);
        return getIntInternal(fieldIndex);
    }

    @Override
    public int getInt(int projectedFieldIndex) {
        int originalFieldIndex = projectedFieldToOriginal[projectedFieldIndex];
        return getIntInternal(originalFieldIndex);
    }

    private int getIntInternal(int originalFieldIndex) {
        SchemaNode fieldSchema = schema.getRootNode().children().get(originalFieldIndex);
        Integer val = ValueConverter.convertToInt(currentRow.getChild(originalFieldIndex), fieldSchema);
        if (val == null) {
            throw new NullPointerException("Column " + originalFieldIndex + " is null");
        }
        return val;
    }

    @Override
    public long getLong(String name) {
        int fieldIndex = getFieldIndex(name);
        return getLongInternal(fieldIndex);
    }

    @Override
    public long getLong(int projectedFieldIndex) {
        int originalFieldIndex = projectedFieldToOriginal[projectedFieldIndex];
        return getLongInternal(originalFieldIndex);
    }

    private long getLongInternal(int originalFieldIndex) {
        SchemaNode fieldSchema = schema.getRootNode().children().get(originalFieldIndex);
        Long val = ValueConverter.convertToLong(currentRow.getChild(originalFieldIndex), fieldSchema);
        if (val == null) {
            throw new NullPointerException("Column " + originalFieldIndex + " is null");
        }
        return val;
    }

    @Override
    public float getFloat(String name) {
        int fieldIndex = getFieldIndex(name);
        return getFloatInternal(fieldIndex);
    }

    @Override
    public float getFloat(int projectedFieldIndex) {
        int originalFieldIndex = projectedFieldToOriginal[projectedFieldIndex];
        return getFloatInternal(originalFieldIndex);
    }

    private float getFloatInternal(int originalFieldIndex) {
        SchemaNode fieldSchema = schema.getRootNode().children().get(originalFieldIndex);
        Float val = ValueConverter.convertToFloat(currentRow.getChild(originalFieldIndex), fieldSchema);
        if (val == null) {
            throw new NullPointerException("Column " + originalFieldIndex + " is null");
        }
        return val;
    }

    @Override
    public double getDouble(String name) {
        int fieldIndex = getFieldIndex(name);
        return getDoubleInternal(fieldIndex);
    }

    @Override
    public double getDouble(int projectedFieldIndex) {
        int originalFieldIndex = projectedFieldToOriginal[projectedFieldIndex];
        return getDoubleInternal(originalFieldIndex);
    }

    private double getDoubleInternal(int originalFieldIndex) {
        SchemaNode fieldSchema = schema.getRootNode().children().get(originalFieldIndex);
        Double val = ValueConverter.convertToDouble(currentRow.getChild(originalFieldIndex), fieldSchema);
        if (val == null) {
            throw new NullPointerException("Column " + originalFieldIndex + " is null");
        }
        return val;
    }

    @Override
    public boolean getBoolean(String name) {
        int fieldIndex = getFieldIndex(name);
        return getBooleanInternal(fieldIndex);
    }

    @Override
    public boolean getBoolean(int projectedFieldIndex) {
        int originalFieldIndex = projectedFieldToOriginal[projectedFieldIndex];
        return getBooleanInternal(originalFieldIndex);
    }

    private boolean getBooleanInternal(int originalFieldIndex) {
        SchemaNode fieldSchema = schema.getRootNode().children().get(originalFieldIndex);
        Boolean val = ValueConverter.convertToBoolean(currentRow.getChild(originalFieldIndex), fieldSchema);
        if (val == null) {
            throw new NullPointerException("Column " + originalFieldIndex + " is null");
        }
        return val;
    }

    // ==================== Object Type Accessors ====================

    @Override
    public String getString(String name) {
        int fieldIndex = getFieldIndex(name);
        return getStringInternal(fieldIndex);
    }

    @Override
    public String getString(int projectedFieldIndex) {
        int originalFieldIndex = projectedFieldToOriginal[projectedFieldIndex];
        return getStringInternal(originalFieldIndex);
    }

    private String getStringInternal(int originalFieldIndex) {
        SchemaNode fieldSchema = schema.getRootNode().children().get(originalFieldIndex);
        return ValueConverter.convertToString(currentRow.getChild(originalFieldIndex), fieldSchema);
    }

    @Override
    public byte[] getBinary(String name) {
        int fieldIndex = getFieldIndex(name);
        return getBinaryInternal(fieldIndex);
    }

    @Override
    public byte[] getBinary(int projectedFieldIndex) {
        int originalFieldIndex = projectedFieldToOriginal[projectedFieldIndex];
        return getBinaryInternal(originalFieldIndex);
    }

    private byte[] getBinaryInternal(int originalFieldIndex) {
        SchemaNode fieldSchema = schema.getRootNode().children().get(originalFieldIndex);
        return ValueConverter.convertToBinary(currentRow.getChild(originalFieldIndex), fieldSchema);
    }

    @Override
    public LocalDate getDate(String name) {
        int fieldIndex = getFieldIndex(name);
        return getDateInternal(fieldIndex);
    }

    @Override
    public LocalDate getDate(int projectedFieldIndex) {
        int originalFieldIndex = projectedFieldToOriginal[projectedFieldIndex];
        return getDateInternal(originalFieldIndex);
    }

    private LocalDate getDateInternal(int originalFieldIndex) {
        SchemaNode fieldSchema = schema.getRootNode().children().get(originalFieldIndex);
        return ValueConverter.convertToDate(currentRow.getChild(originalFieldIndex), fieldSchema);
    }

    @Override
    public LocalTime getTime(String name) {
        int fieldIndex = getFieldIndex(name);
        return getTimeInternal(fieldIndex);
    }

    @Override
    public LocalTime getTime(int projectedFieldIndex) {
        int originalFieldIndex = projectedFieldToOriginal[projectedFieldIndex];
        return getTimeInternal(originalFieldIndex);
    }

    private LocalTime getTimeInternal(int originalFieldIndex) {
        SchemaNode fieldSchema = schema.getRootNode().children().get(originalFieldIndex);
        return ValueConverter.convertToTime(currentRow.getChild(originalFieldIndex), fieldSchema);
    }

    @Override
    public Instant getTimestamp(String name) {
        int fieldIndex = getFieldIndex(name);
        return getTimestampInternal(fieldIndex);
    }

    @Override
    public Instant getTimestamp(int projectedFieldIndex) {
        int originalFieldIndex = projectedFieldToOriginal[projectedFieldIndex];
        return getTimestampInternal(originalFieldIndex);
    }

    private Instant getTimestampInternal(int originalFieldIndex) {
        SchemaNode fieldSchema = schema.getRootNode().children().get(originalFieldIndex);
        return ValueConverter.convertToTimestamp(currentRow.getChild(originalFieldIndex), fieldSchema);
    }

    @Override
    public BigDecimal getDecimal(String name) {
        int fieldIndex = getFieldIndex(name);
        return getDecimalInternal(fieldIndex);
    }

    @Override
    public BigDecimal getDecimal(int projectedFieldIndex) {
        int originalFieldIndex = projectedFieldToOriginal[projectedFieldIndex];
        return getDecimalInternal(originalFieldIndex);
    }

    private BigDecimal getDecimalInternal(int originalFieldIndex) {
        SchemaNode fieldSchema = schema.getRootNode().children().get(originalFieldIndex);
        return ValueConverter.convertToDecimal(currentRow.getChild(originalFieldIndex), fieldSchema);
    }

    @Override
    public UUID getUuid(String name) {
        int fieldIndex = getFieldIndex(name);
        return getUuidInternal(fieldIndex);
    }

    @Override
    public UUID getUuid(int projectedFieldIndex) {
        int originalFieldIndex = projectedFieldToOriginal[projectedFieldIndex];
        return getUuidInternal(originalFieldIndex);
    }

    private UUID getUuidInternal(int originalFieldIndex) {
        SchemaNode fieldSchema = schema.getRootNode().children().get(originalFieldIndex);
        return ValueConverter.convertToUuid(currentRow.getChild(originalFieldIndex), fieldSchema);
    }

    // ==================== Nested Type Accessors ====================

    @Override
    public PqStruct getStruct(String name) {
        int originalIndex = getFieldIndex(name);
        SchemaNode fieldSchema = schema.getRootNode().children().get(originalIndex);
        return ValueConverter.convertToStruct(currentRow.getChild(originalIndex), fieldSchema);
    }

    @Override
    public PqIntList getListOfInts(String name) {
        int originalIndex = getFieldIndex(name);
        SchemaNode fieldSchema = schema.getRootNode().children().get(originalIndex);
        return ValueConverter.convertToIntList(currentRow.getChild(originalIndex), fieldSchema);
    }

    @Override
    public PqLongList getListOfLongs(String name) {
        int originalIndex = getFieldIndex(name);
        SchemaNode fieldSchema = schema.getRootNode().children().get(originalIndex);
        return ValueConverter.convertToLongList(currentRow.getChild(originalIndex), fieldSchema);
    }

    @Override
    public PqDoubleList getListOfDoubles(String name) {
        int originalIndex = getFieldIndex(name);
        SchemaNode fieldSchema = schema.getRootNode().children().get(originalIndex);
        return ValueConverter.convertToDoubleList(currentRow.getChild(originalIndex), fieldSchema);
    }

    @Override
    public PqList getList(String name) {
        int originalIndex = getFieldIndex(name);
        SchemaNode fieldSchema = schema.getRootNode().children().get(originalIndex);
        return ValueConverter.convertToList(currentRow.getChild(originalIndex), fieldSchema);
    }

    @Override
    public PqMap getMap(String name) {
        int originalIndex = getFieldIndex(name);
        SchemaNode fieldSchema = schema.getRootNode().children().get(originalIndex);
        return ValueConverter.convertToMap(currentRow.getChild(originalIndex), fieldSchema);
    }

    // ==================== Generic Fallback ====================

    @Override
    public Object getValue(String name) {
        int originalIndex = getFieldIndex(name);
        return currentRow.getChild(originalIndex);
    }

    // ==================== Metadata ====================

    @Override
    public boolean isNull(String name) {
        int originalIndex = getFieldIndex(name);
        return currentRow.getChild(originalIndex) == null;
    }

    @Override
    public boolean isNull(int projectedFieldIndex) {
        int originalFieldIndex = projectedFieldToOriginal[projectedFieldIndex];
        return currentRow.getChild(originalFieldIndex) == null;
    }

    @Override
    public int getFieldCount() {
        return projectedSchema.getProjectedFieldIndices().length;
    }

    @Override
    public String getFieldName(int projectedFieldIndex) {
        int originalFieldIndex = projectedSchema.getProjectedFieldIndices()[projectedFieldIndex];
        return schema.getRootNode().children().get(originalFieldIndex).name();
    }
}
