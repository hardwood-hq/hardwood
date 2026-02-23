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
import java.util.UUID;

import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.row.PqDoubleList;
import dev.hardwood.row.PqIntList;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqLongList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

/**
 * BatchDataView implementation for nested schemas.
 * <p>
 * Uses pre-computed {@link NestedBatchIndex} and flyweight cursor objects
 * to navigate directly over column arrays without per-row tree assembly.
 * </p>
 */
public final class NestedBatchDataView implements BatchDataView {

    private final FileSchema schema;
    private final ProjectedSchema projectedSchema;
    private final TopLevelFieldMap fieldMap;

    // Maps projected field index -> original field index in root children
    private final int[] projectedFieldToOriginal;
    // Maps original field index -> projected field index (-1 if not projected)
    private final int[] originalFieldToProjected;

    private NestedBatchIndex batchIndex;
    private int rowIndex;

    public NestedBatchDataView(FileSchema schema, ProjectedSchema projectedSchema) {
        this.schema = schema;
        this.projectedSchema = projectedSchema;
        this.fieldMap = TopLevelFieldMap.build(schema, projectedSchema);
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
        this.batchIndex = NestedBatchIndex.build(nested, schema, projectedSchema, fieldMap);
    }

    @Override
    public void setRowIndex(int rowIndex) {
        this.rowIndex = rowIndex;
    }

    // ==================== Index Lookup ====================

    private TopLevelFieldMap.FieldDesc lookupField(String name) {
        return fieldMap.getByName(name);
    }

    private TopLevelFieldMap.FieldDesc lookupFieldByIndex(int projectedIndex) {
        int originalFieldIndex = projectedFieldToOriginal[projectedIndex];
        TopLevelFieldMap.FieldDesc desc = fieldMap.getByOriginalIndex(originalFieldIndex);
        if (desc == null) {
            throw new IllegalArgumentException("Column not in projection: index " + projectedIndex);
        }
        return desc;
    }

    private TopLevelFieldMap.FieldDesc.Primitive lookupPrimitive(String name) {
        TopLevelFieldMap.FieldDesc desc = lookupField(name);
        if (!(desc instanceof TopLevelFieldMap.FieldDesc.Primitive prim)) {
            throw new IllegalArgumentException("Field '" + name + "' is not a primitive type");
        }
        return prim;
    }

    private TopLevelFieldMap.FieldDesc.Primitive lookupPrimitiveByIndex(int projectedIndex) {
        TopLevelFieldMap.FieldDesc desc = lookupFieldByIndex(projectedIndex);
        if (!(desc instanceof TopLevelFieldMap.FieldDesc.Primitive prim)) {
            throw new IllegalArgumentException("Field at index " + projectedIndex + " is not a primitive type");
        }
        return prim;
    }

    @Override
    public boolean isNull(String name) {
        TopLevelFieldMap.FieldDesc desc = lookupField(name);
        return isFieldNull(desc);
    }

    @Override
    public boolean isNull(int projectedIndex) {
        TopLevelFieldMap.FieldDesc desc = lookupFieldByIndex(projectedIndex);
        return isFieldNull(desc);
    }

    private boolean isFieldNull(TopLevelFieldMap.FieldDesc desc) {
        return switch (desc) {
            case TopLevelFieldMap.FieldDesc.Primitive p -> {
                int valueIdx = batchIndex.getValueIndex(p.projectedCol(), rowIndex);
                yield batchIndex.isElementNull(p.projectedCol(), valueIdx);
            }
            case TopLevelFieldMap.FieldDesc.Struct s -> isStructNull(s);
            case TopLevelFieldMap.FieldDesc.ListOf l ->
                    PqListImpl.isListNull(batchIndex, l, rowIndex, -1);
            case TopLevelFieldMap.FieldDesc.MapOf m ->
                    PqMapImpl.isMapNull(batchIndex, m, rowIndex, -1);
        };
    }

    private boolean isStructNull(TopLevelFieldMap.FieldDesc.Struct structDesc) {
        for (TopLevelFieldMap.FieldDesc childDesc : structDesc.children().values()) {
            if (childDesc instanceof TopLevelFieldMap.FieldDesc.Primitive p) {
                int projCol = p.projectedCol();
                int valueIdx = batchIndex.getValueIndex(projCol, rowIndex);
                NestedColumnData data = batchIndex.columns[projCol];
                int defLevel = data.getDefLevel(valueIdx);
                int structDefLevel = structDesc.schema().maxDefinitionLevel();
                return defLevel < structDefLevel;
            }
        }
        return false;
    }

    // ==================== Primitive Type Accessors (by name) ====================

    @Override
    public int getInt(String name) {
        TopLevelFieldMap.FieldDesc.Primitive p = lookupPrimitive(name);
        int projCol = p.projectedCol();
        int valueIdx = batchIndex.getValueIndex(projCol, rowIndex);
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            throw new NullPointerException("Column '" + name + "' is null");
        }
        ValidateHelper.validateInt(p.schema());
        return ((NestedColumnData.IntColumn) batchIndex.columns[projCol]).get(valueIdx);
    }

    @Override
    public long getLong(String name) {
        TopLevelFieldMap.FieldDesc.Primitive p = lookupPrimitive(name);
        int projCol = p.projectedCol();
        int valueIdx = batchIndex.getValueIndex(projCol, rowIndex);
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            throw new NullPointerException("Column '" + name + "' is null");
        }
        ValidateHelper.validateLong(p.schema());
        return ((NestedColumnData.LongColumn) batchIndex.columns[projCol]).get(valueIdx);
    }

    @Override
    public float getFloat(String name) {
        TopLevelFieldMap.FieldDesc.Primitive p = lookupPrimitive(name);
        int projCol = p.projectedCol();
        int valueIdx = batchIndex.getValueIndex(projCol, rowIndex);
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            throw new NullPointerException("Column '" + name + "' is null");
        }
        ValidateHelper.validateFloat(p.schema());
        return ((NestedColumnData.FloatColumn) batchIndex.columns[projCol]).get(valueIdx);
    }

    @Override
    public double getDouble(String name) {
        TopLevelFieldMap.FieldDesc.Primitive p = lookupPrimitive(name);
        int projCol = p.projectedCol();
        int valueIdx = batchIndex.getValueIndex(projCol, rowIndex);
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            throw new NullPointerException("Column '" + name + "' is null");
        }
        ValidateHelper.validateDouble(p.schema());
        return ((NestedColumnData.DoubleColumn) batchIndex.columns[projCol]).get(valueIdx);
    }

    @Override
    public boolean getBoolean(String name) {
        TopLevelFieldMap.FieldDesc.Primitive p = lookupPrimitive(name);
        int projCol = p.projectedCol();
        int valueIdx = batchIndex.getValueIndex(projCol, rowIndex);
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            throw new NullPointerException("Column '" + name + "' is null");
        }
        ValidateHelper.validateBoolean(p.schema());
        return ((NestedColumnData.BooleanColumn) batchIndex.columns[projCol]).get(valueIdx);
    }

    // ==================== Primitive Type Accessors (by index) ====================

    @Override
    public int getInt(int projectedIndex) {
        TopLevelFieldMap.FieldDesc.Primitive p = lookupPrimitiveByIndex(projectedIndex);
        int projCol = p.projectedCol();
        int valueIdx = batchIndex.getValueIndex(projCol, rowIndex);
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            throw new NullPointerException("Column " + projectedIndex + " is null");
        }
        ValidateHelper.validateInt(p.schema());
        return ((NestedColumnData.IntColumn) batchIndex.columns[projCol]).get(valueIdx);
    }

    @Override
    public long getLong(int projectedIndex) {
        TopLevelFieldMap.FieldDesc.Primitive p = lookupPrimitiveByIndex(projectedIndex);
        int projCol = p.projectedCol();
        int valueIdx = batchIndex.getValueIndex(projCol, rowIndex);
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            throw new NullPointerException("Column " + projectedIndex + " is null");
        }
        ValidateHelper.validateLong(p.schema());
        return ((NestedColumnData.LongColumn) batchIndex.columns[projCol]).get(valueIdx);
    }

    @Override
    public float getFloat(int projectedIndex) {
        TopLevelFieldMap.FieldDesc.Primitive p = lookupPrimitiveByIndex(projectedIndex);
        int projCol = p.projectedCol();
        int valueIdx = batchIndex.getValueIndex(projCol, rowIndex);
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            throw new NullPointerException("Column " + projectedIndex + " is null");
        }
        ValidateHelper.validateFloat(p.schema());
        return ((NestedColumnData.FloatColumn) batchIndex.columns[projCol]).get(valueIdx);
    }

    @Override
    public double getDouble(int projectedIndex) {
        TopLevelFieldMap.FieldDesc.Primitive p = lookupPrimitiveByIndex(projectedIndex);
        int projCol = p.projectedCol();
        int valueIdx = batchIndex.getValueIndex(projCol, rowIndex);
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            throw new NullPointerException("Column " + projectedIndex + " is null");
        }
        ValidateHelper.validateDouble(p.schema());
        return ((NestedColumnData.DoubleColumn) batchIndex.columns[projCol]).get(valueIdx);
    }

    @Override
    public boolean getBoolean(int projectedIndex) {
        TopLevelFieldMap.FieldDesc.Primitive p = lookupPrimitiveByIndex(projectedIndex);
        int projCol = p.projectedCol();
        int valueIdx = batchIndex.getValueIndex(projCol, rowIndex);
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            throw new NullPointerException("Column " + projectedIndex + " is null");
        }
        ValidateHelper.validateBoolean(p.schema());
        return ((NestedColumnData.BooleanColumn) batchIndex.columns[projCol]).get(valueIdx);
    }

    // ==================== Object Type Accessors (by name) ====================

    @Override
    public String getString(String name) {
        return getString(lookupPrimitive(name));
    }

    @Override
    public byte[] getBinary(String name) {
        return getBinary(lookupPrimitive(name));
    }

    @Override
    public LocalDate getDate(String name) {
        return readLogicalType(lookupPrimitive(name), LogicalType.DateType.class, LocalDate.class);
    }

    @Override
    public LocalTime getTime(String name) {
        return readLogicalType(lookupPrimitive(name), LogicalType.TimeType.class, LocalTime.class);
    }

    @Override
    public Instant getTimestamp(String name) {
        return readLogicalType(lookupPrimitive(name), LogicalType.TimestampType.class, Instant.class);
    }

    @Override
    public BigDecimal getDecimal(String name) {
        return readLogicalType(lookupPrimitive(name), LogicalType.DecimalType.class, BigDecimal.class);
    }

    @Override
    public UUID getUuid(String name) {
        return readLogicalType(lookupPrimitive(name), LogicalType.UuidType.class, UUID.class);
    }

    // ==================== Object Type Accessors (by index) ====================

    @Override
    public String getString(int projectedIndex) {
        return getString(lookupPrimitiveByIndex(projectedIndex));
    }

    @Override
    public byte[] getBinary(int projectedIndex) {
        return getBinary(lookupPrimitiveByIndex(projectedIndex));
    }

    @Override
    public LocalDate getDate(int projectedIndex) {
        return readLogicalType(lookupPrimitiveByIndex(projectedIndex), LogicalType.DateType.class, LocalDate.class);
    }

    @Override
    public LocalTime getTime(int projectedIndex) {
        return readLogicalType(lookupPrimitiveByIndex(projectedIndex), LogicalType.TimeType.class, LocalTime.class);
    }

    @Override
    public Instant getTimestamp(int projectedIndex) {
        return readLogicalType(lookupPrimitiveByIndex(projectedIndex), LogicalType.TimestampType.class, Instant.class);
    }

    @Override
    public BigDecimal getDecimal(int projectedIndex) {
        return readLogicalType(lookupPrimitiveByIndex(projectedIndex), LogicalType.DecimalType.class, BigDecimal.class);
    }

    @Override
    public UUID getUuid(int projectedIndex) {
        return readLogicalType(lookupPrimitiveByIndex(projectedIndex), LogicalType.UuidType.class, UUID.class);
    }

    // ==================== Nested Type Accessors (by name) ====================

    @Override
    public PqStruct getStruct(String name) {
        TopLevelFieldMap.FieldDesc desc = lookupField(name);
        if (!(desc instanceof TopLevelFieldMap.FieldDesc.Struct structDesc)) {
            throw new IllegalArgumentException("Field '" + name + "' is not a struct");
        }
        if (isStructNull(structDesc)) {
            return null;
        }
        return new PqStructImpl(batchIndex, structDesc, rowIndex);
    }

    @Override
    public PqIntList getListOfInts(String name) {
        TopLevelFieldMap.FieldDesc.ListOf listDesc = lookupList(name);
        return createIntList(listDesc);
    }

    @Override
    public PqLongList getListOfLongs(String name) {
        TopLevelFieldMap.FieldDesc.ListOf listDesc = lookupList(name);
        return createLongList(listDesc);
    }

    @Override
    public PqDoubleList getListOfDoubles(String name) {
        TopLevelFieldMap.FieldDesc.ListOf listDesc = lookupList(name);
        return createDoubleList(listDesc);
    }

    @Override
    public PqList getList(String name) {
        TopLevelFieldMap.FieldDesc.ListOf listDesc = lookupList(name);
        return createList(listDesc);
    }

    @Override
    public PqMap getMap(String name) {
        TopLevelFieldMap.FieldDesc desc = lookupField(name);
        if (!(desc instanceof TopLevelFieldMap.FieldDesc.MapOf mapDesc)) {
            throw new IllegalArgumentException("Field '" + name + "' is not a map");
        }
        return createMap(mapDesc);
    }

    // ==================== Nested Type Accessors (by index) ====================

    @Override
    public PqStruct getStruct(int projectedIndex) {
        TopLevelFieldMap.FieldDesc desc = lookupFieldByIndex(projectedIndex);
        if (!(desc instanceof TopLevelFieldMap.FieldDesc.Struct structDesc)) {
            throw new IllegalArgumentException("Field at index " + projectedIndex + " is not a struct");
        }
        if (isStructNull(structDesc)) {
            return null;
        }
        return new PqStructImpl(batchIndex, structDesc, rowIndex);
    }

    @Override
    public PqIntList getListOfInts(int projectedIndex) {
        return createIntList(lookupListByIndex(projectedIndex));
    }

    @Override
    public PqLongList getListOfLongs(int projectedIndex) {
        return createLongList(lookupListByIndex(projectedIndex));
    }

    @Override
    public PqDoubleList getListOfDoubles(int projectedIndex) {
        return createDoubleList(lookupListByIndex(projectedIndex));
    }

    @Override
    public PqList getList(int projectedIndex) {
        return createList(lookupListByIndex(projectedIndex));
    }

    @Override
    public PqMap getMap(int projectedIndex) {
        TopLevelFieldMap.FieldDesc desc = lookupFieldByIndex(projectedIndex);
        if (!(desc instanceof TopLevelFieldMap.FieldDesc.MapOf mapDesc)) {
            throw new IllegalArgumentException("Field at index " + projectedIndex + " is not a map");
        }
        return createMap(mapDesc);
    }

    // ==================== Generic Value Access ====================

    @Override
    public Object getValue(String name) {
        TopLevelFieldMap.FieldDesc desc = lookupField(name);
        return readRawValue(desc);
    }

    @Override
    public Object getValue(int projectedIndex) {
        TopLevelFieldMap.FieldDesc desc = lookupFieldByIndex(projectedIndex);
        return readRawValue(desc);
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

    // ==================== Internal Helpers ====================

    private String getString(TopLevelFieldMap.FieldDesc.Primitive p) {
        int projCol = p.projectedCol();
        int valueIdx = batchIndex.getValueIndex(projCol, rowIndex);
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            return null;
        }
        ValidateHelper.validateString(p.schema());
        byte[] raw = ((NestedColumnData.ByteArrayColumn) batchIndex.columns[projCol]).get(valueIdx);
        return new String(raw, StandardCharsets.UTF_8);
    }

    private byte[] getBinary(TopLevelFieldMap.FieldDesc.Primitive p) {
        int projCol = p.projectedCol();
        int valueIdx = batchIndex.getValueIndex(projCol, rowIndex);
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            return null;
        }
        ValidateHelper.validateBinary(p.schema());
        return ((NestedColumnData.ByteArrayColumn) batchIndex.columns[projCol]).get(valueIdx);
    }

    private <T> T readLogicalType(TopLevelFieldMap.FieldDesc.Primitive p,
                                  Class<? extends LogicalType> expectedLogicalType,
                                  Class<T> resultClass) {
        int projCol = p.projectedCol();
        int valueIdx = batchIndex.getValueIndex(projCol, rowIndex);
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            return null;
        }
        ValidateHelper.validateLogicalType(p.schema(), expectedLogicalType);
        Object rawValue = batchIndex.columns[projCol].getValue(valueIdx);
        if (resultClass.isInstance(rawValue)) {
            return resultClass.cast(rawValue);
        }
        Object converted = LogicalTypeConverter.convert(rawValue, p.schema().type(), p.schema().logicalType());
        return resultClass.cast(converted);
    }

    private TopLevelFieldMap.FieldDesc.ListOf lookupList(String name) {
        TopLevelFieldMap.FieldDesc desc = lookupField(name);
        if (!(desc instanceof TopLevelFieldMap.FieldDesc.ListOf listDesc)) {
            throw new IllegalArgumentException("Field '" + name + "' is not a list");
        }
        return listDesc;
    }

    private TopLevelFieldMap.FieldDesc.ListOf lookupListByIndex(int projectedIndex) {
        TopLevelFieldMap.FieldDesc desc = lookupFieldByIndex(projectedIndex);
        if (!(desc instanceof TopLevelFieldMap.FieldDesc.ListOf listDesc)) {
            throw new IllegalArgumentException("Field at index " + projectedIndex + " is not a list");
        }
        return listDesc;
    }

    private PqIntList createIntList(TopLevelFieldMap.FieldDesc.ListOf listDesc) {
        return PqListImpl.createIntList(batchIndex, listDesc, rowIndex, -1);
    }

    private PqLongList createLongList(TopLevelFieldMap.FieldDesc.ListOf listDesc) {
        return PqListImpl.createLongList(batchIndex, listDesc, rowIndex, -1);
    }

    private PqDoubleList createDoubleList(TopLevelFieldMap.FieldDesc.ListOf listDesc) {
        return PqListImpl.createDoubleList(batchIndex, listDesc, rowIndex, -1);
    }

    private PqList createList(TopLevelFieldMap.FieldDesc.ListOf listDesc) {
        return PqListImpl.createGenericList(batchIndex, listDesc, rowIndex, -1);
    }

    private PqMap createMap(TopLevelFieldMap.FieldDesc.MapOf mapDesc) {
        return PqMapImpl.create(batchIndex, mapDesc, rowIndex, -1);
    }

    private Object readRawValue(TopLevelFieldMap.FieldDesc desc) {
        return switch (desc) {
            case TopLevelFieldMap.FieldDesc.Primitive p -> {
                int valueIdx = batchIndex.getValueIndex(p.projectedCol(), rowIndex);
                if (batchIndex.isElementNull(p.projectedCol(), valueIdx)) {
                    yield null;
                }
                yield batchIndex.columns[p.projectedCol()].getValue(valueIdx);
            }
            case TopLevelFieldMap.FieldDesc.Struct s -> {
                if (isStructNull(s)) {
                    yield null;
                }
                yield new PqStructImpl(batchIndex, s, rowIndex);
            }
            case TopLevelFieldMap.FieldDesc.ListOf l -> createList(l);
            case TopLevelFieldMap.FieldDesc.MapOf m -> createMap(m);
        };
    }
}
