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

    private NestedBatchIndex batchIndex;
    private int rowIndex;

    // Cached value indices: precomputed per row to avoid repeated offset lookups.
    // cachedValueIndex[projCol] = offsets[projCol][rowIndex] (or rowIndex if no offsets)
    private int[] cachedValueIndex;

    // Direct-access cache for by-index accessors (built once in constructor, arrays refreshed per batch)
    private final TopLevelFieldMap.FieldDesc[] fieldDescs;     // projFieldIdx → FieldDesc
    private final int[] fieldToProjCol;                        // projFieldIdx → projectedColumnIdx (-1 for non-primitives)
    private Object[] fieldValueArrays;                         // projFieldIdx → raw value array (int[], long[], etc.)
    private BitSet[] fieldElementNulls;                        // projFieldIdx → element null BitSet

    public NestedBatchDataView(FileSchema schema, ProjectedSchema projectedSchema) {
        this.schema = schema;
        this.projectedSchema = projectedSchema;
        this.fieldMap = TopLevelFieldMap.build(schema, projectedSchema);
        this.projectedFieldToOriginal = projectedSchema.getProjectedFieldIndices().clone();
        this.cachedValueIndex = new int[projectedSchema.getProjectedColumnCount()];

        // Build direct-access mappings from projected field index
        int fieldCount = projectedFieldToOriginal.length;
        this.fieldDescs = new TopLevelFieldMap.FieldDesc[fieldCount];
        this.fieldToProjCol = new int[fieldCount];
        this.fieldValueArrays = new Object[fieldCount];
        this.fieldElementNulls = new BitSet[fieldCount];
        for (int f = 0; f < fieldCount; f++) {
            TopLevelFieldMap.FieldDesc desc = fieldMap.getByOriginalIndex(projectedFieldToOriginal[f]);
            fieldDescs[f] = desc;
            if (desc instanceof TopLevelFieldMap.FieldDesc.Primitive p) {
                fieldToProjCol[f] = p.projectedCol();
            } else {
                fieldToProjCol[f] = -1;
            }
        }
    }

    @Override
    public void setBatchData(TypedColumnData[] newColumnData) {
        NestedColumnData[] nested = new NestedColumnData[newColumnData.length];
        for (int i = 0; i < newColumnData.length; i++) {
            nested[i] = (NestedColumnData) newColumnData[i];
        }
        this.batchIndex = NestedBatchIndex.build(nested, schema, projectedSchema, fieldMap);
        cacheFieldArrays();
    }

    /**
     * Install batch data from pre-indexed columns where index computation
     * was already done in parallel by the column futures.
     */
    public void setBatchData(IndexedNestedColumnData[] indexedData) {
        this.batchIndex = NestedBatchIndex.buildFromIndexed(indexedData, schema, projectedSchema, fieldMap);
        cacheFieldArrays();
    }

    @Override
    public void setRowIndex(int rowIndex) {
        this.rowIndex = rowIndex;
        int[][] offsets = batchIndex.offsets;
        for (int col = 0; col < cachedValueIndex.length; col++) {
            int[] recordOffsets = offsets[col];
            cachedValueIndex[col] = recordOffsets != null ? recordOffsets[rowIndex] : rowIndex;
        }
    }

    // ==================== Index Lookup ====================

    private TopLevelFieldMap.FieldDesc lookupField(String name) {
        return fieldMap.getByName(name);
    }

    private TopLevelFieldMap.FieldDesc.Primitive lookupPrimitive(String name) {
        TopLevelFieldMap.FieldDesc desc = lookupField(name);
        if (!(desc instanceof TopLevelFieldMap.FieldDesc.Primitive prim)) {
            throw new IllegalArgumentException("Field '" + name + "' is not a primitive type");
        }
        return prim;
    }

    private TopLevelFieldMap.FieldDesc.Primitive lookupPrimitiveByIndex(int projectedIndex) {
        if (!(fieldDescs[projectedIndex] instanceof TopLevelFieldMap.FieldDesc.Primitive prim)) {
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
        int projCol = fieldToProjCol[projectedIndex];
        if (projCol >= 0) {
            int valueIdx = cachedValueIndex[projCol];
            BitSet nulls = fieldElementNulls[projectedIndex];
            return nulls != null && nulls.get(valueIdx);
        }
        return isFieldNull(fieldDescs[projectedIndex]);
    }

    private boolean isFieldNull(TopLevelFieldMap.FieldDesc desc) {
        return switch (desc) {
            case TopLevelFieldMap.FieldDesc.Primitive p -> {
                int valueIdx = cachedValueIndex[p.projectedCol()];
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
        int projCol = structDesc.firstPrimitiveCol();
        if (projCol < 0) {
            return false;
        }
        int valueIdx = cachedValueIndex[projCol];
        int defLevel = batchIndex.columns[projCol].getDefLevel(valueIdx);
        return defLevel < structDesc.schema().maxDefinitionLevel();
    }

    // ==================== Primitive Type Accessors (by name) ====================

    @Override
    public int getInt(String name) {
        TopLevelFieldMap.FieldDesc.Primitive p = lookupPrimitive(name);
        int projCol = p.projectedCol();
        int valueIdx = cachedValueIndex[projCol];
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            throw new NullPointerException("Column '" + name + "' is null");
        }
        return ((NestedColumnData.IntColumn) batchIndex.columns[projCol]).get(valueIdx);
    }

    @Override
    public long getLong(String name) {
        TopLevelFieldMap.FieldDesc.Primitive p = lookupPrimitive(name);
        int projCol = p.projectedCol();
        int valueIdx = cachedValueIndex[projCol];
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            throw new NullPointerException("Column '" + name + "' is null");
        }
        return ((NestedColumnData.LongColumn) batchIndex.columns[projCol]).get(valueIdx);
    }

    @Override
    public float getFloat(String name) {
        TopLevelFieldMap.FieldDesc.Primitive p = lookupPrimitive(name);
        int projCol = p.projectedCol();
        int valueIdx = cachedValueIndex[projCol];
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            throw new NullPointerException("Column '" + name + "' is null");
        }
        return ((NestedColumnData.FloatColumn) batchIndex.columns[projCol]).get(valueIdx);
    }

    @Override
    public double getDouble(String name) {
        TopLevelFieldMap.FieldDesc.Primitive p = lookupPrimitive(name);
        int projCol = p.projectedCol();
        int valueIdx = cachedValueIndex[projCol];
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            throw new NullPointerException("Column '" + name + "' is null");
        }
        return ((NestedColumnData.DoubleColumn) batchIndex.columns[projCol]).get(valueIdx);
    }

    @Override
    public boolean getBoolean(String name) {
        TopLevelFieldMap.FieldDesc.Primitive p = lookupPrimitive(name);
        int projCol = p.projectedCol();
        int valueIdx = cachedValueIndex[projCol];
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            throw new NullPointerException("Column '" + name + "' is null");
        }
        return ((NestedColumnData.BooleanColumn) batchIndex.columns[projCol]).get(valueIdx);
    }

    // ==================== Primitive Type Accessors (by index) ====================

    @Override
    public int getInt(int projectedIndex) {
        int valueIdx = cachedValueIndex[fieldToProjCol[projectedIndex]];
        BitSet nulls = fieldElementNulls[projectedIndex];
        if (nulls != null && nulls.get(valueIdx)) {
            throw new NullPointerException("Column " + projectedIndex + " is null");
        }
        return ((int[]) fieldValueArrays[projectedIndex])[valueIdx];
    }

    @Override
    public long getLong(int projectedIndex) {
        int valueIdx = cachedValueIndex[fieldToProjCol[projectedIndex]];
        BitSet nulls = fieldElementNulls[projectedIndex];
        if (nulls != null && nulls.get(valueIdx)) {
            throw new NullPointerException("Column " + projectedIndex + " is null");
        }
        return ((long[]) fieldValueArrays[projectedIndex])[valueIdx];
    }

    @Override
    public float getFloat(int projectedIndex) {
        int valueIdx = cachedValueIndex[fieldToProjCol[projectedIndex]];
        BitSet nulls = fieldElementNulls[projectedIndex];
        if (nulls != null && nulls.get(valueIdx)) {
            throw new NullPointerException("Column " + projectedIndex + " is null");
        }
        return ((float[]) fieldValueArrays[projectedIndex])[valueIdx];
    }

    @Override
    public double getDouble(int projectedIndex) {
        int valueIdx = cachedValueIndex[fieldToProjCol[projectedIndex]];
        BitSet nulls = fieldElementNulls[projectedIndex];
        if (nulls != null && nulls.get(valueIdx)) {
            throw new NullPointerException("Column " + projectedIndex + " is null");
        }
        return ((double[]) fieldValueArrays[projectedIndex])[valueIdx];
    }

    @Override
    public boolean getBoolean(int projectedIndex) {
        int valueIdx = cachedValueIndex[fieldToProjCol[projectedIndex]];
        BitSet nulls = fieldElementNulls[projectedIndex];
        if (nulls != null && nulls.get(valueIdx)) {
            throw new NullPointerException("Column " + projectedIndex + " is null");
        }
        return ((boolean[]) fieldValueArrays[projectedIndex])[valueIdx];
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
        int valueIdx = cachedValueIndex[fieldToProjCol[projectedIndex]];
        BitSet nulls = fieldElementNulls[projectedIndex];
        if (nulls != null && nulls.get(valueIdx)) {
            return null;
        }
        byte[] raw = ((byte[][]) fieldValueArrays[projectedIndex])[valueIdx];
        return new String(raw, StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getBinary(int projectedIndex) {
        int valueIdx = cachedValueIndex[fieldToProjCol[projectedIndex]];
        BitSet nulls = fieldElementNulls[projectedIndex];
        if (nulls != null && nulls.get(valueIdx)) {
            return null;
        }
        return ((byte[][]) fieldValueArrays[projectedIndex])[valueIdx];
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
        if (!(fieldDescs[projectedIndex] instanceof TopLevelFieldMap.FieldDesc.Struct structDesc)) {
            throw new IllegalArgumentException("Field at index " + projectedIndex + " is not a struct");
        }
        if (isStructNull(structDesc)) {
            return null;
        }
        return new PqStructImpl(batchIndex, structDesc, rowIndex);
    }

    @Override
    public PqIntList getListOfInts(int projectedIndex) {
        return createIntList((TopLevelFieldMap.FieldDesc.ListOf) fieldDescs[projectedIndex]);
    }

    @Override
    public PqLongList getListOfLongs(int projectedIndex) {
        return createLongList((TopLevelFieldMap.FieldDesc.ListOf) fieldDescs[projectedIndex]);
    }

    @Override
    public PqDoubleList getListOfDoubles(int projectedIndex) {
        return createDoubleList((TopLevelFieldMap.FieldDesc.ListOf) fieldDescs[projectedIndex]);
    }

    @Override
    public PqList getList(int projectedIndex) {
        return createList((TopLevelFieldMap.FieldDesc.ListOf) fieldDescs[projectedIndex]);
    }

    @Override
    public PqMap getMap(int projectedIndex) {
        if (!(fieldDescs[projectedIndex] instanceof TopLevelFieldMap.FieldDesc.MapOf mapDesc)) {
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
        return readRawValue(fieldDescs[projectedIndex]);
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

    /**
     * Populate per-field cached value arrays and null BitSets from the current batch.
     * Called once per setBatchData() to enable direct-access by-index primitive accessors.
     */
    private void cacheFieldArrays() {
        for (int f = 0; f < fieldToProjCol.length; f++) {
            int projCol = fieldToProjCol[f];
            if (projCol >= 0) {
                fieldValueArrays[f] = extractValueArray(batchIndex.columns[projCol]);
                fieldElementNulls[f] = batchIndex.elementNulls[projCol];
            }
        }
    }

    private static Object extractValueArray(NestedColumnData data) {
        return switch (data) {
            case NestedColumnData.IntColumn ic -> ic.values();
            case NestedColumnData.LongColumn lc -> lc.values();
            case NestedColumnData.FloatColumn fc -> fc.values();
            case NestedColumnData.DoubleColumn dc -> dc.values();
            case NestedColumnData.BooleanColumn bc -> bc.values();
            case NestedColumnData.ByteArrayColumn bac -> bac.values();
        };
    }

    private String getString(TopLevelFieldMap.FieldDesc.Primitive p) {
        int projCol = p.projectedCol();
        int valueIdx = cachedValueIndex[projCol];
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            return null;
        }
        byte[] raw = ((NestedColumnData.ByteArrayColumn) batchIndex.columns[projCol]).get(valueIdx);
        return new String(raw, StandardCharsets.UTF_8);
    }

    private byte[] getBinary(TopLevelFieldMap.FieldDesc.Primitive p) {
        int projCol = p.projectedCol();
        int valueIdx = cachedValueIndex[projCol];
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            return null;
        }
        return ((NestedColumnData.ByteArrayColumn) batchIndex.columns[projCol]).get(valueIdx);
    }

    private <T> T readLogicalType(TopLevelFieldMap.FieldDesc.Primitive p,
                                  Class<? extends LogicalType> expectedLogicalType,
                                  Class<T> resultClass) {
        int projCol = p.projectedCol();
        int valueIdx = cachedValueIndex[projCol];
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            return null;
        }
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
                int valueIdx = cachedValueIndex[p.projectedCol()];
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
