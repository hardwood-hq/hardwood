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
import dev.hardwood.internal.variant.PqVariantImpl;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.row.PqDoubleList;
import dev.hardwood.row.PqIntList;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqLongList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

/// Batch data view for nested schemas.
///
/// Uses pre-computed [NestedBatchIndex] and flyweight cursor objects
/// to navigate directly over column arrays without per-row tree assembly.
public final class NestedBatchDataView {

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

    /// Install batch data from [NestedBatch] objects whose index fields
    /// have been pre-computed by the drain thread.
    public void setBatchData(NestedBatch[] batches, ColumnSchema[] columnSchemas) {
        this.batchIndex = NestedBatchIndex.buildFromBatches(
                batches, columnSchemas, schema, projectedSchema, fieldMap);
        cacheFieldArrays();
    }

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

    public boolean isNull(String name) {
        TopLevelFieldMap.FieldDesc desc = lookupField(name);
        return isFieldNull(desc);
    }

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
            case TopLevelFieldMap.FieldDesc.Variant v -> isVariantNull(v);
        };
    }

    private boolean isVariantNull(TopLevelFieldMap.FieldDesc.Variant desc) {
        // Variant nullness is determined by the def level of one of its binary
        // children — pick whichever is projected.
        int col = desc.metadataCol() >= 0 ? desc.metadataCol() : desc.valueCol();
        if (col < 0) {
            return true;
        }
        int valueIdx = cachedValueIndex[col];
        int defLevel = batchIndex.getDefLevel(col, valueIdx);
        return defLevel < desc.nullDefLevel();
    }

    private boolean isStructNull(TopLevelFieldMap.FieldDesc.Struct structDesc) {
        int primCol = structDesc.firstPrimitiveCol();
        if (primCol >= 0) {
            int valueIdx = cachedValueIndex[primCol];
            int defLevel = batchIndex.getDefLevel(primCol, valueIdx);
            return defLevel < structDesc.schema().maxDefinitionLevel();
        }
        // Top-level struct with no direct primitive child: use the first leaf at
        // any depth. `cachedValueIndex` already stores that column's per-row leaf
        // position via the column's record offsets, so no further translation is
        // needed for a top-level row.
        int leafCol = structDesc.firstLeafProjCol();
        if (leafCol < 0) {
            return false;
        }
        int valueIdx = cachedValueIndex[leafCol];
        int defLevel = batchIndex.getDefLevel(leafCol, valueIdx);
        return defLevel < structDesc.schema().maxDefinitionLevel();
    }

    // ==================== Primitive Type Accessors (by name) ====================

    public int getInt(String name) {
        TopLevelFieldMap.FieldDesc.Primitive p = lookupPrimitive(name);
        int projCol = p.projectedCol();
        int valueIdx = cachedValueIndex[projCol];
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            throw new NullPointerException("Column '" + name + "' is null");
        }
        return ((int[]) batchIndex.valueArrays[projCol])[valueIdx];
    }

    public long getLong(String name) {
        TopLevelFieldMap.FieldDesc.Primitive p = lookupPrimitive(name);
        int projCol = p.projectedCol();
        int valueIdx = cachedValueIndex[projCol];
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            throw new NullPointerException("Column '" + name + "' is null");
        }
        return ((long[]) batchIndex.valueArrays[projCol])[valueIdx];
    }

    public float getFloat(String name) {
        TopLevelFieldMap.FieldDesc.Primitive p = lookupPrimitive(name);
        int projCol = p.projectedCol();
        int valueIdx = cachedValueIndex[projCol];
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            throw new NullPointerException("Column '" + name + "' is null");
        }
        return ((float[]) batchIndex.valueArrays[projCol])[valueIdx];
    }

    public double getDouble(String name) {
        TopLevelFieldMap.FieldDesc.Primitive p = lookupPrimitive(name);
        int projCol = p.projectedCol();
        int valueIdx = cachedValueIndex[projCol];
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            throw new NullPointerException("Column '" + name + "' is null");
        }
        return ((double[]) batchIndex.valueArrays[projCol])[valueIdx];
    }

    public boolean getBoolean(String name) {
        TopLevelFieldMap.FieldDesc.Primitive p = lookupPrimitive(name);
        int projCol = p.projectedCol();
        int valueIdx = cachedValueIndex[projCol];
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            throw new NullPointerException("Column '" + name + "' is null");
        }
        return ((boolean[]) batchIndex.valueArrays[projCol])[valueIdx];
    }

    // ==================== Primitive Type Accessors (by index) ====================

    public int getInt(int projectedIndex) {
        int valueIdx = cachedValueIndex[fieldToProjCol[projectedIndex]];
        BitSet nulls = fieldElementNulls[projectedIndex];
        if (nulls != null && nulls.get(valueIdx)) {
            throw new NullPointerException("Column " + projectedIndex + " is null");
        }
        return ((int[]) fieldValueArrays[projectedIndex])[valueIdx];
    }

    public long getLong(int projectedIndex) {
        int valueIdx = cachedValueIndex[fieldToProjCol[projectedIndex]];
        BitSet nulls = fieldElementNulls[projectedIndex];
        if (nulls != null && nulls.get(valueIdx)) {
            throw new NullPointerException("Column " + projectedIndex + " is null");
        }
        return ((long[]) fieldValueArrays[projectedIndex])[valueIdx];
    }

    public float getFloat(int projectedIndex) {
        int valueIdx = cachedValueIndex[fieldToProjCol[projectedIndex]];
        BitSet nulls = fieldElementNulls[projectedIndex];
        if (nulls != null && nulls.get(valueIdx)) {
            throw new NullPointerException("Column " + projectedIndex + " is null");
        }
        return ((float[]) fieldValueArrays[projectedIndex])[valueIdx];
    }

    public double getDouble(int projectedIndex) {
        int valueIdx = cachedValueIndex[fieldToProjCol[projectedIndex]];
        BitSet nulls = fieldElementNulls[projectedIndex];
        if (nulls != null && nulls.get(valueIdx)) {
            throw new NullPointerException("Column " + projectedIndex + " is null");
        }
        return ((double[]) fieldValueArrays[projectedIndex])[valueIdx];
    }

    public boolean getBoolean(int projectedIndex) {
        int valueIdx = cachedValueIndex[fieldToProjCol[projectedIndex]];
        BitSet nulls = fieldElementNulls[projectedIndex];
        if (nulls != null && nulls.get(valueIdx)) {
            throw new NullPointerException("Column " + projectedIndex + " is null");
        }
        return ((boolean[]) fieldValueArrays[projectedIndex])[valueIdx];
    }

    // ==================== Object Type Accessors (by name) ====================

    public String getString(String name) {
        return getString(lookupPrimitive(name));
    }

    public byte[] getBinary(String name) {
        return getBinary(lookupPrimitive(name));
    }

    public LocalDate getDate(String name) {
        return readLogicalType(lookupPrimitive(name), LogicalType.DateType.class, LocalDate.class);
    }

    public LocalTime getTime(String name) {
        return readLogicalType(lookupPrimitive(name), LogicalType.TimeType.class, LocalTime.class);
    }

    public Instant getTimestamp(String name) {
        return readLogicalType(lookupPrimitive(name), LogicalType.TimestampType.class, Instant.class);
    }

    public BigDecimal getDecimal(String name) {
        return readLogicalType(lookupPrimitive(name), LogicalType.DecimalType.class, BigDecimal.class);
    }

    public UUID getUuid(String name) {
        return readLogicalType(lookupPrimitive(name), LogicalType.UuidType.class, UUID.class);
    }

    // ==================== Object Type Accessors (by index) ====================

    public String getString(int projectedIndex) {
        int valueIdx = cachedValueIndex[fieldToProjCol[projectedIndex]];
        BitSet nulls = fieldElementNulls[projectedIndex];
        if (nulls != null && nulls.get(valueIdx)) {
            return null;
        }
        byte[] raw = ((byte[][]) fieldValueArrays[projectedIndex])[valueIdx];
        return new String(raw, StandardCharsets.UTF_8);
    }

    public byte[] getBinary(int projectedIndex) {
        int valueIdx = cachedValueIndex[fieldToProjCol[projectedIndex]];
        BitSet nulls = fieldElementNulls[projectedIndex];
        if (nulls != null && nulls.get(valueIdx)) {
            return null;
        }
        return ((byte[][]) fieldValueArrays[projectedIndex])[valueIdx];
    }

    public LocalDate getDate(int projectedIndex) {
        return readLogicalType(lookupPrimitiveByIndex(projectedIndex), LogicalType.DateType.class, LocalDate.class);
    }

    public LocalTime getTime(int projectedIndex) {
        return readLogicalType(lookupPrimitiveByIndex(projectedIndex), LogicalType.TimeType.class, LocalTime.class);
    }

    public Instant getTimestamp(int projectedIndex) {
        return readLogicalType(lookupPrimitiveByIndex(projectedIndex), LogicalType.TimestampType.class, Instant.class);
    }

    public BigDecimal getDecimal(int projectedIndex) {
        return readLogicalType(lookupPrimitiveByIndex(projectedIndex), LogicalType.DecimalType.class, BigDecimal.class);
    }

    public UUID getUuid(int projectedIndex) {
        return readLogicalType(lookupPrimitiveByIndex(projectedIndex), LogicalType.UuidType.class, UUID.class);
    }

    // ==================== Nested Type Accessors (by name) ====================

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

    public PqIntList getListOfInts(String name) {
        TopLevelFieldMap.FieldDesc.ListOf listDesc = lookupList(name);
        return createIntList(listDesc);
    }

    public PqLongList getListOfLongs(String name) {
        TopLevelFieldMap.FieldDesc.ListOf listDesc = lookupList(name);
        return createLongList(listDesc);
    }

    public PqDoubleList getListOfDoubles(String name) {
        TopLevelFieldMap.FieldDesc.ListOf listDesc = lookupList(name);
        return createDoubleList(listDesc);
    }

    public PqList getList(String name) {
        TopLevelFieldMap.FieldDesc.ListOf listDesc = lookupList(name);
        return createList(listDesc);
    }

    public PqMap getMap(String name) {
        TopLevelFieldMap.FieldDesc desc = lookupField(name);
        if (!(desc instanceof TopLevelFieldMap.FieldDesc.MapOf mapDesc)) {
            throw new IllegalArgumentException("Field '" + name + "' is not a map");
        }
        return createMap(mapDesc);
    }

    // ==================== Nested Type Accessors (by index) ====================

    public PqStruct getStruct(int projectedIndex) {
        if (!(fieldDescs[projectedIndex] instanceof TopLevelFieldMap.FieldDesc.Struct structDesc)) {
            throw new IllegalArgumentException("Field at index " + projectedIndex + " is not a struct");
        }
        if (isStructNull(structDesc)) {
            return null;
        }
        return new PqStructImpl(batchIndex, structDesc, rowIndex);
    }

    public PqIntList getListOfInts(int projectedIndex) {
        return createIntList((TopLevelFieldMap.FieldDesc.ListOf) fieldDescs[projectedIndex]);
    }

    public PqLongList getListOfLongs(int projectedIndex) {
        return createLongList((TopLevelFieldMap.FieldDesc.ListOf) fieldDescs[projectedIndex]);
    }

    public PqDoubleList getListOfDoubles(int projectedIndex) {
        return createDoubleList((TopLevelFieldMap.FieldDesc.ListOf) fieldDescs[projectedIndex]);
    }

    public PqList getList(int projectedIndex) {
        return createList((TopLevelFieldMap.FieldDesc.ListOf) fieldDescs[projectedIndex]);
    }

    public PqMap getMap(int projectedIndex) {
        if (!(fieldDescs[projectedIndex] instanceof TopLevelFieldMap.FieldDesc.MapOf mapDesc)) {
            throw new IllegalArgumentException("Field at index " + projectedIndex + " is not a map");
        }
        return createMap(mapDesc);
    }

    // ==================== Generic Value Access ====================

    public Object getValue(String name) {
        TopLevelFieldMap.FieldDesc desc = lookupField(name);
        return readRawValue(desc);
    }

    public Object getValue(int projectedIndex) {
        return readRawValue(fieldDescs[projectedIndex]);
    }

    // ==================== Metadata ====================

    public int getFieldCount() {
        return projectedFieldToOriginal.length;
    }

    public String getFieldName(int projectedIndex) {
        int originalFieldIndex = projectedFieldToOriginal[projectedIndex];
        return schema.getRootNode().children().get(originalFieldIndex).name();
    }



    // ==================== Internal Helpers ====================

    /// Populate per-field cached value arrays and null BitSets from the current batch.
    /// Called once per setBatchData() to enable direct-access by-index primitive accessors.
    private void cacheFieldArrays() {
        for (int f = 0; f < fieldToProjCol.length; f++) {
            int projCol = fieldToProjCol[f];
            if (projCol >= 0) {
                fieldValueArrays[f] = batchIndex.valueArrays[projCol];
                fieldElementNulls[f] = batchIndex.elementNulls[projCol];
            }
        }
    }

    private String getString(TopLevelFieldMap.FieldDesc.Primitive p) {
        int projCol = p.projectedCol();
        int valueIdx = cachedValueIndex[projCol];
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            return null;
        }
        byte[] raw = ((byte[][]) batchIndex.valueArrays[projCol])[valueIdx];
        return new String(raw, StandardCharsets.UTF_8);
    }

    private byte[] getBinary(TopLevelFieldMap.FieldDesc.Primitive p) {
        int projCol = p.projectedCol();
        int valueIdx = cachedValueIndex[projCol];
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            return null;
        }
        return ((byte[][]) batchIndex.valueArrays[projCol])[valueIdx];
    }

    private <T> T readLogicalType(TopLevelFieldMap.FieldDesc.Primitive p,
                                  Class<? extends LogicalType> expectedLogicalType,
                                  Class<T> resultClass) {
        int projCol = p.projectedCol();
        int valueIdx = cachedValueIndex[projCol];
        if (batchIndex.isElementNull(projCol, valueIdx)) {
            return null;
        }
        Object rawValue = batchIndex.getValue(projCol, valueIdx);
        if (resultClass.isInstance(rawValue)) {
            return resultClass.cast(rawValue);
        }
        if (resultClass == Instant.class && p.schema().type() == PhysicalType.INT96) {
            return resultClass.cast(LogicalTypeConverter.int96ToInstant((byte[]) rawValue));
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
                yield batchIndex.getValue(p.projectedCol(), valueIdx);
            }
            case TopLevelFieldMap.FieldDesc.Struct s -> {
                if (isStructNull(s)) {
                    yield null;
                }
                yield new PqStructImpl(batchIndex, s, rowIndex);
            }
            case TopLevelFieldMap.FieldDesc.ListOf l -> createList(l);
            case TopLevelFieldMap.FieldDesc.MapOf m -> createMap(m);
            case TopLevelFieldMap.FieldDesc.Variant v -> createVariant(v);
        };
    }

    // ==================== Variant accessor ====================

    public PqVariant getVariant(String name) {
        TopLevelFieldMap.FieldDesc desc = lookupField(name);
        if (!(desc instanceof TopLevelFieldMap.FieldDesc.Variant variantDesc)) {
            throw new IllegalArgumentException("Field '" + name + "' is not annotated as VARIANT");
        }
        return createVariant(variantDesc);
    }

    public PqVariant getVariant(int projectedIndex) {
        if (!(fieldDescs[projectedIndex] instanceof TopLevelFieldMap.FieldDesc.Variant variantDesc)) {
            throw new IllegalArgumentException("Field at index " + projectedIndex + " is not annotated as VARIANT");
        }
        return createVariant(variantDesc);
    }

    private PqVariant createVariant(TopLevelFieldMap.FieldDesc.Variant desc) {
        if (isVariantNull(desc)) {
            return null;
        }
        if (desc.metadataCol() < 0 || desc.valueCol() < 0) {
            throw new IllegalStateException(
                    "Variant column '" + desc.schema().name() + "' requires both 'metadata' and 'value' children in the projection");
        }
        int metaIdx = cachedValueIndex[desc.metadataCol()];
        int valIdx = cachedValueIndex[desc.valueCol()];
        byte[] metadata = ((byte[][]) batchIndex.valueArrays[desc.metadataCol()])[metaIdx];
        byte[] value = ((byte[][]) batchIndex.valueArrays[desc.valueCol()])[valIdx];
        return new PqVariantImpl(metadata, value);
    }
}
