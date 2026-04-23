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
import dev.hardwood.internal.reader.TopLevelFieldMap.FieldDesc;
import dev.hardwood.internal.reader.TopLevelFieldMap.FieldDesc.Primitive;
import dev.hardwood.internal.reader.TopLevelFieldMap.FieldDesc.Struct;
import dev.hardwood.internal.variant.PqVariantImpl;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.row.PqDoubleList;
import dev.hardwood.row.PqIntList;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqLongList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.schema.SchemaNode;

/// Flyweight [PqStruct] that navigates directly over column arrays.
///
/// Supports two modes:
///
/// - **Record mode**: resolves value position via `getValueIndex(projCol, rowIndex)`.
///       Used for top-level structs.</li>
/// - **Position mode**: uses a fixed value index directly.
///       Used for struct elements within lists/maps.</li>
final class PqStructImpl implements PqStruct {

    private final NestedBatchIndex batch;
    private final TopLevelFieldMap.FieldDesc.Struct desc;
    private final int rowIndex;     // >= 0 for record mode
    private final int valueIndex;   // >= 0 for position mode, -1 for record mode

    /// Record mode: value index resolved from batch offsets.
    PqStructImpl(NestedBatchIndex batch, TopLevelFieldMap.FieldDesc.Struct desc, int rowIndex) {
        this.batch = batch;
        this.desc = desc;
        this.rowIndex = rowIndex;
        this.valueIndex = -1;
    }

    /// Position mode: fixed value index (for struct elements within lists).
    static PqStructImpl atPosition(NestedBatchIndex batch,
                                       TopLevelFieldMap.FieldDesc.Struct desc, int valueIndex) {
        return new PqStructImpl(batch, desc, -1, valueIndex);
    }

    private PqStructImpl(NestedBatchIndex batch, TopLevelFieldMap.FieldDesc.Struct desc,
                             int rowIndex, int valueIndex) {
        this.batch = batch;
        this.desc = desc;
        this.rowIndex = rowIndex;
        this.valueIndex = valueIndex;
    }

    private int resolveValueIndex(int projCol) {
        return valueIndex >= 0 ? valueIndex : batch.getValueIndex(projCol, rowIndex);
    }

    // ==================== Primitive Types ====================

    @Override
    public int getInt(String name) {
        TopLevelFieldMap.FieldDesc.Primitive child = lookupPrimitive(name);
        int projCol = child.projectedCol();
        int idx = resolveValueIndex(projCol);
        if (batch.isElementNull(projCol, idx)) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        return ((int[]) batch.valueArrays[projCol])[idx];
    }

    @Override
    public long getLong(String name) {
        TopLevelFieldMap.FieldDesc.Primitive child = lookupPrimitive(name);
        int projCol = child.projectedCol();
        int idx = resolveValueIndex(projCol);
        if (batch.isElementNull(projCol, idx)) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        return ((long[]) batch.valueArrays[projCol])[idx];
    }

    @Override
    public float getFloat(String name) {
        TopLevelFieldMap.FieldDesc.Primitive child = lookupPrimitive(name);
        int projCol = child.projectedCol();
        int idx = resolveValueIndex(projCol);
        if (batch.isElementNull(projCol, idx)) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        return ((float[]) batch.valueArrays[projCol])[idx];
    }

    @Override
    public double getDouble(String name) {
        TopLevelFieldMap.FieldDesc.Primitive child = lookupPrimitive(name);
        int projCol = child.projectedCol();
        int idx = resolveValueIndex(projCol);
        if (batch.isElementNull(projCol, idx)) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        return ((double[]) batch.valueArrays[projCol])[idx];
    }

    @Override
    public boolean getBoolean(String name) {
        TopLevelFieldMap.FieldDesc.Primitive child = lookupPrimitive(name);
        int projCol = child.projectedCol();
        int idx = resolveValueIndex(projCol);
        if (batch.isElementNull(projCol, idx)) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        return ((boolean[]) batch.valueArrays[projCol])[idx];
    }

    // ==================== Object Types ====================

    @Override
    public String getString(String name) {
        TopLevelFieldMap.FieldDesc.Primitive child = lookupPrimitive(name);
        int projCol = child.projectedCol();
        int idx = resolveValueIndex(projCol);
        if (batch.isElementNull(projCol, idx)) {
            return null;
        }
        byte[] raw = ((byte[][]) batch.valueArrays[projCol])[idx];
        return new String(raw, StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getBinary(String name) {
        TopLevelFieldMap.FieldDesc.Primitive child = lookupPrimitive(name);
        int projCol = child.projectedCol();
        int idx = resolveValueIndex(projCol);
        if (batch.isElementNull(projCol, idx)) {
            return null;
        }
        return ((byte[][]) batch.valueArrays[projCol])[idx];
    }

    @Override
    public LocalDate getDate(String name) {
        return readLogicalType(name, LogicalType.DateType.class, LocalDate.class);
    }

    @Override
    public LocalTime getTime(String name) {
        return readLogicalType(name, LogicalType.TimeType.class, LocalTime.class);
    }

    @Override
    public Instant getTimestamp(String name) {
        return readLogicalType(name, LogicalType.TimestampType.class, Instant.class);
    }

    @Override
    public BigDecimal getDecimal(String name) {
        return readLogicalType(name, LogicalType.DecimalType.class, BigDecimal.class);
    }

    @Override
    public UUID getUuid(String name) {
        return readLogicalType(name, LogicalType.UuidType.class, UUID.class);
    }

    // ==================== Nested Types ====================

    @Override
    public PqStruct getStruct(String name) {
        TopLevelFieldMap.FieldDesc child = lookupChild(name);
        if (!(child instanceof TopLevelFieldMap.FieldDesc.Struct structDesc)) {
            throw new IllegalArgumentException("Field '" + name + "' is not a struct");
        }
        if (isStructNull(structDesc)) {
            return null;
        }
        if (valueIndex >= 0) {
            return PqStructImpl.atPosition(batch, structDesc, valueIndex);
        }
        return new PqStructImpl(batch, structDesc, rowIndex);
    }

    @Override
    public PqIntList getListOfInts(String name) {
        return PqListImpl.createIntList(batch, lookupListChild(name), rowIndex, valueIndex);
    }

    @Override
    public PqLongList getListOfLongs(String name) {
        return PqListImpl.createLongList(batch, lookupListChild(name), rowIndex, valueIndex);
    }

    @Override
    public PqDoubleList getListOfDoubles(String name) {
        return PqListImpl.createDoubleList(batch, lookupListChild(name), rowIndex, valueIndex);
    }

    @Override
    public PqList getList(String name) {
        return PqListImpl.createGenericList(batch, lookupListChild(name), rowIndex, valueIndex);
    }

    @Override
    public PqMap getMap(String name) {
        TopLevelFieldMap.FieldDesc child = lookupChild(name);
        if (!(child instanceof TopLevelFieldMap.FieldDesc.MapOf mapDesc)) {
            throw new IllegalArgumentException("Field '" + name + "' is not a map");
        }
        return PqMapImpl.create(batch, mapDesc, rowIndex, valueIndex);
    }

    @Override
    public PqVariant getVariant(String name) {
        TopLevelFieldMap.FieldDesc child = lookupChild(name);
        if (!(child instanceof TopLevelFieldMap.FieldDesc.Variant variantDesc)) {
            throw new IllegalArgumentException("Field '" + name + "' is not annotated as VARIANT");
        }
        if (variantDesc.metadataCol() < 0 || variantDesc.valueCol() < 0) {
            throw new IllegalStateException(
                    "Variant column '" + name + "' requires both 'metadata' and 'value' children in the projection");
        }
        int metaIdx = resolveValueIndex(variantDesc.metadataCol());
        int valIdx = resolveValueIndex(variantDesc.valueCol());
        if (batch.isElementNull(variantDesc.metadataCol(), metaIdx)) {
            return null;
        }
        byte[] metadata = ((byte[][]) batch.valueArrays[variantDesc.metadataCol()])[metaIdx];
        byte[] value = ((byte[][]) batch.valueArrays[variantDesc.valueCol()])[valIdx];
        return new PqVariantImpl(metadata, value);
    }

    // ==================== Generic Fallback ====================

    @Override
    public Object getValue(String name) {
        TopLevelFieldMap.FieldDesc child = lookupChild(name);
        return readRawValue(child);
    }

    // ==================== Metadata ====================

    @Override
    public boolean isNull(String name) {
        TopLevelFieldMap.FieldDesc child = lookupChild(name);
        return isFieldNull(child);
    }

    @Override
    public int getFieldCount() {
        return desc.children().length;
    }

    @Override
    public String getFieldName(int index) {
        return desc.children()[index].name();
    }

    // ==================== Internal Helpers ====================

    private TopLevelFieldMap.FieldDesc lookupChild(String name) {
        TopLevelFieldMap.FieldDesc child = desc.getChild(name);
        if (child == null) {
            throw new IllegalArgumentException("Field not found: " + name);
        }
        return child;
    }

    private TopLevelFieldMap.FieldDesc.Primitive lookupPrimitive(String name) {
        TopLevelFieldMap.FieldDesc child = lookupChild(name);
        if (!(child instanceof TopLevelFieldMap.FieldDesc.Primitive prim)) {
            throw new IllegalArgumentException("Field '" + name + "' is not a primitive type");
        }
        return prim;
    }

    private TopLevelFieldMap.FieldDesc.ListOf lookupListChild(String name) {
        TopLevelFieldMap.FieldDesc child = lookupChild(name);
        if (!(child instanceof TopLevelFieldMap.FieldDesc.ListOf listDesc)) {
            throw new IllegalArgumentException("Field '" + name + "' is not a list");
        }
        return listDesc;
    }

    private <T> T readLogicalType(String name, Class<? extends LogicalType> expectedLogicalType, Class<T> resultClass) {
        TopLevelFieldMap.FieldDesc.Primitive child = lookupPrimitive(name);
        int projCol = child.projectedCol();
        int idx = resolveValueIndex(projCol);
        if (batch.isElementNull(projCol, idx)) {
            return null;
        }
        Object rawValue = batch.getValue(projCol, idx);
        if (resultClass.isInstance(rawValue)) {
            return resultClass.cast(rawValue);
        }
        SchemaNode.PrimitiveNode prim = child.schema();
        Object converted = LogicalTypeConverter.convert(rawValue, prim.type(), prim.logicalType());
        return resultClass.cast(converted);
    }

    private boolean isFieldNull(TopLevelFieldMap.FieldDesc child) {
        return switch (child) {
            case TopLevelFieldMap.FieldDesc.Primitive p -> {
                int idx = resolveValueIndex(p.projectedCol());
                yield batch.isElementNull(p.projectedCol(), idx);
            }
            case TopLevelFieldMap.FieldDesc.Struct s -> isStructNull(s);
            case TopLevelFieldMap.FieldDesc.ListOf l ->
                    PqListImpl.isListNull(batch, l, rowIndex, valueIndex);
            case TopLevelFieldMap.FieldDesc.MapOf m ->
                    PqMapImpl.isMapNull(batch, m, rowIndex, valueIndex);
            case TopLevelFieldMap.FieldDesc.Variant v -> isVariantNull(v);
        };
    }

    private boolean isVariantNull(TopLevelFieldMap.FieldDesc.Variant desc) {
        int col = desc.metadataCol() >= 0 ? desc.metadataCol() : desc.valueCol();
        if (col < 0) {
            return true;
        }
        int idx = resolveValueIndex(col);
        int defLevel = batch.getDefLevel(col, idx);
        return defLevel < desc.nullDefLevel();
    }

    private boolean isStructNull(TopLevelFieldMap.FieldDesc.Struct structDesc) {
        int primCol = structDesc.firstPrimitiveCol();
        if (primCol >= 0) {
            int idx = resolveValueIndex(primCol);
            int defLevel = batch.getDefLevel(primCol, idx);
            return defLevel < structDesc.schema().maxDefinitionLevel();
        }
        // Struct has no direct primitive child; fall back to the first leaf at any
        // depth. In record-index mode the recorded value index already points to
        // the leaf position. In position mode, `valueIndex` is a rep-level ordinal
        // at the struct's level, so chase through the leaf column's multi-level
        // offsets to reach the leaf position for this struct instance.
        int leafCol = structDesc.firstLeafProjCol();
        if (leafCol < 0) {
            return false;
        }
        int pos;
        if (valueIndex >= 0) {
            pos = valueIndex;
            int structRep = structDesc.schema().maxRepetitionLevel();
            int leafMaxRep = batch.getMaxRepLevel(leafCol);
            for (int k = structRep; k < leafMaxRep; k++) {
                pos = batch.getLevelStart(leafCol, k, pos);
            }
        } else {
            pos = batch.getValueIndex(leafCol, rowIndex);
        }
        int defLevel = batch.getDefLevel(leafCol, pos);
        return defLevel < structDesc.schema().maxDefinitionLevel();
    }

    private Object readRawValue(TopLevelFieldMap.FieldDesc child) {
        return switch (child) {
            case TopLevelFieldMap.FieldDesc.Primitive p -> {
                int idx = resolveValueIndex(p.projectedCol());
                if (batch.isElementNull(p.projectedCol(), idx)) {
                    yield null;
                }
                yield batch.getValue(p.projectedCol(), idx);
            }
            case TopLevelFieldMap.FieldDesc.Struct s -> {
                if (isStructNull(s)) {
                    yield null;
                }
                yield valueIndex >= 0
                        ? PqStructImpl.atPosition(batch, s, valueIndex)
                        : new PqStructImpl(batch, s, rowIndex);
            }
            case TopLevelFieldMap.FieldDesc.ListOf l ->
                    PqListImpl.createGenericList(batch, l, rowIndex, valueIndex);
            case TopLevelFieldMap.FieldDesc.MapOf m ->
                    PqMapImpl.create(batch, m, rowIndex, valueIndex);
            case TopLevelFieldMap.FieldDesc.Variant v -> getVariant(v.schema().name());
        };
    }
}
