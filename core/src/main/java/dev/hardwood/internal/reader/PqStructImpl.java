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
import dev.hardwood.schema.SchemaNode;

/**
 * Flyweight {@link PqStruct} that navigates directly over column arrays.
 * <p>
 * Supports two modes:
 * <ul>
 *   <li><b>Record mode</b>: resolves value position via {@code getValueIndex(projCol, rowIndex)}.
 *       Used for top-level structs.</li>
 *   <li><b>Position mode</b>: uses a fixed value index directly.
 *       Used for struct elements within lists/maps.</li>
 * </ul>
 */
final class PqStructImpl implements PqStruct {

    private final NestedBatchIndex batch;
    private final TopLevelFieldMap.FieldDesc.Struct desc;
    private final int rowIndex;     // >= 0 for record mode
    private final int valueIndex;   // >= 0 for position mode, -1 for record mode

    /** Record mode: value index resolved from batch offsets. */
    PqStructImpl(NestedBatchIndex batch, TopLevelFieldMap.FieldDesc.Struct desc, int rowIndex) {
        this.batch = batch;
        this.desc = desc;
        this.rowIndex = rowIndex;
        this.valueIndex = -1;
    }

    /** Position mode: fixed value index (for struct elements within lists). */
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
        ValidateHelper.validateInt(child.schema());
        return ((NestedColumnData.IntColumn) batch.columns[projCol]).get(idx);
    }

    @Override
    public long getLong(String name) {
        TopLevelFieldMap.FieldDesc.Primitive child = lookupPrimitive(name);
        int projCol = child.projectedCol();
        int idx = resolveValueIndex(projCol);
        if (batch.isElementNull(projCol, idx)) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        ValidateHelper.validateLong(child.schema());
        return ((NestedColumnData.LongColumn) batch.columns[projCol]).get(idx);
    }

    @Override
    public float getFloat(String name) {
        TopLevelFieldMap.FieldDesc.Primitive child = lookupPrimitive(name);
        int projCol = child.projectedCol();
        int idx = resolveValueIndex(projCol);
        if (batch.isElementNull(projCol, idx)) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        ValidateHelper.validateFloat(child.schema());
        return ((NestedColumnData.FloatColumn) batch.columns[projCol]).get(idx);
    }

    @Override
    public double getDouble(String name) {
        TopLevelFieldMap.FieldDesc.Primitive child = lookupPrimitive(name);
        int projCol = child.projectedCol();
        int idx = resolveValueIndex(projCol);
        if (batch.isElementNull(projCol, idx)) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        ValidateHelper.validateDouble(child.schema());
        return ((NestedColumnData.DoubleColumn) batch.columns[projCol]).get(idx);
    }

    @Override
    public boolean getBoolean(String name) {
        TopLevelFieldMap.FieldDesc.Primitive child = lookupPrimitive(name);
        int projCol = child.projectedCol();
        int idx = resolveValueIndex(projCol);
        if (batch.isElementNull(projCol, idx)) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        ValidateHelper.validateBoolean(child.schema());
        return ((NestedColumnData.BooleanColumn) batch.columns[projCol]).get(idx);
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
        ValidateHelper.validateString(child.schema());
        byte[] raw = ((NestedColumnData.ByteArrayColumn) batch.columns[projCol]).get(idx);
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
        ValidateHelper.validateBinary(child.schema());
        return ((NestedColumnData.ByteArrayColumn) batch.columns[projCol]).get(idx);
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
        return desc.schema().children().size();
    }

    @Override
    public String getFieldName(int index) {
        return desc.schema().children().get(index).name();
    }

    // ==================== Internal Helpers ====================

    private TopLevelFieldMap.FieldDesc lookupChild(String name) {
        TopLevelFieldMap.FieldDesc child = desc.children().get(name);
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
        ValidateHelper.validateLogicalType(child.schema(), expectedLogicalType);
        Object rawValue = batch.columns[projCol].getValue(idx);
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
        };
    }

    private boolean isStructNull(TopLevelFieldMap.FieldDesc.Struct structDesc) {
        for (TopLevelFieldMap.FieldDesc childDesc : structDesc.children().values()) {
            if (childDesc instanceof TopLevelFieldMap.FieldDesc.Primitive p) {
                int projCol = p.projectedCol();
                int idx = resolveValueIndex(projCol);
                NestedColumnData data = batch.columns[projCol];
                int defLevel = data.getDefLevel(idx);
                int structDefLevel = structDesc.schema().maxDefinitionLevel();
                return defLevel < structDefLevel;
            }
        }
        return false;
    }

    private Object readRawValue(TopLevelFieldMap.FieldDesc child) {
        return switch (child) {
            case TopLevelFieldMap.FieldDesc.Primitive p -> {
                int idx = resolveValueIndex(p.projectedCol());
                if (batch.isElementNull(p.projectedCol(), idx)) {
                    yield null;
                }
                yield batch.columns[p.projectedCol()].getValue(idx);
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
        };
    }
}
