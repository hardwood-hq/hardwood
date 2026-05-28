/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;

import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.row.PqInterval;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.SchemaNode;

/// Base class for flyweight [PqStruct] implementations that navigate directly
/// over column arrays.
///
/// Provides shared field navigation, type-checked lookup helpers, and all
/// typed accessor implementations (primitives, logical types, nested structs,
/// generic fallback). Subclasses supply the three data-backend primitives:
///
/// - [#resolveValueIndex] — translates a projected column index to the value
///   index within that column's array (may involve offset lookup for nested
///   readers, or is simply `rowIndex` for the flat path).
/// - [#isElementNull] — checks whether the value at a given position is absent.
/// - [#valueArrays] — returns the typed value array for a given projected column.
///
/// Subclasses also implement [#readStruct], [#isFieldNull], and [#readValueImpl]
/// since these depend on how null detection and struct construction differ
/// between the flat and nested paths.
abstract class AbstractPqStruct implements PqStruct {

    protected final TopLevelFieldMap.FieldDesc.Struct desc;

    AbstractPqStruct(TopLevelFieldMap.FieldDesc.Struct desc) {
        this.desc = desc;
    }

    protected abstract boolean isElementNull(int projCol, int idx);

    protected abstract Object valueArrays(int projCol);

    protected abstract int resolveValueIndex(int projCol);

    protected abstract PqStruct readStruct(TopLevelFieldMap.FieldDesc.Struct structDesc);

    protected abstract boolean isFieldNull(TopLevelFieldMap.FieldDesc child);

    protected abstract Object readValueImpl(TopLevelFieldMap.FieldDesc child, boolean decode);

    // ==================== Primitive Types ====================

    @Override
    public int getInt(String name) {
        TopLevelFieldMap.FieldDesc.Primitive child = lookupPrimitive(name);
        int idx = resolveValueIndex(child.projectedCol());
        return readInt(child, idx);
    }

    @Override
    public int getInt(int fieldIndex) {
        TopLevelFieldMap.FieldDesc.Primitive child = primitiveAt(fieldIndex);
        int idx = resolveValueIndex(child.projectedCol());
        return readInt(child, idx);
    }

    @Override
    public long getLong(String name) {
        TopLevelFieldMap.FieldDesc.Primitive child = lookupPrimitive(name);
        int idx = resolveValueIndex(child.projectedCol());
        return readLong(child, idx);
    }

    @Override
    public long getLong(int fieldIndex) {
        TopLevelFieldMap.FieldDesc.Primitive child = primitiveAt(fieldIndex);
        int idx = resolveValueIndex(child.projectedCol());
        return readLong(child, idx);
    }

    @Override
    public float getFloat(String name) {
        TopLevelFieldMap.FieldDesc.Primitive child = lookupPrimitive(name);
        int idx = resolveValueIndex(child.projectedCol());
        return readFloat(child, idx);
    }

    @Override
    public float getFloat(int fieldIndex) {
        TopLevelFieldMap.FieldDesc.Primitive child = primitiveAt(fieldIndex);
        int idx = resolveValueIndex(child.projectedCol());
        return readFloat(child, idx);
    }

    @Override
    public double getDouble(String name) {
        TopLevelFieldMap.FieldDesc.Primitive child = lookupPrimitive(name);
        int idx = resolveValueIndex(child.projectedCol());
        return readDouble(child, idx);
    }

    @Override
    public double getDouble(int fieldIndex) {
        TopLevelFieldMap.FieldDesc.Primitive child = primitiveAt(fieldIndex);
        int idx = resolveValueIndex(child.projectedCol());
        return readDouble(child, idx);
    }

    @Override
    public boolean getBoolean(String name) {
        TopLevelFieldMap.FieldDesc.Primitive child = lookupPrimitive(name);
        int idx = resolveValueIndex(child.projectedCol());
        return readBoolean(child, idx);
    }

    @Override
    public boolean getBoolean(int fieldIndex) {
        TopLevelFieldMap.FieldDesc.Primitive child = primitiveAt(fieldIndex);
        int idx = resolveValueIndex(child.projectedCol());
        return readBoolean(child, idx);
    }

    // ==================== Object Types ====================

    @Override
    public String getString(String name) {
        TopLevelFieldMap.FieldDesc.Primitive child = lookupPrimitive(name);
        int idx = resolveValueIndex(child.projectedCol());
        return readString(child, idx);
    }

    @Override
    public String getString(int fieldIndex) {
        TopLevelFieldMap.FieldDesc.Primitive child = primitiveAt(fieldIndex);
        int idx = resolveValueIndex(child.projectedCol());
        return readString(child, idx);
    }

    @Override
    public byte[] getBinary(String name) {
        TopLevelFieldMap.FieldDesc.Primitive child = lookupPrimitive(name);
        int idx = resolveValueIndex(child.projectedCol());
        return readBinary(child, idx);
    }

    @Override
    public byte[] getBinary(int fieldIndex) {
        TopLevelFieldMap.FieldDesc.Primitive child = primitiveAt(fieldIndex);
        int idx = resolveValueIndex(child.projectedCol());
        return readBinary(child, idx);
    }

    @Override
    public LocalDate getDate(String name) {
        TopLevelFieldMap.FieldDesc.Primitive child = lookupPrimitive(name);
        int idx = resolveValueIndex(child.projectedCol());
        return readLogicalType(child, LocalDate.class, readRaw(child, idx));
    }

    @Override
    public LocalDate getDate(int fieldIndex) {
        TopLevelFieldMap.FieldDesc.Primitive child = primitiveAt(fieldIndex);
        int idx = resolveValueIndex(child.projectedCol());
        return readLogicalType(child, LocalDate.class, readRaw(child, idx));
    }

    @Override
    public LocalTime getTime(String name) {
        TopLevelFieldMap.FieldDesc.Primitive child = lookupPrimitive(name);
        int idx = resolveValueIndex(child.projectedCol());
        return readLogicalType(child, LocalTime.class, readRaw(child, idx));
    }

    @Override
    public LocalTime getTime(int fieldIndex) {
        TopLevelFieldMap.FieldDesc.Primitive child = primitiveAt(fieldIndex);
        int idx = resolveValueIndex(child.projectedCol());
        return readLogicalType(child, LocalTime.class, readRaw(child, idx));
    }

    @Override
    public Instant getTimestamp(String name) {
        TopLevelFieldMap.FieldDesc.Primitive child = lookupPrimitive(name);
        TimestampAccessorKind.require(child.name(), child.schema().logicalType(), true);
        int idx = resolveValueIndex(child.projectedCol());
        return readLogicalType(child, Instant.class, readRaw(child, idx));
    }

    @Override
    public Instant getTimestamp(int fieldIndex) {
        TopLevelFieldMap.FieldDesc.Primitive child = primitiveAt(fieldIndex);
        TimestampAccessorKind.require(child.name(), child.schema().logicalType(), true);
        int idx = resolveValueIndex(child.projectedCol());
        return readLogicalType(child, Instant.class, readRaw(child, idx));
    }

    @Override
    public LocalDateTime getLocalTimestamp(String name) {
        TopLevelFieldMap.FieldDesc.Primitive child = lookupPrimitive(name);
        TimestampAccessorKind.require(child.name(), child.schema().logicalType(), false);
        int idx = resolveValueIndex(child.projectedCol());
        return readLogicalType(child, LocalDateTime.class, readRaw(child, idx));
    }

    @Override
    public LocalDateTime getLocalTimestamp(int fieldIndex) {
        TopLevelFieldMap.FieldDesc.Primitive child = primitiveAt(fieldIndex);
        TimestampAccessorKind.require(child.name(), child.schema().logicalType(), false);
        int idx = resolveValueIndex(child.projectedCol());
        return readLogicalType(child, LocalDateTime.class, readRaw(child, idx));
    }

    @Override
    public BigDecimal getDecimal(String name) {
        TopLevelFieldMap.FieldDesc.Primitive child = lookupPrimitive(name);
        int projCol = child.projectedCol();
        int idx = resolveValueIndex(projCol);
        if (isElementNull(projCol, idx)) {
            return null;
        }
        return readLogicalType(child, BigDecimal.class, readRaw(child, idx));
    }

    @Override
    public BigDecimal getDecimal(int fieldIndex) {
        TopLevelFieldMap.FieldDesc.Primitive child = primitiveAt(fieldIndex);
        int projCol = child.projectedCol();
        int idx = resolveValueIndex(projCol);
        if (isElementNull(projCol, idx)) {
            return null;
        }
        return readLogicalType(child, BigDecimal.class, readRaw(child, idx));
    }

    @Override
    public UUID getUuid(String name) {
        TopLevelFieldMap.FieldDesc.Primitive child = lookupPrimitive(name);
        int projCol = child.projectedCol();
        int idx = resolveValueIndex(projCol);
        if (isElementNull(projCol, idx)) {
            return null;
        }
        return readLogicalType(child, UUID.class, readRaw(child, idx));
    }

    @Override
    public UUID getUuid(int fieldIndex) {
        TopLevelFieldMap.FieldDesc.Primitive child = primitiveAt(fieldIndex);
        int projCol = child.projectedCol();
        int idx = resolveValueIndex(projCol);
        if (isElementNull(projCol, idx)) {
            return null;
        }
        return readLogicalType(child, UUID.class, readRaw(child, idx));
    }

    @Override
    public PqInterval getInterval(String name) {
        TopLevelFieldMap.FieldDesc.Primitive child = lookupPrimitive(name);
        int projCol = child.projectedCol();
        int idx = resolveValueIndex(projCol);
        if (isElementNull(projCol, idx)) {
            return null;
        }
        return readLogicalType(child, PqInterval.class, readRaw(child, idx));
    }

    @Override
    public PqInterval getInterval(int fieldIndex) {
        TopLevelFieldMap.FieldDesc.Primitive child = primitiveAt(fieldIndex);
        int projCol = child.projectedCol();
        int idx = resolveValueIndex(projCol);
        if (isElementNull(projCol, idx)) {
            return null;
        }
        return readLogicalType(child, PqInterval.class, readRaw(child, idx));
    }

    // ==================== Nested Types ====================

    @Override
    public PqStruct getStruct(String name) {
        return readStruct(structAt(lookupChild(name), name));
    }

    @Override
    public PqStruct getStruct(int fieldIndex) {
        TopLevelFieldMap.FieldDesc child = desc.children()[fieldIndex];
        return readStruct(structAt(child, child.name()));
    }

    // ==================== Generic Fallback ====================

    @Override
    public Object getValue(String name) {
        return readValueImpl(lookupChild(name), true);
    }

    @Override
    public Object getValue(int fieldIndex) {
        return readValueImpl(desc.children()[fieldIndex], true);
    }

    @Override
    public Object getRawValue(String name) {
        return readValueImpl(lookupChild(name), false);
    }

    @Override
    public Object getRawValue(int fieldIndex) {
        return readValueImpl(desc.children()[fieldIndex], false);
    }

    // ==================== Metadata ====================

    @Override
    public boolean isNull(String name) {
        return isFieldNull(lookupChild(name));
    }

    @Override
    public boolean isNull(int fieldIndex) {
        return isFieldNull(desc.children()[fieldIndex]);
    }

    @Override
    public int getFieldCount() {
        return desc.children().length;
    }

    @Override
    public String getFieldName(int index) {
        return desc.children()[index].name();
    }

    // ==================== Primitive Read Helpers ====================

    protected int readInt(TopLevelFieldMap.FieldDesc.Primitive child, int idx) {
        int projCol = child.projectedCol();
        if (isElementNull(projCol, idx)) {
            throw new NullPointerException("Field '" + child.name() + "' is null");
        }
        return ((int[]) valueArrays(projCol))[idx];
    }

    protected long readLong(TopLevelFieldMap.FieldDesc.Primitive child, int idx) {
        int projCol = child.projectedCol();
        if (isElementNull(projCol, idx)) {
            throw new NullPointerException("Field '" + child.name() + "' is null");
        }
        return ((long[]) valueArrays(projCol))[idx];
    }

    protected float readFloat(TopLevelFieldMap.FieldDesc.Primitive child, int idx) {
        int projCol = child.projectedCol();
        if (isElementNull(projCol, idx)) {
            throw new NullPointerException("Field '" + child.name() + "' is null");
        }
        if (child.schema().type() == PhysicalType.FLOAT) {
            return ((float[]) valueArrays(projCol))[idx];
        }
        return LogicalTypeConverter.convertToFloat16(
                ((BinaryBatchValues) valueArrays(projCol)).byteArrayAt(idx),
                child.schema().type());
    }

    protected double readDouble(TopLevelFieldMap.FieldDesc.Primitive child, int idx) {
        int projCol = child.projectedCol();
        if (isElementNull(projCol, idx)) {
            throw new NullPointerException("Field '" + child.name() + "' is null");
        }
        return ((double[]) valueArrays(projCol))[idx];
    }

    protected boolean readBoolean(TopLevelFieldMap.FieldDesc.Primitive child, int idx) {
        int projCol = child.projectedCol();
        if (isElementNull(projCol, idx)) {
            throw new NullPointerException("Field '" + child.name() + "' is null");
        }
        return ((boolean[]) valueArrays(projCol))[idx];
    }

    protected String readString(TopLevelFieldMap.FieldDesc.Primitive child, int idx) {
        int projCol = child.projectedCol();
        if (isElementNull(projCol, idx)) {
            return null;
        }
        return ((BinaryBatchValues) valueArrays(projCol)).stringAt(idx);
    }

    protected byte[] readBinary(TopLevelFieldMap.FieldDesc.Primitive child, int idx) {
        int projCol = child.projectedCol();
        if (isElementNull(projCol, idx)) {
            return null;
        }
        return ((BinaryBatchValues) valueArrays(projCol)).byteArrayAt(idx);
    }

    protected Object readRaw(TopLevelFieldMap.FieldDesc.Primitive child, int idx) {
        int projCol = child.projectedCol();
        if (isElementNull(projCol, idx)) {
            return null;
        }

        return switch (valueArrays(projCol)) {
            case int[] a -> a[idx];
            case long[] a -> a[idx];
            case float[] a -> a[idx];
            case double[] a -> a[idx];
            case boolean[] a -> a[idx];
            case BinaryBatchValues bbv -> bbv.byteArrayAt(idx);
            default -> throw new IllegalStateException("Unexpected array type: " + valueArrays(projCol).getClass());
        };
    }

    protected <T> T readLogicalType(TopLevelFieldMap.FieldDesc.Primitive child, Class<T> resultClass, Object rawValue) {
        if (resultClass.isInstance(rawValue)) {
            return resultClass.cast(rawValue);
        }
        SchemaNode.PrimitiveNode prim = child.schema();
        Object converted = LogicalTypeConverter.convert(rawValue, prim.type(), prim.logicalType());
        return resultClass.cast(converted);
    }

    // ==================== Child Resolution ====================

    protected TopLevelFieldMap.FieldDesc.Primitive lookupPrimitive(String name) {
        return primitiveOf(lookupChild(name), name);
    }

    protected TopLevelFieldMap.FieldDesc.Primitive primitiveAt(int fieldIndex) {
        TopLevelFieldMap.FieldDesc child = desc.children()[fieldIndex];
        return primitiveOf(child, child.name());
    }

    protected static TopLevelFieldMap.FieldDesc.Primitive primitiveOf(
            TopLevelFieldMap.FieldDesc child, String fieldName) {
        if (!(child instanceof TopLevelFieldMap.FieldDesc.Primitive prim)) {
            throw new IllegalArgumentException("Field '" + fieldName + "' is not a primitive type");
        }
        return prim;
    }

    protected TopLevelFieldMap.FieldDesc lookupChild(String name) {
        TopLevelFieldMap.FieldDesc child = desc.getChild(name);
        if (child == null) {
            throw new IllegalArgumentException("Field not found: " + name);
        }
        return child;
    }

    protected static TopLevelFieldMap.FieldDesc.Struct structAt(
            TopLevelFieldMap.FieldDesc child, String fieldName) {
        if (!(child instanceof TopLevelFieldMap.FieldDesc.Struct structDesc)) {
            throw new IllegalArgumentException("Field '" + fieldName + "' is not a struct");
        }
        return structDesc;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("PqStruct{");
        int n = getFieldCount();
        for (int i = 0; i < n; i++) {
            if (i > 0) sb.append(", ");
            sb.append(getFieldName(i)).append('=');
            FlyweightFormatter.appendValue(sb, getValue(i));
        }
        sb.append('}');
        return sb.toString();
    }
}