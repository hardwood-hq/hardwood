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
import java.time.LocalTime;
import java.util.UUID;

import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.internal.variant.PqVariantImpl;
import dev.hardwood.internal.variant.VariantMetadata;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.row.PqInterval;
import dev.hardwood.row.PqList;
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
        return readInt(lookupPrimitive(name));
    }

    @Override
    public int getInt(int fieldIndex) {
        return readInt(primitiveAt(fieldIndex));
    }

    @Override
    public long getLong(String name) {
        return readLong(lookupPrimitive(name));
    }

    @Override
    public long getLong(int fieldIndex) {
        return readLong(primitiveAt(fieldIndex));
    }

    @Override
    public float getFloat(String name) {
        return readFloat(lookupPrimitive(name));
    }

    @Override
    public float getFloat(int fieldIndex) {
        return readFloat(primitiveAt(fieldIndex));
    }

    @Override
    public double getDouble(String name) {
        return readDouble(lookupPrimitive(name));
    }

    @Override
    public double getDouble(int fieldIndex) {
        return readDouble(primitiveAt(fieldIndex));
    }

    @Override
    public boolean getBoolean(String name) {
        return readBoolean(lookupPrimitive(name));
    }

    @Override
    public boolean getBoolean(int fieldIndex) {
        return readBoolean(primitiveAt(fieldIndex));
    }

    // ==================== Object Types ====================

    @Override
    public String getString(String name) {
        return readString(lookupPrimitive(name));
    }

    @Override
    public String getString(int fieldIndex) {
        return readString(primitiveAt(fieldIndex));
    }

    @Override
    public byte[] getBinary(String name) {
        return readBinary(lookupPrimitive(name));
    }

    @Override
    public byte[] getBinary(int fieldIndex) {
        return readBinary(primitiveAt(fieldIndex));
    }

    @Override
    public LocalDate getDate(String name) {
        return readLogicalType(lookupPrimitive(name), LocalDate.class);
    }

    @Override
    public LocalDate getDate(int fieldIndex) {
        return readLogicalType(primitiveAt(fieldIndex), LocalDate.class);
    }

    @Override
    public LocalTime getTime(String name) {
        return readLogicalType(lookupPrimitive(name), LocalTime.class);
    }

    @Override
    public LocalTime getTime(int fieldIndex) {
        return readLogicalType(primitiveAt(fieldIndex), LocalTime.class);
    }

    @Override
    public Instant getTimestamp(String name) {
        return readLogicalType(lookupPrimitive(name), Instant.class);
    }

    @Override
    public Instant getTimestamp(int fieldIndex) {
        return readLogicalType(primitiveAt(fieldIndex), Instant.class);
    }

    @Override
    public BigDecimal getDecimal(String name) {
        return readLogicalType(lookupPrimitive(name), BigDecimal.class);
    }

    @Override
    public BigDecimal getDecimal(int fieldIndex) {
        return readLogicalType(primitiveAt(fieldIndex), BigDecimal.class);
    }

    @Override
    public UUID getUuid(String name) {
        return readLogicalType(lookupPrimitive(name), UUID.class);
    }

    @Override
    public UUID getUuid(int fieldIndex) {
        return readLogicalType(primitiveAt(fieldIndex), UUID.class);
    }

    @Override
    public PqInterval getInterval(String name) {
        return readLogicalType(lookupPrimitive(name), PqInterval.class);
    }

    @Override
    public PqInterval getInterval(int fieldIndex) {
        return readLogicalType(primitiveAt(fieldIndex), PqInterval.class);
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

    @Override
    public PqList getList(String name) {
        return PqListImpl.createGenericList(batch, listAt(lookupChild(name), name), rowIndex, valueIndex);
    }

    @Override
    public PqList getList(int fieldIndex) {
        TopLevelFieldMap.FieldDesc child = desc.children()[fieldIndex];
        return PqListImpl.createGenericList(batch, listAt(child, child.name()), rowIndex, valueIndex);
    }

    @Override
    public PqMap getMap(String name) {
        return PqMapImpl.create(batch, mapAt(lookupChild(name), name), rowIndex, valueIndex);
    }

    @Override
    public PqMap getMap(int fieldIndex) {
        TopLevelFieldMap.FieldDesc child = desc.children()[fieldIndex];
        return PqMapImpl.create(batch, mapAt(child, child.name()), rowIndex, valueIndex);
    }

    @Override
    public PqVariant getVariant(String name) {
        return readVariant(variantAt(lookupChild(name), name), name);
    }

    @Override
    public PqVariant getVariant(int fieldIndex) {
        TopLevelFieldMap.FieldDesc child = desc.children()[fieldIndex];
        return readVariant(variantAt(child, child.name()), child.name());
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

    private int readInt(TopLevelFieldMap.FieldDesc.Primitive child) {
        int projCol = child.projectedCol();
        int idx = resolveValueIndex(projCol);
        if (batch.isElementNull(projCol, idx)) {
            throw new NullPointerException("Field '" + child.name() + "' is null");
        }
        return ((int[]) batch.valueArrays[projCol])[idx];
    }

    private long readLong(TopLevelFieldMap.FieldDesc.Primitive child) {
        int projCol = child.projectedCol();
        int idx = resolveValueIndex(projCol);
        if (batch.isElementNull(projCol, idx)) {
            throw new NullPointerException("Field '" + child.name() + "' is null");
        }
        return ((long[]) batch.valueArrays[projCol])[idx];
    }

    private float readFloat(TopLevelFieldMap.FieldDesc.Primitive child) {
        int projCol = child.projectedCol();
        int idx = resolveValueIndex(projCol);
        if (batch.isElementNull(projCol, idx)) {
            throw new NullPointerException("Field '" + child.name() + "' is null");
        }
        if (child.schema().type() == PhysicalType.FLOAT) {
            return ((float[]) batch.valueArrays[projCol])[idx];
        }
        // FLOAT16 path: convertToFloat16 returns primitive float so the value
        // flows through without per-row autoboxing. readLogicalType isn't reused
        // here because its `LogicalTypeConverter.convert` step boxes via Object.
        return LogicalTypeConverter.convertToFloat16(
                ((BinaryBatchValues) batch.valueArrays[projCol]).byteArrayAt(idx),
                child.schema().type());
    }

    private double readDouble(TopLevelFieldMap.FieldDesc.Primitive child) {
        int projCol = child.projectedCol();
        int idx = resolveValueIndex(projCol);
        if (batch.isElementNull(projCol, idx)) {
            throw new NullPointerException("Field '" + child.name() + "' is null");
        }
        return ((double[]) batch.valueArrays[projCol])[idx];
    }

    private boolean readBoolean(TopLevelFieldMap.FieldDesc.Primitive child) {
        int projCol = child.projectedCol();
        int idx = resolveValueIndex(projCol);
        if (batch.isElementNull(projCol, idx)) {
            throw new NullPointerException("Field '" + child.name() + "' is null");
        }
        return ((boolean[]) batch.valueArrays[projCol])[idx];
    }

    private String readString(TopLevelFieldMap.FieldDesc.Primitive child) {
        int projCol = child.projectedCol();
        int idx = resolveValueIndex(projCol);
        if (batch.isElementNull(projCol, idx)) {
            return null;
        }
        return batch.getString(projCol, idx);
    }

    private byte[] readBinary(TopLevelFieldMap.FieldDesc.Primitive child) {
        int projCol = child.projectedCol();
        int idx = resolveValueIndex(projCol);
        if (batch.isElementNull(projCol, idx)) {
            return null;
        }
        return batch.getBinary(projCol, idx);
    }

    private <T> T readLogicalType(TopLevelFieldMap.FieldDesc.Primitive child, Class<T> resultClass) {
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

    private PqStruct readStruct(TopLevelFieldMap.FieldDesc.Struct structDesc) {
        if (isStructNull(structDesc)) {
            return null;
        }
        if (valueIndex >= 0) {
            return PqStructImpl.atPosition(batch, structDesc, valueIndex);
        }
        return new PqStructImpl(batch, structDesc, rowIndex);
    }

    private PqVariant readVariant(TopLevelFieldMap.FieldDesc.Variant variantDesc, String fieldName) {
        if (variantDesc.metadataCol() < 0) {
            throw new IllegalStateException(
                    "Variant column '" + fieldName + "' requires its 'metadata' child in the projection");
        }
        int metaIdx = resolveValueIndex(variantDesc.metadataCol());
        if (batch.isElementNull(variantDesc.metadataCol(), metaIdx)) {
            return null;
        }
        byte[] metadataBytes = batch.getBinary(variantDesc.metadataCol(), metaIdx);

        if (variantDesc.root().typed() != null) {
            // Position-mode (struct inside a list/map) would need list-aware
            // indices inside the reassembler; record-mode uses the row index
            // directly. No fixture exercises the former today, and silently
            // returning bytes reassembled against row 0 would corrupt results.
            // Fail fast until list-aware reassembly is implemented.
            if (rowIndex < 0) {
                throw new UnsupportedOperationException(
                        "Shredded Variant inside a repeated context (list/map element) "
                                + "is not yet supported; field '" + fieldName + "'");
            }
            VariantMetadata meta = new VariantMetadata(metadataBytes);
            VariantShredReassembler reassembler = new VariantShredReassembler();
            reassembler.setCurrentMetadata(meta);
            byte[] value = reassembler.reassemble(variantDesc.root(), batch, rowIndex);
            if (value == null) {
                return null;
            }
            return new PqVariantImpl(meta, value, 0);
        }

        int valueCol = variantDesc.valueCol();
        if (valueCol < 0) {
            throw new IllegalStateException(
                    "Variant column '" + fieldName + "' requires its 'value' child in the projection");
        }
        int valIdx = resolveValueIndex(valueCol);
        byte[] value = batch.getBinary(valueCol, valIdx);
        return new PqVariantImpl(metadataBytes, value);
    }

    // ==================== Child Resolution ====================

    private TopLevelFieldMap.FieldDesc lookupChild(String name) {
        TopLevelFieldMap.FieldDesc child = desc.getChild(name);
        if (child == null) {
            throw new IllegalArgumentException("Field not found: " + name);
        }
        return child;
    }

    private TopLevelFieldMap.FieldDesc.Primitive lookupPrimitive(String name) {
        return primitiveOf(lookupChild(name), name);
    }

    private TopLevelFieldMap.FieldDesc.Primitive primitiveAt(int fieldIndex) {
        TopLevelFieldMap.FieldDesc child = desc.children()[fieldIndex];
        return primitiveOf(child, child.name());
    }

    private static TopLevelFieldMap.FieldDesc.Primitive primitiveOf(
            TopLevelFieldMap.FieldDesc child, String fieldName) {
        if (!(child instanceof TopLevelFieldMap.FieldDesc.Primitive prim)) {
            throw new IllegalArgumentException("Field '" + fieldName + "' is not a primitive type");
        }
        return prim;
    }

    private static TopLevelFieldMap.FieldDesc.Struct structAt(
            TopLevelFieldMap.FieldDesc child, String fieldName) {
        if (!(child instanceof TopLevelFieldMap.FieldDesc.Struct structDesc)) {
            throw new IllegalArgumentException("Field '" + fieldName + "' is not a struct");
        }
        return structDesc;
    }

    private static TopLevelFieldMap.FieldDesc.ListOf listAt(
            TopLevelFieldMap.FieldDesc child, String fieldName) {
        if (!(child instanceof TopLevelFieldMap.FieldDesc.ListOf listDesc)) {
            throw new IllegalArgumentException("Field '" + fieldName + "' is not a list");
        }
        return listDesc;
    }

    private static TopLevelFieldMap.FieldDesc.MapOf mapAt(
            TopLevelFieldMap.FieldDesc child, String fieldName) {
        if (!(child instanceof TopLevelFieldMap.FieldDesc.MapOf mapDesc)) {
            throw new IllegalArgumentException("Field '" + fieldName + "' is not a map");
        }
        return mapDesc;
    }

    private static TopLevelFieldMap.FieldDesc.Variant variantAt(
            TopLevelFieldMap.FieldDesc child, String fieldName) {
        if (!(child instanceof TopLevelFieldMap.FieldDesc.Variant variantDesc)) {
            throw new IllegalArgumentException("Field '" + fieldName + "' is not annotated as VARIANT");
        }
        return variantDesc;
    }

    // ==================== Null Checks ====================

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

    private Object readValueImpl(TopLevelFieldMap.FieldDesc child, boolean decode) {
        return switch (child) {
            case TopLevelFieldMap.FieldDesc.Primitive p -> {
                int idx = resolveValueIndex(p.projectedCol());
                if (batch.isElementNull(p.projectedCol(), idx)) {
                    yield null;
                }
                Object raw = batch.getValue(p.projectedCol(), idx);
                yield decode ? ValueConverter.convertValue(raw, p.schema()) : raw;
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
            // Variants are self-describing; there's no raw-vs-decoded split.
            case TopLevelFieldMap.FieldDesc.Variant v -> readVariant(v, v.schema().name());
        };
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
