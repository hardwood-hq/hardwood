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
import java.util.AbstractList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.internal.variant.PqVariantImpl;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.row.PqInterval;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.schema.SchemaNode;

/// Flyweight [PqMap] that reads key-value entries directly from parallel column arrays.
final class PqMapImpl implements PqMap {

    private final NestedBatchIndex batch;
    private final TopLevelFieldMap.FieldDesc.MapOf mapDesc;
    private final int start;
    private final int end;
    private final SchemaNode keySchema;
    private final SchemaNode valueSchema;

    PqMapImpl(NestedBatchIndex batch, TopLevelFieldMap.FieldDesc.MapOf mapDesc,
                  int start, int end) {
        this.batch = batch;
        this.mapDesc = mapDesc;
        this.start = start;
        this.end = end;

        // Get key/value schemas from MAP -> key_value -> (key, value)
        SchemaNode.GroupNode keyValueGroup = (SchemaNode.GroupNode) mapDesc.schema().children().get(0);
        this.keySchema = keyValueGroup.children().get(0);
        this.valueSchema = keyValueGroup.children().get(1);
    }

    // ==================== Factory Methods ====================

    static PqMap create(NestedBatchIndex batch, TopLevelFieldMap.FieldDesc.MapOf mapDesc,
                        int rowIndex, int valueIndex) {
        int keyProjCol = mapDesc.keyProjCol();
        int mlLevel = mapDesc.schema().maxRepetitionLevel();
        int leafMaxRep = batch.getMaxRepLevel(keyProjCol);

        int start, end;
        if (valueIndex >= 0 && mlLevel > 0) {
            start = batch.getLevelStart(keyProjCol, mlLevel, valueIndex);
            end = batch.getLevelEnd(keyProjCol, mlLevel, valueIndex);
        } else {
            start = batch.getListStart(keyProjCol, rowIndex);
            end = batch.getListEnd(keyProjCol, rowIndex);
        }

        // Chase to value level for defLevel check
        int firstValue = start;
        for (int level = mlLevel + 1; level < leafMaxRep; level++) {
            firstValue = batch.getLevelStart(keyProjCol, level, firstValue);
        }

        int defLevel = batch.getDefLevel(keyProjCol, firstValue);
        if (defLevel < mapDesc.nullDefLevel()) {
            return null; // null map
        }
        if (defLevel < mapDesc.entryDefLevel()) {
            // Empty map
            return new PqMapImpl(batch, mapDesc, start, start);
        }
        return new PqMapImpl(batch, mapDesc, start, end);
    }

    static boolean isMapNull(NestedBatchIndex batch, TopLevelFieldMap.FieldDesc.MapOf mapDesc,
                             int rowIndex, int valueIndex) {
        int keyProjCol = mapDesc.keyProjCol();
        int mlLevel = mapDesc.schema().maxRepetitionLevel();
        int leafMaxRep = batch.getMaxRepLevel(keyProjCol);

        int start;
        if (valueIndex >= 0 && mlLevel > 0) {
            start = batch.getLevelStart(keyProjCol, mlLevel, valueIndex);
        } else {
            start = batch.getListStart(keyProjCol, rowIndex);
        }

        int firstValue = start;
        for (int level = mlLevel + 1; level < leafMaxRep; level++) {
            firstValue = batch.getLevelStart(keyProjCol, level, firstValue);
        }

        int defLevel = batch.getDefLevel(keyProjCol, firstValue);
        return defLevel < mapDesc.nullDefLevel();
    }

    // ==================== PqMap Interface ====================

    @Override
    public List<Entry> getEntries() {
        return new AbstractList<>() {
            @Override
            public Entry get(int index) {
                if (index < 0 || index >= size()) {
                    throw new IndexOutOfBoundsException(
                            "Index " + index + " out of range [0, " + size() + ")");
                }
                return new ColumnarEntry(start + index);
            }

            @Override
            public int size() {
                return end - start;
            }
        };
    }

    @Override
    public int size() {
        return end - start;
    }

    @Override
    public boolean isEmpty() {
        return start >= end;
    }

    // ==================== Key-Based Lookup ====================

    @Override
    public boolean containsKey(String key) {
        return indexOfStringKey(key) >= 0;
    }

    @Override
    public boolean containsKey(int key) {
        return indexOfIntKey(key) >= 0;
    }

    @Override
    public boolean containsKey(long key) {
        return indexOfLongKey(key) >= 0;
    }

    @Override
    public boolean containsKey(byte[] key) {
        return indexOfBinaryKey(key) >= 0;
    }

    @Override
    public Object getValue(String key) {
        int idx = indexOfStringKey(key);
        return idx < 0 ? null : valueAt(idx);
    }

    @Override
    public Object getValue(int key) {
        int idx = indexOfIntKey(key);
        return idx < 0 ? null : valueAt(idx);
    }

    @Override
    public Object getValue(long key) {
        int idx = indexOfLongKey(key);
        return idx < 0 ? null : valueAt(idx);
    }

    @Override
    public Object getValue(byte[] key) {
        int idx = indexOfBinaryKey(key);
        return idx < 0 ? null : valueAt(idx);
    }

    @Override
    public Object getRawValue(String key) {
        int idx = indexOfStringKey(key);
        return idx < 0 ? null : rawValueAt(idx);
    }

    @Override
    public Object getRawValue(int key) {
        int idx = indexOfIntKey(key);
        return idx < 0 ? null : rawValueAt(idx);
    }

    @Override
    public Object getRawValue(long key) {
        int idx = indexOfLongKey(key);
        return idx < 0 ? null : rawValueAt(idx);
    }

    @Override
    public Object getRawValue(byte[] key) {
        int idx = indexOfBinaryKey(key);
        return idx < 0 ? null : rawValueAt(idx);
    }

    private int indexOfStringKey(String key) {
        Objects.requireNonNull(key, "key");
        int keyProjCol = mapDesc.keyProjCol();
        byte[][] keys = (byte[][]) batch.valueArrays[keyProjCol];
        byte[] needle = key.getBytes(StandardCharsets.UTF_8);
        for (int i = start; i < end; i++) {
            if (batch.isElementNull(keyProjCol, i)) {
                continue;
            }
            if (Arrays.equals(keys[i], needle)) {
                return i;
            }
        }
        return -1;
    }

    private int indexOfIntKey(int key) {
        int keyProjCol = mapDesc.keyProjCol();
        int[] keys = (int[]) batch.valueArrays[keyProjCol];
        for (int i = start; i < end; i++) {
            if (batch.isElementNull(keyProjCol, i)) {
                continue;
            }
            if (keys[i] == key) {
                return i;
            }
        }
        return -1;
    }

    private int indexOfLongKey(long key) {
        int keyProjCol = mapDesc.keyProjCol();
        long[] keys = (long[]) batch.valueArrays[keyProjCol];
        for (int i = start; i < end; i++) {
            if (batch.isElementNull(keyProjCol, i)) {
                continue;
            }
            if (keys[i] == key) {
                return i;
            }
        }
        return -1;
    }

    private int indexOfBinaryKey(byte[] key) {
        Objects.requireNonNull(key, "key");
        int keyProjCol = mapDesc.keyProjCol();
        byte[][] keys = (byte[][]) batch.valueArrays[keyProjCol];
        for (int i = start; i < end; i++) {
            if (batch.isElementNull(keyProjCol, i)) {
                continue;
            }
            if (Arrays.equals(keys[i], key)) {
                return i;
            }
        }
        return -1;
    }

    // ==================== Value Readers (by leaf index) ====================

    private Object valueAt(int valueIdx) {
        Object group = groupValueAt(valueIdx);
        if (group != null) {
            return group;
        }
        if (isValueNullAt(valueIdx)) {
            return null;
        }
        return ValueConverter.convertValue(readValueAt(valueIdx), valueSchema);
    }

    private Object rawValueAt(int valueIdx) {
        Object group = groupValueAt(valueIdx);
        if (group != null) {
            return group;
        }
        if (isValueNullAt(valueIdx)) {
            return null;
        }
        return readValueAt(valueIdx);
    }

    private Object groupValueAt(int valueIdx) {
        TopLevelFieldMap.FieldDesc vDesc = mapDesc.valueDesc();
        if (vDesc instanceof TopLevelFieldMap.FieldDesc.Struct) {
            return structValueAt(valueIdx);
        }
        if (vDesc instanceof TopLevelFieldMap.FieldDesc.ListOf) {
            return listValueAt(valueIdx);
        }
        if (vDesc instanceof TopLevelFieldMap.FieldDesc.MapOf) {
            return mapValueAt(valueIdx);
        }
        if (vDesc instanceof TopLevelFieldMap.FieldDesc.Variant) {
            return variantValueAt(valueIdx);
        }
        return null;
    }

    private PqStruct structValueAt(int valueIdx) {
        TopLevelFieldMap.FieldDesc vDesc = mapDesc.valueDesc();
        if (!(vDesc instanceof TopLevelFieldMap.FieldDesc.Struct structDesc)) {
            throw new IllegalArgumentException("Value is not a struct");
        }
        // Null check against the value column. `mapDesc.valueProjCol()` may point
        // to a leaf deeper than the struct's own primitives (e.g. a leaf inside a
        // list inside the struct), so translate the entry index to the value
        // column's leaf position before reading its def level.
        int valueProjCol = mapDesc.valueProjCol();
        if (valueProjCol >= 0) {
            int valLeafIdx = resolveValueLeafIdx(valueIdx);
            int defLevel = batch.getDefLevel(valueProjCol, valLeafIdx);
            if (defLevel < structDesc.schema().maxDefinitionLevel()) {
                return null;
            }
        }
        return PqStructImpl.atPosition(batch, structDesc, valueIdx);
    }

    private PqList listValueAt(int valueIdx) {
        TopLevelFieldMap.FieldDesc vDesc = mapDesc.valueDesc();
        if (!(vDesc instanceof TopLevelFieldMap.FieldDesc.ListOf listDesc)) {
            throw new IllegalArgumentException("Value is not a list");
        }
        return PqListImpl.createGenericList(batch, listDesc, -1, valueIdx);
    }

    private PqMap mapValueAt(int valueIdx) {
        TopLevelFieldMap.FieldDesc vDesc = mapDesc.valueDesc();
        if (!(vDesc instanceof TopLevelFieldMap.FieldDesc.MapOf innerMapDesc)) {
            throw new IllegalArgumentException("Value is not a map");
        }
        return PqMapImpl.create(batch, innerMapDesc, -1, valueIdx);
    }

    private PqVariant variantValueAt(int valueIdx) {
        TopLevelFieldMap.FieldDesc vDesc = mapDesc.valueDesc();
        if (!(vDesc instanceof TopLevelFieldMap.FieldDesc.Variant variantDesc)) {
            throw new IllegalArgumentException("Value is not a variant");
        }
        if (variantDesc.root().typed() != null) {
            // Shredded variants in repeated contexts need position-aware
            // reassembly (tracked in hardwood#467); the unshredded path
            // below works today.
            throw new UnsupportedOperationException(
                    "Shredded Variant inside a map value is not yet supported");
        }
        if (variantDesc.metadataCol() < 0) {
            throw new IllegalStateException(
                    "Variant map value requires its 'metadata' child in the projection");
        }
        if (batch.isElementNull(variantDesc.metadataCol(), valueIdx)) {
            return null;
        }
        byte[] metadataBytes = ((byte[][]) batch.valueArrays[variantDesc.metadataCol()])[valueIdx];
        int valueCol = variantDesc.valueCol();
        if (valueCol < 0) {
            throw new IllegalStateException(
                    "Variant map value requires its 'value' child in the projection");
        }
        byte[] value = ((byte[][]) batch.valueArrays[valueCol])[valueIdx];
        return new PqVariantImpl(metadataBytes, value);
    }

    private boolean isValueNullAt(int valueIdx) {
        int valueProjCol = mapDesc.valueProjCol();
        if (valueProjCol < 0) {
            return false;
        }
        // Compare against the value node's own max def level (not the leaf
        // primitive's), so a non-null complex value with null primitive
        // descendants is not misreported as null. The value column's leaf
        // position must be resolved explicitly — see resolveValueLeafIdx.
        int valLeafIdx = resolveValueLeafIdx(valueIdx);
        int defLevel = batch.getDefLevel(valueProjCol, valLeafIdx);
        return defLevel < valueSchema.maxDefinitionLevel();
    }

    private Object readValueAt(int valueIdx) {
        int valueProjCol = mapDesc.valueProjCol();
        if (batch.isElementNull(valueProjCol, valueIdx)) {
            return null;
        }
        return batch.getValue(valueProjCol, valueIdx);
    }

    /// Translates an entry index (expressed as a position in the key column's leaf
    /// space, which is what [ColumnarEntry] carries as `valueIdx`) to the
    /// corresponding leaf position in the value column.
    ///
    /// When the value column has more rep levels than the key column — i.e. when
    /// the value contains a repeated descendant — each entry occupies one-or-more
    /// records in the value column, while the key column has exactly one record
    /// per entry. In that case the key-leaf index equals the global entry index
    /// and indexes the value column's deepest multi-level offset array, which
    /// maps entry → first-leaf position.
    ///
    /// For primitive-equivalent values (same rep-level depth as the key), the
    /// two indexing spaces coincide and the input is returned unchanged.
    private int resolveValueLeafIdx(int keyLeafIdx) {
        int valueProjCol = mapDesc.valueProjCol();
        if (valueProjCol < 0) {
            return keyLeafIdx;
        }
        int[][] valMl = batch.multiOffsets[valueProjCol];
        int[][] keyMl = batch.multiOffsets[mapDesc.keyProjCol()];
        int valLevels = valMl == null ? 0 : valMl.length;
        int keyLevels = keyMl == null ? 0 : keyMl.length;
        if (valLevels <= keyLevels) {
            return keyLeafIdx;
        }
        return valMl[valLevels - 1][keyLeafIdx];
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Entry e : getEntries()) {
            if (!first) {
				sb.append(", ");
			}
            first = false;
            FlyweightFormatter.appendValue(sb, e.getKey());
            sb.append('=');
            FlyweightFormatter.appendValue(sb, e.getValue());
        }
        sb.append('}');
        return sb.toString();
    }

    // ==================== Flyweight Entry ====================

    private class ColumnarEntry implements Entry {
        private final int valueIdx;

        ColumnarEntry(int valueIdx) {
            this.valueIdx = valueIdx;
        }

        // ==================== Key Accessors ====================

        @Override
        public int getIntKey() {
            int keyProjCol = mapDesc.keyProjCol();
            if (batch.isElementNull(keyProjCol, valueIdx)) {
                throw new NullPointerException("Key is null");
            }
            return ((int[]) batch.valueArrays[keyProjCol])[valueIdx];
        }

        @Override
        public long getLongKey() {
            int keyProjCol = mapDesc.keyProjCol();
            if (batch.isElementNull(keyProjCol, valueIdx)) {
                throw new NullPointerException("Key is null");
            }
            return ((long[]) batch.valueArrays[keyProjCol])[valueIdx];
        }

        @Override
        public String getStringKey() {
            int keyProjCol = mapDesc.keyProjCol();
            if (batch.isElementNull(keyProjCol, valueIdx)) {
                return null;
            }
            byte[] raw = ((byte[][]) batch.valueArrays[keyProjCol])[valueIdx];
            return new String(raw, StandardCharsets.UTF_8);
        }

        @Override
        public byte[] getBinaryKey() {
            int keyProjCol = mapDesc.keyProjCol();
            if (batch.isElementNull(keyProjCol, valueIdx)) {
                return null;
            }
            return ((byte[][]) batch.valueArrays[keyProjCol])[valueIdx];
        }

        @Override
        public Object getKey() {
            return ValueConverter.convertValue(readKey(), keySchema);
        }

        @Override
        public Object getRawKey() {
            return readKey();
        }

        // ==================== Value Accessors ====================

        @Override
        public int getIntValue() {
            int valueProjCol = mapDesc.valueProjCol();
            if (batch.isElementNull(valueProjCol, valueIdx)) {
                throw new NullPointerException("Value is null");
            }
            return ((int[]) batch.valueArrays[valueProjCol])[valueIdx];
        }

        @Override
        public long getLongValue() {
            int valueProjCol = mapDesc.valueProjCol();
            if (batch.isElementNull(valueProjCol, valueIdx)) {
                throw new NullPointerException("Value is null");
            }
            return ((long[]) batch.valueArrays[valueProjCol])[valueIdx];
        }

        @Override
        public float getFloatValue() {
            int valueProjCol = mapDesc.valueProjCol();
            if (batch.isElementNull(valueProjCol, valueIdx)) {
                throw new NullPointerException("Value is null");
            }
            if (valueSchema instanceof SchemaNode.PrimitiveNode primitive
                    && primitive.type() == PhysicalType.FIXED_LEN_BYTE_ARRAY
                    && primitive.logicalType() instanceof LogicalType.Float16Type) {
                // FLOAT16 path: FLBA(2) payload decoded to a single-precision
                // float, matching PqStructImpl.getFloat and FlatRowReader.getFloat.
                return LogicalTypeConverter.convertToFloat16(
                        ((byte[][]) batch.valueArrays[valueProjCol])[valueIdx],
                        primitive.type());
            }
            return ((float[]) batch.valueArrays[valueProjCol])[valueIdx];
        }

        @Override
        public double getDoubleValue() {
            int valueProjCol = mapDesc.valueProjCol();
            if (batch.isElementNull(valueProjCol, valueIdx)) {
                throw new NullPointerException("Value is null");
            }
            return ((double[]) batch.valueArrays[valueProjCol])[valueIdx];
        }

        @Override
        public boolean getBooleanValue() {
            int valueProjCol = mapDesc.valueProjCol();
            if (batch.isElementNull(valueProjCol, valueIdx)) {
                throw new NullPointerException("Value is null");
            }
            return ((boolean[]) batch.valueArrays[valueProjCol])[valueIdx];
        }

        @Override
        public String getStringValue() {
            int valueProjCol = mapDesc.valueProjCol();
            if (batch.isElementNull(valueProjCol, valueIdx)) {
                return null;
            }
            byte[] raw = ((byte[][]) batch.valueArrays[valueProjCol])[valueIdx];
            return new String(raw, StandardCharsets.UTF_8);
        }

        @Override
        public byte[] getBinaryValue() {
            int valueProjCol = mapDesc.valueProjCol();
            if (batch.isElementNull(valueProjCol, valueIdx)) {
                return null;
            }
            return ((byte[][]) batch.valueArrays[valueProjCol])[valueIdx];
        }

        @Override
        public LocalDate getDateValue() {
            Object raw = readValueAt(valueIdx);
            return ValueConverter.convertToDate(raw, valueSchema);
        }

        @Override
        public LocalTime getTimeValue() {
            Object raw = readValueAt(valueIdx);
            return ValueConverter.convertToTime(raw, valueSchema);
        }

        @Override
        public Instant getTimestampValue() {
            Object raw = readValueAt(valueIdx);
            return ValueConverter.convertToTimestamp(raw, valueSchema);
        }

        @Override
        public BigDecimal getDecimalValue() {
            Object raw = readValueAt(valueIdx);
            return ValueConverter.convertToDecimal(raw, valueSchema);
        }

        @Override
        public UUID getUuidValue() {
            Object raw = readValueAt(valueIdx);
            return ValueConverter.convertToUuid(raw, valueSchema);
        }

        @Override
        public PqInterval getIntervalValue() {
            Object raw = readValueAt(valueIdx);
            return ValueConverter.convertToInterval(raw, valueSchema);
        }

        @Override
        public PqStruct getStructValue() {
            return structValueAt(valueIdx);
        }

        @Override
        public PqList getListValue() {
            return listValueAt(valueIdx);
        }

        @Override
        public PqMap getMapValue() {
            return mapValueAt(valueIdx);
        }

        @Override
        public PqVariant getVariantValue() {
            return variantValueAt(valueIdx);
        }

        @Override
        public Object getValue() {
            return valueAt(valueIdx);
        }

        @Override
        public Object getRawValue() {
            return rawValueAt(valueIdx);
        }

        @Override
        public boolean isValueNull() {
            return isValueNullAt(valueIdx);
        }

        // ==================== Internal ====================

        private Object readKey() {
            int keyProjCol = mapDesc.keyProjCol();
            if (batch.isElementNull(keyProjCol, valueIdx)) {
                return null;
            }
            return batch.getValue(keyProjCol, valueIdx);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            FlyweightFormatter.appendValue(sb, getKey());
            sb.append('=');
            FlyweightFormatter.appendValue(sb, getValue());
            return sb.toString();
        }
    }
}
