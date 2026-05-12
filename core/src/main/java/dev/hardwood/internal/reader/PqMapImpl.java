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
import java.util.List;
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
            Object raw = readValue();
            return ValueConverter.convertToDate(raw, valueSchema);
        }

        @Override
        public LocalTime getTimeValue() {
            Object raw = readValue();
            return ValueConverter.convertToTime(raw, valueSchema);
        }

        @Override
        public Instant getTimestampValue() {
            Object raw = readValue();
            return ValueConverter.convertToTimestamp(raw, valueSchema);
        }

        @Override
        public BigDecimal getDecimalValue() {
            Object raw = readValue();
            return ValueConverter.convertToDecimal(raw, valueSchema);
        }

        @Override
        public UUID getUuidValue() {
            Object raw = readValue();
            return ValueConverter.convertToUuid(raw, valueSchema);
        }

        @Override
        public PqInterval getIntervalValue() {
            Object raw = readValue();
            return ValueConverter.convertToInterval(raw, valueSchema);
        }

        @Override
        public PqStruct getStructValue() {
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

        @Override
        public PqList getListValue() {
            TopLevelFieldMap.FieldDesc vDesc = mapDesc.valueDesc();
            if (!(vDesc instanceof TopLevelFieldMap.FieldDesc.ListOf listDesc)) {
                throw new IllegalArgumentException("Value is not a list");
            }
            return PqListImpl.createGenericList(batch, listDesc, -1, valueIdx);
        }

        @Override
        public PqMap getMapValue() {
            TopLevelFieldMap.FieldDesc vDesc = mapDesc.valueDesc();
            if (!(vDesc instanceof TopLevelFieldMap.FieldDesc.MapOf innerMapDesc)) {
                throw new IllegalArgumentException("Value is not a map");
            }
            return PqMapImpl.create(batch, innerMapDesc, -1, valueIdx);
        }

        @Override
        public PqVariant getVariantValue() {
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

        @Override
        public Object getValue() {
            Object group = groupValueOrNull();
            if (group != null) {
                return group;
            }
            if (isValueNull()) {
                return null;
            }
            return ValueConverter.convertValue(readValue(), valueSchema);
        }

        @Override
        public Object getRawValue() {
            Object group = groupValueOrNull();
            if (group != null) {
                return group;
            }
            if (isValueNull()) {
                return null;
            }
            return readValue();
        }

        /// Returns the typed flyweight ([PqStruct] / [PqList] / [PqMap]) when
        /// the value is a nested group, or `null` for primitive-typed values
        /// (so the caller can fall through to raw/decoded primitive handling).
        /// A non-null group whose primitive descendants are all null still
        /// yields the flyweight — the typed accessors handle null shape.
        private Object groupValueOrNull() {
            TopLevelFieldMap.FieldDesc vDesc = mapDesc.valueDesc();
            if (vDesc instanceof TopLevelFieldMap.FieldDesc.Struct) {
                return getStructValue();
            }
            if (vDesc instanceof TopLevelFieldMap.FieldDesc.ListOf) {
                return getListValue();
            }
            if (vDesc instanceof TopLevelFieldMap.FieldDesc.MapOf) {
                return getMapValue();
            }
            if (vDesc instanceof TopLevelFieldMap.FieldDesc.Variant) {
                return getVariantValue();
            }
            return null;
        }

        @Override
        public boolean isValueNull() {
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

        // ==================== Internal ====================

        private Object readKey() {
            int keyProjCol = mapDesc.keyProjCol();
            if (batch.isElementNull(keyProjCol, valueIdx)) {
                return null;
            }
            return batch.getValue(keyProjCol, valueIdx);
        }

        private Object readValue() {
            int valueProjCol = mapDesc.valueProjCol();
            if (batch.isElementNull(valueProjCol, valueIdx)) {
                return null;
            }
            return batch.getValue(valueProjCol, valueIdx);
        }
    }
}
