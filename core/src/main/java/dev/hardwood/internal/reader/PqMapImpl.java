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

import dev.hardwood.internal.reader.NestedColumnData.ByteArrayColumn;
import dev.hardwood.internal.reader.NestedColumnData.IntColumn;
import dev.hardwood.internal.reader.NestedColumnData.LongColumn;
import dev.hardwood.internal.reader.TopLevelFieldMap.FieldDesc;
import dev.hardwood.internal.reader.TopLevelFieldMap.FieldDesc.MapOf;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.SchemaNode;

/**
 * Flyweight {@link PqMap} that reads key-value entries directly from parallel column arrays.
 */
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
        int leafMaxRep = batch.columns[keyProjCol].column().maxRepetitionLevel();

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

        int defLevel = batch.columns[keyProjCol].getDefLevel(firstValue);
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
        int leafMaxRep = batch.columns[keyProjCol].column().maxRepetitionLevel();

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

        int defLevel = batch.columns[keyProjCol].getDefLevel(firstValue);
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
            return ((NestedColumnData.IntColumn) batch.columns[keyProjCol]).get(valueIdx);
        }

        @Override
        public long getLongKey() {
            int keyProjCol = mapDesc.keyProjCol();
            if (batch.isElementNull(keyProjCol, valueIdx)) {
                throw new NullPointerException("Key is null");
            }
            return ((NestedColumnData.LongColumn) batch.columns[keyProjCol]).get(valueIdx);
        }

        @Override
        public String getStringKey() {
            int keyProjCol = mapDesc.keyProjCol();
            if (batch.isElementNull(keyProjCol, valueIdx)) {
                return null;
            }
            byte[] raw = ((NestedColumnData.ByteArrayColumn) batch.columns[keyProjCol]).get(valueIdx);
            return new String(raw, StandardCharsets.UTF_8);
        }

        @Override
        public byte[] getBinaryKey() {
            int keyProjCol = mapDesc.keyProjCol();
            if (batch.isElementNull(keyProjCol, valueIdx)) {
                return null;
            }
            return ((NestedColumnData.ByteArrayColumn) batch.columns[keyProjCol]).get(valueIdx);
        }

        @Override
        public LocalDate getDateKey() {
            Object raw = readKey();
            return ValueConverter.convertToDate(raw, keySchema);
        }

        @Override
        public Instant getTimestampKey() {
            Object raw = readKey();
            return ValueConverter.convertToTimestamp(raw, keySchema);
        }

        @Override
        public UUID getUuidKey() {
            Object raw = readKey();
            return ValueConverter.convertToUuid(raw, keySchema);
        }

        @Override
        public Object getKey() {
            return readKey();
        }

        // ==================== Value Accessors ====================

        @Override
        public int getIntValue() {
            int valueProjCol = mapDesc.valueProjCol();
            if (batch.isElementNull(valueProjCol, valueIdx)) {
                throw new NullPointerException("Value is null");
            }
            return ((NestedColumnData.IntColumn) batch.columns[valueProjCol]).get(valueIdx);
        }

        @Override
        public long getLongValue() {
            int valueProjCol = mapDesc.valueProjCol();
            if (batch.isElementNull(valueProjCol, valueIdx)) {
                throw new NullPointerException("Value is null");
            }
            return ((NestedColumnData.LongColumn) batch.columns[valueProjCol]).get(valueIdx);
        }

        @Override
        public float getFloatValue() {
            int valueProjCol = mapDesc.valueProjCol();
            if (batch.isElementNull(valueProjCol, valueIdx)) {
                throw new NullPointerException("Value is null");
            }
            return ((NestedColumnData.FloatColumn) batch.columns[valueProjCol]).get(valueIdx);
        }

        @Override
        public double getDoubleValue() {
            int valueProjCol = mapDesc.valueProjCol();
            if (batch.isElementNull(valueProjCol, valueIdx)) {
                throw new NullPointerException("Value is null");
            }
            return ((NestedColumnData.DoubleColumn) batch.columns[valueProjCol]).get(valueIdx);
        }

        @Override
        public boolean getBooleanValue() {
            int valueProjCol = mapDesc.valueProjCol();
            if (batch.isElementNull(valueProjCol, valueIdx)) {
                throw new NullPointerException("Value is null");
            }
            return ((NestedColumnData.BooleanColumn) batch.columns[valueProjCol]).get(valueIdx);
        }

        @Override
        public String getStringValue() {
            int valueProjCol = mapDesc.valueProjCol();
            if (batch.isElementNull(valueProjCol, valueIdx)) {
                return null;
            }
            byte[] raw = ((NestedColumnData.ByteArrayColumn) batch.columns[valueProjCol]).get(valueIdx);
            return new String(raw, StandardCharsets.UTF_8);
        }

        @Override
        public byte[] getBinaryValue() {
            int valueProjCol = mapDesc.valueProjCol();
            if (batch.isElementNull(valueProjCol, valueIdx)) {
                return null;
            }
            return ((NestedColumnData.ByteArrayColumn) batch.columns[valueProjCol]).get(valueIdx);
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
        public PqStruct getStructValue() {
            TopLevelFieldMap.FieldDesc vDesc = mapDesc.valueDesc();
            if (!(vDesc instanceof TopLevelFieldMap.FieldDesc.Struct structDesc)) {
                throw new IllegalArgumentException("Value is not a struct");
            }
            // Check null via firstPrimitiveCol
            int projCol = structDesc.firstPrimitiveCol();
            if (projCol >= 0) {
                int defLevel = batch.columns[projCol].getDefLevel(valueIdx);
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
        public Object getValue() {
            return readValue();
        }

        @Override
        public boolean isValueNull() {
            int valueProjCol = mapDesc.valueProjCol();
            return batch.isElementNull(valueProjCol, valueIdx);
        }

        // ==================== Internal ====================

        private Object readKey() {
            int keyProjCol = mapDesc.keyProjCol();
            if (batch.isElementNull(keyProjCol, valueIdx)) {
                return null;
            }
            return batch.columns[keyProjCol].getValue(valueIdx);
        }

        private Object readValue() {
            int valueProjCol = mapDesc.valueProjCol();
            if (batch.isElementNull(valueProjCol, valueIdx)) {
                return null;
            }
            return batch.columns[valueProjCol].getValue(valueIdx);
        }
    }
}
