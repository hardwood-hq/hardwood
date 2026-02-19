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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.SchemaNode;

/**
 * Implementation of PqMap interface.
 */
public class PqMapImpl implements PqMap {

    private final List<Entry> entries;

    /**
     * Constructor for a map from assembled data.
     *
     * @param map the MutableMap containing key-value entries
     * @param mapSchema the MAP schema node
     */
    public PqMapImpl(MutableMap map, SchemaNode.GroupNode mapSchema) {
        if (map == null || map.size() == 0) {
            this.entries = Collections.emptyList();
            return;
        }

        // Get key/value schemas from MAP -> key_value -> (key, value)
        SchemaNode.GroupNode keyValueGroup = (SchemaNode.GroupNode) mapSchema.children().get(0);
        SchemaNode keySchema = keyValueGroup.children().get(0);
        SchemaNode valueSchema = keyValueGroup.children().get(1);

        List<Entry> entryList = new ArrayList<>();
        for (MutableStruct entry : map.entries()) {
            if (entry != null) {
                entryList.add(new EntryImpl(entry.getChild(0), entry.getChild(1), keySchema, valueSchema));
            }
        }
        this.entries = Collections.unmodifiableList(entryList);
    }

    @Override
    public List<Entry> getEntries() {
        return entries;
    }

    @Override
    public int size() {
        return entries.size();
    }

    @Override
    public boolean isEmpty() {
        return entries.isEmpty();
    }

    /**
     * Implementation of a map entry.
     */
    private static class EntryImpl implements Entry {

        private final Object key;
        private final Object value;
        private final SchemaNode keySchema;
        private final SchemaNode valueSchema;

        EntryImpl(Object key, Object value, SchemaNode keySchema, SchemaNode valueSchema) {
            this.key = key;
            this.value = value;
            this.keySchema = keySchema;
            this.valueSchema = valueSchema;
        }

        // ==================== Key Accessors - Primitives ====================

        @Override
        public int getIntKey() {
            Integer val = ValueConverter.convertToInt(key, keySchema);
            if (val == null) {
                throw new NullPointerException("Key is null");
            }
            return val;
        }

        @Override
        public long getLongKey() {
            Long val = ValueConverter.convertToLong(key, keySchema);
            if (val == null) {
                throw new NullPointerException("Key is null");
            }
            return val;
        }

        // ==================== Key Accessors - Objects ====================

        @Override
        public String getStringKey() {
            return ValueConverter.convertToString(key, keySchema);
        }

        @Override
        public byte[] getBinaryKey() {
            return ValueConverter.convertToBinary(key, keySchema);
        }

        @Override
        public LocalDate getDateKey() {
            return ValueConverter.convertToDate(key, keySchema);
        }

        @Override
        public Instant getTimestampKey() {
            return ValueConverter.convertToTimestamp(key, keySchema);
        }

        @Override
        public UUID getUuidKey() {
            return ValueConverter.convertToUuid(key, keySchema);
        }

        @Override
        public Object getKey() {
            return key;
        }

        // ==================== Value Accessors - Primitives ====================

        @Override
        public int getIntValue() {
            Integer val = ValueConverter.convertToInt(value, valueSchema);
            if (val == null) {
                throw new NullPointerException("Value is null");
            }
            return val;
        }

        @Override
        public long getLongValue() {
            Long val = ValueConverter.convertToLong(value, valueSchema);
            if (val == null) {
                throw new NullPointerException("Value is null");
            }
            return val;
        }

        @Override
        public float getFloatValue() {
            Float val = ValueConverter.convertToFloat(value, valueSchema);
            if (val == null) {
                throw new NullPointerException("Value is null");
            }
            return val;
        }

        @Override
        public double getDoubleValue() {
            Double val = ValueConverter.convertToDouble(value, valueSchema);
            if (val == null) {
                throw new NullPointerException("Value is null");
            }
            return val;
        }

        @Override
        public boolean getBooleanValue() {
            Boolean val = ValueConverter.convertToBoolean(value, valueSchema);
            if (val == null) {
                throw new NullPointerException("Value is null");
            }
            return val;
        }

        // ==================== Value Accessors - Objects ====================

        @Override
        public String getStringValue() {
            return ValueConverter.convertToString(value, valueSchema);
        }

        @Override
        public byte[] getBinaryValue() {
            return ValueConverter.convertToBinary(value, valueSchema);
        }

        @Override
        public LocalDate getDateValue() {
            return ValueConverter.convertToDate(value, valueSchema);
        }

        @Override
        public LocalTime getTimeValue() {
            return ValueConverter.convertToTime(value, valueSchema);
        }

        @Override
        public Instant getTimestampValue() {
            return ValueConverter.convertToTimestamp(value, valueSchema);
        }

        @Override
        public BigDecimal getDecimalValue() {
            return ValueConverter.convertToDecimal(value, valueSchema);
        }

        @Override
        public UUID getUuidValue() {
            return ValueConverter.convertToUuid(value, valueSchema);
        }

        // ==================== Value Accessors - Nested Types ====================

        @Override
        public PqStruct getStructValue() {
            return ValueConverter.convertToStruct(value, valueSchema);
        }

        @Override
        public PqList getListValue() {
            return ValueConverter.convertToList(value, valueSchema);
        }

        @Override
        public PqMap getMapValue() {
            return ValueConverter.convertToMap(value, valueSchema);
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public boolean isValueNull() {
            return value == null;
        }
    }
}
