/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.row;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.UUID;

/**
 * Type-safe interface for accessing Parquet MAP values.
 * <p>
 * A MAP in Parquet is stored as a list of key-value entries. This interface
 * provides type-safe access to the entries with dedicated accessor methods.
 * </p>
 *
 * <pre>{@code
 * PqMap attributes = row.getMap("attributes");
 * for (PqMap.Entry entry : attributes.getEntries()) {
 *     String key = entry.getStringKey();
 *     int value = entry.getIntValue();
 * }
 * }</pre>
 */
public interface PqMap {

    /**
     * Get all entries in this map.
     *
     * @return list of map entries
     */
    List<Entry> getEntries();

    /**
     * Get the number of entries in this map.
     *
     * @return the entry count
     */
    int size();

    /**
     * Check if this map is empty.
     *
     * @return true if the map has no entries
     */
    boolean isEmpty();

    /**
     * A single key-value entry in a map.
     */
    interface Entry {

        // ==================== Key Accessors - Primitives ====================

        /**
         * Get the key as an INT32.
         *
         * @return the int key value
         * @throws IllegalArgumentException if the key type is not INT32
         */
        int getIntKey();

        /**
         * Get the key as an INT64.
         *
         * @return the long key value
         * @throws IllegalArgumentException if the key type is not INT64
         */
        long getLongKey();

        // ==================== Key Accessors - Objects ====================

        /**
         * Get the key as a STRING.
         *
         * @return the string key value
         * @throws IllegalArgumentException if the key type is not STRING
         */
        String getStringKey();

        /**
         * Get the key as a BINARY.
         *
         * @return the byte array key value
         * @throws IllegalArgumentException if the key type is not BINARY
         */
        byte[] getBinaryKey();

        /**
         * Get the key as a DATE.
         *
         * @return the date key value
         * @throws IllegalArgumentException if the key type is not DATE
         */
        LocalDate getDateKey();

        /**
         * Get the key as a TIMESTAMP.
         *
         * @return the instant key value
         * @throws IllegalArgumentException if the key type is not TIMESTAMP
         */
        Instant getTimestampKey();

        /**
         * Get the key as a UUID.
         *
         * @return the UUID key value
         * @throws IllegalArgumentException if the key type is not UUID
         */
        UUID getUuidKey();

        /**
         * Get the key without type conversion.
         *
         * @return the raw key value
         */
        Object getKey();

        // ==================== Value Accessors - Primitives ====================

        /**
         * Get the value as an INT32.
         *
         * @return the int value
         * @throws NullPointerException if the value is null
         * @throws IllegalArgumentException if the value type is not INT32
         */
        int getIntValue();

        /**
         * Get the value as an INT64.
         *
         * @return the long value
         * @throws NullPointerException if the value is null
         * @throws IllegalArgumentException if the value type is not INT64
         */
        long getLongValue();

        /**
         * Get the value as a FLOAT.
         *
         * @return the float value
         * @throws NullPointerException if the value is null
         * @throws IllegalArgumentException if the value type is not FLOAT
         */
        float getFloatValue();

        /**
         * Get the value as a DOUBLE.
         *
         * @return the double value
         * @throws NullPointerException if the value is null
         * @throws IllegalArgumentException if the value type is not DOUBLE
         */
        double getDoubleValue();

        /**
         * Get the value as a BOOLEAN.
         *
         * @return the boolean value
         * @throws NullPointerException if the value is null
         * @throws IllegalArgumentException if the value type is not BOOLEAN
         */
        boolean getBooleanValue();

        // ==================== Value Accessors - Objects ====================

        /**
         * Get the value as a STRING.
         *
         * @return the string value, or null if the value is null
         * @throws IllegalArgumentException if the value type is not STRING
         */
        String getStringValue();

        /**
         * Get the value as a BINARY.
         *
         * @return the byte array value, or null if the value is null
         * @throws IllegalArgumentException if the value type is not BINARY
         */
        byte[] getBinaryValue();

        /**
         * Get the value as a DATE.
         *
         * @return the date value, or null if the value is null
         * @throws IllegalArgumentException if the value type is not DATE
         */
        LocalDate getDateValue();

        /**
         * Get the value as a TIME.
         *
         * @return the time value, or null if the value is null
         * @throws IllegalArgumentException if the value type is not TIME
         */
        LocalTime getTimeValue();

        /**
         * Get the value as a TIMESTAMP.
         *
         * @return the instant value, or null if the value is null
         * @throws IllegalArgumentException if the value type is not TIMESTAMP
         */
        Instant getTimestampValue();

        /**
         * Get the value as a DECIMAL.
         *
         * @return the decimal value, or null if the value is null
         * @throws IllegalArgumentException if the value type is not DECIMAL
         */
        BigDecimal getDecimalValue();

        /**
         * Get the value as a UUID.
         *
         * @return the UUID value, or null if the value is null
         * @throws IllegalArgumentException if the value type is not UUID
         */
        UUID getUuidValue();

        // ==================== Value Accessors - Nested Types ====================

        /**
         * Get the value as a nested struct.
         *
         * @return the nested struct, or null if the value is null
         * @throws IllegalArgumentException if the value type is not a struct
         */
        PqStruct getStructValue();

        /**
         * Get the value as a LIST.
         *
         * @return the list value, or null if the value is null
         * @throws IllegalArgumentException if the value type is not a list
         */
        PqList getListValue();

        /**
         * Get the value as a MAP.
         *
         * @return the nested map, or null if the value is null
         * @throws IllegalArgumentException if the value type is not a map
         */
        PqMap getMapValue();

        /**
         * Get the value without type conversion.
         *
         * @return the raw value, or null if the value is null
         */
        Object getValue();

        /**
         * Check if the value is null.
         *
         * @return true if the value is null
         */
        boolean isValueNull();
    }
}
