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
import java.util.UUID;

/**
 * Type-safe struct interface for reading nested Parquet data.
 * <p>
 * Provides dedicated accessor methods for each type, similar to JDBC ResultSet.
 * This interface is used for nested struct access, not for top-level row iteration.
 * For top-level row access, use {@link dev.hardwood.reader.RowReader} directly.
 * </p>
 *
 * <pre>{@code
 * while (rowReader.hasNext()) {
 *     rowReader.next();
 *     int id = rowReader.getInt("id");
 *
 *     // Nested struct
 *     PqStruct address = rowReader.getStruct("address");
 *     String city = address.getString("city");
 *
 *     // List of structs
 *     PqList items = rowReader.getList("items");
 *     for (PqStruct item : items.structs()) { ... }
 * }
 * }</pre>
 */
public interface PqStruct {

    // ==================== Primitive Types ====================

    /**
     * Get an INT32 field value by name.
     *
     * @param name the field name
     * @return the int value
     * @throws NullPointerException if the field is null
     * @throws IllegalArgumentException if the field type is not INT32
     */
    int getInt(String name);

    /**
     * Get an INT64 field value by name.
     *
     * @param name the field name
     * @return the long value
     * @throws NullPointerException if the field is null
     * @throws IllegalArgumentException if the field type is not INT64
     */
    long getLong(String name);

    /**
     * Get a FLOAT field value by name.
     *
     * @param name the field name
     * @return the float value
     * @throws NullPointerException if the field is null
     * @throws IllegalArgumentException if the field type is not FLOAT
     */
    float getFloat(String name);

    /**
     * Get a DOUBLE field value by name.
     *
     * @param name the field name
     * @return the double value
     * @throws NullPointerException if the field is null
     * @throws IllegalArgumentException if the field type is not DOUBLE
     */
    double getDouble(String name);

    /**
     * Get a BOOLEAN field value by name.
     *
     * @param name the field name
     * @return the boolean value
     * @throws NullPointerException if the field is null
     * @throws IllegalArgumentException if the field type is not BOOLEAN
     */
    boolean getBoolean(String name);

    // ==================== Object Types ====================

    /**
     * Get a STRING field value by name.
     *
     * @param name the field name
     * @return the string value, or null if the field is null
     * @throws IllegalArgumentException if the field type is not STRING
     */
    String getString(String name);

    /**
     * Get a BINARY field value by name.
     *
     * @param name the field name
     * @return the byte array, or null if the field is null
     * @throws IllegalArgumentException if the field type is not BINARY
     */
    byte[] getBinary(String name);

    /**
     * Get a DATE field value by name.
     *
     * @param name the field name
     * @return the date value, or null if the field is null
     * @throws IllegalArgumentException if the field type is not DATE
     */
    LocalDate getDate(String name);

    /**
     * Get a TIME field value by name.
     *
     * @param name the field name
     * @return the time value, or null if the field is null
     * @throws IllegalArgumentException if the field type is not TIME
     */
    LocalTime getTime(String name);

    /**
     * Get a TIMESTAMP field value by name.
     *
     * @param name the field name
     * @return the instant value, or null if the field is null
     * @throws IllegalArgumentException if the field type is not TIMESTAMP
     */
    Instant getTimestamp(String name);

    /**
     * Get a DECIMAL field value by name.
     *
     * @param name the field name
     * @return the decimal value, or null if the field is null
     * @throws IllegalArgumentException if the field type is not DECIMAL
     */
    BigDecimal getDecimal(String name);

    /**
     * Get a UUID field value by name.
     *
     * @param name the field name
     * @return the UUID value, or null if the field is null
     * @throws IllegalArgumentException if the field type is not UUID
     */
    UUID getUuid(String name);

    // ==================== Nested Types ====================

    /**
     * Get a nested struct field value by name.
     *
     * @param name the field name
     * @return the nested struct, or null if the field is null
     * @throws IllegalArgumentException if the field type is not a struct
     */
    PqStruct getStruct(String name);

    // ==================== Primitive List Types ====================

    /**
     * Get an INT32 list field by name.
     *
     * @param name the field name
     * @return the int list, or null if the field is null
     * @throws IllegalArgumentException if the field is not a list of INT32
     */
    PqIntList getListOfInts(String name);

    /**
     * Get an INT64 list field by name.
     *
     * @param name the field name
     * @return the long list, or null if the field is null
     * @throws IllegalArgumentException if the field is not a list of INT64
     */
    PqLongList getListOfLongs(String name);

    /**
     * Get a DOUBLE list field by name.
     *
     * @param name the field name
     * @return the double list, or null if the field is null
     * @throws IllegalArgumentException if the field is not a list of DOUBLE
     */
    PqDoubleList getListOfDoubles(String name);

    // ==================== Generic List ====================

    /**
     * Get a LIST field value by name.
     *
     * <pre>{@code
     * PqList tags = struct.getList("tags");
     * for (String tag : tags.strings()) {
     *     System.out.println(tag);
     * }
     * }</pre>
     *
     * @param name the field name
     * @return the list, or null if the field is null
     * @throws IllegalArgumentException if the field type is not a list
     */
    PqList getList(String name);

    /**
     * Get a MAP field value by name.
     *
     * @param name the field name
     * @return the map, or null if the field is null
     * @throws IllegalArgumentException if the field type is not a map
     */
    PqMap getMap(String name);

    // ==================== Generic Fallback ====================

    /**
     * Get a field value by name without type conversion.
     * Returns the raw value as stored internally.
     *
     * @param name the field name
     * @return the raw value, or null if the field is null
     */
    Object getValue(String name);

    // ==================== Metadata ====================

    /**
     * Check if a field is null by name.
     *
     * @param name the field name
     * @return true if the field is null
     */
    boolean isNull(String name);

    /**
     * Get the number of fields in this struct.
     *
     * @return the field count
     */
    int getFieldCount();

    /**
     * Get the name of a field by index.
     *
     * @param index the field index (0-based)
     * @return the field name
     */
    String getFieldName(int index);
}
