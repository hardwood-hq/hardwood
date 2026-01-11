/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.row;

/**
 * Type-safe row interface for reading Parquet data.
 * <p>
 * Provides access to field values using {@link PqType} for compile-time type safety.
 * </p>
 *
 * <pre>{@code
 * PqRow row = ...;
 * int id = row.getValue(PqType.INT32, "id");
 * String name = row.getValue(PqType.STRING, "name");
 * LocalDate date = row.getValue(PqType.DATE, "birth_date");
 *
 * // Nested struct
 * PqRow address = row.getValue(PqType.ROW, "address");
 * String city = address.getValue(PqType.STRING, "city");
 *
 * // List
 * PqList tags = row.getValue(PqType.LIST, "tags");
 * for (String tag : tags.getValues(PqType.STRING)) { ... }
 * }</pre>
 */
public interface PqRow {

    /**
     * Get a field value by index with type safety.
     *
     * @param type the expected type of the field
     * @param index the field index (0-based)
     * @param <T> the Java type corresponding to the PqType
     * @return the field value, or null if the field is null
     * @throws IllegalArgumentException if the requested type doesn't match the schema
     * @throws IndexOutOfBoundsException if the index is out of range
     */
    <T> T getValue(PqType<T> type, int index);

    /**
     * Get a field value by name with type safety.
     *
     * @param type the expected type of the field
     * @param name the field name
     * @param <T> the Java type corresponding to the PqType
     * @return the field value, or null if the field is null
     * @throws IllegalArgumentException if the requested type doesn't match the schema,
     *         or if the field name doesn't exist
     */
    <T> T getValue(PqType<T> type, String name);

    /**
     * Check if a field is null by index.
     *
     * @param index the field index (0-based)
     * @return true if the field is null
     */
    boolean isNull(int index);

    /**
     * Check if a field is null by name.
     *
     * @param name the field name
     * @return true if the field is null
     */
    boolean isNull(String name);

    /**
     * Get the number of fields in this row.
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
