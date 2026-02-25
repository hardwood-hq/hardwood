/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.parquet.example.data;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;

/**
 * Interface for reading Parquet record data.
 * <p>
 * Compatible with parquet-java's Group API. This interface provides type-safe
 * access to field values by name or index.
 * </p>
 *
 * <p>
 * For repeated fields, the second parameter (index) specifies which occurrence
 * to return. For non-repeated fields, the index should always be 0.
 * </p>
 *
 * <pre>{@code
 * // Access non-repeated fields
 * String name = group.getString("name", 0);
 * int age = group.getInteger("age", 0);
 *
 * // Access repeated fields
 * int tagCount = group.getFieldRepetitionCount("tags");
 * for (int i = 0; i < tagCount; i++) {
 *     String tag = group.getString("tags", i);
 * }
 *
 * // Access nested groups
 * Group address = group.getGroup("address", 0);
 * String city = address.getString("city", 0);
 * }</pre>
 */
public interface Group {

    // ---- Schema access ----

    /**
     * Get the schema type for this group.
     *
     * @return the group type schema
     */
    GroupType getType();

    // ---- Field repetition count ----

    /**
     * Get the repetition count for a field by index.
     * <p>
     * For repeated fields, returns the number of values.
     * For optional fields, returns 1 if present, 0 if null.
     * For required fields, always returns 1.
     * </p>
     *
     * @param fieldIndex the field index
     * @return the repetition count
     */
    int getFieldRepetitionCount(int fieldIndex);

    /**
     * Get the repetition count for a field by name.
     *
     * @param field the field name
     * @return the repetition count
     */
    int getFieldRepetitionCount(String field);

    // ---- Value access by field index ----

    /**
     * Get a string value.
     *
     * @param fieldIndex the field index
     * @param index the value index (for repeated fields)
     * @return the string value
     */
    String getString(int fieldIndex, int index);

    /**
     * Get an integer value.
     *
     * @param fieldIndex the field index
     * @param index the value index (for repeated fields)
     * @return the integer value
     */
    int getInteger(int fieldIndex, int index);

    /**
     * Get a long value.
     *
     * @param fieldIndex the field index
     * @param index the value index (for repeated fields)
     * @return the long value
     */
    long getLong(int fieldIndex, int index);

    /**
     * Get a double value.
     *
     * @param fieldIndex the field index
     * @param index the value index (for repeated fields)
     * @return the double value
     */
    double getDouble(int fieldIndex, int index);

    /**
     * Get a float value.
     *
     * @param fieldIndex the field index
     * @param index the value index (for repeated fields)
     * @return the float value
     */
    float getFloat(int fieldIndex, int index);

    /**
     * Get a boolean value.
     *
     * @param fieldIndex the field index
     * @param index the value index (for repeated fields)
     * @return the boolean value
     */
    boolean getBoolean(int fieldIndex, int index);

    /**
     * Get a binary value.
     *
     * @param fieldIndex the field index
     * @param index the value index (for repeated fields)
     * @return the binary value
     */
    Binary getBinary(int fieldIndex, int index);

    /**
     * Get a nested group.
     *
     * @param fieldIndex the field index
     * @param index the value index (for repeated fields)
     * @return the nested group
     */
    Group getGroup(int fieldIndex, int index);

    // ---- Value access by field name ----

    /**
     * Get a string value by field name.
     *
     * @param field the field name
     * @param index the value index (for repeated fields)
     * @return the string value
     */
    String getString(String field, int index);

    /**
     * Get an integer value by field name.
     *
     * @param field the field name
     * @param index the value index (for repeated fields)
     * @return the integer value
     */
    int getInteger(String field, int index);

    /**
     * Get a long value by field name.
     *
     * @param field the field name
     * @param index the value index (for repeated fields)
     * @return the long value
     */
    long getLong(String field, int index);

    /**
     * Get a double value by field name.
     *
     * @param field the field name
     * @param index the value index (for repeated fields)
     * @return the double value
     */
    double getDouble(String field, int index);

    /**
     * Get a float value by field name.
     *
     * @param field the field name
     * @param index the value index (for repeated fields)
     * @return the float value
     */
    float getFloat(String field, int index);

    /**
     * Get a boolean value by field name.
     *
     * @param field the field name
     * @param index the value index (for repeated fields)
     * @return the boolean value
     */
    boolean getBoolean(String field, int index);

    /**
     * Get a binary value by field name.
     *
     * @param field the field name
     * @param index the value index (for repeated fields)
     * @return the binary value
     */
    Binary getBinary(String field, int index);

    /**
     * Get a nested group by field name.
     *
     * @param field the field name
     * @param index the value index (for repeated fields)
     * @return the nested group
     */
    Group getGroup(String field, int index);
}
