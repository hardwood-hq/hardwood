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

/// Common interface for name-based access to fields whose values are primitives,
/// Variants, or absent. Shared by both Parquet-struct accessors ([StructAccessor])
/// and Variant-object accessors ([PqVariantObject]), and by top-level
/// [dev.hardwood.reader.RowReader].
///
/// Complex Parquet-only accessors (`getStruct` / `getList` / `getMap`) live on
/// [StructAccessor]. Variant-specific accessors (`getObject` / `getArray`) live
/// on [PqVariantObject].
public interface FieldAccessor {

    // ==================== Primitive Types ====================

    /// Get an INT32 field value by name.
    ///
    /// @param name the field name
    /// @return the int value
    /// @throws NullPointerException if the field is null
    /// @throws IllegalArgumentException if the field type is not INT32
    int getInt(String name);

    /// Get an INT64 field value by name.
    ///
    /// @param name the field name
    /// @return the long value
    /// @throws NullPointerException if the field is null
    /// @throws IllegalArgumentException if the field type is not INT64
    long getLong(String name);

    /// Get a FLOAT field value by name.
    ///
    /// @param name the field name
    /// @return the float value
    /// @throws NullPointerException if the field is null
    /// @throws IllegalArgumentException if the field type is not FLOAT
    float getFloat(String name);

    /// Get a DOUBLE field value by name.
    ///
    /// @param name the field name
    /// @return the double value
    /// @throws NullPointerException if the field is null
    /// @throws IllegalArgumentException if the field type is not DOUBLE
    double getDouble(String name);

    /// Get a BOOLEAN field value by name.
    ///
    /// @param name the field name
    /// @return the boolean value
    /// @throws NullPointerException if the field is null
    /// @throws IllegalArgumentException if the field type is not BOOLEAN
    boolean getBoolean(String name);

    // ==================== Object Types ====================

    /// Get a STRING field value by name.
    ///
    /// @param name the field name
    /// @return the string value, or null if the field is null
    /// @throws IllegalArgumentException if the field type is not STRING
    String getString(String name);

    /// Get a BINARY field value by name.
    ///
    /// @param name the field name
    /// @return the byte array, or null if the field is null
    /// @throws IllegalArgumentException if the field type is not BINARY
    byte[] getBinary(String name);

    /// Get a DATE field value by name.
    ///
    /// @param name the field name
    /// @return the date value, or null if the field is null
    /// @throws IllegalArgumentException if the field type is not DATE
    LocalDate getDate(String name);

    /// Get a TIME field value by name.
    ///
    /// @param name the field name
    /// @return the time value, or null if the field is null
    /// @throws IllegalArgumentException if the field type is not TIME
    LocalTime getTime(String name);

    /// Get a TIMESTAMP field value by name.
    ///
    /// @param name the field name
    /// @return the instant value, or null if the field is null
    /// @throws IllegalArgumentException if the field type is not TIMESTAMP
    Instant getTimestamp(String name);

    /// Get a DECIMAL field value by name.
    ///
    /// @param name the field name
    /// @return the decimal value, or null if the field is null
    /// @throws IllegalArgumentException if the field type is not DECIMAL
    BigDecimal getDecimal(String name);

    /// Get a UUID field value by name.
    ///
    /// @param name the field name
    /// @return the UUID value, or null if the field is null
    /// @throws IllegalArgumentException if the field type is not UUID
    UUID getUuid(String name);

    // ==================== Variant ====================

    /// Get a VARIANT field value by name. Works both for a Parquet group annotated
    /// with the `VARIANT` logical type (where the returned value is assembled from
    /// the underlying `metadata` + `value` binary columns) and for a Variant
    /// object's field whose value is itself a Variant — every Variant sub-value
    /// can be surfaced through this method.
    ///
    /// @param name the field name
    /// @return the Variant accessor, or null if the field is null
    /// @throws IllegalArgumentException if the field is not annotated as VARIANT
    PqVariant getVariant(String name);

    // ==================== Generic Fallback ====================

    /// Get a field value by name without type conversion.
    /// Returns the raw value as stored internally.
    ///
    /// @param name the field name
    /// @return the raw value, or null if the field is null
    Object getValue(String name);

    // ==================== Metadata ====================

    /// Check if a field is null by name.
    ///
    /// @param name the field name
    /// @return true if the field is null
    boolean isNull(String name);

    /// Get the number of fields in this record.
    ///
    /// @return the field count
    int getFieldCount();

    /// Get the name of a field by index.
    ///
    /// @param index the field index (0-based)
    /// @return the field name
    String getFieldName(int index);
}
