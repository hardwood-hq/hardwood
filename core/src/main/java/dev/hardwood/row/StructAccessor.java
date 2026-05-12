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

/// Parquet-struct access interface. Extends [FieldAccessor] with complex getters
/// specific to the Parquet schema-typed world (nested structs, lists, maps) and
/// with index-based mirrors of the [FieldAccessor] by-name accessors.
///
/// Variant objects do not expose these methods — use [PqVariantObject] for
/// Variant navigation. By-index access lives here rather than on [FieldAccessor]
/// because the "field index" is meaningful for struct-shaped accessors (the
/// position in projected schema order) but not for Variant objects (whose field
/// order is lexicographic on the metadata key dictionary and rarely matches what
/// a caller would consider "first" / "second").
///
/// This interface makes no assumptions about mutability or lifecycle, allowing
/// code to work polymorphically with both [dev.hardwood.reader.RowReader] (a
/// stateful, mutable view over the current row) and [PqStruct] (a flyweight
/// over a nested struct position).
///
/// @see PqStruct
/// @see dev.hardwood.reader.RowReader
public interface StructAccessor extends FieldAccessor {

    // ==================== Nested Types ====================

    /// Get a nested struct field value by name.
    ///
    /// @param name the field name
    /// @return the nested struct, or null if the field is null
    /// @throws IllegalArgumentException if the field type is not a struct
    PqStruct getStruct(String name);

    // ==================== Generic List ====================

    /// Get a LIST field value by name.
    ///
    /// @param name the field name
    /// @return the list, or null if the field is null
    /// @throws IllegalArgumentException if the field type is not a list
    PqList getList(String name);

    /// Get a MAP field value by name.
    ///
    /// @param name the field name
    /// @return the map, or null if the field is null
    /// @throws IllegalArgumentException if the field type is not a map
    PqMap getMap(String name);

    // ==================== Accessors by Index ====================
    //
    // Index-based mirrors of the by-name accessors on [FieldAccessor] and the
    // nested-type accessors above. Faster than name-based access in hot loops
    // over a fixed schema since they skip the per-call name lookup. The index
    // is the field's position within the accessor's projected children, in
    // projected schema order — the same position [#getFieldName] reports.

    /// Get an INT32 field value by field index. See [#getInt(String)].
    int getInt(int fieldIndex);

    /// Get an INT64 field value by field index. See [#getLong(String)].
    long getLong(int fieldIndex);

    /// Get a FLOAT field value by field index. See [#getFloat(String)].
    float getFloat(int fieldIndex);

    /// Get a DOUBLE field value by field index. See [#getDouble(String)].
    double getDouble(int fieldIndex);

    /// Get a BOOLEAN field value by field index. See [#getBoolean(String)].
    boolean getBoolean(int fieldIndex);

    /// Get a STRING field value by field index. See [#getString(String)].
    String getString(int fieldIndex);

    /// Get a BINARY field value by field index. See [#getBinary(String)].
    byte[] getBinary(int fieldIndex);

    /// Get a DATE field value by field index. See [#getDate(String)].
    LocalDate getDate(int fieldIndex);

    /// Get a TIME field value by field index. See [#getTime(String)].
    LocalTime getTime(int fieldIndex);

    /// Get a TIMESTAMP field value by field index. See [#getTimestamp(String)].
    Instant getTimestamp(int fieldIndex);

    /// Get a DECIMAL field value by field index. See [#getDecimal(String)].
    BigDecimal getDecimal(int fieldIndex);

    /// Get a UUID field value by field index. See [#getUuid(String)].
    UUID getUuid(int fieldIndex);

    /// Get an INTERVAL field value by field index. See [#getInterval(String)].
    PqInterval getInterval(int fieldIndex);

    /// Get a VARIANT field value by field index. See [#getVariant(String)].
    PqVariant getVariant(int fieldIndex);

    /// Get a nested struct field value by field index. See [#getStruct(String)].
    PqStruct getStruct(int fieldIndex);

    /// Get a LIST field value by field index. See [#getList(String)].
    PqList getList(int fieldIndex);

    /// Get a MAP field value by field index. See [#getMap(String)].
    PqMap getMap(int fieldIndex);

    /// Get a field value by field index, decoded to its logical-type representation.
    /// See [#getValue(String)].
    Object getValue(int fieldIndex);

    /// Get a field value by field index as its raw physical representation.
    /// See [#getRawValue(String)].
    Object getRawValue(int fieldIndex);

    /// Check if a field is null by field index. See [#isNull(String)].
    boolean isNull(int fieldIndex);
}
