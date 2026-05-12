/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.row;

/// Parquet-struct access interface. Extends [FieldAccessor] with complex getters
/// specific to the Parquet schema-typed world: nested structs, lists (including
/// primitive-specialized ones), and maps.
///
/// Variant objects do not expose these methods — use [PqVariantObject] for
/// Variant navigation. See [FieldAccessor] for the common primitive and Variant
/// accessor surface shared by both.
///
/// This interface makes no assumptions about mutability or lifecycle, allowing
/// code to work polymorphically with both [dev.hardwood.reader.RowReader] (a
/// stateful, mutable view over the current row) and [PqStruct] (an immutable,
/// self-contained nested struct value).
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
}
