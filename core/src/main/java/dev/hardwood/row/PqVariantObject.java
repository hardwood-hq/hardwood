/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.row;

/// Name-based view of a Variant `OBJECT` value.
///
/// Inherits the primitive getters (`getInt`, `getString`, `getTimestamp`, …) and
/// the `getVariant` / `isNull` / `getFieldCount` / `getFieldName` metadata
/// accessors from [FieldAccessor]. The typed getters narrow through the
/// Variant type tag of each field — e.g. `getInt("age")` accepts a field whose
/// Variant type is INT8/INT16/INT32, and throws [VariantTypeException]
/// otherwise.
///
/// Variant-specific complex navigation uses [#getObject(String)] and
/// [#getArray(String)], not the Parquet-schema-typed `getStruct` / `getList` /
/// `getMap` (which are not exposed here).
public interface PqVariantObject extends FieldAccessor {

    /// Get a field whose Variant type is OBJECT and unwrap it as a nested
    /// [PqVariantObject] view.
    ///
    /// @param name the field name
    /// @return the nested object view, or null if the field is null
    /// @throws IllegalArgumentException if the field is absent
    /// @throws VariantTypeException if the field's Variant type is not OBJECT
    PqVariantObject getObject(String name);

    /// Get a field whose Variant type is ARRAY and unwrap it as a nested
    /// [PqVariantArray] view.
    ///
    /// @param name the field name
    /// @return the nested array view, or null if the field is null
    /// @throws IllegalArgumentException if the field is absent
    /// @throws VariantTypeException if the field's Variant type is not ARRAY
    PqVariantArray getArray(String name);
}
