/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

/// Schema element in Parquet file metadata.
///
/// @param name column or group name
/// @param type physical type of this element, or `null` for group nodes
/// @param typeLength fixed byte length for [PhysicalType#FIXED_LEN_BYTE_ARRAY] columns, or `null` otherwise
/// @param repetitionType repetition level (required, optional, or repeated)
/// @param numChildren number of child elements for group nodes, or `null` for primitive nodes
/// @param convertedType legacy converted type annotation, or `null` if absent
/// @param scale decimal scale (number of digits after the decimal point), or `null` if not a decimal
/// @param precision decimal precision (total number of digits), or `null` if not a decimal
/// @param fieldId Thrift field id from the schema, or `null` if absent
/// @param logicalType logical type annotation, or `null` if absent
/// @see <a href="https://parquet.apache.org/docs/file-format/metadata/#file-metadata">File Format – File Metadata</a>
/// @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift</a>
public record SchemaElement(
        String name,
        PhysicalType type,
        Integer typeLength,
        RepetitionType repetitionType,
        Integer numChildren,
        ConvertedType convertedType,
        Integer scale,
        Integer precision,
        Integer fieldId,
        LogicalType logicalType) {

    /// Returns `true` if this element is a group node (has no physical type).
    public boolean isGroup() {
        return type == null;
    }

    /// Returns `true` if this element is a primitive node (has a physical type).
    public boolean isPrimitive() {
        return type != null;
    }
}
