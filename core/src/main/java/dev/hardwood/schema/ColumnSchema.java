/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.schema;

import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;

/// Represents a primitive column in a Parquet schema. Stores computed definition
/// and repetition levels based on schema hierarchy.
///
/// @param fieldPath path from the schema root to this leaf column (unambiguous across schemas with duplicate leaf names)
/// @param type physical (storage) type of the column
/// @param repetitionType whether the column is required, optional, or repeated
/// @param typeLength fixed byte length for [PhysicalType#FIXED_LEN_BYTE_ARRAY] columns, or `null` otherwise
/// @param columnIndex zero-based index of this column among all leaf columns in the schema
/// @param maxDefinitionLevel maximum definition level, computed from the schema hierarchy
/// @param maxRepetitionLevel maximum repetition level, computed from the schema hierarchy
/// @param logicalType logical type annotation, or `null` if absent
/// @see <a href="https://parquet.apache.org/docs/file-format/nestedencoding/">File Format – Nested Encoding</a>
/// @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift</a>
public record ColumnSchema(
        FieldPath fieldPath,
        PhysicalType type,
        RepetitionType repetitionType,
        Integer typeLength,
        int columnIndex,
        int maxDefinitionLevel,
        int maxRepetitionLevel,
        LogicalType logicalType) {

    /// Returns the leaf (last) element of the field path, i.e. the column name.
    public String name() {
        return fieldPath.leafName();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(repetitionType.name().toLowerCase());
        sb.append(" ");
        sb.append(type.name().toLowerCase());
        if (typeLength != null) {
            sb.append("(").append(typeLength).append(")");
        }
        sb.append(" ");
        sb.append(name());
        if (logicalType != null) {
            sb.append(" (").append(logicalType).append(")");
        }
        sb.append(";");
        return sb.toString();
    }
}
