/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import dev.hardwood.internal.reader.TimestampAccessorKind;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.ColumnSchema;

/// The literal types a filter predicate takes on a column, as a refusal names them: the value of
/// the column's logical accessor, where it has one, and that of its physical accessor.
///
/// The switch over [LogicalType] is exhaustive rather than a list of exceptions, so an annotation
/// added later has to state which literal it takes.
final class ColumnLiterals {

    private ColumnLiterals() {
    }

    /// The refusal of a literal the column does not take, naming the column, what it is, the
    /// literals it takes and the one given, such as `a LocalDate`.
    static IllegalArgumentException notTaken(String columnName, ColumnSchema columnSchema, String literal) {
        return new IllegalArgumentException("Column '" + columnName + "' is " + describe(columnSchema)
                + ", which takes " + taken(columnSchema) + " literals, not " + literal);
    }

    /// What the column is, as a refusal names it.
    static String describe(ColumnSchema columnSchema) {
        if (columnSchema.logicalType() != null) {
            return "annotated " + columnSchema.logicalType();
        }
        return columnSchema.type() == PhysicalType.INT96
                ? TimestampAccessorKind.describeLegacyInt96()
                : "an unannotated " + columnSchema.type();
    }

    /// The literal types the column takes, such as `LocalDate and int`.
    static String taken(ColumnSchema columnSchema) {
        String physical = switch (columnSchema.type()) {
            case BOOLEAN -> "boolean";
            case INT32 -> "int";
            case INT64 -> "long";
            case FLOAT -> "float";
            case DOUBLE -> "double";
            case INT96, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> "byte[]";
        };
        String logical = logical(columnSchema);
        return logical == null ? physical : logical + " and " + physical;
    }

    /// The value of the column's logical accessor, or `null` where only the physical accessor reads it.
    private static String logical(ColumnSchema columnSchema) {
        LogicalType logicalType = columnSchema.logicalType();
        if (logicalType == null) {
            return switch (columnSchema.type()) {
                case INT96 -> "Instant";
                case BYTE_ARRAY -> "String";
                default -> null;
            };
        }
        return switch (logicalType) {
            case LogicalType.StringType ignored -> "String";
            case LogicalType.EnumType ignored -> "String";
            case LogicalType.JsonType ignored -> "String";
            case LogicalType.DecimalType ignored -> "BigDecimal";
            case LogicalType.Float16Type ignored -> "float";
            case LogicalType.UuidType ignored -> "UUID";
            case LogicalType.IntervalType ignored -> "PqInterval";
            case LogicalType.DateType ignored -> "LocalDate";
            case LogicalType.TimeType ignored -> "LocalTime";
            case LogicalType.TimestampType timestamp -> timestamp.isAdjustedToUTC() ? "Instant" : "LocalDateTime";
            case LogicalType.IntType ignored -> null;
            case LogicalType.BsonType ignored -> null;
            case LogicalType.GeometryType ignored -> null;
            case LogicalType.GeographyType ignored -> null;
            case LogicalType.NullType ignored -> null;
            case LogicalType.VariantType ignored -> null;
            case LogicalType.ListType ignored -> null;
            case LogicalType.MapType ignored -> null;
        };
    }
}
