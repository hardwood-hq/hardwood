/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;

/// Which columns hold text: a `BYTE_ARRAY` annotated `STRING`, `ENUM` or `JSON`, and one
/// carrying no annotation at all, written before the annotation existed.
///
/// Two contracts rest on this one question and have to give the same answer. `getString`
/// reads exactly these columns, and a `String` is the filter literal for exactly these
/// columns: a literal is a value the column's accessors return, so a column a `String`
/// cannot be read out of is a column a `String` cannot filter either.
///
/// The switch in [#nonTextLiterals] is exhaustive rather than a list of exceptions, so an
/// annotation added later has to state whether a `String` reads it.
public final class TextColumns {

    private TextColumns() {
    }

    /// Whether `getString` reads this column, and a `String` is its filter literal.
    ///
    /// An annotation only a wider physical type can carry never reaches here: `FileSchema`
    /// drops `STRING`, `ENUM` and `JSON` off anything but a `BYTE_ARRAY` when the footer is
    /// read, leaving the column unannotated.
    public static boolean holdsText(PhysicalType type, LogicalType logicalType) {
        return type == PhysicalType.BYTE_ARRAY
                && (logicalType == null || nonTextLiterals(logicalType) == null);
    }

    /// The literals a binary column takes in place of a `String`, or `null` where a `String`
    /// reads the column.
    ///
    /// The annotations that reach an arm returning `byte[]` alone read the stored bytes as an
    /// opaque payload, or annotate a physical type no binary column has —
    /// [dev.hardwood.schema.FileSchema] drops the latter when the footer is read.
    public static String nonTextLiterals(LogicalType logicalType) {
        return switch (logicalType) {
            case LogicalType.StringType ignored -> null;
            case LogicalType.EnumType ignored -> null;
            case LogicalType.JsonType ignored -> null;
            case LogicalType.DecimalType ignored -> "BigDecimal and byte[]";
            case LogicalType.Float16Type ignored -> "float and byte[]";
            case LogicalType.UuidType ignored -> "UUID and byte[]";
            case LogicalType.BsonType ignored -> "byte[]";
            case LogicalType.IntervalType ignored -> "byte[]";
            case LogicalType.GeometryType ignored -> "byte[]";
            case LogicalType.GeographyType ignored -> "byte[]";
            case LogicalType.NullType ignored -> "byte[]";
            case LogicalType.VariantType ignored -> "byte[]";
            case LogicalType.ListType ignored -> "byte[]";
            case LogicalType.MapType ignored -> "byte[]";
            case LogicalType.IntType ignored -> "byte[]";
            case LogicalType.DateType ignored -> "byte[]";
            case LogicalType.TimeType ignored -> "byte[]";
            case LogicalType.TimestampType ignored -> "byte[]";
        };
    }
}
