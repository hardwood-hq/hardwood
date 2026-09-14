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
/// The switch in [#isText] is exhaustive rather than a list of exceptions, so an
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
        return type == PhysicalType.BYTE_ARRAY && (logicalType == null || isText(logicalType));
    }

    /// Whether the annotation says the stored bytes are the UTF-8 encoding of a string.
    private static boolean isText(LogicalType logicalType) {
        return switch (logicalType) {
            case LogicalType.StringType ignored -> true;
            case LogicalType.EnumType ignored -> true;
            case LogicalType.JsonType ignored -> true;
            case LogicalType.DecimalType ignored -> false;
            case LogicalType.Float16Type ignored -> false;
            case LogicalType.UuidType ignored -> false;
            case LogicalType.BsonType ignored -> false;
            case LogicalType.IntervalType ignored -> false;
            case LogicalType.GeometryType ignored -> false;
            case LogicalType.GeographyType ignored -> false;
            case LogicalType.NullType ignored -> false;
            case LogicalType.VariantType ignored -> false;
            case LogicalType.ListType ignored -> false;
            case LogicalType.MapType ignored -> false;
            case LogicalType.IntType ignored -> false;
            case LogicalType.DateType ignored -> false;
            case LogicalType.TimeType ignored -> false;
            case LogicalType.TimestampType ignored -> false;
        };
    }
}
