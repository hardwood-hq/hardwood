/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

import dev.hardwood.metadata.ConvertedType;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.SchemaElement;

/// The annotation fields a schema node writes to its [SchemaElement]: the modern `LogicalType`
/// union and, where one exists, the legacy `converted_type` with the sibling `scale` and
/// `precision` a `DECIMAL` needs.
///
/// parquet-format requires both: a writer must always write the union where applicable, and
/// must also write the corresponding `converted_type` so pre-union readers still see the
/// annotation. Deriving one from the other here is what makes that a single declaration for the
/// caller. This is the inverse of `FileSchema.effectiveLogicalType`, which collapses a legacy
/// annotation into a `LogicalType` when a file is read.
///
/// @param union the `LogicalType` union member to write, or `null` if the annotation has none
/// @param convertedType the legacy annotation to write, or `null` if the annotation has none
/// @param scale a `DECIMAL`'s scale, `null` otherwise
/// @param precision a `DECIMAL`'s precision, `null` otherwise
public record LogicalTypeAnnotations(LogicalType union, ConvertedType convertedType, Integer scale,
                                     Integer precision) {

    /// An unannotated node.
    public static final LogicalTypeAnnotations NONE = new LogicalTypeAnnotations(null, null, null, null);

    /// The annotations written for a primitive column's logical type.
    ///
    /// Two members are asymmetric. `INTERVAL` writes only the legacy annotation, because
    /// parquet.thrift reserves union field 9 for it without ever defining the member struct.
    /// `TIME` and `TIMESTAMP` derive their legacy annotation from the unit alone, ignoring
    /// `isAdjustedToUTC`: the legacy annotations denoted UTC-normalized values, but
    /// parquet-format requires writers to annotate local times with them too, for forward
    /// compatibility with the libraries that did so before the union existed. Nanosecond units
    /// have no legacy counterpart and are union-only.
    public static LogicalTypeAnnotations of(LogicalType logicalType) {
        if (logicalType == null) {
            return NONE;
        }
        return switch (logicalType) {
            case LogicalType.StringType ignored -> both(logicalType, ConvertedType.UTF8);
            case LogicalType.MapType ignored -> both(logicalType, ConvertedType.MAP);
            case LogicalType.ListType ignored -> both(logicalType, ConvertedType.LIST);
            case LogicalType.EnumType ignored -> both(logicalType, ConvertedType.ENUM);
            case LogicalType.DateType ignored -> both(logicalType, ConvertedType.DATE);
            case LogicalType.JsonType ignored -> both(logicalType, ConvertedType.JSON);
            case LogicalType.BsonType ignored -> both(logicalType, ConvertedType.BSON);
            case LogicalType.DecimalType decimal -> new LogicalTypeAnnotations(
                    decimal, ConvertedType.DECIMAL, decimal.scale(), decimal.precision());
            case LogicalType.IntType integer -> both(logicalType, intConvertedType(integer));
            case LogicalType.TimeType time -> both(logicalType, switch (time.unit()) {
                case MILLIS -> ConvertedType.TIME_MILLIS;
                case MICROS -> ConvertedType.TIME_MICROS;
                case NANOS -> null;
            });
            case LogicalType.TimestampType timestamp -> both(logicalType, switch (timestamp.unit()) {
                case MILLIS -> ConvertedType.TIMESTAMP_MILLIS;
                case MICROS -> ConvertedType.TIMESTAMP_MICROS;
                case NANOS -> null;
            });
            case LogicalType.IntervalType ignored -> new LogicalTypeAnnotations(
                    null, ConvertedType.INTERVAL, null, null);
            case LogicalType.NullType ignored -> unionOnly(logicalType);
            case LogicalType.UuidType ignored -> unionOnly(logicalType);
            case LogicalType.Float16Type ignored -> unionOnly(logicalType);
            case LogicalType.VariantType ignored -> unionOnly(logicalType);
            case LogicalType.GeometryType ignored -> unionOnly(logicalType);
            case LogicalType.GeographyType ignored -> unionOnly(logicalType);
        };
    }

    /// The annotations written for a group, which the schema model may hold in either
    /// representation: a group built by the writer carries a `LogicalType`, while one read from
    /// a file may carry only the legacy `LIST` / `MAP`. Either way both are written back out.
    ///
    /// The deprecated `MAP_KEY_VALUE` has no union member and is passed through unchanged.
    public static LogicalTypeAnnotations ofGroup(ConvertedType convertedType, LogicalType logicalType) {
        if (logicalType != null) {
            return of(logicalType);
        }
        if (convertedType == null) {
            return NONE;
        }
        return switch (convertedType) {
            case LIST -> both(LogicalType.list(), ConvertedType.LIST);
            case MAP -> both(LogicalType.map(), ConvertedType.MAP);
            default -> new LogicalTypeAnnotations(null, convertedType, null, null);
        };
    }

    private static ConvertedType intConvertedType(LogicalType.IntType integer) {
        boolean signed = integer.isSigned();
        return switch (integer.bitWidth()) {
            case 8 -> signed ? ConvertedType.INT_8 : ConvertedType.UINT_8;
            case 16 -> signed ? ConvertedType.INT_16 : ConvertedType.UINT_16;
            case 32 -> signed ? ConvertedType.INT_32 : ConvertedType.UINT_32;
            case 64 -> signed ? ConvertedType.INT_64 : ConvertedType.UINT_64;
            default -> throw new IllegalArgumentException("Invalid integer bit width: " + integer.bitWidth());
        };
    }

    private static LogicalTypeAnnotations both(LogicalType union, ConvertedType convertedType) {
        return new LogicalTypeAnnotations(union, convertedType, null, null);
    }

    private static LogicalTypeAnnotations unionOnly(LogicalType union) {
        return new LogicalTypeAnnotations(union, null, null, null);
    }
}
