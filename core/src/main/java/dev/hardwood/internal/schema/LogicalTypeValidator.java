/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

import java.math.BigInteger;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;

/// Checks that a logical type annotation is legal for the physical type it annotates.
///
/// A logical type is only meaningful over the physical representations parquet-format defines
/// for it — a `DATE` is days in an `INT32` and nothing else, a `UUID` is exactly 16 fixed bytes.
/// An illegal pairing produces a file whose annotation no reader can honour, so the writer
/// rejects it where the schema is declared rather than emitting it.
///
/// Only the writer validates. The reader stays lenient, because a file that already exists on
/// disk has to be readable whatever a foreign writer put in its footer.
public class LogicalTypeValidator {

    /// Validates a primitive column's annotation.
    ///
    /// @param columnName the column name, for the failure message
    /// @param type the column's physical type
    /// @param repetition the column's repetition
    /// @param typeLength the `FIXED_LEN_BYTE_ARRAY` byte length, `null` for any other type
    /// @param logicalType the annotation to validate, `null` for an unannotated column
    /// @throws IllegalArgumentException if the annotation is not legal for the physical type, its
    ///         type length, or — for `UNKNOWN` — the column's repetition
    public static void validate(String columnName, PhysicalType type, RepetitionType repetition,
                                Integer typeLength, LogicalType logicalType) {
        if (logicalType == null) {
            return;
        }
        switch (logicalType) {
            case LogicalType.StringType ignored -> require(columnName, logicalType, type, PhysicalType.BYTE_ARRAY);
            case LogicalType.EnumType ignored -> require(columnName, logicalType, type, PhysicalType.BYTE_ARRAY);
            case LogicalType.JsonType ignored -> require(columnName, logicalType, type, PhysicalType.BYTE_ARRAY);
            case LogicalType.BsonType ignored -> require(columnName, logicalType, type, PhysicalType.BYTE_ARRAY);
            case LogicalType.GeometryType ignored -> require(columnName, logicalType, type, PhysicalType.BYTE_ARRAY);
            case LogicalType.GeographyType ignored -> require(columnName, logicalType, type, PhysicalType.BYTE_ARRAY);
            case LogicalType.DateType ignored -> require(columnName, logicalType, type, PhysicalType.INT32);
            case LogicalType.UuidType ignored -> requireFixed(columnName, logicalType, type, typeLength, 16);
            case LogicalType.Float16Type ignored -> requireFixed(columnName, logicalType, type, typeLength, 2);
            case LogicalType.IntervalType ignored -> requireFixed(columnName, logicalType, type, typeLength, 12);
            case LogicalType.IntType integer -> require(columnName, logicalType, type,
                    integer.bitWidth() == 64 ? PhysicalType.INT64 : PhysicalType.INT32);
            case LogicalType.TimeType time -> require(columnName, logicalType, type,
                    time.unit() == LogicalType.TimeUnit.MILLIS ? PhysicalType.INT32 : PhysicalType.INT64);
            case LogicalType.TimestampType ignored -> require(columnName, logicalType, type, PhysicalType.INT64);
            case LogicalType.DecimalType decimal -> validateDecimal(columnName, type, typeLength, decimal);
            case LogicalType.NullType ignored -> requireNullable(columnName, repetition);
            case LogicalType.ListType ignored -> throw groupAnnotation(columnName, logicalType);
            case LogicalType.MapType ignored -> throw groupAnnotation(columnName, logicalType);
            case LogicalType.VariantType ignored -> throw new IllegalArgumentException(
                    "VARIANT annotates a group of metadata and value children, which the writer "
                            + "does not yet build: " + columnName);
        }
    }

    /// `UNKNOWN` annotates a column of any physical type whose every value is null, which a
    /// `REQUIRED` column can never be: no value it could legally hold matches the annotation,
    /// and reading one back fails on the null the annotation promises.
    private static void requireNullable(String columnName, RepetitionType repetition) {
        if (repetition == RepetitionType.REQUIRED) {
            throw new IllegalArgumentException("UNKNOWN annotates a column holding only nulls, so it cannot be "
                    + "REQUIRED (column " + columnName + ")");
        }
    }

    /// A `DECIMAL`'s precision must fit the digits its physical representation can hold: 9 for
    /// an `INT32`, 18 for an `INT64`, and for a `FIXED_LEN_BYTE_ARRAY` whatever the two's
    /// complement of that byte length spans. A `BYTE_ARRAY` is unbounded.
    private static void validateDecimal(String columnName, PhysicalType type, Integer typeLength,
                                        LogicalType.DecimalType decimal) {
        int maxPrecision = switch (type) {
            case INT32 -> 9;
            case INT64 -> 18;
            case BYTE_ARRAY -> Integer.MAX_VALUE;
            case FIXED_LEN_BYTE_ARRAY -> maxFixedPrecision(typeLength);
            default -> throw new IllegalArgumentException("DECIMAL is not valid on physical type " + type
                    + " (column " + columnName + "); use INT32, INT64, BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY");
        };
        if (decimal.precision() > maxPrecision) {
            throw new IllegalArgumentException("DECIMAL precision " + decimal.precision()
                    + " exceeds the maximum " + maxPrecision + " a " + type
                    + " can represent on column " + columnName);
        }
    }

    /// The largest precision a two's-complement value of `length` bytes represents:
    /// `floor(log10(2^(8 * length - 1) - 1))`, computed exactly rather than through a
    /// floating-point logarithm.
    private static int maxFixedPrecision(int length) {
        return BigInteger.ONE.shiftLeft(8 * length - 1).subtract(BigInteger.ONE).toString().length() - 1;
    }

    private static void require(String columnName, LogicalType logicalType, PhysicalType actual,
                                PhysicalType expected) {
        if (actual != expected) {
            throw new IllegalArgumentException(logicalType + " annotates a " + expected + " column, not "
                    + actual + " (column " + columnName + ")");
        }
    }

    private static void requireFixed(String columnName, LogicalType logicalType, PhysicalType actual,
                                     Integer typeLength, int expectedLength) {
        require(columnName, logicalType, actual, PhysicalType.FIXED_LEN_BYTE_ARRAY);
        if (typeLength == null || typeLength != expectedLength) {
            throw new IllegalArgumentException(logicalType + " annotates a FIXED_LEN_BYTE_ARRAY of length "
                    + expectedLength + ", not " + typeLength + " (column " + columnName + ")");
        }
    }

    private static IllegalArgumentException groupAnnotation(String columnName, LogicalType logicalType) {
        return new IllegalArgumentException(logicalType + " annotates a group, not a primitive column: "
                + columnName + "; declare it with the list or map builder verb instead");
    }

    private LogicalTypeValidator() {
    }
}
