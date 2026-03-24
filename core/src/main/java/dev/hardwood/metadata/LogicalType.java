/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

/// Logical types that provide semantic meaning to physical types.
/// Sealed interface allows for parameterized types (e.g., DECIMAL with scale/precision).
///
/// @see <a href="https://parquet.apache.org/docs/file-format/types/logicaltypes/">File Format – Logical Types</a>
/// @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift</a>
public sealed
interface LogicalType
permits LogicalType.StringType,LogicalType.EnumType,LogicalType.UuidType,LogicalType.IntType,LogicalType.DecimalType,LogicalType.DateType,LogicalType.TimeType,LogicalType.TimestampType,LogicalType.IntervalType,LogicalType.JsonType,LogicalType.BsonType,LogicalType.ListType,LogicalType.MapType
{

    /// UTF-8 encoded string.
    record StringType() implements LogicalType {}

    /// Enum stored as a UTF-8 string.
    record EnumType() implements LogicalType {}

    /// UUID stored as a 16-byte fixed-length byte array.
    record UuidType() implements LogicalType {}

    /// Calendar date (days since Unix epoch).
    record DateType() implements LogicalType {}

    /// JSON document stored as a UTF-8 string.
    record JsonType() implements LogicalType {}

    /// BSON document stored as a byte array.
    record BsonType() implements LogicalType {}

    /// Interval stored as a 12-byte fixed-length byte array (months, days, millis).
    record IntervalType() implements LogicalType {}

    /// Integer type with a specific bit width and signedness.
    ///
    /// @param bitWidth number of bits (8, 16, 32, or 64)
    /// @param isSigned `true` for signed integers, `false` for unsigned
    record IntType(int bitWidth, boolean isSigned) implements LogicalType {
        public IntType {
            if (bitWidth != 8 && bitWidth != 16 && bitWidth != 32 && bitWidth != 64) {
                throw new IllegalArgumentException("Invalid bit width: " + bitWidth);
            }
        }
    }

    /// Decimal with fixed scale and precision.
    ///
    /// @param scale number of digits after the decimal point
    /// @param precision total number of digits (must be positive)
    record DecimalType(int scale, int precision) implements LogicalType {
        public DecimalType {
            if (precision <= 0) {
                throw new IllegalArgumentException("Precision must be positive: " + precision);
            }
            if (scale < 0) {
                throw new IllegalArgumentException("Scale cannot be negative: " + scale);
            }
        }
    }

    /// Time of day with configurable precision and UTC adjustment.
    ///
    /// @param isAdjustedToUTC `true` if the value is normalized to UTC
    /// @param unit time resolution (millis, micros, or nanos)
    record TimeType(boolean isAdjustedToUTC, TimeUnit unit) implements LogicalType {}

    /// Timestamp with configurable precision and UTC adjustment.
    ///
    /// @param isAdjustedToUTC `true` if the value is normalized to UTC
    /// @param unit time resolution (millis, micros, or nanos)
    record TimestampType(boolean isAdjustedToUTC, TimeUnit unit) implements LogicalType {}

    /// List (repeated element) logical type.
    record ListType() implements LogicalType {}

    /// Map (key-value pairs) logical type.
    record MapType() implements LogicalType {}

    /// Resolution of time and timestamp logical types.
    enum TimeUnit {
        /// Millisecond resolution.
        MILLIS,
        /// Microsecond resolution.
        MICROS,
        /// Nanosecond resolution.
        NANOS
    }
}
