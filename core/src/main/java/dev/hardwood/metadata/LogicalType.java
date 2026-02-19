/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

/**
 * Logical types that provide semantic meaning to physical types.
 * Sealed interface allows for parameterized types (e.g., DECIMAL with scale/precision).
 */
public sealed
interface LogicalType
permits LogicalType.StringType,LogicalType.EnumType,LogicalType.UuidType,LogicalType.IntType,LogicalType.DecimalType,LogicalType.DateType,LogicalType.TimeType,LogicalType.TimestampType,LogicalType.IntervalType,LogicalType.JsonType,LogicalType.BsonType,LogicalType.ListType,LogicalType.MapType
{

    // Simple types (no parameters)
    record StringType() implements LogicalType {}

    record EnumType() implements LogicalType {}

    record UuidType() implements LogicalType {}

    record DateType() implements LogicalType {}

    record JsonType() implements LogicalType {}

    record BsonType() implements LogicalType {}

    record IntervalType() implements LogicalType {}

    // Parameterized: Integer types with bitWidth and sign
    record IntType(int bitWidth, boolean isSigned) implements LogicalType {
        public IntType {
            if (bitWidth != 8 && bitWidth != 16 && bitWidth != 32 && bitWidth != 64) {
                throw new IllegalArgumentException("Invalid bit width: " + bitWidth);
            }
        }
    }

    // Parameterized: Decimal with scale and precision
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

    // Parameterized: Time with unit and UTC adjustment
    record TimeType(boolean isAdjustedToUTC, TimeUnit unit) implements LogicalType {}

    // Parameterized: Timestamp with unit and UTC adjustment
    record TimestampType(boolean isAdjustedToUTC, TimeUnit unit) implements LogicalType {}

    // Complex types (not fully supported in Milestone 1)
    record ListType() implements LogicalType {}

    record MapType() implements LogicalType {}

    enum TimeUnit { MILLIS, MICROS, NANOS }
}
