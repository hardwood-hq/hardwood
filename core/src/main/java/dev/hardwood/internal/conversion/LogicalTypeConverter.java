/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.conversion;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;

/**
 * Converts physical values to their logical type representations.
 */
public class LogicalTypeConverter {

    /**
     * Convert a physical value to its logical type representation.
     * Returns the original value if no conversion is needed.
     */
    public static Object convert(Object physicalValue, PhysicalType physicalType, LogicalType logicalType) {
        if (physicalValue == null || logicalType == null) {
            return physicalValue;
        }

        return switch (logicalType) {
            case LogicalType.StringType t -> convertToString(physicalValue, physicalType);
            case LogicalType.DateType t -> convertToDate(physicalValue, physicalType);
            case LogicalType.TimestampType tt -> convertToTimestamp(physicalValue, physicalType, tt);
            case LogicalType.TimeType tt -> convertToTime(physicalValue, physicalType, tt);
            case LogicalType.DecimalType dt -> convertToDecimal(physicalValue, physicalType, dt);
            case LogicalType.IntType it -> convertToInt(physicalValue, physicalType, it);
            case LogicalType.UuidType t -> convertToUuid(physicalValue, physicalType);
            case LogicalType.JsonType t -> convertToString(physicalValue, physicalType);
            case LogicalType.BsonType t -> convertToString(physicalValue, physicalType);
            // EnumType and IntervalType: pass through as-is (no conversion needed or not supported)
            // ListType and MapType are structural types handled by RecordAssembler, not primitive conversions
            default -> physicalValue;
        };
    }

    public static String convertToString(Object value, PhysicalType physicalType) {
        if (physicalType != PhysicalType.BYTE_ARRAY) {
            throw new IllegalArgumentException("STRING logical type requires BYTE_ARRAY physical type, got " + physicalType);
        }
        return new String((byte[]) value, StandardCharsets.UTF_8);
    }

    public static LocalDate convertToDate(Object value, PhysicalType physicalType) {
        if (physicalType != PhysicalType.INT32) {
            throw new IllegalArgumentException("DATE logical type requires INT32 physical type, got " + physicalType);
        }
        // DATE is days since Unix epoch
        int daysSinceEpoch = (Integer) value;
        return LocalDate.ofEpochDay(daysSinceEpoch);
    }

    public static Instant convertToTimestamp(Object value, PhysicalType physicalType,
                                             LogicalType.TimestampType timestampType) {
        if (physicalType != PhysicalType.INT64) {
            throw new IllegalArgumentException("TIMESTAMP logical type requires INT64 physical type, got " + physicalType);
        }

        long rawValue = (Long) value;
        return switch (timestampType.unit()) {
            case MILLIS -> Instant.ofEpochMilli(rawValue);
            case MICROS -> Instant.ofEpochSecond(rawValue / 1_000_000, (rawValue % 1_000_000) * 1000);
            case NANOS -> Instant.ofEpochSecond(rawValue / 1_000_000_000, rawValue % 1_000_000_000);
        };
    }

    public static LocalTime convertToTime(Object value, PhysicalType physicalType,
                                          LogicalType.TimeType timeType) {
        if (physicalType != PhysicalType.INT32 && physicalType != PhysicalType.INT64) {
            throw new IllegalArgumentException(
                    "TIME logical type requires INT32 or INT64 physical type, got " + physicalType);
        }

        long rawValue = physicalType == PhysicalType.INT32 ? (Integer) value : (Long) value;

        return switch (timeType.unit()) {
            case MILLIS -> LocalTime.ofNanoOfDay(rawValue * 1_000_000);
            case MICROS -> LocalTime.ofNanoOfDay(rawValue * 1000);
            case NANOS -> LocalTime.ofNanoOfDay(rawValue);
        };
    }

    public static BigDecimal convertToDecimal(Object value, PhysicalType physicalType,
                                              LogicalType.DecimalType decimalType) {
        BigInteger unscaled = switch (physicalType) {
            case INT32 -> BigInteger.valueOf((Integer) value);
            case INT64 -> BigInteger.valueOf((Long) value);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> {
                byte[] bytes = (byte[]) value;
                // Parquet uses big-endian two's complement for decimal
                yield new BigInteger(bytes);
            }
            default -> throw new IllegalArgumentException(
                    "DECIMAL requires INT32, INT64, BYTE_ARRAY, or FIXED_LEN_BYTE_ARRAY, got " + physicalType);
        };

        return new BigDecimal(unscaled, decimalType.scale());
    }

    public static Object convertToInt(Object value, PhysicalType physicalType,
                                      LogicalType.IntType intType) {
        if (physicalType != PhysicalType.INT32 && physicalType != PhysicalType.INT64) {
            throw new IllegalArgumentException("INT logical type requires INT32 or INT64 physical type, got " + physicalType);
        }

        // For signed integers with narrowing
        if (intType.isSigned()) {
            if (intType.bitWidth() == 8 && physicalType == PhysicalType.INT32) {
                return ((Integer) value).byteValue();
            }
            else if (intType.bitWidth() == 16 && physicalType == PhysicalType.INT32) {
                return ((Integer) value).shortValue();
            }
        }

        // For 32 or 64 bit, or unsigned types, pass through
        // Note: Java doesn't have native unsigned types, so we return the same value
        return value;
    }

    public static UUID convertToUuid(Object value, PhysicalType physicalType) {
        if (physicalType != PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            throw new IllegalArgumentException("UUID logical type requires FIXED_LEN_BYTE_ARRAY physical type, got " + physicalType);
        }

        byte[] bytes = (byte[]) value;
        if (bytes.length != 16) {
            throw new IllegalArgumentException("UUID requires exactly 16 bytes, got " + bytes.length);
        }

        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long mostSigBits = bb.getLong();
        long leastSigBits = bb.getLong();
        return new UUID(mostSigBits, leastSigBits);
    }
}
