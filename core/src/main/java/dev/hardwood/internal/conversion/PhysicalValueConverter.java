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
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.UUID;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.LogicalType.TimeUnit;
import dev.hardwood.row.PqInterval;
import dev.hardwood.writer.PrecisionLossPolicy;

/// Converts a logical-type value to the physical representation Parquet stores it as — the
/// write-side inverse of [LogicalTypeConverter].
///
/// A value the column cannot represent at all — a date beyond the `INT32` day range, an
/// unscaled decimal wider than the declared precision — always throws
/// [IllegalArgumentException]; there is no narrowing that would preserve its magnitude.
///
/// A value the column can hold only approximately — an [Instant] with sub-millisecond
/// precision into a `TIMESTAMP(MILLIS)` column, a [BigDecimal] whose rescale to the declared
/// scale would drop digits — is governed by the caller's [PrecisionLossPolicy]: rejected
/// under [PrecisionLossPolicy#REJECT], narrowed under [PrecisionLossPolicy#TRUNCATE]. A value
/// that happens to be exact at the column's unit or scale is written under either.
///
/// Each method takes the field's name so the message names the field the caller set.
public final class PhysicalValueConverter {

    /// Bytes of a `FIXED_LEN_BYTE_ARRAY` holding a `UUID`.
    private static final int UUID_BYTES = 16;

    /// Bytes of a `FIXED_LEN_BYTE_ARRAY` holding an `INTERVAL`: three little-endian unsigned
    /// 4-byte fields (months, days, millis).
    private static final int INTERVAL_BYTES = 12;

    private PhysicalValueConverter() {
    }

    /// The UTF-8 bytes of a `STRING`, `ENUM` or `JSON` value.
    public static byte[] stringToBytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /// Days since the Unix epoch, as a `DATE` column's `INT32` stores them.
    public static int dateToInt(String field, LocalDate value) {
        long epochDay = value.toEpochDay();
        if (epochDay < Integer.MIN_VALUE || epochDay > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Field " + field + ": " + value
                    + " is out of range for a DATE column");
        }
        return (int) epochDay;
    }

    /// Time of day in the column's unit. The caller narrows to `int` for an `INT32` `TIME`
    /// column, whose only valid unit is `MILLIS`.
    public static long timeToLong(String field, LocalTime value, TimeUnit unit, PrecisionLossPolicy policy) {
        return narrow(field, value.toNanoOfDay(), nanosPerUnit(unit), "TIME", unit, policy);
    }

    /// Offset from the Unix epoch in the column's unit, for a UTC-adjusted `TIMESTAMP`.
    public static long timestampToLong(String field, Instant value, TimeUnit unit, PrecisionLossPolicy policy) {
        return epochOffset(field, value, value.getEpochSecond(), value.getNano(), unit, policy);
    }

    /// Offset from the epoch of the wall clock itself, for a local `TIMESTAMP`. The stored
    /// bits are the same epoch arithmetic a UTC-adjusted value uses; only the type label
    /// differs, which is why the reader decodes both at [ZoneOffset#UTC].
    public static long localTimestampToLong(String field, LocalDateTime value, TimeUnit unit,
                                            PrecisionLossPolicy policy) {
        return epochOffset(field, value, value.toEpochSecond(ZoneOffset.UTC), value.getNano(), unit, policy);
    }

    /// Combines a whole-second count and a nanosecond-of-second into the column's unit. The
    /// nanosecond part is never negative, so dropping its sub-unit digits floors the value —
    /// the same result [Instant#toEpochMilli()] produces, and what every other Parquet writer
    /// does with a value it narrows.
    ///
    /// The magnitude is checked before the precision: a `TIMESTAMP(NANOS)` column spans only
    /// about 1677 to 2262, and a value outside its unit's range is unrepresentable rather than
    /// merely too precise, so it is reported the same way under either policy.
    private static long epochOffset(String field, Object value, long epochSecond, int nano, TimeUnit unit,
                                    PrecisionLossPolicy policy) {
        long nanosPerUnit = nanosPerUnit(unit);
        long units = nano / nanosPerUnit;
        long offset;
        try {
            offset = Math.addExact(Math.multiplyExact(epochSecond, 1_000_000_000L / nanosPerUnit), units);
        }
        catch (ArithmeticException e) {
            throw new IllegalArgumentException("Field " + field + ": " + value
                    + " is outside the range a TIMESTAMP(" + unit + ") column can represent", e);
        }
        if (nano % nanosPerUnit != 0 && policy == PrecisionLossPolicy.REJECT) {
            throw finerThanUnit(field, "TIMESTAMP", unit);
        }
        return offset;
    }

    /// `nanos / divisor`, with a non-zero remainder either rejected or dropped as the policy
    /// says. `nanos` is a non-negative sub-unit count, so the division floors. A time of day
    /// cannot overflow its unit — the largest is under 24 hours — so only precision is at
    /// stake here.
    private static long narrow(String field, long nanos, long divisor, String annotation, TimeUnit unit,
                               PrecisionLossPolicy policy) {
        if (nanos % divisor != 0 && policy == PrecisionLossPolicy.REJECT) {
            throw finerThanUnit(field, annotation, unit);
        }
        return nanos / divisor;
    }

    /// The rejection for a value the column could hold only by dropping digits, naming every
    /// way the caller can say what they meant.
    private static IllegalArgumentException finerThanUnit(String field, String annotation, TimeUnit unit) {
        return new IllegalArgumentException("Field " + field + ": value has finer precision than the column's "
                + annotation + "(" + unit + ") unit. Truncate it at the call site (for example "
                + "Instant.truncatedTo(ChronoUnit.MILLIS)), declare the column at a finer unit, or "
                + "configure WriterConfig.precisionLossPolicy(TRUNCATE)");
    }

    private static long nanosPerUnit(TimeUnit unit) {
        return switch (unit) {
            case MILLIS -> 1_000_000L;
            case MICROS -> 1_000L;
            case NANOS -> 1L;
        };
    }

    /// The unscaled value of a `DECIMAL` stored in an `INT32`.
    public static int decimalToInt(String field, BigDecimal value, LogicalType.DecimalType type,
            PrecisionLossPolicy policy) {
        BigInteger unscaled = decimalToUnscaled(field, value, type, policy);
        try {
            return unscaled.intValueExact();
        }
        catch (ArithmeticException e) {
            throw new IllegalArgumentException("Field " + field + ": " + value
                    + " does not fit the INT32 storage of " + type, e);
        }
    }

    /// The unscaled value of a `DECIMAL` stored in an `INT64`.
    public static long decimalToLong(String field, BigDecimal value, LogicalType.DecimalType type,
            PrecisionLossPolicy policy) {
        BigInteger unscaled = decimalToUnscaled(field, value, type, policy);
        try {
            return unscaled.longValueExact();
        }
        catch (ArithmeticException e) {
            throw new IllegalArgumentException("Field " + field + ": " + value
                    + " does not fit the INT64 storage of " + type, e);
        }
    }

    /// The unscaled value of a `DECIMAL` stored in a byte array, big-endian two's complement.
    /// `typeLength` is the declared width of a `FIXED_LEN_BYTE_ARRAY` column, or `-1` for a
    /// `BYTE_ARRAY` column, whose values carry their own length.
    public static byte[] decimalToBytes(String field, BigDecimal value, LogicalType.DecimalType type,
                                        int typeLength, PrecisionLossPolicy policy) {
        byte[] minimal = decimalToUnscaled(field, value, type, policy).toByteArray();
        if (typeLength < 0) {
            return minimal;
        }
        if (minimal.length > typeLength) {
            throw new IllegalArgumentException("Field " + field + ": " + value + " needs " + minimal.length
                    + " bytes but the column is FIXED_LEN_BYTE_ARRAY(" + typeLength + ")");
        }
        byte[] padded = new byte[typeLength];
        // Sign-extend into the leading bytes so the two's complement value is preserved.
        byte fill = (byte) (minimal[0] < 0 ? 0xFF : 0x00);
        int prefix = typeLength - minimal.length;
        for (int i = 0; i < prefix; i++) {
            padded[i] = fill;
        }
        System.arraycopy(minimal, 0, padded, prefix, minimal.length);
        return padded;
    }

    /// Rescales to the column's declared scale — rejecting a rescale that would drop digits,
    /// or dropping them toward zero, as the policy says — and checks the result against the
    /// declared precision, which no policy relaxes.
    private static BigInteger decimalToUnscaled(String field, BigDecimal value, LogicalType.DecimalType type,
                                                PrecisionLossPolicy policy) {
        BigDecimal rescaled;
        if (policy == PrecisionLossPolicy.TRUNCATE) {
            rescaled = value.setScale(type.scale(), RoundingMode.DOWN);
        }
        else {
            try {
                rescaled = value.setScale(type.scale());
            }
            catch (ArithmeticException e) {
                throw new IllegalArgumentException("Field " + field + ": " + value
                        + " cannot be rescaled to the column's " + type + " without dropping digits. Rescale it "
                        + "at the call site (for example BigDecimal.setScale(" + type.scale()
                        + ", RoundingMode.HALF_UP)), declare a finer scale, or configure "
                        + "WriterConfig.precisionLossPolicy(TRUNCATE)", e);
            }
        }
        if (rescaled.precision() > type.precision()) {
            throw new IllegalArgumentException("Field " + field + ": " + value + " has precision "
                    + rescaled.precision() + ", exceeding the column's " + type);
        }
        return rescaled.unscaledValue();
    }

    /// The 16 big-endian bytes of a `UUID`, most significant half first.
    public static byte[] uuidToBytes(UUID value) {
        byte[] bytes = new byte[UUID_BYTES];
        writeLongBigEndian(bytes, 0, value.getMostSignificantBits());
        writeLongBigEndian(bytes, Long.BYTES, value.getLeastSignificantBits());
        return bytes;
    }

    private static void writeLongBigEndian(byte[] target, int offset, long value) {
        for (int i = 0; i < Long.BYTES; i++) {
            target[offset + i] = (byte) (value >>> (Long.SIZE - Byte.SIZE - i * Byte.SIZE));
        }
    }

    /// The 12 bytes of an `INTERVAL`: months, days and millis as little-endian unsigned
    /// 4-byte fields, the layout [LogicalTypeConverter#bytesToInterval] reads back.
    public static byte[] intervalToBytes(String field, PqInterval value) {
        byte[] bytes = new byte[INTERVAL_BYTES];
        writeUnsignedIntLittleEndian(field, bytes, 0, value.months(), "months");
        writeUnsignedIntLittleEndian(field, bytes, Integer.BYTES, value.days(), "days");
        writeUnsignedIntLittleEndian(field, bytes, 2 * Integer.BYTES, value.milliseconds(), "millis");
        return bytes;
    }

    private static void writeUnsignedIntLittleEndian(String field, byte[] target, int offset, long value,
                                                     String component) {
        if (value < 0 || value > 0xFFFFFFFFL) {
            throw new IllegalArgumentException("Field " + field + ": INTERVAL " + component + " is " + value
                    + ", outside the unsigned 32-bit range the format stores");
        }
        for (int i = 0; i < Integer.BYTES; i++) {
            target[offset + i] = (byte) (value >>> (i * Byte.SIZE));
        }
    }
}
