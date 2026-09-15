/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.conversion;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.Temporal;
import java.util.HexFormat;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;

/// The `TIMESTAMP` values a `FIXED_LEN_BYTE_ARRAY(12)` column stores.
///
/// A value is a **signed two's complement little-endian 96-bit integer** counting the
/// annotation's [LogicalType.TimeUnit] since the Unix epoch. 96 bits of nanoseconds spans roughly
/// ±1.26 × 10¹² years, so the form covers the years 0001–9999 that `INT64` nanoseconds, good for
/// about 585 years, cannot.
///
/// The 12 bytes are handled as two machine words: `hi`, the top 32 bits as a signed `int`, and
/// `lo`, the low 64 bits as an unsigned `long`.
///
/// Not to be confused with the legacy `INT96`, which is also 12 bytes but packs a Julian day beside
/// a count of nanoseconds of the day; see [LogicalTypeConverter#int96ToInstant].
public final class Flba12Timestamps {

    /// Byte width of a value.
    public static final int WIDTH = 12;

    /// Little-endian `byte[]` views. The stored layout is little-endian, so on a little-endian
    /// machine each word is one unaligned native load.
    private static final VarHandle LONG_LE =
            MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle INT_LE =
            MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    private static final HexFormat HEX = HexFormat.of();

    private Flba12Timestamps() {
    }

    /// Whether a column of this physical type and annotation stores its timestamps in 12 bytes
    /// rather than an `INT64`. The width itself is not consulted: `FileSchema` drops a `TIMESTAMP`
    /// from a `FIXED_LEN_BYTE_ARRAY` of any other width, so an annotated column of that physical
    /// type is 12 bytes wide.
    public static boolean isCarriedBy(PhysicalType physicalType, LogicalType logicalType) {
        return physicalType == PhysicalType.FIXED_LEN_BYTE_ARRAY
                && logicalType instanceof LogicalType.TimestampType;
    }

    /// The instant a UTC-adjusted column's value stands for.
    ///
    /// @throws IllegalArgumentException if `bytes` is not [#WIDTH] bytes long
    /// @throws DateTimeException if the value lies outside the range of [Instant]
    public static Instant toInstant(byte[] bytes, LogicalType.TimeUnit unit) {
        return toInstant(bytes, 0, bytes.length, unit);
    }

    /// The instant the `length` bytes at `offset` stand for, for a caller holding the value inside
    /// a larger buffer.
    ///
    /// @throws IllegalArgumentException if `length` is not [#WIDTH]
    /// @throws DateTimeException if the value lies outside the range of [Instant]
    public static Instant toInstant(byte[] bytes, int offset, int length, LogicalType.TimeUnit unit) {
        requireWidth(length);
        long lo = lowWord(bytes, offset);
        int divisor = unitsPerSecond(unit);
        long seconds = floorSeconds(highWord(bytes, offset), lo, divisor, bytes, offset, "Instant");
        try {
            return Instant.ofEpochSecond(seconds, nanoOfSecond(lo, seconds, divisor, unit));
        }
        catch (DateTimeException e) {
            throw outOfRange(bytes, offset, "Instant");
        }
    }

    /// The wall clock a local column's value stands for. The stored count is the offset from the
    /// epoch of the wall clock itself, so the arithmetic that gives an [Instant] gives the
    /// [LocalDateTime] read at UTC.
    ///
    /// @throws IllegalArgumentException if `bytes` is not [#WIDTH] bytes long
    /// @throws DateTimeException if the value lies outside the range of [LocalDateTime]
    public static LocalDateTime toLocalDateTime(byte[] bytes, LogicalType.TimeUnit unit) {
        return toLocalDateTime(bytes, 0, bytes.length, unit);
    }

    /// The wall clock the `length` bytes at `offset` stand for, for a caller holding the value
    /// inside a larger buffer.
    ///
    /// @throws IllegalArgumentException if `length` is not [#WIDTH]
    /// @throws DateTimeException if the value lies outside the range of [LocalDateTime]
    public static LocalDateTime toLocalDateTime(byte[] bytes, int offset, int length, LogicalType.TimeUnit unit) {
        requireWidth(length);
        long lo = lowWord(bytes, offset);
        int divisor = unitsPerSecond(unit);
        long seconds = floorSeconds(highWord(bytes, offset), lo, divisor, bytes, offset, "LocalDateTime");
        try {
            return LocalDateTime.ofEpochSecond(seconds, nanoOfSecond(lo, seconds, divisor, unit), ZoneOffset.UTC);
        }
        catch (DateTimeException e) {
            throw outOfRange(bytes, offset, "LocalDateTime");
        }
    }

    /// The value a column annotated `type` stores in the `length` bytes at `offset`: an [Instant]
    /// where the annotation is UTC-adjusted, a [LocalDateTime] where it is local.
    ///
    /// @throws IllegalArgumentException if `length` is not [#WIDTH]
    /// @throws DateTimeException if the value lies outside the range of the Java type
    public static Temporal toTemporal(byte[] bytes, int offset, int length, LogicalType.TimestampType type) {
        return type.isAdjustedToUTC()
                ? toInstant(bytes, offset, length, type.unit())
                : toLocalDateTime(bytes, offset, length, type.unit());
    }

    /// The value a UTC-adjusted or local column stores for `epochSecond` and `unitOfSecond`, the
    /// count of whole units within that second.
    ///
    /// Every [Instant] and [LocalDateTime] fits: the widest count, `Instant.MAX` in nanoseconds,
    /// is below 2⁸⁵.
    ///
    /// @param epochSecond seconds since the epoch
    /// @param unitOfSecond whole units within the second, in `[0, units per second)`
    public static byte[] encode(long epochSecond, int unitOfSecond, LogicalType.TimeUnit unit) {
        int divisor = unitsPerSecond(unit);
        // epochSecond * divisor as a signed 128-bit product, then the units added with carry.
        long lo = epochSecond * divisor;
        long hi = Math.multiplyHigh(epochSecond, divisor);
        long sum = lo + unitOfSecond;
        if (Long.compareUnsigned(sum, lo) < 0) {
            hi++;
        }
        byte[] bytes = new byte[WIDTH];
        LONG_LE.set(bytes, 0, sum);
        INT_LE.set(bytes, Long.BYTES, Math.toIntExact(hi));
        return bytes;
    }

    /// Nanoseconds per unit of the count.
    public static int nanosPerUnit(LogicalType.TimeUnit unit) {
        return switch (unit) {
            case MILLIS -> 1_000_000;
            case MICROS -> 1_000;
            case NANOS -> 1;
        };
    }

    /// The low 64 bits, as an unsigned quantity in a `long`.
    public static long lowWord(byte[] bytes, int offset) {
        return (long) LONG_LE.get(bytes, offset);
    }

    /// The top 32 bits, sign-extended: the value is `highWord * 2^64 + lowWord`, with `lowWord`
    /// read as unsigned.
    public static int highWord(byte[] bytes, int offset) {
        return (int) INT_LE.get(bytes, offset + Long.BYTES);
    }

    /// Units of the count per second.
    private static int unitsPerSecond(LogicalType.TimeUnit unit) {
        return switch (unit) {
            case MILLIS -> 1_000;
            case MICROS -> 1_000_000;
            case NANOS -> 1_000_000_000;
        };
    }

    /// `floorDiv(value, divisor)` over the 96-bit `hi:lo`, as long division on 32-bit limbs.
    ///
    /// `bytes`, `offset` and `target` are carried only to name the offending value if the quotient
    /// does not fit a `long`, which puts it past `target` as well.
    private static long floorSeconds(int hi, long lo, int divisor, byte[] bytes, int offset, String target) {
        boolean negative = hi < 0;
        // Two's complement negation of the 96-bit value: the low word carries into the high word
        // exactly when it is zero.
        int magHi = negative ? (lo == 0 ? -hi : ~hi) : hi;
        long magLo = negative ? -lo : lo;

        long limb2 = magHi & 0xFFFF_FFFFL;
        long limb1 = magLo >>> 32;
        long limb0 = magLo & 0xFFFF_FFFFL;

        // Each accumulator is `remainder * 2^32 + limb` with remainder < divisor < 2^30, so it
        // stays below 2^62 and the division is ordinary signed long division.
        long quotient2 = limb2 / divisor;
        long remainder = limb2 % divisor;
        long accumulator = (remainder << 32) | limb1;
        long quotient1 = accumulator / divisor;
        remainder = accumulator % divisor;
        accumulator = (remainder << 32) | limb0;
        long quotient0 = accumulator / divisor;
        remainder = accumulator % divisor;

        if (quotient2 != 0 || quotient1 > Integer.MAX_VALUE) {
            throw outOfRange(bytes, offset, target);
        }
        long seconds = (quotient1 << 32) + quotient0;
        if (!negative) {
            return seconds;
        }
        // Round toward negative infinity, as Math#floorDiv does.
        return remainder == 0 ? -seconds : -seconds - 1;
    }

    /// The sub-second part, in nanoseconds.
    ///
    /// `value = seconds * divisor + remainder` holds over the integers, so it holds modulo 2^64,
    /// and `remainder` is below 2^30: recomputing it from the low word in wrapping `long`
    /// arithmetic gives it exactly.
    private static int nanoOfSecond(long lo, long seconds, int divisor, LogicalType.TimeUnit unit) {
        long remainder = lo - seconds * divisor;
        return (int) remainder * nanosPerUnit(unit);
    }

    private static void requireWidth(int length) {
        if (length != WIDTH) {
            throw new IllegalArgumentException(
                    "A FIXED_LEN_BYTE_ARRAY(12) TIMESTAMP is " + WIDTH + " bytes, not " + length);
        }
    }

    private static DateTimeException outOfRange(byte[] bytes, int offset, String target) {
        return new DateTimeException("The FIXED_LEN_BYTE_ARRAY(12) TIMESTAMP 0x"
                + HEX.formatHex(bytes, offset, offset + WIDTH) + " (little-endian) lies outside the range of "
                + target);
    }
}
