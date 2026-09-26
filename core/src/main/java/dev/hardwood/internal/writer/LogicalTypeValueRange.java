/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.math.BigInteger;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// The range of stored values a column's logical-type annotation declares.
///
/// An annotation narrows what its physical type may hold: an `INT32` annotated `INT(8)` holds
/// 256 of its bit patterns, one annotated `DECIMAL(9, 2)` holds the unscaled values of at most
/// nine digits. A value outside that range produces a file whose values fall outside the range
/// its own annotation declares — a `uint8` consumer reads a stored 300 back as 44, and the
/// bounds in the column's statistics describe values the annotation says cannot exist.
///
/// The range is resolved once per column and applied by both write APIs, so which entry point
/// a caller picked does not decide whether an out-of-range value is caught. Which check applies
/// follows from the column's physical type: [#contains(long)] for an integral column,
/// [#containsUnscaled] for a binary `DECIMAL`.
///
/// A column whose annotation narrows nothing resolves to an unbounded range, which reports
/// [#isBounded()] as `false` so the caller skips the per-value scan altogether. `UNKNOWN` is the
/// opposite end: its range holds no value at all, which [#holdsNoValue()] reports and which the
/// APIs enforce on the column's nulls rather than on its values, there being none to check.
public final class LogicalTypeValueRange {

    /// The range of a column no annotation narrows.
    private static final LogicalTypeValueRange UNBOUNDED = new LogicalTypeValueRange(null, 0, 0, null, 0, false);

    /// The empty range of a column annotated `UNKNOWN`, which holds only nulls.
    private static final LogicalTypeValueRange NO_VALUE = new LogicalTypeValueRange(null, 0, 0, null, 0, true);

    /// Powers of ten up to the largest an `INT64` holds, so a `DECIMAL` bound needs no
    /// arithmetic per column.
    private static final long[] POWERS_OF_TEN = powersOfTen();

    /// Digits of unscaled value an `INT64` holds, the widest precision an integral `DECIMAL`
    /// declares.
    private static final int INT64_DIGITS = 18;

    /// The annotation the bound comes from, named in the caller's rejection. `null` marks the
    /// unbounded range.
    private final LogicalType logicalType;

    /// Inclusive bounds of an integral column.
    private final long min;
    private final long max;

    /// The largest unscaled magnitude a binary `DECIMAL` column's values may reach, `null` for
    /// any other column.
    private final BigInteger unscaledBound;

    /// The longest value of a binary `DECIMAL` column that is in range whatever its bytes are,
    /// so only a longer one is worth decoding.
    private final int alwaysSafeBytes;

    /// Whether the column's annotation admits no value at all, which only `UNKNOWN` does.
    private final boolean noValue;

    private LogicalTypeValueRange(LogicalType logicalType, long min, long max, BigInteger unscaledBound,
                            int alwaysSafeBytes, boolean noValue) {
        this.logicalType = logicalType;
        this.min = min;
        this.max = max;
        this.unscaledBound = unscaledBound;
        this.alwaysSafeBytes = alwaysSafeBytes;
        this.noValue = noValue;
    }

    /// The range of every column of `schema`, in column order.
    public static LogicalTypeValueRange[] forSchema(FileSchema schema) {
        LogicalTypeValueRange[] ranges = new LogicalTypeValueRange[schema.getColumnCount()];
        for (int c = 0; c < ranges.length; c++) {
            ranges[c] = of(schema.getColumn(c));
        }
        return ranges;
    }

    /// The range `column`'s annotation declares.
    public static LogicalTypeValueRange of(ColumnSchema column) {
        LogicalType logicalType = column.logicalType();
        if (logicalType == null) {
            return UNBOUNDED;
        }
        if (logicalType instanceof LogicalType.NullType) {
            return NO_VALUE;
        }
        return switch (column.type()) {
            case INT32, INT64 -> integral(column.type(), logicalType);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> binaryDecimal(logicalType);
            default -> UNBOUNDED;
        };
    }

    /// Whether the annotation narrows the physical type at all. A caller checks this once and
    /// skips the per-value scan when it is `false`.
    public boolean isBounded() {
        return logicalType != null;
    }

    /// Whether the annotation admits no value at all, so every row of the column must be null.
    /// `UNKNOWN` is the only annotation that does: it describes a column holding nothing, and the
    /// reader refuses to materialize a value found under one.
    public boolean holdsNoValue() {
        return noValue;
    }

    /// The annotation the bound comes from, for the caller's rejection message.
    public LogicalType annotation() {
        return logicalType;
    }

    /// The smallest value an integral column may hold.
    ///
    /// @throws IllegalStateException if the annotation narrows nothing, there being no such value
    ///         to report — the unbounded range's zero would otherwise read as a bound
    public long min() {
        requireBounded();
        return min;
    }

    /// The largest value an integral column may hold.
    ///
    /// @throws IllegalStateException if the annotation narrows nothing
    public long max() {
        requireBounded();
        return max;
    }

    private void requireBounded() {
        if (!isBounded()) {
            throw new IllegalStateException("An unbounded range has no bounds to report; check isBounded() first");
        }
    }

    /// The largest unscaled magnitude a binary `DECIMAL` column may hold, `null` for any other
    /// column.
    public BigInteger unscaledBound() {
        return unscaledBound;
    }

    /// Whether `value` is within an integral column's declared range.
    public boolean contains(long value) {
        return value >= min && value <= max;
    }

    /// Whether `value`, the big-endian two's complement unscaled value of a binary `DECIMAL`,
    /// is within the declared precision.
    ///
    /// An empty value is not: two's complement has no zero-byte encoding, so it denotes no
    /// unscaled value at all and the reader raises `Zero length BigInteger` on it.
    public boolean containsUnscaled(byte[] value) {
        if (value.length == 0) {
            return false;
        }
        if (value.length <= alwaysSafeBytes) {
            return true;
        }
        return new BigInteger(value).abs().compareTo(unscaledBound) <= 0;
    }

    /// The range of an `INT32` or `INT64` column. An `INT(n)` narrower than its physical type
    /// bounds the value to what `n` bits hold; `INT(32)` / `INT(64)` and their unsigned forms
    /// bound nothing, because every bit pattern of the physical type is a value of the column
    /// and spelling a large unsigned one as a negative is the only way to reach it — which is
    /// also how the reader returns it.
    ///
    /// A `TIME` bounds the value to one day of its unit, and a `DECIMAL` to the digits its
    /// precision declares. That precision never exceeds what the physical type can hold: the
    /// schema builder's `LogicalTypeValidator` refuses a wider one, and a schema read from a file
    /// has it dropped by `LeafAnnotation.dropFault`, which counts digits the same way.
    private static LogicalTypeValueRange integral(PhysicalType type, LogicalType logicalType) {
        int typeBits = type == PhysicalType.INT32 ? Integer.SIZE : Long.SIZE;
        if (logicalType instanceof LogicalType.IntType intType) {
            int bitWidth = intType.bitWidth();
            if (bitWidth >= typeBits) {
                return UNBOUNDED;
            }
            long min = intType.isSigned() ? -(1L << (bitWidth - 1)) : 0L;
            long max = intType.isSigned() ? (1L << (bitWidth - 1)) - 1 : (1L << bitWidth) - 1;
            return new LogicalTypeValueRange(logicalType, min, max, null, 0, false);
        }
        if (logicalType instanceof LogicalType.TimeType time) {
            return new LogicalTypeValueRange(logicalType, 0L, unitsPerDay(time.unit()) - 1, null, 0, false);
        }
        if (logicalType instanceof LogicalType.DecimalType decimal) {
            long bound = POWERS_OF_TEN[decimal.precision()] - 1;
            return new LogicalTypeValueRange(logicalType, -bound, bound, null, 0, false);
        }
        return UNBOUNDED;
    }

    /// The range of a `BYTE_ARRAY` or `FIXED_LEN_BYTE_ARRAY` column annotated `DECIMAL`, whose
    /// values are big-endian two's complement unscaled values of at most the declared precision.
    private static LogicalTypeValueRange binaryDecimal(LogicalType logicalType) {
        if (!(logicalType instanceof LogicalType.DecimalType decimal)) {
            return UNBOUNDED;
        }
        BigInteger bound = BigInteger.TEN.pow(decimal.precision()).subtract(BigInteger.ONE);
        // A value of L bytes spans at most 2^(8L-1) in magnitude, so it is in range whatever
        // its bytes are while 2^(8L-1) <= bound — which holds for every L up to the bound's
        // bit length divided by eight.
        return new LogicalTypeValueRange(logicalType, 0, 0, bound, bound.bitLength() / Byte.SIZE, false);
    }

    /// How many of `unit` a day holds, which is one past the largest value a `TIME` column can
    /// carry: the annotation defines the value as the elapsed time *after midnight*, so a value
    /// of a full day or more denotes no time of day. A day exactly — a `24:00:00` spelling — is
    /// outside it, matching the [java.time.LocalTime] the reader materializes.
    private static long unitsPerDay(LogicalType.TimeUnit unit) {
        return switch (unit) {
            case MILLIS -> 86_400_000L;
            case MICROS -> 86_400_000_000L;
            case NANOS -> 86_400_000_000_000L;
        };
    }

    private static long[] powersOfTen() {
        long[] powers = new long[INT64_DIGITS + 1];
        powers[0] = 1L;
        for (int i = 1; i < powers.length; i++) {
            powers[i] = powers[i - 1] * 10L;
        }
        return powers;
    }
}
