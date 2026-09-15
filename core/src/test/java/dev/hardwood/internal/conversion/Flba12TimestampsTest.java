/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.conversion;

import java.math.BigInteger;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import dev.hardwood.metadata.LogicalType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The `FIXED_LEN_BYTE_ARRAY(12)` `TIMESTAMP` decoder and encoder. The arithmetic is a 96-bit floor
/// division and its inverse, so most cases pin values that a truncating divide, a sign slip or a
/// 64-bit intermediate would get wrong.
class Flba12TimestampsTest {

    private static final LogicalType.TimeUnit NANOS = LogicalType.TimeUnit.NANOS;
    private static final LogicalType.TimeUnit MICROS = LogicalType.TimeUnit.MICROS;
    private static final LogicalType.TimeUnit MILLIS = LogicalType.TimeUnit.MILLIS;

    /// Little-endian two's complement encoding, written independently of the
    /// production encoder so the two can disagree.
    private static byte[] le(BigInteger value) {
        byte[] bytes = new byte[Flba12Timestamps.WIDTH];
        BigInteger unsigned = value.signum() < 0
                ? value.add(BigInteger.ONE.shiftLeft(96))
                : value;
        for (int i = 0; i < Flba12Timestamps.WIDTH; i++) {
            bytes[i] = unsigned.shiftRight(8 * i).byteValue();
        }
        return bytes;
    }

    private static byte[] le(long value) {
        return le(BigInteger.valueOf(value));
    }

    @Test
    void decodesTheEpoch() {
        assertThat(Flba12Timestamps.toInstant(le(0), NANOS)).isEqualTo(Instant.EPOCH);
    }

    @Test
    void decodesPositiveValuesWithinTheLongRange() {
        Instant expected = Instant.parse("2026-08-13T12:34:56.123456789Z");
        long nanos = expected.getEpochSecond() * 1_000_000_000L + expected.getNano();
        assertThat(Flba12Timestamps.toInstant(le(nanos), NANOS)).isEqualTo(expected);
    }

    /// A truncating divide would put this one second late, at the epoch itself.
    @Test
    void roundsNegativeValuesTowardNegativeInfinity() {
        assertThat(Flba12Timestamps.toInstant(le(-1), NANOS))
                .isEqualTo(Instant.parse("1969-12-31T23:59:59.999999999Z"));
        assertThat(Flba12Timestamps.toInstant(le(-1_000_000_000L), NANOS))
                .isEqualTo(Instant.parse("1969-12-31T23:59:59Z"));
        assertThat(Flba12Timestamps.toInstant(le(-1_000_000_001L), NANOS))
                .isEqualTo(Instant.parse("1969-12-31T23:59:58.999999999Z"));
    }

    /// The whole point of the format: year 1 and year 9999 in nanoseconds both need
    /// more than 64 bits, so `INT64` cannot carry them at all.
    @Test
    void decodesTheSqlRangeEndpointsBeyondTheLongRange() {
        Instant year1 = Instant.parse("0001-01-01T00:00:00Z");
        Instant year9999 = Instant.parse("9999-12-31T23:59:59.999999999Z");

        assertThat(nanosOf(year1).bitLength()).isGreaterThan(63);
        assertThat(nanosOf(year9999).bitLength()).isGreaterThan(63);

        assertThat(Flba12Timestamps.toInstant(le(nanosOf(year1)), NANOS)).isEqualTo(year1);
        assertThat(Flba12Timestamps.toInstant(le(nanosOf(year9999)), NANOS)).isEqualTo(year9999);
    }

    @Test
    void decodesMillisAndMicrosUnits() {
        assertThat(Flba12Timestamps.toInstant(le(1_500L), MILLIS))
                .isEqualTo(Instant.parse("1970-01-01T00:00:01.500Z"));
        assertThat(Flba12Timestamps.toInstant(le(-1L), MILLIS))
                .isEqualTo(Instant.parse("1969-12-31T23:59:59.999Z"));
        assertThat(Flba12Timestamps.toInstant(le(1_500_000L), MICROS))
                .isEqualTo(Instant.parse("1970-01-01T00:00:01.500Z"));
        assertThat(Flba12Timestamps.toInstant(le(-1L), MICROS))
                .isEqualTo(Instant.parse("1969-12-31T23:59:59.999999Z"));
    }

    @Test
    void decodesWallClockTimestampsWithTheSameArithmetic() {
        LocalDateTime expected = LocalDateTime.parse("0001-01-01T00:00:00");
        BigInteger nanos = nanosOf(expected.toInstant(ZoneOffset.UTC));
        assertThat(Flba12Timestamps.toLocalDateTime(le(nanos), NANOS)).isEqualTo(expected);
        assertThat(Flba12Timestamps.toLocalDateTime(le(-1), NANOS))
                .isEqualTo(LocalDateTime.parse("1969-12-31T23:59:59.999999999"));
    }

    /// Agreement with `BigInteger` over the sign boundary and over the 64-bit
    /// boundary, where the limb arithmetic is easiest to get wrong.
    @ParameterizedTest
    @EnumSource(LogicalType.TimeUnit.class)
    void agreesWithBigIntegerArithmeticOverRandomValues(LogicalType.TimeUnit unit) {
        Random random = new Random(20260813L);
        BigInteger divisor = BigInteger.valueOf(switch (unit) {
            case MILLIS -> 1_000;
            case MICROS -> 1_000_000;
            case NANOS -> 1_000_000_000;
        });
        // The widest count Instant can hold at this unit. Even at MILLIS that is past
        // the long range, so every unit exercises the multi-limb path.
        BigInteger bound = BigInteger.valueOf(Instant.MAX.getEpochSecond()).multiply(divisor);
        assertThat(bound.bitLength()).isGreaterThan(64);
        for (int i = 0; i < 2_000; i++) {
            BigInteger value = new BigInteger(bound.bitLength(), random).mod(bound);
            if (random.nextBoolean()) {
                value = value.negate();
            }
            // BigInteger#mod is always non-negative, so subtracting it leaves an exact
            // multiple and the quotient is the floor.
            int expectedSubSecond = value.mod(divisor).intValueExact();
            long expectedSeconds = value.subtract(value.mod(divisor)).divide(divisor).longValueExact();

            Instant decoded = Flba12Timestamps.toInstant(le(value), unit);
            assertThat(decoded.getEpochSecond()).as("seconds for %s", value).isEqualTo(expectedSeconds);
            assertThat(decoded.getNano()).as("nanos for %s", value)
                    .isEqualTo(expectedSubSecond * nanosPerUnit(unit));
        }
    }

    /// A seconds count past a `long`, which puts the value past `Instant` as well.
    @Test
    void rejectsACountOfSecondsPastALong() {
        BigInteger tooLarge = BigInteger.ONE.shiftLeft(95).subtract(BigInteger.ONE);
        assertThatThrownBy(() -> Flba12Timestamps.toInstant(le(tooLarge), NANOS))
                .isInstanceOf(DateTimeException.class)
                .hasMessage("The FIXED_LEN_BYTE_ARRAY(12) TIMESTAMP 0xffffffffffffffffffffff7f (little-endian) "
                        + "lies outside the range of Instant");
        assertThatThrownBy(() -> Flba12Timestamps.toLocalDateTime(le(BigInteger.ONE.shiftLeft(95).negate()), MILLIS))
                .isInstanceOf(DateTimeException.class)
                .hasMessage("The FIXED_LEN_BYTE_ARRAY(12) TIMESTAMP 0x000000000000000000000080 (little-endian) "
                        + "lies outside the range of LocalDateTime");
    }

    /// A seconds count that fits a `long` but lies past the year 1 000 000 000.
    @Test
    void rejectsACountOfSecondsPastTheCalendar() {
        BigInteger pastInstant = BigInteger.valueOf(Instant.MAX.getEpochSecond() + 1).multiply(BigInteger.valueOf(1_000));
        assertThatThrownBy(() -> Flba12Timestamps.toInstant(le(pastInstant), MILLIS))
                .isInstanceOf(DateTimeException.class)
                .hasMessage("The FIXED_LEN_BYTE_ARRAY(12) TIMESTAMP 0x00a8e0d72298f0b501000000 (little-endian) "
                        + "lies outside the range of Instant");
    }

    @Test
    void rejectsAWrongWidthValue() {
        assertThatThrownBy(() -> Flba12Timestamps.toInstant(new byte[8], NANOS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("A FIXED_LEN_BYTE_ARRAY(12) TIMESTAMP is 12 bytes, not 8");
        assertThatThrownBy(() -> Flba12Timestamps.toLocalDateTime(new byte[24], 12, 11, NANOS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("A FIXED_LEN_BYTE_ARRAY(12) TIMESTAMP is 12 bytes, not 11");
    }

    @Test
    void decodesAtAnOffsetWithinAPackedBuffer() {
        byte[] packed = new byte[3 * Flba12Timestamps.WIDTH];
        System.arraycopy(le(-1), 0, packed, Flba12Timestamps.WIDTH, Flba12Timestamps.WIDTH);
        assertThat(Flba12Timestamps.toInstant(packed, Flba12Timestamps.WIDTH, Flba12Timestamps.WIDTH, NANOS))
                .isEqualTo(Instant.parse("1969-12-31T23:59:59.999999999Z"));
    }

    @Test
    void toTemporalFollowsTheUtcFlag() {
        assertThat(Flba12Timestamps.toTemporal(le(-1), 0, 12, LogicalType.timestamp(true, MICROS)))
                .isEqualTo(Instant.parse("1969-12-31T23:59:59.999999Z"));
        assertThat(Flba12Timestamps.toTemporal(le(-1), 0, 12, LogicalType.timestamp(false, MICROS)))
                .isEqualTo(LocalDateTime.parse("1969-12-31T23:59:59.999999"));
    }

    /// The encoder written independently of the decoder, checked against `BigInteger` at the
    /// extremes of `Instant` and across the epoch.
    @ParameterizedTest
    @EnumSource(LogicalType.TimeUnit.class)
    void encodesTheCountOfTheUnit(LogicalType.TimeUnit unit) {
        long unitsPerSecond = 1_000_000_000L / nanosPerUnit(unit);
        for (Instant value : new Instant[] { Instant.MIN, Instant.parse("0001-01-01T00:00:00Z"),
                Instant.parse("1969-12-31T23:59:59.999999999Z"), Instant.EPOCH,
                Instant.parse("9999-12-31T23:59:59.999999999Z"), Instant.MAX }) {
            int unitOfSecond = value.getNano() / nanosPerUnit(unit);
            BigInteger count = BigInteger.valueOf(value.getEpochSecond()).multiply(BigInteger.valueOf(unitsPerSecond))
                    .add(BigInteger.valueOf(unitOfSecond));
            assertThat(Flba12Timestamps.encode(value.getEpochSecond(), unitOfSecond, unit))
                    .as("%s at %s", value, unit)
                    .isEqualTo(le(count));
        }
    }

    @ParameterizedTest
    @EnumSource(LogicalType.TimeUnit.class)
    void encodeRoundTripsThroughTheDecoder(LogicalType.TimeUnit unit) {
        for (Instant value : new Instant[] { Instant.MIN, Instant.parse("0001-01-01T00:00:00Z"),
                Instant.parse("1969-12-31T23:59:59Z"), Instant.EPOCH, Instant.parse("2026-08-13T12:34:56Z"),
                Instant.MAX.minusNanos(Instant.MAX.getNano()) }) {
            byte[] encoded = Flba12Timestamps.encode(value.getEpochSecond(), 0, unit);
            assertThat(Flba12Timestamps.toInstant(encoded, unit)).as("round trip of %s at %s", value, unit)
                    .isEqualTo(value);
        }
    }

    @Test
    void wordLoadsSplitTheValueAtTheSixtyFourthBit() {
        BigInteger value = BigInteger.ONE.shiftLeft(64).add(BigInteger.valueOf(7));
        byte[] bytes = le(value);
        assertThat(Flba12Timestamps.highWord(bytes, 0)).isEqualTo(1);
        assertThat(Flba12Timestamps.lowWord(bytes, 0)).isEqualTo(7L);

        byte[] negative = le(-1);
        assertThat(Flba12Timestamps.highWord(negative, 0)).isEqualTo(-1);
        assertThat(Flba12Timestamps.lowWord(negative, 0)).isEqualTo(-1L);
    }

    private static BigInteger nanosOf(Instant value) {
        return BigInteger.valueOf(value.getEpochSecond())
                .multiply(BigInteger.valueOf(1_000_000_000L))
                .add(BigInteger.valueOf(value.getNano()));
    }

    private static int nanosPerUnit(LogicalType.TimeUnit unit) {
        return switch (unit) {
            case MILLIS -> 1_000_000;
            case MICROS -> 1_000;
            case NANOS -> 1;
        };
    }
}
