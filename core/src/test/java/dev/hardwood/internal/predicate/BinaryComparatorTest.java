/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BinaryComparatorTest {

    private static final long NANOS_PER_DAY = 86_400_000_000_000L;

    @Test
    void int96ComparesByDayBeforeNanosecondsOfTheDay() {
        byte[] noonOnTheThirteenth = int96(NANOS_PER_DAY / 2, 2_460_262);
        byte[] midnightOnTheFourteenth = int96(0, 2_460_263);

        assertThat(BinaryComparator.compareInt96(noonOnTheThirteenth, midnightOnTheFourteenth)).isNegative();
        assertThat(BinaryComparator.compareInt96(midnightOnTheFourteenth, noonOnTheThirteenth)).isPositive();
        assertThat(Arrays.compareUnsigned(noonOnTheThirteenth, midnightOnTheFourteenth))
                .as("byte-wise the low byte of the nanoseconds leads, which orders the two the other way")
                .isPositive();
    }

    /// The format does not bound the nanoseconds by a day, so one instant has many encodings.
    @Test
    void int96EncodingsOfOneInstantCompareEqual() {
        byte[] canonical = int96(5, 2_460_263);

        assertThat(BinaryComparator.compareInt96(canonical, int96(NANOS_PER_DAY + 5, 2_460_262))).isZero();
        assertThat(BinaryComparator.compareInt96(canonical, int96(-NANOS_PER_DAY + 5, 2_460_264))).isZero();
        assertThat(BinaryComparator.compareInt96(canonical, int96(NANOS_PER_DAY + 6, 2_460_262))).isNegative();
    }

    @Test
    void int96ComparesAtTheExtremesWithoutOverflow() {
        byte[] latest = int96(Long.MAX_VALUE, Integer.MAX_VALUE);
        byte[] earliest = int96(Long.MIN_VALUE, Integer.MIN_VALUE);

        assertThat(BinaryComparator.compareInt96(latest, int96(NANOS_PER_DAY - 1, Integer.MAX_VALUE))).isPositive();
        assertThat(BinaryComparator.compareInt96(earliest, int96(0, Integer.MIN_VALUE))).isNegative();
        assertThat(BinaryComparator.compareInt96(earliest, latest)).isNegative();
    }

    /// The last byte carries the sign and leads; byte-wise, the first byte leads and a negative value
    /// sorts above every positive one.
    @Test
    void signedLittleEndianComparesByTheValue() {
        byte[] minusOne = littleEndian(-1);
        byte[] one = littleEndian(1);
        byte[] twoFiftySix = littleEndian(256);
        byte[] beyondALong = littleEndian(0);
        beyondALong[8] = 1;

        assertThat(BinaryComparator.compareSignedLittleEndian(minusOne, one)).isNegative();
        assertThat(BinaryComparator.compareSignedLittleEndian(one, twoFiftySix)).isNegative();
        assertThat(BinaryComparator.compareSignedLittleEndian(twoFiftySix, beyondALong)).isNegative();
        assertThat(BinaryComparator.compareSignedLittleEndian(littleEndian(-256), minusOne)).isNegative();
        assertThat(BinaryComparator.compareSignedLittleEndian(one, littleEndian(1))).isZero();
        assertThat(Arrays.compareUnsigned(minusOne, one))
                .as("byte-wise the sign byte comes last, which orders the two the other way")
                .isPositive();
        assertThat(Arrays.compareUnsigned(one, twoFiftySix))
                .as("byte-wise the least significant byte leads")
                .isPositive();
    }

    @Test
    void signedLittleEndianOfDifferentWidthsIsRefused() {
        assertThatThrownBy(() -> BinaryComparator.compareSignedLittleEndian(new byte[12], new byte[11]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("A little-endian signed comparison takes values of one width, not 12 and 11 bytes");
    }

    @Test
    void int96OfAnotherWidthIsRefused() {
        assertThatThrownBy(() -> BinaryComparator.compareInt96(new byte[12], new byte[16]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("An INT96 value is 12 bytes, not 16");
    }

    /// `value` as twelve little-endian two's complement bytes.
    private static byte[] littleEndian(long value) {
        byte[] bytes = new byte[12];
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).putLong(value).putInt(value < 0 ? -1 : 0);
        return bytes;
    }

    private static byte[] int96(long nanosOfDay, int julianDay) {
        return ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN).putLong(nanosOfDay).putInt(julianDay).array();
    }
}
