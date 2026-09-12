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

    @Test
    void int96OfAnotherWidthIsRefused() {
        assertThatThrownBy(() -> BinaryComparator.compareInt96(new byte[12], new byte[16]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("An INT96 value is 12 bytes, not 16");
    }

    private static byte[] int96(long nanosOfDay, int julianDay) {
        return ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN).putLong(nanosOfDay).putInt(julianDay).array();
    }
}
