/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.util.Arrays;
import java.util.Random;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.schema.ByteColumnOrder;
import dev.hardwood.metadata.Statistics;

import static org.assertj.core.api.Assertions.assertThat;

/// The unsigned bounds [BinaryStatisticsCollector] accumulates agree with `Arrays.compareUnsigned`,
/// including where the fast prefix test reads a short value at the end of its array and where the
/// first eight bytes are equal and only the remainder decides.
class BinaryStatisticsCollectorTest {

    @Test
    void unsignedBoundsAgreeWithArraysCompareUnsigned() {
        Random random = new Random(20_260_819L);
        BinaryStatisticsCollector collector = collector();
        byte[] expectedMin = null;
        byte[] expectedMax = null;

        for (int i = 0; i < 10_000; i++) {
            int length = random.nextInt(25);
            byte[] value = new byte[length];
            random.nextBytes(value);

            int offset = random.nextInt(8);
            int trailing = random.nextInt(9);
            byte[] backing = new byte[offset + length + trailing];
            random.nextBytes(backing);
            System.arraycopy(value, 0, backing, offset, length);

            collector.accept(backing, offset, length);

            if (expectedMin == null || Arrays.compareUnsigned(value, expectedMin) < 0) {
                expectedMin = value.clone();
            }
            if (expectedMax == null || Arrays.compareUnsigned(value, expectedMax) > 0) {
                expectedMax = value.clone();
            }
        }

        Statistics statistics = collector.toStatistics();
        assertThat(statistics.minValue()).isEqualTo(expectedMin);
        assertThat(statistics.maxValue()).isEqualTo(expectedMax);
    }

    @Test
    void unsignedBoundsCompareLengthsAndEqualPrefixes() {
        byte[] empty = {};
        byte[] zero = { 0 };
        byte[] eightZeros = new byte[Long.BYTES];
        byte[] nineZeros = new byte[Long.BYTES + 1];
        byte[] prefixedLow = new byte[12];
        byte[] prefixedHigh = new byte[12];
        Arrays.fill(prefixedLow, 0, Long.BYTES, (byte) 0x61);
        Arrays.fill(prefixedHigh, 0, Long.BYTES, (byte) 0x61);
        prefixedLow[Long.BYTES] = 1;
        prefixedHigh[Long.BYTES] = 2;
        byte[] allOnes = { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF };

        BinaryStatisticsCollector collector = collector();
        collector.accept(nineZeros, 0, nineZeros.length);
        Statistics statistics = collector.toStatistics();
        assertThat(statistics.minValue()).isEqualTo(nineZeros);
        assertThat(statistics.maxValue()).isEqualTo(nineZeros);

        // The first eight bytes tie, so the shorter value sorts first.
        collector.accept(eightZeros, 0, eightZeros.length);
        statistics = collector.toStatistics();
        assertThat(statistics.minValue()).isEqualTo(eightZeros);
        assertThat(statistics.maxValue()).isEqualTo(nineZeros);

        // A one-byte value is also a prefix of both longer zero values.
        collector.accept(zero, 0, zero.length);
        statistics = collector.toStatistics();
        assertThat(statistics.minValue()).isEqualTo(zero);
        assertThat(statistics.maxValue()).isEqualTo(nineZeros);

        collector.accept(allOnes, 0, allOnes.length);
        statistics = collector.toStatistics();
        assertThat(statistics.minValue()).isEqualTo(zero);
        assertThat(statistics.maxValue()).isEqualTo(allOnes);

        collector.accept(empty, 0, empty.length);
        statistics = collector.toStatistics();
        assertThat(statistics.minValue()).isEqualTo(empty);
        assertThat(statistics.maxValue()).isEqualTo(allOnes);

        // Equal eight-byte prefixes leave the tail to decide both bounds.
        BinaryStatisticsCollector equalPrefix = collector();
        equalPrefix.accept(prefixedHigh, 0, prefixedHigh.length);
        equalPrefix.accept(prefixedLow, 0, prefixedLow.length);
        statistics = equalPrefix.toStatistics();
        assertThat(statistics.minValue()).isEqualTo(prefixedLow);
        assertThat(statistics.maxValue()).isEqualTo(prefixedHigh);
    }

    @Test
    void mergeCarriesCachedPrefixesForBothBounds() {
        BinaryStatisticsCollector chunk = collector();
        chunk.accept(value(0x80, 0x00), 0, Long.BYTES + 1);

        BinaryStatisticsCollector page = collector();
        page.accept(value(0xF0, 0x20), 0, Long.BYTES + 1);
        page.accept(value(0x10, 0x20), 0, Long.BYTES + 1);

        chunk.mergeValues(page);

        // These values fall between the merged bounds. If the merge left either cached prefix
        // at the chunk's original 0x80 prefix, each would incorrectly extend that bound.
        chunk.accept(value(0x20, 0x00), 0, Long.BYTES + 1);
        chunk.accept(value(0xE0, 0x00), 0, Long.BYTES + 1);

        Statistics statistics = chunk.toStatistics();
        assertThat(statistics.minValue()).isEqualTo(value(0x10, 0x20));
        assertThat(statistics.maxValue()).isEqualTo(value(0xF0, 0x20));

        // A matching prefix still needs the tail comparison after the merge.
        chunk.accept(value(0x10, 0x10), 0, Long.BYTES + 1);
        chunk.accept(value(0xF0, 0x30), 0, Long.BYTES + 1);

        statistics = chunk.toStatistics();
        assertThat(statistics.minValue()).isEqualTo(value(0x10, 0x10));
        assertThat(statistics.maxValue()).isEqualTo(value(0xF0, 0x30));
    }

    private static byte[] value(int prefixByte, int tailByte) {
        byte[] value = new byte[Long.BYTES + 1];
        Arrays.fill(value, 0, Long.BYTES, (byte) prefixByte);
        value[Long.BYTES] = (byte) tailByte;
        return value;
    }

    private static BinaryStatisticsCollector collector() {
        return new BinaryStatisticsCollector(ByteColumnOrder.BYTES, Integer.MAX_VALUE, false);
    }
}
