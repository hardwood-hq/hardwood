/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// The fixed-width dictionary encoders spread key families that agree in their low bits. Measured
/// as the mean number of slots a lookup visits, with each encoder holding [#KEYS] values in its
/// table at up to 75% load.
class DictionarySlotsTest {

    private static final int KEYS = 3_000;
    private static final double MAX_MEAN_PROBES = 3.0;

    @Test
    void spreadsDecimalDoubles() {
        LongDictionaryEncoder dictionary = new LongDictionaryEncoder();
        for (int i = 0; i < KEYS; i++) {
            dictionary.add(Double.doubleToRawLongBits((i - KEYS / 2) / 10.0));
        }
        assertThat(dictionary.meanProbes()).isLessThan(MAX_MEAN_PROBES);
    }

    @Test
    void spreadsDecimalFloats() {
        DictionaryEncoder dictionary = new DictionaryEncoder();
        for (int i = 0; i < KEYS; i++) {
            dictionary.add(Float.floatToRawIntBits((i - KEYS / 2) / 10.0f));
        }
        assertThat(dictionary.meanProbes()).isLessThan(MAX_MEAN_PROBES);
    }

    @Test
    void spreadsLongMultiplesOfAPowerOfTwo() {
        LongDictionaryEncoder dictionary = new LongDictionaryEncoder();
        for (int i = 0; i < KEYS; i++) {
            dictionary.add((long) i << 20);
        }
        assertThat(dictionary.meanProbes()).isLessThan(MAX_MEAN_PROBES);
    }

    @Test
    void spreadsIntMultiplesOfAPowerOfTwo() {
        DictionaryEncoder dictionary = new DictionaryEncoder();
        for (int i = 0; i < KEYS; i++) {
            dictionary.add(i << 16);
        }
        assertThat(dictionary.meanProbes()).isLessThan(MAX_MEAN_PROBES);
    }

    @Test
    void shiftLeavesTheSlotBits() {
        assertThat(DictionarySlots.shift(64)).isEqualTo(58);
        for (long key : new long[]{ 0, 1, -1, Long.MIN_VALUE, Long.MAX_VALUE, 0x123456789L }) {
            assertThat(DictionarySlots.home(key, DictionarySlots.shift(64))).isBetween(0, 63);
        }
    }
}
