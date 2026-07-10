/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// Unit tests for [DictionaryEncoder]: first-seen index assignment, repeat lookup, byte-size
/// accounting, and that the open-addressed table stays correct across a resize.
class DictionaryEncoderTest {

    @Test
    void assignsIndicesInFirstSeenOrder() {
        DictionaryEncoder dict = new DictionaryEncoder();

        assertThat(dict.indexOf(42)).isEqualTo(-1);
        assertThat(dict.add(42)).isEqualTo(0);
        assertThat(dict.add(7)).isEqualTo(1);
        assertThat(dict.add(-3)).isEqualTo(2);

        // Repeats resolve to their assigned index without growing the dictionary.
        assertThat(dict.indexOf(42)).isEqualTo(0);
        assertThat(dict.indexOf(7)).isEqualTo(1);
        assertThat(dict.indexOf(-3)).isEqualTo(2);
        assertThat(dict.size()).isEqualTo(3);
        assertThat(dict.byteSize()).isEqualTo(3L * Integer.BYTES);
        assertThat(Arrays.copyOf(dict.values(), dict.size())).containsExactly(42, 7, -3);
    }

    @Test
    void handlesZeroValueDistinctFromEmptySlot() {
        // The empty-slot sentinel is the index (-1), not the value, so value 0 at index 0 is
        // found rather than mistaken for a free slot.
        DictionaryEncoder dict = new DictionaryEncoder();
        assertThat(dict.add(0)).isEqualTo(0);
        assertThat(dict.indexOf(0)).isEqualTo(0);
    }

    @Test
    void staysCorrectAcrossTableResize() {
        // Insert enough distinct values to force at least one rehash, then verify every value
        // still resolves to its original first-seen index and the value order is preserved.
        DictionaryEncoder dict = new DictionaryEncoder();
        int n = 5_000;
        for (int i = 0; i < n; i++) {
            assertThat(dict.add(i * 13 - 1)).isEqualTo(i);
        }
        assertThat(dict.size()).isEqualTo(n);
        for (int i = 0; i < n; i++) {
            assertThat(dict.indexOf(i * 13 - 1)).isEqualTo(i);
        }
        assertThat(dict.indexOf(999_999)).isEqualTo(-1);
        int[] values = Arrays.copyOf(dict.values(), dict.size());
        for (int i = 0; i < n; i++) {
            assertThat(values[i]).isEqualTo(i * 13 - 1);
        }
    }
}
