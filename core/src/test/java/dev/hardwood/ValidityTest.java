/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.Validity;

import static org.assertj.core.api.Assertions.assertThat;

/// Unit tests for [Validity]'s predicate and index-helper methods. The
/// `ColumnReader`-level tests exercise the singleton/Backed dispatch through
/// the public API; these target the boundary conditions of `nullCount` /
/// `nextNull` / `nextNotNull` directly.
class ValidityTest {

    /// Builds a packed `long[]` of size `(rowCount + 63) >>> 6` with the
    /// given indices set (set-bit = present polarity).
    private static long[] wordsWith(int rowCount, int... presentIndices) {
        long[] w = new long[(rowCount + 63) >>> 6];
        for (int i : presentIndices) {
            w[i >>> 6] |= 1L << i;
        }
        return w;
    }

    @Test
    void noNullsIsSingletonReturnedByOfNull() {
        assertThat(Validity.of(null)).isSameAs(Validity.NO_NULLS);
    }

    @Test
    void noNullsPredicates() {
        Validity v = Validity.NO_NULLS;

        assertThat(v.hasNulls()).isFalse();
        assertThat(v.isNull(0)).isFalse();
        assertThat(v.isNull(100)).isFalse();
        assertThat(v.isNotNull(0)).isTrue();
        assertThat(v.isNotNull(100)).isTrue();
    }

    @Test
    void noNullsNullCountIsZero() {
        assertThat(Validity.NO_NULLS.nullCount(0)).isZero();
        assertThat(Validity.NO_NULLS.nullCount(1)).isZero();
        assertThat(Validity.NO_NULLS.nullCount(1000)).isZero();
    }

    @Test
    void noNullsNextNullAlwaysMinusOne() {
        Validity v = Validity.NO_NULLS;
        assertThat(v.nextNull(0, 10)).isEqualTo(-1);
        assertThat(v.nextNull(5, 10)).isEqualTo(-1);
        assertThat(v.nextNull(10, 10)).isEqualTo(-1);   // from == count
        assertThat(v.nextNull(100, 10)).isEqualTo(-1);  // from > count
    }

    @Test
    void noNullsNextNotNullReturnsFromWhenInRange() {
        Validity v = Validity.NO_NULLS;
        assertThat(v.nextNotNull(0, 10)).isEqualTo(0);
        assertThat(v.nextNotNull(5, 10)).isEqualTo(5);
        assertThat(v.nextNotNull(9, 10)).isEqualTo(9);
        assertThat(v.nextNotNull(10, 10)).isEqualTo(-1);   // from == count → exhausted
        assertThat(v.nextNotNull(100, 10)).isEqualTo(-1);  // from > count
    }

    @Test
    void noNullsWordsReturnsNull() {
        assertThat(Validity.NO_NULLS.words()).isNull();
    }

    @Test
    void backedHasNullsTrue() {
        Validity v = Validity.of(wordsWith(5, 0, 1, 2, 3, 4));
        assertThat(v).isInstanceOf(Validity.Backed.class);
        assertThat(v.hasNulls()).isTrue();
    }

    /// Set bit = present. Item at index 2 is the only absent one.
    @Test
    void backedPredicates() {
        Validity v = Validity.of(wordsWith(5, 0, 1, 3, 4));

        assertThat(v.isNotNull(0)).isTrue();
        assertThat(v.isNotNull(1)).isTrue();
        assertThat(v.isNull(2)).isTrue();
        assertThat(v.isNotNull(3)).isTrue();
        assertThat(v.isNotNull(4)).isTrue();
    }

    @Test
    void backedNullCount() {
        // 3 set bits = 3 present out of 5
        Validity v = Validity.of(wordsWith(5, 0, 2, 4));
        assertThat(v.nullCount(5)).isEqualTo(2);
    }

    @Test
    void backedNextNullFindsClearBitWithinRange() {
        // bit 2 and 4+ are clear
        Validity v = Validity.of(wordsWith(5, 0, 1, 3));
        assertThat(v.nextNull(0, 5)).isEqualTo(2);
        assertThat(v.nextNull(3, 5)).isEqualTo(4);
        assertThat(v.nextNull(0, 2)).isEqualTo(-1);   // count == 2 excludes index 2
    }

    @Test
    void backedNextNullReturnsMinusOneWhenAllPresent() {
        Validity v = Validity.of(wordsWith(5, 0, 1, 2, 3, 4));
        assertThat(v.nextNull(0, 5)).isEqualTo(-1);
        assertThat(v.nextNull(4, 5)).isEqualTo(-1);
    }

    @Test
    void backedNextNotNullFindsSetBit() {
        Validity v = Validity.of(wordsWith(10, 3, 7));
        assertThat(v.nextNotNull(0, 10)).isEqualTo(3);
        assertThat(v.nextNotNull(4, 10)).isEqualTo(7);
        assertThat(v.nextNotNull(8, 10)).isEqualTo(-1);
        assertThat(v.nextNotNull(0, 3)).isEqualTo(-1);   // count == 3 excludes index 3
    }

    @Test
    void backedNextNotNullExhausted() {
        Validity v = Validity.of(new long[1]);   // all clear
        assertThat(v.nextNotNull(0, 5)).isEqualTo(-1);
    }

    @Test
    void backedFromAtCountReturnsMinusOne() {
        Validity v = Validity.of(wordsWith(5, 0, 1, 2, 3, 4));
        assertThat(v.nextNull(5, 5)).isEqualTo(-1);
        assertThat(v.nextNotNull(5, 5)).isEqualTo(-1);
    }

    @Test
    void backedWordsReturnsDefensiveCopy() {
        long[] backing = wordsWith(5, 0, 1, 3, 4);
        Validity v = Validity.of(backing);
        long[] returned = v.words();
        assertThat(returned).isNotSameAs(backing);
        assertThat(returned).containsExactly(backing);
        returned[0] = 0L;
        assertThat(v.words()).containsExactly(backing);
    }
}
