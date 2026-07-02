/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import org.junit.jupiter.api.Test;

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
        // A bitmap-backed validity exposes its backing array; the no-nulls
        // shape returns null from words().
        assertThat(v.words()).isNotNull();
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
    void backedWordsReturnsBackingArray() {
        long[] backing = wordsWith(5, 0, 1, 3, 4);
        Validity v = Validity.of(backing);
        assertThat(v.words()).isSameAs(backing);
    }

    // ==================== Multi-word coverage ====================
    //
    // The hand-rolled scan loops in nextNull / nextNotNull / nullCount
    // span words; these tests exercise the cross-word, fully-null-word,
    // and exact-64-boundary paths.

    @Test
    void backedNullCountAcrossMultipleWords() {
        // count = 128 → fullWords = 2, tail = 0. Mark indices 70 (word 1)
        // and 5 (word 0) as null; all others present.
        long[] words = new long[2];
        for (int i = 0; i < 128; i++) {
            if (i != 5 && i != 70) {
                words[i >>> 6] |= 1L << i;
            }
        }
        Validity v = Validity.of(words);
        assertThat(v.nullCount(128)).isEqualTo(2);
    }

    @Test
    void backedNullCountWithPartialTail() {
        // count = 130 → fullWords = 2, tail = 2. Null at 129 (tail) only.
        long[] words = new long[3];
        for (int i = 0; i < 130; i++) {
            if (i != 129) words[i >>> 6] |= 1L << i;
        }
        Validity v = Validity.of(words);
        assertThat(v.nullCount(130)).isEqualTo(1);
    }

    @Test
    void backedNextNullCrossesWordBoundary() {
        // Word 0 fully present, null at index 70 (word 1).
        long[] words = new long[3];
        words[0] = ~0L;
        for (int i = 64; i < 130; i++) {
            if (i != 70) words[i >>> 6] |= 1L << i;
        }
        Validity v = Validity.of(words);
        assertThat(v.nextNull(0, 130)).isEqualTo(70);
        assertThat(v.nextNull(64, 130)).isEqualTo(70);
        assertThat(v.nextNull(71, 130)).isEqualTo(-1);
        // Multi-word fixture covering the `from >= count` early-return.
        assertThat(v.nextNull(130, 130)).isEqualTo(-1);
        assertThat(v.nextNotNull(130, 130)).isEqualTo(-1);
    }

    @Test
    void backedNextNotNullSkipsFullyNullWord() {
        // Word 0 entirely null, first present bit at index 100 (word 1).
        long[] words = new long[3];
        words[1] = 1L << (100 - 64);
        Validity v = Validity.of(words);
        assertThat(v.nextNotNull(0, 130)).isEqualTo(100);
        assertThat(v.nextNotNull(50, 130)).isEqualTo(100);
        assertThat(v.nextNotNull(101, 130)).isEqualTo(-1);
    }

    @Test
    void backedScanAtExact64Boundary() {
        // count == 64 → tail == 0 branch in nullCount; endWord == 0 and
        // endMask == ~0L in nextNull / nextNotNull. Null at index 17 only.
        long[] words = new long[1];
        for (int i = 0; i < 64; i++) {
            if (i != 17) words[0] |= 1L << i;
        }
        Validity v = Validity.of(words);
        assertThat(v.nullCount(64)).isEqualTo(1);
        assertThat(v.nextNull(0, 64)).isEqualTo(17);
        assertThat(v.nextNotNull(17, 64)).isEqualTo(18);
    }

    @Test
    void ofNullsWithNoNullsIsNoNullsSingleton() {
        assertThat(Validity.ofNulls(new boolean[] { false, false, false })).isSameAs(Validity.NO_NULLS);
    }

    @Test
    void ofNullsEmptyMaskIsNoNullsSingleton() {
        assertThat(Validity.ofNulls(new boolean[0])).isSameAs(Validity.NO_NULLS);
    }

    @Test
    void ofNullsMixedMaskReportsNulls() {
        // Null at indices 1 and 3; present elsewhere. ofNulls stores set-bit = present.
        Validity v = Validity.ofNulls(new boolean[] { false, true, false, true, false });

        assertThat(v.hasNulls()).isTrue();
        assertThat(v.isNull(0)).isFalse();
        assertThat(v.isNull(1)).isTrue();
        assertThat(v.isNull(3)).isTrue();
        assertThat(v.isNotNull(4)).isTrue();
        assertThat(v.nullCount(5)).isEqualTo(2);
        assertThat(v.nextNull(0, 5)).isEqualTo(1);
        assertThat(v.nextNull(2, 5)).isEqualTo(3);
    }

    @Test
    void ofNullsAllNullReportsEveryItemNull() {
        Validity v = Validity.ofNulls(new boolean[] { true, true, true });

        assertThat(v.hasNulls()).isTrue();
        assertThat(v.nullCount(3)).isEqualTo(3);
        assertThat(v.nextNotNull(0, 3)).isEqualTo(-1);
    }

    @Test
    void ofNullsSpansWordBoundary() {
        boolean[] nulls = new boolean[130];
        nulls[65] = true;
        nulls[128] = true;
        Validity v = Validity.ofNulls(nulls);

        assertThat(v.nullCount(130)).isEqualTo(2);
        assertThat(v.nextNull(0, 130)).isEqualTo(65);
        assertThat(v.nextNull(66, 130)).isEqualTo(128);
    }
}
