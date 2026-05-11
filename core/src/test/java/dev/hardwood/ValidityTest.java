/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.util.BitSet;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.Validity;

import static org.assertj.core.api.Assertions.assertThat;

/// Unit tests for [Validity]'s predicate and index-helper methods. The
/// `ColumnReader`-level tests exercise the singleton/Backed dispatch through
/// the public API; these target the boundary conditions of `nullCount` /
/// `nextNull` / `nextNotNull` directly.
class ValidityTest {

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
    void backedHasNullsTrue() {
        BitSet bits = new BitSet();
        bits.set(0, 5);
        Validity v = Validity.of(bits);
        assertThat(v).isInstanceOf(Validity.Backed.class);
        assertThat(v.hasNulls()).isTrue();
    }

    /// Set bit = present. Item at index 2 is the only absent one.
    @Test
    void backedPredicates() {
        BitSet bits = new BitSet();
        bits.set(0);
        bits.set(1);
        bits.set(3);
        bits.set(4);
        Validity v = Validity.of(bits);

        assertThat(v.isNotNull(0)).isTrue();
        assertThat(v.isNotNull(1)).isTrue();
        assertThat(v.isNull(2)).isTrue();
        assertThat(v.isNotNull(3)).isTrue();
        assertThat(v.isNotNull(4)).isTrue();
    }

    @Test
    void backedNullCount() {
        BitSet bits = new BitSet();
        bits.set(0);
        bits.set(2);
        bits.set(4);   // 3 set bits = 3 present out of 5

        Validity v = Validity.of(bits);
        assertThat(v.nullCount(5)).isEqualTo(2);
    }

    @Test
    void backedNextNullFindsClearBitWithinRange() {
        BitSet bits = new BitSet();
        bits.set(0);
        bits.set(1);
        bits.set(3);   // bit 2 and 4+ are clear

        Validity v = Validity.of(bits);
        assertThat(v.nextNull(0, 5)).isEqualTo(2);
        assertThat(v.nextNull(3, 5)).isEqualTo(4);
        assertThat(v.nextNull(0, 2)).isEqualTo(-1);   // count == 2 excludes index 2
    }

    @Test
    void backedNextNullReturnsMinusOneWhenAllPresent() {
        BitSet bits = new BitSet();
        bits.set(0, 5);

        Validity v = Validity.of(bits);
        assertThat(v.nextNull(0, 5)).isEqualTo(-1);
        assertThat(v.nextNull(4, 5)).isEqualTo(-1);
    }

    @Test
    void backedNextNotNullFindsSetBit() {
        BitSet bits = new BitSet();
        bits.set(3);
        bits.set(7);

        Validity v = Validity.of(bits);
        assertThat(v.nextNotNull(0, 10)).isEqualTo(3);
        assertThat(v.nextNotNull(4, 10)).isEqualTo(7);
        assertThat(v.nextNotNull(8, 10)).isEqualTo(-1);
        assertThat(v.nextNotNull(0, 3)).isEqualTo(-1);   // count == 3 excludes index 3
    }

    @Test
    void backedNextNotNullExhausted() {
        BitSet bits = new BitSet();   // empty
        Validity v = Validity.of(bits);
        assertThat(v.nextNotNull(0, 5)).isEqualTo(-1);
    }

    @Test
    void backedFromAtCountReturnsMinusOne() {
        BitSet bits = new BitSet();
        bits.set(0, 5);
        Validity v = Validity.of(bits);

        assertThat(v.nextNull(5, 5)).isEqualTo(-1);
        assertThat(v.nextNotNull(5, 5)).isEqualTo(-1);
    }
}
