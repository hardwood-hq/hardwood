/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PluralsTest {

    @Test
    void formatPicksSingularForOneAndPluralOtherwise() {
        assertThat(Plurals.format(0, "row", "rows")).isEqualTo("0 rows");
        assertThat(Plurals.format(1, "row", "rows")).isEqualTo("1 row");
        assertThat(Plurals.format(2, "row", "rows")).isEqualTo("2 rows");
    }

    @Test
    void formatGroupsLargeNumbersWithComma() {
        assertThat(Plurals.format(12_400_000L, "row", "rows")).isEqualTo("12,400,000 rows");
    }

    @Test
    void rangeOfHandlesZeroTotal() {
        assertThat(Plurals.rangeOf(0, 0, 10)).isEqualTo("0");
    }

    @Test
    void rangeOfShowsSingleElementWhenTotalIsOne() {
        assertThat(Plurals.rangeOf(0, 1, 10)).isEqualTo("1 of 1");
    }

    @Test
    void rangeOfShowsFullRangeWhenTotalFitsViewport() {
        assertThat(Plurals.rangeOf(2, 5, 10)).isEqualTo("1-5 of 5");
    }

    @Test
    void rangeOfShowsFullRangeWhenTotalEqualsViewport() {
        assertThat(Plurals.rangeOf(0, 10, 10)).isEqualTo("1-10 of 10");
    }

    @Test
    void rangeOfPinsToTopWhenSelectionWithinFirstViewport() {
        // selection 0..viewport-1 → window stays 1..viewport
        assertThat(Plurals.rangeOf(0, 100, 10)).isEqualTo("1-10 of 100");
        assertThat(Plurals.rangeOf(9, 100, 10)).isEqualTo("1-10 of 100");
    }

    @Test
    void rangeOfBottomPinsWhenSelectionPastFirstViewport() {
        // sel=10 → end=11, start=2 → "2-11 of 100"
        assertThat(Plurals.rangeOf(10, 100, 10)).isEqualTo("2-11 of 100");
        assertThat(Plurals.rangeOf(99, 100, 10)).isEqualTo("91-100 of 100");
    }

    @Test
    void rangeOfClampsSelectionPastTotal() {
        assertThat(Plurals.rangeOf(500, 100, 10)).isEqualTo("91-100 of 100");
    }

    @Test
    void rangeOfTreatsZeroOrNegativeViewportAsOne() {
        // viewport=0 collapses to 1: a single-row sliding window
        assertThat(Plurals.rangeOf(5, 100, 0)).isEqualTo("6-6 of 100");
        assertThat(Plurals.rangeOf(5, 100, -3)).isEqualTo("6-6 of 100");
    }

    @Test
    void rangeOfFormatsLargeTotalsWithComma() {
        assertThat(Plurals.rangeOf(0, 12_400_000, 20)).isEqualTo("1-20 of 12,400,000");
    }
}
