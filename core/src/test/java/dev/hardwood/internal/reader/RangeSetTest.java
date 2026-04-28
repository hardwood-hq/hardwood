/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RangeSetTest {

    @Test
    void emptySetContainsNothing() {
        RangeSet s = new RangeSet();
        assertThat(s.contains(0, 100)).isFalse();
        assertThat(s.missing(0, 100)).containsExactly(new long[]{ 0, 100 });
    }

    @Test
    void singleAddIsContained() {
        RangeSet s = new RangeSet();
        s.add(10, 50);
        assertThat(s.contains(10, 50)).isTrue();
        assertThat(s.contains(20, 40)).isTrue();
        assertThat(s.contains(0, 50)).isFalse();
        assertThat(s.contains(10, 60)).isFalse();
    }

    @Test
    void overlappingAddsMerge() {
        RangeSet s = new RangeSet();
        s.add(10, 30);
        s.add(20, 40);
        assertThat(s.size()).isEqualTo(1);
        assertThat(s.contains(10, 40)).isTrue();
    }

    @Test
    void touchingAddsMerge() {
        RangeSet s = new RangeSet();
        s.add(10, 20);
        s.add(20, 30);
        assertThat(s.size()).isEqualTo(1);
        assertThat(s.contains(10, 30)).isTrue();
    }

    @Test
    void disjointAddsDoNotMerge() {
        RangeSet s = new RangeSet();
        s.add(10, 20);
        s.add(30, 40);
        assertThat(s.size()).isEqualTo(2);
        assertThat(s.contains(10, 40)).isFalse();
    }

    @Test
    void addAbsorbsMultipleNeighbours() {
        RangeSet s = new RangeSet();
        s.add(10, 20);
        s.add(30, 40);
        s.add(50, 60);
        s.add(15, 55);
        assertThat(s.size()).isEqualTo(1);
        assertThat(s.contains(10, 60)).isTrue();
    }

    @Test
    void missingReportsGapsBetweenStoredRanges() {
        RangeSet s = new RangeSet();
        s.add(20, 40);
        s.add(60, 80);
        List<long[]> gaps = s.missing(0, 100);
        assertThat(gaps).hasSize(3);
        assertThat(gaps.get(0)).containsExactly(0, 20);
        assertThat(gaps.get(1)).containsExactly(40, 60);
        assertThat(gaps.get(2)).containsExactly(80, 100);
    }

    @Test
    void missingReportsNothingWhenFullyCovered() {
        RangeSet s = new RangeSet();
        s.add(0, 100);
        assertThat(s.missing(20, 80)).isEmpty();
    }

    @Test
    void missingClampsToRequestedBounds() {
        RangeSet s = new RangeSet();
        s.add(0, 50);
        s.add(80, 200);
        // Requested [40, 100): only [50, 80) is missing.
        List<long[]> gaps = s.missing(40, 100);
        assertThat(gaps).hasSize(1);
        assertThat(gaps.get(0)).containsExactly(50, 80);
    }

    @Test
    void emptyRangesAreNoOps() {
        RangeSet s = new RangeSet();
        s.add(50, 50);
        assertThat(s.size()).isZero();
        assertThat(s.contains(50, 50)).isTrue();
        assertThat(s.missing(50, 50)).isEmpty();
    }
}
