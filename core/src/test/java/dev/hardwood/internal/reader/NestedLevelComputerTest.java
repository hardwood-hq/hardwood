/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.BitSet;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// Tests for [NestedLevelComputer].
///
/// The empty-list disambiguator added in #422 requires a single-pass
/// computation that separates four container states from one another using
/// just definition / repetition levels: null, empty, non-empty with null
/// element(s), and non-empty with value(s). These tests pin the encoding
/// boundaries so future refactors don't silently collapse them again.
class NestedLevelComputerTest {

    /// For schema `optional group my_list (LIST) { repeated group list { optional int32 element; } }`:
    /// - my_list.maxDef = 1 (optional)
    /// - list.maxDef = 2 (repeated counts as +1)
    /// - element.maxDef = 3 (optional leaf)
    /// - threshold[0] = list.maxDef - 1 = 1
    ///
    /// Four rows, one per container state:
    /// - row 0: my_list = null         (def=0)
    /// - row 1: my_list = []           (def=1, empty)
    /// - row 2: my_list = [null]       (def=2)
    /// - row 3: my_list = [42]         (def=3)
    @Test
    void distinguishesAllFourContainerStates() {
        int[] defLevels = { 0, 1, 2, 3 };
        int[] repLevels = { 0, 0, 0, 0 };
        int valueCount = 4;
        int maxRepLevel = 1;
        int[] thresholds = { 1 };

        NestedLevelComputer.LevelIndex index = NestedLevelComputer.computeLevelIndex(
                defLevels, repLevels, valueCount, maxRepLevel, thresholds);

        BitSet levelNulls = index.levelNulls()[0];
        BitSet emptyMarkers = index.emptyListMarkers()[0];

        // Only row 0 is null at the list level.
        assertThat(levelNulls).isNotNull();
        assertThat(levelNulls.get(0)).isTrue();
        assertThat(levelNulls.get(1)).isFalse();
        assertThat(levelNulls.get(2)).isFalse();
        assertThat(levelNulls.get(3)).isFalse();

        // Only row 1 is empty.
        assertThat(emptyMarkers).isNotNull();
        assertThat(emptyMarkers.get(0)).isFalse();
        assertThat(emptyMarkers.get(1)).isTrue();
        assertThat(emptyMarkers.get(2)).isFalse();
        assertThat(emptyMarkers.get(3)).isFalse();

        // Element-null bitmap must keep rows 2 (null element) and 1 / 0 (phantom)
        // separate from row 3 (real value); the disambiguation between rows 1
        // and 2 lives entirely in `emptyMarkers`.
        BitSet elementNulls = NestedLevelComputer.computeElementNulls(defLevels, valueCount, 3);
        assertThat(elementNulls).isNotNull();
        assertThat(elementNulls.get(0)).isTrue();
        assertThat(elementNulls.get(1)).isTrue();
        assertThat(elementNulls.get(2)).isTrue();
        assertThat(elementNulls.get(3)).isFalse();
    }

    /// When all containers are non-null and non-empty, both bitmaps stay null
    /// (sparse representation — no allocation when nothing is set).
    @Test
    void leavesBitmapsNullWhenNoSpecialStates() {
        int[] defLevels = { 3, 3, 3 };
        int[] repLevels = { 0, 0, 0 };
        int[] thresholds = { 1 };

        NestedLevelComputer.LevelIndex index = NestedLevelComputer.computeLevelIndex(
                defLevels, repLevels, 3, 1, thresholds);

        assertThat(index.levelNulls()[0]).isNull();
        assertThat(index.emptyListMarkers()[0]).isNull();
    }

    /// Nested-list case (maxRepLevel=2): the inner-list empty state is
    /// captured at level 1, independently of the outer-list state at level 0.
    /// Schema: `optional group outer (LIST) { repeated group list { optional group inner (LIST)
    /// { repeated group list { optional int32 element; } } } }`
    /// Thresholds: [1, 3] (outer-list at def 1, inner-list at def 3).
    @Test
    void emptyMarkersAreScopedPerLevel() {
        // Three rows:
        //   row 0: outer = []                           (def=1)
        //   row 1: outer = [ null ]                     (def=2; inner null)
        //   row 2: outer = [ [] ]                       (def=3; inner empty)
        int[] defLevels = { 1, 2, 3 };
        int[] repLevels = { 0, 0, 0 };
        int[] thresholds = { 1, 3 };

        NestedLevelComputer.LevelIndex index = NestedLevelComputer.computeLevelIndex(
                defLevels, repLevels, 3, 2, thresholds);

        // Level 0 (outer list)
        assertThat(index.levelNulls()[0]).isNull();        // none of the outer lists are null
        BitSet outerEmpty = index.emptyListMarkers()[0];
        assertThat(outerEmpty.get(0)).isTrue();             // row 0 outer is empty
        assertThat(outerEmpty.get(1)).isFalse();
        assertThat(outerEmpty.get(2)).isFalse();

        // Level 1 (inner list) — itemIdx counts every rep<=1 boundary, which
        // here is every value (3 items).
        BitSet innerNulls = index.levelNulls()[1];
        assertThat(innerNulls.get(0)).isTrue();             // row 0: outer empty, inner shadowed as null
        assertThat(innerNulls.get(1)).isTrue();             // row 1: inner explicitly null
        assertThat(innerNulls.get(2)).isFalse();            // row 2: inner is empty, not null

        BitSet innerEmpty = index.emptyListMarkers()[1];
        assertThat(innerEmpty.get(0)).isFalse();
        assertThat(innerEmpty.get(1)).isFalse();
        assertThat(innerEmpty.get(2)).isTrue();             // row 2 inner is empty
    }
}
