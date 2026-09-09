/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Slicing the page-major level histograms of a [ColumnIndex].
class ColumnIndexTest {

    @Test
    void slicesEachPageOutOfADefinitionLevelHistogram() {
        // 3 pages of a maxDef 1 column: 2 entries per page.
        ColumnIndex index = columnIndex(3, null, new long[]{ 0, 4, 3, 0, 0, 4 }, null);

        assertThat(index.definitionLevelHistogram(0)).containsExactly(0L, 4L);
        assertThat(index.definitionLevelHistogram(1)).containsExactly(3L, 0L);
        assertThat(index.definitionLevelHistogram(2)).containsExactly(0L, 4L);
    }

    @Test
    void derivesAOneEntryStrideForANonRepeatedColumn() {
        // maxRep 0, so each page contributes a single entry and the slice is that entry.
        ColumnIndex index = columnIndex(3, new long[]{ 4, 3, 4 }, null, null);

        assertThat(index.repetitionLevelHistogram(0)).containsExactly(4L);
        assertThat(index.repetitionLevelHistogram(2)).containsExactly(4L);
    }

    @Test
    void reportsAnAbsentHistogramAsNull() {
        ColumnIndex index = columnIndex(2, null, null, null);

        assertThat(index.repetitionLevelHistogram(0)).isNull();
        assertThat(index.definitionLevelHistogram(1)).isNull();
    }

    @Test
    void rejectsALengthThatIsNotAWholeNumberOfPages() {
        // 5 entries across 2 pages: no per-page stride describes it, so the file is
        // malformed rather than the caller being wrong.
        ColumnIndex index = columnIndex(2, null, new long[]{ 1, 2, 3, 4, 5 }, null);

        assertThatThrownBy(() -> index.definitionLevelHistogram(0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Malformed Parquet metadata: definition-level histogram holds 5 entries for 2 "
                         + "pages, which is not a whole number of entries per page");
    }

    @Test
    void rejectsAPageIndexOutsideTheIndex() {
        ColumnIndex index = columnIndex(2, null, new long[]{ 1, 2, 3, 4 }, null);

        assertThatThrownBy(() -> index.definitionLevelHistogram(2))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> index.definitionLevelHistogram(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void returnsACopySoTheRecordIsNotWritableThroughASlice() {
        // The whole-chunk accessor hands out the array the file was read into; the slice
        // must not be a second door onto it.
        ColumnIndex index = columnIndex(2, null, new long[]{ 1, 2, 3, 4 }, null);

        long[] slice = index.definitionLevelHistogram(0);
        slice[0] = 99;

        assertThat(index.definitionLevelHistograms()).containsExactly(1L, 2L, 3L, 4L);
        assertThat(index.definitionLevelHistogram(0)).containsExactly(1L, 2L);
    }

    private static ColumnIndex columnIndex(int pageCount, long[] repetitionLevelHistograms,
                                           long[] definitionLevelHistograms, long[] nanCounts) {
        boolean[] nullPages = new boolean[pageCount];
        List<byte[]> bounds = new ArrayList<>(pageCount);
        for (int i = 0; i < pageCount; i++) {
            bounds.add(new byte[]{ 0 });
        }
        return new ColumnIndex(nullPages, bounds, bounds, ColumnIndex.BoundaryOrder.UNORDERED,
                null, repetitionLevelHistograms, definitionLevelHistograms, nanCounts);
    }
}
