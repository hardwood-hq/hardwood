/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.reader.plan.NeededPage;
import dev.hardwood.internal.reader.select.PageRowMask;
import dev.hardwood.internal.reader.select.RowRanges;
import dev.hardwood.metadata.PageLocation;

import static org.assertj.core.api.Assertions.assertThat;

/// [UnitPageIndex#select] visits only the pages from the one holding the first matching row to the one
/// holding the last, and selects what a walk over every page selects.
class PageSelectionTest {

    /// four pages of ten rows, of 100 bytes each
    private static final List<PageLocation> PAGES = pages(0, 10, 20, 30);
    private static final long ROWS = 40;

    @Test
    void allRowsSelectEveryPageWhole() {
        assertThat(describe(UnitPageIndex.select(PAGES, RowRanges.ALL, ROWS))).containsExactly("0:ALL", "1:ALL", "2:ALL",
                "3:ALL");
    }

    @Test
    void noRowSelectsNoPage() {
        assertThat(UnitPageIndex.select(PAGES, RowRanges.NONE, ROWS)).isEmpty();
    }

    @Test
    void aRangeInsideOnePageSelectsItAlone() {
        assertThat(describe(UnitPageIndex.select(PAGES, RowRanges.range(13, 15), ROWS))).containsExactly("1:[3,5)");
    }

    @Test
    void aRangeOnPageBoundariesSelectsThosePagesWhole() {
        assertThat(describe(UnitPageIndex.select(PAGES, RowRanges.range(10, 30), ROWS))).containsExactly("1:ALL", "2:ALL");
    }

    @Test
    void theFirstAndTheLastRowSelectTheFirstAndTheLastPage() {
        assertThat(describe(UnitPageIndex.select(PAGES, RowRanges.range(0, 1), ROWS))).containsExactly("0:[0,1)");
        assertThat(describe(UnitPageIndex.select(PAGES, RowRanges.range(39, 40), ROWS))).containsExactly("3:[9,10)");
    }

    @Test
    void separateRangesSkipThePagesBetweenThem() {
        RowRanges ranges = RowRanges.range(5, 6).union(RowRanges.range(35, 36));
        assertThat(describe(UnitPageIndex.select(PAGES, ranges, ROWS))).containsExactly("0:[5,6)", "3:[5,6)");
    }

    @Test
    void aSinglePageChunk() {
        assertThat(describe(UnitPageIndex.select(pages(0), RowRanges.range(3, 4), 10))).containsExactly("0:[3,4)");
    }

    @Test
    void aPageStartingInsideARecordIsSelectedAsTheWalkSelectsIt() {
        // page 1 begins and ends inside the record at row 10, which page 2 also starts at
        List<PageLocation> pages = pages(0, 10, 10, 20);
        for (RowRanges ranges : List.of(RowRanges.range(10, 11), RowRanges.range(9, 11), RowRanges.range(15, 25))) {
            assertThat(describe(UnitPageIndex.select(pages, ranges, 30))).isEqualTo(walk(pages, ranges, 30));
        }
    }

    @Test
    void matchesAWalkOverEveryPage() {
        Random random = new Random(1);
        for (int trial = 0; trial < 2_000; trial++) {
            int pageCount = 1 + random.nextInt(12);
            long[] firstRows = new long[pageCount];
            for (int i = 1; i < pageCount; i++) {
                firstRows[i] = firstRows[i - 1] + random.nextInt(4);
            }
            long rows = firstRows[pageCount - 1] + 1 + random.nextInt(4);
            List<PageLocation> pages = pages(firstRows);
            RowRanges ranges = RowRanges.NONE;
            for (int r = random.nextInt(4); r > 0; r--) {
                long start = random.nextLong(rows);
                ranges = ranges.union(RowRanges.range(start, start + 1 + random.nextLong(rows - start)));
            }
            assertThat(describe(UnitPageIndex.select(pages, ranges, rows))).as("pages %s, ranges %s", pages, ranges)
                    .isEqualTo(walk(pages, ranges, rows));
        }
    }

    /// The pages a walk over every page selects: those whose rows overlap `ranges`.
    private static List<String> walk(List<PageLocation> pages, RowRanges ranges, long rows) {
        List<String> selected = new ArrayList<>();
        for (int i = 0; i < pages.size(); i++) {
            long first = pages.get(i).firstRowIndex();
            long end = i + 1 < pages.size() ? pages.get(i + 1).firstRowIndex() : rows;
            PageRowMask mask = ranges.maskForPage(first, end);
            if (mask != null) {
                selected.add(describe(i, mask));
            }
        }
        return selected;
    }

    private static List<String> describe(List<NeededPage> pages) {
        return pages.stream().map(page -> describe(page.pageIndex(), page.mask())).toList();
    }

    private static String describe(int page, PageRowMask mask) {
        if (mask.isAll()) {
            return page + ":ALL";
        }
        StringBuilder text = new StringBuilder().append(page).append(':');
        for (int i = 0; i < mask.intervalCount(); i++) {
            text.append('[').append(mask.start(i)).append(',').append(mask.end(i)).append(')');
        }
        return text.toString();
    }

    private static List<PageLocation> pages(long... firstRows) {
        List<PageLocation> pages = new ArrayList<>();
        for (int i = 0; i < firstRows.length; i++) {
            pages.add(new PageLocation(100L * i, 100, firstRows[i]));
        }
        return pages;
    }
}
