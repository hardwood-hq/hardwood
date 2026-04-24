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

import dev.hardwood.metadata.PageLocation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RowRangesTest {

    @Test
    void testAllMatchesEverything() {
        RowRanges ranges = RowRanges.all(1000);
        assertTrue(ranges.isAll());
        assertEquals(1, ranges.intervalCount());
        assertTrue(ranges.overlapsPage(0, 500));
        assertTrue(ranges.overlapsPage(999, 1000));
    }

    @Test
    void testFromPagesKeepAll() {
        List<PageLocation> pages = List.of(
                new PageLocation(0, 100, 0),
                new PageLocation(100, 100, 50),
                new PageLocation(200, 100, 100));

        RowRanges ranges = RowRanges.fromPages(pages, new boolean[]{ true, true, true }, 150);

        assertEquals(1, ranges.intervalCount());
        assertTrue(ranges.overlapsPage(0, 150));
    }

    @Test
    void testFromPagesKeepNone() {
        List<PageLocation> pages = List.of(
                new PageLocation(0, 100, 0),
                new PageLocation(100, 100, 50),
                new PageLocation(200, 100, 100));

        RowRanges ranges = RowRanges.fromPages(pages, new boolean[]{ false, false, false }, 150);

        assertEquals(0, ranges.intervalCount());
        assertFalse(ranges.overlapsPage(0, 150));
    }

    @Test
    void testFromPagesKeepFirst() {
        // 3 pages: rows [0,50), [50,100), [100,150)
        List<PageLocation> pages = List.of(
                new PageLocation(0, 100, 0),
                new PageLocation(100, 100, 50),
                new PageLocation(200, 100, 100));

        RowRanges ranges = RowRanges.fromPages(pages, new boolean[]{ true, false, false }, 150);

        assertEquals(1, ranges.intervalCount());
        assertTrue(ranges.overlapsPage(0, 50));
        assertFalse(ranges.overlapsPage(50, 100));
        assertFalse(ranges.overlapsPage(100, 150));
    }

    @Test
    void testFromPagesKeepLast() {
        List<PageLocation> pages = List.of(
                new PageLocation(0, 100, 0),
                new PageLocation(100, 100, 50),
                new PageLocation(200, 100, 100));

        RowRanges ranges = RowRanges.fromPages(pages, new boolean[]{ false, false, true }, 150);

        assertEquals(1, ranges.intervalCount());
        assertFalse(ranges.overlapsPage(0, 50));
        assertFalse(ranges.overlapsPage(50, 100));
        assertTrue(ranges.overlapsPage(100, 150));
    }

    @Test
    void testFromPagesKeepFirstAndLast() {
        // Keep pages 0 and 2, skip page 1 → two disjoint intervals
        List<PageLocation> pages = List.of(
                new PageLocation(0, 100, 0),
                new PageLocation(100, 100, 50),
                new PageLocation(200, 100, 100));

        RowRanges ranges = RowRanges.fromPages(pages, new boolean[]{ true, false, true }, 150);

        assertEquals(2, ranges.intervalCount());
        assertTrue(ranges.overlapsPage(0, 50));
        assertFalse(ranges.overlapsPage(50, 100));
        assertTrue(ranges.overlapsPage(100, 150));
    }

    @Test
    void testFromPagesMergesAdjacentKeptPages() {
        // Keep pages 0 and 1 → should merge into single interval [0, 100)
        List<PageLocation> pages = List.of(
                new PageLocation(0, 100, 0),
                new PageLocation(100, 100, 50),
                new PageLocation(200, 100, 100));

        RowRanges ranges = RowRanges.fromPages(pages, new boolean[]{ true, true, false }, 150);

        assertEquals(1, ranges.intervalCount());
        assertTrue(ranges.overlapsPage(0, 100));
        assertFalse(ranges.overlapsPage(100, 150));
    }

    @Test
    void testOverlapsPagePartialOverlap() {
        // Range [10, 30)
        List<PageLocation> pages = List.of(
                new PageLocation(0, 100, 0),
                new PageLocation(100, 100, 10),
                new PageLocation(200, 100, 30));

        RowRanges ranges = RowRanges.fromPages(pages, new boolean[]{ false, true, false }, 50);

        // Page [5, 15) partially overlaps [10, 30)
        assertTrue(ranges.overlapsPage(5, 15));
        // Page [25, 35) partially overlaps [10, 30)
        assertTrue(ranges.overlapsPage(25, 35));
        // Page [30, 40) does not overlap [10, 30) — boundary is exclusive
        assertFalse(ranges.overlapsPage(30, 40));
        // Page [0, 10) does not overlap [10, 30) — boundary is exclusive
        assertFalse(ranges.overlapsPage(0, 10));
    }

    @Test
    void testIntersectWithAll() {
        List<PageLocation> pages = List.of(
                new PageLocation(0, 100, 0),
                new PageLocation(100, 100, 50));

        RowRanges a = RowRanges.fromPages(pages, new boolean[]{ true, false }, 100);
        RowRanges all = RowRanges.all(100);

        // intersect with all should return the original
        RowRanges result = a.intersect(all);
        assertTrue(result.overlapsPage(0, 50));
        assertFalse(result.overlapsPage(50, 100));

        // symmetry
        result = all.intersect(a);
        assertTrue(result.overlapsPage(0, 50));
        assertFalse(result.overlapsPage(50, 100));
    }

    @Test
    void testIntersectOverlappingRanges() {
        // a: [0, 60)
        // b: [40, 100)
        // intersection: [40, 60)
        List<PageLocation> pagesA = List.of(
                new PageLocation(0, 100, 0),
                new PageLocation(100, 100, 60));
        RowRanges a = RowRanges.fromPages(pagesA, new boolean[]{ true, false }, 100);

        List<PageLocation> pagesB = List.of(
                new PageLocation(0, 100, 0),
                new PageLocation(100, 100, 40));
        RowRanges b = RowRanges.fromPages(pagesB, new boolean[]{ false, true }, 100);

        RowRanges result = a.intersect(b);
        assertEquals(1, result.intervalCount());
        assertFalse(result.overlapsPage(0, 40));
        assertTrue(result.overlapsPage(40, 60));
        assertFalse(result.overlapsPage(60, 100));
    }

    @Test
    void testIntersectDisjointRanges() {
        // a: [0, 30)
        // b: [50, 100)
        // intersection: empty
        List<PageLocation> pagesA = List.of(
                new PageLocation(0, 100, 0),
                new PageLocation(100, 100, 30));
        RowRanges a = RowRanges.fromPages(pagesA, new boolean[]{ true, false }, 100);

        List<PageLocation> pagesB = List.of(
                new PageLocation(0, 100, 0),
                new PageLocation(100, 100, 50));
        RowRanges b = RowRanges.fromPages(pagesB, new boolean[]{ false, true }, 100);

        RowRanges result = a.intersect(b);
        assertEquals(0, result.intervalCount());
    }

    @Test
    void testIntersectSingleRangeAgainstMultipleRanges() {
        // a: [0, 60)
        List<PageLocation> pagesA = List.of(
                new PageLocation(0, 100, 0),
                new PageLocation(100, 100, 60));
        RowRanges a = RowRanges.fromPages(pagesA, new boolean[]{ true, false }, 60);

        // b: [0, 10), [20, 30), [40, 50)
        List<PageLocation> pagesB = List.of(
                new PageLocation(0, 100, 0),
                new PageLocation(100, 100, 10),
                new PageLocation(200, 100, 20),
                new PageLocation(300, 100, 30),
                new PageLocation(400, 100, 40),
                new PageLocation(500, 100, 50));
        RowRanges b = RowRanges.fromPages(pagesB, new boolean[]{ true, false, true, false, true, false }, 60);

        RowRanges result = a.intersect(b);
        assertEquals(3, result.intervalCount());
        assertTrue(result.overlapsPage(0, 10));
        assertFalse(result.overlapsPage(10, 20));
        assertTrue(result.overlapsPage(20, 30));
        assertFalse(result.overlapsPage(30, 40));
        assertTrue(result.overlapsPage(40, 50));
        assertFalse(result.overlapsPage(50, 60));
    }

    @Test
    void testUnionWithAll() {
        List<PageLocation> pages = List.of(
                new PageLocation(0, 100, 0),
                new PageLocation(100, 100, 50));
        RowRanges a = RowRanges.fromPages(pages, new boolean[]{ true, false }, 100);
        RowRanges all = RowRanges.all(100);

        RowRanges result = a.union(all);
        assertTrue(result.isAll());

        result = all.union(a);
        assertTrue(result.isAll());
    }

    @Test
    void testUnionDisjointRanges() {
        // a: [0, 30), b: [60, 100)
        List<PageLocation> pagesA = List.of(
                new PageLocation(0, 100, 0),
                new PageLocation(100, 100, 30));
        RowRanges a = RowRanges.fromPages(pagesA, new boolean[]{ true, false }, 100);

        List<PageLocation> pagesB = List.of(
                new PageLocation(0, 100, 0),
                new PageLocation(100, 100, 30),
                new PageLocation(200, 100, 60));
        RowRanges b = RowRanges.fromPages(pagesB, new boolean[]{ false, false, true }, 100);

        RowRanges result = a.union(b);
        assertEquals(2, result.intervalCount());
        assertTrue(result.overlapsPage(0, 30));
        assertFalse(result.overlapsPage(30, 60));
        assertTrue(result.overlapsPage(60, 100));
    }

    @Test
    void testUnionOverlappingRangesMerges() {
        // a: [0, 60), b: [40, 100) → union: [0, 100)
        List<PageLocation> pagesA = List.of(
                new PageLocation(0, 100, 0),
                new PageLocation(100, 100, 60));
        RowRanges a = RowRanges.fromPages(pagesA, new boolean[]{ true, false }, 100);

        List<PageLocation> pagesB = List.of(
                new PageLocation(0, 100, 0),
                new PageLocation(100, 100, 40));
        RowRanges b = RowRanges.fromPages(pagesB, new boolean[]{ false, true }, 100);

        RowRanges result = a.union(b);
        assertEquals(1, result.intervalCount());
        assertTrue(result.overlapsPage(0, 100));
    }

    @Test
    void testUnionAdjacentRangesMerges() {
        // a: [0, 50), b: [50, 100) → union: [0, 100)
        List<PageLocation> pages = List.of(
                new PageLocation(0, 100, 0),
                new PageLocation(100, 100, 50));

        RowRanges a = RowRanges.fromPages(pages, new boolean[]{ true, false }, 100);
        RowRanges b = RowRanges.fromPages(pages, new boolean[]{ false, true }, 100);

        RowRanges result = a.union(b);
        assertEquals(1, result.intervalCount());
        assertTrue(result.overlapsPage(0, 100));
    }

    @Test
    void testIntersectWithEmpty() {
        List<PageLocation> pages = List.of(
                new PageLocation(0, 100, 0),
                new PageLocation(100, 100, 50));
        RowRanges a = RowRanges.fromPages(pages, new boolean[]{ true, true }, 100);
        RowRanges empty = RowRanges.fromPages(pages, new boolean[]{ false, false }, 100);

        assertEquals(0, a.intersect(empty).intervalCount());
        assertEquals(0, empty.intersect(a).intervalCount());
    }

    @Test
    void testUnionWithEmpty() {
        List<PageLocation> pages = List.of(
                new PageLocation(0, 100, 0),
                new PageLocation(100, 100, 50));
        RowRanges a = RowRanges.fromPages(pages, new boolean[]{ true, false }, 100);
        RowRanges empty = RowRanges.fromPages(pages, new boolean[]{ false, false }, 100);

        RowRanges result = a.union(empty);
        assertEquals(1, result.intervalCount());
        assertTrue(result.overlapsPage(0, 50));
        assertFalse(result.overlapsPage(50, 100));

        // Symmetry
        result = empty.union(a);
        assertEquals(1, result.intervalCount());
        assertTrue(result.overlapsPage(0, 50));
    }

    @Test
    void testMaskForPageOnAllReturnsAllSentinel() {
        assertSame(PageRowMask.ALL, RowRanges.ALL.maskForPage(0, 100));
        assertSame(PageRowMask.ALL, RowRanges.ALL.maskForPage(50, 60));
    }

    @Test
    void testMaskForPageReturnsNullWhenPageDisjointFromAllRanges() {
        // Range [100, 200), page entirely before
        RowRanges ranges = singleRange(100, 200);
        assertNull(ranges.maskForPage(0, 100));
        assertNull(ranges.maskForPage(50, 100));
        // Page entirely after
        assertNull(ranges.maskForPage(200, 300));
        assertNull(ranges.maskForPage(250, 350));
    }

    @Test
    void testMaskForPageWholePageInsideSingleRange() {
        // Range [100, 200), page [120, 180) entirely inside → mask covers full page
        RowRanges ranges = singleRange(100, 200);
        PageRowMask mask = ranges.maskForPage(120, 180);
        assertEquals(1, mask.intervalCount());
        assertEquals(0, mask.start(0));
        assertEquals(60, mask.end(0));
        assertEquals(60, mask.totalRecords());
    }

    @Test
    void testMaskForPageRangeStartsInsidePage() {
        // Range [120, 200), page [100, 200) → mask is [20, 100)
        RowRanges ranges = singleRange(120, 200);
        PageRowMask mask = ranges.maskForPage(100, 200);
        assertEquals(1, mask.intervalCount());
        assertEquals(20, mask.start(0));
        assertEquals(100, mask.end(0));
    }

    @Test
    void testMaskForPageRangeEndsInsidePage() {
        // Range [50, 120), page [100, 200) → mask is [0, 20)
        RowRanges ranges = singleRange(50, 120);
        PageRowMask mask = ranges.maskForPage(100, 200);
        assertEquals(1, mask.intervalCount());
        assertEquals(0, mask.start(0));
        assertEquals(20, mask.end(0));
    }

    @Test
    void testMaskForPageRangeStraddlesBothEnds() {
        // Range [50, 250), page [100, 200) → mask is [0, 100)
        RowRanges ranges = singleRange(50, 250);
        PageRowMask mask = ranges.maskForPage(100, 200);
        assertEquals(1, mask.intervalCount());
        assertEquals(0, mask.start(0));
        assertEquals(100, mask.end(0));
    }

    @Test
    void testMaskForPageMultipleRangesProducesMultipleIntervals() {
        // Ranges [110, 130) ∪ [160, 190), page [100, 200) → mask [10, 30) ∪ [60, 90)
        RowRanges ranges = singleRange(110, 130).union(singleRange(160, 190));
        PageRowMask mask = ranges.maskForPage(100, 200);
        assertEquals(2, mask.intervalCount());
        assertEquals(10, mask.start(0));
        assertEquals(30, mask.end(0));
        assertEquals(60, mask.start(1));
        assertEquals(90, mask.end(1));
        assertEquals(50, mask.totalRecords());
    }

    @Test
    void testMaskForPageDropsRangesEntirelyOutsidePage() {
        // Ranges [10, 30) ∪ [120, 150) ∪ [400, 500), page [100, 200) → mask [20, 50)
        RowRanges ranges = singleRange(10, 30)
                .union(singleRange(120, 150))
                .union(singleRange(400, 500));
        PageRowMask mask = ranges.maskForPage(100, 200);
        assertEquals(1, mask.intervalCount());
        assertEquals(20, mask.start(0));
        assertEquals(50, mask.end(0));
    }

    @Test
    void testMaskForPageBoundariesAreExclusive() {
        // Range [100, 200), page [0, 100) — touches at 100 but no overlap
        RowRanges ranges = singleRange(100, 200);
        assertNull(ranges.maskForPage(0, 100));
        // Page [200, 300) — touches at 200 but no overlap
        assertNull(ranges.maskForPage(200, 300));
    }

    /// Builds a `RowRanges` containing the single interval `[start, end)`.
    /// Helper for the maskForPage tests so each one can read at a glance.
    private static RowRanges singleRange(long start, long end) {
        // 2-page setup: drop page 0, keep page 1 covering [start, end).
        List<PageLocation> pages = List.of(
                new PageLocation(0, 100, 0),
                new PageLocation(100, 100, start));
        return RowRanges.fromPages(pages, new boolean[]{ false, true }, end);
    }
}
