/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import dev.hardwood.internal.predicate.PageFilterEvaluator;
import dev.hardwood.internal.predicate.PageFilterEvaluator.IndexPair;
import dev.hardwood.internal.reader.plan.NeededPage;
import dev.hardwood.internal.reader.plan.RowGroupIndexBuffers;
import dev.hardwood.internal.reader.select.PageRowMask;
import dev.hardwood.internal.reader.select.RowRanges;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.metadata.RowGroup;

/// The page index of one unit's column chunks, parsed from the bytes its window fetched, each
/// structure of each chunk at most once per plan: page filtering, which asks for a column once per
/// leaf testing it, and page planning, which asks for every decoded column's OffsetIndex, share
/// what it parsed. Holds the parses of one plan only.
final class UnitPageIndex implements PageFilterEvaluator.IndexLookup {

    /// The bytes `[offset, offset + length)` of the unit's window, `null` where it holds none.
    @FunctionalInterface
    interface Window {
        ByteBuffer read(long offset, int length);
    }

    private final RowGroup rowGroup;
    private final Window window;
    /// the file ordinals of the columns whose OffsetIndex and ColumnIndex the window holds
    private final BitSet offsetIndexes;
    private final BitSet columnIndexes;
    /// per file ordinal, allocated with the first parse of each kind
    private OffsetIndex[] parsedOffsetIndexes;
    private IndexPair[] pairs;

    UnitPageIndex(RowGroup rowGroup, Window window, BitSet offsetIndexes, BitSet columnIndexes) {
        this.rowGroup = rowGroup;
        this.window = window;
        this.offsetIndexes = offsetIndexes;
        this.columnIndexes = columnIndexes;
    }

    /// The chunk's ColumnIndex and OffsetIndex, checked to describe the same pages, for page
    /// filtering; `null` where the window holds no pair for it. A failure to parse says it was the
    /// page index.
    ///
    /// @throws IllegalStateException if the read did not ask for the column's indexes
    @Override
    public IndexPair pageIndex(int column) {
        if (!offsetIndexes.get(column) && !columnIndexes.get(column)) {
            throw new IllegalStateException("Page index of column " + column + " was not fetched for this read");
        }
        if (pairs != null && pairs[column] != null) {
            return pairs[column];
        }
        ColumnChunk chunk = rowGroup.columns().get(column);
        if (!columnIndexes.get(column) || chunk.columnIndexOffset() == null
                || !offsetIndexes.get(column) || chunk.offsetIndexOffset() == null) {
            return null;
        }
        IndexPair pair = new IndexPair(
                PageFilterEvaluator.parseColumnIndex(slice(chunk.columnIndexOffset(), chunk.columnIndexLength())),
                offsetIndex(column, true));
        PageFilterEvaluator.checkPageCounts(pair);
        if (pairs == null) {
            pairs = new IndexPair[rowGroup.columns().size()];
        }
        pairs[column] = pair;
        return pair;
    }

    /// The chunk's OffsetIndex, for planning its pages; the chunk records one and the window holds
    /// it. A failure to parse is left for the planner to place.
    OffsetIndex offsetIndex(int column) {
        return offsetIndex(column, false);
    }

    /// The pages of the chunk's OffsetIndex that hold a row of `matching`, each with its mask.
    List<NeededPage> neededPages(int column, RowRanges matching) {
        return select(offsetIndex(column).pageLocations(), matching, rowGroup.numRows());
    }

    private OffsetIndex offsetIndex(int column, boolean prefixed) {
        if (parsedOffsetIndexes == null) {
            parsedOffsetIndexes = new OffsetIndex[rowGroup.columns().size()];
        }
        if (parsedOffsetIndexes[column] == null) {
            ColumnChunk chunk = rowGroup.columns().get(column);
            parsedOffsetIndexes[column] = PageFilterEvaluator.parseOffsetIndex(
                    slice(chunk.offsetIndexOffset(), chunk.offsetIndexLength()), chunk, rowGroup.numRows(), prefixed);
        }
        return parsedOffsetIndexes[column];
    }

    private ByteBuffer slice(long offset, int length) {
        return RowGroupIndexBuffers.requirePresent(window.read(offset, length), offset, length);
    }

    /// The pages of `allPages` that hold a row of `matchingRows`, each with its mask.
    ///
    /// `allPages` must start at non-decreasing rows, as an OffsetIndex parsed with its chunk's
    /// checks lists them: under a filter only the pages from the one holding the first matching
    /// row up to the first starting at or past the last are visited, and pages out of order would
    /// be missed.
    static List<NeededPage> select(List<PageLocation> allPages, RowRanges matchingRows, long rowGroupRowCount) {
        if (matchingRows.isAll()) {
            List<NeededPage> needed = new ArrayList<>(allPages.size());
            for (int i = 0; i < allPages.size(); i++) {
                needed.add(new NeededPage(allPages.get(i), PageRowMask.ALL, i, pageRows(allPages, i, rowGroupRowCount)));
            }
            return needed;
        }
        List<NeededPage> needed = new ArrayList<>(8);
        long end = matchingRows.endRow();
        for (int i = pageHolding(allPages, matchingRows.startRow());
                i < allPages.size() && allPages.get(i).firstRowIndex() < end; i++) {
            long pageFirstRow = allPages.get(i).firstRowIndex();
            int rows = pageRows(allPages, i, rowGroupRowCount);
            PageRowMask mask = matchingRows.maskForPage(pageFirstRow, pageFirstRow + rows);
            if (mask != null) {
                needed.add(new NeededPage(allPages.get(i), mask, i, rows));
            }
        }
        return needed;
    }

    /// The last page of `pages` starting at or before `row`, `0` where none does.
    private static int pageHolding(List<PageLocation> pages, long row) {
        int low = 0;
        int high = pages.size() - 1;
        while (low < high) {
            int middle = (low + high + 1) >>> 1;
            if (pages.get(middle).firstRowIndex() <= row) {
                low = middle;
            }
            else {
                high = middle - 1;
            }
        }
        return low;
    }

    /// The rows of page `i` by the offset index: up to the next page's first row, or to the end
    /// of the row group for the last page.
    private static int pageRows(List<PageLocation> allPages, int i, long rowGroupRowCount) {
        long pageLastRow = i + 1 < allPages.size() ? allPages.get(i + 1).firstRowIndex() : rowGroupRowCount;
        return Math.toIntExact(pageLastRow - allPages.get(i).firstRowIndex());
    }
}
