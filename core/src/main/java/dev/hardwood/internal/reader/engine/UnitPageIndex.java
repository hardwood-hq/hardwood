/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;

import dev.hardwood.internal.predicate.PageFilterEvaluator;
import dev.hardwood.internal.predicate.PageFilterEvaluator.IndexPair;
import dev.hardwood.internal.reader.plan.NeededPage;
import dev.hardwood.internal.reader.plan.RowGroupIndexBuffers;
import dev.hardwood.internal.reader.select.RowRanges;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.OffsetIndex;
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
        return NeededPage.select(offsetIndex(column).pageLocations(), matching, rowGroup.numRows());
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
}
