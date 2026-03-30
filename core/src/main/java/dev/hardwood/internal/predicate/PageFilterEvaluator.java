/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import dev.hardwood.internal.reader.ColumnIndexBuffers;
import dev.hardwood.internal.reader.RowGroupIndexBuffers;
import dev.hardwood.internal.reader.RowRanges;
import dev.hardwood.internal.thrift.ColumnIndexReader;
import dev.hardwood.internal.thrift.OffsetIndexReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnIndex;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.metadata.RowGroup;

/// Evaluates a [ResolvedPredicate] against per-page statistics from the Column Index
/// to produce [RowRanges] representing rows that might match.
///
/// This is the page-level equivalent of [RowGroupFilterEvaluator]. While that class
/// decides whether an entire row group can be skipped, this class determines which
/// pages within a surviving row group can be skipped.
///
/// Leaf predicate evaluation is delegated to [StatisticsFilterSupport#canDropLeaf],
/// which handles all resolved predicate types against a [MinMaxStats] abstraction.
public class PageFilterEvaluator {

    /// Computes the row ranges within a row group that might match the given predicate,
    /// based on per-page min/max statistics from the Column Index.
    ///
    /// Returns `RowRanges.all()` when the Column Index is absent or the predicate
    /// cannot be evaluated at the page level (conservative fallback).
    ///
    /// @param predicate    the resolved predicate to evaluate
    /// @param rowGroup     the row group to evaluate against
    /// @param indexBuffers pre-fetched index buffers for the row group
    /// @return row ranges that might contain matching rows
    public static RowRanges computeMatchingRows(ResolvedPredicate predicate, RowGroup rowGroup,
            RowGroupIndexBuffers indexBuffers) {
        long rowCount = rowGroup.numRows();
        return evaluate(predicate, rowGroup, indexBuffers, rowCount);
    }

    private static RowRanges evaluate(ResolvedPredicate predicate, RowGroup rowGroup,
            RowGroupIndexBuffers indexBuffers, long rowCount) {
        return switch (predicate) {
            case ResolvedPredicate.And a -> {
                RowRanges result = RowRanges.all(rowCount);
                for (ResolvedPredicate child : a.children()) {
                    result = result.intersect(evaluate(child, rowGroup, indexBuffers, rowCount));
                }
                yield result;
            }
            case ResolvedPredicate.Or o -> {
                RowRanges result = null;
                for (ResolvedPredicate child : o.children()) {
                    RowRanges childRanges = evaluate(child, rowGroup, indexBuffers, rowCount);
                    result = (result == null) ? childRanges : result.union(childRanges);
                }
                yield (result != null) ? result : RowRanges.all(rowCount);
            }
            case ResolvedPredicate.Not ignored -> RowRanges.all(rowCount);
            default -> evaluateLeafPages(predicate, rowGroup, indexBuffers, rowCount);
        };
    }

    /// Evaluates a leaf predicate against per-page Column Index statistics,
    /// using [StatisticsFilterSupport#canDropLeaf] for the actual comparison.
    private static RowRanges evaluateLeafPages(ResolvedPredicate predicate, RowGroup rowGroup,
            RowGroupIndexBuffers indexBuffers, long rowCount) {

        int columnIndex = leafColumnIndex(predicate);
        if (columnIndex < 0 || columnIndex >= rowGroup.columns().size()) {
            return RowRanges.all(rowCount);
        }

        ColumnIndexBuffers colBuffers = indexBuffers.forColumn(columnIndex);
        if (colBuffers == null || colBuffers.columnIndex() == null || colBuffers.offsetIndex() == null) {
            return RowRanges.all(rowCount);
        }

        ColumnIndex columnIdx;
        OffsetIndex offsetIdx;
        try {
            columnIdx = ColumnIndexReader.read(new ThriftCompactReader(colBuffers.columnIndex()));
            offsetIdx = OffsetIndexReader.read(new ThriftCompactReader(colBuffers.offsetIndex()));
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to parse Column/Offset Index for column index " + columnIndex, e);
        }

        List<PageLocation> pages = offsetIdx.pageLocations();
        int pageCount = pages.size();
        boolean[] keep = new boolean[pageCount];

        for (int i = 0; i < pageCount; i++) {
            if (columnIdx.nullPages().get(i)) {
                continue;
            }
            keep[i] = !StatisticsFilterSupport.canDropLeaf(predicate, MinMaxStats.ofPage(columnIdx, i));
        }

        return RowRanges.fromPages(pages, keep, rowCount);
    }

    /// Extracts the column index from a leaf predicate.
    private static int leafColumnIndex(ResolvedPredicate predicate) {
        return switch (predicate) {
            case ResolvedPredicate.IntPredicate p -> p.columnIndex();
            case ResolvedPredicate.LongPredicate p -> p.columnIndex();
            case ResolvedPredicate.FloatPredicate p -> p.columnIndex();
            case ResolvedPredicate.DoublePredicate p -> p.columnIndex();
            case ResolvedPredicate.BooleanPredicate p -> p.columnIndex();
            case ResolvedPredicate.BinaryPredicate p -> p.columnIndex();
            case ResolvedPredicate.IntInPredicate p -> p.columnIndex();
            case ResolvedPredicate.LongInPredicate p -> p.columnIndex();
            case ResolvedPredicate.BinaryInPredicate p -> p.columnIndex();
            case ResolvedPredicate.And ignored -> -1;
            case ResolvedPredicate.Or ignored -> -1;
            case ResolvedPredicate.Not ignored -> -1;
        };
    }

    /// Evaluates a keep bitmap for pages using pre-parsed Column Index and Offset Index.
    /// Used by [#evaluateLeafPages] and directly by tests.
    static RowRanges evaluatePages(ColumnIndex columnIdx, OffsetIndex offsetIdx,
            long rowCount, PageCanDropTest canDropTest) {
        List<PageLocation> pages = offsetIdx.pageLocations();
        int pageCount = pages.size();
        boolean[] keep = new boolean[pageCount];

        for (int i = 0; i < pageCount; i++) {
            if (columnIdx.nullPages().get(i)) {
                continue;
            }
            keep[i] = !canDropTest.canDrop(columnIdx, i);
        }

        return RowRanges.fromPages(pages, keep, rowCount);
    }

    /// Functional interface for testing whether a page can be dropped based on its
    /// Column Index min/max values.
    @FunctionalInterface
    interface PageCanDropTest {
        boolean canDrop(ColumnIndex columnIndex, int pageIndex);
    }
}
