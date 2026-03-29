/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import dev.hardwood.internal.thrift.ColumnIndexReader;
import dev.hardwood.internal.thrift.OffsetIndexReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnIndex;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.FilterPredicate.And;
import dev.hardwood.reader.FilterPredicate.DateColumnPredicate;
import dev.hardwood.reader.FilterPredicate.DecimalColumnPredicate;
import dev.hardwood.reader.FilterPredicate.InstantColumnPredicate;
import dev.hardwood.reader.FilterPredicate.Not;
import dev.hardwood.reader.FilterPredicate.Or;
import dev.hardwood.reader.FilterPredicate.TimeColumnPredicate;
import dev.hardwood.schema.FileSchema;

/// Evaluates a [FilterPredicate] against per-page statistics from the Column Index
/// to produce [RowRanges] representing rows that might match.
///
/// This is the page-level equivalent of [RowGroupFilterEvaluator]. While that class
/// decides whether an entire row group can be skipped, this class determines which
/// pages within a surviving row group can be skipped.
///
/// Leaf predicate evaluation is delegated to [StatisticsFilterSupport#canDropLeaf],
/// which handles all physical predicate types against a [MinMaxStats] abstraction.
public class PageFilterEvaluator {

    /// Computes the row ranges within a row group that might match the given predicate,
    /// based on per-page min/max statistics from the Column Index.
    ///
    /// Returns `RowRanges.all()` when the Column Index is absent or the predicate
    /// cannot be evaluated at the page level (conservative fallback).
    ///
    /// @param predicate    the filter predicate to evaluate
    /// @param rowGroup     the row group to evaluate against
    /// @param schema       the file schema for column resolution
    /// @param indexBuffers pre-fetched index buffers for the row group
    /// @return row ranges that might contain matching rows
    public static RowRanges computeMatchingRows(FilterPredicate predicate, RowGroup rowGroup,
            FileSchema schema, RowGroupIndexBuffers indexBuffers) {
        long rowCount = rowGroup.numRows();
        return evaluate(predicate, rowGroup, schema, indexBuffers, rowCount);
    }

    private static RowRanges evaluate(FilterPredicate predicate, RowGroup rowGroup,
            FileSchema schema, RowGroupIndexBuffers indexBuffers, long rowCount) {
        return switch (predicate) {
            case DateColumnPredicate p -> throw FilterPredicateResolver.unresolvedPredicate(p);
            case InstantColumnPredicate p -> throw FilterPredicateResolver.unresolvedPredicate(p);
            case TimeColumnPredicate p -> throw FilterPredicateResolver.unresolvedPredicate(p);
            case DecimalColumnPredicate p -> throw FilterPredicateResolver.unresolvedPredicate(p);
            case And a -> {
                RowRanges result = RowRanges.all(rowCount);
                for (FilterPredicate child : a.filters()) {
                    result = result.intersect(evaluate(child, rowGroup, schema, indexBuffers, rowCount));
                }
                yield result;
            }
            case Or o -> {
                RowRanges result = null;
                for (FilterPredicate child : o.filters()) {
                    RowRanges childRanges = evaluate(child, rowGroup, schema, indexBuffers, rowCount);
                    result = (result == null) ? childRanges : result.union(childRanges);
                }
                yield (result != null) ? result : RowRanges.all(rowCount);
            }
            case Not ignored -> RowRanges.all(rowCount);
            default -> evaluateLeafPages(predicate, rowGroup, schema, indexBuffers, rowCount);
        };
    }

    /// Evaluates a leaf predicate against per-page Column Index statistics,
    /// using [StatisticsFilterSupport#canDropLeaf] for the actual comparison.
    private static RowRanges evaluateLeafPages(FilterPredicate predicate, RowGroup rowGroup,
            FileSchema schema, RowGroupIndexBuffers indexBuffers, long rowCount) {

        String columnName = StatisticsFilterSupport.columnOf(predicate);
        PhysicalType expectedType = StatisticsFilterSupport.expectedPhysicalType(predicate);

        int columnIndex = RowGroupFilterEvaluator.resolveColumnIndex(columnName, rowGroup, schema);
        if (columnIndex < 0) {
            return RowRanges.all(rowCount);
        }
        PhysicalType actualType = rowGroup.columns().get(columnIndex).metaData().type();
        if (actualType != expectedType && !StatisticsFilterSupport.isBinaryCompatible(actualType, expectedType)) {
            throw new IllegalArgumentException(
                    "Column '" + columnName + "' has physical type " + actualType
                            + "; given filter predicate type " + expectedType + " is incompatible");
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
            throw new UncheckedIOException("Failed to parse Column/Offset Index for column '" + columnName + "'", e);
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
