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

import dev.hardwood.internal.ExceptionContext;
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
///
/// When the `ColumnIndex` is absent, this evaluator returns `RowRanges.all()` — page
/// skipping then happens per-column inside [dev.hardwood.internal.reader.SequentialFetchPlan]
/// based on inline `DataPageHeader.statistics`.
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
    /// @param location     file and row group being filtered, for error attribution
    /// @return row ranges that might contain matching rows
    public static RowRanges computeMatchingRows(ResolvedPredicate predicate, RowGroup rowGroup,
            RowGroupIndexBuffers indexBuffers, IndexLocation location) {
        long rowCount = rowGroup.numRows();
        return evaluate(predicate, rowGroup, indexBuffers, rowCount, location);
    }

    private static RowRanges evaluate(ResolvedPredicate predicate, RowGroup rowGroup,
            RowGroupIndexBuffers indexBuffers, long rowCount, IndexLocation location) {
        return switch (predicate) {
            case ResolvedPredicate.And a -> {
                RowRanges result = RowRanges.all(rowCount);
                for (ResolvedPredicate child : a.children()) {
                    result = result.intersect(evaluate(child, rowGroup, indexBuffers, rowCount, location));
                }
                yield result;
            }
            case ResolvedPredicate.Or o -> {
                RowRanges result = null;
                for (ResolvedPredicate child : o.children()) {
                    RowRanges childRanges = evaluate(child, rowGroup, indexBuffers, rowCount, location);
                    result = (result == null) ? childRanges : result.union(childRanges);
                }
                yield (result != null) ? result : RowRanges.all(rowCount);
            }
            case ResolvedPredicate.IsNullPredicate p -> evaluateNullPages(p.columnIndex(), true, rowGroup, indexBuffers, rowCount, location);
            case ResolvedPredicate.IsNotNullPredicate p -> evaluateNullPages(p.columnIndex(), false, rowGroup, indexBuffers, rowCount, location);
            // Parquet has no per-page geospatial statistics (GeospatialStatistics lives only on
            // ColumnMetaData, applied during row-group filtering), so no page-level pruning is possible.
            case ResolvedPredicate.GeospatialPredicate ignored -> RowRanges.all(rowCount);
            default -> evaluateLeafPages(predicate, rowGroup, indexBuffers, rowCount, location);
        };
    }

    /// Evaluates a leaf predicate against per-page Column Index statistics,
    /// using [StatisticsFilterSupport#canDropLeaf] for the actual comparison.
    private static RowRanges evaluateLeafPages(ResolvedPredicate predicate, RowGroup rowGroup,
            RowGroupIndexBuffers indexBuffers, long rowCount, IndexLocation location) {

        int columnIndex = leafColumnIndex(predicate);
        if (columnIndex < 0 || columnIndex >= rowGroup.columns().size()) {
            return RowRanges.all(rowCount);
        }

        ColumnIndexBuffers colBuffers = indexBuffers.forColumn(columnIndex);
        if (colBuffers == null || colBuffers.columnIndex() == null || colBuffers.offsetIndex() == null) {
            return RowRanges.all(rowCount);
        }

        IndexPair indexPair = readIndexPair(colBuffers, location, columnIndex);
        ColumnIndex columnIdx = indexPair.columnIndex;
        OffsetIndex offsetIdx = indexPair.offsetIndex;

        List<PageLocation> pages = offsetIdx.pageLocations();
        int pageCount = pages.size();
        boolean[] keep = new boolean[pageCount];

        for (int i = 0; i < pageCount; i++) {
            if (columnIdx.nullPages()[i]) {
                continue;
            }
            keep[i] = !StatisticsFilterSupport.canDropLeaf(predicate, MinMaxStats.ofPage(columnIdx, i));
        }

        return RowRanges.fromPages(pages, keep, rowCount);
    }

    /// Evaluates IS NULL / IS NOT NULL predicates against per-page null information
    /// from the Column Index to produce [RowRanges] representing rows that might match.
    ///
    /// @param columnIndex  the column to check
    /// @param seekingNulls `true` for IS NULL (keep pages that might contain nulls),
    ///                     `false` for IS NOT NULL (keep pages that might contain non-nulls)
    /// @param rowGroup     the row group being evaluated
    /// @param indexBuffers pre-fetched index buffers
    /// @param rowCount     total rows in the row group
    /// @param location     file and row group being filtered, for error attribution
    /// @return row ranges that might contain matching rows
    private static RowRanges evaluateNullPages(int columnIndex, boolean seekingNulls,
            RowGroup rowGroup, RowGroupIndexBuffers indexBuffers, long rowCount, IndexLocation location) {

        if (columnIndex < 0 || columnIndex >= rowGroup.columns().size()) {
            return RowRanges.all(rowCount);
        }

        ColumnIndexBuffers colBuffers = indexBuffers.forColumn(columnIndex);
        if (colBuffers == null || colBuffers.columnIndex() == null || colBuffers.offsetIndex() == null) {
            return RowRanges.all(rowCount);
        }

        IndexPair indexPair = readIndexPair(colBuffers, location, columnIndex);
        ColumnIndex columnIdx = indexPair.columnIndex;
        OffsetIndex offsetIdx = indexPair.offsetIndex;

        List<PageLocation> pages = offsetIdx.pageLocations();
        int pageCount = pages.size();
        boolean[] keep = new boolean[pageCount];
        long[] nullCounts = columnIdx.nullCounts();

        for (int i = 0; i < pageCount; i++) {
            if (seekingNulls) {
                // IS NULL: keep page if it might contain nulls
                // Drop page only if we KNOW it has no nulls (nullCounts[i] == 0)
                if (nullCounts != null && nullCounts[i] == 0) {
                    keep[i] = false;
                }
                else {
                    keep[i] = true;
                }
            }
            else {
                // IS NOT NULL: keep page if it might contain non-nulls
                // Drop page if nullPages[i] == true (entire page is null)
                keep[i] = !columnIdx.nullPages()[i];
            }
        }

        return RowRanges.fromPages(pages, keep, rowCount);
    }

    /// Extracts the column index from a leaf predicate.
    private static int leafColumnIndex(ResolvedPredicate predicate) {
        return switch (predicate) {
            case ResolvedPredicate.IntPredicate p -> p.columnIndex();
            case ResolvedPredicate.LongPredicate p -> p.columnIndex();
            case ResolvedPredicate.FloatPredicate p -> p.columnIndex();
            case ResolvedPredicate.Float16Predicate p -> p.columnIndex();
            case ResolvedPredicate.DoublePredicate p -> p.columnIndex();
            case ResolvedPredicate.BooleanPredicate p -> p.columnIndex();
            case ResolvedPredicate.BinaryPredicate p -> p.columnIndex();
            case ResolvedPredicate.IntInPredicate p -> p.columnIndex();
            case ResolvedPredicate.LongInPredicate p -> p.columnIndex();
            case ResolvedPredicate.BinaryInPredicate p -> p.columnIndex();
            case ResolvedPredicate.DoubleInPredicate p -> p.columnIndex();
            case ResolvedPredicate.IsNullPredicate p -> p.columnIndex();
            case ResolvedPredicate.IsNotNullPredicate p -> p.columnIndex();
            case ResolvedPredicate.GeospatialPredicate p -> p.columnIndex();
            case ResolvedPredicate.And ignored -> -1;
            case ResolvedPredicate.Or ignored -> -1;
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
            if (columnIdx.nullPages()[i]) {
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

    /// Parses one column chunk's `ColumnIndex` and `OffsetIndex` together.
    ///
    /// The two structs describe the same pages from different angles — one holds the per-page
    /// statistics, the other the per-page locations — and page filtering walks them with a
    /// single index. A file where they disagree on the page count is rejected here rather than
    /// indexing one with the other's length further down.
    ///
    /// The page index is read per column chunk, outside the footer parse that
    /// [dev.hardwood.internal.reader.ParquetMetadataReader] attributes, so failures are named
    /// here: file, row group and column are all at hand.
    private static IndexPair readIndexPair(ColumnIndexBuffers colBuffers, IndexLocation location,
            int columnIndex) {
        try {
            ColumnIndex columnIdx = ColumnIndexReader.read(new ThriftCompactReader(colBuffers.columnIndex()));
            OffsetIndex offsetIdx = OffsetIndexReader.read(new ThriftCompactReader(colBuffers.offsetIndex()));
            int columnIndexPages = columnIdx.getPageCount();
            int offsetIndexPages = offsetIdx.pageLocations().size();
            if (columnIndexPages != offsetIndexPages) {
                throw new IOException("Malformed Parquet metadata: ColumnIndex describes "
                        + columnIndexPages + " pages but OffsetIndex locates " + offsetIndexPages);
            }
            return new IndexPair(columnIdx, offsetIdx);
        }
        catch (IOException e) {
            throw new UncheckedIOException(ExceptionContext.filePrefix(location.fileName())
                    + "Failed to parse the page index of column " + columnIndex
                    + " in row group " + location.rowGroupIndex() + ": " + e.getMessage(), e);
        }
    }

    /// The column chunk whose page index is being parsed, minus the column: file and row group
    /// are fixed for a whole evaluation, the column varies per leaf predicate.
    ///
    /// @param fileName name of the file being read
    /// @param rowGroupIndex index of the row group being filtered
    public record IndexLocation(String fileName, int rowGroupIndex) {}

    private record IndexPair(ColumnIndex columnIndex, OffsetIndex offsetIndex) {}
}
