/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

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
import dev.hardwood.reader.ParquetReadException;

/// Evaluates a [ResolvedPredicate] against per-page statistics from the Column Index
/// to produce [RowRanges] representing rows that might match.
///
/// This is the page-level equivalent of [RowGroupFilterEvaluator]. While that class
/// decides whether an entire row group can be skipped, this class determines which
/// pages within a surviving row group can be skipped.
///
/// Leaf predicate evaluation is delegated to [MinMaxStats#canDrop], which decodes each page's
/// bounds once and answers for the leaf they were decoded for.
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
    /// @param logContext  where this row group is, for the warning raised when a column's
    ///                     page bounds turn out to be unusable
    /// @return row ranges that might contain matching rows
    public static RowRanges computeMatchingRows(ResolvedPredicate predicate, RowGroup rowGroup,
            RowGroupIndexBuffers indexBuffers, LogContext logContext) {
        long rowCount = rowGroup.numRows();
        return evaluate(predicate, rowGroup, indexBuffers, rowCount, logContext);
    }

    private static RowRanges evaluate(ResolvedPredicate predicate, RowGroup rowGroup,
            RowGroupIndexBuffers indexBuffers, long rowCount, LogContext logContext) {
        return switch (predicate) {
            case ResolvedPredicate.And a -> {
                RowRanges result = RowRanges.all(rowCount);
                for (ResolvedPredicate child : a.children()) {
                    result = result.intersect(evaluate(child, rowGroup, indexBuffers, rowCount, logContext));
                }
                yield result;
            }
            case ResolvedPredicate.Or o -> {
                RowRanges result = null;
                for (ResolvedPredicate child : o.children()) {
                    RowRanges childRanges = evaluate(child, rowGroup, indexBuffers, rowCount, logContext);
                    result = (result == null) ? childRanges : result.union(childRanges);
                }
                yield (result != null) ? result : RowRanges.all(rowCount);
            }
            case ResolvedPredicate.IsNullPredicate p -> evaluateNullPages(p.columnIndex(), p.definitionLevel(),
                    p.leafDefinitionLevel(), true, rowGroup, indexBuffers, rowCount);
            case ResolvedPredicate.IsNotNullPredicate p -> evaluateNullPages(p.columnIndex(), p.definitionLevel(),
                    p.leafDefinitionLevel(), false, rowGroup, indexBuffers, rowCount);
            // Parquet has no per-page geospatial statistics (GeospatialStatistics lives only on
            // ColumnMetaData, applied during row-group filtering), so no page-level pruning is possible.
            case ResolvedPredicate.GeospatialPredicate ignored -> RowRanges.all(rowCount);
            default -> evaluateLeafPages(predicate, rowGroup, indexBuffers, rowCount, logContext);
        };
    }

    /// Evaluates a leaf predicate against per-page Column Index statistics,
    /// using [MinMaxStats#canDrop] for the actual comparison.
    private static RowRanges evaluateLeafPages(ResolvedPredicate predicate, RowGroup rowGroup,
            RowGroupIndexBuffers indexBuffers, long rowCount, LogContext logContext) {

        int columnIndex = ResolvedPredicate.leafColumnIndex(predicate);
        if (columnIndex < 0 || columnIndex >= rowGroup.columns().size()) {
            return RowRanges.all(rowCount);
        }

        ColumnIndexBuffers colBuffers = indexBuffers.forColumn(columnIndex);
        if (colBuffers == null || colBuffers.columnIndex() == null || colBuffers.offsetIndex() == null) {
            return RowRanges.all(rowCount);
        }

        IndexPair indexPair = readIndexPair(colBuffers, columnIndex);
        ColumnIndex columnIdx = indexPair.columnIndex;
        OffsetIndex offsetIdx = indexPair.offsetIndex;

        List<PageLocation> pages = offsetIdx.pageLocations();
        int pageCount = pages.size();
        boolean[] keep = new boolean[pageCount];

        LogContext columnContext = logContext.withColumn(
                rowGroup.columns().get(columnIndex).metaData().pathInSchema());

        for (int i = 0; i < pageCount; i++) {
            if (columnIdx.nullPages()[i]) {
                continue;
            }
            MinMaxStats pageStats = MinMaxStats.ofPage(columnIdx, i, predicate);
            pageStats.reportIfDiscarded(columnContext.withPageIndex(i));
            keep[i] = !pageStats.canDrop(predicate);
        }

        return RowRanges.fromPages(pages, keep, rowCount);
    }

    /// Evaluates IS NULL / IS NOT NULL predicates against per-page null information
    /// from the Column Index to produce [RowRanges] representing rows that might match.
    ///
    /// A predicate on an enclosing group is answered from the per-page definition level histogram
    /// instead, which is the only per-page statistic that separates an absent group from a present
    /// one. A page whose histogram the file omits, or wrote at a length that does not match the
    /// column, is kept.
    ///
    /// @param columnIndex         the leaf column to check
    /// @param definitionLevel     the level at or above which the tested node is present
    /// @param leafDefinitionLevel the leaf column's maximum definition level
    /// @param seekingNulls        `true` for IS NULL (keep pages that might contain nulls),
    ///                            `false` for IS NOT NULL (keep pages that might contain non-nulls)
    /// @param rowGroup            the row group being evaluated
    /// @param indexBuffers        pre-fetched index buffers
    /// @param rowCount            total rows in the row group
    /// @return row ranges that might contain matching rows
    private static RowRanges evaluateNullPages(int columnIndex, int definitionLevel, int leafDefinitionLevel,
            boolean seekingNulls, RowGroup rowGroup, RowGroupIndexBuffers indexBuffers, long rowCount) {

        if (columnIndex < 0 || columnIndex >= rowGroup.columns().size()) {
            return RowRanges.all(rowCount);
        }

        ColumnIndexBuffers colBuffers = indexBuffers.forColumn(columnIndex);
        if (colBuffers == null || colBuffers.columnIndex() == null || colBuffers.offsetIndex() == null) {
            return RowRanges.all(rowCount);
        }

        IndexPair indexPair = readIndexPair(colBuffers, columnIndex);
        ColumnIndex columnIdx = indexPair.columnIndex;
        OffsetIndex offsetIdx = indexPair.offsetIndex;

        List<PageLocation> pages = offsetIdx.pageLocations();
        int pageCount = pages.size();
        boolean[] keep = new boolean[pageCount];

        if (definitionLevel < leafDefinitionLevel) {
            for (int i = 0; i < pageCount; i++) {
                long[] histogram = columnIdx.definitionLevelHistogram(i);
                keep[i] = histogram == null
                        || histogram.length != leafDefinitionLevel + 1
                        || hasEntryOnSideOf(histogram, definitionLevel, seekingNulls);
            }

            return RowRanges.fromPages(pages, keep, rowCount);
        }

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

    /// Whether `histogram` counts an entry on the side of `definitionLevel` a null predicate
    /// matches: below it for IS NULL, at or above it for IS NOT NULL.
    ///
    /// A definition level histogram holds one bucket per level up to the leaf's maximum, counting
    /// the entries written at that level. A null predicate on a node in the leaf's path splits
    /// those buckets at the node's own level: everything below was written with the node absent,
    /// everything at or above with it present. No entry on the matching side proves no row
    /// matches, which is what lets a page be dropped.
    private static boolean hasEntryOnSideOf(long[] histogram, int definitionLevel, boolean seekingNulls) {
        int from = seekingNulls ? 0 : definitionLevel;
        int to = seekingNulls ? definitionLevel : histogram.length;

        for (int level = from; level < to; level++) {
            if (histogram[level] > 0) {
                return true;
            }
        }

        return false;
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
    /// here by the read pipeline, which names the file, row group and column on the way
    /// out; what this frame adds is which of the chunk's two indexes was being read.
    private static IndexPair readIndexPair(ColumnIndexBuffers colBuffers, int columnIndex) {
        ColumnIndex columnIdx;
        OffsetIndex offsetIdx;
        try {
            columnIdx = ColumnIndexReader.read(new ThriftCompactReader(colBuffers.columnIndex()));
            offsetIdx = OffsetIndexReader.read(new ThriftCompactReader(colBuffers.offsetIndex()));
        }
        catch (ParquetReadException e) {
            // The reader already says the failure is the file's; what it cannot
            // say is which column's index it was parsing.
            throw new ParquetReadException(prefix(columnIndex) + e.getMessage(), e);
        }
        // Outside the catch, and prefixed once: both structs parsed, so this is the
        // pair disagreeing rather than either of them failing to read.
        int columnIndexPages = columnIdx.getPageCount();
        int offsetIndexPages = offsetIdx.pageLocations().size();
        if (columnIndexPages != offsetIndexPages) {
            throw new ParquetReadException(prefix(columnIndex)
                    + "Malformed Parquet metadata: ColumnIndex describes "
                    + columnIndexPages + " pages but OffsetIndex locates " + offsetIndexPages);
        }
        return new IndexPair(columnIdx, offsetIdx);
    }

    /// Says which of the chunk's indexes was being read, for the message of every failure
    /// [#readIndexPair] reports.
    ///
    /// The file and row group are not repeated: a page index is read only while a column's
    /// pages are planned, so the pipeline names both on the way out. The column's ordinal
    /// stays, because the pipeline names the path and a chunk deferring to another file has
    /// none to take.
    private static String prefix(int columnIndex) {
        return "Failed to parse the page index of column " + columnIndex + ": ";
    }


    private record IndexPair(ColumnIndex columnIndex, OffsetIndex offsetIndex) {}
}
