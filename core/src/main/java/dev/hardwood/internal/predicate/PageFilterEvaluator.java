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
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.reader.ParquetReadException;

/// Evaluates a [ResolvedPredicate] against per-page statistics from the Column Index
/// to produce [RowRanges] representing rows that might match.
///
/// This is the page-level equivalent of [RowGroupFilterEvaluator]. While that class
/// decides whether an entire row group can be skipped, this class determines which
/// pages within a surviving row group can be skipped.
///
/// Each leaf is decided page by page through [UnitStats#decide], the same decision
/// [RowGroupFilterEvaluator] asks of a column chunk, and a page is kept unless it is
/// [FilterDecision#CANNOT_MATCH].
///
/// When the `ColumnIndex` is absent, this evaluator returns `RowRanges.all()` — page
/// skipping then happens per-column inside [dev.hardwood.internal.reader.SequentialFetchPlan]
/// based on inline `DataPageHeader.statistics`.
public class PageFilterEvaluator {

    /// Computes the row ranges within a row group that might match the given predicate,
    /// based on per-page statistics from the Column Index.
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
            RowGroupIndexBuffers indexBuffers, LogContext logContext, BoundsReadability readability) {
        long rowCount = rowGroup.numRows();
        return evaluate(predicate, rowGroup, indexBuffers, rowCount, logContext, readability);
    }

    private static RowRanges evaluate(ResolvedPredicate predicate, RowGroup rowGroup,
            RowGroupIndexBuffers indexBuffers, long rowCount, LogContext logContext, BoundsReadability readability) {
        return switch (predicate) {
            case ResolvedPredicate.And a -> {
                RowRanges result = RowRanges.all(rowCount);
                for (ResolvedPredicate child : a.children()) {
                    result = result.intersect(evaluate(child, rowGroup, indexBuffers, rowCount, logContext, readability));
                }
                yield result;
            }
            case ResolvedPredicate.Or o -> {
                RowRanges result = null;
                for (ResolvedPredicate child : o.children()) {
                    RowRanges childRanges = evaluate(child, rowGroup, indexBuffers, rowCount, logContext, readability);
                    result = (result == null) ? childRanges : result.union(childRanges);
                }
                yield (result != null) ? result : RowRanges.all(rowCount);
            }
            // Parquet has no per-page geospatial statistics (GeospatialStatistics lives only on
            // ColumnMetaData, applied during row-group filtering), so no page-level pruning is possible.
            case ResolvedPredicate.GeospatialPredicate ignored -> RowRanges.all(rowCount);
            default -> evaluateLeafPages(predicate, rowGroup, indexBuffers, rowCount, logContext, readability);
        };
    }

    /// Evaluates a leaf predicate against the Column Index of the column it tests, keeping every
    /// page where the column has none.
    private static RowRanges evaluateLeafPages(ResolvedPredicate leaf, RowGroup rowGroup,
            RowGroupIndexBuffers indexBuffers, long rowCount, LogContext logContext, BoundsReadability readability) {

        int columnIndex = ResolvedPredicate.leafColumnIndex(leaf);
        if (columnIndex < 0 || columnIndex >= rowGroup.columns().size()) {
            return RowRanges.all(rowCount);
        }

        ColumnIndexBuffers colBuffers = indexBuffers.forColumn(columnIndex);
        if (colBuffers.columnIndex() == null || colBuffers.offsetIndex() == null) {
            return RowRanges.all(rowCount);
        }

        ColumnMetaData metaData = rowGroup.columns().get(columnIndex).metaData();
        IndexPair indexPair = readIndexPair(colBuffers, columnIndex, metaData);

        LogContext columnContext = metaData == null ? logContext : logContext.withColumn(metaData.pathInSchema());

        return evaluatePages(indexPair.columnIndex, indexPair.offsetIndex, rowCount, leaf, columnContext,
                readability);
    }

    /// Decides a leaf page by page over a pre-parsed Column Index and Offset Index, keeping every
    /// page the decision does not rule out. Used by [#evaluateLeafPages] and directly by tests.
    ///
    /// @param columnContext the column chunk the two indexes describe, which each page narrows
    ///        to itself to report bounds it had to discard
    static RowRanges evaluatePages(ColumnIndex columnIdx, OffsetIndex offsetIdx, long rowCount,
            ResolvedPredicate leaf, LogContext columnContext, BoundsReadability readability) {
        List<PageLocation> pages = offsetIdx.pageLocations();
        int pageCount = pages.size();
        boolean[] keep = new boolean[pageCount];

        PageRange candidates = orderedCandidates(columnIdx, leaf, readability);
        for (int i = candidates.first(); i < candidates.end(); i++) {
            UnitStats page = UnitStats.IndexPageStats.of(columnIdx, pages, i, rowCount, readability);
            keep[i] = page.decide(leaf, columnContext) != FilterDecision.CANNOT_MATCH;
        }

        return RowRanges.fromPages(pages, keep, rowCount);
    }

    /// The contiguous page span an ordered Column Index says may match `leaf`, or the full page
    /// range when the index or predicate does not support a sound binary search. The returned span
    /// is only a candidate: pages inside it still take the ordinary [UnitStats#decide] path above.
    private static PageRange orderedCandidates(ColumnIndex columnIndex, ResolvedPredicate leaf,
            BoundsReadability readability) {
        int pageCount = columnIndex.getPageCount();
        Operator operator = orderedOperator(leaf);
        if (pageCount == 0 || columnIndex.boundaryOrder() == ColumnIndex.BoundaryOrder.UNORDERED
                || operator == null || operator == Operator.NOT_EQ) {
            return new PageRange(0, pageCount);
        }
        // BoundaryOrder says nothing about where null-only pages sit. Their min/max entries are
        // placeholders rather than bounds, so even one makes the linear decision path necessary.
        for (boolean nullPage : columnIndex.nullPages()) {
            if (nullPage) {
                return new PageRange(0, pageCount);
            }
        }

        ResolvedPredicate tooSmall = switch (operator) {
            case EQ -> withOperator(leaf, Operator.GT_EQ);
            case GT, GT_EQ -> leaf;
            case LT, LT_EQ -> null;
            case NOT_EQ -> throw new IllegalStateException("NOT_EQ is not searched");
        };
        ResolvedPredicate tooLarge = switch (operator) {
            case EQ -> withOperator(leaf, Operator.LT_EQ);
            case LT, LT_EQ -> leaf;
            case GT, GT_EQ -> null;
            case NOT_EQ -> throw new IllegalStateException("NOT_EQ is not searched");
        };

        boolean ascending = columnIndex.boundaryOrder() == ColumnIndex.BoundaryOrder.ASCENDING;
        ResolvedPredicate atStart = ascending ? tooSmall : tooLarge;
        ResolvedPredicate atEnd = ascending ? tooLarge : tooSmall;

        OrderedSearch search = new OrderedSearch(columnIndex, leaf, readability, pageCount);
        int first = atStart == null ? 0 : search.firstKept(atStart);
        int end = atEnd == null ? pageCount : search.firstDropped(atEnd);

        return search.usable() ? new PageRange(first, Math.max(first, end)) : new PageRange(0, pageCount);
    }

    /// Operators whose non-matching pages form one or two ordered runs.
    private static Operator orderedOperator(ResolvedPredicate leaf) {
        return switch (leaf) {
            case ResolvedPredicate.IntPredicate p -> p.op();
            case ResolvedPredicate.LongPredicate p -> p.op();
            case ResolvedPredicate.UnsignedIntPredicate p -> p.op();
            case ResolvedPredicate.UnsignedLongPredicate p -> p.op();
            case ResolvedPredicate.BinaryPredicate p -> p.op();
            default -> null;
        };
    }

    /// The same scalar leaf with another operator, preserving its decoded literal and comparison.
    private static ResolvedPredicate withOperator(ResolvedPredicate leaf, Operator operator) {
        return switch (leaf) {
            case ResolvedPredicate.IntPredicate p ->
                    new ResolvedPredicate.IntPredicate(p.columnIndex(), operator, p.value());
            case ResolvedPredicate.LongPredicate p ->
                    new ResolvedPredicate.LongPredicate(p.columnIndex(), operator, p.value());
            case ResolvedPredicate.UnsignedIntPredicate p ->
                    new ResolvedPredicate.UnsignedIntPredicate(p.columnIndex(), operator, p.value());
            case ResolvedPredicate.UnsignedLongPredicate p ->
                    new ResolvedPredicate.UnsignedLongPredicate(p.columnIndex(), operator, p.value());
            case ResolvedPredicate.BinaryPredicate p ->
                    new ResolvedPredicate.BinaryPredicate(p.columnIndex(), operator, p.value(), p.comparison());
            default -> throw new IllegalArgumentException("Not an ordered scalar predicate: " + leaf);
        };
    }

    /// Binary-searches predicates over one ordered min/max list. A bound that [MinMaxStats]
    /// refuses makes the whole attempt unusable; the caller then performs the linear scan.
    private static final class OrderedSearch {

        private final ColumnIndex columnIndex;
        private final ResolvedPredicate leaf;
        private final BoundsReadability readability;
        private final int pageCount;
        private boolean usable = true;

        private OrderedSearch(ColumnIndex columnIndex, ResolvedPredicate leaf,
                BoundsReadability readability, int pageCount) {
            this.columnIndex = columnIndex;
            this.leaf = leaf;
            this.readability = readability;
            this.pageCount = pageCount;
        }

        /// The first page whose answer differs from `leadingAnswer`. Sound only because sorted
        /// bounds put every page answering `leadingAnswer` before every page that does not.
        private int edge(ResolvedPredicate tested, boolean leadingAnswer) {
            int from = 0;
            int to = pageCount;
            while (from < to && usable) {
                int middle = (from + to) >>> 1;
                if (test(middle, tested) == leadingAnswer) {
                    from = middle + 1;
                }
                else {
                    to = middle;
                }
            }
            return from;
        }

        /// The first page that is not dropped, where the dropped pages come first.
        private int firstKept(ResolvedPredicate tested) {
            return edge(tested, true);
        }

        /// The first page that is dropped, where the dropped pages come last.
        private int firstDropped(ResolvedPredicate tested) {
            return edge(tested, false);
        }

        private boolean test(int pageIndex, ResolvedPredicate tested) {
            MinMaxStats stats = MinMaxStats.ofPage(columnIndex, pageIndex, leaf, readability);
            if (stats instanceof MinMaxStats.NoBounds) {
                usable = false;
                return false;
            }
            return stats.canDrop(tested);
        }

        private boolean usable() {
            return usable;
        }
    }

    private record PageRange(int first, int end) {}

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
    private static IndexPair readIndexPair(ColumnIndexBuffers colBuffers, int columnIndex,
            ColumnMetaData metaData) {
        ColumnIndex columnIdx;
        OffsetIndex offsetIdx;
        try {
            columnIdx = ColumnIndexReader.read(new ThriftCompactReader(colBuffers.columnIndex()));
            offsetIdx = OffsetIndexReader.read(new ThriftCompactReader(colBuffers.offsetIndex()), metaData);
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
