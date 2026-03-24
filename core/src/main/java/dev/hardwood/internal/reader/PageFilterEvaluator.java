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
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.FilterPredicate.And;
import dev.hardwood.reader.FilterPredicate.BinaryColumnPredicate;
import dev.hardwood.reader.FilterPredicate.BooleanColumnPredicate;
import dev.hardwood.reader.FilterPredicate.DoubleColumnPredicate;
import dev.hardwood.reader.FilterPredicate.FloatColumnPredicate;
import dev.hardwood.reader.FilterPredicate.IntColumnPredicate;
import dev.hardwood.reader.FilterPredicate.LongColumnPredicate;
import dev.hardwood.reader.FilterPredicate.Not;
import dev.hardwood.reader.FilterPredicate.Or;
import dev.hardwood.schema.FileSchema;

/// Evaluates a [FilterPredicate] against per-page statistics from the Column Index
/// to produce [RowRanges] representing rows that might match.
///
/// This is the page-level equivalent of [RowGroupFilterEvaluator]. While that class
/// decides whether an entire row group can be skipped, this class determines which
/// pages within a surviving row group can be skipped.
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
            case IntColumnPredicate p -> evaluateLeaf(p.column(), rowGroup, schema, indexBuffers, rowCount,
                    (ci, i) -> {
                        int min = StatisticsDecoder.decodeInt(ci.minValues().get(i));
                        int max = StatisticsDecoder.decodeInt(ci.maxValues().get(i));
                        return RowGroupFilterEvaluator.canDrop(p.op(), p.value(), min, max);
                    });
            case LongColumnPredicate p -> evaluateLeaf(p.column(), rowGroup, schema, indexBuffers, rowCount,
                    (ci, i) -> {
                        long min = StatisticsDecoder.decodeLong(ci.minValues().get(i));
                        long max = StatisticsDecoder.decodeLong(ci.maxValues().get(i));
                        return RowGroupFilterEvaluator.canDrop(p.op(), p.value(), min, max);
                    });
            case FloatColumnPredicate p -> evaluateLeaf(p.column(), rowGroup, schema, indexBuffers, rowCount,
                    (ci, i) -> {
                        float min = StatisticsDecoder.decodeFloat(ci.minValues().get(i));
                        float max = StatisticsDecoder.decodeFloat(ci.maxValues().get(i));
                        return RowGroupFilterEvaluator.canDropFloat(p.op(), p.value(), min, max);
                    });
            case DoubleColumnPredicate p -> evaluateLeaf(p.column(), rowGroup, schema, indexBuffers, rowCount,
                    (ci, i) -> {
                        double min = StatisticsDecoder.decodeDouble(ci.minValues().get(i));
                        double max = StatisticsDecoder.decodeDouble(ci.maxValues().get(i));
                        return RowGroupFilterEvaluator.canDropDouble(p.op(), p.value(), min, max);
                    });
            case BooleanColumnPredicate p -> evaluateLeaf(p.column(), rowGroup, schema, indexBuffers, rowCount,
                    (ci, i) -> {
                        int min = StatisticsDecoder.decodeBoolean(ci.minValues().get(i)) ? 1 : 0;
                        int max = StatisticsDecoder.decodeBoolean(ci.maxValues().get(i)) ? 1 : 0;
                        int value = p.value() ? 1 : 0;
                        return RowGroupFilterEvaluator.canDrop(p.op(), value, min, max);
                    });
            case BinaryColumnPredicate p -> evaluateLeaf(p.column(), rowGroup, schema, indexBuffers, rowCount,
                    (ci, i) -> {
                        byte[] min = ci.minValues().get(i);
                        byte[] max = ci.maxValues().get(i);
                        int cmpMin = StatisticsDecoder.compareBinary(p.value(), min);
                        int cmpMax = StatisticsDecoder.compareBinary(p.value(), max);
                        return RowGroupFilterEvaluator.canDropCompared(p.op(), cmpMin, cmpMax,
                                StatisticsDecoder.compareBinary(min, max));
                    });
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
        };
    }

    /// Evaluates a leaf predicate against the Column Index for a single column,
    /// producing RowRanges for pages that cannot be dropped.
    private static RowRanges evaluateLeaf(String columnName, RowGroup rowGroup,
            FileSchema schema, RowGroupIndexBuffers indexBuffers, long rowCount,
            PageCanDropTest canDropTest) {

        int columnIndex = RowGroupFilterEvaluator.findColumnIndex(columnName, schema);
        if (columnIndex < 0) {
            columnIndex = RowGroupFilterEvaluator.findColumnIndexByPath(columnName, rowGroup);
        }
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
            throw new UncheckedIOException("Failed to parse Column/Offset Index for column '" + columnName + "'", e);
        }

        return evaluatePages(columnIdx, offsetIdx, rowCount, canDropTest);
    }

    /// Evaluates a keep bitmap for pages using pre-parsed Column Index and Offset Index.
    static RowRanges evaluatePages(ColumnIndex columnIdx, OffsetIndex offsetIdx,
            long rowCount, PageCanDropTest canDropTest) {
        List<PageLocation> pages = offsetIdx.pageLocations();
        int pageCount = pages.size();
        boolean[] keep = new boolean[pageCount];

        for (int i = 0; i < pageCount; i++) {
            // Null-only pages cannot match any value predicate
            if (columnIdx.nullPages().get(i)) {
                continue;
            }
            // Keep the page if it cannot be dropped
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
