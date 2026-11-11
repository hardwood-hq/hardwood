/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import dev.hardwood.internal.reader.plan.ColumnIndexBuffers;
import dev.hardwood.metadata.ColumnIndex;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.RowGroup;

/// The two steps of [PageFilterEvaluator#computeMatchingRows] for one column, each callable on its
/// own, for benchmarks outside the predicate package: parsing a chunk's page index, and deciding a
/// predicate page by page over an index already parsed.
public final class PagePruningSteps {

    private static final LogContext LOG_CONTEXT = new LogContext("benchmark", 0);

    private PagePruningSteps() {
    }

    /// The Column Index and Offset Index of column `column`, parsed.
    public record ParsedIndexes(int column, ColumnIndex columnIndex, OffsetIndex offsetIndex) {

        /// The same indexes with the Column Index's boundary order replaced by `UNORDERED`, so
        /// deciding them takes the linear path over identical bounds.
        public ParsedIndexes unordered() {
            ColumnIndex index = new ColumnIndex(columnIndex.nullPages(), columnIndex.minValues(),
                    columnIndex.maxValues(), ColumnIndex.BoundaryOrder.UNORDERED, columnIndex.nullCounts(),
                    columnIndex.repetitionLevelHistograms(), columnIndex.definitionLevelHistograms(),
                    columnIndex.nanCounts());
            return new ParsedIndexes(column, index, offsetIndex);
        }
    }

    /// Parses the Column Index and Offset Index of column `column` from their serialized bytes,
    /// as [PageFilterEvaluator] does before deciding any page.
    public static ParsedIndexes parse(ColumnIndexBuffers buffers, int column, RowGroup rowGroup) {
        PageFilterEvaluator.IndexPair pair = PageFilterEvaluator.readIndexPair(buffers, column, rowGroup);
        return new ParsedIndexes(column, pair.columnIndex(), pair.offsetIndex());
    }

    /// Decides the leaf `leaf` over the parsed indexes of the column it tests, as
    /// [PageFilterEvaluator#computeMatchingRows] does for each leaf. Returns the number of rows
    /// kept.
    ///
    /// @throws IllegalArgumentException if `leaf` is an `AND` or `OR`, or tests a column other
    ///         than the parsed one
    public static long decide(ResolvedPredicate leaf, ParsedIndexes indexes, long rowCount) {
        if (leaf instanceof ResolvedPredicate.And || leaf instanceof ResolvedPredicate.Or) {
            throw new IllegalArgumentException("Expected a leaf predicate but got "
                    + leaf.getClass().getSimpleName());
        }
        int column = ResolvedPredicate.leafColumnIndex(leaf);
        if (column != indexes.column()) {
            throw new IllegalArgumentException("The leaf tests column " + column
                    + " but the parsed indexes are those of column " + indexes.column());
        }
        return PageFilterEvaluator.evaluatePages(indexes.columnIndex(), indexes.offsetIndex(), rowCount, leaf,
                LOG_CONTEXT, BoundsReadability.ALL).rowCount(rowCount);
    }
}
