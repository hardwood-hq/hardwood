/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.Statistics;

/// Evaluates filter predicates against row group statistics to determine
/// whether a row group can be skipped.
///
/// Uses a conservative approach: if statistics are absent for a column,
/// the row group is never dropped (it may contain matching rows).
public class RowGroupFilterEvaluator {

    /// Determines whether a row group can be skipped based on the given resolved predicate.
    ///
    /// @param predicate the resolved predicate to evaluate
    /// @param rowGroup the row group to check
    /// @return `true` if the row group can be safely skipped (no matching rows),
    ///         `false` if it may contain matching rows
    public static boolean canDropRowGroup(ResolvedPredicate predicate, RowGroup rowGroup) {
        return switch (predicate) {
            case ResolvedPredicate.IntPredicate p -> {
                Statistics stats = getStatistics(p.columnIndex(), rowGroup);
                yield stats != null && StatisticsFilterSupport.canDropLeaf(p, MinMaxStats.of(stats));
            }
            case ResolvedPredicate.LongPredicate p -> {
                Statistics stats = getStatistics(p.columnIndex(), rowGroup);
                yield stats != null && StatisticsFilterSupport.canDropLeaf(p, MinMaxStats.of(stats));
            }
            case ResolvedPredicate.FloatPredicate p -> {
                Statistics stats = getStatistics(p.columnIndex(), rowGroup);
                yield stats != null && StatisticsFilterSupport.canDropLeaf(p, MinMaxStats.of(stats));
            }
            case ResolvedPredicate.DoublePredicate p -> {
                Statistics stats = getStatistics(p.columnIndex(), rowGroup);
                yield stats != null && StatisticsFilterSupport.canDropLeaf(p, MinMaxStats.of(stats));
            }
            case ResolvedPredicate.BooleanPredicate p -> {
                Statistics stats = getStatistics(p.columnIndex(), rowGroup);
                yield stats != null && StatisticsFilterSupport.canDropLeaf(p, MinMaxStats.of(stats));
            }
            case ResolvedPredicate.BinaryPredicate p -> {
                Statistics stats = getStatistics(p.columnIndex(), rowGroup);
                yield stats != null && StatisticsFilterSupport.canDropLeaf(p, MinMaxStats.of(stats));
            }
            case ResolvedPredicate.IntInPredicate p -> {
                Statistics stats = getStatistics(p.columnIndex(), rowGroup);
                yield stats != null && StatisticsFilterSupport.canDropLeaf(p, MinMaxStats.of(stats));
            }
            case ResolvedPredicate.LongInPredicate p -> {
                Statistics stats = getStatistics(p.columnIndex(), rowGroup);
                yield stats != null && StatisticsFilterSupport.canDropLeaf(p, MinMaxStats.of(stats));
            }
            case ResolvedPredicate.BinaryInPredicate p -> {
                Statistics stats = getStatistics(p.columnIndex(), rowGroup);
                yield stats != null && StatisticsFilterSupport.canDropLeaf(p, MinMaxStats.of(stats));
            }
            case ResolvedPredicate.IsNullPredicate p -> {
                Statistics stats = getStatistics(p.columnIndex(), rowGroup);
                // Can drop IS NULL if nullCount is known to be 0 (no nulls exist)
                yield stats != null && stats.nullCount() != null && stats.nullCount() == 0;
            }
            case ResolvedPredicate.IsNotNullPredicate p -> {
                Statistics stats = getStatistics(p.columnIndex(), rowGroup);
                // Can drop IS NOT NULL if all values are null (nullCount == numRows)
                yield stats != null && stats.nullCount() != null && stats.nullCount() == rowGroup.numRows();
            }
            case ResolvedPredicate.And a -> {
                for (ResolvedPredicate child : a.children()) {
                    if (canDropRowGroup(child, rowGroup)) {
                        yield true;
                    }
                }
                yield false;
            }
            case ResolvedPredicate.Or o -> {
                for (ResolvedPredicate child : o.children()) {
                    if (!canDropRowGroup(child, rowGroup)) {
                        yield false;
                    }
                }
                yield true;
            }
        };
    }

    /// Gets statistics for a column by its pre-resolved index.
    /// Returns null if the column index is out of bounds or statistics are absent.
    private static Statistics getStatistics(int columnIndex, RowGroup rowGroup) {
        if (columnIndex < 0 || columnIndex >= rowGroup.columns().size()) {
            return null;
        }
        return rowGroup.columns().get(columnIndex).metaData().statistics();
    }
}
