/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import dev.hardwood.internal.util.Geospatial;
import dev.hardwood.metadata.BoundingBox;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.GeospatialStatistics;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.FilterPredicate;

/// Evaluates filter predicates against row group statistics and bloom filters to determine
/// whether a row group can be skipped.
///
/// Uses a conservative approach: if statistics are absent for a column,
/// the row group is never dropped (it may contain matching rows).
///
/// Equality (`EQ`) and membership (`IN`) leaves additionally consult the column's bloom filter
/// when one is supplied via a [BloomFilterSource]: a value that hashes to a definitely-absent
/// slot drops the row group even when it falls inside the statistics min/max range. The two
/// checks are complementary — either one proving no match is sufficient.
public class RowGroupFilterEvaluator {

    /// Determines whether a row group can be skipped using statistics only (no bloom filters).
    ///
    /// @param predicate the resolved predicate to evaluate
    /// @param rowGroup the row group to check
    /// @return `true` if the row group can be safely skipped (no matching rows),
    ///         `false` if it may contain matching rows
    public static boolean canDropRowGroup(ResolvedPredicate predicate, RowGroup rowGroup) {
        return canDropRowGroup(predicate, rowGroup, null);
    }

    /// Determines whether a row group can be skipped based on the given resolved predicate.
    ///
    /// @param predicate the resolved predicate to evaluate
    /// @param rowGroup the row group to check
    /// @param bloomFilters source of the row group's bloom filters, or `null` to evaluate
    ///        statistics only
    /// @return `true` if the row group can be safely skipped (no matching rows),
    ///         `false` if it may contain matching rows
    public static boolean canDropRowGroup(ResolvedPredicate predicate, RowGroup rowGroup,
            BloomFilterSource bloomFilters) {
        return switch (predicate) {
            case ResolvedPredicate.IntPredicate p -> {
                if (statisticsDrop(p, p.columnIndex(), rowGroup)) {
                    yield true;
                }
                yield p.op() == FilterPredicate.Operator.EQ
                        && BloomFilterSupport.absent(bloomFilters, p.columnIndex(), p.value());
            }
            case ResolvedPredicate.LongPredicate p -> {
                if (statisticsDrop(p, p.columnIndex(), rowGroup)) {
                    yield true;
                }
                yield p.op() == FilterPredicate.Operator.EQ
                        && BloomFilterSupport.absent(bloomFilters, p.columnIndex(), p.value());
            }
            case ResolvedPredicate.FloatPredicate p -> {
                if (statisticsDrop(p, p.columnIndex(), rowGroup)) {
                    yield true;
                }
                yield p.op() == FilterPredicate.Operator.EQ
                        && BloomFilterSupport.absent(bloomFilters, p.columnIndex(), p.value());
            }
            case ResolvedPredicate.Float16Predicate p ->
                    statisticsDrop(p, p.columnIndex(), rowGroup);
            case ResolvedPredicate.DoublePredicate p -> {
                if (statisticsDrop(p, p.columnIndex(), rowGroup)) {
                    yield true;
                }
                yield p.op() == FilterPredicate.Operator.EQ
                        && BloomFilterSupport.absent(bloomFilters, p.columnIndex(), p.value());
            }
            case ResolvedPredicate.BooleanPredicate p ->
                    statisticsDrop(p, p.columnIndex(), rowGroup);
            case ResolvedPredicate.BinaryPredicate p -> {
                if (statisticsDrop(p, p.columnIndex(), rowGroup)) {
                    yield true;
                }
                yield p.op() == FilterPredicate.Operator.EQ
                        && BloomFilterSupport.absent(bloomFilters, p.columnIndex(), p.value());
            }
            case ResolvedPredicate.IntInPredicate p -> {
                if (statisticsDrop(p, p.columnIndex(), rowGroup)) {
                    yield true;
                }
                yield BloomFilterSupport.absentAll(bloomFilters, p.columnIndex(), p.values());
            }
            case ResolvedPredicate.LongInPredicate p -> {
                if (statisticsDrop(p, p.columnIndex(), rowGroup)) {
                    yield true;
                }
                yield BloomFilterSupport.absentAll(bloomFilters, p.columnIndex(), p.values());
            }
            case ResolvedPredicate.BinaryInPredicate p -> {
                if (statisticsDrop(p, p.columnIndex(), rowGroup)) {
                    yield true;
                }
                yield BloomFilterSupport.absentAll(bloomFilters, p.columnIndex(), p.values());
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
                    if (canDropRowGroup(child, rowGroup, bloomFilters)) {
                        yield true;
                    }
                }
                yield false;
            }
            case ResolvedPredicate.Or o -> {
                for (ResolvedPredicate child : o.children()) {
                    if (!canDropRowGroup(child, rowGroup, bloomFilters)) {
                        yield false;
                    }
                }
                yield true;
            }
            case ResolvedPredicate.GeospatialPredicate p -> {
                ColumnMetaData cmd = rowGroup.columns().get(p.columnIndex()).metaData();
                GeospatialStatistics geospatialStatistics = cmd.geospatialStatistics();
                if (geospatialStatistics == null || geospatialStatistics.bbox() == null) {
                    yield false; // no stats, can't drop
                }
                BoundingBox bbox = geospatialStatistics.bbox();
                yield !Geospatial.xAxisOverlaps(bbox.xmin(), bbox.xmax(), p.xmin(), p.xmax()) ||
                        bbox.ymax() < p.ymin() ||
                        bbox.ymin() > p.ymax();
            }
        };
    }

    /// Whether the column's min/max statistics prove the leaf matches no rows.
    private static boolean statisticsDrop(ResolvedPredicate leaf, int columnIndex, RowGroup rowGroup) {
        Statistics stats = getStatistics(columnIndex, rowGroup);
        return stats != null && StatisticsFilterSupport.canDropLeaf(leaf, MinMaxStats.of(stats));
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
