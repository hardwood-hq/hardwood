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
/// whether a row group can be skipped — or read without per-row filtering.
///
/// Uses a conservative approach: if statistics are absent for a column,
/// the row group is never dropped (it may contain matching rows) and never
/// promised to match in full.
///
/// Equality (`EQ`) and membership (`IN`) leaves additionally consult the column's bloom filter
/// when one is supplied via a [BloomFilterSource]: a value that hashes to a definitely-absent
/// slot drops the row group even when it falls inside the statistics min/max range. The two
/// checks are complementary — either one proving no match is sufficient. A bloom filter can
/// only prove absence, so it never upgrades a decision to [StatsDecision#ALWAYS_MATCHES].
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
        return decideRowGroup(predicate, rowGroup, bloomFilters) == StatsDecision.CANNOT_MATCH;
    }

    /// Evaluates the predicate against the row group's statistics and bloom filters as a
    /// three-valued [StatsDecision].
    ///
    /// [StatsDecision#CANNOT_MATCH] row groups can be skipped entirely;
    /// [StatsDecision#ALWAYS_MATCHES] row groups can be read with per-row predicate
    /// evaluation skipped, since statistics prove every row satisfies the predicate.
    ///
    /// @param predicate the resolved predicate to evaluate
    /// @param rowGroup the row group to check
    /// @param bloomFilters source of the row group's bloom filters, or `null` to evaluate
    ///        statistics only
    /// @return the statistics decision for the row group
    public static StatsDecision decideRowGroup(ResolvedPredicate predicate, RowGroup rowGroup,
            BloomFilterSource bloomFilters) {
        return switch (predicate) {
            case ResolvedPredicate.IntPredicate p -> {
                StatsDecision decision = statisticsDecision(p, p.columnIndex(), rowGroup);
                if (decision != StatsDecision.CANNOT_MATCH
                        && p.op() == FilterPredicate.Operator.EQ
                        && BloomFilterSupport.valueAbsent(bloomFilters, p.columnIndex(), p.value())) {
                    yield StatsDecision.CANNOT_MATCH;
                }
                yield decision;
            }
            case ResolvedPredicate.LongPredicate p -> {
                StatsDecision decision = statisticsDecision(p, p.columnIndex(), rowGroup);
                if (decision != StatsDecision.CANNOT_MATCH
                        && p.op() == FilterPredicate.Operator.EQ
                        && BloomFilterSupport.valueAbsent(bloomFilters, p.columnIndex(), p.value())) {
                    yield StatsDecision.CANNOT_MATCH;
                }
                yield decision;
            }
            case ResolvedPredicate.FloatPredicate p -> {
                StatsDecision decision = statisticsDecision(p, p.columnIndex(), rowGroup);
                if (decision != StatsDecision.CANNOT_MATCH
                        && p.op() == FilterPredicate.Operator.EQ
                        && BloomFilterSupport.valueAbsent(bloomFilters, p.columnIndex(), p.value())) {
                    yield StatsDecision.CANNOT_MATCH;
                }
                yield decision;
            }
            case ResolvedPredicate.Float16Predicate p ->
                    statisticsDecision(p, p.columnIndex(), rowGroup);
            case ResolvedPredicate.DoublePredicate p -> {
                StatsDecision decision = statisticsDecision(p, p.columnIndex(), rowGroup);
                if (decision != StatsDecision.CANNOT_MATCH
                        && p.op() == FilterPredicate.Operator.EQ
                        && BloomFilterSupport.valueAbsent(bloomFilters, p.columnIndex(), p.value())) {
                    yield StatsDecision.CANNOT_MATCH;
                }
                yield decision;
            }
            case ResolvedPredicate.BooleanPredicate p ->
                    statisticsDecision(p, p.columnIndex(), rowGroup);
            case ResolvedPredicate.BinaryPredicate p -> {
                StatsDecision decision = statisticsDecision(p, p.columnIndex(), rowGroup);
                if (decision != StatsDecision.CANNOT_MATCH
                        && p.op() == FilterPredicate.Operator.EQ
                        && BloomFilterSupport.valueAbsent(bloomFilters, p.columnIndex(), p.value())) {
                    yield StatsDecision.CANNOT_MATCH;
                }
                yield decision;
            }
            case ResolvedPredicate.IntInPredicate p -> {
                StatsDecision decision = statisticsDecision(p, p.columnIndex(), rowGroup);
                if (decision != StatsDecision.CANNOT_MATCH
                        && BloomFilterSupport.absentAll(bloomFilters, p.columnIndex(), p.values())) {
                    yield StatsDecision.CANNOT_MATCH;
                }
                yield decision;
            }
            case ResolvedPredicate.LongInPredicate p -> {
                StatsDecision decision = statisticsDecision(p, p.columnIndex(), rowGroup);
                if (decision != StatsDecision.CANNOT_MATCH
                        && BloomFilterSupport.absentAll(bloomFilters, p.columnIndex(), p.values())) {
                    yield StatsDecision.CANNOT_MATCH;
                }
                yield decision;
            }
            case ResolvedPredicate.BinaryInPredicate p -> {
                StatsDecision decision = statisticsDecision(p, p.columnIndex(), rowGroup);
                if (decision != StatsDecision.CANNOT_MATCH
                        && BloomFilterSupport.absentAll(bloomFilters, p.columnIndex(), p.values())) {
                    yield StatsDecision.CANNOT_MATCH;
                }
                yield decision;
            }
            case ResolvedPredicate.IsNullPredicate p -> {
                Statistics stats = getStatistics(p.columnIndex(), rowGroup);
                // Can drop IS NULL if nullCount is known to be 0 (no nulls exist).
                // The always-matching dual (every row null) is deliberately not derived:
                // for nested columns the null count tallies leaf values, not rows, so
                // nullCount == numRows does not prove every row's leaf is null.
                yield stats != null && stats.nullCount() != null && stats.nullCount() == 0
                        ? StatsDecision.CANNOT_MATCH
                        : StatsDecision.MIGHT_MATCH;
            }
            case ResolvedPredicate.IsNotNullPredicate p -> {
                Statistics stats = getStatistics(p.columnIndex(), rowGroup);
                if (stats == null || stats.nullCount() == null) {
                    yield StatsDecision.MIGHT_MATCH;
                }
                // Can drop IS NOT NULL if all values are null (nullCount == numRows)
                if (stats.nullCount() == rowGroup.numRows()) {
                    yield StatsDecision.CANNOT_MATCH;
                }
                yield stats.nullCount() == 0
                        ? StatsDecision.ALWAYS_MATCHES
                        : StatsDecision.MIGHT_MATCH;
            }
            case ResolvedPredicate.And a -> {
                if (a.children().isEmpty()) {
                    yield StatsDecision.MIGHT_MATCH;
                }
                StatsDecision result = StatsDecision.ALWAYS_MATCHES;
                for (ResolvedPredicate child : a.children()) {
                    result = StatsDecision.and(result, decideRowGroup(child, rowGroup, bloomFilters));
                    if (result == StatsDecision.CANNOT_MATCH) {
                        break;
                    }
                }
                yield result;
            }
            case ResolvedPredicate.Or o -> {
                if (o.children().isEmpty()) {
                    yield StatsDecision.MIGHT_MATCH;
                }
                StatsDecision result = StatsDecision.CANNOT_MATCH;
                for (ResolvedPredicate child : o.children()) {
                    result = StatsDecision.or(result, decideRowGroup(child, rowGroup, bloomFilters));
                    if (result == StatsDecision.ALWAYS_MATCHES) {
                        break;
                    }
                }
                yield result;
            }
            case ResolvedPredicate.GeospatialPredicate p -> {
                ColumnMetaData cmd = rowGroup.columns().get(p.columnIndex()).metaData();
                GeospatialStatistics geospatialStatistics = cmd.geospatialStatistics();
                if (geospatialStatistics == null || geospatialStatistics.bbox() == null) {
                    yield StatsDecision.MIGHT_MATCH; // no stats, can't drop
                }
                BoundingBox bbox = geospatialStatistics.bbox();
                yield !Geospatial.xAxisOverlaps(bbox.xmin(), bbox.xmax(), p.xmin(), p.xmax()) ||
                        bbox.ymax() < p.ymin() ||
                        bbox.ymin() > p.ymax()
                        ? StatsDecision.CANNOT_MATCH
                        : StatsDecision.MIGHT_MATCH;
            }
        };
    }

    /// The column's min/max statistics decision for the leaf, [StatsDecision#MIGHT_MATCH]
    /// when statistics are absent.
    private static StatsDecision statisticsDecision(ResolvedPredicate leaf, int columnIndex,
            RowGroup rowGroup) {
        Statistics stats = getStatistics(columnIndex, rowGroup);
        return stats == null
                ? StatsDecision.MIGHT_MATCH
                : StatisticsFilterSupport.decideLeaf(leaf, MinMaxStats.of(stats));
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
