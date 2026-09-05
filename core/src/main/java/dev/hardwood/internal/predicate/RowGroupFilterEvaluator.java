/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import dev.hardwood.internal.predicate.dictionary.DictionaryFilterSupport;
import dev.hardwood.internal.predicate.dictionary.RowGroupDictionaryFilterSource;
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
/// only prove absence, so it never upgrades a decision to [FilterDecision#ALWAYS_MATCHES].
///
/// The same leaves consult the column's dictionary when a [RowGroupDictionaryFilterSource] is
/// supplied. A dictionary only proves absence for a column whose every data page is
/// dictionary-encoded, in which case the dictionary holds every non-null value in the chunk and a
/// value missing from it cannot occur in any row.
/// All three sources are independent: whichever proves no match first drops the row group.
public class RowGroupFilterEvaluator {

    /// Determines whether a row group can be skipped using statistics only (no bloom filters and no
    /// dictionaries).
    ///
    /// @param predicate the resolved predicate to evaluate
    /// @param rowGroup the row group to check
    /// @return `true` if the row group can be safely skipped (no matching rows),
    ///         `false` if it may contain matching rows
    public static boolean canDropRowGroup(ResolvedPredicate predicate, RowGroup rowGroup) {
        return canDropRowGroup(predicate, rowGroup, null);
    }

    /// Determines whether a row group can be skipped based on the given resolved predicate,
    /// consulting bloom filters but no dictionaries.
    ///
    /// @param predicate the resolved predicate to evaluate
    /// @param rowGroup the row group to check
    /// @param bloomFilters source of the row group's bloom filters, or `null` to evaluate
    ///        statistics only
    /// @return `true` if the row group can be safely skipped (no matching rows),
    ///         `false` if it may contain matching rows
    public static boolean canDropRowGroup(ResolvedPredicate predicate, RowGroup rowGroup,
            BloomFilterSource bloomFilters) {
        return decideRowGroup(predicate, rowGroup, bloomFilters) == FilterDecision.CANNOT_MATCH;
    }

    /// Evaluates the predicate against the row group's statistics and bloom filters as a
    /// three-valued [FilterDecision], without consulting dictionaries.
    ///
    /// @param predicate the resolved predicate to evaluate
    /// @param rowGroup the row group to check
    /// @param bloomFilters source of the row group's bloom filters, or `null` to evaluate
    ///        statistics only
    /// @return the statistics decision for the row group
    public static FilterDecision decideRowGroup(ResolvedPredicate predicate, RowGroup rowGroup,
            BloomFilterSource bloomFilters) {
        return decideRowGroup(predicate, rowGroup, bloomFilters, null);
    }

    /// Evaluates the predicate against the row group's statistics, bloom filters and dictionaries
    /// as a three-valued [FilterDecision].
    ///
    /// [FilterDecision#CANNOT_MATCH] row groups can be skipped entirely;
    /// [FilterDecision#ALWAYS_MATCHES] row groups can be read with per-row predicate
    /// evaluation skipped, since statistics prove every row satisfies the predicate.
    ///
    /// @param predicate the resolved predicate to evaluate
    /// @param rowGroup the row group to check
    /// @param bloomFilters source of the row group's bloom filters, or `null` to skip the bloom
    ///        filter checks
    /// @param dictionaries source of the row group's dictionaries, or `null` to skip the dictionary
    ///        checks
    /// @return the statistics decision for the row group
    public static FilterDecision decideRowGroup(ResolvedPredicate predicate, RowGroup rowGroup,
            BloomFilterSource bloomFilters, RowGroupDictionaryFilterSource dictionaries) {
        return switch (predicate) {
            case ResolvedPredicate.IntPredicate p -> {
                FilterDecision decision = statisticsDecision(p, p.columnIndex(), rowGroup);
                if (decision != FilterDecision.CANNOT_MATCH
                        && p.op() == FilterPredicate.Operator.EQ
                        && (BloomFilterSupport.valueAbsent(bloomFilters, p.columnIndex(), p.value())
                                || DictionaryFilterSupport.valueAbsent(dictionaries, p.columnIndex(), p.value()))) {
                    yield FilterDecision.CANNOT_MATCH;
                }
                yield decision;
            }
            case ResolvedPredicate.LongPredicate p -> {
                FilterDecision decision = statisticsDecision(p, p.columnIndex(), rowGroup);
                if (decision != FilterDecision.CANNOT_MATCH
                        && p.op() == FilterPredicate.Operator.EQ
                        && (BloomFilterSupport.valueAbsent(bloomFilters, p.columnIndex(), p.value())
                                || DictionaryFilterSupport.valueAbsent(dictionaries, p.columnIndex(), p.value()))) {
                    yield FilterDecision.CANNOT_MATCH;
                }
                yield decision;
            }
            case ResolvedPredicate.FloatPredicate p -> {
                FilterDecision decision = statisticsDecision(p, p.columnIndex(), rowGroup);
                if (decision != FilterDecision.CANNOT_MATCH
                        && p.op() == FilterPredicate.Operator.EQ
                        && (BloomFilterSupport.valueAbsent(bloomFilters, p.columnIndex(), p.value())
                                || DictionaryFilterSupport.valueAbsent(dictionaries, p.columnIndex(), p.value()))) {
                    yield FilterDecision.CANNOT_MATCH;
                }
                yield decision;
            }
            // No bloom check: a bloom filter hashes the 2-byte stored form, so probing it would
            // mean narrowing the predicate to binary16 first — lossy, and a probe rounded to a
            // neighbouring value would prove the wrong one absent. The dictionary holds the
            // stored values, so its entries can be widened instead and compared exactly.
            case ResolvedPredicate.Float16Predicate p -> {
                FilterDecision decision = statisticsDecision(p, p.columnIndex(), rowGroup);
                if (decision != FilterDecision.CANNOT_MATCH
                        && p.op() == FilterPredicate.Operator.EQ
                        && DictionaryFilterSupport.valueAbsentFloat16(dictionaries, p.columnIndex(), p.value())) {
                    yield FilterDecision.CANNOT_MATCH;
                }
                yield decision;
            }
            case ResolvedPredicate.DoublePredicate p -> {
                FilterDecision decision = statisticsDecision(p, p.columnIndex(), rowGroup);
                if (decision != FilterDecision.CANNOT_MATCH
                        && p.op() == FilterPredicate.Operator.EQ
                        && (BloomFilterSupport.valueAbsent(bloomFilters, p.columnIndex(), p.value())
                                || DictionaryFilterSupport.valueAbsent(dictionaries, p.columnIndex(), p.value()))) {
                    yield FilterDecision.CANNOT_MATCH;
                }
                yield decision;
            }
            case ResolvedPredicate.BooleanPredicate p ->
                    statisticsDecision(p, p.columnIndex(), rowGroup);
            case ResolvedPredicate.BinaryPredicate p -> {
                FilterDecision decision = statisticsDecision(p, p.columnIndex(), rowGroup);
                // Bloom filters and dictionary membership answer "are these exact bytes here?",
                // which stands in for "is this value here?" only where the value has one
                // encoding — see BinaryPredicate#byteExact. Statistics compare in the column's
                // order and stay available either way.
                if (decision != FilterDecision.CANNOT_MATCH
                        && p.op() == FilterPredicate.Operator.EQ
                        && p.byteExact()
                        && (BloomFilterSupport.valueAbsent(bloomFilters, p.columnIndex(), p.value())
                                || DictionaryFilterSupport.valueAbsent(dictionaries, p.columnIndex(), p.value()))) {
                    yield FilterDecision.CANNOT_MATCH;
                }
                yield decision;
            }
            case ResolvedPredicate.IntInPredicate p -> {
                FilterDecision decision = statisticsDecision(p, p.columnIndex(), rowGroup);
                if (decision != FilterDecision.CANNOT_MATCH
                        && (BloomFilterSupport.absentAll(bloomFilters, p.columnIndex(), p.values())
                                || DictionaryFilterSupport.absentAll(dictionaries, p.columnIndex(), p.values()))) {
                    yield FilterDecision.CANNOT_MATCH;
                }
                yield decision;
            }
            case ResolvedPredicate.LongInPredicate p -> {
                FilterDecision decision = statisticsDecision(p, p.columnIndex(), rowGroup);
                if (decision != FilterDecision.CANNOT_MATCH
                        && (BloomFilterSupport.absentAll(bloomFilters, p.columnIndex(), p.values())
                                || DictionaryFilterSupport.absentAll(dictionaries, p.columnIndex(), p.values()))) {
                    yield FilterDecision.CANNOT_MATCH;
                }
                yield decision;
            }
            case ResolvedPredicate.BinaryInPredicate p -> {
                FilterDecision decision = statisticsDecision(p, p.columnIndex(), rowGroup);
                if (decision != FilterDecision.CANNOT_MATCH
                        && (BloomFilterSupport.absentAll(bloomFilters, p.columnIndex(), p.values())
                                || DictionaryFilterSupport.absentAll(dictionaries, p.columnIndex(), p.values()))) {
                    yield FilterDecision.CANNOT_MATCH;
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
                        ? FilterDecision.CANNOT_MATCH
                        : FilterDecision.MIGHT_MATCH;
            }
            case ResolvedPredicate.IsNotNullPredicate p -> {
                Statistics stats = getStatistics(p.columnIndex(), rowGroup);
                if (stats == null || stats.nullCount() == null) {
                    yield FilterDecision.MIGHT_MATCH;
                }
                // Can drop IS NOT NULL if all values are null (nullCount == numRows)
                if (stats.nullCount() == rowGroup.numRows()) {
                    yield FilterDecision.CANNOT_MATCH;
                }
                yield stats.nullCount() == 0
                        ? FilterDecision.ALWAYS_MATCHES
                        : FilterDecision.MIGHT_MATCH;
            }
            case ResolvedPredicate.And a -> {
                if (a.children().isEmpty()) {
                    yield FilterDecision.MIGHT_MATCH;
                }
                FilterDecision result = FilterDecision.ALWAYS_MATCHES;
                for (ResolvedPredicate child : a.children()) {
                    result = FilterDecision.and(result, decideRowGroup(child, rowGroup, bloomFilters, dictionaries));
                    if (result == FilterDecision.CANNOT_MATCH) {
                        break;
                    }
                }
                yield result;
            }
            case ResolvedPredicate.Or o -> {
                if (o.children().isEmpty()) {
                    yield FilterDecision.MIGHT_MATCH;
                }
                FilterDecision result = FilterDecision.CANNOT_MATCH;
                for (ResolvedPredicate child : o.children()) {
                    result = FilterDecision.or(result, decideRowGroup(child, rowGroup, bloomFilters, dictionaries));
                    if (result == FilterDecision.ALWAYS_MATCHES) {
                        break;
                    }
                }
                yield result;
            }
            case ResolvedPredicate.GeospatialPredicate p -> {
                ColumnMetaData cmd = rowGroup.columns().get(p.columnIndex()).metaData();
                GeospatialStatistics geospatialStatistics = cmd.geospatialStatistics();
                if (geospatialStatistics == null || geospatialStatistics.bbox() == null) {
                    yield FilterDecision.MIGHT_MATCH; // no stats, can't drop
                }
                BoundingBox bbox = geospatialStatistics.bbox();
                yield !Geospatial.xAxisOverlaps(bbox.xmin(), bbox.xmax(), p.xmin(), p.xmax()) ||
                        bbox.ymax() < p.ymin() ||
                        bbox.ymin() > p.ymax()
                        ? FilterDecision.CANNOT_MATCH
                        : FilterDecision.MIGHT_MATCH;
            }
        };
    }

    /// The column's min/max statistics decision for the leaf, [FilterDecision#MIGHT_MATCH]
    /// when statistics are absent.
    private static FilterDecision statisticsDecision(ResolvedPredicate leaf, int columnIndex,
            RowGroup rowGroup) {
        Statistics stats = getStatistics(columnIndex, rowGroup);
        return stats == null
                ? FilterDecision.MIGHT_MATCH
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
