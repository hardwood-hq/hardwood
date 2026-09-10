/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.io.IOException;

import dev.hardwood.internal.bloomfilter.BloomFilter;
import dev.hardwood.internal.predicate.dictionary.DictionaryFilterSupport;
import dev.hardwood.internal.predicate.dictionary.RowGroupDictionaryFilterSource;
import dev.hardwood.internal.reader.Dictionary;
import dev.hardwood.internal.util.Geospatial;
import dev.hardwood.metadata.BoundingBox;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.GeospatialStatistics;
import dev.hardwood.metadata.RowGroup;
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
    /// @param logContext where this row group is, for the warning raised when a column's
    ///        statistics bounds turn out to be unusable
    /// @return the statistics decision for the row group
    public static FilterDecision decideRowGroup(ResolvedPredicate predicate, RowGroup rowGroup,
            BloomFilterSource bloomFilters, RowGroupDictionaryFilterSource dictionaries,
            LogContext logContext, BoundsReadability readability) throws IOException {
        return decide(predicate, rowGroup, bloomFilters, dictionaries, logContext, readability);
    }

    /// The recursive body of [#decideRowGroup], carrying where the row group is so that a
    /// column whose bounds turn out to be unusable can be named.
    private static FilterDecision decide(ResolvedPredicate predicate, RowGroup rowGroup,
            BloomFilterSource bloomFilters, RowGroupDictionaryFilterSource dictionaries,
            LogContext logContext, BoundsReadability readability) throws IOException {
        return switch (predicate) {
            case ResolvedPredicate.IntPredicate p -> intEqualityDecision(p, p.op(), p.value(),
                    rowGroup, bloomFilters, dictionaries, logContext, readability);
            case ResolvedPredicate.UnsignedIntPredicate p -> intEqualityDecision(p, p.op(), p.value(),
                    rowGroup, bloomFilters, dictionaries, logContext, readability);
            case ResolvedPredicate.LongPredicate p -> longEqualityDecision(p, p.op(), p.value(),
                    rowGroup, bloomFilters, dictionaries, logContext, readability);
            case ResolvedPredicate.UnsignedLongPredicate p -> longEqualityDecision(p, p.op(), p.value(),
                    rowGroup, bloomFilters, dictionaries, logContext, readability);
            case ResolvedPredicate.FloatPredicate p -> {
                FilterDecision decision = statisticsDecision(p, rowGroup, logContext, p.columnIndex(), readability);
                if (decision != FilterDecision.CANNOT_MATCH
                        && p.op() == FilterPredicate.Operator.EQ
                        // The NaN case is repeated from BloomFilterSupport so the filter is not
                        // read for a value it could not decide either way.
                        && ((!Float.isNaN(p.value())
                                && BloomFilterSupport.valueAbsent(bloom(bloomFilters, p.columnIndex()), p.value()))
                                || DictionaryFilterSupport.valueAbsent(dictionary(dictionaries, p.columnIndex()), p.value()))) {
                    yield FilterDecision.CANNOT_MATCH;
                }
                yield decision;
            }
            // No bloom check: a bloom filter hashes the 2-byte stored form, so probing it would
            // mean narrowing the predicate to binary16 first — lossy, and a probe rounded to a
            // neighbouring value would prove the wrong one absent. The dictionary holds the
            // stored values, so its entries can be widened instead and compared exactly.
            case ResolvedPredicate.Float16Predicate p -> {
                FilterDecision decision = statisticsDecision(p, rowGroup, logContext, p.columnIndex(), readability);
                if (decision != FilterDecision.CANNOT_MATCH
                        && p.op() == FilterPredicate.Operator.EQ
                        && DictionaryFilterSupport.valueAbsentFloat16(dictionary(dictionaries, p.columnIndex()), p.value())) {
                    yield FilterDecision.CANNOT_MATCH;
                }
                yield decision;
            }
            case ResolvedPredicate.Float16InPredicate p -> {
                FilterDecision decision = statisticsDecision(p, rowGroup, logContext, p.columnIndex(), readability);
                // As for Float16Predicate: the dictionary holds the chunk's exact halves and decides
                // membership. A FLOAT16 column's Bloom filter hashes its two stored bytes and is not
                // consulted for a numeric probe.
                if (decision != FilterDecision.CANNOT_MATCH
                        && DictionaryFilterSupport.absentAllFloat16(dictionary(dictionaries, p.columnIndex()), p.values())) {
                    yield FilterDecision.CANNOT_MATCH;
                }
                yield decision;
            }
            case ResolvedPredicate.DoublePredicate p -> {
                FilterDecision decision = statisticsDecision(p, rowGroup, logContext, p.columnIndex(), readability);
                if (decision != FilterDecision.CANNOT_MATCH
                        && p.op() == FilterPredicate.Operator.EQ
                        // The NaN case is repeated from BloomFilterSupport so the filter is not
                        // read for a value it could not decide either way.
                        && ((!Double.isNaN(p.value())
                                && BloomFilterSupport.valueAbsent(bloom(bloomFilters, p.columnIndex()), p.value()))
                                || DictionaryFilterSupport.valueAbsent(dictionary(dictionaries, p.columnIndex()), p.value()))) {
                    yield FilterDecision.CANNOT_MATCH;
                }
                yield decision;
            }
            case ResolvedPredicate.BooleanPredicate p ->
                    statisticsDecision(p, rowGroup, logContext, p.columnIndex(), readability);
            case ResolvedPredicate.BinaryPredicate p -> {
                FilterDecision decision = statisticsDecision(p, rowGroup, logContext, p.columnIndex(), readability);
                // Bloom filters and dictionary membership answer "are these exact bytes here?",
                // which stands in for "is this value here?" only where the value has one
                // encoding — see BinaryPredicate#byteExact. Statistics compare in the column's
                // order and stay available either way.
                if (decision != FilterDecision.CANNOT_MATCH
                        && p.op() == FilterPredicate.Operator.EQ
                        && p.byteExact()
                        && (BloomFilterSupport.valueAbsent(bloom(bloomFilters, p.columnIndex()), p.value())
                                || DictionaryFilterSupport.valueAbsent(dictionary(dictionaries, p.columnIndex()), p.value()))) {
                    yield FilterDecision.CANNOT_MATCH;
                }
                yield decision;
            }
            case ResolvedPredicate.IntInPredicate p -> intInDecision(p, p.values(),
                    rowGroup, bloomFilters, dictionaries, logContext, readability);
            case ResolvedPredicate.UnsignedIntInPredicate p -> intInDecision(p, p.values(),
                    rowGroup, bloomFilters, dictionaries, logContext, readability);
            case ResolvedPredicate.LongInPredicate p -> longInDecision(p, p.values(),
                    rowGroup, bloomFilters, dictionaries, logContext, readability);
            case ResolvedPredicate.UnsignedLongInPredicate p -> longInDecision(p, p.values(),
                    rowGroup, bloomFilters, dictionaries, logContext, readability);
            case ResolvedPredicate.BinaryInPredicate p -> {
                FilterDecision decision = statisticsDecision(p, rowGroup, logContext, p.columnIndex(), readability);
                // As for BinaryPredicate: exact-byte shortcuts stand in for membership only where
                // a value has one encoding.
                if (decision != FilterDecision.CANNOT_MATCH
                        && p.byteExact()
                        && (BloomFilterSupport.absentAll(bloom(bloomFilters, p.columnIndex()), p.values())
                                || DictionaryFilterSupport.absentAll(dictionary(dictionaries, p.columnIndex()), p.values()))) {
                    yield FilterDecision.CANNOT_MATCH;
                }
                yield decision;
            }
            case ResolvedPredicate.DoubleInPredicate p -> {
                FilterDecision decision = statisticsDecision(p, rowGroup, logContext, p.columnIndex(), readability);
                if (decision != FilterDecision.CANNOT_MATCH
                        // The NaN case is repeated from BloomFilterSupport so the filter is not
                        // read for a list it could not decide either way. The dictionary holds the
                        // chunk's exact values, so it decides a NaN probe and stays in play.
                        && ((!BloomFilterSupport.anyNaN(p.values())
                                && BloomFilterSupport.absentAll(
                                        bloom(bloomFilters, p.columnIndex()), p.values(), p.floatColumn()))
                                || DictionaryFilterSupport.absentAll(
                                        dictionary(dictionaries, p.columnIndex()), p.values(), p.floatColumn()))) {
                    yield FilterDecision.CANNOT_MATCH;
                }
                yield decision;
            }
            case ResolvedPredicate.IsNullPredicate p ->
                    statisticsDecision(p, rowGroup, logContext, p.columnIndex(), readability);
            case ResolvedPredicate.IsNotNullPredicate p ->
                    statisticsDecision(p, rowGroup, logContext, p.columnIndex(), readability);
            case ResolvedPredicate.And a -> {
                if (a.children().isEmpty()) {
                    yield FilterDecision.MIGHT_MATCH;
                }
                FilterDecision result = FilterDecision.ALWAYS_MATCHES;
                for (ResolvedPredicate child : a.children()) {
                    result = FilterDecision.and(result, decide(child, rowGroup, bloomFilters, dictionaries, logContext, readability));
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
                    result = FilterDecision.or(result, decide(child, rowGroup, bloomFilters, dictionaries, logContext, readability));
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

    /// The row-group decision for an integer comparison: the statistics verdict, sharpened by the
    /// bloom filter and the dictionary when the operator is equality.
    ///
    /// Signed and unsigned columns share this, because both shortcuts answer "are these exact bits
    /// stored?" and the bits of a literal do not depend on how the column orders them. Only the
    /// statistics half reads an order, and the leaf's own [MinMaxStats] variant supplies it.
    private static FilterDecision intEqualityDecision(ResolvedPredicate leaf,
            FilterPredicate.Operator op, int value, RowGroup rowGroup, BloomFilterSource bloomFilters,
            RowGroupDictionaryFilterSource dictionaries, LogContext logContext, BoundsReadability readability) throws IOException {
        int columnIndex = ResolvedPredicate.leafColumnIndex(leaf);
        FilterDecision decision = statisticsDecision(leaf, rowGroup, logContext, columnIndex, readability);
        if (decision != FilterDecision.CANNOT_MATCH
                && op == FilterPredicate.Operator.EQ
                && (BloomFilterSupport.valueAbsent(bloom(bloomFilters, columnIndex), value)
                        || DictionaryFilterSupport.valueAbsent(dictionary(dictionaries, columnIndex), value))) {
            return FilterDecision.CANNOT_MATCH;
        }
        return decision;
    }

    /// [#intEqualityDecision] for an `INT64` column.
    private static FilterDecision longEqualityDecision(ResolvedPredicate leaf,
            FilterPredicate.Operator op, long value, RowGroup rowGroup, BloomFilterSource bloomFilters,
            RowGroupDictionaryFilterSource dictionaries, LogContext logContext, BoundsReadability readability) throws IOException {
        int columnIndex = ResolvedPredicate.leafColumnIndex(leaf);
        FilterDecision decision = statisticsDecision(leaf, rowGroup, logContext, columnIndex, readability);
        if (decision != FilterDecision.CANNOT_MATCH
                && op == FilterPredicate.Operator.EQ
                && (BloomFilterSupport.valueAbsent(bloom(bloomFilters, columnIndex), value)
                        || DictionaryFilterSupport.valueAbsent(dictionary(dictionaries, columnIndex), value))) {
            return FilterDecision.CANNOT_MATCH;
        }
        return decision;
    }

    /// [#intEqualityDecision] for an `INT32` membership test, where every probe must be absent
    /// before the row group can be dropped.
    private static FilterDecision intInDecision(ResolvedPredicate leaf, int[] values, RowGroup rowGroup,
            BloomFilterSource bloomFilters, RowGroupDictionaryFilterSource dictionaries,
            LogContext logContext, BoundsReadability readability) throws IOException {
        int columnIndex = ResolvedPredicate.leafColumnIndex(leaf);
        FilterDecision decision = statisticsDecision(leaf, rowGroup, logContext, columnIndex, readability);
        if (decision != FilterDecision.CANNOT_MATCH
                && (BloomFilterSupport.absentAll(bloom(bloomFilters, columnIndex), values)
                        || DictionaryFilterSupport.absentAll(dictionary(dictionaries, columnIndex), values))) {
            return FilterDecision.CANNOT_MATCH;
        }
        return decision;
    }

    /// [#intInDecision] for an `INT64` column.
    private static FilterDecision longInDecision(ResolvedPredicate leaf, long[] values, RowGroup rowGroup,
            BloomFilterSource bloomFilters, RowGroupDictionaryFilterSource dictionaries,
            LogContext logContext, BoundsReadability readability) throws IOException {
        int columnIndex = ResolvedPredicate.leafColumnIndex(leaf);
        FilterDecision decision = statisticsDecision(leaf, rowGroup, logContext, columnIndex, readability);
        if (decision != FilterDecision.CANNOT_MATCH
                && (BloomFilterSupport.absentAll(bloom(bloomFilters, columnIndex), values)
                        || DictionaryFilterSupport.absentAll(dictionary(dictionaries, columnIndex), values))) {
            return FilterDecision.CANNOT_MATCH;
        }
        return decision;
    }

    /// What the column chunk's statistics prove about the leaf; [UnitStats#decide] says which
    /// statistic answers which leaf. [FilterDecision#MIGHT_MATCH] when the chunk carries none
    /// that apply — or when its bounds are unusable, which [MinMaxStats] decides and the unit
    /// reports.
    private static FilterDecision statisticsDecision(ResolvedPredicate leaf, RowGroup rowGroup,
            LogContext logContext, int columnIndex, BoundsReadability readability) {
        return UnitStats.ChunkStats.of(rowGroup, columnIndex, readability).decide(leaf, logContext);
    }

    /// Resolves the column's bloom filter, or `null` when no source is supplied or the column
    /// carries none. The read lives here rather than in [BloomFilterSupport] because the guards
    /// that decide whether it is worth doing — statistics did not already drop the row group, the
    /// operator is one a filter can answer — are here, and the `||` below keeps it from running
    /// when an earlier probe has already proved the value absent.
    private static BloomFilter bloom(BloomFilterSource bloomFilters, int columnIndex) throws IOException {
        return bloomFilters == null ? null : bloomFilters.forColumn(columnIndex);
    }

    /// Resolves the column's dictionary, or `null` when no source is supplied or the chunk has
    /// none. See [#bloom] for why the read is here.
    private static Dictionary dictionary(RowGroupDictionaryFilterSource dictionaries, int columnIndex)
            throws IOException {
        return dictionaries == null ? null : dictionaries.forColumn(columnIndex);
    }

}
