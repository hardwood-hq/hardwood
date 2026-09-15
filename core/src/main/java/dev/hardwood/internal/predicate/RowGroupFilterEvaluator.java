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

/// Evaluates filter predicates against row group statistics, bloom filters and dictionaries to
/// determine whether a row group can be skipped — or read without per-row filtering.
///
/// A leaf is decided in two steps. Its column chunk's statistics answer first, through
/// [UnitStats#decide], which is the same decision [PageFilterEvaluator] asks of a page. The
/// absence probes follow: a bloom filter or the chunk's dictionary can prove the leaf's literal
/// absent from every row, which drops a row group whose bounds could not. Neither proves the
/// dual, so neither ever raises a decision to [FilterDecision#ALWAYS_MATCHES].
///
/// The approach is conservative throughout: a column whose statistics are absent, partial or
/// unusable is never dropped and never promised to match in full.
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
            case ResolvedPredicate.And a -> {
                if (a.children().isEmpty()) {
                    yield FilterDecision.MIGHT_MATCH;
                }
                FilterDecision result = FilterDecision.ALWAYS_MATCHES;
                for (ResolvedPredicate child : a.children()) {
                    result = FilterDecision.and(result,
                            decide(child, rowGroup, bloomFilters, dictionaries, logContext, readability));
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
                    result = FilterDecision.or(result,
                            decide(child, rowGroup, bloomFilters, dictionaries, logContext, readability));
                    if (result == FilterDecision.ALWAYS_MATCHES) {
                        break;
                    }
                }
                yield result;
            }
            // Geospatial statistics are no statistic of a unit — Parquet carries them per column
            // chunk alone — so [UnitStats] reports them unknown and the bounding box is read here.
            case ResolvedPredicate.GeospatialPredicate p -> geospatialDecision(p, rowGroup);
            default -> leafDecision(predicate, rowGroup, bloomFilters, dictionaries, logContext, readability);
        };
    }

    /// What one leaf's column chunk proves: its statistics, and then the absence probes, which
    /// can only sharpen the answer to [FilterDecision#CANNOT_MATCH].
    ///
    /// A leaf the statistics already drop reaches no probe, which is what keeps a bloom filter or
    /// a dictionary from being read for a row group that is going anyway.
    private static FilterDecision leafDecision(ResolvedPredicate leaf, RowGroup rowGroup,
            BloomFilterSource bloomFilters, RowGroupDictionaryFilterSource dictionaries,
            LogContext logContext, BoundsReadability readability) throws IOException {
        FilterDecision decision = UnitStats.ChunkStats
                .of(rowGroup, ResolvedPredicate.leafColumnIndex(leaf), readability)
                .decide(leaf, logContext);
        if (decision == FilterDecision.CANNOT_MATCH) {
            return decision;
        }
        return absent(leaf, bloomFilters, dictionaries) ? FilterDecision.CANNOT_MATCH : decision;
    }

    /// Whether a bloom filter or the chunk's dictionary proves the leaf's literal absent from
    /// every row of the chunk.
    ///
    /// Both answer "are these exact stored bytes here?", so both are read only where that
    /// question decides the leaf: an equality or a membership test, and on a binary column only
    /// where a value has a single encoding — see [ResolvedPredicate.BinaryPredicate.Comparison].
    /// The two are independent, and the `||` keeps the dictionary unread where the filter has
    /// already proved the literal absent.
    ///
    /// Every leaf type is named rather than reached by a `default`, so a new one does not compile
    /// until it states what proves its literal absent, `false` where nothing does.
    private static boolean absent(ResolvedPredicate leaf, BloomFilterSource bloomFilters,
            RowGroupDictionaryFilterSource dictionaries) throws IOException {
        return switch (leaf) {
            // Signed and unsigned columns probe alike: both shortcuts test stored bits, and the
            // bits of a literal do not depend on how the column orders them.
            case ResolvedPredicate.IntPredicate p -> p.op() == FilterPredicate.Operator.EQ
                    && (BloomFilterSupport.valueAbsent(bloom(bloomFilters, p.columnIndex()), p.value())
                            || DictionaryFilterSupport.valueAbsent(dictionary(dictionaries, p.columnIndex()), p.value()));
            case ResolvedPredicate.UnsignedIntPredicate p -> p.op() == FilterPredicate.Operator.EQ
                    && (BloomFilterSupport.valueAbsent(bloom(bloomFilters, p.columnIndex()), p.value())
                            || DictionaryFilterSupport.valueAbsent(dictionary(dictionaries, p.columnIndex()), p.value()));
            case ResolvedPredicate.LongPredicate p -> p.op() == FilterPredicate.Operator.EQ
                    && (BloomFilterSupport.valueAbsent(bloom(bloomFilters, p.columnIndex()), p.value())
                            || DictionaryFilterSupport.valueAbsent(dictionary(dictionaries, p.columnIndex()), p.value()));
            case ResolvedPredicate.UnsignedLongPredicate p -> p.op() == FilterPredicate.Operator.EQ
                    && (BloomFilterSupport.valueAbsent(bloom(bloomFilters, p.columnIndex()), p.value())
                            || DictionaryFilterSupport.valueAbsent(dictionary(dictionaries, p.columnIndex()), p.value()));
            // The NaN case is repeated from BloomFilterSupport so the filter is not read for a
            // value it could not decide either way. The dictionary holds the chunk's exact values,
            // so it decides a NaN probe and stays in play.
            case ResolvedPredicate.FloatPredicate p -> p.op() == FilterPredicate.Operator.EQ
                    && ((!Float.isNaN(p.value())
                            && BloomFilterSupport.valueAbsent(bloom(bloomFilters, p.columnIndex()), p.value()))
                            || DictionaryFilterSupport.valueAbsent(dictionary(dictionaries, p.columnIndex()), p.value()));
            case ResolvedPredicate.DoublePredicate p -> p.op() == FilterPredicate.Operator.EQ
                    && ((!Double.isNaN(p.value())
                            && BloomFilterSupport.valueAbsent(bloom(bloomFilters, p.columnIndex()), p.value()))
                            || DictionaryFilterSupport.valueAbsent(dictionary(dictionaries, p.columnIndex()), p.value()));
            // No bloom check: a bloom filter hashes the 2-byte stored form, so probing it would
            // mean narrowing the predicate to binary16 first — lossy, and a probe rounded to a
            // neighbouring value would prove the wrong one absent. The dictionary holds the stored
            // values, so its entries can be widened instead and compared exactly.
            case ResolvedPredicate.Float16Predicate p -> p.op() == FilterPredicate.Operator.EQ
                    && DictionaryFilterSupport.valueAbsentFloat16(dictionary(dictionaries, p.columnIndex()), p.value());
            case ResolvedPredicate.BinaryPredicate p -> p.op() == FilterPredicate.Operator.EQ
                    && p.byteExact()
                    && (BloomFilterSupport.valueAbsent(bloom(bloomFilters, p.columnIndex()), p.value())
                            || DictionaryFilterSupport.valueAbsent(dictionary(dictionaries, p.columnIndex()), p.value()));
            // A membership test drops the row group only where every probe is absent.
            case ResolvedPredicate.IntInPredicate p ->
                    BloomFilterSupport.absentAll(bloom(bloomFilters, p.columnIndex()), p.values())
                            || DictionaryFilterSupport.absentAll(dictionary(dictionaries, p.columnIndex()), p.values());
            case ResolvedPredicate.UnsignedIntInPredicate p ->
                    BloomFilterSupport.absentAll(bloom(bloomFilters, p.columnIndex()), p.values())
                            || DictionaryFilterSupport.absentAll(dictionary(dictionaries, p.columnIndex()), p.values());
            case ResolvedPredicate.LongInPredicate p ->
                    BloomFilterSupport.absentAll(bloom(bloomFilters, p.columnIndex()), p.values())
                            || DictionaryFilterSupport.absentAll(dictionary(dictionaries, p.columnIndex()), p.values());
            case ResolvedPredicate.UnsignedLongInPredicate p ->
                    BloomFilterSupport.absentAll(bloom(bloomFilters, p.columnIndex()), p.values())
                            || DictionaryFilterSupport.absentAll(dictionary(dictionaries, p.columnIndex()), p.values());
            case ResolvedPredicate.BinaryInPredicate p -> p.byteExact()
                    && (BloomFilterSupport.absentAll(bloom(bloomFilters, p.columnIndex()), p.values())
                            || DictionaryFilterSupport.absentAll(dictionary(dictionaries, p.columnIndex()), p.values()));
            case ResolvedPredicate.FloatInPredicate p ->
                    (!BloomFilterSupport.anyNaN(p.values())
                            && BloomFilterSupport.absentAll(bloom(bloomFilters, p.columnIndex()), p.values()))
                            || DictionaryFilterSupport.absentAll(dictionary(dictionaries, p.columnIndex()), p.values());
            case ResolvedPredicate.DoubleInPredicate p ->
                    (!BloomFilterSupport.anyNaN(p.values())
                            && BloomFilterSupport.absentAll(bloom(bloomFilters, p.columnIndex()), p.values()))
                            || DictionaryFilterSupport.absentAll(dictionary(dictionaries, p.columnIndex()), p.values());
            // As for Float16Predicate: the dictionary holds the chunk's exact halves and decides
            // membership.
            case ResolvedPredicate.Float16InPredicate p ->
                    DictionaryFilterSupport.absentAllFloat16(dictionary(dictionaries, p.columnIndex()), p.values());
            // A BOOLEAN column holds two values, which its min/max already decide between.
            case ResolvedPredicate.BooleanPredicate ignored -> false;
            // These name no literal to look for: a null predicate asks about absence itself, and
            // the constants are decided where the statistics are.
            case ResolvedPredicate.IsNullPredicate ignored -> false;
            case ResolvedPredicate.IsNotNullPredicate ignored -> false;
            case ResolvedPredicate.EveryNonNullRowPredicate ignored -> false;
            case ResolvedPredicate.NoRowPredicate ignored -> false;
            // Geospatial statistics are read before a leaf reaches a probe.
            case ResolvedPredicate.GeospatialPredicate ignored -> false;
            case ResolvedPredicate.And ignored -> throw notALeaf(leaf);
            case ResolvedPredicate.Or ignored -> throw notALeaf(leaf);
        };
    }

    /// The chunk's geospatial bounding box against the predicate's, or
    /// [FilterDecision#MIGHT_MATCH] where the row group records none for the column.
    private static FilterDecision geospatialDecision(ResolvedPredicate.GeospatialPredicate predicate,
            RowGroup rowGroup) {
        int columnIndex = predicate.columnIndex();
        ColumnMetaData metaData = columnIndex >= 0 && columnIndex < rowGroup.columns().size()
                ? rowGroup.columns().get(columnIndex).metaData()
                : null;
        GeospatialStatistics geospatialStatistics =
                metaData == null ? null : metaData.geospatialStatistics();
        if (geospatialStatistics == null || geospatialStatistics.bbox() == null) {
            return FilterDecision.MIGHT_MATCH;
        }
        BoundingBox bbox = geospatialStatistics.bbox();
        return !Geospatial.xAxisOverlaps(bbox.xmin(), bbox.xmax(), predicate.xmin(), predicate.xmax())
                || bbox.ymax() < predicate.ymin()
                || bbox.ymin() > predicate.ymax()
                        ? FilterDecision.CANNOT_MATCH
                        : FilterDecision.MIGHT_MATCH;
    }

    /// Reports a compound predicate reaching the per-leaf probes. [#decide] folds `AND` and `OR`
    /// before either is asked about a literal, so this is a wiring mistake rather than anything a
    /// file can cause.
    private static IllegalArgumentException notALeaf(ResolvedPredicate predicate) {
        return new IllegalArgumentException(
                "Absence probes decide a leaf predicate, not " + predicate.getClass().getSimpleName());
    }

    /// Resolves the column's bloom filter, or `null` when no source is supplied or the column
    /// carries none. The read lives here rather than in [BloomFilterSupport] because the guards
    /// that decide whether it is worth doing — statistics did not already drop the row group, the
    /// operator is one a filter can answer — are here, and the `||` above keeps it from running
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
