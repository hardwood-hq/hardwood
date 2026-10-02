/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import dev.hardwood.metadata.ColumnIndex;
import dev.hardwood.metadata.Statistics;

/// One unit's `NaN` count, and what it proves about a predicate on a floating-point leaf.
///
/// A floating-point unit's bounds describe its non-`NaN` values only, so the count is the one
/// statistic that speaks about the rest. A count of zero proves the bounds describe every
/// non-null value, which [MinMaxStats] reads through [#nanFree]. A count that covers every
/// non-null value proves the unit all-`NaN`, which needs no bounds at all: every row is either
/// null or `NaN`, so a predicate is decided by whether a `NaN` row satisfies it. A unit whose
/// writer recorded no count proves neither.
///
/// The count is held against the unit's row count, which [NullStats] carries and which counts
/// the leaf's entries for the reason given there.
///
/// @param nanCount the number of `NaN` values in the unit, or [#UNKNOWN_NAN_COUNT]
record NaNStats(long nanCount) {

    /// The `NaN` count of a unit whose source does not carry one.
    static final long UNKNOWN_NAN_COUNT = -1;

    /// The count a column chunk, or a page carrying statistics inline on its header, records.
    static NaNStats of(Statistics statistics) {
        Long nanCount = statistics.nanCount();
        return new NaNStats(nanCount == null ? UNKNOWN_NAN_COUNT : nanCount);
    }

    /// The count the [ColumnIndex] records for page `pageIndex`.
    static NaNStats ofPage(ColumnIndex columnIndex, int pageIndex) {
        long[] nanCounts = columnIndex.nanCounts();
        return new NaNStats(nanCounts == null ? UNKNOWN_NAN_COUNT : nanCounts[pageIndex]);
    }

    /// Whether the unit is proven to hold no `NaN`.
    boolean nanFree() {
        return nanCount == 0;
    }

    /// Whether every non-null value of the unit is proven to be `NaN`. Where the null count is
    /// unknown, only a `NaN` count equal to the row count proves it, which also proves the unit
    /// holds no nulls.
    boolean allNaN(NullStats nulls) {
        if (nanCount <= 0 || nulls.rowCount() == UnitStats.UNKNOWN_ROW_COUNT) {
            return false;
        }
        long nullCount = nulls.nullCount() == NullStats.UNKNOWN_NULL_COUNT ? 0 : nulls.nullCount();
        return nanCount + nullCount == nulls.rowCount();
    }

    /// What an all-`NaN` unit proves about a floating-point leaf: no row matches where a `NaN`
    /// row fails it, since every other row is null and a null satisfies no value predicate, and
    /// every row matches where a `NaN` row satisfies it and the unit holds no nulls.
    ///
    /// @param leaf a floating-point comparison or `IN` list
    FilterDecision decideAllNaN(ResolvedPredicate leaf, NullStats nulls) {
        if (!naNRowSatisfies(leaf)) {
            return FilterDecision.CANNOT_MATCH;
        }
        return nanCount == nulls.rowCount() ? FilterDecision.ALWAYS_MATCHES : FilterDecision.MIGHT_MATCH;
    }

    private static boolean naNRowSatisfies(ResolvedPredicate leaf) {
        return switch (leaf) {
            case ResolvedPredicate.FloatPredicate p ->
                    StatisticsFilterSupport.naNRowSatisfies(p.op(), Float.isNaN(p.value()));
            case ResolvedPredicate.Float16Predicate p ->
                    StatisticsFilterSupport.naNRowSatisfies(p.op(), Float.isNaN(p.value()));
            case ResolvedPredicate.DoublePredicate p ->
                    StatisticsFilterSupport.naNRowSatisfies(p.op(), Double.isNaN(p.value()));
            case ResolvedPredicate.FloatInPredicate p -> StatisticsFilterSupport.containsNaN(p.values());
            case ResolvedPredicate.Float16InPredicate p -> StatisticsFilterSupport.containsNaN(p.values());
            case ResolvedPredicate.DoubleInPredicate p -> StatisticsFilterSupport.containsNaN(p.values());
            default -> throw new IllegalArgumentException(
                    "A NaN count cannot decide a " + leaf.getClass().getSimpleName());
        };
    }
}
