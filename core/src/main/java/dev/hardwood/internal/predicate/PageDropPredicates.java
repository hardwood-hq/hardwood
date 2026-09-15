/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dev.hardwood.metadata.Statistics;

/// Collects leaf predicates safe to use for per-page drop decisions in
/// [dev.hardwood.internal.reader.SequentialFetchPlan] when only inline page
/// statistics are available.
///
/// A leaf is **AND-necessary** — i.e. falsifying it falsifies the whole
/// predicate — iff the path from the predicate root to the leaf consists only
/// of `AND` nodes. Leaves under any `OR` branch are excluded because their
/// falsification leaves sibling branches free to match.
///
/// Since there is no `Not` in the [ResolvedPredicate] AST (negation is
/// desugared by [ResolvedPredicate#negate]), the walk is a straightforward
/// recursive descent: recurse into `And` children, skip `Or` subtrees, keep
/// leaves.
///
/// A page this path drops is replaced by a page of nulls, which the per-row filter then rejects,
/// so only a leaf a null row fails may drop one. `IS NULL` is the one leaf a null row satisfies,
/// and it is left out of the result for that reason.
public final class PageDropPredicates {

    /// The row count of a page whose header does not carry one: a v1 `DataPageHeader` counts
    /// values, which are rows only for a column with no repeated node above it. See
    /// [UnitStats#UNKNOWN_ROW_COUNT].
    public static final long UNKNOWN_ROW_COUNT = UnitStats.UNKNOWN_ROW_COUNT;

    private PageDropPredicates() {
    }

    /// Returns AND-necessary leaves organised by the column index they reference.
    public static Map<Integer, List<ResolvedPredicate>> byColumn(ResolvedPredicate root) {
        Map<Integer, List<ResolvedPredicate>> result = new HashMap<>();
        collect(root, result);
        return result;
    }

    private static void collect(ResolvedPredicate p, Map<Integer, List<ResolvedPredicate>> out) {
        switch (p) {
            case ResolvedPredicate.And a -> {
                for (ResolvedPredicate child : a.children()) {
                    collect(child, out);
                }
            }
            case ResolvedPredicate.Or ignored -> {
                // Leaves beneath an OR are not AND-necessary — skip the entire subtree.
            }
            case ResolvedPredicate.IntPredicate l -> add(out, l.columnIndex(), l);
            case ResolvedPredicate.LongPredicate l -> add(out, l.columnIndex(), l);
            case ResolvedPredicate.UnsignedIntPredicate l -> add(out, l.columnIndex(), l);
            case ResolvedPredicate.UnsignedLongPredicate l -> add(out, l.columnIndex(), l);
            case ResolvedPredicate.FloatPredicate l -> add(out, l.columnIndex(), l);
            case ResolvedPredicate.Float16Predicate l -> add(out, l.columnIndex(), l);
            case ResolvedPredicate.DoublePredicate l -> add(out, l.columnIndex(), l);
            case ResolvedPredicate.BooleanPredicate l -> add(out, l.columnIndex(), l);
            case ResolvedPredicate.BinaryPredicate l -> add(out, l.columnIndex(), l);
            case ResolvedPredicate.IntInPredicate l -> add(out, l.columnIndex(), l);
            case ResolvedPredicate.LongInPredicate l -> add(out, l.columnIndex(), l);
            case ResolvedPredicate.UnsignedIntInPredicate l -> add(out, l.columnIndex(), l);
            case ResolvedPredicate.UnsignedLongInPredicate l -> add(out, l.columnIndex(), l);
            case ResolvedPredicate.BinaryInPredicate l -> add(out, l.columnIndex(), l);
            case ResolvedPredicate.FloatInPredicate l -> add(out, l.columnIndex(), l);
            case ResolvedPredicate.DoubleInPredicate l -> add(out, l.columnIndex(), l);
            case ResolvedPredicate.Float16InPredicate l -> add(out, l.columnIndex(), l);
            case ResolvedPredicate.IsNullPredicate ignored -> {
                // Left out: a dropped page is replaced by nulls, which IS NULL matches, so
                // dropping one would return its rows rather than skip them.
            }
            case ResolvedPredicate.IsNotNullPredicate l -> add(out, l.columnIndex(), l);
            case ResolvedPredicate.EveryNonNullRowPredicate l -> add(out, l.columnIndex(), l);
            case ResolvedPredicate.NoRowPredicate l -> add(out, l.columnIndex(), l);
            case ResolvedPredicate.GeospatialPredicate l -> add(out, l.columnIndex(), l);
        }
    }

    private static void add(Map<Integer, List<ResolvedPredicate>> out, int column, ResolvedPredicate leaf) {
        out.computeIfAbsent(column, k -> new ArrayList<>()).add(leaf);
    }

    /// Returns `true` if any of the given AND-necessary leaves proves, from the
    /// supplied inline page [Statistics] alone, that the page cannot match — i.e.
    /// the page can be skipped.
    ///
    /// Each leaf is decided through [UnitStats#decide], the same decision a column chunk and a
    /// page of the column index answer, so a page proves what its statistics support: its bounds
    /// exclude the literal, or its null count accounts for every row it holds. Only
    /// [FilterDecision#CANNOT_MATCH] is read; a page this path keeps is read and filtered as it
    /// always was.
    ///
    /// Returns `false` when `stats` is `null`, when its bounds are unusable — see
    /// [MinMaxStats] — or when no leaf can drop.
    ///
    /// @param leaves the AND-necessary leaves testing this page's column, empty where the file
    ///        this page belongs to records the column's bounds in an order this reader cannot
    ///        read (see [BoundsReadability]), which is why the unit reads them as readable
    /// @param stats the page's inline statistics, or `null` if it carries none
    /// @param rowCount the page's rows, or [#UNKNOWN_ROW_COUNT] where its header does not give
    ///        them, which leaves every proof resting on a row count undecided
    /// @param logContext the page these statistics came from, for a discard to name
    public static boolean canDropPage(List<ResolvedPredicate> leaves, Statistics stats,
            long rowCount, LogContext logContext) {
        if (leaves == null || leaves.isEmpty() || stats == null) {
            return false;
        }
        UnitStats page = new UnitStats.InlinePageStats(stats, rowCount, BoundsReadability.ALL);
        for (ResolvedPredicate leaf : leaves) {
            if (page.decide(leaf, logContext) == FilterDecision.CANNOT_MATCH) {
                return true;
            }
        }
        return false;
    }
}
