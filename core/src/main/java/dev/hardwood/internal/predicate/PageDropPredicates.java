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
/// [StatisticsFilterSupport#canDropLeaf] returns `false` for `IsNullPredicate`
/// and `IsNotNullPredicate`, so including them in the result is harmless —
/// they will never cause a page drop. They are included anyway so callers do
/// not have to distinguish.
public final class PageDropPredicates {

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
            case ResolvedPredicate.FloatPredicate l -> add(out, l.columnIndex(), l);
            case ResolvedPredicate.DoublePredicate l -> add(out, l.columnIndex(), l);
            case ResolvedPredicate.BooleanPredicate l -> add(out, l.columnIndex(), l);
            case ResolvedPredicate.BinaryPredicate l -> add(out, l.columnIndex(), l);
            case ResolvedPredicate.IntInPredicate l -> add(out, l.columnIndex(), l);
            case ResolvedPredicate.LongInPredicate l -> add(out, l.columnIndex(), l);
            case ResolvedPredicate.BinaryInPredicate l -> add(out, l.columnIndex(), l);
            case ResolvedPredicate.IsNullPredicate l -> add(out, l.columnIndex(), l);
            case ResolvedPredicate.IsNotNullPredicate l -> add(out, l.columnIndex(), l);
        }
    }

    private static void add(Map<Integer, List<ResolvedPredicate>> out, int column, ResolvedPredicate leaf) {
        out.computeIfAbsent(column, k -> new ArrayList<>()).add(leaf);
    }

    /// Returns `true` if any of the given AND-necessary leaves proves, from the
    /// supplied inline page [Statistics] alone, that the page cannot match — i.e.
    /// the page can be skipped.
    ///
    /// Returns `false` when `stats` is `null`, carries only deprecated (unsigned
    /// sort order) min/max bytes, or when no leaf can drop.
    public static boolean canDropPage(List<ResolvedPredicate> leaves, Statistics stats) {
        if (leaves == null || leaves.isEmpty() || stats == null || stats.isMinMaxDeprecated()) {
            return false;
        }
        MinMaxStats minMax = MinMaxStats.of(stats);
        for (ResolvedPredicate leaf : leaves) {
            if (StatisticsFilterSupport.canDropLeaf(leaf, minMax)) {
                return true;
            }
        }
        return false;
    }
}
