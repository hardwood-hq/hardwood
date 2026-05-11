/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.util.List;
import java.util.function.IntUnaryOperator;

import dev.hardwood.internal.predicate.matcher.booleans.BooleanEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.booleans.BooleanNotEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.doubles.DoubleEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.doubles.DoubleGtBatchMatcher;
import dev.hardwood.internal.predicate.matcher.doubles.DoubleGtEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.doubles.DoubleLtBatchMatcher;
import dev.hardwood.internal.predicate.matcher.doubles.DoubleLtEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.doubles.DoubleNotEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.floats.FloatEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.floats.FloatGtBatchMatcher;
import dev.hardwood.internal.predicate.matcher.floats.FloatGtEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.floats.FloatLtBatchMatcher;
import dev.hardwood.internal.predicate.matcher.floats.FloatLtEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.floats.FloatNotEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.ints.IntEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.ints.IntGtBatchMatcher;
import dev.hardwood.internal.predicate.matcher.ints.IntGtEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.ints.IntInBatchMatcher;
import dev.hardwood.internal.predicate.matcher.ints.IntLtBatchMatcher;
import dev.hardwood.internal.predicate.matcher.ints.IntLtEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.ints.IntNotEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.longs.LongEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.longs.LongGtBatchMatcher;
import dev.hardwood.internal.predicate.matcher.longs.LongGtEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.longs.LongInBatchMatcher;
import dev.hardwood.internal.predicate.matcher.longs.LongLtBatchMatcher;
import dev.hardwood.internal.predicate.matcher.longs.LongLtEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.longs.LongNotEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.nulls.IsNotNullBatchMatcher;
import dev.hardwood.internal.predicate.matcher.nulls.IsNullBatchMatcher;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.schema.FileSchema;

/// Compiles an eligible [ResolvedPredicate] into per-column [ColumnBatchMatcher] fragments
/// for drain-side evaluation in [dev.hardwood.internal.reader.FlatColumnWorker].
///
/// Eligibility is **all-or-nothing** per query:
///
/// - A single column-local leaf with a supported `(type, op)`, OR
/// - `ResolvedPredicate.And(children)` where every child is a column-local leaf.
///
/// Any other shape (`Or`, `Not`, intermediate-struct paths, unsupported `(type, op)`,
/// two leaves on the same column) returns `null`. The caller falls back to the
/// existing [dev.hardwood.internal.reader.FilteredRowReader] path on `null`.
///
/// Supported `(type, op)` pairs in v1: `long` and `double` with comparison
/// operators (`EQ`, `NOT_EQ`, `LT`, `LT_EQ`, `GT`, `GT_EQ`).
public final class BatchFilterCompiler {

    private BatchFilterCompiler() {}

    /// Returns a per-column matcher array (indexed by **projected** column index)
    /// or `null` if the predicate is not eligible.
    ///
    /// @param predicate resolved predicate tree
    /// @param schema the file schema (for top-level path checks)
    /// @param topLevelFieldIndex maps a file column index to the projected
    ///     column index, or `-1` if the column is not addressable as a top-level
    ///     projected leaf
    public static ColumnBatchMatcher[] tryCompile(ResolvedPredicate predicate, FileSchema schema,
            IntUnaryOperator topLevelFieldIndex) {
        List<ResolvedPredicate> leaves = flattenAnd(predicate);
        if (leaves == null) {
            return null;
        }

        int leavesSize = leaves.size();
        int[] projectedIdx = new int[leavesSize];
        int maxProjected = -1;

        for (int i = 0; i < leavesSize; i++) {
            ResolvedPredicate leaf = leaves.get(i);

            int fileIdx = leafColumnIndex(leaf);
            if (fileIdx == -1 || !isTopLevel(schema, fileIdx)) {
                return null;
            }

            int projected = topLevelFieldIndex.applyAsInt(fileIdx);
            if (projected < 0 || !isSupported(leaf)) {
                return null;
            }

            projectedIdx[i] = projected;

            if (projected > maxProjected) {
                maxProjected = projected;
            }
        }

        ColumnBatchMatcher[] result = new ColumnBatchMatcher[maxProjected + 1];

        for (int i = 0; i < leavesSize; i++) {
            int projected = projectedIdx[i];
            ColumnBatchMatcher matcher = compileLeaf(leaves.get(i));

            // Multiple leaves on the same column (e.g. `id >= x AND id <= y`)
            // compose into a single AND-merged matcher kept in the same slot.
            result[projected] = result[projected] == null
                    ? matcher
                    : new AndBatchMatcher(result[projected], matcher);
        }
        return result;
    }

    /// `ResolvedPredicate.And` is canonicalized to a flat list of children at
    /// construction time (see [ResolvedPredicate.And]), so a nested `And` here
    /// would indicate a broken invariant. `Or` children remain a bail-out — the
    /// batch path only supports column-local conjunctions.
    private static List<ResolvedPredicate> flattenAnd(ResolvedPredicate predicate) {
        if (predicate instanceof ResolvedPredicate.And(List<ResolvedPredicate> children)) {
            for (ResolvedPredicate child : children) {
                if (child instanceof ResolvedPredicate.And) {
                    throw new IllegalStateException(
                            "ResolvedPredicate.And must be flat; found nested And child");
                }
                if (child instanceof ResolvedPredicate.Or) {
                    return null;
                }
            }
            return children;
        }
        if (predicate instanceof ResolvedPredicate.Or
                || predicate instanceof ResolvedPredicate.GeospatialPredicate) {
            return null;
        }
        return List.of(predicate);
    }

    private static int leafColumnIndex(ResolvedPredicate leaf) {
        return switch (leaf) {
            case ResolvedPredicate.LongPredicate p -> p.columnIndex();
            case ResolvedPredicate.DoublePredicate p -> p.columnIndex();
            case ResolvedPredicate.IntPredicate p -> p.columnIndex();
            case ResolvedPredicate.FloatPredicate p -> p.columnIndex();
            case ResolvedPredicate.BooleanPredicate p -> p.columnIndex();
            case ResolvedPredicate.IntInPredicate p -> p.columnIndex();
            case ResolvedPredicate.LongInPredicate p -> p.columnIndex();
            case ResolvedPredicate.IsNullPredicate p -> p.columnIndex();
            case ResolvedPredicate.IsNotNullPredicate p -> p.columnIndex();
            default -> -1;
        };
    }

    private static boolean isTopLevel(FileSchema schema, int columnIndex) {
        return schema.getColumn(columnIndex).fieldPath().elements().size() == 1;
    }

    private static boolean isSupported(ResolvedPredicate leaf) {
        return switch (leaf) {
            case ResolvedPredicate.LongPredicate ignored -> true;
            case ResolvedPredicate.DoublePredicate ignored -> true;
            case ResolvedPredicate.IntPredicate ignored -> true;
            case ResolvedPredicate.FloatPredicate ignored -> true;
            case ResolvedPredicate.IntInPredicate ignored -> true;
            case ResolvedPredicate.LongInPredicate ignored -> true;
            case ResolvedPredicate.IsNullPredicate ignored -> true;
            case ResolvedPredicate.IsNotNullPredicate ignored -> true;
            case ResolvedPredicate.BooleanPredicate p ->
                    p.op() == FilterPredicate.Operator.EQ || p.op() == FilterPredicate.Operator.NOT_EQ;
            default -> false;
        };
    }

    private static ColumnBatchMatcher compileLeaf(ResolvedPredicate leaf) {
        return switch (leaf) {
            case ResolvedPredicate.LongPredicate p -> switch (p.op()) {
                case GT -> new LongGtBatchMatcher(p.value());
                case LT -> new LongLtBatchMatcher(p.value());
                case LT_EQ -> new LongLtEqBatchMatcher(p.value());
                case GT_EQ -> new LongGtEqBatchMatcher(p.value());
                case EQ -> new LongEqBatchMatcher(p.value());
                case NOT_EQ -> new LongNotEqBatchMatcher(p.value());
            };
            case ResolvedPredicate.DoublePredicate p -> switch (p.op()) {
                case GT -> new DoubleGtBatchMatcher(p.value());
                case LT -> new DoubleLtBatchMatcher(p.value());
                case LT_EQ -> new DoubleLtEqBatchMatcher(p.value());
                case GT_EQ -> new DoubleGtEqBatchMatcher(p.value());
                case EQ -> new DoubleEqBatchMatcher(p.value());
                case NOT_EQ -> new DoubleNotEqBatchMatcher(p.value());
            };
            case ResolvedPredicate.IntPredicate p -> switch (p.op()) {
                case GT -> new IntGtBatchMatcher(p.value());
                case LT -> new IntLtBatchMatcher(p.value());
                case LT_EQ -> new IntLtEqBatchMatcher(p.value());
                case GT_EQ -> new IntGtEqBatchMatcher(p.value());
                case EQ -> new IntEqBatchMatcher(p.value());
                case NOT_EQ -> new IntNotEqBatchMatcher(p.value());
            };
            case ResolvedPredicate.FloatPredicate p -> switch (p.op()) {
                case GT -> new FloatGtBatchMatcher(p.value());
                case LT -> new FloatLtBatchMatcher(p.value());
                case LT_EQ -> new FloatLtEqBatchMatcher(p.value());
                case GT_EQ -> new FloatGtEqBatchMatcher(p.value());
                case EQ -> new FloatEqBatchMatcher(p.value());
                case NOT_EQ -> new FloatNotEqBatchMatcher(p.value());
            };
            case ResolvedPredicate.BooleanPredicate p -> switch (p.op()) {
                case EQ -> new BooleanEqBatchMatcher(p.value());
                case NOT_EQ -> new BooleanNotEqBatchMatcher(p.value());
                default -> throw new IllegalStateException(
                        "Unsupported boolean operator reached compileLeaf: " + p.op()
                                + " — isSupported should have rejected this");
            };
            case ResolvedPredicate.IntInPredicate p -> new IntInBatchMatcher(p.values());
            case ResolvedPredicate.LongInPredicate p -> new LongInBatchMatcher(p.values());
            case ResolvedPredicate.IsNullPredicate p -> new IsNullBatchMatcher();
            case ResolvedPredicate.IsNotNullPredicate p -> new IsNotNullBatchMatcher();
            default -> throw new IllegalStateException(
                    "Unsupported predicate type reached compileLeaf: " + leaf.getClass().getSimpleName()
                            + " — isSupported should have rejected this");
        };
    }
}
