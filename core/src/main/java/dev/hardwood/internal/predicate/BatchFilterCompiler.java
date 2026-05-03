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
import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.schema.FileSchema;

/// Compiles an eligible [ResolvedPredicate] into per-column [BatchMatcher] fragments
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
    public static BatchMatcher[] tryCompile(ResolvedPredicate predicate, FileSchema schema,
            IntUnaryOperator topLevelFieldIndex) {
        List<ResolvedPredicate> leaves = flattenAnd(predicate);
        if (leaves == null) {
            return null;
        }
        if (leaves.size() < 2) {
            // Drain-side parallelism only pays off when there are >= 2 distinct
            // column fragments. A single-leaf query has no peer drain to absorb
            // the matcher cost in parallel, and the compiled per-row path is
            // tighter than the batch-loop + nextSetBit iteration shape emitted
            // here. Falling through lets the caller use FilteredRowReader.
            return null;
        }

        int maxProjected = -1;
        for (ResolvedPredicate leaf : leaves) {
            int fileIdx = leafColumnIndex(leaf);
            if (fileIdx < 0) {
                return null;
            }
            if (!isTopLevel(schema, fileIdx)) {
                return null;
            }
            int projected = topLevelFieldIndex.applyAsInt(fileIdx);
            if (projected < 0) {
                return null;
            }
            if (!isSupported(leaf)) {
                return null;
            }
            if (projected > maxProjected) {
                maxProjected = projected;
            }
        }

        BatchMatcher[] result = new BatchMatcher[maxProjected + 1];
        for (ResolvedPredicate leaf : leaves) {
            int fileIdx = leafColumnIndex(leaf);
            int projected = topLevelFieldIndex.applyAsInt(fileIdx);
            if (result[projected] != null) {
                // Two leaves on the same column: not supported in v1.
                return null;
            }
            BatchMatcher matcher = compileLeaf(leaf, projected);
            if (matcher == null) {
                return null;
            }
            result[projected] = matcher;
        }
        return result;
    }

    private static List<ResolvedPredicate> flattenAnd(ResolvedPredicate predicate) {
        if (predicate instanceof ResolvedPredicate.And and) {
            for (ResolvedPredicate child : and.children()) {
                if (child instanceof ResolvedPredicate.And
                        || child instanceof ResolvedPredicate.Or) {
                    return null;
                }
            }
            return and.children();
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
            case ResolvedPredicate.LongPredicate p -> isComparisonOp(p.op());
            case ResolvedPredicate.DoublePredicate p -> isComparisonOp(p.op());
            case ResolvedPredicate.IntPredicate p -> isComparisonOp(p.op());
            case ResolvedPredicate.FloatPredicate p -> isComparisonOp(p.op());
            // BooleanPredicate is only meaningful for EQ/NOT_EQ — other ops in
            // RecordFilterCompiler.booleanLeaf collapse to a non-null check,
            // which doesn't map to a typed boolean matcher.
            case ResolvedPredicate.BooleanPredicate p -> p.op() == Operator.EQ || p.op() == Operator.NOT_EQ;
            case ResolvedPredicate.IntInPredicate ignored -> true;
            case ResolvedPredicate.LongInPredicate ignored -> true;
            case ResolvedPredicate.IsNullPredicate ignored -> true;
            case ResolvedPredicate.IsNotNullPredicate ignored -> true;
            default -> false;
        };
    }

    private static boolean isComparisonOp(Operator op) {
        return switch (op) {
            case EQ, NOT_EQ, LT, LT_EQ, GT, GT_EQ -> true;
        };
    }

    private static BatchMatcher compileLeaf(ResolvedPredicate leaf, int projectedIdx) {
        return switch (leaf) {
            case ResolvedPredicate.LongPredicate p -> switch (p.op()) {
                case GT -> new LongGtBatchMatcher(projectedIdx, p.value());
                case LT -> new LongLtBatchMatcher(projectedIdx, p.value());
                case LT_EQ -> new LongLtEqBatchMatcher(projectedIdx, p.value());
                case GT_EQ -> new LongGtEqBatchMatcher(projectedIdx, p.value());
                case EQ -> new LongEqBatchMatcher(projectedIdx, p.value());
                case NOT_EQ -> new LongNotEqBatchMatcher(projectedIdx, p.value());
            };
            case ResolvedPredicate.DoublePredicate p -> switch (p.op()) {
                case GT -> new DoubleGtBatchMatcher(projectedIdx, p.value());
                case LT -> new DoubleLtBatchMatcher(projectedIdx, p.value());
                case LT_EQ -> new DoubleLtEqBatchMatcher(projectedIdx, p.value());
                case GT_EQ -> new DoubleGtEqBatchMatcher(projectedIdx, p.value());
                case EQ -> new DoubleEqBatchMatcher(projectedIdx, p.value());
                case NOT_EQ -> new DoubleNotEqBatchMatcher(projectedIdx, p.value());
            };
            case ResolvedPredicate.IntPredicate p -> switch (p.op()) {
                case GT -> new IntGtBatchMatcher(projectedIdx, p.value());
                case LT -> new IntLtBatchMatcher(projectedIdx, p.value());
                case LT_EQ -> new IntLtEqBatchMatcher(projectedIdx, p.value());
                case GT_EQ -> new IntGtEqBatchMatcher(projectedIdx, p.value());
                case EQ -> new IntEqBatchMatcher(projectedIdx, p.value());
                case NOT_EQ -> new IntNotEqBatchMatcher(projectedIdx, p.value());
            };
            case ResolvedPredicate.FloatPredicate p -> switch (p.op()) {
                case GT -> new FloatGtBatchMatcher(projectedIdx, p.value());
                case LT -> new FloatLtBatchMatcher(projectedIdx, p.value());
                case LT_EQ -> new FloatLtEqBatchMatcher(projectedIdx, p.value());
                case GT_EQ -> new FloatGtEqBatchMatcher(projectedIdx, p.value());
                case EQ -> new FloatEqBatchMatcher(projectedIdx, p.value());
                case NOT_EQ -> new FloatNotEqBatchMatcher(projectedIdx, p.value());
            };
            case ResolvedPredicate.BooleanPredicate p -> switch (p.op()) {
                case EQ -> new BooleanEqBatchMatcher(projectedIdx, p.value());
                case NOT_EQ -> new BooleanNotEqBatchMatcher(projectedIdx, p.value());
                default -> null;
            };
            case ResolvedPredicate.IntInPredicate p -> new IntInBatchMatcher(projectedIdx, p.values());
            case ResolvedPredicate.LongInPredicate p -> new LongInBatchMatcher(projectedIdx, p.values());
            case ResolvedPredicate.IsNullPredicate p -> new IsNullBatchMatcher(projectedIdx);
            case ResolvedPredicate.IsNotNullPredicate p -> new IsNotNullBatchMatcher(projectedIdx);
            default -> null;
        };
    }
}
