/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

/// Compiles an eligible [ResolvedPredicate] into a [CompiledBatchFilter]:
/// per-column [ColumnBatchMatcher] fragments plus a [MergePlan] describing
/// how the consumer (in [dev.hardwood.internal.reader.FlatRowReader]) combines
/// the per-column bitmaps into one per-batch survivor mask.
///
/// Eligibility:
///
/// - Every leaf must be a supported `(type, op)` whose column is a **top-level**
///   projected field.
/// - `And`/`Or` may nest, as long as no single column appears in more than one
///   independent subtree. Same-column leaves under a single shared `And`/`Or`
///   fold into a per-column [AndBatchMatcher] / [OrBatchMatcher] composite — the
///   same mechanism that handles `id >= x AND id <= y` today.
///
/// Anything else (intermediate-struct paths, `BinaryPredicate`,
/// `GeospatialPredicate`, unsupported `(type, op)`) returns `null` and the
/// caller falls back to [dev.hardwood.internal.reader.FilteredRowReader].
public final class BatchFilterCompiler {

    private BatchFilterCompiler() {}

    /// Returns a [CompiledBatchFilter] or `null` if the predicate is not eligible.
    public static CompiledBatchFilter tryCompile(ResolvedPredicate predicate, FileSchema schema,
            IntUnaryOperator topLevelFieldIndex) {
        // Shared scratch array — threaded through every recursive `compile` call
        // and populated in-place as subtrees commit their per-column matchers.
        // Not a "result" of any subtree; it lives here, so its lifetime is one
        // tryCompile invocation.
        ColumnBatchMatcher[] matchers = new ColumnBatchMatcher[schema.getColumnCount()];
        Result r = compile(predicate, schema, topLevelFieldIndex, matchers);
        if (r == null) {
            return null;
        }
        // Single-column top-level result: the matcher is still "held" — commit
        // it and wrap as a one-leaf MergePlan. (Mixed-column results committed
        // their matchers and built their plan during recursion.)
        if (r.columnIndex >= 0) {
            matchers[r.columnIndex] = r.columnMatcher;
            r.mergePlan = new MergePlan.Column(r.columnIndex);
        }
        return new CompiledBatchFilter(matchers, r.mergePlan);
    }

    /// A subtree compile result. Carries one of two mutually exclusive states,
    /// keyed by [#columnIndex]:
    ///
    /// - `columnIndex >= 0`: the whole subtree lives on one projected column.
    ///   [#columnMatcher] is its composite matcher, **not yet committed** to the
    ///   `matchers[]` scratch array — the caller decides whether to fold it
    ///   further with siblings or commit it. [#mergePlan] is `null`.
    /// - `columnIndex == -1`: the subtree spans multiple columns. Its
    ///   per-column matchers were committed to `matchers[]` during recursion,
    ///   and [#mergePlan] is the plan the consumer walks to combine the
    ///   per-column bitmaps. [#columns] lists the columns the subtree touches
    ///   (used by the parent to detect cross-sibling column conflicts).
    private static final class Result {
        int columnIndex = -1;
        ColumnBatchMatcher columnMatcher;
        MergePlan mergePlan;
        Set<Integer> columns;
    }

    private static Result compile(ResolvedPredicate p, FileSchema schema, IntUnaryOperator projection,
            ColumnBatchMatcher[] matchers) {
        if (p instanceof ResolvedPredicate.And and) {
            return compileCompound(and.children(), false, schema, projection, matchers);
        }
        if (p instanceof ResolvedPredicate.Or or) {
            return compileCompound(or.children(), true, schema, projection, matchers);
        }
        return compileLeaf(p, schema, projection);
    }

    private static Result compileLeaf(ResolvedPredicate leaf, FileSchema schema, IntUnaryOperator projection) {
        int fileIdx = leafColumnIndex(leaf);
        if (fileIdx == -1 || !isTopLevel(schema, fileIdx) || !isSupported(leaf)) {
            return null;
        }
        int projected = projection.applyAsInt(fileIdx);
        if (projected < 0) {
            return null;
        }
        Result r = new Result();
        r.columnIndex = projected;
        r.columnMatcher = leafMatcher(leaf);
        return r;
    }

    private static ColumnBatchMatcher fold(boolean isOr, ColumnBatchMatcher a, ColumnBatchMatcher b) {
        return isOr ? new OrBatchMatcher(a, b) : new AndBatchMatcher(a, b);
    }

    private static Result compileCompound(List<ResolvedPredicate> children, boolean isOr,
            FileSchema schema, IntUnaryOperator projection, ColumnBatchMatcher[] matchers) {
        int n = children.size();
        if (n == 1) {
            return compile(children.getFirst(), schema, projection, matchers);
        }
        // Compile each child and track whether every child so far is single-column
        // on the same column. `sharedColumnIndex` ends >= 0 iff that holds for all
        // children — the same-column fast path below.
        Result[] childResults = new Result[n];
        int sharedColumnIndex = -1;
        for (int i = 0; i < n; i++) {
            Result cr = compile(children.get(i), schema, projection, matchers);
            if (cr == null) {
                return null;
            }
            childResults[i] = cr;
            if (i == 0) {
                sharedColumnIndex = cr.columnIndex;
            }
            else if (cr.columnIndex != sharedColumnIndex) {
                sharedColumnIndex = -1;
            }
        }

        Result out = new Result();

        if (sharedColumnIndex >= 0) {
            // Same column throughout — fold every child's matcher into one
            // composite, held on the Result without committing to matchers[].
            // The parent decides whether to fold further or finally commit.
            ColumnBatchMatcher composite = childResults[0].columnMatcher;
            for (int i = 1; i < n; i++) {
                composite = fold(isOr, composite, childResults[i].columnMatcher);
            }
            out.columnIndex = sharedColumnIndex;
            out.columnMatcher = composite;
            return out;
        }

        // Mixed columns: build the MergePlan child list. Same-column single
        // siblings under this And/Or (e.g. `id >= x AND id <= y AND value < z`)
        // fold into one per-column composite matcher and contribute a single
        // MergePlan.Column entry. Mixed-column children already committed their
        // matchers during recursion; we just splice their sub-plans in. Two
        // mixed-column siblings claiming the same column, or a single-column
        // bucket whose column was already claimed elsewhere, both reject —
        // `Batch.matches[c]` can hold only one per-row bitmap.
        Map<Integer, ColumnBatchMatcher> singleColumnBuckets = new LinkedHashMap<>(n);
        Set<Integer> touchedColumns = new HashSet<>(n);
        List<Result> mixedResults = new ArrayList<>(n);
        for (Result cr : childResults) {
            if (cr.columnIndex >= 0) {
                singleColumnBuckets.merge(cr.columnIndex, cr.columnMatcher,
                        (existing, next) -> fold(isOr, existing, next));
            }
            else {
                for (int c : cr.columns) {
                    if (!touchedColumns.add(c)) {
                        // Two mixed siblings both touch this column.
                        return null;
                    }
                }
                mixedResults.add(cr);
            }
        }

        // Commit single-column buckets to matchers[] and assemble the merge
        // plan's children. `matchers[c] != null` covers both within-compound
        // collisions (a mixed sibling already committed `c`) and ancestor-side
        // collisions.
        MergePlan[] planChildren = new MergePlan[singleColumnBuckets.size() + mixedResults.size()];
        int idx = 0;
        for (Map.Entry<Integer, ColumnBatchMatcher> bucket : singleColumnBuckets.entrySet()) {
            int c = bucket.getKey();
            if (matchers[c] != null) {
                return null;
            }
            matchers[c] = bucket.getValue();
            touchedColumns.add(c);
            planChildren[idx++] = new MergePlan.Column(c);
        }
        for (Result cr : mixedResults) {
            planChildren[idx++] = cr.mergePlan;
        }
        out.mergePlan = isOr
                ? new MergePlan.Or(planChildren)
                : new MergePlan.And(planChildren);
        out.columns = touchedColumns;
        return out;
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

    private static ColumnBatchMatcher leafMatcher(ResolvedPredicate leaf) {
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
                        "Unsupported boolean operator reached leafMatcher: " + p.op()
                                + " — isSupported should have rejected this");
            };
            case ResolvedPredicate.IntInPredicate p -> new IntInBatchMatcher(p.values());
            case ResolvedPredicate.LongInPredicate p -> new LongInBatchMatcher(p.values());
            case ResolvedPredicate.IsNullPredicate p -> new IsNullBatchMatcher();
            case ResolvedPredicate.IsNotNullPredicate p -> new IsNotNullBatchMatcher();
            default -> throw new IllegalStateException(
                    "Unsupported predicate type reached leafMatcher: " + leaf.getClass().getSimpleName()
                            + " — isSupported should have rejected this");
        };
    }
}
