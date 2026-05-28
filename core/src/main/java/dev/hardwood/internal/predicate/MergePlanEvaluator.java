/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

/// Evaluates a [MergePlan] against a set of per-column bitmaps, writing the
/// combined survivor bitmap into a caller-supplied output buffer.
///
/// One instance owns a depth-indexed scratch pool: each recursion level of an
/// And/Or merge gets its own scratch buffer, so sibling frames at the same
/// depth share a buffer (their lives don't overlap) while a frame at depth+1
/// gets a distinct buffer (the depth's buffer is still in use). Pool entries
/// persist across calls — zero per-call allocations after warm-up.
///
/// The same instance is used by [dev.hardwood.internal.reader.FlatRowReader]
/// on the hot path and by `DrainSideOracleTest` to verify equivalence with the
/// per-row compile path.
public final class MergePlanEvaluator {

    private final int wordsLen;
    private long[][] scratchPool;

    /// @param wordsLen size of every output and scratch buffer in `long`s
    ///         (= max-possible `activeWords`).
    public MergePlanEvaluator(int wordsLen) {
        this.wordsLen = wordsLen;
    }

    /// Evaluates `plan` and writes the result into `out` over its first
    /// `activeWords` words (positions past `activeWords` are not touched).
    ///
    /// @param plan the merge plan
    /// @param out destination buffer; must have length `>= wordsLen`
    /// @param activeWords number of valid words for this call (`<= wordsLen`)
    /// @param perColumn per-column bitmaps indexed by projected column index;
    ///         each entry must be non-null and have length `>= activeWords`
    ///         for any column referenced by `plan`
    public void eval(MergePlan plan, long[] out, int activeWords, long[][] perColumn) {
        evalNode(plan, out, 0, activeWords, perColumn);
    }

    private void evalNode(MergePlan node, long[] out, int depth, int activeWords, long[][] perColumn) {
        switch (node) {
            case MergePlan.Column c -> System.arraycopy(perColumn[c.projectedIndex()], 0, out, 0, activeWords);
            case MergePlan.And a -> evalAnd(a.children(), out, depth, activeWords, perColumn);
            case MergePlan.Or o -> evalOr(o.children(), out, depth, activeWords, perColumn);
        }
    }

    /// AND of children writing to `out`. Column children merge directly from
    /// their bitmap; compound children write into a depth-indexed scratch
    /// buffer first. The last sibling skips the all-zero-detection accumulator
    /// since there is nothing after it to short-circuit.
    private void evalAnd(MergePlan[] children, long[] out, int depth, int activeWords, long[][] perColumn) {
        writeFirstChild(children[0], out, depth, activeWords, perColumn);
        int n = children.length;
        long[] scratch = null;
        for (int i = 1; i < n; i++) {
            MergePlan child = children[i];
            long[] src;
            if (child instanceof MergePlan.Column cc) {
                src = perColumn[cc.projectedIndex()];
            }
            else {
                if (scratch == null) scratch = scratchAt(depth);
                evalNode(child, scratch, depth + 1, activeWords, perColumn);
                src = scratch;
            }
            if (i == n - 1) {
                for (int w = 0; w < activeWords; w++) out[w] &= src[w];
            }
            else {
                long anyBit = 0L;
                for (int w = 0; w < activeWords; w++) {
                    long merged = out[w] & src[w];
                    out[w] = merged;
                    anyBit |= merged;
                }
                if (anyBit == 0L) return;
            }
        }
    }

    /// OR of children writing to `out`. Mirror of [#evalAnd] without short-circuit.
    private void evalOr(MergePlan[] children, long[] out, int depth, int activeWords, long[][] perColumn) {
        writeFirstChild(children[0], out, depth, activeWords, perColumn);
        int n = children.length;
        long[] scratch = null;
        for (int i = 1; i < n; i++) {
            MergePlan child = children[i];
            long[] src;
            if (child instanceof MergePlan.Column cc) {
                src = perColumn[cc.projectedIndex()];
            }
            else {
                if (scratch == null) scratch = scratchAt(depth);
                evalNode(child, scratch, depth + 1, activeWords, perColumn);
                src = scratch;
            }
            for (int w = 0; w < activeWords; w++) out[w] |= src[w];
        }
    }

    /// Writes the first child of an And/Or into `out`, inlining the Column
    /// case to skip one recursive call frame.
    private void writeFirstChild(MergePlan first, long[] out, int depth, int activeWords, long[][] perColumn) {
        if (first instanceof MergePlan.Column c) {
            System.arraycopy(perColumn[c.projectedIndex()], 0, out, 0, activeWords);
        }
        else {
            evalNode(first, out, depth, activeWords, perColumn);
        }
    }

    /// Returns the scratch buffer for the given recursion depth, allocating
    /// (and growing the pool) on demand.
    private long[] scratchAt(int depth) {
        long[][] pool = scratchPool;
        if (pool == null || depth >= pool.length) {
            int newLen = pool == null ? 4 : pool.length * 2;
            while (newLen <= depth) {
                newLen *= 2;
            }
            long[][] grown = new long[newLen][];
            if (pool != null) {
                System.arraycopy(pool, 0, grown, 0, pool.length);
            }
            scratchPool = grown;
            pool = grown;
        }
        long[] buf = pool[depth];
        if (buf == null) {
            buf = new long[wordsLen];
            pool[depth] = buf;
        }
        return buf;
    }
}
