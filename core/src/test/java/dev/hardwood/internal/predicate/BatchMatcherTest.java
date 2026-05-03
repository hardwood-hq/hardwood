/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.util.BitSet;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.predicate.matcher.doubles.DoubleEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.doubles.DoubleGtBatchMatcher;
import dev.hardwood.internal.predicate.matcher.doubles.DoubleGtEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.doubles.DoubleLtBatchMatcher;
import dev.hardwood.internal.predicate.matcher.doubles.DoubleLtEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.doubles.DoubleNotEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.longs.LongEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.longs.LongGtBatchMatcher;
import dev.hardwood.internal.predicate.matcher.longs.LongGtEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.longs.LongLtBatchMatcher;
import dev.hardwood.internal.predicate.matcher.longs.LongLtEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.longs.LongNotEqBatchMatcher;
import dev.hardwood.internal.reader.BatchExchange;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class BatchMatcherTest {

    private static BatchExchange.Batch longBatch(long[] values, BitSet nulls) {
        BatchExchange.Batch batch = new BatchExchange.Batch();
        batch.values = values;
        batch.nulls = nulls;
        batch.recordCount = values.length;
        return batch;
    }

    private static BitSet nullsAt(int... rows) {
        BitSet b = new BitSet();
        for (int row : rows) {
            b.set(row);
        }
        return b;
    }

    private static long[] runMatcher(BatchMatcher matcher, BatchExchange.Batch batch) {
        long[] out = new long[(batch.recordCount + 63) >>> 6];
        matcher.test(batch, out);
        return out;
    }

    private static long bits(int... rows) {
        long w = 0L;
        for (int row : rows) {
            w |= 1L << row;
        }
        return w;
    }

    @Test
    void longGt_keepsValuesGreaterThanLiteralAndExcludesNulls() {
        long[] vals = {1L, 5L, 6L, 10L, 0L};
        BatchExchange.Batch batch = longBatch(vals, nullsAt(3));
        // Row 2 (6 > 5) matches. Row 3 (10 > 5) is NULL → excluded.
        assertArrayEquals(new long[]{bits(2)}, runMatcher(new LongGtBatchMatcher(0, 5L), batch));
    }

    @Test
    void longLt_excludesEqualAndNulls() {
        long[] vals = {1L, 5L, 6L, 10L, 0L};
        BatchExchange.Batch batch = longBatch(vals, nullsAt(0));
        // Row 0 (1<5) NULL → excluded. Row 4 (0<5) matches.
        assertArrayEquals(new long[]{bits(4)}, runMatcher(new LongLtBatchMatcher(0, 5L), batch));
    }

    @Test
    void longLtEq_includesEqual() {
        long[] vals = {1L, 5L, 6L, 10L, 0L};
        BatchExchange.Batch batch = longBatch(vals, null);
        assertArrayEquals(new long[]{bits(0, 1, 4)}, runMatcher(new LongLtEqBatchMatcher(0, 5L), batch));
    }

    @Test
    void longGtEq_includesEqual() {
        long[] vals = {1L, 5L, 6L, 10L, 0L};
        BatchExchange.Batch batch = longBatch(vals, null);
        assertArrayEquals(new long[]{bits(1, 2, 3)}, runMatcher(new LongGtEqBatchMatcher(0, 5L), batch));
    }

    @Test
    void longEq_matchesOnlyExactValueAndExcludesNulls() {
        long[] vals = {5L, 5L, 6L, 5L, 0L};
        BatchExchange.Batch batch = longBatch(vals, nullsAt(1));
        // Row 1 is NULL → excluded even though stored value is 5.
        assertArrayEquals(new long[]{bits(0, 3)}, runMatcher(new LongEqBatchMatcher(0, 5L), batch));
    }

    @Test
    void longNotEq_excludesNullsLikeOtherOps() {
        long[] vals = {5L, 5L, 6L, 5L, 0L};
        BatchExchange.Batch batch = longBatch(vals, nullsAt(1));
        // Row 1 NULL → excluded (NULL != x is unknown → false). Row 2 (6 != 5) and row 4 (0 != 5) match.
        assertArrayEquals(new long[]{bits(2, 4)}, runMatcher(new LongNotEqBatchMatcher(0, 5L), batch));
    }

    @Test
    void longGt_acrossWordBoundary_setsBitsInBothWords() {
        long[] vals = new long[70];
        for (int i = 0; i < vals.length; i++) {
            vals[i] = i; // matches > 5 → rows 6..69
        }
        BatchExchange.Batch batch = longBatch(vals, null);
        long[] out = runMatcher(new LongGtBatchMatcher(0, 5L), batch);
        long w0 = 0;
        for (int b = 6; b < 64; b++) w0 |= 1L << b;
        long w1 = 0;
        for (int b = 0; b < 6; b++) w1 |= 1L << b;
        assertArrayEquals(new long[]{w0, w1}, out);
    }

    private static BatchExchange.Batch doubleBatch(double[] values, BitSet nulls) {
        BatchExchange.Batch batch = new BatchExchange.Batch();
        batch.values = values;
        batch.nulls = nulls;
        batch.recordCount = values.length;
        return batch;
    }

    @Test
    void doubleGt_keepsValuesGreaterThanLiteralAndExcludesNulls() {
        double[] vals = {1.0, 5.0, 6.5, 10.0, 0.0};
        BatchExchange.Batch batch = doubleBatch(vals, nullsAt(3));
        assertArrayEquals(new long[]{bits(2)}, runMatcher(new DoubleGtBatchMatcher(0, 5.0), batch));
    }

    @Test
    void doubleLt_excludesEqualAndNulls() {
        double[] vals = {1.0, 5.0, 6.0, 10.0, 0.0};
        BatchExchange.Batch batch = doubleBatch(vals, nullsAt(0));
        assertArrayEquals(new long[]{bits(4)}, runMatcher(new DoubleLtBatchMatcher(0, 5.0), batch));
    }

    @Test
    void doubleLtEq_includesEqual() {
        double[] vals = {1.0, 5.0, 6.0, 10.0, 0.0};
        BatchExchange.Batch batch = doubleBatch(vals, null);
        assertArrayEquals(new long[]{bits(0, 1, 4)}, runMatcher(new DoubleLtEqBatchMatcher(0, 5.0), batch));
    }

    @Test
    void doubleGtEq_includesEqual() {
        double[] vals = {1.0, 5.0, 6.0, 10.0, 0.0};
        BatchExchange.Batch batch = doubleBatch(vals, null);
        assertArrayEquals(new long[]{bits(1, 2, 3)}, runMatcher(new DoubleGtEqBatchMatcher(0, 5.0), batch));
    }

    @Test
    void doubleEq_matchesOnlyExactValueAndExcludesNulls() {
        double[] vals = {5.0, 5.0, 6.0, 5.0, 0.0};
        BatchExchange.Batch batch = doubleBatch(vals, nullsAt(1));
        assertArrayEquals(new long[]{bits(0, 3)}, runMatcher(new DoubleEqBatchMatcher(0, 5.0), batch));
    }

    @Test
    void doubleNotEq_excludesNulls() {
        double[] vals = {5.0, 5.0, 6.0, 5.0, 0.0};
        BatchExchange.Batch batch = doubleBatch(vals, nullsAt(1));
        assertArrayEquals(new long[]{bits(2, 4)}, runMatcher(new DoubleNotEqBatchMatcher(0, 5.0), batch));
    }

    // NaN ordering follows Double.compare to mirror RecordFilterCompiler.indexedDoubleLeaf:
    // NaN compares greater than any non-NaN and equal to itself.
    @Test
    void doubleGt_nanMatchesGreaterThanAnyFiniteLiteral() {
        double[] vals = {Double.NaN, Double.POSITIVE_INFINITY};
        BatchExchange.Batch batch = doubleBatch(vals, null);
        assertArrayEquals(new long[]{bits(0, 1)}, runMatcher(new DoubleGtBatchMatcher(0, 1.0e9), batch));
    }

    @Test
    void doubleEq_nanLiteralMatchesNaNValuesOnly() {
        double[] vals = {Double.NaN, 1.0, Double.NaN};
        BatchExchange.Batch batch = doubleBatch(vals, null);
        assertArrayEquals(new long[]{bits(0, 2)}, runMatcher(new DoubleEqBatchMatcher(0, Double.NaN), batch));
    }

    @Test
    void doubleLt_nanIsNotLessThanAnything() {
        double[] vals = {Double.NaN, 0.0};
        BatchExchange.Batch batch = doubleBatch(vals, null);
        assertArrayEquals(new long[]{bits(1)}, runMatcher(new DoubleLtBatchMatcher(0, 5.0), batch));
    }
}