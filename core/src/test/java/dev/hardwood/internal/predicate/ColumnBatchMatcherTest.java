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
import dev.hardwood.internal.predicate.matcher.doubles.DoubleInBatchMatcher;
import dev.hardwood.internal.predicate.matcher.doubles.DoubleLtBatchMatcher;
import dev.hardwood.internal.predicate.matcher.doubles.DoubleLtEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.doubles.DoubleNotEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.floats.FloatWideningDoubleInBatchMatcher;
import dev.hardwood.internal.predicate.matcher.longs.LongEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.longs.LongGtBatchMatcher;
import dev.hardwood.internal.predicate.matcher.longs.LongGtEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.longs.LongLtBatchMatcher;
import dev.hardwood.internal.predicate.matcher.longs.LongLtEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.longs.LongNotEqBatchMatcher;
import dev.hardwood.internal.reader.BatchExchange;

import static java.util.Arrays.copyOf;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class ColumnBatchMatcherTest {

    private static BatchExchange.Batch longBatch(long[] values, BitSet nulls) {
        BatchExchange.Batch batch = new BatchExchange.Batch();
        batch.values = values;
        batch.validity = toValidity(nulls, values.length);
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

    private static long[] toValidity(BitSet nulls, int n) {
        if (nulls == null) {
            return null;
        }
        BitSet validity = new BitSet(n);
        validity.set(0, n);
        validity.andNot(nulls);
        int wordsLen = (n + 63) >>> 6;
        long[] words = validity.toLongArray();
        return words.length < wordsLen ? copyOf(words, wordsLen) : words;
    }

    private static long[] runMatcher(ColumnBatchMatcher matcher, BatchExchange.Batch batch) {
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
        assertArrayEquals(new long[]{bits(2)}, runMatcher(new LongGtBatchMatcher(5L), batch));
    }

    @Test
    void longLt_excludesEqualAndNulls() {
        long[] vals = {1L, 5L, 6L, 10L, 0L};
        BatchExchange.Batch batch = longBatch(vals, nullsAt(0));
        // Row 0 (1<5) NULL → excluded. Row 4 (0<5) matches.
        assertArrayEquals(new long[]{bits(4)}, runMatcher(new LongLtBatchMatcher(5L), batch));
    }

    @Test
    void longLtEq_includesEqual() {
        long[] vals = {1L, 5L, 6L, 10L, 0L};
        BatchExchange.Batch batch = longBatch(vals, null);
        assertArrayEquals(new long[]{bits(0, 1, 4)}, runMatcher(new LongLtEqBatchMatcher(5L), batch));
    }

    @Test
    void longGtEq_includesEqual() {
        long[] vals = {1L, 5L, 6L, 10L, 0L};
        BatchExchange.Batch batch = longBatch(vals, null);
        assertArrayEquals(new long[]{bits(1, 2, 3)}, runMatcher(new LongGtEqBatchMatcher(5L), batch));
    }

    @Test
    void longEq_matchesOnlyExactValueAndExcludesNulls() {
        long[] vals = {5L, 5L, 6L, 5L, 0L};
        BatchExchange.Batch batch = longBatch(vals, nullsAt(1));
        // Row 1 is NULL → excluded even though stored value is 5.
        assertArrayEquals(new long[]{bits(0, 3)}, runMatcher(new LongEqBatchMatcher(5L), batch));
    }

    @Test
    void longNotEq_excludesNullsLikeOtherOps() {
        long[] vals = {5L, 5L, 6L, 5L, 0L};
        BatchExchange.Batch batch = longBatch(vals, nullsAt(1));
        // Row 1 NULL → excluded (NULL != x is unknown → false). Row 2 (6 != 5) and row 4 (0 != 5) match.
        assertArrayEquals(new long[]{bits(2, 4)}, runMatcher(new LongNotEqBatchMatcher(5L), batch));
    }

    @Test
    void longGt_acrossWordBoundary_setsBitsInBothWords() {
        long[] vals = new long[70];
        for (int i = 0; i < vals.length; i++) {
            vals[i] = i; // matches > 5 → rows 6..69
        }
        BatchExchange.Batch batch = longBatch(vals, null);
        long[] out = runMatcher(new LongGtBatchMatcher(5L), batch);
        long w0 = 0;
        for (int b = 6; b < 64; b++) w0 |= 1L << b;
        long w1 = 0;
        for (int b = 0; b < 6; b++) w1 |= 1L << b;
        assertArrayEquals(new long[]{w0, w1}, out);
    }

    private static BatchExchange.Batch doubleBatch(double[] values, BitSet nulls) {
        BatchExchange.Batch batch = new BatchExchange.Batch();
        batch.values = values;
        batch.validity = toValidity(nulls, values.length);
        batch.recordCount = values.length;
        return batch;
    }

    @Test
    void doubleGt_keepsValuesGreaterThanLiteralAndExcludesNulls() {
        double[] vals = {1.0, 5.0, 6.5, 10.0, 0.0};
        BatchExchange.Batch batch = doubleBatch(vals, nullsAt(3));
        assertArrayEquals(new long[]{bits(2)}, runMatcher(new DoubleGtBatchMatcher(5.0), batch));
    }

    @Test
    void doubleLt_excludesEqualAndNulls() {
        double[] vals = {1.0, 5.0, 6.0, 10.0, 0.0};
        BatchExchange.Batch batch = doubleBatch(vals, nullsAt(0));
        assertArrayEquals(new long[]{bits(4)}, runMatcher(new DoubleLtBatchMatcher(5.0), batch));
    }

    @Test
    void doubleLtEq_includesEqual() {
        double[] vals = {1.0, 5.0, 6.0, 10.0, 0.0};
        BatchExchange.Batch batch = doubleBatch(vals, null);
        assertArrayEquals(new long[]{bits(0, 1, 4)}, runMatcher(new DoubleLtEqBatchMatcher(5.0), batch));
    }

    @Test
    void doubleGtEq_includesEqual() {
        double[] vals = {1.0, 5.0, 6.0, 10.0, 0.0};
        BatchExchange.Batch batch = doubleBatch(vals, null);
        assertArrayEquals(new long[]{bits(1, 2, 3)}, runMatcher(new DoubleGtEqBatchMatcher(5.0), batch));
    }

    @Test
    void doubleEq_matchesOnlyExactValueAndExcludesNulls() {
        double[] vals = {5.0, 5.0, 6.0, 5.0, 0.0};
        BatchExchange.Batch batch = doubleBatch(vals, nullsAt(1));
        assertArrayEquals(new long[]{bits(0, 3)}, runMatcher(new DoubleEqBatchMatcher(5.0), batch));
    }

    @Test
    void doubleNotEq_excludesNulls() {
        double[] vals = {5.0, 5.0, 6.0, 5.0, 0.0};
        BatchExchange.Batch batch = doubleBatch(vals, nullsAt(1));
        assertArrayEquals(new long[]{bits(2, 4)}, runMatcher(new DoubleNotEqBatchMatcher(5.0), batch));
    }

    // NaN ordering follows Double.compare to mirror RecordFilterCompiler.indexedDoubleLeaf:
    // NaN compares greater than any non-NaN and equal to itself.
    @Test
    void doubleGt_nanMatchesGreaterThanAnyFiniteLiteral() {
        double[] vals = {Double.NaN, Double.POSITIVE_INFINITY};
        BatchExchange.Batch batch = doubleBatch(vals, null);
        assertArrayEquals(new long[]{bits(0, 1)}, runMatcher(new DoubleGtBatchMatcher(1.0e9), batch));
    }

    @Test
    void doubleEq_nanLiteralMatchesNaNValuesOnly() {
        double[] vals = {Double.NaN, 1.0, Double.NaN};
        BatchExchange.Batch batch = doubleBatch(vals, null);
        assertArrayEquals(new long[]{bits(0, 2)}, runMatcher(new DoubleEqBatchMatcher(Double.NaN), batch));
    }

    @Test
    void doubleLt_nanIsNotLessThanAnything() {
        double[] vals = {Double.NaN, 0.0};
        BatchExchange.Batch batch = doubleBatch(vals, null);
        assertArrayEquals(new long[]{bits(1)}, runMatcher(new DoubleLtBatchMatcher(5.0), batch));
    }

    @Test
    void doubleIn_matchesOnlyExactValuesAndExcludesNulls() {
        double[] vals = {1.5, 2.5, -0.0, +0.0, Double.NaN, 3.5};
        BatchExchange.Batch batch = doubleBatch(vals, nullsAt(1));
        double[] inValues = {2.5, -0.0, Double.NaN};
        assertArrayEquals(new long[]{bits(2, 4)}, runMatcher(new DoubleInBatchMatcher(inValues), batch));
    }

    @Test
    void doubleIn_acrossWordBoundary_setsBitsInBothWords() {
        double[] vals = new double[70];
        for (int i = 0; i < vals.length; i++) {
            vals[i] = (double) i;
        }
        BatchExchange.Batch batch = doubleBatch(vals, null);
        long[] out = runMatcher(new DoubleInBatchMatcher(new double[]{5.0, 65.0}), batch);
        assertArrayEquals(new long[]{1L << 5, 1L << (65 - 64)}, out);
    }

    private static BatchExchange.Batch floatBatch(float[] values, BitSet nulls) {
        BatchExchange.Batch batch = new BatchExchange.Batch();
        batch.values = values;
        batch.validity = toValidity(nulls, values.length);
        batch.recordCount = values.length;
        return batch;
    }

    @Test
    void floatWideningDoubleIn_matchesOnlyExactValuesAndExcludesNulls() {
        float[] vals = {0.5f, 1.5f, -0.0f, +0.0f, Float.NaN, 0.1f};
        BatchExchange.Batch batch = floatBatch(vals, nullsAt(1));
        double[] inValues = {1.5, -0.0, Double.NaN, 0.1};
        assertArrayEquals(new long[]{bits(2, 4)}, runMatcher(new FloatWideningDoubleInBatchMatcher(inValues), batch));
    }

    @Test
    void floatWideningDoubleIn_acrossWordBoundary_setsBitsInBothWords() {
        float[] vals = new float[70];
        for (int i = 0; i < vals.length; i++) {
            vals[i] = (float) i;
        }
        BatchExchange.Batch batch = floatBatch(vals, null);
        long[] out = runMatcher(new FloatWideningDoubleInBatchMatcher(new double[]{5.0, 65.0}), batch);
        assertArrayEquals(new long[]{1L << 5, 1L << (65 - 64)}, out);
    }

    @Test
    void doubleIn_allMatchAndNoneMatchAndAllNulls() {
        double[] vals = {5.0, 5.0, 5.0, 5.0};
        assertArrayEquals(new long[]{bits(0, 1, 2, 3)},
                runMatcher(new DoubleInBatchMatcher(new double[]{5.0}), doubleBatch(vals, null)));
        assertArrayEquals(new long[]{0L},
                runMatcher(new DoubleInBatchMatcher(new double[]{10.0}), doubleBatch(vals, null)));
        assertArrayEquals(new long[]{0L},
                runMatcher(new DoubleInBatchMatcher(new double[]{5.0}), doubleBatch(vals, nullsAt(0, 1, 2, 3))));
    }

    @Test
    void doubleIn_signedZeroAndNaNPrecision() {
        double[] vals = {+0.0, -0.0, Double.NaN};
        assertArrayEquals(new long[]{bits(0)},
                runMatcher(new DoubleInBatchMatcher(new double[]{+0.0}), doubleBatch(vals, null)));
        assertArrayEquals(new long[]{bits(1)},
                runMatcher(new DoubleInBatchMatcher(new double[]{-0.0}), doubleBatch(vals, null)));
        assertArrayEquals(new long[]{bits(2)},
                runMatcher(new DoubleInBatchMatcher(new double[]{Double.NaN}), doubleBatch(vals, null)));
    }

    @Test
    void doubleIn_nanPayloadsAndInfinities() {
        double customNan1 = Double.longBitsToDouble(0x7ff8000000000001L);
        double customNan2 = Double.longBitsToDouble(0x7ff8000000000042L);
        double[] vals = {customNan1, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 1.0};
        BatchExchange.Batch batch = doubleBatch(vals, null);

        // Probe with canonical NaN matches customNan1
        assertArrayEquals(new long[]{bits(0)},
                runMatcher(new DoubleInBatchMatcher(new double[]{Double.NaN}), batch));

        // Probe with customNan2 matches customNan1
        assertArrayEquals(new long[]{bits(0)},
                runMatcher(new DoubleInBatchMatcher(new double[]{customNan2}), batch));

        // Probe with +Inf matches +Inf
        assertArrayEquals(new long[]{bits(1)},
                runMatcher(new DoubleInBatchMatcher(new double[]{Double.POSITIVE_INFINITY}), batch));

        // Probe with -Inf matches -Inf
        assertArrayEquals(new long[]{bits(2)},
                runMatcher(new DoubleInBatchMatcher(new double[]{Double.NEGATIVE_INFINITY}), batch));

        // Both infinities in probe list
        assertArrayEquals(new long[]{bits(1, 2)},
                runMatcher(new DoubleInBatchMatcher(new double[]{Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}), batch));
    }

    @Test
    void floatWideningDoubleIn_signedZeroAndExactWideningPrecision() {
        float[] vals = {+0.0f, -0.0f, Float.NaN, 0.1f};
        assertArrayEquals(new long[]{bits(0)},
                runMatcher(new FloatWideningDoubleInBatchMatcher(new double[]{+0.0}), floatBatch(vals, null)));
        assertArrayEquals(new long[]{bits(1)},
                runMatcher(new FloatWideningDoubleInBatchMatcher(new double[]{-0.0}), floatBatch(vals, null)));
        assertArrayEquals(new long[]{bits(2)},
                runMatcher(new FloatWideningDoubleInBatchMatcher(new double[]{Double.NaN}), floatBatch(vals, null)));
        // 0.1d does not equal (double) 0.1f -> 0L
        assertArrayEquals(new long[]{0L},
                runMatcher(new FloatWideningDoubleInBatchMatcher(new double[]{0.1}), floatBatch(vals, null)));
        // Widened probe (double) 0.1f DOES match
        assertArrayEquals(new long[]{bits(3)},
                runMatcher(new FloatWideningDoubleInBatchMatcher(new double[]{(double) 0.1f}), floatBatch(vals, null)));
    }

    @Test
    void floatWideningDoubleIn_nanPayloadsAndInfinities() {
        float customFloatNan1 = Float.intBitsToFloat(0x7fc00001);
        double customDoubleNan2 = Double.longBitsToDouble(0x7ff8000000000042L);
        float[] vals = {customFloatNan1, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, 1.0f};
        BatchExchange.Batch batch = floatBatch(vals, null);

        // Probe with canonical Double.NaN matches customFloatNan1 (widened)
        assertArrayEquals(new long[]{bits(0)},
                runMatcher(new FloatWideningDoubleInBatchMatcher(new double[]{Double.NaN}), batch));

        // Probe with customDoubleNan2 matches customFloatNan1 (widened)
        assertArrayEquals(new long[]{bits(0)},
                runMatcher(new FloatWideningDoubleInBatchMatcher(new double[]{customDoubleNan2}), batch));

        // Probe with Double.+Inf matches Float.+Inf (widened)
        assertArrayEquals(new long[]{bits(1)},
                runMatcher(new FloatWideningDoubleInBatchMatcher(new double[]{Double.POSITIVE_INFINITY}), batch));

        // Probe with Double.-Inf matches Float.-Inf (widened)
        assertArrayEquals(new long[]{bits(2)},
                runMatcher(new FloatWideningDoubleInBatchMatcher(new double[]{Double.NEGATIVE_INFINITY}), batch));

        // Both infinities in probe list
        assertArrayEquals(new long[]{bits(1, 2)},
                runMatcher(new FloatWideningDoubleInBatchMatcher(new double[]{Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}), batch));
    }

    @Test
    void doubleIn_killsShiftAndBoundaryMutants() {
        double[] vals64 = new double[64];
        vals64[63] = 42.0;
        assertArrayEquals(new long[]{1L << 63},
                runMatcher(new DoubleInBatchMatcher(new double[]{42.0}), doubleBatch(vals64, null)));

        double[] vals130 = new double[130];
        vals130[65] = 99.0;
        long[] out = runMatcher(new DoubleInBatchMatcher(new double[]{99.0}), doubleBatch(vals130, null));
        assertArrayEquals(new long[]{0L, 1L << 1, 0L}, out);
    }

    @Test
    void floatWideningDoubleIn_killsShiftAndBoundaryMutants() {
        float[] vals64 = new float[64];
        vals64[63] = 42.0f;
        assertArrayEquals(new long[]{1L << 63},
                runMatcher(new FloatWideningDoubleInBatchMatcher(new double[]{42.0}), floatBatch(vals64, null)));

        float[] vals130 = new float[130];
        vals130[65] = 99.0f;
        long[] out = runMatcher(new FloatWideningDoubleInBatchMatcher(new double[]{99.0}), floatBatch(vals130, null));
        assertArrayEquals(new long[]{0L, 1L << 1, 0L}, out);
    }
}
