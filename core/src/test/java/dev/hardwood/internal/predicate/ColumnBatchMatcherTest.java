/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.nio.charset.StandardCharsets;
import java.util.BitSet;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.predicate.ResolvedPredicate.BinaryPredicate.Comparison;
import dev.hardwood.internal.predicate.matcher.binaries.BinaryEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.binaries.BinaryGtBatchMatcher;
import dev.hardwood.internal.predicate.matcher.binaries.BinaryGtEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.binaries.BinaryInBatchMatcher;
import dev.hardwood.internal.predicate.matcher.binaries.BinaryLtBatchMatcher;
import dev.hardwood.internal.predicate.matcher.binaries.BinaryLtEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.binaries.BinaryNotEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.doubles.DoubleEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.doubles.DoubleGtBatchMatcher;
import dev.hardwood.internal.predicate.matcher.doubles.DoubleGtEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.doubles.DoubleInBatchMatcher;
import dev.hardwood.internal.predicate.matcher.doubles.DoubleLtBatchMatcher;
import dev.hardwood.internal.predicate.matcher.doubles.DoubleLtEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.doubles.DoubleNotEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.floats.FloatInBatchMatcher;
import dev.hardwood.internal.predicate.matcher.longs.LongEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.longs.LongGtBatchMatcher;
import dev.hardwood.internal.predicate.matcher.longs.LongGtEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.longs.LongLtBatchMatcher;
import dev.hardwood.internal.predicate.matcher.longs.LongLtEqBatchMatcher;
import dev.hardwood.internal.predicate.matcher.longs.LongNotEqBatchMatcher;
import dev.hardwood.internal.reader.BatchExchange;
import dev.hardwood.internal.reader.BinaryBatchValues;

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
    void floatIn_matchesOnlyExactValuesAndExcludesNulls() {
        float[] vals = {0.5f, 1.5f, -0.0f, +0.0f, Float.NaN, 0.1f};
        BatchExchange.Batch batch = floatBatch(vals, nullsAt(1));
        float[] inValues = {1.5f, -0.0f, Float.NaN, 0.1f};
        assertArrayEquals(new long[]{bits(2, 4, 5)}, runMatcher(new FloatInBatchMatcher(inValues), batch));
    }

    @Test
    void floatIn_acrossWordBoundary_setsBitsInBothWords() {
        float[] vals = new float[70];
        for (int i = 0; i < vals.length; i++) {
            vals[i] = (float) i;
        }
        BatchExchange.Batch batch = floatBatch(vals, null);
        long[] out = runMatcher(new FloatInBatchMatcher(new float[]{5.0f, 65.0f}), batch);
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
    void floatIn_signedZeroAndNaNPrecision() {
        float[] vals = {+0.0f, -0.0f, Float.NaN};
        assertArrayEquals(new long[]{bits(0)},
                runMatcher(new FloatInBatchMatcher(new float[]{+0.0f}), floatBatch(vals, null)));
        assertArrayEquals(new long[]{bits(1)},
                runMatcher(new FloatInBatchMatcher(new float[]{-0.0f}), floatBatch(vals, null)));
        assertArrayEquals(new long[]{bits(2)},
                runMatcher(new FloatInBatchMatcher(new float[]{Float.NaN}), floatBatch(vals, null)));
    }

    @Test
    void floatIn_nanPayloadsAndInfinities() {
        float customFloatNan1 = Float.intBitsToFloat(0x7fc00001);
        float customFloatNan2 = Float.intBitsToFloat(0x7fc00042);
        float[] vals = {customFloatNan1, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, 1.0f};
        BatchExchange.Batch batch = floatBatch(vals, null);

        // Probe with canonical NaN matches customFloatNan1
        assertArrayEquals(new long[]{bits(0)},
                runMatcher(new FloatInBatchMatcher(new float[]{Float.NaN}), batch));

        // Probe with customFloatNan2 matches customFloatNan1
        assertArrayEquals(new long[]{bits(0)},
                runMatcher(new FloatInBatchMatcher(new float[]{customFloatNan2}), batch));

        // Probe with +Inf matches +Inf
        assertArrayEquals(new long[]{bits(1)},
                runMatcher(new FloatInBatchMatcher(new float[]{Float.POSITIVE_INFINITY}), batch));

        // Probe with -Inf matches -Inf
        assertArrayEquals(new long[]{bits(2)},
                runMatcher(new FloatInBatchMatcher(new float[]{Float.NEGATIVE_INFINITY}), batch));

        // Both infinities in probe list
        assertArrayEquals(new long[]{bits(1, 2)},
                runMatcher(new FloatInBatchMatcher(new float[]{Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY}), batch));
    }

    @Test
    void doubleIn_matchesAtTheLastBitOfAWordAndInTheMiddleWord() {
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
    void floatIn_matchesAtTheLastBitOfAWordAndInTheMiddleWord() {
        float[] vals64 = new float[64];
        vals64[63] = 42.0f;
        assertArrayEquals(new long[]{1L << 63},
                runMatcher(new FloatInBatchMatcher(new float[]{42.0f}), floatBatch(vals64, null)));

        float[] vals130 = new float[130];
        vals130[65] = 99.0f;
        long[] out = runMatcher(new FloatInBatchMatcher(new float[]{99.0f}), floatBatch(vals130, null));
        assertArrayEquals(new long[]{0L, 1L << 1, 0L}, out);
    }

    /// Packs `values` back to back into a [BinaryBatchValues], the layout `FlatColumnWorker`
    /// assembles. A `null` entry occupies an empty slice; nullness itself comes from `nulls`, so a
    /// row can be marked null while still holding bytes — the scratch a null fixed-length slot
    /// keeps.
    private static BatchExchange.Batch binaryBatch(BitSet nulls, byte[]... values) {
        int[] offsets = new int[values.length + 1];
        for (int i = 0; i < values.length; i++) {
            offsets[i + 1] = offsets[i] + (values[i] == null ? 0 : values[i].length);
        }
        byte[] bytes = new byte[offsets[values.length]];
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                System.arraycopy(values[i], 0, bytes, offsets[i], values[i].length);
            }
        }
        BatchExchange.Batch batch = new BatchExchange.Batch();
        batch.values = new BinaryBatchValues(bytes, offsets);
        batch.validity = toValidity(nulls, values.length);
        batch.recordCount = values.length;
        return batch;
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /// Big-endian two's complement in two bytes — a `FIXED_LEN_BYTE_ARRAY(2)` DECIMAL value.
    private static byte[] int16(int v) {
        return new byte[]{(byte) (v >> 8), (byte) v};
    }

    @Test
    void binaryEq_matchesExactBytesAndExcludesNulls() {
        BatchExchange.Batch batch = binaryBatch(nullsAt(2),
                utf8("apple"), utf8("app"), utf8("apple"), utf8("apples"), utf8(""));
        // Row 2 holds "apple" but is NULL → excluded. The prefix "app" and extension "apples" differ.
        assertArrayEquals(new long[]{bits(0)},
                runMatcher(new BinaryEqBatchMatcher(utf8("apple"), Comparison.BYTE_STRING), batch));
    }

    @Test
    void binaryNotEq_excludesNulls() {
        BatchExchange.Batch batch = binaryBatch(nullsAt(1),
                utf8("apple"), null, utf8("pear"), utf8(""));
        // Row 1 NULL → excluded (NULL != x is unknown → false).
        assertArrayEquals(new long[]{bits(2, 3)},
                runMatcher(new BinaryNotEqBatchMatcher(utf8("apple"), Comparison.BYTE_STRING), batch));
    }

    @Test
    void binaryOrderingOps_byteStringComparesUnsignedLexicographically() {
        // A proper prefix sorts before the literal; 0xFF sorts after every ASCII byte unsigned.
        BatchExchange.Batch batch = binaryBatch(null,
                utf8("app"), utf8("apple"), utf8("apples"), new byte[]{(byte) 0xFF}, utf8(""), utf8("b"));
        byte[] literal = utf8("apple");
        assertArrayEquals(new long[]{bits(0, 4)},
                runMatcher(new BinaryLtBatchMatcher(literal, Comparison.BYTE_STRING), batch));
        assertArrayEquals(new long[]{bits(0, 1, 4)},
                runMatcher(new BinaryLtEqBatchMatcher(literal, Comparison.BYTE_STRING), batch));
        assertArrayEquals(new long[]{bits(2, 3, 5)},
                runMatcher(new BinaryGtBatchMatcher(literal, Comparison.BYTE_STRING), batch));
        assertArrayEquals(new long[]{bits(1, 2, 3, 5)},
                runMatcher(new BinaryGtEqBatchMatcher(literal, Comparison.BYTE_STRING), batch));
    }

    @Test
    void binaryOrderingOps_fixedDecimalComparesAsTwosComplement() {
        // -1 (0xFFFF) and -256 (0xFF00) sort below zero only under signed order. Row 4 (-300) is NULL.
        BatchExchange.Batch batch = binaryBatch(nullsAt(4),
                int16(-1), int16(1), int16(-256), int16(256), int16(-300));
        byte[] zero = int16(0);
        assertArrayEquals(new long[]{bits(0, 2)},
                runMatcher(new BinaryLtBatchMatcher(zero, Comparison.FIXED_DECIMAL), batch));
        assertArrayEquals(new long[]{bits(1, 3)},
                runMatcher(new BinaryGtEqBatchMatcher(zero, Comparison.FIXED_DECIMAL), batch));
        // As a byte string, nothing sorts below 0x0000.
        assertArrayEquals(new long[]{0L},
                runMatcher(new BinaryLtBatchMatcher(zero, Comparison.BYTE_STRING), batch));
    }

    @Test
    void binaryFixedDecimal_skipsNullSlotsInsteadOfComparingThem() {
        // Row 1 is NULL with an empty slice, which reads as zero under a signed comparison — so
        // comparing it would answer against a value the row does not hold.
        BatchExchange.Batch batch = binaryBatch(nullsAt(1), int16(5), null, int16(-5));
        assertArrayEquals(new long[]{bits(0)},
                runMatcher(new BinaryGtBatchMatcher(int16(0), Comparison.FIXED_DECIMAL), batch));
        assertArrayEquals(new long[]{bits(2)},
                runMatcher(new BinaryNotEqBatchMatcher(int16(5), Comparison.FIXED_DECIMAL), batch));
    }

    /// A `BYTE_ARRAY` DECIMAL stores each value in the fewest bytes that hold it, so widths differ
    /// and length does not track magnitude: byte-wise `0x7F` would outrank `0x00 0x80`, and `0x9C`
    /// would fall below `0xFF 0x00`. Both are backwards, so the shorter value sign-extends first.
    @Test
    void binaryOrderingOps_variableDecimalSignExtendsBeforeComparing() {
        // 1.27 -> 7F, 1.28 -> 00 80, -1.00 -> 9C, -2.56 -> FF 00
        BatchExchange.Batch batch = binaryBatch(null,
                new byte[]{0x7F}, new byte[]{0x00, (byte) 0x80},
                new byte[]{(byte) 0x9C}, new byte[]{(byte) 0xFF, 0x00});
        byte[] oneTwentyEight = {0x00, (byte) 0x80};
        assertArrayEquals(new long[]{bits(0, 2, 3)},
                runMatcher(new BinaryLtBatchMatcher(oneTwentyEight, Comparison.VARIABLE_DECIMAL), batch));
        // -1.00 outranks -2.56 despite being the shorter string.
        assertArrayEquals(new long[]{bits(0, 1, 2)},
                runMatcher(new BinaryGtBatchMatcher(new byte[]{(byte) 0xFF, 0x00}, Comparison.VARIABLE_DECIMAL), batch));
    }

    /// The same number may be spelled with padding on a `BYTE_ARRAY` DECIMAL, so equality there
    /// cannot be byte equality: `00 7F` is another spelling of the `7F` the literal carries.
    @Test
    void binaryEq_variableDecimalMatchesAPaddedSpellingOfTheSameValue() {
        BatchExchange.Batch batch = binaryBatch(null,
                new byte[]{0x7F}, new byte[]{0x00, 0x7F}, new byte[]{0x00, 0x00, 0x7F}, new byte[]{(byte) 0x80});
        assertArrayEquals(new long[]{bits(0, 1, 2)},
                runMatcher(new BinaryEqBatchMatcher(new byte[]{0x7F}, Comparison.VARIABLE_DECIMAL), batch));
        // A byte string is exactly its bytes, so there the padded spellings are different values.
        assertArrayEquals(new long[]{bits(0)},
                runMatcher(new BinaryEqBatchMatcher(new byte[]{0x7F}, Comparison.BYTE_STRING), batch));
    }

    @Test
    void binaryGt_acrossWordBoundary_setsBitsInBothWords() {
        byte[][] vals = new byte[70][];
        for (int i = 0; i < vals.length; i++) {
            vals[i] = new byte[]{(byte) i}; // matches > 5 → rows 6..69
        }
        BatchExchange.Batch batch = binaryBatch(null, vals);
        long[] out = runMatcher(new BinaryGtBatchMatcher(new byte[]{5}, Comparison.BYTE_STRING), batch);
        long w0 = 0;
        for (int b = 6; b < 64; b++) w0 |= 1L << b;
        long w1 = 0;
        for (int b = 0; b < 6; b++) w1 |= 1L << b;
        assertArrayEquals(new long[]{w0, w1}, out);
    }

    @Test
    void binaryIn_matchesAnyMemberAndExcludesNulls() {
        BatchExchange.Batch batch = binaryBatch(nullsAt(3),
                utf8("a"), utf8("bb"), utf8(""), utf8("bb"), utf8("ccc"), utf8("b"));
        // Rows 1 ("bb") and 2 ("") are members; row 3 is NULL; "b" is only a prefix of "bb".
        byte[][] members = {utf8("bb"), utf8(""), utf8("zz")};
        assertArrayEquals(new long[]{bits(1, 2)},
                runMatcher(new BinaryInBatchMatcher(members, Comparison.BYTE_STRING), batch));
    }

    @Test
    void binaryIn_variableDecimalMatchesAPaddedSpellingOfAMember() {
        BatchExchange.Batch batch = binaryBatch(null,
                new byte[]{0x00, 0x7F}, new byte[]{(byte) 0xFF, (byte) 0x80}, new byte[]{0x01});
        byte[][] members = {new byte[]{0x7F}, new byte[]{(byte) 0x80}};
        // 0x00 0x7F is 127 and 0xFF 0x80 is -128: both members, spelled one byte wider.
        assertArrayEquals(new long[]{bits(0, 1)},
                runMatcher(new BinaryInBatchMatcher(members, Comparison.VARIABLE_DECIMAL), batch));
    }

}
