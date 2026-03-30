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
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.reader.FilterPredicate.Operator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecordFilterEvaluatorTest {

    // Column index 0 for all single-column tests
    private static final int COL_0 = 0;
    // Column indices for two-column tests
    private static final int COL_A = 0;
    private static final int COL_B = 1;

    @ParameterizedTest(name = "{0} {1} → row0={2}, row1={3}, row2={4}")
    @MethodSource
    void testIntComparison(Operator op, int predicateValue, boolean row0, boolean row1, boolean row2) {
        // rows: [10, 20, 30]
        Object[] values = { new int[]{ 10, 20, 30 } };
        BitSet[] nulls = { null };
        int[] mapping = identityMapping(1);

        ResolvedPredicate predicate = new ResolvedPredicate.IntPredicate(COL_0, op, predicateValue);

        assertMatch(row0, predicate, 0, values, nulls, mapping);
        assertMatch(row1, predicate, 1, values, nulls, mapping);
        assertMatch(row2, predicate, 2, values, nulls, mapping);
    }

    static Stream<Arguments> testIntComparison() {
        return Stream.of(
                Arguments.of(Operator.EQ,     20, false, true,  false),
                Arguments.of(Operator.NOT_EQ, 20, true,  false, true),
                Arguments.of(Operator.LT,     20, true,  false, false),
                Arguments.of(Operator.LT_EQ,  20, true,  true,  false),
                Arguments.of(Operator.GT,     20, false, false, true),
                Arguments.of(Operator.GT_EQ,  20, false, true,  true)
        );
    }

    @ParameterizedTest(name = "{0} {1} → row0={2}, row1={3}, row2={4}")
    @MethodSource
    void testLongComparison(Operator op, long predicateValue, boolean row0, boolean row1, boolean row2) {
        Object[] values = { new long[]{ 100L, 200L, 300L } };
        BitSet[] nulls = { null };
        int[] mapping = identityMapping(1);

        ResolvedPredicate predicate = new ResolvedPredicate.LongPredicate(COL_0, op, predicateValue);

        assertMatch(row0, predicate, 0, values, nulls, mapping);
        assertMatch(row1, predicate, 1, values, nulls, mapping);
        assertMatch(row2, predicate, 2, values, nulls, mapping);
    }

    static Stream<Arguments> testLongComparison() {
        return Stream.of(
                Arguments.of(Operator.EQ,     200L, false, true,  false),
                Arguments.of(Operator.NOT_EQ, 200L, true,  false, true),
                Arguments.of(Operator.LT,     200L, true,  false, false),
                Arguments.of(Operator.LT_EQ,  200L, true,  true,  false),
                Arguments.of(Operator.GT,     200L, false, false, true),
                Arguments.of(Operator.GT_EQ,  200L, false, true,  true)
        );
    }

    @ParameterizedTest(name = "{0} {1} → row0={2}, row1={3}, row2={4}")
    @MethodSource
    void testFloatComparison(Operator op, float predicateValue, boolean row0, boolean row1, boolean row2) {
        Object[] values = { new float[]{ 1.0f, 2.0f, 3.0f } };
        BitSet[] nulls = { null };
        int[] mapping = identityMapping(1);

        ResolvedPredicate predicate = new ResolvedPredicate.FloatPredicate(COL_0, op, predicateValue);

        assertMatch(row0, predicate, 0, values, nulls, mapping);
        assertMatch(row1, predicate, 1, values, nulls, mapping);
        assertMatch(row2, predicate, 2, values, nulls, mapping);
    }

    static Stream<Arguments> testFloatComparison() {
        return Stream.of(
                Arguments.of(Operator.EQ,     2.0f, false, true,  false),
                Arguments.of(Operator.NOT_EQ, 2.0f, true,  false, true),
                Arguments.of(Operator.LT,     2.0f, true,  false, false),
                Arguments.of(Operator.LT_EQ,  2.0f, true,  true,  false),
                Arguments.of(Operator.GT,     2.0f, false, false, true),
                Arguments.of(Operator.GT_EQ,  2.0f, false, true,  true)
        );
    }

    @ParameterizedTest(name = "{0} {1} → row0={2}, row1={3}, row2={4}")
    @MethodSource
    void testDoubleComparison(Operator op, double predicateValue, boolean row0, boolean row1, boolean row2) {
        Object[] values = { new double[]{ 10.0, 20.0, 30.0 } };
        BitSet[] nulls = { null };
        int[] mapping = identityMapping(1);

        ResolvedPredicate predicate = new ResolvedPredicate.DoublePredicate(COL_0, op, predicateValue);

        assertMatch(row0, predicate, 0, values, nulls, mapping);
        assertMatch(row1, predicate, 1, values, nulls, mapping);
        assertMatch(row2, predicate, 2, values, nulls, mapping);
    }

    static Stream<Arguments> testDoubleComparison() {
        return Stream.of(
                Arguments.of(Operator.EQ,     20.0, false, true,  false),
                Arguments.of(Operator.NOT_EQ, 20.0, true,  false, true),
                Arguments.of(Operator.LT,     20.0, true,  false, false),
                Arguments.of(Operator.LT_EQ,  20.0, true,  true,  false),
                Arguments.of(Operator.GT,     20.0, false, false, true),
                Arguments.of(Operator.GT_EQ,  20.0, false, true,  true)
        );
    }

    // ==================== Float/Double NaN and -0.0 ====================

    @Test
    void testFloatNaN() {
        // NaN is ordered after all other values by Float.compare
        Object[] values = { new float[]{ 1.0f, Float.NaN, -0.0f } };
        BitSet[] nulls = { null };
        int[] mapping = identityMapping(1);

        // EQ NaN: only NaN matches
        ResolvedPredicate eqNaN = new ResolvedPredicate.FloatPredicate(COL_0, Operator.EQ, Float.NaN);
        assertFalse(matches(eqNaN, 0, values, nulls, mapping));
        assertTrue(matches(eqNaN, 1, values, nulls, mapping));
        assertFalse(matches(eqNaN, 2, values, nulls, mapping));

        // GT 1.0f: NaN > 1.0f via Float.compare
        ResolvedPredicate gt1 = new ResolvedPredicate.FloatPredicate(COL_0, Operator.GT, 1.0f);
        assertFalse(matches(gt1, 0, values, nulls, mapping));
        assertTrue(matches(gt1, 1, values, nulls, mapping));
        assertFalse(matches(gt1, 2, values, nulls, mapping));
    }

    @Test
    void testFloatNegativeZero() {
        // -0.0f < +0.0f via Float.compare
        Object[] values = { new float[]{ -0.0f, 0.0f } };
        BitSet[] nulls = { null };
        int[] mapping = identityMapping(1);

        ResolvedPredicate lt0 = new ResolvedPredicate.FloatPredicate(COL_0, Operator.LT, 0.0f);
        assertTrue(matches(lt0, 0, values, nulls, mapping));
        assertFalse(matches(lt0, 1, values, nulls, mapping));

        ResolvedPredicate eq0 = new ResolvedPredicate.FloatPredicate(COL_0, Operator.EQ, -0.0f);
        assertTrue(matches(eq0, 0, values, nulls, mapping));
        assertFalse(matches(eq0, 1, values, nulls, mapping));
    }

    @Test
    void testDoubleNaN() {
        Object[] values = { new double[]{ 1.0, Double.NaN, Double.NEGATIVE_INFINITY } };
        BitSet[] nulls = { null };
        int[] mapping = identityMapping(1);

        ResolvedPredicate eqNaN = new ResolvedPredicate.DoublePredicate(COL_0, Operator.EQ, Double.NaN);
        assertFalse(matches(eqNaN, 0, values, nulls, mapping));
        assertTrue(matches(eqNaN, 1, values, nulls, mapping));
        assertFalse(matches(eqNaN, 2, values, nulls, mapping));

        // NaN > everything via Double.compare
        ResolvedPredicate gtNaN = new ResolvedPredicate.DoublePredicate(COL_0, Operator.GT, Double.NaN);
        assertFalse(matches(gtNaN, 0, values, nulls, mapping));
        assertFalse(matches(gtNaN, 1, values, nulls, mapping));
        assertFalse(matches(gtNaN, 2, values, nulls, mapping));
    }

    @Test
    void testDoubleNegativeZero() {
        Object[] values = { new double[]{ -0.0, 0.0 } };
        BitSet[] nulls = { null };
        int[] mapping = identityMapping(1);

        ResolvedPredicate lt0 = new ResolvedPredicate.DoublePredicate(COL_0, Operator.LT, 0.0);
        assertTrue(matches(lt0, 0, values, nulls, mapping));
        assertFalse(matches(lt0, 1, values, nulls, mapping));
    }

    // ==================== Boolean ====================

    @Test
    void testBooleanEq() {
        Object[] values = { new boolean[]{ true, false } };
        BitSet[] nulls = { null };
        int[] mapping = identityMapping(1);

        ResolvedPredicate eqTrue = new ResolvedPredicate.BooleanPredicate(COL_0, Operator.EQ, true);
        assertTrue(matches(eqTrue, 0, values, nulls, mapping));
        assertFalse(matches(eqTrue, 1, values, nulls, mapping));

        ResolvedPredicate eqFalse = new ResolvedPredicate.BooleanPredicate(COL_0, Operator.EQ, false);
        assertFalse(matches(eqFalse, 0, values, nulls, mapping));
        assertTrue(matches(eqFalse, 1, values, nulls, mapping));
    }

    @Test
    void testBooleanNotEq() {
        Object[] values = { new boolean[]{ true, false } };
        BitSet[] nulls = { null };
        int[] mapping = identityMapping(1);

        ResolvedPredicate notEqTrue = new ResolvedPredicate.BooleanPredicate(COL_0, Operator.NOT_EQ, true);
        assertFalse(matches(notEqTrue, 0, values, nulls, mapping));
        assertTrue(matches(notEqTrue, 1, values, nulls, mapping));
    }

    @Test
    void testBinaryEq() {
        Object[] values = { new byte[][]{ bytes("apple"), bytes("banana"), bytes("cherry") } };
        BitSet[] nulls = { null };
        int[] mapping = identityMapping(1);

        ResolvedPredicate eq = new ResolvedPredicate.BinaryPredicate(COL_0, Operator.EQ,
                bytes("banana"), false);
        assertFalse(matches(eq, 0, values, nulls, mapping));
        assertTrue(matches(eq, 1, values, nulls, mapping));
        assertFalse(matches(eq, 2, values, nulls, mapping));
    }

    @Test
    void testBinaryLt() {
        Object[] values = { new byte[][]{ bytes("apple"), bytes("banana"), bytes("cherry") } };
        BitSet[] nulls = { null };
        int[] mapping = identityMapping(1);

        ResolvedPredicate lt = new ResolvedPredicate.BinaryPredicate(COL_0, Operator.LT,
                bytes("banana"), false);
        assertTrue(matches(lt, 0, values, nulls, mapping));
        assertFalse(matches(lt, 1, values, nulls, mapping));
        assertFalse(matches(lt, 2, values, nulls, mapping));
    }

    @Test
    void testIntIn() {
        Object[] values = { new int[]{ 1, 2, 3, 4, 5 } };
        BitSet[] nulls = { null };
        int[] mapping = identityMapping(1);

        ResolvedPredicate in = new ResolvedPredicate.IntInPredicate(COL_0, new int[]{ 2, 4 });
        assertFalse(matches(in, 0, values, nulls, mapping));
        assertTrue(matches(in, 1, values, nulls, mapping));
        assertFalse(matches(in, 2, values, nulls, mapping));
        assertTrue(matches(in, 3, values, nulls, mapping));
        assertFalse(matches(in, 4, values, nulls, mapping));
    }

    @Test
    void testLongIn() {
        Object[] values = { new long[]{ 100L, 200L, 300L } };
        BitSet[] nulls = { null };
        int[] mapping = identityMapping(1);

        ResolvedPredicate in = new ResolvedPredicate.LongInPredicate(COL_0, new long[]{ 200L, 300L });
        assertFalse(matches(in, 0, values, nulls, mapping));
        assertTrue(matches(in, 1, values, nulls, mapping));
        assertTrue(matches(in, 2, values, nulls, mapping));
    }

    @Test
    void testBinaryIn() {
        Object[] values = { new byte[][]{ bytes("apple"), bytes("banana"), bytes("cherry") } };
        BitSet[] nulls = { null };
        int[] mapping = identityMapping(1);

        ResolvedPredicate in = new ResolvedPredicate.BinaryInPredicate(COL_0,
                new byte[][]{ bytes("apple"), bytes("cherry") });
        assertTrue(matches(in, 0, values, nulls, mapping));
        assertFalse(matches(in, 1, values, nulls, mapping));
        assertTrue(matches(in, 2, values, nulls, mapping));
    }

    @Test
    void testNullValueDoesNotMatch() {
        Object[] values = { new int[]{ 10, 0, 30 } };
        BitSet nullBits = new BitSet();
        nullBits.set(1); // row 1 is null
        BitSet[] nulls = { nullBits };
        int[] mapping = identityMapping(1);

        // Null row should not match any comparison
        ResolvedPredicate eq = new ResolvedPredicate.IntPredicate(COL_0, Operator.EQ, 0);
        assertFalse(matches(eq, 1, values, nulls, mapping));

        ResolvedPredicate gt = new ResolvedPredicate.IntPredicate(COL_0, Operator.GT, -1);
        assertFalse(matches(gt, 1, values, nulls, mapping));
    }

    @Test
    void testUnknownColumnReturnsTrue() {
        Object[] values = { new int[]{ 10, 20, 30 } };
        BitSet[] nulls = { null };
        // Column index 99 is not in the mapping (mapping only covers index 0)
        int[] mapping = identityMapping(1);

        ResolvedPredicate eq = new ResolvedPredicate.IntPredicate(99, Operator.EQ, 20);
        assertTrue(matches(eq, 0, values, nulls, mapping));
        assertTrue(matches(eq, 1, values, nulls, mapping));
    }

    @Test
    void testAndRequiresAllChildren() {
        // Two columns: col_a=[10, 20, 30], col_b=[100, 200, 300]
        Object[] values = { new int[]{ 10, 20, 30 }, new int[]{ 100, 200, 300 } };
        BitSet[] nulls = { null, null };
        int[] mapping = identityMapping(2);

        // AND(col_a > 15, col_b < 250)
        ResolvedPredicate and = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.IntPredicate(COL_A, Operator.GT, 15),
                new ResolvedPredicate.IntPredicate(COL_B, Operator.LT, 250)));

        assertFalse(matches(and, 0, values, nulls, mapping)); // 10 > 15 false
        assertTrue(matches(and, 1, values, nulls, mapping));   // 20 > 15 && 200 < 250
        assertFalse(matches(and, 2, values, nulls, mapping)); // 300 < 250 false
    }

    @Test
    void testOrRequiresAnyChild() {
        Object[] values = { new int[]{ 10, 20, 30 } };
        BitSet[] nulls = { null };
        int[] mapping = identityMapping(1);

        // OR(col == 10, col == 30)
        ResolvedPredicate or = new ResolvedPredicate.Or(List.of(
                new ResolvedPredicate.IntPredicate(COL_0, Operator.EQ, 10),
                new ResolvedPredicate.IntPredicate(COL_0, Operator.EQ, 30)));

        assertTrue(matches(or, 0, values, nulls, mapping));
        assertFalse(matches(or, 1, values, nulls, mapping));
        assertTrue(matches(or, 2, values, nulls, mapping));
    }

    @Test
    void testNotNegatesDelegate() {
        Object[] values = { new int[]{ 10, 20, 30 } };
        BitSet[] nulls = { null };
        int[] mapping = identityMapping(1);

        // NOT_EQ(col, 20) → row 0 and 2 match (NOT is resolved away during predicate resolution)
        ResolvedPredicate notEq = new ResolvedPredicate.IntPredicate(COL_0, Operator.NOT_EQ, 20);

        assertTrue(matches(notEq, 0, values, nulls, mapping));
        assertFalse(matches(notEq, 1, values, nulls, mapping));
        assertTrue(matches(notEq, 2, values, nulls, mapping));
    }

    @ParameterizedTest(name = "matchBatch {0} {1} → cardinality={2}")
    @MethodSource
    void testMatchBatchOperators(Operator op, int predicateValue, int expectedCardinality) {
        Object[] values = { new int[]{ 10, 20, 30 } };
        BitSet[] nulls = { null };
        int[] mapping = identityMapping(1);

        ResolvedPredicate predicate = new ResolvedPredicate.IntPredicate(COL_0, op, predicateValue);

        BitSet result = RecordFilterEvaluator.matchBatch(predicate, 3, values, nulls, mapping);
        assertThat(result.cardinality()).isEqualTo(expectedCardinality);
    }

    static Stream<Arguments> testMatchBatchOperators() {
        return Stream.of(
                Arguments.of(Operator.EQ,     20, 1),
                Arguments.of(Operator.NOT_EQ, 20, 2),
                Arguments.of(Operator.LT,     20, 1),
                Arguments.of(Operator.LT_EQ,  20, 2),
                Arguments.of(Operator.GT,     20, 1),
                Arguments.of(Operator.GT_EQ,  20, 2),
                // Edge cases
                Arguments.of(Operator.EQ,    999, 0),  // no matches
                Arguments.of(Operator.GT,      0, 3)   // all match
        );
    }

    @Test
    void testMatchBatchAnd() {
        // col_a=[10, 20, 30], col_b=[100, 200, 300]
        Object[] values = { new int[]{ 10, 20, 30 }, new int[]{ 100, 200, 300 } };
        BitSet[] nulls = { null, null };
        int[] mapping = identityMapping(2);

        // AND(col_a > 15, col_b < 250) → only row 1 matches
        ResolvedPredicate predicate = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.IntPredicate(COL_A, Operator.GT, 15),
                new ResolvedPredicate.IntPredicate(COL_B, Operator.LT, 250)));
        BitSet result = RecordFilterEvaluator.matchBatch(predicate, 3, values, nulls, mapping);

        assertFalse(result.get(0));
        assertTrue(result.get(1));
        assertFalse(result.get(2));
    }

    @Test
    void testMatchBatchOr() {
        Object[] values = { new int[]{ 10, 20, 30 } };
        BitSet[] nulls = { null };
        int[] mapping = identityMapping(1);

        // OR(col == 10, col == 30) → rows 0, 2
        ResolvedPredicate predicate = new ResolvedPredicate.Or(List.of(
                new ResolvedPredicate.IntPredicate(COL_0, Operator.EQ, 10),
                new ResolvedPredicate.IntPredicate(COL_0, Operator.EQ, 30)));
        BitSet result = RecordFilterEvaluator.matchBatch(predicate, 3, values, nulls, mapping);

        assertTrue(result.get(0));
        assertFalse(result.get(1));
        assertTrue(result.get(2));
    }

    @Test
    void testMatchBatchNot() {
        Object[] values = { new int[]{ 10, 20, 30 } };
        BitSet[] nulls = { null };
        int[] mapping = identityMapping(1);

        // NOT_EQ(col, 20) → rows 0, 2 (NOT is resolved away during predicate resolution)
        ResolvedPredicate predicate = new ResolvedPredicate.IntPredicate(COL_0, Operator.NOT_EQ, 20);
        BitSet result = RecordFilterEvaluator.matchBatch(predicate, 3, values, nulls, mapping);

        assertTrue(result.get(0));
        assertFalse(result.get(1));
        assertTrue(result.get(2));
    }

    @Test
    void testMatchBatchWithNulls() {
        Object[] values = { new int[]{ 10, 0, 30, 40 } };
        BitSet nullBits = new BitSet();
        nullBits.set(1); // row 1 is null
        BitSet[] nulls = { nullBits };
        int[] mapping = identityMapping(1);

        ResolvedPredicate predicate = new ResolvedPredicate.IntPredicate(COL_0, Operator.GT, 0);
        BitSet result = RecordFilterEvaluator.matchBatch(predicate, 4, values, nulls, mapping);

        assertTrue(result.get(0));
        assertFalse(result.get(1)); // null row excluded
        assertTrue(result.get(2));
        assertTrue(result.get(3));
        assertThat(result.cardinality()).isEqualTo(3);
    }

    @Test
    void testMatchBatchWithUnknownColumn() {
        Object[] values = { new int[]{ 10, 20, 30 } };
        BitSet[] nulls = { null };
        int[] mapping = identityMapping(1);

        // Filter on column index 99 which is not in mapping → all match (conservative)
        ResolvedPredicate predicate = new ResolvedPredicate.IntPredicate(99, Operator.EQ, 20);
        BitSet result = RecordFilterEvaluator.matchBatch(predicate, 3, values, nulls, mapping);

        assertThat(result.cardinality()).isEqualTo(3);
    }

    @Test
    void testIsNullMatchesNullRows() {
        Object[] values = { new int[]{ 10, 0, 30 } };
        BitSet nullBits = new BitSet();
        nullBits.set(1); // row 1 is null
        BitSet[] nulls = { nullBits };
        int[] mapping = identityMapping(1);

        ResolvedPredicate isNull = new ResolvedPredicate.IsNullPredicate(COL_0);
        assertFalse(matches(isNull, 0, values, nulls, mapping));
        assertTrue(matches(isNull, 1, values, nulls, mapping));
        assertFalse(matches(isNull, 2, values, nulls, mapping));
    }

    @Test
    void testIsNotNullMatchesNonNullRows() {
        Object[] values = { new int[]{ 10, 0, 30 } };
        BitSet nullBits = new BitSet();
        nullBits.set(1); // row 1 is null
        BitSet[] nulls = { nullBits };
        int[] mapping = identityMapping(1);

        ResolvedPredicate isNotNull = new ResolvedPredicate.IsNotNullPredicate(COL_0);
        assertTrue(matches(isNotNull, 0, values, nulls, mapping));
        assertFalse(matches(isNotNull, 1, values, nulls, mapping));
        assertTrue(matches(isNotNull, 2, values, nulls, mapping));
    }

    @Test
    void testIsNullOnRequiredColumnNeverMatches() {
        // Required column has null BitSet == null (no nulls possible)
        Object[] values = { new int[]{ 10, 20, 30 } };
        BitSet[] nulls = { null };
        int[] mapping = identityMapping(1);

        ResolvedPredicate isNull = new ResolvedPredicate.IsNullPredicate(COL_0);
        assertFalse(matches(isNull, 0, values, nulls, mapping));
        assertFalse(matches(isNull, 1, values, nulls, mapping));
        assertFalse(matches(isNull, 2, values, nulls, mapping));
    }

    @Test
    void testIsNotNullOnRequiredColumnAlwaysMatches() {
        // Required column has null BitSet == null (no nulls possible)
        Object[] values = { new int[]{ 10, 20, 30 } };
        BitSet[] nulls = { null };
        int[] mapping = identityMapping(1);

        ResolvedPredicate isNotNull = new ResolvedPredicate.IsNotNullPredicate(COL_0);
        assertTrue(matches(isNotNull, 0, values, nulls, mapping));
        assertTrue(matches(isNotNull, 1, values, nulls, mapping));
        assertTrue(matches(isNotNull, 2, values, nulls, mapping));
    }

    @Test
    void testIsNullOnUnknownColumnIsConservative() {
        Object[] values = { new int[]{ 10 } };
        BitSet[] nulls = { null };
        int[] mapping = identityMapping(1);

        // Column index 99 is not in mapping
        ResolvedPredicate isNull = new ResolvedPredicate.IsNullPredicate(99);
        assertTrue(matches(isNull, 0, values, nulls, mapping));

        ResolvedPredicate isNotNull = new ResolvedPredicate.IsNotNullPredicate(99);
        assertTrue(matches(isNotNull, 0, values, nulls, mapping));
    }

    @Test
    void testMatchBatchIsNull() {
        Object[] values = { new int[]{ 10, 0, 30, 0 } };
        BitSet nullBits = new BitSet();
        nullBits.set(1);
        nullBits.set(3);
        BitSet[] nulls = { nullBits };
        int[] mapping = identityMapping(1);

        ResolvedPredicate isNull = new ResolvedPredicate.IsNullPredicate(COL_0);
        BitSet result = RecordFilterEvaluator.matchBatch(isNull, 4, values, nulls, mapping);
        assertThat(result.cardinality()).isEqualTo(2);
        assertFalse(result.get(0));
        assertTrue(result.get(1));
        assertFalse(result.get(2));
        assertTrue(result.get(3));
    }

    private static boolean matches(ResolvedPredicate predicate, int rowIndex,
            Object[] values, BitSet[] nulls, int[] mapping) {
        return RecordFilterEvaluator.matches(predicate, rowIndex, values, nulls, mapping);
    }

    private static void assertMatch(boolean expected, ResolvedPredicate predicate, int rowIndex,
            Object[] values, BitSet[] nulls, int[] mapping) {
        boolean actual = matches(predicate, rowIndex, values, nulls, mapping);
        if (expected) {
            assertTrue(actual, "Expected row " + rowIndex + " to match");
        }
        else {
            assertFalse(actual, "Expected row " + rowIndex + " to not match");
        }
    }

    /// Creates an identity column mapping where schema column index i maps to projected index i.
    private static int[] identityMapping(int size) {
        int[] mapping = new int[size];
        for (int i = 0; i < size; i++) {
            mapping[i] = i;
        }
        return mapping;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
