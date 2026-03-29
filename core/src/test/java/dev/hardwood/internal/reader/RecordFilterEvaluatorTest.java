/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.internal.util.StringToIntMap;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.FilterPredicate.Operator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecordFilterEvaluatorTest {

    @ParameterizedTest(name = "{0} {1} → row0={2}, row1={3}, row2={4}")
    @MethodSource
    void testIntComparison(Operator op, int predicateValue, boolean row0, boolean row1, boolean row2) {
        // rows: [10, 20, 30]
        Object[] values = { new int[]{ 10, 20, 30 } };
        BitSet[] nulls = { null };
        StringToIntMap names = nameCache("col");

        FilterPredicate predicate = switch (op) {
            case EQ -> FilterPredicate.eq("col", predicateValue);
            case NOT_EQ -> FilterPredicate.notEq("col", predicateValue);
            case LT -> FilterPredicate.lt("col", predicateValue);
            case LT_EQ -> FilterPredicate.ltEq("col", predicateValue);
            case GT -> FilterPredicate.gt("col", predicateValue);
            case GT_EQ -> FilterPredicate.gtEq("col", predicateValue);
        };

        assertMatch(row0, predicate, 0, values, nulls, names);
        assertMatch(row1, predicate, 1, values, nulls, names);
        assertMatch(row2, predicate, 2, values, nulls, names);
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
        StringToIntMap names = nameCache("col");

        FilterPredicate predicate = switch (op) {
            case EQ -> FilterPredicate.eq("col", predicateValue);
            case NOT_EQ -> FilterPredicate.notEq("col", predicateValue);
            case LT -> FilterPredicate.lt("col", predicateValue);
            case LT_EQ -> FilterPredicate.ltEq("col", predicateValue);
            case GT -> FilterPredicate.gt("col", predicateValue);
            case GT_EQ -> FilterPredicate.gtEq("col", predicateValue);
        };

        assertMatch(row0, predicate, 0, values, nulls, names);
        assertMatch(row1, predicate, 1, values, nulls, names);
        assertMatch(row2, predicate, 2, values, nulls, names);
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
        StringToIntMap names = nameCache("col");

        FilterPredicate predicate = switch (op) {
            case EQ -> FilterPredicate.eq("col", predicateValue);
            case NOT_EQ -> FilterPredicate.notEq("col", predicateValue);
            case LT -> FilterPredicate.lt("col", predicateValue);
            case LT_EQ -> FilterPredicate.ltEq("col", predicateValue);
            case GT -> FilterPredicate.gt("col", predicateValue);
            case GT_EQ -> FilterPredicate.gtEq("col", predicateValue);
        };

        assertMatch(row0, predicate, 0, values, nulls, names);
        assertMatch(row1, predicate, 1, values, nulls, names);
        assertMatch(row2, predicate, 2, values, nulls, names);
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
        StringToIntMap names = nameCache("col");

        FilterPredicate predicate = switch (op) {
            case EQ -> FilterPredicate.eq("col", predicateValue);
            case NOT_EQ -> FilterPredicate.notEq("col", predicateValue);
            case LT -> FilterPredicate.lt("col", predicateValue);
            case LT_EQ -> FilterPredicate.ltEq("col", predicateValue);
            case GT -> FilterPredicate.gt("col", predicateValue);
            case GT_EQ -> FilterPredicate.gtEq("col", predicateValue);
        };

        assertMatch(row0, predicate, 0, values, nulls, names);
        assertMatch(row1, predicate, 1, values, nulls, names);
        assertMatch(row2, predicate, 2, values, nulls, names);
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

    @Test
    void testBooleanEq() {
        Object[] values = { new boolean[]{ true, false } };
        BitSet[] nulls = { null };
        StringToIntMap names = nameCache("col");

        FilterPredicate eqTrue = FilterPredicate.eq("col", true);
        assertTrue(matches(eqTrue, 0, values, nulls, names));
        assertFalse(matches(eqTrue, 1, values, nulls, names));

        FilterPredicate eqFalse = FilterPredicate.eq("col", false);
        assertFalse(matches(eqFalse, 0, values, nulls, names));
        assertTrue(matches(eqFalse, 1, values, nulls, names));
    }

    @Test
    void testBooleanNotEq() {
        Object[] values = { new boolean[]{ true, false } };
        BitSet[] nulls = { null };
        StringToIntMap names = nameCache("col");

        FilterPredicate notEqTrue = FilterPredicate.notEq("col", true);
        assertFalse(matches(notEqTrue, 0, values, nulls, names));
        assertTrue(matches(notEqTrue, 1, values, nulls, names));
    }

    @Test
    void testBinaryEq() {
        Object[] values = { new byte[][]{ bytes("apple"), bytes("banana"), bytes("cherry") } };
        BitSet[] nulls = { null };
        StringToIntMap names = nameCache("col");

        FilterPredicate eq = FilterPredicate.eq("col", "banana");
        assertFalse(matches(eq, 0, values, nulls, names));
        assertTrue(matches(eq, 1, values, nulls, names));
        assertFalse(matches(eq, 2, values, nulls, names));
    }

    @Test
    void testBinaryLt() {
        Object[] values = { new byte[][]{ bytes("apple"), bytes("banana"), bytes("cherry") } };
        BitSet[] nulls = { null };
        StringToIntMap names = nameCache("col");

        FilterPredicate lt = FilterPredicate.lt("col", "banana");
        assertTrue(matches(lt, 0, values, nulls, names));
        assertFalse(matches(lt, 1, values, nulls, names));
        assertFalse(matches(lt, 2, values, nulls, names));
    }

    @Test
    void testIntIn() {
        Object[] values = { new int[]{ 1, 2, 3, 4, 5 } };
        BitSet[] nulls = { null };
        StringToIntMap names = nameCache("col");

        FilterPredicate in = FilterPredicate.in("col", 2, 4);
        assertFalse(matches(in, 0, values, nulls, names));
        assertTrue(matches(in, 1, values, nulls, names));
        assertFalse(matches(in, 2, values, nulls, names));
        assertTrue(matches(in, 3, values, nulls, names));
        assertFalse(matches(in, 4, values, nulls, names));
    }

    @Test
    void testLongIn() {
        Object[] values = { new long[]{ 100L, 200L, 300L } };
        BitSet[] nulls = { null };
        StringToIntMap names = nameCache("col");

        FilterPredicate in = FilterPredicate.in("col", 200L, 300L);
        assertFalse(matches(in, 0, values, nulls, names));
        assertTrue(matches(in, 1, values, nulls, names));
        assertTrue(matches(in, 2, values, nulls, names));
    }

    @Test
    void testBinaryIn() {
        Object[] values = { new byte[][]{ bytes("apple"), bytes("banana"), bytes("cherry") } };
        BitSet[] nulls = { null };
        StringToIntMap names = nameCache("col");

        FilterPredicate in = FilterPredicate.inStrings("col", "apple", "cherry");
        assertTrue(matches(in, 0, values, nulls, names));
        assertFalse(matches(in, 1, values, nulls, names));
        assertTrue(matches(in, 2, values, nulls, names));
    }

    @Test
    void testNullValueDoesNotMatch() {
        Object[] values = { new int[]{ 10, 0, 30 } };
        BitSet nullBits = new BitSet();
        nullBits.set(1); // row 1 is null
        BitSet[] nulls = { nullBits };
        StringToIntMap names = nameCache("col");

        // Null row should not match any comparison
        FilterPredicate eq = FilterPredicate.eq("col", 0);
        assertFalse(matches(eq, 1, values, nulls, names));

        FilterPredicate gt = FilterPredicate.gt("col", -1);
        assertFalse(matches(gt, 1, values, nulls, names));
    }

    @Test
    void testUnknownColumnReturnsTrue() {
        Object[] values = { new int[]{ 10, 20, 30 } };
        BitSet[] nulls = { null };
        StringToIntMap names = nameCache("other_col");

        // Filter on "col" but only "other_col" is in projection — conservative match
        FilterPredicate eq = FilterPredicate.eq("col", 20);
        assertTrue(matches(eq, 0, values, nulls, names));
        assertTrue(matches(eq, 1, values, nulls, names));
    }

    @Test
    void testAndRequiresAllChildren() {
        // Two columns: col_a=[10, 20, 30], col_b=[100, 200, 300]
        Object[] values = { new int[]{ 10, 20, 30 }, new int[]{ 100, 200, 300 } };
        BitSet[] nulls = { null, null };
        StringToIntMap names = nameCache("col_a", "col_b");

        // AND(col_a > 15, col_b < 250)
        FilterPredicate and = FilterPredicate.and(
                FilterPredicate.gt("col_a", 15),
                FilterPredicate.lt("col_b", 250));

        assertFalse(matches(and, 0, values, nulls, names)); // 10 > 15 false
        assertTrue(matches(and, 1, values, nulls, names));   // 20 > 15 && 200 < 250
        assertFalse(matches(and, 2, values, nulls, names)); // 300 < 250 false
    }

    @Test
    void testOrRequiresAnyChild() {
        Object[] values = { new int[]{ 10, 20, 30 } };
        BitSet[] nulls = { null };
        StringToIntMap names = nameCache("col");

        // OR(col == 10, col == 30)
        FilterPredicate or = FilterPredicate.or(
                FilterPredicate.eq("col", 10),
                FilterPredicate.eq("col", 30));

        assertTrue(matches(or, 0, values, nulls, names));
        assertFalse(matches(or, 1, values, nulls, names));
        assertTrue(matches(or, 2, values, nulls, names));
    }

    @Test
    void testNotNegatesDelegate() {
        Object[] values = { new int[]{ 10, 20, 30 } };
        BitSet[] nulls = { null };
        StringToIntMap names = nameCache("col");

        // NOT(col == 20) → row 0 and 2 match
        FilterPredicate not = FilterPredicate.not(FilterPredicate.eq("col", 20));

        assertTrue(matches(not, 0, values, nulls, names));
        assertFalse(matches(not, 1, values, nulls, names));
        assertTrue(matches(not, 2, values, nulls, names));
    }

    private static boolean matches(FilterPredicate predicate, int rowIndex,
            Object[] values, BitSet[] nulls, StringToIntMap names) {
        return RecordFilterEvaluator.matches(predicate, rowIndex, values, nulls, names);
    }

    private static void assertMatch(boolean expected, FilterPredicate predicate, int rowIndex,
            Object[] values, BitSet[] nulls, StringToIntMap names) {
        boolean actual = matches(predicate, rowIndex, values, nulls, names);
        if (expected) {
            assertTrue(actual, "Expected row " + rowIndex + " to match");
        }
        else {
            assertFalse(actual, "Expected row " + rowIndex + " to not match");
        }
    }

    private static StringToIntMap nameCache(String... names) {
        StringToIntMap map = new StringToIntMap(names.length);
        for (int i = 0; i < names.length; i++) {
            map.put(names[i], i);
        }
        return map;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
