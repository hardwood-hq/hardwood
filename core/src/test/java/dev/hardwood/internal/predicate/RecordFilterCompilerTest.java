/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.row.PqDoubleList;
import dev.hardwood.row.PqIntList;
import dev.hardwood.row.PqInterval;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqLongList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.row.StructAccessor;
import dev.hardwood.schema.FileSchema;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecordFilterCompilerTest {

    // ==================== Int ====================

    @ParameterizedTest(name = "{0} {1} → {2}")
    @MethodSource
    void testIntComparison(Operator op, int predicateValue, boolean expected) {
        FileSchema schema = intSchema("col");
        StructAccessor row = stub("col", 20, false);
        ResolvedPredicate predicate = new ResolvedPredicate.IntPredicate(0, op, predicateValue);
        assertMatch(expected, predicate, row, schema);
    }

    static Stream<Arguments> testIntComparison() {
        return Stream.of(
                Arguments.of(Operator.EQ,     20, true),
                Arguments.of(Operator.EQ,     10, false),
                Arguments.of(Operator.NOT_EQ, 20, false),
                Arguments.of(Operator.NOT_EQ, 10, true),
                Arguments.of(Operator.LT,     30, true),
                Arguments.of(Operator.LT,     20, false),
                Arguments.of(Operator.LT_EQ,  20, true),
                Arguments.of(Operator.LT_EQ,  19, false),
                Arguments.of(Operator.GT,     10, true),
                Arguments.of(Operator.GT,     20, false),
                Arguments.of(Operator.GT_EQ,  20, true),
                Arguments.of(Operator.GT_EQ,  21, false)
        );
    }

    // ==================== Long ====================

    @ParameterizedTest(name = "{0} {1} → {2}")
    @MethodSource
    void testLongComparison(Operator op, long predicateValue, boolean expected) {
        FileSchema schema = longSchema("col");
        StructAccessor row = longStub("col", 200L, false);
        ResolvedPredicate predicate = new ResolvedPredicate.LongPredicate(0, op, predicateValue);
        assertMatch(expected, predicate, row, schema);
    }

    static Stream<Arguments> testLongComparison() {
        return Stream.of(
                Arguments.of(Operator.EQ,     200L, true),
                Arguments.of(Operator.EQ,     100L, false),
                Arguments.of(Operator.NOT_EQ, 200L, false),
                Arguments.of(Operator.LT,     300L, true),
                Arguments.of(Operator.LT,     200L, false),
                Arguments.of(Operator.GT,     100L, true),
                Arguments.of(Operator.GT,     200L, false)
        );
    }

    // ==================== Float ====================

    @ParameterizedTest(name = "{0} {1} → {2}")
    @MethodSource
    void testFloatComparison(Operator op, float predicateValue, boolean expected) {
        FileSchema schema = floatSchema("col");
        StructAccessor row = floatStub("col", 2.0f, false);
        ResolvedPredicate predicate = new ResolvedPredicate.FloatPredicate(0, op, predicateValue);
        assertMatch(expected, predicate, row, schema);
    }

    static Stream<Arguments> testFloatComparison() {
        return Stream.of(
                Arguments.of(Operator.EQ,     2.0f, true),
                Arguments.of(Operator.EQ,     1.0f, false),
                Arguments.of(Operator.LT,     3.0f, true),
                Arguments.of(Operator.LT,     2.0f, false),
                Arguments.of(Operator.GT,     1.0f, true),
                Arguments.of(Operator.GT,     2.0f, false)
        );
    }

    @Test
    void testFloatNaN() {
        FileSchema schema = floatSchema("col");
        // NaN is ordered after all other values by Float.compare
        ResolvedPredicate eqNaN = new ResolvedPredicate.FloatPredicate(0, Operator.EQ, Float.NaN);
        assertTrue(matchesRow(eqNaN, floatStub("col", Float.NaN, false), schema));
        assertFalse(matchesRow(eqNaN, floatStub("col", 1.0f, false), schema));

        // GT 1.0f: NaN > 1.0f via Float.compare
        ResolvedPredicate gt1 = new ResolvedPredicate.FloatPredicate(0, Operator.GT, 1.0f);
        assertTrue(matchesRow(gt1, floatStub("col", Float.NaN, false), schema));
    }

    @Test
    void testFloatNegativeZero() {
        FileSchema schema = floatSchema("col");
        // -0.0f < +0.0f via Float.compare
        ResolvedPredicate lt0 = new ResolvedPredicate.FloatPredicate(0, Operator.LT, 0.0f);
        assertTrue(matchesRow(lt0, floatStub("col", -0.0f, false), schema));
        assertFalse(matchesRow(lt0, floatStub("col", 0.0f, false), schema));
    }

    // ==================== Double ====================

    @ParameterizedTest(name = "{0} {1} → {2}")
    @MethodSource
    void testDoubleComparison(Operator op, double predicateValue, boolean expected) {
        FileSchema schema = doubleSchema("col");
        StructAccessor row = doubleStub("col", 20.0, false);
        ResolvedPredicate predicate = new ResolvedPredicate.DoublePredicate(0, op, predicateValue);
        assertMatch(expected, predicate, row, schema);
    }

    static Stream<Arguments> testDoubleComparison() {
        return Stream.of(
                Arguments.of(Operator.EQ,     20.0, true),
                Arguments.of(Operator.EQ,     10.0, false),
                Arguments.of(Operator.LT,     30.0, true),
                Arguments.of(Operator.LT,     20.0, false),
                Arguments.of(Operator.GT,     10.0, true),
                Arguments.of(Operator.GT,     20.0, false)
        );
    }

    @Test
    void testDoubleNaN() {
        FileSchema schema = doubleSchema("col");
        ResolvedPredicate eqNaN = new ResolvedPredicate.DoublePredicate(0, Operator.EQ, Double.NaN);
        assertTrue(matchesRow(eqNaN, doubleStub("col", Double.NaN, false), schema));
        assertFalse(matchesRow(eqNaN, doubleStub("col", 1.0, false), schema));

        // NaN > everything via Double.compare
        ResolvedPredicate gtNaN = new ResolvedPredicate.DoublePredicate(0, Operator.GT, Double.NaN);
        assertFalse(matchesRow(gtNaN, doubleStub("col", Double.NaN, false), schema));
        assertFalse(matchesRow(gtNaN, doubleStub("col", Double.NEGATIVE_INFINITY, false), schema));
    }

    @Test
    void testDoubleNegativeZero() {
        FileSchema schema = doubleSchema("col");
        ResolvedPredicate lt0 = new ResolvedPredicate.DoublePredicate(0, Operator.LT, 0.0);
        assertTrue(matchesRow(lt0, doubleStub("col", -0.0, false), schema));
        assertFalse(matchesRow(lt0, doubleStub("col", 0.0, false), schema));
    }

    // ==================== Boolean ====================

    @Test
    void testBooleanEq() {
        FileSchema schema = booleanSchema("col");
        ResolvedPredicate eqTrue = new ResolvedPredicate.BooleanPredicate(0, Operator.EQ, true);
        assertTrue(matchesRow(eqTrue, booleanStub("col", true, false), schema));
        assertFalse(matchesRow(eqTrue, booleanStub("col", false, false), schema));
    }

    @Test
    void testBooleanNotEq() {
        FileSchema schema = booleanSchema("col");
        ResolvedPredicate notEqTrue = new ResolvedPredicate.BooleanPredicate(0, Operator.NOT_EQ, true);
        assertFalse(matchesRow(notEqTrue, booleanStub("col", true, false), schema));
        assertTrue(matchesRow(notEqTrue, booleanStub("col", false, false), schema));
    }

    // ==================== Binary ====================

    @Test
    void testBinaryEq() {
        FileSchema schema = binarySchema("col");
        ResolvedPredicate eq = new ResolvedPredicate.BinaryPredicate(0, Operator.EQ, bytes("banana"), false);
        assertTrue(matchesRow(eq, binaryStub("col", bytes("banana"), false), schema));
        assertFalse(matchesRow(eq, binaryStub("col", bytes("apple"), false), schema));
    }

    @Test
    void testBinaryLt() {
        FileSchema schema = binarySchema("col");
        ResolvedPredicate lt = new ResolvedPredicate.BinaryPredicate(0, Operator.LT, bytes("banana"), false);
        assertTrue(matchesRow(lt, binaryStub("col", bytes("apple"), false), schema));
        assertFalse(matchesRow(lt, binaryStub("col", bytes("cherry"), false), schema));
    }

    // ==================== IN predicates ====================

    @Test
    void testIntIn() {
        FileSchema schema = intSchema("col");
        ResolvedPredicate in = new ResolvedPredicate.IntInPredicate(0, new int[]{ 2, 4 });
        assertTrue(matchesRow(in, stub("col", 2, false), schema));
        assertTrue(matchesRow(in, stub("col", 4, false), schema));
        assertFalse(matchesRow(in, stub("col", 3, false), schema));
    }

    @Test
    void testLongIn() {
        FileSchema schema = longSchema("col");
        ResolvedPredicate in = new ResolvedPredicate.LongInPredicate(0, new long[]{ 200L, 300L });
        assertTrue(matchesRow(in, longStub("col", 200L, false), schema));
        assertFalse(matchesRow(in, longStub("col", 100L, false), schema));
    }

    @Test
    void testBinaryIn() {
        FileSchema schema = binarySchema("col");
        ResolvedPredicate in = new ResolvedPredicate.BinaryInPredicate(0, new byte[][]{ bytes("apple"), bytes("cherry") });
        assertTrue(matchesRow(in, binaryStub("col", bytes("apple"), false), schema));
        assertFalse(matchesRow(in, binaryStub("col", bytes("banana"), false), schema));
    }

    // ==================== Null handling ====================

    @Test
    void testNullValueDoesNotMatch() {
        FileSchema schema = intSchema("col");
        StructAccessor nullRow = stub("col", 0, true);
        ResolvedPredicate eq = new ResolvedPredicate.IntPredicate(0, Operator.EQ, 0);
        assertFalse(matchesRow(eq, nullRow, schema));
    }

    @Test
    void testIsNullMatchesNullRows() {
        FileSchema schema = intSchema("col");
        ResolvedPredicate isNull = new ResolvedPredicate.IsNullPredicate(0);
        assertTrue(matchesRow(isNull, stub("col", 0, true), schema));
        assertFalse(matchesRow(isNull, stub("col", 10, false), schema));
    }

    @Test
    void testIsNotNullMatchesNonNullRows() {
        FileSchema schema = intSchema("col");
        ResolvedPredicate isNotNull = new ResolvedPredicate.IsNotNullPredicate(0);
        assertFalse(matchesRow(isNotNull, stub("col", 0, true), schema));
        assertTrue(matchesRow(isNotNull, stub("col", 10, false), schema));
    }

    // ==================== AND / OR ====================

    @Test
    void testAndRequiresAllChildren() {
        FileSchema schema = twoIntSchema("a", "b");
        ResolvedPredicate and = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.IntPredicate(0, Operator.GT, 15),
                new ResolvedPredicate.IntPredicate(1, Operator.LT, 250)));

        assertTrue(matchesRow(and, twoIntStub("a", 20, "b", 200), schema));
        assertFalse(matchesRow(and, twoIntStub("a", 10, "b", 200), schema));   // first fails
        assertFalse(matchesRow(and, twoIntStub("a", 20, "b", 300), schema));   // second fails
    }

    @Test
    void testOrRequiresAnyChild() {
        FileSchema schema = intSchema("col");
        ResolvedPredicate or = new ResolvedPredicate.Or(List.of(
                new ResolvedPredicate.IntPredicate(0, Operator.EQ, 10),
                new ResolvedPredicate.IntPredicate(0, Operator.EQ, 30)));

        assertTrue(matchesRow(or, stub("col", 10, false), schema));
        assertFalse(matchesRow(or, stub("col", 20, false), schema));
        assertTrue(matchesRow(or, stub("col", 30, false), schema));
    }

    // ==================== Helpers ====================

    private static boolean matchesRow(ResolvedPredicate predicate, StructAccessor row, FileSchema schema) {
        return RecordFilterCompiler.compile(predicate, schema).test(row);
    }

    private static void assertMatch(boolean expected, ResolvedPredicate predicate,
            StructAccessor row, FileSchema schema) {
        boolean actual = matchesRow(predicate, row, schema);
        if (expected) {
            assertTrue(actual, "Expected row to match");
        }
        else {
            assertFalse(actual, "Expected row to not match");
        }
    }

    private static FileSchema intSchema(String name) {
        return schema(name, PhysicalType.INT32);
    }

    private static FileSchema longSchema(String name) {
        return schema(name, PhysicalType.INT64);
    }

    private static FileSchema floatSchema(String name) {
        return schema(name, PhysicalType.FLOAT);
    }

    private static FileSchema doubleSchema(String name) {
        return schema(name, PhysicalType.DOUBLE);
    }

    private static FileSchema booleanSchema(String name) {
        return schema(name, PhysicalType.BOOLEAN);
    }

    private static FileSchema binarySchema(String name) {
        return schema(name, PhysicalType.BYTE_ARRAY);
    }

    private static FileSchema schema(String name, PhysicalType type) {
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement col = new SchemaElement(name, type, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, col));
    }

    private static FileSchema twoIntSchema(String name1, String name2) {
        SchemaElement root = new SchemaElement("root", null, null, null, 2, null, null, null, null, null);
        SchemaElement col1 = new SchemaElement(name1, PhysicalType.INT32, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        SchemaElement col2 = new SchemaElement(name2, PhysicalType.INT32, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, col1, col2));
    }

    private static StructAccessor stub(String name, int value, boolean isNull) {
        return new SingleFieldStub(name, isNull) {
            @Override public int getInt(String n) { return value; }
        };
    }

    private static StructAccessor longStub(String name, long value, boolean isNull) {
        return new SingleFieldStub(name, isNull) {
            @Override public long getLong(String n) { return value; }
        };
    }

    private static StructAccessor floatStub(String name, float value, boolean isNull) {
        return new SingleFieldStub(name, isNull) {
            @Override public float getFloat(String n) { return value; }
        };
    }

    private static StructAccessor doubleStub(String name, double value, boolean isNull) {
        return new SingleFieldStub(name, isNull) {
            @Override public double getDouble(String n) { return value; }
        };
    }

    private static StructAccessor booleanStub(String name, boolean value, boolean isNull) {
        return new SingleFieldStub(name, isNull) {
            @Override public boolean getBoolean(String n) { return value; }
        };
    }

    private static StructAccessor binaryStub(String name, byte[] value, boolean isNull) {
        return new SingleFieldStub(name, isNull) {
            @Override public byte[] getBinary(String n) { return value; }
        };
    }

    private static StructAccessor twoIntStub(String name1, int value1, String name2, int value2) {
        return new StructAccessor() {
            @Override public int getInt(String name) {
                if (name.equals(name1)) {
                    return value1;
                }
                if (name.equals(name2)) {
                    return value2;
                }
                throw new IllegalArgumentException(name);
            }
            @Override public boolean isNull(String name) { return false; }
            @Override public long getLong(String name) { throw new UnsupportedOperationException(); }
            @Override public float getFloat(String name) { throw new UnsupportedOperationException(); }
            @Override public double getDouble(String name) { throw new UnsupportedOperationException(); }
            @Override public boolean getBoolean(String name) { throw new UnsupportedOperationException(); }
            @Override public String getString(String name) { throw new UnsupportedOperationException(); }
            @Override public byte[] getBinary(String name) { throw new UnsupportedOperationException(); }
            @Override public LocalDate getDate(String name) { throw new UnsupportedOperationException(); }
            @Override public LocalTime getTime(String name) { throw new UnsupportedOperationException(); }
            @Override public Instant getTimestamp(String name) { throw new UnsupportedOperationException(); }
            @Override public BigDecimal getDecimal(String name) { throw new UnsupportedOperationException(); }
            @Override public UUID getUuid(String name) { throw new UnsupportedOperationException(); }
            @Override public PqInterval getInterval(String name) { throw new UnsupportedOperationException(); }
            @Override public PqStruct getStruct(String name) { throw new UnsupportedOperationException(); }
            @Override public PqIntList getListOfInts(String name) { throw new UnsupportedOperationException(); }
            @Override public PqLongList getListOfLongs(String name) { throw new UnsupportedOperationException(); }
            @Override public PqDoubleList getListOfDoubles(String name) { throw new UnsupportedOperationException(); }
            @Override public PqList getList(String name) { throw new UnsupportedOperationException(); }
            @Override public PqMap getMap(String name) { throw new UnsupportedOperationException(); }
            @Override public PqVariant getVariant(String name) { throw new UnsupportedOperationException(); }
            @Override public Object getValue(String name) { throw new UnsupportedOperationException(); }
            @Override public int getFieldCount() { return 2; }
            @Override public String getFieldName(int index) { return index == 0 ? name1 : name2; }
        };
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /// Base stub implementing [StructAccessor] for a single field.
    private abstract static class SingleFieldStub implements StructAccessor {
        private final String fieldName;
        private final boolean isNull;

        SingleFieldStub(String fieldName, boolean isNull) {
            this.fieldName = fieldName;
            this.isNull = isNull;
        }

        @Override public boolean isNull(String name) { return isNull; }
        @Override public int getInt(String name) { throw new UnsupportedOperationException(); }
        @Override public long getLong(String name) { throw new UnsupportedOperationException(); }
        @Override public float getFloat(String name) { throw new UnsupportedOperationException(); }
        @Override public double getDouble(String name) { throw new UnsupportedOperationException(); }
        @Override public boolean getBoolean(String name) { throw new UnsupportedOperationException(); }
        @Override public String getString(String name) { throw new UnsupportedOperationException(); }
        @Override public byte[] getBinary(String name) { throw new UnsupportedOperationException(); }
        @Override public LocalDate getDate(String name) { throw new UnsupportedOperationException(); }
        @Override public LocalTime getTime(String name) { throw new UnsupportedOperationException(); }
        @Override public Instant getTimestamp(String name) { throw new UnsupportedOperationException(); }
        @Override public BigDecimal getDecimal(String name) { throw new UnsupportedOperationException(); }
        @Override public UUID getUuid(String name) { throw new UnsupportedOperationException(); }
        @Override public PqInterval getInterval(String name) { throw new UnsupportedOperationException(); }
        @Override public PqStruct getStruct(String name) { throw new UnsupportedOperationException(); }
        @Override public PqIntList getListOfInts(String name) { throw new UnsupportedOperationException(); }
        @Override public PqLongList getListOfLongs(String name) { throw new UnsupportedOperationException(); }
        @Override public PqDoubleList getListOfDoubles(String name) { throw new UnsupportedOperationException(); }
        @Override public PqList getList(String name) { throw new UnsupportedOperationException(); }
        @Override public PqMap getMap(String name) { throw new UnsupportedOperationException(); }
        @Override public PqVariant getVariant(String name) { throw new UnsupportedOperationException(); }
        @Override public Object getValue(String name) { throw new UnsupportedOperationException(); }
        @Override public int getFieldCount() { return 1; }
        @Override public String getFieldName(int index) { return fieldName; }
    }
}
