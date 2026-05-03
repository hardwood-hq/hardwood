/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.math.BigDecimal;
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
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqDoubleList;
import dev.hardwood.row.PqIntList;
import dev.hardwood.row.PqInterval;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqLongList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Equivalence tests for the indexed compile path. For every predicate
/// shape the indexed matcher (built via the projection-aware
/// `RecordFilterCompiler.compile` overload) must agree with the
/// name-based matcher and with the expected boolean.
///
/// Rows here implement [RowReader], so they support both the name-based
/// accessors (inherited from [StructAccessor]) and the indexed accessors
/// (`getInt(int)`, `isNull(int)`, …) the compiled-indexed path uses.
class RecordFilterIndexedTest {

    // ==================== Single leaves ====================

    @ParameterizedTest(name = "long {0} {1} on a={2} → {3}")
    @MethodSource("singleLongCases")
    void singleLong(Operator op, long v, long aVal, boolean expected) {
        FileSchema schema = twoLongSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        TwoLongIndexedRow row = new TwoLongIndexedRow("a", aVal, false, "b", 0L, false);
        ResolvedPredicate p = new ResolvedPredicate.LongPredicate(0, op, v);
        assertEquivalent(p, row, schema, projection, expected);
    }

    static Stream<Arguments> singleLongCases() {
        return Stream.of(
                Arguments.of(Operator.EQ, 50L, 50L, true),
                Arguments.of(Operator.EQ, 50L, 51L, false),
                Arguments.of(Operator.NOT_EQ, 50L, 51L, true),
                Arguments.of(Operator.LT, 50L, 49L, true),
                Arguments.of(Operator.LT_EQ, 50L, 50L, true),
                Arguments.of(Operator.GT, 50L, 51L, true),
                Arguments.of(Operator.GT_EQ, 50L, 50L, true));
    }

    @ParameterizedTest(name = "int {0} {1} on a={2} → {3}")
    @MethodSource("singleIntCases")
    void singleInt(Operator op, int v, int aVal, boolean expected) {
        FileSchema schema = twoIntSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        TwoIntIndexedRow row = new TwoIntIndexedRow("a", aVal, false, "b", 0, false);
        ResolvedPredicate p = new ResolvedPredicate.IntPredicate(0, op, v);
        assertEquivalent(p, row, schema, projection, expected);
    }

    static Stream<Arguments> singleIntCases() {
        return Stream.of(
                Arguments.of(Operator.EQ, 50, 50, true),
                Arguments.of(Operator.LT, 50, 49, true),
                Arguments.of(Operator.GT_EQ, 50, 50, true));
    }

    @ParameterizedTest(name = "double {0} {1} on a={2} → {3}")
    @MethodSource("singleDoubleCases")
    void singleDouble(Operator op, double v, double aVal, boolean expected) {
        FileSchema schema = twoDoubleSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        TwoDoubleIndexedRow row = new TwoDoubleIndexedRow("a", aVal, false, "b", 0.0, false);
        ResolvedPredicate p = new ResolvedPredicate.DoublePredicate(0, op, v);
        assertEquivalent(p, row, schema, projection, expected);
    }

    static Stream<Arguments> singleDoubleCases() {
        return Stream.of(
                Arguments.of(Operator.EQ, 1.5, 1.5, true),
                Arguments.of(Operator.LT, 1.5, 1.4, true),
                Arguments.of(Operator.GT, 1.5, 1.6, true),
                // NaN ordering — Double.compare semantics: NaN is greater than +Inf.
                Arguments.of(Operator.GT, 100.0, Double.NaN, true),
                Arguments.of(Operator.LT, Double.NaN, 1.0, true));
    }

    @Test
    void singleLeafNullColumnRejects() {
        FileSchema schema = twoLongSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        TwoLongIndexedRow row = new TwoLongIndexedRow("a", 0L, true, "b", 0L, false);
        for (Operator op : Operator.values()) {
            assertEquivalent(new ResolvedPredicate.LongPredicate(0, op, 0L), row, schema, projection, false);
        }
        assertEquivalent(new ResolvedPredicate.IsNullPredicate(0), row, schema, projection, true);
        assertEquivalent(new ResolvedPredicate.IsNotNullPredicate(0), row, schema, projection, false);
    }

    // ==================== Compounds with indexed leaves ====================

    @Test
    void twoArityAndUsesIndexedLeaves() {
        FileSchema schema = twoLongSchema("a", "b");
        ProjectedSchema projection = projectAll(schema);
        ResolvedPredicate p = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.LongPredicate(0, Operator.GT_EQ, 0L),
                new ResolvedPredicate.LongPredicate(1, Operator.LT, 1000L)));
        TwoLongIndexedRow row = new TwoLongIndexedRow("a", 5L, false, "b", 100L, false);
        assertEquivalent(p, row, schema, projection, true);
        TwoLongIndexedRow nullA = new TwoLongIndexedRow("a", 5L, true, "b", 100L, false);
        TwoLongIndexedRow nullB = new TwoLongIndexedRow("a", 5L, false, "b", 100L, true);
        assertEquivalent(p, nullA, schema, projection, false);
        assertEquivalent(p, nullB, schema, projection, false);
    }

    @Test
    void threeArityAndUsesIndexedLeaves() {
        FileSchema schema = threeLongSchema("a", "b", "c");
        ProjectedSchema projection = projectAll(schema);
        ResolvedPredicate p = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.LongPredicate(0, Operator.GT_EQ, 0L),
                new ResolvedPredicate.LongPredicate(1, Operator.LT, 1000L),
                new ResolvedPredicate.LongPredicate(2, Operator.NOT_EQ, -1L)));
        ThreeLongIndexedRow row = new ThreeLongIndexedRow(5L, 100L, 50L);
        assertEquivalent(p, row, schema, projection, true);
    }

    // ==================== Helpers ====================

    private static void assertEquivalent(ResolvedPredicate predicate, RowReader row, FileSchema schema,
            ProjectedSchema projection, boolean expected) {
        boolean compiledName = RecordFilterCompiler.compile(predicate, schema).test(row);
        boolean compiledIndexed = RecordFilterCompiler.compile(predicate, schema, projection::toProjectedIndex).test(row);
        assertThat(compiledName).as("compiled-name disagreed with expected for %s", predicate).isEqualTo(expected);
        assertThat(compiledIndexed).as("compiled-indexed disagreed with expected for %s", predicate).isEqualTo(expected);
    }

    private static ProjectedSchema projectAll(FileSchema schema) {
        // Project every column in original order — the test rows are constructed
        // with the same column order, so projected index == file column index.
        return ProjectedSchema.create(schema, ColumnProjection.all());
    }

    private static FileSchema twoLongSchema(String n1, String n2) {
        SchemaElement root = new SchemaElement("root", null, null, null, 2, null, null, null, null, null);
        SchemaElement c1 = new SchemaElement(n1, PhysicalType.INT64, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        SchemaElement c2 = new SchemaElement(n2, PhysicalType.INT64, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, c1, c2));
    }

    private static FileSchema twoIntSchema(String n1, String n2) {
        SchemaElement root = new SchemaElement("root", null, null, null, 2, null, null, null, null, null);
        SchemaElement c1 = new SchemaElement(n1, PhysicalType.INT32, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        SchemaElement c2 = new SchemaElement(n2, PhysicalType.INT32, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, c1, c2));
    }

    private static FileSchema twoDoubleSchema(String n1, String n2) {
        SchemaElement root = new SchemaElement("root", null, null, null, 2, null, null, null, null, null);
        SchemaElement c1 = new SchemaElement(n1, PhysicalType.DOUBLE, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        SchemaElement c2 = new SchemaElement(n2, PhysicalType.DOUBLE, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, c1, c2));
    }

    private static FileSchema threeLongSchema(String n1, String n2, String n3) {
        SchemaElement root = new SchemaElement("root", null, null, null, 3, null, null, null, null, null);
        SchemaElement c1 = new SchemaElement(n1, PhysicalType.INT64, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        SchemaElement c2 = new SchemaElement(n2, PhysicalType.INT64, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        SchemaElement c3 = new SchemaElement(n3, PhysicalType.INT64, null, RepetitionType.OPTIONAL,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, c1, c2, c3));
    }

    // ==================== Stub rows ====================

    private static final class TwoLongIndexedRow extends BaseIndexedRow {
        private final long v0;
        private final long v1;
        TwoLongIndexedRow(String n0, long v0, boolean null0, String n1, long v1, boolean null1) {
            super(new String[] { n0, n1 }, new boolean[] { null0, null1 });
            this.v0 = v0;
            this.v1 = v1;
        }
        @Override public long getLong(String name) { return getLong(indexOf(name)); }
        @Override public long getLong(int idx) { return idx == 0 ? v0 : v1; }
    }

    private static final class TwoIntIndexedRow extends BaseIndexedRow {
        private final int v0;
        private final int v1;
        TwoIntIndexedRow(String n0, int v0, boolean null0, String n1, int v1, boolean null1) {
            super(new String[] { n0, n1 }, new boolean[] { null0, null1 });
            this.v0 = v0;
            this.v1 = v1;
        }
        @Override public int getInt(String name) { return getInt(indexOf(name)); }
        @Override public int getInt(int idx) { return idx == 0 ? v0 : v1; }
    }

    private static final class TwoDoubleIndexedRow extends BaseIndexedRow {
        private final double v0;
        private final double v1;
        TwoDoubleIndexedRow(String n0, double v0, boolean null0, String n1, double v1, boolean null1) {
            super(new String[] { n0, n1 }, new boolean[] { null0, null1 });
            this.v0 = v0;
            this.v1 = v1;
        }
        @Override public double getDouble(String name) { return getDouble(indexOf(name)); }
        @Override public double getDouble(int idx) { return idx == 0 ? v0 : v1; }
    }

    private static final class ThreeLongIndexedRow extends BaseIndexedRow {
        private final long v0;
        private final long v1;
        private final long v2;
        ThreeLongIndexedRow(long v0, long v1, long v2) {
            super(new String[] { "a", "b", "c" }, new boolean[] { false, false, false });
            this.v0 = v0;
            this.v1 = v1;
            this.v2 = v2;
        }
        @Override public long getLong(String name) { return getLong(indexOf(name)); }
        @Override public long getLong(int idx) {
            return switch (idx) {
                case 0 -> v0;
                case 1 -> v1;
                case 2 -> v2;
                default -> throw new IndexOutOfBoundsException(idx);
            };
        }
    }

    /// Common stub plumbing implementing [RowReader]. Subclasses override
    /// only the typed accessors they need; everything else throws
    /// `UnsupportedOperationException`, which makes test failures point at
    /// the predicate path that called the wrong accessor.
    private abstract static class BaseIndexedRow implements RowReader {
        private final String[] names;
        private final boolean[] nulls;

        BaseIndexedRow(String[] names, boolean[] nulls) {
            this.names = names;
            this.nulls = nulls;
        }

        protected int indexOf(String name) {
            for (int i = 0; i < names.length; i++) {
                if (names[i].equals(name)) return i;
            }
            throw new IllegalArgumentException(name);
        }

        @Override public boolean isNull(String name) { return nulls[indexOf(name)]; }
        @Override public boolean isNull(int idx) { return nulls[idx]; }

        @Override public boolean hasNext() { throw new UnsupportedOperationException(); }
        @Override public void next() { throw new UnsupportedOperationException(); }
        @Override public void close() { }

        @Override public int getInt(String name) { throw new UnsupportedOperationException(name); }
        @Override public long getLong(String name) { throw new UnsupportedOperationException(name); }
        @Override public float getFloat(String name) { throw new UnsupportedOperationException(name); }
        @Override public double getDouble(String name) { throw new UnsupportedOperationException(name); }
        @Override public boolean getBoolean(String name) { throw new UnsupportedOperationException(name); }
        @Override public String getString(String name) { throw new UnsupportedOperationException(name); }
        @Override public byte[] getBinary(String name) { throw new UnsupportedOperationException(name); }
        @Override public LocalDate getDate(String name) { throw new UnsupportedOperationException(name); }
        @Override public LocalTime getTime(String name) { throw new UnsupportedOperationException(name); }
        @Override public Instant getTimestamp(String name) { throw new UnsupportedOperationException(name); }
        @Override public BigDecimal getDecimal(String name) { throw new UnsupportedOperationException(name); }
        @Override public UUID getUuid(String name) { throw new UnsupportedOperationException(name); }
        @Override public PqInterval getInterval(String name) { throw new UnsupportedOperationException(name); }
        @Override public PqStruct getStruct(String name) { throw new UnsupportedOperationException(name); }
        @Override public PqIntList getListOfInts(String name) { throw new UnsupportedOperationException(name); }
        @Override public PqLongList getListOfLongs(String name) { throw new UnsupportedOperationException(name); }
        @Override public PqDoubleList getListOfDoubles(String name) { throw new UnsupportedOperationException(name); }
        @Override public PqList getList(String name) { throw new UnsupportedOperationException(name); }
        @Override public PqMap getMap(String name) { throw new UnsupportedOperationException(name); }
        @Override public PqVariant getVariant(String name) { throw new UnsupportedOperationException(name); }
        @Override public Object getValue(String name) { throw new UnsupportedOperationException(name); }
        @Override public int getFieldCount() { return names.length; }
        @Override public String getFieldName(int index) { return names[index]; }

        @Override public int getInt(int idx) { throw new UnsupportedOperationException(); }
        @Override public long getLong(int idx) { throw new UnsupportedOperationException(); }
        @Override public float getFloat(int idx) { throw new UnsupportedOperationException(); }
        @Override public double getDouble(int idx) { throw new UnsupportedOperationException(); }
        @Override public boolean getBoolean(int idx) { throw new UnsupportedOperationException(); }
        @Override public String getString(int idx) { throw new UnsupportedOperationException(); }
        @Override public byte[] getBinary(int idx) { throw new UnsupportedOperationException(); }
        @Override public LocalDate getDate(int idx) { throw new UnsupportedOperationException(); }
        @Override public LocalTime getTime(int idx) { throw new UnsupportedOperationException(); }
        @Override public Instant getTimestamp(int idx) { throw new UnsupportedOperationException(); }
        @Override public BigDecimal getDecimal(int idx) { throw new UnsupportedOperationException(); }
        @Override public UUID getUuid(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqInterval getInterval(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqStruct getStruct(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqIntList getListOfInts(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqLongList getListOfLongs(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqDoubleList getListOfDoubles(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqList getList(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqMap getMap(int idx) { throw new UnsupportedOperationException(); }
        @Override public Object getValue(int idx) { throw new UnsupportedOperationException(); }
    }
}
