/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.internal.predicate.ResolvedPredicate.BinaryPredicate.Comparison;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.FilterPredicate.Operator;

import static org.assertj.core.api.Assertions.assertThat;

/// Regression tests for #1172: statistics whose `min` sorts above its `max` must not prune.
///
/// The Parquet spec requires `min <= max`, but writers that fill the two slots the wrong way
/// round, or that misapply a column's sort order, have emitted pairs that violate it. Such a
/// pair is wrong in both directions at once — it excludes the values it should contain, so a
/// drop check skips units holding matching rows, and it contains the values it should exclude,
/// so an always-match check promises rows that do not match. Both are silent wrong results, so
/// the bounds are discarded where [MinMaxStats] sources them and every unit is kept.
class InvertedStatisticsFilterTest {

    /// The reason [MinMaxStats] gives for dropping bounds that are the wrong way round.
    private static final String INVERTED = "the minimum sorts above the maximum";

    // ==================== The drop side ====================

    @ParameterizedTest(name = "{1} on inverted {0} bounds")
    @MethodSource("columnsAndOperators")
    void invertedBoundsNeverDrop(Column column, Operator op) {
        assertThat(column.inverted().canDrop(column.leaf(op)))
                .as("inverted bounds prove nothing and must never drop a unit")
                .isFalse();
    }

    @ParameterizedTest(name = "{0} IN on inverted bounds")
    @MethodSource("inColumns")
    void invertedBoundsNeverDropForInPredicates(Column column) {
        assertThat(column.inverted().canDrop(column.leaf(null)))
                .isFalse();
    }

    // ==================== The always-match side ====================

    @ParameterizedTest(name = "{1} on inverted {0} bounds")
    @MethodSource("columnsAndOperators")
    void invertedBoundsNeverPromiseAlwaysMatches(Column column, Operator op) {
        assertThat(column.inverted().decideLeaf(column.leaf(op)))
                .as("inverted bounds prove nothing and must never skip per-row filtering")
                .isEqualTo(FilterDecision.MIGHT_MATCH);
    }

    @ParameterizedTest(name = "{0} IN on inverted bounds")
    @MethodSource("inColumns")
    void invertedBoundsNeverPromiseAlwaysMatchesForInPredicates(Column column) {
        assertThat(column.inverted().decideLeaf(column.leaf(null)))
                .isEqualTo(FilterDecision.MIGHT_MATCH);
    }

    // ==================== The bounds are discarded, and said to be ====================

    @ParameterizedTest(name = "{0}")
    @MethodSource({ "columns", "inColumns" })
    void invertedBoundsAreDiscardedWithTheirReason(Column column) {
        assertThat(column.inverted())
                .isInstanceOf(MinMaxStats.NullCountOnlyStats.class)
                .extracting(MinMaxStats::discardReason).isEqualTo(INVERTED);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource({ "columns", "inColumns" })
    void orderedBoundsAreKept(Column column) {
        assertThat(column.ordered())
                .as("bounds the right way round are kept and decoded")
                .isNotInstanceOf(MinMaxStats.NullCountOnlyStats.class);
    }

    // ==================== Ordered bounds still prune ====================

    /// The same columns with their bounds the right way round: a probe below the range still
    /// drops on `GT_EQ`, so the guard has not disabled pruning wholesale.
    @ParameterizedTest(name = "{0}")
    @MethodSource("columns")
    void orderedBoundsStillPrune(Column column) {
        // Every probe sits strictly inside [low, high], so "everything is >= the probe" is
        // false for the low end and the unit cannot match "< probe".
        assertThat(column.ordered().decideLeaf(column.leaf(Operator.LT_EQ)))
                .isEqualTo(FilterDecision.MIGHT_MATCH);
        assertThat(column.ordered().canDrop(column.leaf(Operator.GT)))
                .as("the probe is inside [low, high], so GT cannot be ruled out")
                .isFalse();
    }

    @Test
    void orderedIntBoundsStillDropAndPromise() {
        Column column = intColumn();
        ResolvedPredicate below = new ResolvedPredicate.IntPredicate(0, Operator.EQ, 5);
        assertThat(column.ordered().decideLeaf(below))
                .isEqualTo(FilterDecision.CANNOT_MATCH);

        ResolvedPredicate everything = new ResolvedPredicate.IntPredicate(0, Operator.GT, 5);
        assertThat(column.ordered().decideLeaf(everything))
                .isEqualTo(FilterDecision.ALWAYS_MATCHES);
    }

    // ==================== ±0 bounds are not inverted ====================

    /// Under the type-defined ordering the spec leaves `+0` and `-0` interchangeable, so
    /// `(min = +0, max = -0)` is a legitimate pair that the comparators widen rather than
    /// reject. The usability check applies the same widening, so it must not read the pair as
    /// inverted.
    @Test
    void typeDefinedZeroBoundsAreNotInverted() {
        MinMaxStats floatStats = MinMaxStats.of(stats(floatBytes(0.0f), floatBytes(-0.0f)),
                new ResolvedPredicate.FloatPredicate(0, Operator.EQ, 5.0f, false));
        assertThat(floatStats).isNotInstanceOf(MinMaxStats.NullCountOnlyStats.class);

        MinMaxStats doubleStats = MinMaxStats.of(stats(doubleBytes(0.0), doubleBytes(-0.0)),
                new ResolvedPredicate.DoublePredicate(0, Operator.EQ, 5.0, false));
        assertThat(doubleStats).isNotInstanceOf(MinMaxStats.NullCountOnlyStats.class);

        // And they still prune a value neither zero can be.
        assertThat(doubleStats.canDrop(new ResolvedPredicate.DoublePredicate(0, Operator.EQ, 5.0, false)))
                .isTrue();
    }

    /// The IEEE 754 total order is unambiguous — `-0` sorts below `+0` — so the same pair is
    /// genuinely the wrong way round there.
    @Test
    void ieee754ZeroBoundsTheWrongWayRoundAreInverted() {
        MinMaxStats doubleStats = MinMaxStats.of(stats(doubleBytes(0.0), doubleBytes(-0.0)),
                new ResolvedPredicate.DoublePredicate(0, Operator.EQ, 5.0, true));
        assertThat(doubleStats.discardReason()).isEqualTo(INVERTED);
    }

    // ==================== Fixtures ====================

    /// One column's bounds and the leaf that reads them, in the order that leaf compares in.
    ///
    /// @param name the physical type, for the test's display name
    /// @param leaf builds the leaf under test for an operator, ignoring it for `IN` leaves
    /// @param low the lower of the two bounds
    /// @param high the higher of the two bounds
    private record Column(String name, Function<Operator, ResolvedPredicate> leaf,
            byte[] low, byte[] high) {

        /// The bounds the wrong way round, as a buggy writer emits them.
        MinMaxStats inverted() {
            return MinMaxStats.of(stats(high, low), leaf.apply(Operator.EQ));
        }

        /// The same bounds the right way round.
        MinMaxStats ordered() {
            return MinMaxStats.of(stats(low, high), leaf.apply(Operator.EQ));
        }

        ResolvedPredicate leaf(Operator op) {
            return leaf.apply(op);
        }

        @Override
        public String toString() {
            return name;
        }
    }

    static Stream<Column> columns() {
        return Stream.of(
                intColumn(),
                new Column("INT64", op -> new ResolvedPredicate.LongPredicate(0, op, 15L),
                        longBytes(10L), longBytes(20L)),
                new Column("FLOAT", op -> new ResolvedPredicate.FloatPredicate(0, op, 15.0f, false),
                        floatBytes(10.0f), floatBytes(20.0f)),
                new Column("FLOAT16", op -> new ResolvedPredicate.Float16Predicate(0, op, 15.0f, false),
                        float16Bytes(10.0f), float16Bytes(20.0f)),
                new Column("DOUBLE", op -> new ResolvedPredicate.DoublePredicate(0, op, 15.0, false),
                        doubleBytes(10.0), doubleBytes(20.0)),
                new Column("BOOLEAN", op -> new ResolvedPredicate.BooleanPredicate(0, op, false),
                        new byte[]{ 0 }, new byte[]{ 1 }),
                new Column("BYTE_ARRAY", op -> new ResolvedPredicate.BinaryPredicate(0, op,
                        utf8("mango"), Comparison.BYTE_STRING), utf8("apple"), utf8("peach")),
                // Two's complement bounds: -100 sorts below +100 signed but above it unsigned,
                // so only the column's own order tells the pair apart from an inverted one.
                new Column("FIXED_LEN_BYTE_ARRAY DECIMAL", op -> new ResolvedPredicate.BinaryPredicate(0, op,
                        fixedDecimalBytes(0), Comparison.FIXED_DECIMAL),
                        fixedDecimalBytes(-100), fixedDecimalBytes(100)),
                new Column("BYTE_ARRAY DECIMAL", op -> new ResolvedPredicate.BinaryPredicate(0, op,
                        new byte[]{ 0 }, Comparison.VARIABLE_DECIMAL),
                        new byte[]{ (byte) 0x9C }, new byte[]{ 0x64 }));
    }

    static Stream<Column> inColumns() {
        return Stream.of(
                new Column("INT32 IN", op -> new ResolvedPredicate.IntInPredicate(0, new int[]{ 15 }),
                        intBytes(10), intBytes(20)),
                new Column("INT64 IN", op -> new ResolvedPredicate.LongInPredicate(0, new long[]{ 15L }),
                        longBytes(10L), longBytes(20L)),
                new Column("BYTE_ARRAY IN", op -> new ResolvedPredicate.BinaryInPredicate(0,
                        new byte[][]{ utf8("mango") }), utf8("apple"), utf8("peach")),
                new Column("FLOAT IN", op -> new ResolvedPredicate.DoubleInPredicate(0,
                        new double[]{ 15.0 }, true, false), floatBytes(10.0f), floatBytes(20.0f)),
                new Column("DOUBLE IN", op -> new ResolvedPredicate.DoubleInPredicate(0,
                        new double[]{ 15.0 }, false, false), doubleBytes(10.0), doubleBytes(20.0)));
    }

    static Stream<Arguments> columnsAndOperators() {
        return columns().flatMap(column -> Stream.of(Operator.values())
                .map(op -> Arguments.of(column, op)));
    }

    private static Column intColumn() {
        return new Column("INT32", op -> new ResolvedPredicate.IntPredicate(0, op, 15),
                intBytes(10), intBytes(20));
    }

    /// Statistics with a proven-zero null count, so that [FilterDecision#ALWAYS_MATCHES] is
    /// reachable and the always-match side of the bug is actually exercised.
    private static Statistics stats(byte[] min, byte[] max) {
        return new Statistics(min, max, 0L, null, false);
    }

    private static byte[] intBytes(int value) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
    }

    private static byte[] longBytes(long value) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
    }

    private static byte[] floatBytes(float value) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(value).array();
    }

    private static byte[] doubleBytes(double value) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(value).array();
    }

    private static byte[] float16Bytes(float value) {
        short raw = Float.floatToFloat16(value);
        return new byte[]{ (byte) (raw & 0xFF), (byte) ((raw >> 8) & 0xFF) };
    }

    /// A four-byte big-endian two's complement unscaled value, as a `FIXED_LEN_BYTE_ARRAY(4)`
    /// `DECIMAL` column stores it.
    private static byte[] fixedDecimalBytes(int unscaled) {
        return ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(unscaled).array();
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
