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
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.FilterPredicate.Operator;

import static org.assertj.core.api.Assertions.assertThat;

/// Regression tests for #566: float/double predicate pruning must not drop matching rows
/// when row-group / page statistics carry `NaN` in `min` or `max`.
///
/// The Parquet spec forbids writing `NaN` to `min`/`max`, but older or buggy writers have
/// emitted such values. A conformant reader must treat a `NaN` bound as unusable for
/// pruning (parquet-mr does the same).
///
/// The check lives where [MinMaxStats] sources the bounds, alongside the other reasons a pair
/// of bounds can be unusable (#1172), so these cases go in through a leaf and its statistics
/// rather than straight at a comparator. The comparators below are still exercised directly
/// where the bounds they are given are usable ones.
class NaNStatisticsFilterTest {

    @ParameterizedTest(name = "double {0} with NaN min")
    @EnumSource(Operator.class)
    void doubleNaNMinNeverDrops(Operator op) {
        assertThat(dropsDouble(op, 1.0, Double.NaN, 10.0))
                .as("NaN min must be treated as no-bound and never prune")
                .isFalse();
    }

    @ParameterizedTest(name = "double {0} with NaN max")
    @EnumSource(Operator.class)
    void doubleNaNMaxNeverDrops(Operator op) {
        assertThat(dropsDouble(op, 1.0, -10.0, Double.NaN))
                .as("NaN max must be treated as no-bound and never prune")
                .isFalse();
    }

    @ParameterizedTest(name = "double {0} with NaN min and max")
    @EnumSource(Operator.class)
    void doubleNaNBothNeverDrops(Operator op) {
        assertThat(dropsDouble(op, 1.0, Double.NaN, Double.NaN)).isFalse();
    }

    @ParameterizedTest(name = "float {0} with NaN min")
    @EnumSource(Operator.class)
    void floatNaNMinNeverDrops(Operator op) {
        assertThat(dropsFloat(op, 1.0f, Float.NaN, 10.0f))
                .as("NaN min must be treated as no-bound and never prune")
                .isFalse();
    }

    @ParameterizedTest(name = "float {0} with NaN max")
    @EnumSource(Operator.class)
    void floatNaNMaxNeverDrops(Operator op) {
        assertThat(dropsFloat(op, 1.0f, -10.0f, Float.NaN))
                .as("NaN max must be treated as no-bound and never prune")
                .isFalse();
    }

    @ParameterizedTest(name = "float {0} with NaN min and max")
    @EnumSource(Operator.class)
    void floatNaNBothNeverDrops(Operator op) {
        assertThat(dropsFloat(op, 1.0f, Float.NaN, Float.NaN)).isFalse();
    }

    /// `FLOAT16` bounds are two bytes wide and decode through their own path, so the check
    /// has to reach them as well as the four- and eight-byte ones.
    @ParameterizedTest(name = "float16 {0} with a NaN bound")
    @EnumSource(Operator.class)
    void float16NaNNeverDrops(Operator op) {
        ResolvedPredicate leaf = new ResolvedPredicate.Float16Predicate(0, op, 1.0f, false);
        MinMaxStats stats = MinMaxStats.of(
                new Statistics(float16Bytes(Float.NaN), float16Bytes(10.0f), 0L, null, false), leaf, BoundsReadability.ALL);

        assertThat(stats.canDrop(leaf)).isFalse();
        assertThat(stats.discardReason())
                .isEqualTo("one of them is NaN, which sits outside the column's ordering");
    }

    /// A NaN bound is discarded rather than compared against, and says so.
    @Test
    void naNBoundsAreDiscardedAtTheSource() {
        ResolvedPredicate leaf = new ResolvedPredicate.DoublePredicate(0, Operator.GT, 1.0, false);
        MinMaxStats stats = MinMaxStats.of(
                new Statistics(doubleBytes(Double.NaN), doubleBytes(10.0), 0L, null, false),
                leaf, BoundsReadability.ALL);

        assertThat(stats)
                .isInstanceOf(MinMaxStats.NullCountOnlyStats.class)
                .extracting(MinMaxStats::discardReason)
                .isEqualTo("one of them is NaN, which sits outside the column's ordering");
    }

    /// Sanity check that non-NaN stats still prune as before — guarding against an
    /// overly broad fix that disables pruning for finite values too.
    @ParameterizedTest(name = "double {0}({1}) on [{2},{3}] expectDrop={4}")
    @MethodSource
    void doubleFiniteStillPrunes(Operator op, double value, double min, double max, boolean expectDrop) {
        assertThat(StatisticsFilterSupport.canDropDouble(op, value, min, max, false)).isEqualTo(expectDrop);
    }

    static Stream<Arguments> doubleFiniteStillPrunes() {
        return Stream.of(
                Arguments.of(Operator.LT,    0.0, 1.0, 10.0, true),  // all values >= 1 → drop
                Arguments.of(Operator.LT,    5.0, 1.0, 10.0, false), // some values < 5 possible
                Arguments.of(Operator.GT,   20.0, 1.0, 10.0, true),  // all values <= 10 → drop
                Arguments.of(Operator.EQ,  100.0, 1.0, 10.0, true),  // 100 out of [1,10] → drop
                Arguments.of(Operator.EQ,    5.0, 1.0, 10.0, false));
    }

    /// Order-aware ±0 pruning (#595). Under the type-defined ordering (`ieee754TotalOrder == false`)
    /// the spec leaves `+0`/`-0` ambiguous, so a zero bound is widened and the opposite zero is not
    /// dropped. Under the IEEE 754 total order the bounds are exact, so `-0 < +0` prunes precisely.
    @Test
    void typeDefinedZeroBoundDoesNotDropOppositeZero() {
        // min = +0, max = +0 may also hold -0 → == -0 must not drop.
        assertThat(StatisticsFilterSupport.canDropFloat(Operator.EQ, -0.0f, 0.0f, 0.0f, false)).isFalse();
        assertThat(StatisticsFilterSupport.canDropDouble(Operator.EQ, -0.0, 0.0, 0.0, false)).isFalse();
        // max = -0, min = -0 may also hold +0 → == +0 must not drop.
        assertThat(StatisticsFilterSupport.canDropFloat(Operator.EQ, 0.0f, -0.0f, -0.0f, false)).isFalse();
        assertThat(StatisticsFilterSupport.canDropDouble(Operator.EQ, 0.0, -0.0, -0.0, false)).isFalse();
    }

    @Test
    void ieee754ZeroBoundPrunesExactly() {
        // Total order is unambiguous: min = +0 genuinely excludes -0 → == -0 prunes.
        assertThat(StatisticsFilterSupport.canDropFloat(Operator.EQ, -0.0f, 0.0f, 0.0f, true)).isTrue();
        assertThat(StatisticsFilterSupport.canDropDouble(Operator.EQ, -0.0, 0.0, 0.0, true)).isTrue();
        // max = -0 genuinely excludes +0 → == +0 prunes.
        assertThat(StatisticsFilterSupport.canDropFloat(Operator.EQ, 0.0f, -0.0f, -0.0f, true)).isTrue();
        assertThat(StatisticsFilterSupport.canDropDouble(Operator.EQ, 0.0, -0.0, -0.0, true)).isTrue();
    }

    @Test
    void zeroBoundsStillPruneNonZeroValuesUnderBothOrders() {
        assertThat(StatisticsFilterSupport.canDropFloat(Operator.EQ, 5.0f, -0.0f, 0.0f, false)).isTrue();
        assertThat(StatisticsFilterSupport.canDropFloat(Operator.EQ, 5.0f, -0.0f, 0.0f, true)).isTrue();
    }

    // ==================== Fixtures ====================

    /// Whether the leaf drops a unit with these bounds, sourced the way the evaluators source
    /// them so that the usability check runs.
    private static boolean dropsDouble(Operator op, double value, double min, double max) {
        ResolvedPredicate leaf = new ResolvedPredicate.DoublePredicate(0, op, value, false);
        return MinMaxStats.of(new Statistics(doubleBytes(min), doubleBytes(max), 0L, null, false), leaf, BoundsReadability.ALL)
                .canDrop(leaf);
    }

    /// See [#dropsDouble].
    private static boolean dropsFloat(Operator op, float value, float min, float max) {
        ResolvedPredicate leaf = new ResolvedPredicate.FloatPredicate(0, op, value, false);
        return MinMaxStats.of(new Statistics(floatBytes(min), floatBytes(max), 0L, null, false), leaf, BoundsReadability.ALL)
                .canDrop(leaf);
    }

    private static byte[] float16Bytes(float value) {
        short raw = Float.floatToFloat16(value);
        return new byte[]{ (byte) (raw & 0xFF), (byte) ((raw >> 8) & 0xFF) };
    }

    private static byte[] floatBytes(float value) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(value).array();
    }

    private static byte[] doubleBytes(double value) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(value).array();
    }
}
