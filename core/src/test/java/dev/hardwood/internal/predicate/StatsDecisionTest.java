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
import java.util.Random;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.FilterPredicate;

import static dev.hardwood.internal.predicate.StatsDecision.ALWAYS_MATCHES;
import static dev.hardwood.internal.predicate.StatsDecision.CANNOT_MATCH;
import static dev.hardwood.internal.predicate.StatsDecision.MIGHT_MATCH;
import static org.assertj.core.api.Assertions.assertThat;

/// Unit matrix for the three-valued statistics decision: [StatsDecision] combinators and
/// [StatisticsFilterSupport#decideLeaf] over synthetic [MinMaxStats].
///
/// The [#decisionAgreesWithBruteForceOnRandomData] property pins the decision's meaning
/// against decoded values: [StatsDecision#ALWAYS_MATCHES] must imply zero non-matching
/// rows and [StatsDecision#CANNOT_MATCH] zero matching rows, over randomized
/// high-cardinality data (low-cardinality fixtures satisfy broken decisions silently).
class StatsDecisionTest {

    // ==================== Combinators ====================

    @Test
    void andCombinator() {
        assertThat(StatsDecision.and(ALWAYS_MATCHES, ALWAYS_MATCHES)).isEqualTo(ALWAYS_MATCHES);
        assertThat(StatsDecision.and(ALWAYS_MATCHES, MIGHT_MATCH)).isEqualTo(MIGHT_MATCH);
        assertThat(StatsDecision.and(MIGHT_MATCH, ALWAYS_MATCHES)).isEqualTo(MIGHT_MATCH);
        assertThat(StatsDecision.and(MIGHT_MATCH, MIGHT_MATCH)).isEqualTo(MIGHT_MATCH);
        assertThat(StatsDecision.and(CANNOT_MATCH, ALWAYS_MATCHES)).isEqualTo(CANNOT_MATCH);
        assertThat(StatsDecision.and(ALWAYS_MATCHES, CANNOT_MATCH)).isEqualTo(CANNOT_MATCH);
        assertThat(StatsDecision.and(CANNOT_MATCH, MIGHT_MATCH)).isEqualTo(CANNOT_MATCH);
        assertThat(StatsDecision.and(CANNOT_MATCH, CANNOT_MATCH)).isEqualTo(CANNOT_MATCH);
    }

    @Test
    void orCombinator() {
        assertThat(StatsDecision.or(ALWAYS_MATCHES, ALWAYS_MATCHES)).isEqualTo(ALWAYS_MATCHES);
        assertThat(StatsDecision.or(ALWAYS_MATCHES, MIGHT_MATCH)).isEqualTo(ALWAYS_MATCHES);
        assertThat(StatsDecision.or(ALWAYS_MATCHES, CANNOT_MATCH)).isEqualTo(ALWAYS_MATCHES);
        assertThat(StatsDecision.or(CANNOT_MATCH, ALWAYS_MATCHES)).isEqualTo(ALWAYS_MATCHES);
        assertThat(StatsDecision.or(MIGHT_MATCH, MIGHT_MATCH)).isEqualTo(MIGHT_MATCH);
        assertThat(StatsDecision.or(MIGHT_MATCH, CANNOT_MATCH)).isEqualTo(MIGHT_MATCH);
        assertThat(StatsDecision.or(CANNOT_MATCH, MIGHT_MATCH)).isEqualTo(MIGHT_MATCH);
        assertThat(StatsDecision.or(CANNOT_MATCH, CANNOT_MATCH)).isEqualTo(CANNOT_MATCH);
    }

    // ==================== Integer leaf decisions ====================

    @Test
    void intRangeDecisions() {
        // Stats: [10, 20], no nulls
        assertThat(decideInt(FilterPredicate.Operator.GT, 5, 10, 20)).isEqualTo(ALWAYS_MATCHES);
        assertThat(decideInt(FilterPredicate.Operator.GT, 9, 10, 20)).isEqualTo(ALWAYS_MATCHES);
        assertThat(decideInt(FilterPredicate.Operator.GT, 10, 10, 20)).isEqualTo(MIGHT_MATCH);
        assertThat(decideInt(FilterPredicate.Operator.GT, 20, 10, 20)).isEqualTo(CANNOT_MATCH);

        assertThat(decideInt(FilterPredicate.Operator.GT_EQ, 10, 10, 20)).isEqualTo(ALWAYS_MATCHES);
        assertThat(decideInt(FilterPredicate.Operator.GT_EQ, 11, 10, 20)).isEqualTo(MIGHT_MATCH);

        assertThat(decideInt(FilterPredicate.Operator.LT, 21, 10, 20)).isEqualTo(ALWAYS_MATCHES);
        assertThat(decideInt(FilterPredicate.Operator.LT, 20, 10, 20)).isEqualTo(MIGHT_MATCH);
        assertThat(decideInt(FilterPredicate.Operator.LT, 10, 10, 20)).isEqualTo(CANNOT_MATCH);

        assertThat(decideInt(FilterPredicate.Operator.LT_EQ, 20, 10, 20)).isEqualTo(ALWAYS_MATCHES);
        assertThat(decideInt(FilterPredicate.Operator.LT_EQ, 19, 10, 20)).isEqualTo(MIGHT_MATCH);

        assertThat(decideInt(FilterPredicate.Operator.EQ, 42, 42, 42)).isEqualTo(ALWAYS_MATCHES);
        assertThat(decideInt(FilterPredicate.Operator.EQ, 15, 10, 20)).isEqualTo(MIGHT_MATCH);
        assertThat(decideInt(FilterPredicate.Operator.EQ, 5, 10, 20)).isEqualTo(CANNOT_MATCH);

        assertThat(decideInt(FilterPredicate.Operator.NOT_EQ, 5, 10, 20)).isEqualTo(ALWAYS_MATCHES);
        assertThat(decideInt(FilterPredicate.Operator.NOT_EQ, 25, 10, 20)).isEqualTo(ALWAYS_MATCHES);
        assertThat(decideInt(FilterPredicate.Operator.NOT_EQ, 15, 10, 20)).isEqualTo(MIGHT_MATCH);
        assertThat(decideInt(FilterPredicate.Operator.NOT_EQ, 42, 42, 42)).isEqualTo(CANNOT_MATCH);
    }

    @Test
    void intInDecisions() {
        ResolvedPredicate in = new ResolvedPredicate.IntInPredicate(0, new int[]{ 5, 42, 99 });
        assertThat(StatisticsFilterSupport.decideLeaf(in, intStats(42, 42, 0L)))
                .isEqualTo(ALWAYS_MATCHES);
        assertThat(StatisticsFilterSupport.decideLeaf(in, intStats(42, 43, 0L)))
                .isEqualTo(MIGHT_MATCH);
        assertThat(StatisticsFilterSupport.decideLeaf(in, intStats(6, 41, 0L)))
                .isEqualTo(CANNOT_MATCH);
    }

    // ==================== Null-count gating ====================

    @Test
    void nullsPreventAlwaysMatchesForValuePredicates() {
        // [10, 20] fully satisfies GT 5, but nulls (or an unknown null count) may hide
        // non-matching rows: a null row satisfies no value predicate.
        ResolvedPredicate gt = new ResolvedPredicate.IntPredicate(0, FilterPredicate.Operator.GT, 5);
        assertThat(StatisticsFilterSupport.decideLeaf(gt, intStats(10, 20, 0L)))
                .isEqualTo(ALWAYS_MATCHES);
        assertThat(StatisticsFilterSupport.decideLeaf(gt, intStats(10, 20, 3L)))
                .isEqualTo(MIGHT_MATCH);
        assertThat(StatisticsFilterSupport.decideLeaf(gt, intStats(10, 20, null)))
                .isEqualTo(MIGHT_MATCH);
    }

    @Test
    void missingBoundsNeverPromiseAlwaysMatches() {
        ResolvedPredicate gt = new ResolvedPredicate.IntPredicate(0, FilterPredicate.Operator.GT, 5);
        assertThat(StatisticsFilterSupport.decideLeaf(gt, stats(null, null, 0L)))
                .isEqualTo(MIGHT_MATCH);
    }

    // ==================== Floating point: never ALWAYS_MATCHES ====================

    @Test
    void floatingPointNeverPromisesAlwaysMatches() {
        // nan_count is not consumed yet: NaN rows sit outside [min, max], so even a
        // fully-satisfying interval cannot promise every row for FP columns.
        ResolvedPredicate gtDouble =
                new ResolvedPredicate.DoublePredicate(0, FilterPredicate.Operator.GT, 1.0);
        assertThat(StatisticsFilterSupport.decideLeaf(gtDouble, doubleStats(10.0, 20.0, 0L)))
                .isEqualTo(MIGHT_MATCH);

        ResolvedPredicate gtFloat =
                new ResolvedPredicate.FloatPredicate(0, FilterPredicate.Operator.GT, 1.0f);
        assertThat(StatisticsFilterSupport.decideLeaf(gtFloat, floatStats(10.0f, 20.0f, 0L)))
                .isEqualTo(MIGHT_MATCH);

        // The CANNOT_MATCH side is unaffected.
        ResolvedPredicate gtOutside =
                new ResolvedPredicate.DoublePredicate(0, FilterPredicate.Operator.GT, 25.0);
        assertThat(StatisticsFilterSupport.decideLeaf(gtOutside, doubleStats(10.0, 20.0, 0L)))
                .isEqualTo(CANNOT_MATCH);
    }

    // ==================== Binary leaf decisions ====================

    @Test
    void binaryRangeDecisions() {
        // Stats: ["mango", "peach"], unsigned comparison, no nulls
        assertThat(decideBinary(FilterPredicate.Operator.GT, "apple", "mango", "peach"))
                .isEqualTo(ALWAYS_MATCHES);
        assertThat(decideBinary(FilterPredicate.Operator.GT, "mango", "mango", "peach"))
                .isEqualTo(MIGHT_MATCH);
        assertThat(decideBinary(FilterPredicate.Operator.LT, "plum", "mango", "peach"))
                .isEqualTo(ALWAYS_MATCHES);
        assertThat(decideBinary(FilterPredicate.Operator.EQ, "kiwi", "kiwi", "kiwi"))
                .isEqualTo(ALWAYS_MATCHES);
        assertThat(decideBinary(FilterPredicate.Operator.NOT_EQ, "apple", "mango", "peach"))
                .isEqualTo(ALWAYS_MATCHES);
        assertThat(decideBinary(FilterPredicate.Operator.EQ, "apple", "mango", "peach"))
                .isEqualTo(CANNOT_MATCH);
    }

    // ==================== IS NOT NULL ====================

    @Test
    void isNotNullDecidedByNullCountAlone() {
        ResolvedPredicate isNotNull = new ResolvedPredicate.IsNotNullPredicate(0);
        assertThat(StatisticsFilterSupport.decideLeaf(isNotNull, stats(null, null, 0L)))
                .isEqualTo(ALWAYS_MATCHES);
        assertThat(StatisticsFilterSupport.decideLeaf(isNotNull, stats(null, null, 3L)))
                .isEqualTo(MIGHT_MATCH);
        assertThat(StatisticsFilterSupport.decideLeaf(isNotNull, stats(null, null, null)))
                .isEqualTo(MIGHT_MATCH);
    }

    // ==================== Property: decision agrees with brute force ====================

    @Test
    void decisionAgreesWithBruteForceOnRandomData() {
        Random random = new Random(795);
        FilterPredicate.Operator[] ops = FilterPredicate.Operator.values();

        for (int round = 0; round < 10_000; round++) {
            int size = 1 + random.nextInt(50);
            long[] values = new long[size];
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            for (int i = 0; i < size; i++) {
                // Narrow domain so single-point and boundary cases occur often
                values[i] = random.nextInt(100);
                min = Math.min(min, values[i]);
                max = Math.max(max, values[i]);
            }
            long literal = random.nextInt(120) - 10;
            FilterPredicate.Operator op = ops[random.nextInt(ops.length)];

            ResolvedPredicate leaf = new ResolvedPredicate.LongPredicate(0, op, literal);
            StatsDecision decision =
                    StatisticsFilterSupport.decideLeaf(leaf, longStats(min, max, 0L));

            int matching = 0;
            for (long value : values) {
                if (matches(op, value, literal)) {
                    matching++;
                }
            }
            if (decision == ALWAYS_MATCHES) {
                assertThat(matching)
                        .as("ALWAYS_MATCHES for %s %s over [%s, %s]", op, literal, min, max)
                        .isEqualTo(size);
            }
            if (decision == CANNOT_MATCH) {
                assertThat(matching)
                        .as("CANNOT_MATCH for %s %s over [%s, %s]", op, literal, min, max)
                        .isZero();
            }
        }
    }

    private static boolean matches(FilterPredicate.Operator op, long value, long literal) {
        return switch (op) {
            case EQ -> value == literal;
            case NOT_EQ -> value != literal;
            case LT -> value < literal;
            case LT_EQ -> value <= literal;
            case GT -> value > literal;
            case GT_EQ -> value >= literal;
        };
    }

    // ==================== Fixtures ====================

    private static StatsDecision decideInt(FilterPredicate.Operator op, int value, int min, int max) {
        ResolvedPredicate leaf = new ResolvedPredicate.IntPredicate(0, op, value);
        return StatisticsFilterSupport.decideLeaf(leaf, intStats(min, max, 0L));
    }

    private static StatsDecision decideBinary(FilterPredicate.Operator op, String value,
            String min, String max) {
        ResolvedPredicate leaf = new ResolvedPredicate.BinaryPredicate(
                0, op, value.getBytes(StandardCharsets.UTF_8), false);
        return StatisticsFilterSupport.decideLeaf(leaf, stats(
                min.getBytes(StandardCharsets.UTF_8), max.getBytes(StandardCharsets.UTF_8), 0L));
    }

    private static MinMaxStats intStats(int min, int max, Long nullCount) {
        return stats(intBytes(min), intBytes(max), nullCount);
    }

    private static MinMaxStats longStats(long min, long max, Long nullCount) {
        return stats(longBytes(min), longBytes(max), nullCount);
    }

    private static MinMaxStats floatStats(float min, float max, Long nullCount) {
        return stats(floatBytes(min), floatBytes(max), nullCount);
    }

    private static MinMaxStats doubleStats(double min, double max, Long nullCount) {
        return stats(doubleBytes(min), doubleBytes(max), nullCount);
    }

    private static MinMaxStats stats(byte[] min, byte[] max, Long nullCount) {
        return new MinMaxStats() {
            @Override
            public byte[] minValue() {
                return min;
            }

            @Override
            public byte[] maxValue() {
                return max;
            }

            @Override
            public Long nullCount() {
                return nullCount;
            }
        };
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
}
