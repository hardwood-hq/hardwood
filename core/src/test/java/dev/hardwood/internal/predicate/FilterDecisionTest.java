/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.nio.charset.StandardCharsets;
import java.util.Random;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.predicate.ResolvedPredicate.BinaryPredicate.Comparison;
import dev.hardwood.reader.FilterPredicate;

import static dev.hardwood.internal.predicate.FilterDecision.ALWAYS_MATCHES;
import static dev.hardwood.internal.predicate.FilterDecision.CANNOT_MATCH;
import static dev.hardwood.internal.predicate.FilterDecision.MIGHT_MATCH;
import static org.assertj.core.api.Assertions.assertThat;

/// Unit matrix for the three-valued statistics decision: [FilterDecision] combinators and
/// [MinMaxStats#decideLeaf] over synthetic bounds.
///
/// The [#decisionAgreesWithBruteForceOnRandomData] property pins the decision's meaning
/// against decoded values: [FilterDecision#ALWAYS_MATCHES] must imply zero non-matching
/// rows and [FilterDecision#CANNOT_MATCH] zero matching rows, over randomized
/// high-cardinality data (low-cardinality fixtures satisfy broken decisions silently).
class FilterDecisionTest {

    // ==================== Combinators ====================

    @Test
    void andCombinator() {
        assertThat(FilterDecision.and(ALWAYS_MATCHES, ALWAYS_MATCHES)).isEqualTo(ALWAYS_MATCHES);
        assertThat(FilterDecision.and(ALWAYS_MATCHES, MIGHT_MATCH)).isEqualTo(MIGHT_MATCH);
        assertThat(FilterDecision.and(MIGHT_MATCH, ALWAYS_MATCHES)).isEqualTo(MIGHT_MATCH);
        assertThat(FilterDecision.and(MIGHT_MATCH, MIGHT_MATCH)).isEqualTo(MIGHT_MATCH);
        assertThat(FilterDecision.and(CANNOT_MATCH, ALWAYS_MATCHES)).isEqualTo(CANNOT_MATCH);
        assertThat(FilterDecision.and(ALWAYS_MATCHES, CANNOT_MATCH)).isEqualTo(CANNOT_MATCH);
        assertThat(FilterDecision.and(CANNOT_MATCH, MIGHT_MATCH)).isEqualTo(CANNOT_MATCH);
        assertThat(FilterDecision.and(CANNOT_MATCH, CANNOT_MATCH)).isEqualTo(CANNOT_MATCH);
    }

    @Test
    void orCombinator() {
        assertThat(FilterDecision.or(ALWAYS_MATCHES, ALWAYS_MATCHES)).isEqualTo(ALWAYS_MATCHES);
        assertThat(FilterDecision.or(ALWAYS_MATCHES, MIGHT_MATCH)).isEqualTo(ALWAYS_MATCHES);
        assertThat(FilterDecision.or(ALWAYS_MATCHES, CANNOT_MATCH)).isEqualTo(ALWAYS_MATCHES);
        assertThat(FilterDecision.or(CANNOT_MATCH, ALWAYS_MATCHES)).isEqualTo(ALWAYS_MATCHES);
        assertThat(FilterDecision.or(MIGHT_MATCH, MIGHT_MATCH)).isEqualTo(MIGHT_MATCH);
        assertThat(FilterDecision.or(MIGHT_MATCH, CANNOT_MATCH)).isEqualTo(MIGHT_MATCH);
        assertThat(FilterDecision.or(CANNOT_MATCH, MIGHT_MATCH)).isEqualTo(MIGHT_MATCH);
        assertThat(FilterDecision.or(CANNOT_MATCH, CANNOT_MATCH)).isEqualTo(CANNOT_MATCH);
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
        assertThat(intStats(42, 42, 0L).decideLeaf(in))
                .isEqualTo(ALWAYS_MATCHES);
        assertThat(intStats(42, 43, 0L).decideLeaf(in))
                .isEqualTo(MIGHT_MATCH);
        assertThat(intStats(6, 41, 0L).decideLeaf(in))
                .isEqualTo(CANNOT_MATCH);
    }

    // ==================== Null-count gating ====================

    @Test
    void nullsPreventAlwaysMatchesForValuePredicates() {
        // [10, 20] fully satisfies GT 5, but nulls (or an unknown null count) may hide
        // non-matching rows: a null row satisfies no value predicate.
        ResolvedPredicate gt = new ResolvedPredicate.IntPredicate(0, FilterPredicate.Operator.GT, 5);
        assertThat(intStats(10, 20, 0L).decideLeaf(gt))
                .isEqualTo(ALWAYS_MATCHES);
        assertThat(intStats(10, 20, 3L).decideLeaf(gt))
                .isEqualTo(MIGHT_MATCH);
        assertThat(intStats(10, 20, null).decideLeaf(gt))
                .isEqualTo(MIGHT_MATCH);
    }

    @Test
    void missingBoundsNeverPromiseAlwaysMatches() {
        ResolvedPredicate gt = new ResolvedPredicate.IntPredicate(0, FilterPredicate.Operator.GT, 5);
        assertThat(noBounds(0L).decideLeaf(gt))
                .isEqualTo(MIGHT_MATCH);
    }

    // ==================== Floating point: never ALWAYS_MATCHES ====================

    @Test
    void floatingPointNeverPromisesAlwaysMatches() {
        // nan_count is not consumed yet: NaN rows sit outside [min, max], so even a
        // fully-satisfying interval cannot promise every row for FP columns.
        ResolvedPredicate gtDouble =
                new ResolvedPredicate.DoublePredicate(0, FilterPredicate.Operator.GT, 1.0);
        assertThat(doubleStats(10.0, 20.0, 0L).decideLeaf(gtDouble))
                .isEqualTo(MIGHT_MATCH);

        ResolvedPredicate gtFloat =
                new ResolvedPredicate.FloatPredicate(0, FilterPredicate.Operator.GT, 1.0f);
        assertThat(floatStats(10.0f, 20.0f, 0L).decideLeaf(gtFloat))
                .isEqualTo(MIGHT_MATCH);

        // The CANNOT_MATCH side is unaffected.
        ResolvedPredicate gtOutside =
                new ResolvedPredicate.DoublePredicate(0, FilterPredicate.Operator.GT, 25.0);
        assertThat(doubleStats(10.0, 20.0, 0L).decideLeaf(gtOutside))
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
            FilterDecision decision =
                    longStats(min, max, 0L).decideLeaf(leaf);

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

    private static FilterDecision decideInt(FilterPredicate.Operator op, int value, int min, int max) {
        ResolvedPredicate leaf = new ResolvedPredicate.IntPredicate(0, op, value);
        return intStats(min, max, 0L).decideLeaf(leaf);
    }

    private static FilterDecision decideBinary(FilterPredicate.Operator op, String value,
            String min, String max) {
        ResolvedPredicate leaf = new ResolvedPredicate.BinaryPredicate(
                0, op, value.getBytes(StandardCharsets.UTF_8), Comparison.BYTE_STRING);
        return MinMaxStats.BinaryStats.of(min.getBytes(StandardCharsets.UTF_8),
                max.getBytes(StandardCharsets.UTF_8), false, 0L).decideLeaf(leaf);
    }

    private static MinMaxStats intStats(int min, int max, Long nullCount) {
        return MinMaxStats.IntStats.of(min, max, nullCount);
    }

    private static MinMaxStats longStats(long min, long max, Long nullCount) {
        return MinMaxStats.LongStats.of(min, max, nullCount);
    }

    private static MinMaxStats floatStats(float min, float max, Long nullCount) {
        return MinMaxStats.FloatStats.of(min, max, false, nullCount);
    }

    private static MinMaxStats doubleStats(double min, double max, Long nullCount) {
        return MinMaxStats.DoubleStats.of(min, max, false, nullCount);
    }

    private static MinMaxStats noBounds(Long nullCount) {
        return new MinMaxStats.NullCountOnlyStats(nullCount, null);
    }
}
