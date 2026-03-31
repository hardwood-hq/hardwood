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

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.Statistics;

import static org.assertj.core.api.Assertions.assertThat;

class MinMaxStatsTest {

    @Test
    void testDeprecatedMinMaxStatsReturnsNull() {
        // Simulate deprecated statistics where min/max bytes are present but unreliable
        // (e.g. written by an older Parquet writer using unsigned byte comparison for signed types).
        // Column with values [-10, 5]: deprecated stats might have min=5, max=-10 due to
        // unsigned byte ordering, which is wrong for signed types.
        byte[] fakeMin = intBytes(5);
        byte[] fakeMax = intBytes(-10);
        Statistics deprecated = new Statistics(fakeMin, fakeMax, 0L, null, true);

        MinMaxStats stats = MinMaxStats.of(deprecated);

        // When isMinMaxDeprecated is true, minValue/maxValue must be null so that
        // canDropLeaf conservatively returns false (never drops the row group).
        assertThat(stats.minValue()).isNull();
        assertThat(stats.maxValue()).isNull();
    }

    @Test
    void testNonDeprecatedMinMaxStatsReturnsValues() {
        byte[] min = intBytes(1);
        byte[] max = intBytes(100);
        Statistics nonDeprecated = new Statistics(min, max, 0L, null, false);

        MinMaxStats stats = MinMaxStats.of(nonDeprecated);

        assertThat(stats.minValue()).isEqualTo(min);
        assertThat(stats.maxValue()).isEqualTo(max);
    }

    @Test
    void testDeprecatedStatsPreventRowGroupDrop() {
        // Scenario from issue #205: column with values [-10, 5], deprecated stats have
        // min=5 (0x00000005) and max=-10 (0xFFFFFFF6) due to unsigned comparison.
        // A GT("col", -5) predicate with correct stats would NOT drop this row group
        // because max(5) > -5. But with the inverted deprecated stats,
        // canDrop(GT, -5, 5, -10) would compute max(-10) <= -5 → true → incorrectly drop.
        byte[] deprecatedMin = intBytes(5);
        byte[] deprecatedMax = intBytes(-10);
        Statistics stats = new Statistics(deprecatedMin, deprecatedMax, 0L, null, true);

        MinMaxStats minMaxStats = MinMaxStats.of(stats);

        // With the fix, canDropLeaf sees null min/max and returns false (conservative)
        ResolvedPredicate.IntPredicate predicate = new ResolvedPredicate.IntPredicate(
                0, dev.hardwood.reader.FilterPredicate.Operator.GT, -5);
        boolean canDrop = StatisticsFilterSupport.canDropLeaf(predicate, minMaxStats);
        assertThat(canDrop).isFalse();
    }

    private static byte[] intBytes(int value) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
    }
}
