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
import org.junit.jupiter.api.extension.RegisterExtension;

import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.FilterPredicate.Operator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MinMaxStatsTest {

    /// The leaf every case here decides against, and the one the bounds are read in the order
    /// of: `INT32`, compared signed.
    private static final ResolvedPredicate.IntPredicate GT_MINUS_FIVE =
            new ResolvedPredicate.IntPredicate(0, Operator.GT, -5);

    @RegisterExtension
    final CapturedWarnings warnings = new CapturedWarnings();

    @Test
    void testDeprecatedMinMaxStatsReturnsNull() {
        // Simulate deprecated statistics where min/max bytes are present but unreliable
        // (e.g. written by an older Parquet writer using unsigned byte comparison for signed types).
        // Column with values [-10, 5]: deprecated stats might have min=5, max=-10 due to
        // unsigned byte ordering, which is wrong for signed types.
        byte[] fakeMin = intBytes(5);
        byte[] fakeMax = intBytes(-10);
        Statistics deprecated = new Statistics(fakeMin, fakeMax, 0L, null, true);

        MinMaxStats stats = MinMaxStats.of(deprecated, GT_MINUS_FIVE);

        // When isMinMaxDeprecated is true, minValue/maxValue must be null so that
        // canDropLeaf conservatively returns false (never drops the row group).
        assertThat(stats)
                .isInstanceOf(MinMaxStats.NullCountOnlyStats.class)
                .extracting(MinMaxStats::discardReason)
                .isEqualTo("they come from the deprecated min/max fields, which compare unsigned");
    }

    @Test
    void testNonDeprecatedMinMaxStatsReturnsValues() {
        byte[] min = intBytes(1);
        byte[] max = intBytes(100);
        Statistics nonDeprecated = new Statistics(min, max, 0L, null, false);

        MinMaxStats stats = MinMaxStats.of(nonDeprecated, GT_MINUS_FIVE);

        assertThat(stats).isEqualTo(new MinMaxStats.IntStats(1, 100, 0L));
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

        MinMaxStats minMaxStats = MinMaxStats.of(stats, GT_MINUS_FIVE);

        // With the fix, canDropLeaf sees null min/max and returns false (conservative)
        boolean canDrop = minMaxStats.canDrop(GT_MINUS_FIVE);
        assertThat(canDrop).isFalse();
    }

    @Test
    void boundsSourcedForOneWidthRefuseALeafOfAnother() {
        // A unit is decoded for the leaf it is then asked about, so a mismatch is a wiring
        // mistake in the reader rather than anything a file can cause. It fails loudly rather
        // than answering "cannot prove anything", which would silently disable pruning.
        MinMaxStats int32 = MinMaxStats.IntStats.of(10, 20, 0L);
        ResolvedPredicate longLeaf = new ResolvedPredicate.LongPredicate(0, Operator.GT, 5L);

        assertThatThrownBy(() -> int32.canDrop(longLeaf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("INT32 statistics cannot decide a LongPredicate");
        assertThatThrownBy(() -> int32.alwaysMatches(longLeaf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("INT32 statistics cannot decide a LongPredicate");
    }

    @Test
    void aLeafThatReadsNoBoundsGetsNoneAndDiscardsNothing() {
        // IS NULL and IS NOT NULL are decided by the null count; the bytes the file wrote are
        // not theirs to read, and not discarded either — nothing should warn about them.
        MinMaxStats stats = MinMaxStats.of(
                new Statistics(intBytes(10), intBytes(20), 0L, null, false),
                new ResolvedPredicate.IsNullPredicate(0, 1));

        assertThat(stats).isInstanceOf(MinMaxStats.NullCountOnlyStats.class);
        assertThat(stats.discardReason()).isNull();
        assertThat(stats.nullCount()).isZero();
    }

    @Test
    void discardedBoundsSayWhereTheyCameFromAndWhy() {
        MinMaxStats stats = MinMaxStats.of(
                new Statistics(intBytes(20), intBytes(10), 0L, null, false), GT_MINUS_FIVE);

        stats.reportIfDiscarded(chunk());

        assertThat(warnings.messages()).containsExactly(
                "[orders.parquet: row group 3, column 'order.price'] Ignoring the min/max "
                        + "statistics for pruning: the minimum sorts above the maximum. Rows they "
                        + "could have skipped are read and filtered instead.");
    }

    @Test
    void aPageSaysWhichPageItWas() {
        MinMaxStats stats = MinMaxStats.of(
                new Statistics(intBytes(20), intBytes(10), 0L, null, false), GT_MINUS_FIVE);

        stats.reportIfDiscarded(chunk().withPageIndex(7));

        assertThat(warnings.messages()).singleElement().asString()
                .startsWith("[orders.parquet: row group 3, column 'order.price', page 7] ");
    }

    @Test
    void usableBoundsSayNothing() {
        MinMaxStats stats = MinMaxStats.of(
                new Statistics(intBytes(10), intBytes(20), 0L, null, false), GT_MINUS_FIVE);

        stats.reportIfDiscarded(chunk());

        assertThat(warnings.messages()).isEmpty();
    }

    @Test
    void everyDiscardIsReported() {
        // Repeats are not collapsed: statistics that will not compare are rare, and a reader
        // who finds the volume unhelpful can raise the level on this logger.
        MinMaxStats stats = MinMaxStats.of(
                new Statistics(intBytes(20), intBytes(10), 0L, null, false), GT_MINUS_FIVE);
        stats.reportIfDiscarded(chunk().withPageIndex(0));
        stats.reportIfDiscarded(chunk().withPageIndex(1));

        assertThat(warnings.messages()).hasSize(2);
    }

    private static LogContext chunk() {
        return new LogContext("orders.parquet", 3).withColumn(FieldPath.of("order", "price"));
    }

    private static byte[] intBytes(int value) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
    }
}
