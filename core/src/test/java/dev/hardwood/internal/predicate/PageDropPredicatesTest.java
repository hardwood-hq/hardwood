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
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.FilterPredicate.Operator;

import static org.assertj.core.api.Assertions.assertThat;

/// The inline-page-statistics drop path used by
/// [dev.hardwood.internal.reader.SequentialFetchPlan] when a file carries no page index:
/// which AND-necessary leaves reach a column, and whether a page's own statistics prove it
/// holds no matching row.
class PageDropPredicatesTest {

    private static final int COLUMN = 0;
    private static final LogContext PAGE = new LogContext("orders.parquet", 0)
            .withColumn(FieldPath.of("order", "price")).withPageIndex(0);

    // ==================== Collecting AND-necessary leaves ====================

    @Test
    void collectsLeavesUnderAndAndSkipsThoseUnderOr() {
        ResolvedPredicate andNecessary = intEq(1);
        ResolvedPredicate underOr = intEq(2);
        ResolvedPredicate root = new ResolvedPredicate.And(List.of(
                andNecessary,
                new ResolvedPredicate.Or(List.of(underOr, intEq(3)))));

        Map<Integer, List<ResolvedPredicate>> byColumn = PageDropPredicates.byColumn(root);

        assertThat(byColumn).containsOnlyKeys(COLUMN);
        assertThat(byColumn.get(COLUMN)).containsExactly(andNecessary);
    }

    // ==================== Dropping on the page's own statistics ====================

    @Test
    void boundsExcludingTheProbeDropThePage() {
        assertThat(canDropPage(intEq(15), stats(intBytes(20), intBytes(30)))).isTrue();
    }

    @Test
    void boundsContainingTheProbeKeepThePage() {
        assertThat(canDropPage(intEq(15), stats(intBytes(10), intBytes(20)))).isFalse();
    }

    @Test
    void invertedBoundsKeepThePage() {
        // min = 20, max = 10 reads as an empty interval, so the drop check would skip a page
        // that holds 15.
        assertThat(canDropPage(intEq(15), stats(intBytes(20), intBytes(10)))).isFalse();
    }

    @Test
    void deprecatedBoundsKeepThePage() {
        Statistics deprecated = new Statistics(intBytes(20), intBytes(30), 0L, null, true);
        assertThat(canDropPage(intEq(15), deprecated)).isFalse();
    }

    @Test
    void absentStatisticsKeepThePage() {
        assertThat(canDropPage(intEq(15), null)).isFalse();
        assertThat(canDropPage(intEq(15), stats(null, null))).isFalse();
    }

    @Test
    void noLeavesKeepThePage() {
        assertThat(PageDropPredicates.canDropPage(List.of(), stats(intBytes(20), intBytes(30)),
                PAGE)).isFalse();
        assertThat(PageDropPredicates.canDropPage(null, stats(intBytes(20), intBytes(30)),
                PAGE)).isFalse();
    }

    @Test
    void anyOneLeafProvingNoMatchDropsThePage() {
        // [10, 20] cannot match "> 25", though it can match "> 15".
        List<ResolvedPredicate> leaves = List.of(
                new ResolvedPredicate.IntPredicate(COLUMN, Operator.GT, 15),
                new ResolvedPredicate.IntPredicate(COLUMN, Operator.GT, 25));

        assertThat(PageDropPredicates.canDropPage(leaves, stats(intBytes(10), intBytes(20)),
                PAGE)).isTrue();
    }

    @Test
    void nullPredicatesNeverDropAPage() {
        assertThat(canDropPage(new ResolvedPredicate.IsNullPredicate(COLUMN, 1),
                stats(intBytes(10), intBytes(20)))).isFalse();
        assertThat(canDropPage(new ResolvedPredicate.IsNotNullPredicate(COLUMN, 1),
                stats(intBytes(10), intBytes(20)))).isFalse();
    }

    // ==================== Fixtures ====================

    private static boolean canDropPage(ResolvedPredicate leaf, Statistics stats) {
        return PageDropPredicates.canDropPage(List.of(leaf), stats,
                PAGE);
    }

    private static ResolvedPredicate intEq(int value) {
        return new ResolvedPredicate.IntPredicate(COLUMN, Operator.EQ, value);
    }

    private static Statistics stats(byte[] min, byte[] max) {
        return new Statistics(min, max, 0L, null, false);
    }

    private static byte[] intBytes(int value) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
    }
}
