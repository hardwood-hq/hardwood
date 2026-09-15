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

    /// The rows a `DataPageHeaderV2` counts for the page under test.
    private static final int ROWS = 100;

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
                ROWS, PAGE)).isFalse();
        assertThat(PageDropPredicates.canDropPage(null, stats(intBytes(20), intBytes(30)),
                ROWS, PAGE)).isFalse();
    }

    @Test
    void anyOneLeafProvingNoMatchDropsThePage() {
        // [10, 20] cannot match "> 25", though it can match "> 15".
        List<ResolvedPredicate> leaves = List.of(
                new ResolvedPredicate.IntPredicate(COLUMN, Operator.GT, 15),
                new ResolvedPredicate.IntPredicate(COLUMN, Operator.GT, 25));

        assertThat(PageDropPredicates.canDropPage(leaves, stats(intBytes(10), intBytes(20)),
                ROWS, PAGE)).isTrue();
    }

    // ==================== A page null on every row ====================

    @Test
    void aPageNullOnEveryRowDropsForALeafANullFails() {
        // The page's null count accounts for every row it holds, so no row holds a value to
        // compare, and IS NOT NULL has nothing to return either.
        Statistics allNull = new Statistics(null, null, (long) ROWS, null, false);

        assertThat(canDropPage(intEq(15), allNull)).isTrue();
        assertThat(canDropPage(new ResolvedPredicate.IsNotNullPredicate(COLUMN, 1), allNull)).isTrue();
    }

    @Test
    void aPageNullOnEveryRowIsNotDroppedForIsNull() {
        // A dropped page is replaced by nulls, which IS NULL matches, so it never reaches the
        // leaves this path drops with — and cannot drop a page even when handed one directly.
        Statistics allNull = new Statistics(null, null, (long) ROWS, null, false);
        ResolvedPredicate isNull = new ResolvedPredicate.IsNullPredicate(COLUMN, 1);

        assertThat(PageDropPredicates.byColumn(isNull)).isEmpty();
        assertThat(canDropPage(isNull, allNull)).isFalse();
    }

    @Test
    void aPageWithoutARowCountKeepsItsNullCountUnread() {
        // Below a repeated node a v1 header gives no row count, and a null count equal to the
        // page's values proves nothing about its rows.
        Statistics allNull = new Statistics(null, null, (long) ROWS, null, false);

        assertThat(PageDropPredicates.canDropPage(List.of(intEq(15)), allNull,
                PageDropPredicates.UNKNOWN_ROW_COUNT, PAGE)).isFalse();
        assertThat(PageDropPredicates.canDropPage(
                List.of(new ResolvedPredicate.IsNotNullPredicate(COLUMN, 1)), allNull,
                PageDropPredicates.UNKNOWN_ROW_COUNT, PAGE)).isFalse();
    }

    @Test
    void aPageWithNullsAmongValuesKeepsThePage() {
        // Fewer nulls than rows proves nothing on its own, and the bounds hold the probe.
        Statistics someNulls = new Statistics(intBytes(10), intBytes(20), ROWS - 1L, null, false);

        assertThat(canDropPage(intEq(15), someNulls)).isFalse();
        assertThat(canDropPage(new ResolvedPredicate.IsNotNullPredicate(COLUMN, 1), someNulls))
                .isFalse();
    }

    // ==================== Fixtures ====================

    private static boolean canDropPage(ResolvedPredicate leaf, Statistics stats) {
        return PageDropPredicates.canDropPage(List.of(leaf), stats, ROWS, PAGE);
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
