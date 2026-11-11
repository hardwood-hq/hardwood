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

import dev.hardwood.internal.metadata.DataPageHeader;
import dev.hardwood.internal.metadata.DataPageHeaderV2;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.PageType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.schema.ColumnSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// The inline-page-statistics drop path for a file without a page index: which AND-necessary
/// leaves reach a column, and whether a page's own statistics, or its header, prove it holds no
/// matching row.
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
                ROWS, BoundsReadability.ALL, PAGE)).isFalse();
        assertThat(PageDropPredicates.canDropPage(null, stats(intBytes(20), intBytes(30)),
                ROWS, BoundsReadability.ALL, PAGE)).isFalse();
    }

    @Test
    void anyOneLeafProvingNoMatchDropsThePage() {
        // [10, 20] cannot match "> 25", though it can match "> 15".
        List<ResolvedPredicate> leaves = List.of(
                new ResolvedPredicate.IntPredicate(COLUMN, Operator.GT, 15),
                new ResolvedPredicate.IntPredicate(COLUMN, Operator.GT, 25));

        assertThat(PageDropPredicates.canDropPage(leaves, stats(intBytes(10), intBytes(20)),
                ROWS, BoundsReadability.ALL, PAGE)).isTrue();
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
                PageDropPredicates.UNKNOWN_ROW_COUNT, BoundsReadability.ALL, PAGE)).isFalse();
        assertThat(PageDropPredicates.canDropPage(
                List.of(new ResolvedPredicate.IsNotNullPredicate(COLUMN, 1)), allNull,
                PageDropPredicates.UNKNOWN_ROW_COUNT, BoundsReadability.ALL, PAGE)).isFalse();
    }

    @Test
    void aPageWithNullsAmongValuesKeepsThePage() {
        // Fewer nulls than rows proves nothing on its own, and the bounds hold the probe.
        Statistics someNulls = new Statistics(intBytes(10), intBytes(20), ROWS - 1L, null, false);

        assertThat(canDropPage(intEq(15), someNulls)).isFalse();
        assertThat(canDropPage(new ResolvedPredicate.IsNotNullPredicate(COLUMN, 1), someNulls))
                .isFalse();
    }

    // ==================== An INT96 column ====================

    /// An `INT96` page's bounds are read only where its column's file records them in
    /// `INT96_TIMESTAMP_ORDER`, which the readability handed over says.
    @Test
    void anInt96PageDropsOnlyUnderTheTimestampOrder() {
        ResolvedPredicate storedBytes = new ResolvedPredicate.BinaryPredicate(COLUMN, Operator.EQ,
                int96(2_460_587, 0L), ResolvedPredicate.BinaryPredicate.Comparison.STORED_BYTES);
        Statistics oneDay = stats(int96(2_460_585, 0L), int96(2_460_585, 1_000L));

        assertThat(PageDropPredicates.canDropPage(List.of(storedBytes), oneDay, ROWS,
                BoundsReadability.ofOneColumn(true), PAGE)).isTrue();
        assertThat(PageDropPredicates.canDropPage(List.of(storedBytes), oneDay, ROWS,
                BoundsReadability.ofOneColumn(false), PAGE)).isFalse();
    }

    // ==================== Deciding a page by its header ====================

    @Test
    void aV2HeaderWhoseBoundsExcludeTheProbeDropsAnOptionalColumnsPage() {
        PageHeader page = v2(stats(intBytes(20), intBytes(30)));

        assertThat(PageDropPredicates.canDropPage(List.of(intEq(15)), page, OPTIONAL, BoundsReadability.ALL, PAGE))
                .isTrue();
    }

    @Test
    void aRequiredColumnsPageIsNeverDropped() {
        // a required column cannot hold the nulls a dropped page is replaced by
        PageHeader page = v2(stats(intBytes(20), intBytes(30)));

        assertThat(PageDropPredicates.canDropPage(List.of(intEq(15)), page, REQUIRED, BoundsReadability.ALL, PAGE))
                .isFalse();
    }

    @Test
    void aV1HeaderCountsItsValuesAsRowsOnlyAboveEveryRepeatedNode() {
        // a v1 header counts values: the rows of a flat column, nothing below a repeated node
        PageHeader allNull = v1(new Statistics(null, null, (long) ROWS, null, false));

        assertThat(PageDropPredicates.canDropPage(List.of(intEq(15)), allNull, OPTIONAL, BoundsReadability.ALL,
                PAGE)).isTrue();
        assertThat(PageDropPredicates.canDropPage(List.of(intEq(15)), allNull, REPEATED, BoundsReadability.ALL,
                PAGE)).isFalse();
        assertThat(PageDropPredicates.canDropPage(List.of(intEq(15)), v1(stats(intBytes(20), intBytes(30))),
                REPEATED, BoundsReadability.ALL, PAGE)).isTrue();
    }

    @Test
    void aPageThatIsNoDataPageIsNotDropped() {
        PageHeader dictionary = new PageHeader(PageType.DICTIONARY_PAGE, 0, 0, null, null, null, null);

        assertThat(PageDropPredicates.canDropPage(List.of(intEq(15)), dictionary, OPTIONAL, BoundsReadability.ALL,
                PAGE)).isFalse();
    }

    // ==================== Fixtures ====================

    private static final ColumnSchema OPTIONAL = column(RepetitionType.OPTIONAL, 1, 0);
    private static final ColumnSchema REQUIRED = column(RepetitionType.REQUIRED, 0, 0);
    private static final ColumnSchema REPEATED = column(RepetitionType.OPTIONAL, 2, 1);

    private static ColumnSchema column(RepetitionType repetition, int maxDefinitionLevel, int maxRepetitionLevel) {
        return new ColumnSchema(FieldPath.of("order", "price"), PhysicalType.INT32, repetition, null, COLUMN,
                maxDefinitionLevel, maxRepetitionLevel, null);
    }

    private static PageHeader v1(Statistics stats) {
        return new PageHeader(PageType.DATA_PAGE, 0, 0,
                new DataPageHeader(ROWS, Encoding.PLAIN, 0, Encoding.RLE, Encoding.RLE, stats), null, null, null);
    }

    private static PageHeader v2(Statistics stats) {
        return new PageHeader(PageType.DATA_PAGE_V2, 0, 0, null,
                new DataPageHeaderV2(ROWS, 0, ROWS, Encoding.PLAIN, 0, 0, 0, false, stats), null, null);
    }

    private static boolean canDropPage(ResolvedPredicate leaf, Statistics stats) {
        return PageDropPredicates.canDropPage(List.of(leaf), stats, ROWS, BoundsReadability.ALL, PAGE);
    }

    private static ResolvedPredicate intEq(int value) {
        return new ResolvedPredicate.IntPredicate(COLUMN, Operator.EQ, value);
    }

    private static Statistics stats(byte[] min, byte[] max) {
        return new Statistics(min, max, 0L, null, false);
    }

    private static byte[] int96(int julianDay, long nanosOfDay) {
        return ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN).putLong(nanosOfDay).putInt(julianDay).array();
    }

    private static byte[] intBytes(int value) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
    }
}
