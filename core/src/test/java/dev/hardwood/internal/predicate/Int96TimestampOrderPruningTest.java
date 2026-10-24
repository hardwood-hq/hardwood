/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.reader.select.RowRanges;
import dev.hardwood.metadata.ColumnIndex;
import dev.hardwood.metadata.ColumnOrder;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;

import static dev.hardwood.internal.predicate.FilterDecision.ALWAYS_MATCHES;
import static dev.hardwood.internal.predicate.FilterDecision.CANNOT_MATCH;
import static dev.hardwood.internal.predicate.FilterDecision.MIGHT_MATCH;
import static org.assertj.core.api.Assertions.assertThat;

/// `INT96` bounds recorded in `INT96_TIMESTAMP_ORDER`: the Julian day, then the nanoseconds of the
/// day, each compared signed as stored. A file declaring the order vouches that every value keeps
/// its nanoseconds within one day, where that order is the instants' order, so the bounds decide an
/// [Instant] on any span of days and a `byte[]` literal in their own order. A pair that shows the
/// promise broken is discarded.
class Int96TimestampOrderPruningTest {

    private static final long NANOS_PER_HOUR = 3_600_000_000_000L;
    private static final long NANOS_PER_DAY = 24 * NANOS_PER_HOUR;
    private static final int OCT_1 = julianDay(LocalDate.of(2024, 10, 1));
    private static final int OCT_2 = OCT_1 + 1;
    private static final int OCT_7 = OCT_1 + 6;

    private static final FileSchema SCHEMA = FileSchema.builder("s")
            .addColumn("ts", PhysicalType.INT96, RepetitionType.REQUIRED)
            .build();
    private static final BoundsReadability CHRONOLOGICAL =
            BoundsReadability.of(SCHEMA, List.of(ColumnOrder.INT96_TIMESTAMP_ORDER), ordinal -> false);

    /// A row group of a week, as a writer recording one row group per week lays it out.
    @Test
    void boundsOnAnySpanOfDaysDecideAnInstant() {
        Statistics week = bounds(int96(OCT_1, 3 * NANOS_PER_HOUR), int96(OCT_7, 23 * NANOS_PER_HOUR));

        assertThat(decide(FilterPredicate.gtEq("ts", Instant.parse("2024-10-08T00:00:00Z")), week))
                .isEqualTo(CANNOT_MATCH);
        assertThat(decide(FilterPredicate.lt("ts", Instant.parse("2024-10-01T03:00:00Z")), week))
                .isEqualTo(CANNOT_MATCH);
        assertThat(decide(FilterPredicate.gt("ts", Instant.parse("2024-10-04T00:00:00Z")), week))
                .isEqualTo(MIGHT_MATCH);
        assertThat(decide(FilterPredicate.gtEq("ts", Instant.parse("2024-10-01T03:00:00Z")), week))
                .isEqualTo(ALWAYS_MATCHES);
        assertThat(decide(FilterPredicate.in("ts", Instant.parse("2024-09-30T00:00:00Z"),
                Instant.parse("2024-10-08T00:00:00Z")), week)).isEqualTo(CANNOT_MATCH);
        assertThat(decide(FilterPredicate.notEq("ts", Instant.parse("2024-10-03T00:00:00Z")),
                bounds(int96(OCT_1, 0), int96(OCT_1, 0)))).isEqualTo(ALWAYS_MATCHES);
    }

    /// Nanoseconds of the day below zero or past one day order below a later day's values while
    /// standing for an instant past them, which the order the file declares rules out. A pair
    /// holding them shows the file breaking it, so the pair bounds nothing.
    @Test
    void boundsHoldingNanosecondsOutsideOneDayAreDiscarded() {
        ResolvedPredicate leaf = FilterPredicateResolver.resolve(
                FilterPredicate.gtEq("ts", Instant.parse("2024-10-08T00:00:00Z")), SCHEMA);

        for (Statistics broken : List.of(
                bounds(int96(OCT_1, 0), int96(OCT_1, 5 * NANOS_PER_DAY)),
                bounds(int96(OCT_1, NANOS_PER_DAY), int96(OCT_2, 0)),
                bounds(int96(OCT_1, -1), int96(OCT_2, 0)))) {
            MinMaxStats stats = MinMaxStats.of(broken, leaf, CHRONOLOGICAL);

            assertThat(stats.discardReason()).isEqualTo(MinMaxStats.NANOS_OUTSIDE_A_DAY);
            assertThat(stats.canDrop(leaf)).isFalse();
        }
    }

    /// A `byte[]` literal is compared in the bounds' own order, in which two values are equal
    /// exactly when their bytes are.
    @Test
    void boundsDecideAByteLiteralInTheirOwnOrder() {
        Statistics bounds = bounds(int96(OCT_1, 0), int96(OCT_2, 0));
        byte[] inside = int96(OCT_1, 12 * NANOS_PER_HOUR);
        // Oct 1 12:00 spelled from the day before, which no value of a file declaring the order
        // holds: below the minimum as stored.
        byte[] belowAsStored = int96(OCT_1 - 1, 36 * NANOS_PER_HOUR);

        assertThat(decide(FilterPredicate.eq("ts", inside), bounds)).isEqualTo(MIGHT_MATCH);
        assertThat(decide(FilterPredicate.eq("ts", belowAsStored), bounds)).isEqualTo(CANNOT_MATCH);
        assertThat(decide(FilterPredicate.eq("ts", int96(OCT_2, 1)), bounds)).isEqualTo(CANNOT_MATCH);
        assertThat(decide(FilterPredicate.in("ts", belowAsStored, int96(OCT_2, 1)), bounds))
                .isEqualTo(CANNOT_MATCH);
        assertThat(decide(FilterPredicate.in("ts", belowAsStored, inside), bounds)).isEqualTo(MIGHT_MATCH);
        assertThat(decide(FilterPredicate.eq("ts", inside), bounds(inside, inside))).isEqualTo(ALWAYS_MATCHES);
    }

    /// A pair holding one value drops `notEq` on it, spelled as an instant or as its bytes. The same
    /// instant spelled from the day before is other bytes, which no row of the unit stores.
    @Test
    void aSingleValuePairDropsNotEqOnIt() {
        byte[] value = int96(OCT_1, 12 * NANOS_PER_HOUR);
        Statistics single = bounds(value, value);

        assertThat(decide(FilterPredicate.notEq("ts", Instant.parse("2024-10-01T12:00:00Z")), single))
                .isEqualTo(CANNOT_MATCH);
        assertThat(decide(FilterPredicate.notEq("ts", value), single)).isEqualTo(CANNOT_MATCH);
        assertThat(decide(FilterPredicate.notEq("ts", int96(OCT_1 - 1, 36 * NANOS_PER_HOUR)), single))
                .isEqualTo(ALWAYS_MATCHES);
    }

    /// Column-index pages carry bounds in the same order: three pages of a day each, of which an
    /// instant keeps the one holding its day, and keeps all three under `TYPE_ORDER`.
    @Test
    void pageBoundsDecideAnInstant() {
        ColumnIndex days = new ColumnIndex(new boolean[] { false, false, false },
                List.of(int96(OCT_1, 0), int96(OCT_2, 0), int96(OCT_2 + 1, 0)),
                List.of(int96(OCT_1, 23 * NANOS_PER_HOUR), int96(OCT_2, 23 * NANOS_PER_HOUR),
                        int96(OCT_2 + 1, 23 * NANOS_PER_HOUR)),
                ColumnIndex.BoundaryOrder.ASCENDING, new long[] { 0L, 0L, 0L }, null, null, null);
        OffsetIndex tenRowPages = new OffsetIndex(List.of(new PageLocation(0, 100, 0),
                new PageLocation(100, 100, 10), new PageLocation(200, 100, 20)), null);
        ResolvedPredicate leaf = FilterPredicateResolver.resolve(
                FilterPredicate.eq("ts", Instant.parse("2024-10-02T12:00:00Z")), SCHEMA);
        LogContext logContext = new LogContext(null, ExceptionContext.UNKNOWN_ROW_GROUP);

        RowRanges chronological = PageFilterEvaluator.evaluatePages(days, tenRowPages, 30, leaf, logContext,
                CHRONOLOGICAL);
        RowRanges typeDefined = PageFilterEvaluator.evaluatePages(days, tenRowPages, 30, leaf, logContext,
                BoundsReadability.of(SCHEMA, List.of(ColumnOrder.TYPE_DEFINED_ORDER), ordinal -> false));

        assertThat(chronological.overlapsPage(0, 9)).isFalse();
        assertThat(chronological.overlapsPage(10, 19)).isTrue();
        assertThat(chronological.overlapsPage(20, 29)).isFalse();
        assertThat(typeDefined.overlapsPage(0, 9)).isTrue();
        assertThat(typeDefined.overlapsPage(20, 29)).isTrue();
    }

    @Test
    void boundsThatAreNotAPairInTheOrderAreDiscarded() {
        ResolvedPredicate leaf = FilterPredicateResolver.resolve(FilterPredicate.eq("ts", int96(OCT_1, 0)), SCHEMA);

        MinMaxStats inverted = MinMaxStats.of(bounds(int96(OCT_2, 0), int96(OCT_1, 12 * NANOS_PER_HOUR)),
                leaf, CHRONOLOGICAL);
        MinMaxStats narrow = MinMaxStats.of(bounds(new byte[11], int96(OCT_1, 0)), leaf, CHRONOLOGICAL);

        assertThat(inverted.discardReason()).isEqualTo(MinMaxStats.INVERTED);
        assertThat(inverted.canDrop(leaf)).isFalse();
        assertThat(narrow.discardReason()).isEqualTo(MinMaxStats.NOT_THE_COLUMN_WIDTH);
        assertThat(narrow.canDrop(leaf)).isFalse();
    }

    /// The order is the file's, per column: the same bounds under `TYPE_ORDER` prune nothing.
    @Test
    void theOrderIsReadFromTheFile() {
        BoundsReadability typeDefined =
                BoundsReadability.of(SCHEMA, List.of(ColumnOrder.TYPE_DEFINED_ORDER), ordinal -> false);
        ResolvedPredicate leaf = FilterPredicateResolver.resolve(FilterPredicate.eq("ts", int96(OCT_2, 1)), SCHEMA);
        Statistics bounds = bounds(int96(OCT_1, 0), int96(OCT_2, 0));

        assertThat(CHRONOLOGICAL.int96TimestampOrder(0)).isTrue();
        assertThat(typeDefined.int96TimestampOrder(0)).isFalse();
        assertThat(BoundsReadability.ALL.int96TimestampOrder(0)).isFalse();
        assertThat(MinMaxStats.of(bounds, leaf, typeDefined).canDrop(leaf)).isFalse();
        assertThat(MinMaxStats.of(bounds, leaf, typeDefined).discardReason()).isNull();
    }

    /// `predicate_int96_ordered_multi.parquet` declares `INT96_TIMESTAMP_ORDER` and holds ten
    /// hourly rows per row group. An instant prunes every row group but the one holding it; the
    /// predicates agree with the values through every read path in `PredicatePathAgreementTest`.
    @Test
    void aFileDeclaringTheOrderPrunesItsRowGroups() throws IOException {
        Path path = Paths.get("src/test/resources/predicate/predicate_int96_ordered_multi.parquet");
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) {
            FileSchema schema = reader.getFileSchema();
            BoundsReadability readability = BoundsReadability.of(schema,
                    reader.getFileMetaData().columnOrders(), ordinal -> false);
            Instant rowTwoHundred;
            try (RowReader rows = reader.buildRowReader().build()) {
                for (int row = 0; row <= 200; row++) {
                    assertThat(rows.hasNext()).isTrue();
                    rows.next();
                }
                rowTwoHundred = rows.getTimestamp("ts96");
            }
            ResolvedPredicate resolved = FilterPredicateResolver.resolve(FilterPredicate.eq("ts96", rowTwoHundred), schema);

            List<FilterDecision> decisions = new ArrayList<>();
            for (RowGroup rowGroup : reader.getFileMetaData().rowGroups()) {
                decisions.add(RowGroupFilterEvaluator.decideRowGroup(resolved, rowGroup, null, null,
                        new LogContext(null, ExceptionContext.UNKNOWN_ROW_GROUP), readability));
            }

            assertThat(decisions.get(20)).isEqualTo(MIGHT_MATCH);
            assertThat(decisions).filteredOn(decision -> decision == CANNOT_MATCH).hasSize(decisions.size() - 1);
        }
    }

    private static FilterDecision decide(FilterPredicate predicate, Statistics bounds) {
        ResolvedPredicate leaf = FilterPredicateResolver.resolve(predicate, SCHEMA);
        return MinMaxStats.of(bounds, leaf, CHRONOLOGICAL).decideLeaf(leaf, true);
    }

    private static Statistics bounds(byte[] min, byte[] max) {
        return new Statistics(min, max, 0L, null, false);
    }

    private static byte[] int96(int julianDay, long nanosOfDay) {
        return ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN).putLong(nanosOfDay).putInt(julianDay).array();
    }

    private static int julianDay(LocalDate date) {
        return Math.toIntExact(date.toEpochDay() + 2_440_588);
    }
}
