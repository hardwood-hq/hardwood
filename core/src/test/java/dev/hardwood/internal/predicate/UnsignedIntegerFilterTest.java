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
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.ColumnIndex;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;

/// A column annotated `INT(bitWidth, isSigned = false)` orders by unsigned magnitude, and its
/// values reach a caller as the stored two's-complement bit pattern — `4_000_000_000` as
/// `-294_967_296`. Predicates over such a column order the same way the column does, so that
/// literal outranks every positive `int` rather than falling below zero.
class UnsignedIntegerFilterTest {

    /// Straddles 2^31: unsigned these run `0 < 7 < 3e9 < 4e9`, signed they run
    /// `3e9 < 4e9 < 0 < 7`, so every ordered comparison separates the two readings.
    private static final int ZERO = 0;
    private static final int SEVEN = 7;
    private static final int THREE_BILLION = (int) 3_000_000_000L;
    private static final int FOUR_BILLION = (int) 4_000_000_000L;

    private static final int[] VALUES = { ZERO, FOUR_BILLION, SEVEN, THREE_BILLION };

    /// `2^64 - 1`, which is `-1L` signed, so it outranks `7` unsigned and falls below it signed.
    private static final long HUGE_LONG = Long.parseUnsignedLong("18446744073709551615");
    private static final long SMALL_LONG = 7L;

    private static final long[] LONG_VALUES = { SMALL_LONG, HUGE_LONG };

    @Test
    void greaterThanOrdersByUnsignedMagnitude() throws Exception {
        assertThat(filteredInts(FilterPredicate.gt("v", SEVEN)))
                .containsExactly(FOUR_BILLION, THREE_BILLION);
    }

    @Test
    void lessThanOrdersByUnsignedMagnitude() throws Exception {
        assertThat(filteredInts(FilterPredicate.lt("v", THREE_BILLION)))
                .containsExactly(ZERO, SEVEN);
    }

    @Test
    void everyValueIsAtLeastZero() throws Exception {
        assertThat(filteredInts(FilterPredicate.gtEq("v", ZERO)))
                .containsExactly(ZERO, FOUR_BILLION, SEVEN, THREE_BILLION);
    }

    @Test
    void equalityFindsAValueAboveTheSignedRange() throws Exception {
        assertThat(filteredInts(FilterPredicate.eq("v", FOUR_BILLION))).containsExactly(FOUR_BILLION);
    }

    @Test
    void membershipFindsValuesOnBothSidesOfTheSignedRange() throws Exception {
        assertThat(filteredInts(FilterPredicate.in("v", SEVEN, FOUR_BILLION)))
                .containsExactly(FOUR_BILLION, SEVEN);
    }

    /// Bounds that straddle 2^31 read as `min > max` when taken signed, which the statistics
    /// layer discards as unusable. Reading them in the column's own order keeps the pair intact,
    /// so a row group that cannot hold the value is still skipped.
    @Test
    void statisticsStillPruneAcrossTheSignedBoundary() throws Exception {
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.gt("v", FOUR_BILLION), schema());
        MinMaxStats stats = MinMaxStats.of(
                new Statistics(encode(ZERO), encode(FOUR_BILLION), 0L, null, false), resolved, BoundsReadability.ALL);

        assertThat(stats.canDrop(resolved)).isTrue();
    }

    @Test
    void aSignedColumnIsUnaffected() throws Exception {
        FileSchema signed = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED,
                        new LogicalType.IntType(32, true))
                .build();

        assertThat(FilterPredicateResolver.resolve(FilterPredicate.gt("v", SEVEN), signed))
                .isInstanceOf(ResolvedPredicate.IntPredicate.class);
    }

    @Test
    void anUnsignedLongOrdersByUnsignedMagnitude() throws Exception {
        assertThat(filteredLongs(FilterPredicate.gt("v", SMALL_LONG))).containsExactly(HUGE_LONG);
    }

    @Test
    void inequalityExcludesTheValueAboveTheSignedRange() throws Exception {
        assertThat(filteredInts(FilterPredicate.notEq("v", FOUR_BILLION)))
                .containsExactly(ZERO, SEVEN, THREE_BILLION);
    }

    /// `not` inverts the operator, and the inverted comparison reads the column's order too.
    @Test
    void negationInvertsInTheColumnsOrder() throws Exception {
        assertThat(filteredInts(FilterPredicate.not(FilterPredicate.gt("v", SEVEN))))
                .containsExactly(ZERO, SEVEN);
    }

    /// `not` over a membership test becomes a conjunction of unsigned inequalities, one per
    /// member.
    @Test
    void negatedMembershipExcludesEveryMember() throws Exception {
        assertThat(filteredInts(FilterPredicate.not(FilterPredicate.in("v", SEVEN, FOUR_BILLION))))
                .containsExactly(ZERO, THREE_BILLION);
    }

    /// A width narrower than the physical type takes the unsigned form too — `INT(8, false)`
    /// holds `0..255` and orders the same either way, so the annotation alone decides.
    @Test
    void aNarrowUnsignedColumnTakesTheUnsignedForm() {
        for (int bitWidth : new int[] { 8, 16, 32 }) {
            FileSchema narrow = FileSchema.builder("schema")
                    .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED,
                            new LogicalType.IntType(bitWidth, false))
                    .build();

            assertThat(FilterPredicateResolver.resolve(FilterPredicate.gt("v", SEVEN), narrow))
                    .isInstanceOf(ResolvedPredicate.UnsignedIntPredicate.class);
            assertThat(FilterPredicateResolver.resolve(FilterPredicate.in("v", SEVEN), narrow))
                    .isInstanceOf(ResolvedPredicate.UnsignedIntInPredicate.class);
        }
    }

    /// Bounds pinned to one value above the signed range prove every row matches, which needs
    /// the pair to survive the unsigned reading in the first place.
    @Test
    void statisticsProveEveryRowMatchesAboveTheSignedRange() {
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.gt("v", THREE_BILLION), schema());
        MinMaxStats stats = MinMaxStats.of(
                new Statistics(encode(FOUR_BILLION), encode(FOUR_BILLION), 0L, null, false), resolved, BoundsReadability.ALL);

        assertThat(stats.alwaysMatches(resolved)).isTrue();
        assertThat(stats.canDrop(resolved)).isFalse();
    }

    /// The same for a membership test, whose single-point bound is bit equality and so reads the
    /// same in either order.
    @Test
    void membershipStatisticsProveEveryRowMatches() {
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.in("v", SEVEN, FOUR_BILLION), schema());
        MinMaxStats stats = MinMaxStats.of(
                new Statistics(encode(FOUR_BILLION), encode(FOUR_BILLION), 0L, null, false), resolved, BoundsReadability.ALL);

        assertThat(stats.alwaysMatches(resolved)).isTrue();
    }

    /// A membership test whose every probe falls outside the unsigned bounds drops the chunk;
    /// read signed, `4e9` would fall inside `[0, 3e9]` and the chunk would be kept.
    @Test
    void membershipStatisticsPruneAcrossTheSignedBoundary() {
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.in("v", FOUR_BILLION), schema());
        MinMaxStats stats = MinMaxStats.of(
                new Statistics(encode(ZERO), encode(THREE_BILLION), 0L, null, false), resolved, BoundsReadability.ALL);

        assertThat(stats.canDrop(resolved)).isTrue();
    }

    /// Page bounds read in the column's order too, so a page that cannot hold the value is
    /// skipped rather than discarded as inverted.
    @Test
    void pageStatisticsPruneAcrossTheSignedBoundary() {
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.gt("v", FOUR_BILLION), schema());
        ColumnIndex columnIndex = new ColumnIndex(new boolean[] { false },
                List.of(encode(ZERO)), List.of(encode(FOUR_BILLION)),
                ColumnIndex.BoundaryOrder.UNORDERED, new long[] { 0L }, null, null, null);

        assertThat(MinMaxStats.ofPage(columnIndex, 0, resolved, BoundsReadability.ALL).canDrop(resolved)).isTrue();
    }

    /// The `INT64` bounds read the same way: `[2^64 - 1, 2^64 - 1]` is `[-1, -1]` signed and
    /// would fall below a literal of `7` rather than above it.
    @Test
    void longStatisticsReadInTheColumnsOrder() {
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.gt("v", SMALL_LONG), longSchema());
        MinMaxStats stats = MinMaxStats.of(
                new Statistics(encodeLong(HUGE_LONG), encodeLong(HUGE_LONG), 0L, null, false), resolved, BoundsReadability.ALL);

        assertThat(stats.alwaysMatches(resolved)).isTrue();
        assertThat(stats.canDrop(resolved)).isFalse();
    }

    /// A predicate column below a struct is addressed by name rather than by index, which is a
    /// separate set of record-level comparisons; they order unsigned as well.
    @Test
    void aNestedUnsignedColumnOrdersByUnsignedMagnitude() throws Exception {
        FileSchema nested = FileSchema.builder("schema")
                .struct("s", RepetitionType.REQUIRED, s -> s
                        .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED,
                                new LogicalType.IntType(32, false)))
                .build();

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, nested)) {
            for (int value : VALUES) {
                final int v = value;
                writer.rowWriter().writeRow(row -> row.setStruct("s", s -> s.setInt("v", v)));
            }
        }
        try (ParquetFileReader reader = ParquetFileReader.open(
                    InputFile.of(ByteBuffer.wrap(out.toByteArray())));
                RowReader rows = reader.buildRowReader()
                        .filter(FilterPredicate.gt("s.v", SEVEN)).build()) {
            List<Integer> matched = new ArrayList<>();
            while (rows.hasNext()) {
                rows.next();
                matched.add(rows.getStruct("s").getInt("v"));
            }
            assertThat(matched).containsExactly(FOUR_BILLION, THREE_BILLION);
        }
    }

    /// A conjunct the batch compiler has no matcher for takes the whole predicate off the batch
    /// path, so the unsigned column is compared by the indexed record-level leaf instead.
    @Test
    void anUnsignedColumnOrdersUnsignedOnTheRecordPath() throws Exception {
        FileSchema mixed = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED,
                        new LogicalType.IntType(32, false))
                .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                        LogicalType.string())
                .build();

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, mixed)) {
            for (int value : VALUES) {
                final int v = value;
                writer.rowWriter().writeRow(row -> row.setInt("v", v).setString("name", "keep"));
            }
        }
        try (ParquetFileReader reader = ParquetFileReader.open(
                    InputFile.of(ByteBuffer.wrap(out.toByteArray())));
                RowReader rows = reader.buildRowReader()
                        .filter(FilterPredicate.and(
                                FilterPredicate.gt("v", SEVEN),
                                FilterPredicate.eq("name", "keep")))
                        .build()) {
            List<Integer> matched = new ArrayList<>();
            while (rows.hasNext()) {
                rows.next();
                matched.add(rows.getInt("v"));
            }
            assertThat(matched).containsExactly(FOUR_BILLION, THREE_BILLION);
        }
    }

    @Test
    void anUnsignedLongMembershipFindsAValueAboveTheSignedRange() throws Exception {
        assertThat(filteredLongs(FilterPredicate.in("v", SMALL_LONG, HUGE_LONG)))
                .containsExactly(SMALL_LONG, HUGE_LONG);
        assertThat(filteredLongs(FilterPredicate.notEq("v", SMALL_LONG))).containsExactly(HUGE_LONG);
    }

    // ==================== Fixtures ====================

    private static FileSchema schema() {
        return FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED,
                        new LogicalType.IntType(32, false))
                .build();
    }

    private static FileSchema longSchema() {
        return FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT64, RepetitionType.REQUIRED,
                        new LogicalType.IntType(64, false))
                .build();
    }

    private static List<Long> filteredLongs(FilterPredicate predicate) throws Exception {
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, longSchema())) {
            for (long value : LONG_VALUES) {
                final long v = value;
                writer.rowWriter().writeRow(row -> row.setLong("v", v));
            }
        }
        try (ParquetFileReader reader = ParquetFileReader.open(
                    InputFile.of(ByteBuffer.wrap(out.toByteArray())));
                RowReader rows = reader.buildRowReader().filter(predicate).build()) {
            List<Long> matched = new ArrayList<>();
            while (rows.hasNext()) {
                rows.next();
                matched.add(rows.getLong("v"));
            }
            return matched;
        }
    }

    private static byte[] encode(int value) {
        return ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(value).array();
    }

    private static byte[] encodeLong(long value) {
        return ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN)
                .putLong(value).array();
    }

    private static List<Integer> filteredInts(FilterPredicate predicate) throws Exception {
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema())) {
            for (int value : VALUES) {
                final int v = value;
                writer.rowWriter().writeRow(row -> row.setInt("v", v));
            }
        }
        try (ParquetFileReader reader = ParquetFileReader.open(
                    InputFile.of(ByteBuffer.wrap(out.toByteArray())));
                RowReader rows = reader.buildRowReader().filter(predicate).build()) {
            List<Integer> matched = new ArrayList<>();
            while (rows.hasNext()) {
                rows.next();
                matched.add(rows.getInt("v"));
            }
            return matched;
        }
    }
}
