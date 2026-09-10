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
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnIndex;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.SizeStatistics;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.FilterPredicate.Operator;

import static dev.hardwood.internal.predicate.FilterDecision.ALWAYS_MATCHES;
import static dev.hardwood.internal.predicate.FilterDecision.CANNOT_MATCH;
import static dev.hardwood.internal.predicate.FilterDecision.MIGHT_MATCH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// [UnitStats#decide] over a column chunk and over a page carrying the same statistics: the two
/// units are sourced from different structures, and must prove the same things from them.
class UnitStatsTest {

    private static final LogContext UNNAMED =
            new LogContext(null, ExceptionContext.UNKNOWN_ROW_GROUP);

    private static final int ROWS = 100;

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void chunkAndPageProveTheSameFromTheSameStatistics(String name, ResolvedPredicate leaf,
            long nullCount, long[] histogram, FilterDecision expected) {
        assertThat(chunk(nullCount, histogram).decide(leaf, UNNAMED)).isEqualTo(expected);
        assertThat(page(nullCount, histogram).decide(leaf, UNNAMED)).isEqualTo(expected);
    }

    static Stream<Arguments> chunkAndPageProveTheSameFromTheSameStatistics() {
        ResolvedPredicate isNull = new ResolvedPredicate.IsNullPredicate(0, 1);
        ResolvedPredicate isNotNull = new ResolvedPredicate.IsNotNullPredicate(0, 1);
        ResolvedPredicate groupIsNull = new ResolvedPredicate.IsNullPredicate(0, 2, 3);
        ResolvedPredicate groupIsNotNull = new ResolvedPredicate.IsNotNullPredicate(0, 2, 3);
        ResolvedPredicate gt5 = new ResolvedPredicate.IntPredicate(0, Operator.GT, 5);
        ResolvedPredicate gt15 = new ResolvedPredicate.IntPredicate(0, Operator.GT, 15);

        return Stream.of(
                Arguments.of("IS NULL, no nulls", isNull, 0L, null, CANNOT_MATCH),
                Arguments.of("IS NULL, some nulls", isNull, 40L, null, MIGHT_MATCH),
                Arguments.of("IS NULL, every row null", isNull, 100L, null, MIGHT_MATCH),
                Arguments.of("IS NULL, null count unknown", isNull, NullStats.UNKNOWN_NULL_COUNT, null, MIGHT_MATCH),
                Arguments.of("IS NOT NULL, no nulls", isNotNull, 0L, null, ALWAYS_MATCHES),
                Arguments.of("IS NOT NULL, some nulls", isNotNull, 40L, null, MIGHT_MATCH),
                Arguments.of("IS NOT NULL, every row null", isNotNull, 100L, null, CANNOT_MATCH),
                Arguments.of("value, bounds satisfy it, no nulls", gt5, 0L, null, ALWAYS_MATCHES),
                Arguments.of("value, bounds straddle it", gt15, 0L, null, MIGHT_MATCH),
                Arguments.of("value, bounds satisfy it, some nulls", gt5, 40L, null, MIGHT_MATCH),
                Arguments.of("value, every row null", gt5, 100L, null, CANNOT_MATCH),
                Arguments.of("group IS NULL, group always absent", groupIsNull, 100L,
                        new long[]{ 30, 70, 0, 0 }, ALWAYS_MATCHES),
                Arguments.of("group IS NULL, group always present", groupIsNull, 40L,
                        new long[]{ 0, 0, 40, 60 }, CANNOT_MATCH),
                Arguments.of("group IS NOT NULL, group always present", groupIsNotNull, 40L,
                        new long[]{ 0, 0, 40, 60 }, ALWAYS_MATCHES),
                Arguments.of("group IS NOT NULL, histogram short of the rows", groupIsNotNull, 40L,
                        new long[]{ 0, 0, 40, 50 }, MIGHT_MATCH),
                // The null count equals the row count, which a fallback to it would read as the
                // group absent throughout.
                Arguments.of("group IS NOT NULL, no histogram", groupIsNotNull, 100L, null, MIGHT_MATCH),
                Arguments.of("group IS NULL, histogram of the wrong length", groupIsNull, 100L,
                        new long[]{ 30, 70, 0 }, MIGHT_MATCH));
    }

    @Test
    void chunkOfAColumnTheRowGroupDoesNotCarryProvesNothing() {
        RowGroup rowGroup = new RowGroup(List.of(chunkWith(0L, new long[]{ 0, 0, 40, 60 })), 1000, ROWS);
        UnitStats absent = UnitStats.ChunkStats.of(rowGroup, 1, BoundsReadability.ALL);

        assertThat(absent.decide(new ResolvedPredicate.IsNullPredicate(1, 1), UNNAMED)).isEqualTo(MIGHT_MATCH);
        assertThat(absent.decide(new ResolvedPredicate.IsNotNullPredicate(1, 1), UNNAMED)).isEqualTo(MIGHT_MATCH);
        assertThat(absent.decide(new ResolvedPredicate.IsNullPredicate(1, 2, 3), UNNAMED)).isEqualTo(MIGHT_MATCH);
        assertThat(absent.decide(new ResolvedPredicate.IntPredicate(1, Operator.GT, 5), UNNAMED))
                .isEqualTo(MIGHT_MATCH);
    }

    @Test
    void pageSpansTheRowsUpToTheNextPage() {
        ColumnIndex columnIndex = columnIndex(new boolean[]{ false, false }, null, null);
        List<PageLocation> pages = List.of(new PageLocation(0, 100, 0), new PageLocation(100, 100, 30));

        assertThat(UnitStats.IndexPageStats.of(columnIndex, pages, 0, 70, BoundsReadability.ALL).rowCount())
                .isEqualTo(30);
        assertThat(UnitStats.IndexPageStats.of(columnIndex, pages, 1, 70, BoundsReadability.ALL).rowCount())
                .isEqualTo(40);
    }

    @Test
    void pageNullOnEveryRowIsProvenByItsCountAlone() {
        // Neither page is flagged null-only. The second page's null count equals the 40 rows it
        // spans; the first page's falls one short of its 30.
        ColumnIndex columnIndex = columnIndex(new boolean[]{ false, false }, new long[]{ 29, 40 }, null);
        List<PageLocation> pages = List.of(new PageLocation(0, 100, 0), new PageLocation(100, 100, 30));
        UnitStats first = UnitStats.IndexPageStats.of(columnIndex, pages, 0, 70, BoundsReadability.ALL);
        UnitStats second = UnitStats.IndexPageStats.of(columnIndex, pages, 1, 70, BoundsReadability.ALL);
        ResolvedPredicate isNotNull = new ResolvedPredicate.IsNotNullPredicate(0, 1);
        ResolvedPredicate gt5 = new ResolvedPredicate.IntPredicate(0, Operator.GT, 5);

        assertThat(first.decide(isNotNull, UNNAMED)).isEqualTo(MIGHT_MATCH);
        assertThat(first.decide(gt5, UNNAMED)).isEqualTo(MIGHT_MATCH);
        assertThat(second.decide(isNotNull, UNNAMED)).isEqualTo(CANNOT_MATCH);
        assertThat(second.decide(gt5, UNNAMED)).isEqualTo(CANNOT_MATCH);
    }

    @Test
    void compoundPredicateIsNotALeaf() {
        ResolvedPredicate and = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.IntPredicate(0, Operator.GT, 5),
                new ResolvedPredicate.IntPredicate(0, Operator.LT, 25)));

        assertThatThrownBy(() -> chunk(0L, null).decide(and, UNNAMED))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unit statistics decide a leaf predicate, not And");
    }

    // ==================== Fixtures ====================

    /// A chunk of [#ROWS] rows bounded `[10, 20]`.
    private static UnitStats chunk(long nullCount, long[] histogram) {
        RowGroup rowGroup = new RowGroup(List.of(chunkWith(nullCount, histogram)), 1000, ROWS);
        return UnitStats.ChunkStats.of(rowGroup, 0, BoundsReadability.ALL);
    }

    /// The only page of a chunk of [#ROWS] rows bounded `[10, 20]`, flagged null-only exactly
    /// when its null count is its row count.
    private static UnitStats page(long nullCount, long[] histogram) {
        ColumnIndex columnIndex = columnIndex(
                new boolean[]{ nullCount == ROWS },
                nullCount == NullStats.UNKNOWN_NULL_COUNT ? null : new long[]{ nullCount },
                histogram);
        return UnitStats.IndexPageStats.of(columnIndex, List.of(new PageLocation(0, 100, 0)), 0, ROWS,
                BoundsReadability.ALL);
    }

    private static ColumnChunk chunkWith(long nullCount, long[] histogram) {
        Statistics statistics = new Statistics(intBytes(10), intBytes(20),
                nullCount == NullStats.UNKNOWN_NULL_COUNT ? null : nullCount, null, false);
        ColumnMetaData metaData = new ColumnMetaData(
                PhysicalType.INT32, List.of(Encoding.PLAIN), FieldPath.of("order", "price"),
                CompressionCodec.UNCOMPRESSED, ROWS, 1000, 1000, Map.of(), 0, null, statistics,
                null, null, null, List.of(), new SizeStatistics(null, null, histogram));
        return new ColumnChunk(metaData, null, null, null, null, "");
    }

    private static ColumnIndex columnIndex(boolean[] nullPages, long[] nullCounts, long[] histograms) {
        List<byte[]> minValues = new ArrayList<>();
        List<byte[]> maxValues = new ArrayList<>();
        for (int i = 0; i < nullPages.length; i++) {
            minValues.add(intBytes(10));
            maxValues.add(intBytes(20));
        }
        return new ColumnIndex(nullPages, minValues, maxValues, ColumnIndex.BoundaryOrder.UNORDERED,
                nullCounts, null, histograms, null);
    }

    private static byte[] intBytes(int value) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
    }
}
