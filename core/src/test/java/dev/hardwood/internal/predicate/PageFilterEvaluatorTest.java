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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.ParquetMetadataReader;
import dev.hardwood.internal.reader.RowGroupIndexBuffers;
import dev.hardwood.internal.reader.RowRanges;
import dev.hardwood.metadata.BoundingBox;
import dev.hardwood.metadata.ColumnIndex;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.GeospatialStatistics;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.schema.FileSchema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PageFilterEvaluatorTest {

    // Integer Filtering Tests

    // 3 pages: [1,10], [11,20], [21,30] with 30 rows each
    private static final ColumnIndex INT_COLUMN_INDEX = intColumnIndex(
            new int[]{ 1, 11, 21 },
            new int[]{ 10, 20, 30 });
    private static final OffsetIndex THREE_PAGE_OFFSET_INDEX = offsetIndex(30, 30, 30);
    private static final long THREE_PAGE_ROW_COUNT = 90;

    @ParameterizedTest(name = "{0} {1} → pages kept: [{2}, {3}, {4}]")
    @MethodSource
    void testIntPageFiltering(Operator op, int value, boolean page0Kept, boolean page1Kept, boolean page2Kept) {
        RowRanges ranges = PageFilterEvaluator.evaluatePages(INT_COLUMN_INDEX, THREE_PAGE_OFFSET_INDEX, THREE_PAGE_ROW_COUNT,
                (columnIndex, pageIndex) -> {
                    int min = StatisticsDecoder.decodeInt(columnIndex.minValues().get(pageIndex));
                    int max = StatisticsDecoder.decodeInt(columnIndex.maxValues().get(pageIndex));
                    return StatisticsFilterSupport.canDrop(op, value, min, max);
                });

        assertEquals(page0Kept, ranges.overlapsPage(0, 30),  "page 0 (rows 0-30)");
        assertEquals(page1Kept, ranges.overlapsPage(30, 60), "page 1 (rows 30-60)");
        assertEquals(page2Kept, ranges.overlapsPage(60, 90), "page 2 (rows 60-90)");
    }

    /// Provides (operator, value, page0Kept, page1Kept, page2Kept) for the three-page int layout.
    static Stream<Arguments> testIntPageFiltering() {
        return Stream.of(
                // EQ: value in range of page 1 only
                Arguments.of(Operator.EQ,    15,  false, true,  false),
                // EQ: value below all pages
                Arguments.of(Operator.EQ,    0,   false, false, false),
                // EQ: value in range of page 0
                Arguments.of(Operator.EQ,    5,   true,  false, false),

                // NOT_EQ: only drops when min==max==value (never for these ranges)
                Arguments.of(Operator.NOT_EQ, 15, true,  true,  true),

                // LT: keep pages where min < value
                Arguments.of(Operator.LT,    11,  true,  false, false),
                Arguments.of(Operator.LT,    21,  true,  true,  false),
                Arguments.of(Operator.LT,    1,   false, false, false),

                // LT_EQ: keep pages where min <= value
                Arguments.of(Operator.LT_EQ, 10,  true,  false, false),
                Arguments.of(Operator.LT_EQ, 20,  true,  true,  false),
                Arguments.of(Operator.LT_EQ, 0,   false, false, false),

                // GT: keep pages where max > value
                Arguments.of(Operator.GT,    20,  false, false, true),
                Arguments.of(Operator.GT,    10,  false, true,  true),
                Arguments.of(Operator.GT,    30,  false, false, false),

                // GT_EQ: keep pages where max >= value
                Arguments.of(Operator.GT_EQ, 21,  false, false, true),
                Arguments.of(Operator.GT_EQ, 11,  false, true,  true),
                Arguments.of(Operator.GT_EQ, 31,  false, false, false)
        );
    }

    @Test
    void testAllPagesMatch() {
        RowRanges ranges = PageFilterEvaluator.evaluatePages(INT_COLUMN_INDEX, THREE_PAGE_OFFSET_INDEX, THREE_PAGE_ROW_COUNT,
                (columnIndex, pageIndex) -> {
                    int min = StatisticsDecoder.decodeInt(columnIndex.minValues().get(pageIndex));
                    int max = StatisticsDecoder.decodeInt(columnIndex.maxValues().get(pageIndex));
                    return StatisticsFilterSupport.canDrop(Operator.GT, 0, min, max);
                });

        assertEquals(1, ranges.intervalCount());
        assertTrue(ranges.overlapsPage(0, 90));
    }

    @Test
    void testNoPagesMatch() {
        RowRanges ranges = PageFilterEvaluator.evaluatePages(INT_COLUMN_INDEX, THREE_PAGE_OFFSET_INDEX, THREE_PAGE_ROW_COUNT,
                (columnIndex, pageIndex) -> {
                    int min = StatisticsDecoder.decodeInt(columnIndex.minValues().get(pageIndex));
                    int max = StatisticsDecoder.decodeInt(columnIndex.maxValues().get(pageIndex));
                    return StatisticsFilterSupport.canDrop(Operator.GT, 100, min, max);
                });

        assertEquals(0, ranges.intervalCount());
    }

    @Test
    void testNullOnlyPagesAreSkipped() {
        // Page 1 is null-only
        ColumnIndex nullPageColumnIndex = new ColumnIndex(
                List.of(false, true, false),
                List.of(intBytes(1), intBytes(0), intBytes(21)),
                List.of(intBytes(10), intBytes(0), intBytes(30)),
                ColumnIndex.BoundaryOrder.UNORDERED,
                null, null);

        RowRanges ranges = PageFilterEvaluator.evaluatePages(nullPageColumnIndex, THREE_PAGE_OFFSET_INDEX, THREE_PAGE_ROW_COUNT,
                (columnIndex, pageIndex) -> {
                    int min = StatisticsDecoder.decodeInt(columnIndex.minValues().get(pageIndex));
                    int max = StatisticsDecoder.decodeInt(columnIndex.maxValues().get(pageIndex));
                    return StatisticsFilterSupport.canDrop(Operator.GT, 0, min, max);
                });

        assertTrue(ranges.overlapsPage(0, 30));
        assertFalse(ranges.overlapsPage(30, 60));
        assertTrue(ranges.overlapsPage(60, 90));
    }

    // Long Filtering Tests

    // 2 pages: [100,199], [200,299] with 50 rows each
    private static final ColumnIndex LONG_COLUMN_INDEX = longColumnIndex(
            new long[]{ 100L, 200L },
            new long[]{ 199L, 299L });
    private static final OffsetIndex TWO_PAGE_OFFSET_INDEX = offsetIndex(50, 50);
    private static final long TWO_PAGE_ROW_COUNT = 100;

    @ParameterizedTest(name = "{0} {1} → pages kept: [{2}, {3}]")
    @MethodSource
    void testLongPageFiltering(Operator op, long value, boolean page0Kept, boolean page1Kept) {
        RowRanges ranges = PageFilterEvaluator.evaluatePages(LONG_COLUMN_INDEX, TWO_PAGE_OFFSET_INDEX, TWO_PAGE_ROW_COUNT,
                (columnIndex, pageIndex) -> {
                    long min = StatisticsDecoder.decodeLong(columnIndex.minValues().get(pageIndex));
                    long max = StatisticsDecoder.decodeLong(columnIndex.maxValues().get(pageIndex));
                    return StatisticsFilterSupport.canDrop(op, value, min, max);
                });

        assertEquals(page0Kept, ranges.overlapsPage(0, 50),  "page 0 (rows 0-50)");
        assertEquals(page1Kept, ranges.overlapsPage(50, 100), "page 1 (rows 50-100)");
    }

    static Stream<Arguments> testLongPageFiltering() {
        return Stream.of(
                Arguments.of(Operator.EQ,     150L, true,  false),
                Arguments.of(Operator.EQ,     250L, false, true),
                Arguments.of(Operator.EQ,     50L,  false, false),
                Arguments.of(Operator.NOT_EQ, 150L, true,  true),
                Arguments.of(Operator.LT,     200L, true,  false),
                Arguments.of(Operator.LT,     100L, false, false),
                Arguments.of(Operator.LT_EQ,  199L, true,  false),
                Arguments.of(Operator.LT_EQ,  299L, true,  true),
                Arguments.of(Operator.GT,     199L, false, true),
                Arguments.of(Operator.GT,     299L, false, false),
                Arguments.of(Operator.GT_EQ,  200L, false, true),
                Arguments.of(Operator.GT_EQ,  100L, true,  true)
        );
    }

    // Float Filtering Tests

    // 2 pages: [1.0,2.0], [3.0,4.0]
    private static final ColumnIndex FLOAT_COLUMN_INDEX = floatColumnIndex(
            new float[]{ 1.0f, 3.0f },
            new float[]{ 2.0f, 4.0f });

    @ParameterizedTest(name = "{0} {1} → pages kept: [{2}, {3}]")
    @MethodSource
    void testFloatPageFiltering(Operator op, float value, boolean page0Kept, boolean page1Kept) {
        RowRanges ranges = PageFilterEvaluator.evaluatePages(FLOAT_COLUMN_INDEX, TWO_PAGE_OFFSET_INDEX, TWO_PAGE_ROW_COUNT,
                (columnIndex, pageIndex) -> {
                    float min = StatisticsDecoder.decodeFloat(columnIndex.minValues().get(pageIndex));
                    float max = StatisticsDecoder.decodeFloat(columnIndex.maxValues().get(pageIndex));
                    return StatisticsFilterSupport.canDropFloat(op, value, min, max);
                });

        assertEquals(page0Kept, ranges.overlapsPage(0, 50),  "page 0 (rows 0-50)");
        assertEquals(page1Kept, ranges.overlapsPage(50, 100), "page 1 (rows 50-100)");
    }

    static Stream<Arguments> testFloatPageFiltering() {
        return Stream.of(
                Arguments.of(Operator.EQ,     1.5f, true,  false),
                Arguments.of(Operator.EQ,     3.5f, false, true),
                Arguments.of(Operator.EQ,     2.5f, false, false),
                Arguments.of(Operator.NOT_EQ, 1.5f, true,  true),
                Arguments.of(Operator.LT,     3.0f, true,  false),
                Arguments.of(Operator.LT,     1.0f, false, false),
                Arguments.of(Operator.LT_EQ,  2.0f, true,  false),
                Arguments.of(Operator.LT_EQ,  4.0f, true,  true),
                Arguments.of(Operator.GT,     2.0f, false, true),
                Arguments.of(Operator.GT,     4.0f, false, false),
                Arguments.of(Operator.GT_EQ,  3.0f, false, true),
                Arguments.of(Operator.GT_EQ,  1.0f, true,  true)
        );
    }

    // Double Filtering Tests

    // 2 pages: [10.0,20.0], [30.0,40.0]
    private static final ColumnIndex DOUBLE_COLUMN_INDEX = doubleColumnIndex(
            new double[]{ 10.0, 30.0 },
            new double[]{ 20.0, 40.0 });

    @ParameterizedTest(name = "{0} {1} → pages kept: [{2}, {3}]")
    @MethodSource
    void testDoublePageFiltering(Operator op, double value, boolean page0Kept, boolean page1Kept) {
        RowRanges ranges = PageFilterEvaluator.evaluatePages(DOUBLE_COLUMN_INDEX, TWO_PAGE_OFFSET_INDEX, TWO_PAGE_ROW_COUNT,
                (columnIndex, pageIndex) -> {
                    double min = StatisticsDecoder.decodeDouble(columnIndex.minValues().get(pageIndex));
                    double max = StatisticsDecoder.decodeDouble(columnIndex.maxValues().get(pageIndex));
                    return StatisticsFilterSupport.canDropDouble(op, value, min, max);
                });

        assertEquals(page0Kept, ranges.overlapsPage(0, 50),  "page 0 (rows 0-50)");
        assertEquals(page1Kept, ranges.overlapsPage(50, 100), "page 1 (rows 50-100)");
    }

    static Stream<Arguments> testDoublePageFiltering() {
        return Stream.of(
                Arguments.of(Operator.EQ,     15.0, true,  false),
                Arguments.of(Operator.EQ,     35.0, false, true),
                Arguments.of(Operator.EQ,     25.0, false, false),
                Arguments.of(Operator.NOT_EQ, 15.0, true,  true),
                Arguments.of(Operator.LT,     30.0, true,  false),
                Arguments.of(Operator.LT,     10.0, false, false),
                Arguments.of(Operator.LT_EQ,  20.0, true,  false),
                Arguments.of(Operator.LT_EQ,  40.0, true,  true),
                Arguments.of(Operator.GT,     20.0, false, true),
                Arguments.of(Operator.GT,     40.0, false, false),
                Arguments.of(Operator.GT_EQ,  30.0, false, true),
                Arguments.of(Operator.GT_EQ,  10.0, true,  true)
        );
    }

    // Boolean Filtering Tests

    // 2 pages: [false,false], [false,true]
    private static final ColumnIndex BOOLEAN_COLUMN_INDEX = booleanColumnIndex(
            new boolean[]{ false, false },
            new boolean[]{ false, true });

    @ParameterizedTest(name = "{0} {1} → pages kept: [{2}, {3}]")
    @MethodSource
    void testBooleanPageFiltering(Operator op, boolean value, boolean page0Kept, boolean page1Kept) {
        RowRanges ranges = PageFilterEvaluator.evaluatePages(BOOLEAN_COLUMN_INDEX, TWO_PAGE_OFFSET_INDEX, TWO_PAGE_ROW_COUNT,
                (columnIndex, pageIndex) -> {
                    int min = StatisticsDecoder.decodeBoolean(columnIndex.minValues().get(pageIndex)) ? 1 : 0;
                    int max = StatisticsDecoder.decodeBoolean(columnIndex.maxValues().get(pageIndex)) ? 1 : 0;
                    int val = value ? 1 : 0;
                    return StatisticsFilterSupport.canDrop(op, val, min, max);
                });

        assertEquals(page0Kept, ranges.overlapsPage(0, 50),  "page 0 (rows 0-50)");
        assertEquals(page1Kept, ranges.overlapsPage(50, 100), "page 1 (rows 50-100)");
    }

    static Stream<Arguments> testBooleanPageFiltering() {
        return Stream.of(
                // Page 0: [false,false], Page 1: [false,true]
                Arguments.of(Operator.EQ,     false, true,  true),   // false is in both ranges
                Arguments.of(Operator.EQ,     true,  false, true),   // true only in page 1
                Arguments.of(Operator.NOT_EQ, false, false, true),   // drop page 0 (min==max==false)
                Arguments.of(Operator.NOT_EQ, true,  true,  true)    // neither page has min==max==true
        );
    }

    // Binary Filtering Tests

    // 2 pages: [apple,banana], [cherry,date]
    private static final ColumnIndex BINARY_COLUMN_INDEX = binaryColumnIndex(
            List.of("apple".getBytes(StandardCharsets.UTF_8), "cherry".getBytes(StandardCharsets.UTF_8)),
            List.of("banana".getBytes(StandardCharsets.UTF_8), "date".getBytes(StandardCharsets.UTF_8)));

    @ParameterizedTest(name = "{0} \"{1}\" → pages kept: [{2}, {3}]")
    @MethodSource
    void testBinaryPageFiltering(Operator op, String value, boolean page0Kept, boolean page1Kept) {
        byte[] searchValue = value.getBytes(StandardCharsets.UTF_8);
        RowRanges ranges = PageFilterEvaluator.evaluatePages(BINARY_COLUMN_INDEX, TWO_PAGE_OFFSET_INDEX, TWO_PAGE_ROW_COUNT,
                (columnIndex, pageIndex) -> {
                    byte[] min = columnIndex.minValues().get(pageIndex);
                    byte[] max = columnIndex.maxValues().get(pageIndex);
                    int cmpMin = BinaryComparator.compareUnsigned(searchValue, min);
                    int cmpMax = BinaryComparator.compareUnsigned(searchValue, max);
                    return StatisticsFilterSupport.canDropCompared(op,
                            cmpMin, cmpMax, BinaryComparator.compareUnsigned(min, max));
                });

        assertEquals(page0Kept, ranges.overlapsPage(0, 50),  "page 0 (rows 0-50)");
        assertEquals(page1Kept, ranges.overlapsPage(50, 100), "page 1 (rows 50-100)");
    }

    static Stream<Arguments> testBinaryPageFiltering() {
        return Stream.of(
                Arguments.of(Operator.EQ,     "avocado", true,  false),
                Arguments.of(Operator.EQ,     "corn",    false, true),
                Arguments.of(Operator.EQ,     "cat",     false, false),
                Arguments.of(Operator.NOT_EQ, "avocado", true,  true),
                Arguments.of(Operator.LT,     "cherry",  true,  false),
                Arguments.of(Operator.LT,     "apple",   false, false),
                Arguments.of(Operator.LT_EQ,  "banana",  true,  false),
                Arguments.of(Operator.LT_EQ,  "date",    true,  true),
                Arguments.of(Operator.GT,     "banana",  false, true),
                Arguments.of(Operator.GT,     "date",    false, false),
                Arguments.of(Operator.GT_EQ,  "cherry",  false, true),
                Arguments.of(Operator.GT_EQ,  "apple",   true,  true)
        );
    }

    // Compound Predicate Tests
    //
    // Uses column_index_pushdown.parquet: 1 row group, 10000 rows,
    // sorted id [0,9999] and value [1000,10999], Parquet v2 with Column Index,
    // ~10 pages of 1024 values each.

    private static final Path COLUMN_INDEX_FILE = Path.of("src/test/resources/column_index_pushdown.parquet");

    @Test
    void testAndNarrowsMatchingRows() throws IOException {
        // AND(id >= 2000, id < 4000) → only pages covering rows 2000-3999 should match,
        // which is fewer pages than either predicate alone
        RowRanges gtEq2000 = computeMatchingRows(FilterPredicate.gtEq("id", 2000L));
        RowRanges lt4000 = computeMatchingRows(FilterPredicate.lt("id", 4000L));
        RowRanges andResult = computeMatchingRows(FilterPredicate.and(
                FilterPredicate.gtEq("id", 2000L),
                FilterPredicate.lt("id", 4000L)));

        assertFalse(andResult.isAll());
        assertTrue(andResult.intervalCount() <= gtEq2000.intervalCount());
        assertTrue(andResult.intervalCount() <= lt4000.intervalCount());
    }

    @Test
    void testOrWidensMatchingRows() throws IOException {
        // OR(id < 1000, id >= 9000) → union should cover both ends
        RowRanges lt1000 = computeMatchingRows(FilterPredicate.lt("id", 1000L));
        RowRanges gtEq9000 = computeMatchingRows(FilterPredicate.gtEq("id", 9000L));
        RowRanges orResult = computeMatchingRows(FilterPredicate.or(
                FilterPredicate.lt("id", 1000L),
                FilterPredicate.gtEq("id", 9000L)));

        assertFalse(orResult.isAll());
        assertTrue(orResult.intervalCount() >= lt1000.intervalCount());
        assertTrue(orResult.intervalCount() >= gtEq9000.intervalCount());
        // Start and end of the row group should overlap
        assertTrue(orResult.overlapsPage(0, 1));
        assertTrue(orResult.overlapsPage(9999, 10000));
    }

    @Test
    void testNotInvertsLeafPredicate() throws IOException {
        // NOT(id < 5000) → inverted to id >= 5000, should skip early pages
        RowRanges gtEq5000 = computeMatchingRows(FilterPredicate.gtEq("id", 5000L));
        RowRanges notLt5000 = computeMatchingRows(FilterPredicate.not(FilterPredicate.lt("id", 5000L)));

        // NOT(LT) should produce the same result as GT_EQ with the same value
        assertFalse(notLt5000.isAll());
        assertEquals(gtEq5000.intervalCount(), notLt5000.intervalCount());
    }

    @Test
    void testAndWithDisjointPredicatesNarrows() throws IOException {
        // AND(id < 1000, id >= 9000) → with 10 pages of 1024 rows each,
        // id < 1000 keeps only page 0, id >= 9000 keeps only the last page.
        // Their row ranges don't overlap, so intersection is empty.
        RowRanges andResult = computeMatchingRows(FilterPredicate.and(
                FilterPredicate.lt("id", 1000L),
                FilterPredicate.gtEq("id", 9000L)));

        assertEquals(0, andResult.intervalCount());
    }

    @Test
    void testOrMatchingAllPages() throws IOException {
        // OR(id >= 0, id < 10000) → both sides match everything
        FilterPredicate filter = FilterPredicate.or(
                FilterPredicate.gtEq("id", 0L),
                FilterPredicate.lt("id", 10000L));

        RowRanges ranges = computeMatchingRows(filter);

        assertTrue(ranges.overlapsPage(0, 10000));
    }

    @Test
    void testAndWithUnknownColumnThrowsAtResolve() throws IOException {
        // AND(id < 5000, nonexistent > 0) → unknown column now throws at resolve time
        InputFile inputFile = InputFile.of(COLUMN_INDEX_FILE);
        inputFile.open();
        try {
            FileMetaData metaData = ParquetMetadataReader.readMetadata(inputFile);
            FileSchema schema = FileSchema.fromSchemaElements(metaData.schema());
            FilterPredicate filter = FilterPredicate.and(
                    FilterPredicate.lt("id", 5000L),
                    FilterPredicate.gt("nonexistent", 0L));
            org.assertj.core.api.Assertions.assertThatThrownBy(
                    () -> FilterPredicateResolver.resolve(filter, schema))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("not found");
        }
        finally {
            inputFile.close();
        }
    }

    private RowRanges computeMatchingRows(FilterPredicate filter) throws IOException {
        InputFile inputFile = InputFile.of(COLUMN_INDEX_FILE);
        inputFile.open();
        try {
            FileMetaData metaData = ParquetMetadataReader.readMetadata(inputFile);
            FileSchema schema = FileSchema.fromSchemaElements(metaData.schema());
            ResolvedPredicate resolved = FilterPredicateResolver.resolve(filter, schema);
            RowGroup rowGroup = metaData.rowGroups().get(0);
            RowGroupIndexBuffers indexBuffers = RowGroupIndexBuffers.fetch(inputFile, rowGroup);
            return PageFilterEvaluator.computeMatchingRows(resolved, rowGroup, indexBuffers);
        }
        finally {
            inputFile.close();
        }
    }

    // Helpers

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

    private static ColumnIndex intColumnIndex(int[] mins, int[] maxs) {
        List<byte[]> minValues = new ArrayList<>();
        List<byte[]> maxValues = new ArrayList<>();
        List<Boolean> nullPages = new ArrayList<>();
        for (int i = 0; i < mins.length; i++) {
            minValues.add(intBytes(mins[i]));
            maxValues.add(intBytes(maxs[i]));
            nullPages.add(false);
        }
        return new ColumnIndex(nullPages, minValues, maxValues,
                ColumnIndex.BoundaryOrder.UNORDERED, null, null);
    }

    private static ColumnIndex longColumnIndex(long[] mins, long[] maxs) {
        List<byte[]> minValues = new ArrayList<>();
        List<byte[]> maxValues = new ArrayList<>();
        List<Boolean> nullPages = new ArrayList<>();
        for (int i = 0; i < mins.length; i++) {
            minValues.add(longBytes(mins[i]));
            maxValues.add(longBytes(maxs[i]));
            nullPages.add(false);
        }
        return new ColumnIndex(nullPages, minValues, maxValues,
                ColumnIndex.BoundaryOrder.UNORDERED, null, null);
    }

    @Test
    void testIntInPageFiltering() {
        RowRanges ranges = PageFilterEvaluator.evaluatePages(INT_COLUMN_INDEX, THREE_PAGE_OFFSET_INDEX, THREE_PAGE_ROW_COUNT,
                (ci, i) -> {
                    int min = StatisticsDecoder.decodeInt(ci.minValues().get(i));
                    int max = StatisticsDecoder.decodeInt(ci.maxValues().get(i));
                    return StatisticsFilterSupport.canDropIntIn(new int[]{ 5, 15 }, min, max);
                });
        assertTrue(ranges.overlapsPage(0, 30));
        assertTrue(ranges.overlapsPage(30, 60));
        assertFalse(ranges.overlapsPage(60, 90));
    }

    @Test
    void testIntInPageFilteringAllOutside() {
        RowRanges ranges = PageFilterEvaluator.evaluatePages(INT_COLUMN_INDEX, THREE_PAGE_OFFSET_INDEX, THREE_PAGE_ROW_COUNT,
                (ci, i) -> {
                    int min = StatisticsDecoder.decodeInt(ci.minValues().get(i));
                    int max = StatisticsDecoder.decodeInt(ci.maxValues().get(i));
                    return StatisticsFilterSupport.canDropIntIn(new int[]{ 50, 60 }, min, max);
                });
        assertFalse(ranges.overlapsPage(0, 30));
        assertFalse(ranges.overlapsPage(30, 60));
        assertFalse(ranges.overlapsPage(60, 90));
    }

    @Test
    void testLongInPageFiltering() {
        ColumnIndex longIdx = longColumnIndex(new long[]{ 100, 200, 300 }, new long[]{ 199, 299, 399 });
        OffsetIndex oi = offsetIndex(30, 30, 30);

        RowRanges ranges = PageFilterEvaluator.evaluatePages(longIdx, oi, 90,
                (ci, i) -> {
                    long min = StatisticsDecoder.decodeLong(ci.minValues().get(i));
                    long max = StatisticsDecoder.decodeLong(ci.maxValues().get(i));
                    return StatisticsFilterSupport.canDropLongIn(new long[]{ 150, 350 }, min, max);
                });
        assertTrue(ranges.overlapsPage(0, 30));
        assertFalse(ranges.overlapsPage(30, 60));
        assertTrue(ranges.overlapsPage(60, 90));
    }

    @Test
    void testBinaryInPageFiltering() {
        ColumnIndex binIdx = binaryColumnIndex(
                List.of("apple".getBytes(StandardCharsets.UTF_8), "date".getBytes(StandardCharsets.UTF_8)),
                List.of("cherry".getBytes(StandardCharsets.UTF_8), "fig".getBytes(StandardCharsets.UTF_8)));
        OffsetIndex oi = offsetIndex(30, 30);

        RowRanges ranges = PageFilterEvaluator.evaluatePages(binIdx, oi, 60,
                (ci, i) -> {
                    byte[] min = ci.minValues().get(i);
                    byte[] max = ci.maxValues().get(i);
                    return StatisticsFilterSupport.canDropBinaryIn(
                            new byte[][]{ "banana".getBytes(StandardCharsets.UTF_8), "zebra".getBytes(StandardCharsets.UTF_8) },
                            min, max);
                });
        assertTrue(ranges.overlapsPage(0, 30));
        assertFalse(ranges.overlapsPage(30, 60));
    }

    private static ColumnIndex floatColumnIndex(float[] mins, float[] maxs) {
        List<byte[]> minValues = new ArrayList<>();
        List<byte[]> maxValues = new ArrayList<>();
        List<Boolean> nullPages = new ArrayList<>();
        for (int i = 0; i < mins.length; i++) {
            minValues.add(floatBytes(mins[i]));
            maxValues.add(floatBytes(maxs[i]));
            nullPages.add(false);
        }
        return new ColumnIndex(nullPages, minValues, maxValues,
                ColumnIndex.BoundaryOrder.UNORDERED, null, null);
    }

    private static ColumnIndex doubleColumnIndex(double[] mins, double[] maxs) {
        List<byte[]> minValues = new ArrayList<>();
        List<byte[]> maxValues = new ArrayList<>();
        List<Boolean> nullPages = new ArrayList<>();
        for (int i = 0; i < mins.length; i++) {
            minValues.add(doubleBytes(mins[i]));
            maxValues.add(doubleBytes(maxs[i]));
            nullPages.add(false);
        }
        return new ColumnIndex(nullPages, minValues, maxValues,
                ColumnIndex.BoundaryOrder.UNORDERED, null, null);
    }

    private static ColumnIndex booleanColumnIndex(boolean[] mins, boolean[] maxs) {
        List<byte[]> minValues = new ArrayList<>();
        List<byte[]> maxValues = new ArrayList<>();
        List<Boolean> nullPages = new ArrayList<>();
        for (int i = 0; i < mins.length; i++) {
            minValues.add(new byte[]{ (byte) (mins[i] ? 1 : 0) });
            maxValues.add(new byte[]{ (byte) (maxs[i] ? 1 : 0) });
            nullPages.add(false);
        }
        return new ColumnIndex(nullPages, minValues, maxValues,
                ColumnIndex.BoundaryOrder.UNORDERED, null, null);
    }

    private static ColumnIndex binaryColumnIndex(List<byte[]> mins, List<byte[]> maxs) {
        List<Boolean> nullPages = new ArrayList<>();
        for (int i = 0; i < mins.size(); i++) {
            nullPages.add(false);
        }
        return new ColumnIndex(nullPages, mins, maxs,
                ColumnIndex.BoundaryOrder.UNORDERED, null, null);
    }

    // Geospatial Filtering Tests

    @Test
    void testSpatialPagesDropsDisjointPagesAndKeepsIntersecting() {
        // 3 pages, each with a distinct bbox (Europe, Asia, Americas)
        ColumnIndex columnIdx = geospatialColumnIndex(
                bbox(-25.0, 45.0, 35.0, 72.0),     // Europe
                bbox(60.0, 150.0, -10.0, 50.0),    // Asia
                bbox(-130.0, -60.0, 25.0, 50.0));  // Americas
        OffsetIndex offsetIdx = offsetIndex(30, 30, 30);

        // Query box matching Europe
        ResolvedPredicate.GeospatialPredicate europe = new ResolvedPredicate.GeospatialPredicate(
                0, -10.0, 40.0, 30.0, 60.0);
        RowRanges ranges = PageFilterEvaluator.evaluateSpatialPages(columnIdx, offsetIdx, europe, 90);

        assertTrue(ranges.overlapsPage(0, 30));
        assertFalse(ranges.overlapsPage(30, 60));
        assertFalse(ranges.overlapsPage(60, 90));
    }

    @Test
    void testSpatialPagesKeepsAllWhenNoGeospatialStats() {
        // ColumnIndex with no geospatial stats — everything must be kept.
        ColumnIndex columnIdx = new ColumnIndex(
                List.of(false, false, false),
                List.of(intBytes(0), intBytes(0), intBytes(0)),
                List.of(intBytes(0), intBytes(0), intBytes(0)),
                ColumnIndex.BoundaryOrder.UNORDERED, null, null);
        OffsetIndex offsetIdx = offsetIndex(30, 30, 30);
        ResolvedPredicate.GeospatialPredicate predicate = new ResolvedPredicate.GeospatialPredicate(
                0, -10.0, 40.0, 30.0, 60.0);

        RowRanges ranges = PageFilterEvaluator.evaluateSpatialPages(columnIdx, offsetIdx, predicate, 90);

        assertTrue(ranges.overlapsPage(0, 30));
        assertTrue(ranges.overlapsPage(30, 60));
        assertTrue(ranges.overlapsPage(60, 90));
    }

    @Test
    void testSpatialPagesKeepsPageWithMissingPerPageBbox() {
        // Middle page's stats slot is null; should be kept.
        List<GeospatialStatistics> stats = new ArrayList<>();
        stats.add(new GeospatialStatistics(bbox(-25.0, 45.0, 35.0, 72.0), List.of()));
        stats.add(null);
        stats.add(new GeospatialStatistics(bbox(-130.0, -60.0, 25.0, 50.0), List.of()));
        ColumnIndex columnIdx = geospatialColumnIndexFromStats(stats);
        OffsetIndex offsetIdx = offsetIndex(30, 30, 30);
        ResolvedPredicate.GeospatialPredicate europe = new ResolvedPredicate.GeospatialPredicate(
                0, -10.0, 40.0, 30.0, 60.0);

        RowRanges ranges = PageFilterEvaluator.evaluateSpatialPages(columnIdx, offsetIdx, europe, 90);

        assertTrue(ranges.overlapsPage(0, 30));
        assertTrue(ranges.overlapsPage(30, 60));   // unknown stats: keep
        assertFalse(ranges.overlapsPage(60, 90));  // disjoint: drop
    }

    @Test
    void testSpatialPagesAntimeridianWrappingPageIsKept() {
        // Wrapping chunk bbox (xmin > xmax) overlapping a non-wrapping query bbox.
        ColumnIndex columnIdx = geospatialColumnIndex(bbox(170.0, -170.0, -10.0, 10.0));
        OffsetIndex offsetIdx = offsetIndex(50);
        ResolvedPredicate.GeospatialPredicate touchingDateLine = new ResolvedPredicate.GeospatialPredicate(
                0, 160.0, -5.0, 180.0, 5.0);

        RowRanges ranges = PageFilterEvaluator.evaluateSpatialPages(
                columnIdx, offsetIdx, touchingDateLine, 50);

        assertTrue(ranges.overlapsPage(0, 50));
    }

    private static BoundingBox bbox(double xmin, double xmax, double ymin, double ymax) {
        return new BoundingBox(xmin, xmax, ymin, ymax, null, null, null, null);
    }

    private static ColumnIndex geospatialColumnIndex(BoundingBox... pageBboxes) {
        List<GeospatialStatistics> stats = new ArrayList<>();
        for (BoundingBox bbox : pageBboxes) {
            stats.add(new GeospatialStatistics(bbox, List.of()));
        }
        return geospatialColumnIndexFromStats(stats);
    }

    private static ColumnIndex geospatialColumnIndexFromStats(List<GeospatialStatistics> stats) {
        List<Boolean> nullPages = new ArrayList<>();
        List<byte[]> minValues = new ArrayList<>();
        List<byte[]> maxValues = new ArrayList<>();
        for (int i = 0; i < stats.size(); i++) {
            nullPages.add(false);
            minValues.add(new byte[0]);
            maxValues.add(new byte[0]);
        }
        return new ColumnIndex(nullPages, minValues, maxValues,
                ColumnIndex.BoundaryOrder.UNORDERED, null, stats);
    }

    /// Creates an OffsetIndex with the given number of rows per page.
    /// File offsets and compressed sizes are synthetic — only `firstRowIndex` matters for filtering.
    private static OffsetIndex offsetIndex(int... rowsPerPage) {
        List<PageLocation> pages = new ArrayList<>();
        long currentRow = 0;
        long currentOffset = 0;
        for (int rows : rowsPerPage) {
            pages.add(new PageLocation(currentOffset, 100, currentRow));
            currentRow += rows;
            currentOffset += 100;
        }
        return new OffsetIndex(pages);
    }
}
