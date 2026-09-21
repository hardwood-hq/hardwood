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
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.bloomfilter.BloomFilter;
import dev.hardwood.internal.bloomfilter.BloomFilterHeader;
import dev.hardwood.internal.predicate.dictionary.DictionaryFilterSupport;
import dev.hardwood.internal.predicate.dictionary.RowGroupDictionaryFilterSource;
import dev.hardwood.internal.reader.Dictionary;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Dictionary-based row-group pruning, one nested class per single-row-group fixture.
///
/// The discriminating cases probe values that fall *inside* the column's statistics min/max range,
/// so statistics alone keep the row group, but were never written, so only the dictionary can prove
/// their absence.
///
/// Asserts the evaluator decision directly; [dev.hardwood.DictionaryEndToEndTest] drives the string
/// fixture through the public reader APIs.
class DictionaryPushDownTest {

    /// A position with nothing to point at: these cases assert decisions, not diagnostics.
    private static final LogContext UNNAMED =
            new LogContext(null, ExceptionContext.UNKNOWN_ROW_GROUP);

    /// A bloom filter whose bitset is all zeroes, so every probe misses.
    private static BloomFilter emptyBloomFilter() {
        return new BloomFilter(
                new BloomFilterHeader(BLOOM_FILTER_BYTES, BloomFilterHeader.Algorithm.BLOCK,
                        BloomFilterHeader.Hash.XXHASH, BloomFilterHeader.Compression.UNCOMPRESSED),
                ByteBuffer.allocate(BLOOM_FILTER_BYTES).order(ByteOrder.LITTLE_ENDIAN).asReadOnlyBuffer());
    }

    private static final int BLOOM_FILTER_BYTES = 32;

    /// Opens the first row group of one fixture under `src/test/resources` for the lifetime of a
    /// nested class, and decides predicates against it.
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    abstract static class OnFixture {

        private final String fixture;

        private ParquetFileReader reader;
        private InputFile inputFile;
        private RowGroup rowGroup;
        FileSchema schema;
        private HardwoodContextImpl context;

        OnFixture(String fixture) {
            this.fixture = fixture;
        }

        @BeforeAll
        void open() throws Exception {
            inputFile = InputFile.of(Paths.get("src/test/resources", fixture));
            reader = ParquetFileReader.open(inputFile);
            rowGroup = reader.getFileMetaData().rowGroups().getFirst();
            schema = FileSchema.fromSchemaElements(reader.getFileMetaData().schema());
            context = HardwoodContextImpl.create();
        }

        @AfterAll
        void close() throws Exception {
            reader.close();
            context.close();
        }

        Dictionary dict(int columnIndex) throws IOException {
            return dictionaries().forColumn(columnIndex);
        }

        RowGroupDictionaryFilterSource dictionaries() {
            return new RowGroupDictionaryFilterSource(inputFile, rowGroup, schema, context);
        }

        RowGroup rowGroup() {
            return rowGroup;
        }

        boolean dictionaryDrop(FilterPredicate filter) throws IOException {
            ResolvedPredicate resolved = FilterPredicateResolver.resolve(filter, schema);
            return RowGroupFilterEvaluator.decideRowGroup(resolved, rowGroup, null, dictionaries(), UNNAMED, BoundsReadability.ALL)
                    == FilterDecision.CANNOT_MATCH;
        }

        boolean statisticsDrop(FilterPredicate filter) throws IOException {
            ResolvedPredicate resolved = FilterPredicateResolver.resolve(filter, schema);
            return RowGroupFilterEvaluator.decideRowGroup(resolved, rowGroup, null, null, UNNAMED, BoundsReadability.ALL) == FilterDecision.CANNOT_MATCH;
        }
    }

    /// `column_index_pushdown_dict.parquet` (one row group, 10 000 rows; `id` INT64 `0..9999`,
    /// `category` STRING cycling `"cat_0".."cat_9"`): the `BYTE_ARRAY` arms.
    @Nested
    class StringColumn extends OnFixture {

        private static final int ID_COLUMN = 0;
        private static final int CATEGORY_COLUMN = 1;

        StringColumn() {
            super("column_index_pushdown_dict.parquet");
        }

        @Test
        void fixtureIsDictionaryEncoded() throws IOException {
            // A readable dictionary is exactly what the push-down precondition amounts to: the chunk is
            // fully dictionary-encoded and its page is locatable.
            assertThat(dictionaries().forColumn(CATEGORY_COLUMN)).isNotNull();
        }

        @Test
        void absentValueInsideStatisticsRangeIsDropped() throws IOException {
            // "cat_5x" sorts between the min "cat_0" and max "cat_9", so statistics cannot drop it.
            FilterPredicate absent = FilterPredicate.eq("category", "cat_5x");

            assertThat(statisticsDrop(absent)).isFalse();
            assertThat(dictionaryDrop(absent)).isTrue();
        }

        @Test
        void presentValueIsKept() throws IOException {
            assertThat(dictionaryDrop(FilterPredicate.eq("category", "cat_5"))).isFalse();
        }

        @Test
        void orOfABloomProvenAndADictionaryProvenBranchIsDroppedWithoutReadingTheBloomFilterAgain()
                throws IOException {
            // `id` 42 is present, so neither statistics nor a dictionary drop its branch; the empty
            // bloom filter does. "cat_5x" only the dictionary drops. Planning sees one branch
            // dropped; refining has to replay that answer to drop both.
            ResolvedPredicate or = FilterPredicateResolver.resolve(FilterPredicate.or(
                    FilterPredicate.eq("id", 42L), FilterPredicate.eq("category", "cat_5x")), schema);
            AtomicInteger bloomReads = new AtomicInteger();
            BloomFilterSource bloomFilters = columnIndex -> {
                bloomReads.incrementAndGet();
                return columnIndex == ID_COLUMN ? emptyBloomFilter() : null;
            };
            RowGroupFilterEvaluator.LeafDecisions leafDecisions = new RowGroupFilterEvaluator.LeafDecisions();

            assertThat(RowGroupFilterEvaluator.planRowGroup(or, rowGroup(), bloomFilters, UNNAMED,
                    BoundsReadability.ALL, leafDecisions)).isEqualTo(FilterDecision.MIGHT_MATCH);
            int bloomReadsWhilePlanning = bloomReads.get();

            assertThat(RowGroupFilterEvaluator.refineWithDictionaries(or, rowGroup(), leafDecisions, dictionaries()))
                    .isEqualTo(FilterDecision.CANNOT_MATCH);
            assertThat(bloomReads.get()).isEqualTo(bloomReadsWhilePlanning);
        }

        @Test
        void inListDropsOnlyWhenAllValuesAreAbsent() throws IOException {
            assertThat(dictionaryDrop(FilterPredicate.in("category", "nope_a", "nope_b"))).isTrue();
            // One present value is enough to keep it.
            assertThat(dictionaryDrop(FilterPredicate.in("category", "nope_a", "cat_3"))).isFalse();
        }

        @Test
        void pruningAppliesInsideBooleanCombinators() throws IOException {
            assertThat(dictionaryDrop(FilterPredicate.or(
                    FilterPredicate.eq("category", "cat_5x"),
                    FilterPredicate.eq("category", "cat_7x")))).isTrue();

            assertThat(dictionaryDrop(FilterPredicate.and(
                    FilterPredicate.eq("category", "cat_5x"),
                    FilterPredicate.gtEq("id", 0L)))).isTrue();

            assertThat(dictionaryDrop(FilterPredicate.or(
                    FilterPredicate.eq("category", "cat_5x"),
                    FilterPredicate.eq("category", "cat_2")))).isFalse();
        }

        @Test
        void outOfBoundsColumnIndexYieldsNoDictionary() throws IOException {
            RowGroupDictionaryFilterSource source = dictionaries();
            assertThat(source.forColumn(-1)).isNull();
        }

        @Test
        void dictionaryIsCachedPerColumn() throws IOException {
            RowGroupDictionaryFilterSource source = dictionaries();
            assertThat(source.forColumn(CATEGORY_COLUMN))
                    .isNotNull()
                    .isSameAs(source.forColumn(CATEGORY_COLUMN));
        }

        @Test
        void byteArrayValueAbsentAndAbsentAllDirectTests() throws IOException {
            byte[] cat5 = "cat_5".getBytes(StandardCharsets.UTF_8);
            byte[] cat0 = "cat_0".getBytes(StandardCharsets.UTF_8);
            byte[] nope = "nope".getBytes(StandardCharsets.UTF_8);

            // valueAbsent
            assertThat(DictionaryFilterSupport.valueAbsent(dict(CATEGORY_COLUMN), cat5)).isFalse();
            assertThat(DictionaryFilterSupport.valueAbsent(dict(CATEGORY_COLUMN), nope)).isTrue();
            assertThat(DictionaryFilterSupport.valueAbsent(null, cat5)).isFalse();
            assertThat(DictionaryFilterSupport.valueAbsent(dict(999), cat5)).isFalse();

            // absentAll: probes given out of order still match, since the probe list is sorted first
            assertThat(DictionaryFilterSupport.absentAll(dict(CATEGORY_COLUMN),
                    new byte[][]{ nope, cat5 })).isFalse();

            // absentAll: a match at the sorted probe list's first index counts as present
            assertThat(DictionaryFilterSupport.absentAll(dict(CATEGORY_COLUMN),
                    new byte[][]{ cat0, nope })).isFalse();

            // absentAll: null / wrong column
            assertThat(DictionaryFilterSupport.absentAll(null, new byte[][]{ cat0 })).isFalse();
            assertThat(DictionaryFilterSupport.absentAll(dict(999), new byte[][]{ cat0 })).isFalse();
        }
    }

    /// `dict_numeric_pushdown.parquet` (one row group, 4096 rows; `i32` `{0,3,6,9}`, `i64`
    /// `{0,1000,2000,3000}`, `f32` / `f64` `{1.5, 2.5, NaN, 4.5}`, all dictionary-encoded): the
    /// `INT32`, `INT64`, `FLOAT` and `DOUBLE` arms of [DictionaryFilterSupport], for both `eq` and
    /// `in`.
    @Nested
    class NumericColumns extends OnFixture {

        private static final int F32_COLUMN = 2;
        private static final int F64_COLUMN = 3;

        NumericColumns() {
            super("dict_numeric_pushdown.parquet");
        }

        @Test
        void absentInt32IsDroppedOnlyByTheDictionary() throws IOException {
            FilterPredicate absent = FilterPredicate.eq("i32", 5);

            assertThat(statisticsDrop(absent)).as("5 lies within [0, 9]").isFalse();
            assertThat(dictionaryDrop(absent)).isTrue();
            assertThat(dictionaryDrop(FilterPredicate.eq("i32", 6))).isFalse();
        }

        @Test
        void absentInt64IsDroppedOnlyByTheDictionary() throws IOException {
            FilterPredicate absent = FilterPredicate.eq("i64", 1500L);

            assertThat(statisticsDrop(absent)).as("1500 lies within [0, 3000]").isFalse();
            assertThat(dictionaryDrop(absent)).isTrue();
            assertThat(dictionaryDrop(FilterPredicate.eq("i64", 2000L))).isFalse();
        }

        @Test
        void absentFloatIsDroppedOnlyByTheDictionary() throws IOException {
            FilterPredicate absent = FilterPredicate.eq("f32", 3.5f);

            assertThat(statisticsDrop(absent)).as("3.5 lies within [1.5, 4.5]").isFalse();
            assertThat(dictionaryDrop(absent)).isTrue();
            assertThat(dictionaryDrop(FilterPredicate.eq("f32", 2.5f))).isFalse();
        }

        @Test
        void absentDoubleIsDroppedOnlyByTheDictionary() throws IOException {
            FilterPredicate absent = FilterPredicate.eq("f64", 3.5);

            assertThat(statisticsDrop(absent)).as("3.5 lies within [1.5, 4.5]").isFalse();
            assertThat(dictionaryDrop(absent)).isTrue();
            assertThat(dictionaryDrop(FilterPredicate.eq("f64", 2.5))).isFalse();
        }

        @Test
        void naNIsReportedPresentByADictionaryHoldingIt() throws IOException {
            // Equality runs through Float.compare / Double.compare, the IEEE 754 total order, which
            // treats all NaNs as equal — unlike `==`, under which no NaN matches anything. Both float
            // columns carry NaN, so a NaN probe must find it.
            //
            // Asserted against the dictionary arm directly rather than through the row-group decision:
            // the format keeps NaN out of min/max, so statistics judge a NaN probe to fall outside
            // [1.5, 4.5] and drop the row group before any dictionary is read.
            assertThat(DictionaryFilterSupport.valueAbsent(dict(F32_COLUMN), 3.5f))
                    .as("an absent value is still absent, so the arm under test is live")
                    .isTrue();

            assertThat(DictionaryFilterSupport.valueAbsent(dict(F32_COLUMN), Float.NaN)).isFalse();
            assertThat(DictionaryFilterSupport.valueAbsent(dict(F64_COLUMN), Double.NaN)).isFalse();
        }

        @Test
        void int32InListDropsOnlyWhenEveryValueIsAbsent() throws IOException {
            assertThat(dictionaryDrop(FilterPredicate.in("i32", 5, 7))).isTrue();
            assertThat(dictionaryDrop(FilterPredicate.in("i32", 5, 6))).isFalse();
        }

        @Test
        void int64InListDropsOnlyWhenEveryValueIsAbsent() throws IOException {
            assertThat(dictionaryDrop(FilterPredicate.in("i64", 1500L, 2500L))).isTrue();
            assertThat(dictionaryDrop(FilterPredicate.in("i64", 1500L, 2000L))).isFalse();
        }

        @Test
        void floatInListDropsOnlyWhenEveryValueIsAbsent() throws IOException {
            assertThat(dictionaryDrop(FilterPredicate.in("f32", 3.0f, 3.5f))).isTrue();
            assertThat(dictionaryDrop(FilterPredicate.in("f32", 3.0f, 2.5f))).isFalse();

            assertThat(DictionaryFilterSupport.absentAll(dict(F32_COLUMN), new float[]{ 3.5f, Float.NaN }))
                    .isFalse();
        }

        @Test
        void doubleInListDropsOnlyWhenEveryValueIsAbsent() throws IOException {
            assertThat(dictionaryDrop(FilterPredicate.in("f64", 3.0, 3.5))).isTrue();
            assertThat(dictionaryDrop(FilterPredicate.in("f64", 3.0, 2.5))).isFalse();

            assertThat(DictionaryFilterSupport.absentAll(dict(F64_COLUMN), new double[]{ 3.5, Double.NaN }))
                    .isFalse();
        }

        @Test
        void int32UnsortedProbesAndFirstIndexMatch() throws IOException {
            assertThat(DictionaryFilterSupport.absentAll(dict(0), new int[]{ 9, 1, 5 })).isFalse();
            assertThat(DictionaryFilterSupport.absentAll(dict(0), new int[]{ 0, 5, 7 })).isFalse();
            assertThat(DictionaryFilterSupport.absentAll(null, new int[]{ 0 })).isFalse();
            assertThat(DictionaryFilterSupport.absentAll(dict(999), new int[]{ 0 })).isFalse();
            assertThat(DictionaryFilterSupport.valueAbsent(null, 0)).isFalse();
            assertThat(DictionaryFilterSupport.valueAbsent(dict(999), 0)).isFalse();
        }

        @Test
        void int64UnsortedProbesAndFirstIndexMatch() throws IOException {
            assertThat(DictionaryFilterSupport.absentAll(dict(1), new long[]{ 3000L, 100L, 500L })).isFalse();
            assertThat(DictionaryFilterSupport.absentAll(dict(1), new long[]{ 0L, 500L, 700L })).isFalse();
            assertThat(DictionaryFilterSupport.absentAll(null, new long[]{ 0L })).isFalse();
            assertThat(DictionaryFilterSupport.absentAll(dict(999), new long[]{ 0L })).isFalse();
            assertThat(DictionaryFilterSupport.valueAbsent(null, 0L)).isFalse();
            assertThat(DictionaryFilterSupport.valueAbsent(dict(999), 0L)).isFalse();
        }

        @Test
        void floatingPointInListNeedsTheDictionaryArmOfItsOwnWidth() throws IOException {
            // Unsorted probes, and a match at the sorted list's first index, as the integer arms above.
            assertThat(DictionaryFilterSupport.absentAll(dict(F64_COLUMN), new double[]{ 4.5, 1.5, 3.5 }))
                    .isFalse();
            assertThat(DictionaryFilterSupport.absentAll(dict(F32_COLUMN), new float[]{ 4.5f, 1.5f, 3.5f }))
                    .isFalse();

            // No dictionary at all, and a column index past this row group: neither proves absence.
            assertThat(DictionaryFilterSupport.absentAll(null, new double[]{ 3.5 })).isFalse();
            assertThat(DictionaryFilterSupport.absentAll(null, new float[]{ 3.5f })).isFalse();
            assertThat(DictionaryFilterSupport.absentAll(dict(999), new double[]{ 3.5 })).isFalse();
            assertThat(DictionaryFilterSupport.absentAll(dict(999), new float[]{ 3.5f })).isFalse();

            // A FLOAT list meeting DOUBLE entries, and the reverse: the arm that does not match the
            // stored width proves nothing, rather than reading the entries at the wrong one.
            assertThat(DictionaryFilterSupport.absentAll(dict(F64_COLUMN), new float[]{ 3.5f })).isFalse();
            assertThat(DictionaryFilterSupport.absentAll(dict(F32_COLUMN), new double[]{ 3.5 })).isFalse();
        }
    }

    /// `dict_float16_pushdown.parquet` (one row group, 4096 rows; `half` cycling 1.0, 2.0, 4.0,
    /// 8.0).
    ///
    /// `FLOAT16` is `FIXED_LEN_BYTE_ARRAY(2)` annotated `Float16Type`, so it reaches the
    /// `ByteArrayDictionary` arm like [FixedLenByteArrayColumn], but resolves to
    /// [ResolvedPredicate.Float16Predicate] rather than a binary predicate and so takes its own path
    /// through [RowGroupFilterEvaluator]. Every probe sits inside the `[1.0, 8.0]` range statistics
    /// advertise.
    @Nested
    class Float16Column extends OnFixture {

        Float16Column() {
            super("dict_float16_pushdown.parquet");
        }

        @Test
        void theFixtureColumnIsAnAnnotatedTwoByteFixedLenByteArray() {
            // Without the Float16 annotation the probes below would resolve to a binary predicate and
            // exercise the FLBA arm instead, leaving the float16 path untested while still passing.
            assertThat(schema.getColumn(0).type()).isEqualTo(PhysicalType.FIXED_LEN_BYTE_ARRAY);
            assertThat(schema.getColumn(0).logicalType()).isInstanceOf(LogicalType.Float16Type.class);
        }

        @Test
        void absentHalfIsDroppedOnlyByTheDictionary() throws IOException {
            FilterPredicate absent = FilterPredicate.eq("half", 3.0f);

            assertThat(statisticsDrop(absent)).as("3.0 lies within [1.0, 8.0]").isFalse();
            assertThat(dictionaryDrop(absent)).isTrue();
            assertThat(dictionaryDrop(FilterPredicate.eq("half", 6.0f))).isTrue();
            assertThat(dictionaryDrop(FilterPredicate.eq("half", 4.0f))).isFalse();
        }

        @Test
        void aSetIsDroppedOnlyWhenEveryProbeIsAbsent() throws IOException {
            assertThat(dictionaryDrop(FilterPredicate.in("half", 6.0f, 3.0f))).isTrue();
            assertThat(dictionaryDrop(FilterPredicate.in("half", 6.0f, 4.0f))).isFalse();
        }

        @Test
        void valueAbsentFloat16FallbacksAndMatches() throws IOException {
            // Present
            assertThat(DictionaryFilterSupport.valueAbsentFloat16(dict(0), 1.0f)).isFalse();
            // Absent
            assertThat(DictionaryFilterSupport.valueAbsentFloat16(dict(0), 3.0f)).isTrue();
            // Null dictionary or wrong column
            assertThat(DictionaryFilterSupport.valueAbsentFloat16(null, 1.0f)).isFalse();
            assertThat(DictionaryFilterSupport.valueAbsentFloat16(dict(999), 1.0f)).isFalse();
        }
    }

    /// `dict_flba_pushdown.parquet` (one row group, 4096 rows; `code` FLBA(4) cycling `aa00`,
    /// `aa03`, `aa06`, `aa09`).
    ///
    /// [StringColumn] reaches the same `ByteArrayDictionary` arm through `BYTE_ARRAY`, but
    /// `FIXED_LEN_BYTE_ARRAY` arrives as a distinct physical type.
    @Nested
    class FixedLenByteArrayColumn extends OnFixture {

        FixedLenByteArrayColumn() {
            super("dict_flba_pushdown.parquet");
        }

        @Test
        void theFixtureColumnIsAFixedLenByteArray() {
            // The physical type is the point of this fixture; a BYTE_ARRAY column here would leave the
            // FLBA arm untested while every assertion below still passed.
            assertThat(schema.getColumn(0).type()).isEqualTo(PhysicalType.FIXED_LEN_BYTE_ARRAY);
        }

        @Test
        void absentCodeIsDroppedOnlyByTheDictionary() throws IOException {
            FilterPredicate absent = FilterPredicate.eq("code", code("aa05"));

            assertThat(statisticsDrop(absent)).as("aa05 sorts within [aa00, aa09]").isFalse();
            assertThat(dictionaryDrop(absent)).isTrue();
            assertThat(dictionaryDrop(FilterPredicate.eq("code", code("aa06")))).isFalse();
        }

        @Test
        void codeInListDropsOnlyWhenEveryValueIsAbsent() throws IOException {
            assertThat(dictionaryDrop(FilterPredicate.in("code", code("aa05"), code("aa07")))).isTrue();
            assertThat(dictionaryDrop(FilterPredicate.in("code", code("aa05"), code("aa06")))).isFalse();
        }

        /// A `code` literal, which the column stores as the four bytes of its ASCII spelling.
        private static byte[] code(String value) {
            return value.getBytes(StandardCharsets.US_ASCII);
        }
    }

    /// Signed-zero handling in `FLOAT` / `DOUBLE` dictionary push-down against
    /// `dict_signed_zero.parquet` (one row group, 4096 rows, values `{-0.0, 1.5, 2.5, 3.5}` in both
    /// the `f` and `d` columns; `+0.0` never occurs).
    ///
    /// A dictionary holds exact stored values, so `Float.compare` / `Double.compare`, the total order
    /// every matcher applies, separates `-0.0` from `+0.0`. A `+0.0` probe is therefore pruned, and
    /// this does not depend on the column's [dev.hardwood.metadata.ColumnOrder]: the `±0` ambiguity the
    /// Parquet spec describes applies to statistics min/max, not to stored values.
    @Nested
    class SignedZero extends OnFixture {

        private static final int FLOAT_COLUMN = 0;
        private static final int DOUBLE_COLUMN = 1;

        SignedZero() {
            super("dict_signed_zero.parquet");
        }

        @Test
        void positiveZeroIsProvenAbsent() throws IOException {
            // The dictionary holds -0.0 and no +0.0. Rows holding -0.0 would not match a +0.0
            // predicate either, so proving +0.0 absent is exact, not merely permissible.
            assertThat(DictionaryFilterSupport.valueAbsent(dict(FLOAT_COLUMN), 0.0f)).isTrue();
            assertThat(DictionaryFilterSupport.valueAbsent(dict(DOUBLE_COLUMN), 0.0)).isTrue();
        }

        @Test
        void negativeZeroIsPresent() throws IOException {
            assertThat(DictionaryFilterSupport.valueAbsent(dict(FLOAT_COLUMN), -0.0f)).isFalse();
            assertThat(DictionaryFilterSupport.valueAbsent(dict(DOUBLE_COLUMN), -0.0)).isFalse();
        }

        @Test
        void signedZeroInAnInListIsDecidedAtEachZeroSeparately() throws IOException {
            // The list path indexes its probes and searches them per entry, where the single-value
            // path scans; both orders separate the zeroes, so a +0.0 list is proven absent too.
            assertThat(absentAll(FLOAT_COLUMN, 0.0f)).isTrue();
            assertThat(absentAll(DOUBLE_COLUMN, 0.0)).isTrue();

            // -0.0 is stored, so a list holding it is kept — including one holding both zeroes,
            // which the probe order keeps distinct rather than collapsing onto one value.
            assertThat(absentAll(FLOAT_COLUMN, -0.0f)).isFalse();
            assertThat(absentAll(DOUBLE_COLUMN, -0.0)).isFalse();
            assertThat(absentAll(FLOAT_COLUMN, 0.0f, -0.0f)).isFalse();
            assertThat(absentAll(DOUBLE_COLUMN, 0.0, -0.0)).isFalse();

            // A list of values the dictionary does not hold, a zero among them, still drops.
            assertThat(absentAll(FLOAT_COLUMN, 0.0f, 4.5f)).isTrue();
            assertThat(absentAll(DOUBLE_COLUMN, 0.0, 4.5)).isTrue();
        }

        @Test
        void nonZeroValuesAreDecidedTheSameWay() throws IOException {
            assertThat(DictionaryFilterSupport.valueAbsent(dict(FLOAT_COLUMN), 2.5f)).isFalse();
            assertThat(DictionaryFilterSupport.valueAbsent(dict(FLOAT_COLUMN), 4.5f)).isTrue();
            assertThat(DictionaryFilterSupport.valueAbsent(dict(DOUBLE_COLUMN), 2.5)).isFalse();
            assertThat(DictionaryFilterSupport.valueAbsent(dict(DOUBLE_COLUMN), 4.5)).isTrue();
        }

        private boolean absentAll(int columnIndex, float... probes) throws IOException {
            return DictionaryFilterSupport.absentAll(dict(columnIndex), probes);
        }

        private boolean absentAll(int columnIndex, double... probes) throws IOException {
            return DictionaryFilterSupport.absentAll(dict(columnIndex), probes);
        }
    }
}
