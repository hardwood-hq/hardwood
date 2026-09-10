/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.predicate.dictionary.DictionaryFilterSupport;
import dev.hardwood.internal.predicate.dictionary.RowGroupDictionaryFilterSource;
import dev.hardwood.internal.reader.Dictionary;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Dictionary push-down on the numeric arms of [DictionaryFilterSupport], against
/// `dict_numeric_pushdown.parquet` (one row group, 4096 rows; `i32` `{0,3,6,9}`, `i64`
/// `{0,1000,2000,3000}`, `f32` / `f64` `{1.5, 2.5, NaN, 4.5}` — all dictionary-encoded).
///
/// [DictionaryPushDownTest] covers the `BYTE_ARRAY` arms with a `STRING` column; this covers
/// `INT32`, `INT64`, `FLOAT` and `DOUBLE` for both `eq` and `in`. Every probe sits *inside* the
/// column's statistics min/max range, so statistics alone keep the row group and only the
/// dictionary can prove absence.
class DictionaryNumericPushDownTest {

    /// A position with nothing to point at: these cases assert decisions, not diagnostics.
    private static final LogContext UNNAMED =
            new LogContext(null, ExceptionContext.UNKNOWN_ROW_GROUP);


    private static final Path FIXTURE = Paths.get("src/test/resources/dict_numeric_pushdown.parquet");

    private static final int F32_COLUMN = 2;
    private static final int F64_COLUMN = 3;

    private static ParquetFileReader reader;
    private static InputFile inputFile;
    private static RowGroup rowGroup;
    private static FileSchema schema;
    private static HardwoodContextImpl context;

    @BeforeAll
    static void open() throws Exception {
        inputFile = InputFile.of(FIXTURE);
        reader = ParquetFileReader.open(inputFile);
        rowGroup = reader.getFileMetaData().rowGroups().getFirst();
        schema = FileSchema.fromSchemaElements(reader.getFileMetaData().schema());
        context = HardwoodContextImpl.create();
    }

    @AfterAll
    static void close() throws Exception {
        reader.close();
        context.close();
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
        assertThat(DictionaryFilterSupport.valueAbsent(dictionaries().forColumn(F32_COLUMN), 3.5f))
                .as("an absent value is still absent, so the arm under test is live")
                .isTrue();

        assertThat(DictionaryFilterSupport.valueAbsent(dictionaries().forColumn(F32_COLUMN), Float.NaN)).isFalse();
        assertThat(DictionaryFilterSupport.valueAbsent(dictionaries().forColumn(F64_COLUMN), Double.NaN)).isFalse();
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
        assertThat(dictionaryDrop(FilterPredicate.in("f32", 3.0, 3.5))).isTrue();
        assertThat(dictionaryDrop(FilterPredicate.in("f32", 3.0, 2.5))).isFalse();
        assertThat(dictionaryDrop(FilterPredicate.in("f32", 3.5, 0.1))).isTrue();

        assertThat(DictionaryFilterSupport.absentAll(dict(F32_COLUMN), new double[]{ 3.5, Double.NaN }, true))
                .isFalse();
    }

    @Test
    void doubleInListDropsOnlyWhenEveryValueIsAbsent() throws IOException {
        assertThat(dictionaryDrop(FilterPredicate.in("f64", 3.0, 3.5))).isTrue();
        assertThat(dictionaryDrop(FilterPredicate.in("f64", 3.0, 2.5))).isFalse();

        assertThat(DictionaryFilterSupport.absentAll(dict(F64_COLUMN), new double[]{ 3.5, Double.NaN }, false))
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
    void doubleInListNeedsTheDictionaryArmOfItsOwnWidth() throws IOException {
        // Unsorted probes, and a match at the sorted list's first index, as the integer arms above.
        assertThat(DictionaryFilterSupport.absentAll(dict(F64_COLUMN), new double[]{ 4.5, 1.5, 3.5 }, false))
                .isFalse();

        // No dictionary at all, and a column index past this row group: neither proves absence.
        assertThat(DictionaryFilterSupport.absentAll(null, new double[]{ 3.5 }, false)).isFalse();
        assertThat(DictionaryFilterSupport.absentAll(null, new double[]{ 3.5 }, true)).isFalse();
        assertThat(DictionaryFilterSupport.absentAll(dict(999), new double[]{ 3.5 }, false)).isFalse();
        assertThat(DictionaryFilterSupport.absentAll(dict(999), new double[]{ 3.5 }, true)).isFalse();

        // A FLOAT-column list meeting DOUBLE entries, and the reverse: the arm that does not match
        // the stored width proves nothing, rather than reading the entries at the wrong one.
        assertThat(DictionaryFilterSupport.absentAll(dict(F64_COLUMN), new double[]{ 3.5 }, true)).isFalse();
        assertThat(DictionaryFilterSupport.absentAll(dict(F32_COLUMN), new double[]{ 3.5 }, false)).isFalse();
    }

    private static Dictionary dict(int columnIndex) throws IOException {
        return dictionaries().forColumn(columnIndex);
    }

    private static RowGroupDictionaryFilterSource dictionaries() {
        return new RowGroupDictionaryFilterSource(inputFile, rowGroup, schema, context);
    }

    private static boolean dictionaryDrop(FilterPredicate filter) throws IOException {
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(filter, schema);
        return RowGroupFilterEvaluator.decideRowGroup(resolved, rowGroup, null, dictionaries(), UNNAMED, BoundsReadability.ALL)
                == FilterDecision.CANNOT_MATCH;
    }

    private static boolean statisticsDrop(FilterPredicate filter) throws IOException {
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(filter, schema);
        return RowGroupFilterEvaluator.decideRowGroup(resolved, rowGroup, null, null, UNNAMED, BoundsReadability.ALL) == FilterDecision.CANNOT_MATCH;
    }
}
