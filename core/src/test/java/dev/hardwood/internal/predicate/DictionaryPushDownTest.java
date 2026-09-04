/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.predicate.dictionary.DictionaryFilterSupport;
import dev.hardwood.internal.predicate.dictionary.RowGroupDictionaryFilterSource;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Dictionary-based row-group pruning against `column_index_pushdown_dict.parquet` (one row group,
/// 10 000 rows; `id` INT64 `0..9999`, `category` STRING cycling `"cat_0".."cat_9"`).
///
/// The discriminating cases probe values that fall *inside* the column's statistics min/max range —
/// so statistics alone keep the row group — but were never written, so only the dictionary can prove
/// their absence.
///
/// Asserts the evaluator decision directly; the dictionary end-to-end suite drives the same
/// fixture through the public reader APIs.
class DictionaryPushDownTest {

    private static final Path FIXTURE = Paths.get("src/test/resources/column_index_pushdown_dict.parquet");

    private static final int CATEGORY_COLUMN = 1;

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
    void fixtureIsDictionaryEncoded() {
        // A readable dictionary is exactly what the push-down precondition amounts to: the chunk is
        // fully dictionary-encoded and its page is locatable.
        assertThat(new RowGroupDictionaryFilterSource(inputFile, rowGroup, schema, context)
                .forColumn(CATEGORY_COLUMN)).isNotNull();
    }

    @Test
    void absentValueInsideStatisticsRangeIsDropped() {
        // "cat_5x" sorts between the min "cat_0" and max "cat_9", so statistics cannot drop it.
        FilterPredicate absent = FilterPredicate.eq("category", "cat_5x");

        assertThat(statisticsDrop(absent)).isFalse();
        assertThat(dictionaryDrop(absent)).isTrue();
    }

    @Test
    void presentValueIsKept() {
        assertThat(dictionaryDrop(FilterPredicate.eq("category", "cat_5"))).isFalse();
    }

    @Test
    void inListDropsOnlyWhenAllValuesAreAbsent() {
        assertThat(dictionaryDrop(FilterPredicate.inStrings("category", "nope_a", "nope_b"))).isTrue();
        // One present value is enough to keep it.
        assertThat(dictionaryDrop(FilterPredicate.inStrings("category", "nope_a", "cat_3"))).isFalse();
    }

    @Test
    void pruningAppliesInsideBooleanCombinators() {
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
    void outOfBoundsColumnIndexYieldsNoDictionary() {
        RowGroupDictionaryFilterSource source = dictionaries();
        assertThat(source.forColumn(-1)).isNull();
    }

    @Test
    void dictionaryIsCachedPerColumn() {
        RowGroupDictionaryFilterSource source = dictionaries();
        assertThat(source.forColumn(CATEGORY_COLUMN))
                .isNotNull()
                .isSameAs(source.forColumn(CATEGORY_COLUMN));
    }

    @Test
    void byteArrayValueAbsentAndAbsentAllDirectTests() {
        byte[] cat5 = "cat_5".getBytes(StandardCharsets.UTF_8);
        byte[] cat0 = "cat_0".getBytes(StandardCharsets.UTF_8);
        byte[] nope = "nope".getBytes(StandardCharsets.UTF_8);

        // valueAbsent
        assertThat(DictionaryFilterSupport.valueAbsent(dictionaries(), CATEGORY_COLUMN, cat5)).isFalse();
        assertThat(DictionaryFilterSupport.valueAbsent(dictionaries(), CATEGORY_COLUMN, nope)).isTrue();
        assertThat(DictionaryFilterSupport.valueAbsent(null, CATEGORY_COLUMN, cat5)).isFalse();
        assertThat(DictionaryFilterSupport.valueAbsent(dictionaries(), 999, cat5)).isFalse();

        // absentAll: unsorted probes killing Arrays.sort removal
        assertThat(DictionaryFilterSupport.absentAll(dictionaries(), CATEGORY_COLUMN,
                new byte[][]{ nope, cat5 })).isFalse();

        // absentAll: match at index 0 after sort killing >= 0 -> > 0 mutant
        assertThat(DictionaryFilterSupport.absentAll(dictionaries(), CATEGORY_COLUMN,
                new byte[][]{ cat0, nope })).isFalse();

        // absentAll: null / wrong column
        assertThat(DictionaryFilterSupport.absentAll(null, CATEGORY_COLUMN, new byte[][]{ cat0 })).isFalse();
        assertThat(DictionaryFilterSupport.absentAll(dictionaries(), 999, new byte[][]{ cat0 })).isFalse();
    }

    private static RowGroupDictionaryFilterSource dictionaries() {
        return new RowGroupDictionaryFilterSource(inputFile, rowGroup, schema, context);
    }

    private static boolean dictionaryDrop(FilterPredicate filter) {
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(filter, schema);
        return RowGroupFilterEvaluator.decideRowGroup(resolved, rowGroup, null, dictionaries())
                == FilterDecision.CANNOT_MATCH;
    }

    private static boolean statisticsDrop(FilterPredicate filter) {
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(filter, schema);
        return RowGroupFilterEvaluator.canDropRowGroup(resolved, rowGroup);
    }
}
