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
/// Asserts the evaluator decision directly; [dev.hardwood.DictionaryEndToEndTest] drives the same
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
    void fixtureIsDictionaryEncoded() throws IOException {
        // A readable dictionary is exactly what the push-down precondition amounts to: the chunk is
        // fully dictionary-encoded and its page is locatable.
        assertThat(new RowGroupDictionaryFilterSource(inputFile, rowGroup, 0, schema, context)
                .forColumn(CATEGORY_COLUMN)).isNotNull();
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
    void inListDropsOnlyWhenAllValuesAreAbsent() throws IOException {
        assertThat(dictionaryDrop(FilterPredicate.inStrings("category", "nope_a", "nope_b"))).isTrue();
        // One present value is enough to keep it.
        assertThat(dictionaryDrop(FilterPredicate.inStrings("category", "nope_a", "cat_3"))).isFalse();
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

    private static RowGroupDictionaryFilterSource dictionaries() {
        return new RowGroupDictionaryFilterSource(inputFile, rowGroup, 0, schema, context);
    }

    private static boolean dictionaryDrop(FilterPredicate filter) throws IOException {
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(filter, schema);
        return RowGroupFilterEvaluator.decideRowGroup(resolved, rowGroup, null, dictionaries())
                == FilterDecision.CANNOT_MATCH;
    }

    private static boolean statisticsDrop(FilterPredicate filter) throws IOException {
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(filter, schema);
        return RowGroupFilterEvaluator.decideRowGroup(resolved, rowGroup, null, null) == FilterDecision.CANNOT_MATCH;
    }
}
