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
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Dictionary push-down on a `FIXED_LEN_BYTE_ARRAY` column, against `dict_flba_pushdown.parquet`
/// (one row group, 4096 rows; `code` FLBA(4) cycling `aa00`, `aa03`, `aa06`, `aa09`).
///
/// [DictionaryPushDownTest] reaches the same `ByteArrayDictionary` arm through `BYTE_ARRAY`, but
/// `FIXED_LEN_BYTE_ARRAY` arrives as a distinct physical type. Every probe sits *inside* the
/// column's statistics min/max range, so statistics alone keep the row group and only the
/// dictionary can prove absence.
class DictionaryFixedLenByteArrayPushDownTest {

    private static final Path FIXTURE = Paths.get("src/test/resources/dict_flba_pushdown.parquet");

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
    void theFixtureColumnIsAFixedLenByteArray() {
        // The physical type is the point of this fixture; a BYTE_ARRAY column here would leave the
        // FLBA arm untested while every assertion below still passed.
        assertThat(schema.getColumn(0).type()).isEqualTo(PhysicalType.FIXED_LEN_BYTE_ARRAY);
    }

    @Test
    void absentCodeIsDroppedOnlyByTheDictionary() throws IOException {
        FilterPredicate absent = FilterPredicate.eq("code", "aa05");

        assertThat(statisticsDrop(absent)).as("aa05 sorts within [aa00, aa09]").isFalse();
        assertThat(dictionaryDrop(absent)).isTrue();
        assertThat(dictionaryDrop(FilterPredicate.eq("code", "aa06"))).isFalse();
    }

    @Test
    void codeInListDropsOnlyWhenEveryValueIsAbsent() throws IOException {
        assertThat(dictionaryDrop(FilterPredicate.inStrings("code", "aa05", "aa07"))).isTrue();
        assertThat(dictionaryDrop(FilterPredicate.inStrings("code", "aa05", "aa06"))).isFalse();
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
