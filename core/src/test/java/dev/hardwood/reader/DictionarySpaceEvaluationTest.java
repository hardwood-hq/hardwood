/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.BinaryBatchValues;

import static org.assertj.core.api.Assertions.assertThat;

/// Production-path coverage for dictionary-space binary predicates.
class DictionarySpaceEvaluationTest {

    /// 4096 rows, `code` a dictionary-encoded `FIXED_LEN_BYTE_ARRAY(4)` cycling
    /// `aa00`, `aa03`, `aa06`, `aa09`.
    private static final Path FLBA = Path.of("src/test/resources/dict_flba_pushdown.parquet");

    /// Two row groups with disjoint dictionary pools. The file is smaller than
    /// the row-reader batch floor, so one unfiltered batch crosses the chunk
    /// boundary.
    private static final Path CROSS_CHUNK = Path.of("src/test/resources/dict_cross_chunk.parquet");

    @Test
    void fixedLengthPredicateBatchRetainsDictionaryIds() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(FLBA));
             ColumnReader codes = file.buildColumnReader("code")
                     .filter(FilterPredicate.eq("code", new byte[]{'a', 'a', '0', '6'}))
                     .build()) {
            assertThat(codes.nextBatch()).isTrue();
            assertThat(codes.getRecordCount()).isEqualTo(1024);

            BinaryBatchValues values = (BinaryBatchValues) codes.currentFlatBatch().values;
            assertThat(values.dictionary).isNotNull();
            assertThat(values.dictIndices).hasSize(1024);
            assertThat(codes.nextBatch()).isFalse();
        }
    }

    @Test
    void crossChunkFixtureDecodesAsOneStraddlingBatch() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(CROSS_CHUNK));
             ColumnReader labels = file.buildColumnReader("label").build()) {
            assertThat(labels.nextBatch()).isTrue();
            assertThat(labels.getRecordCount()).isEqualTo(200);
            assertThat(labels.nextBatch()).isFalse();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("stringPredicates")
    void rowAndColumnReadersMatchTheUnfilteredOracle(
            String description, FilterPredicate filter, Predicate<String> oracle) throws Exception {
        List<String> expected = allLabels().stream().filter(oracle).toList();

        assertThat(readRows(filter)).as(description + " row reader").isEqualTo(expected);
        assertThat(readColumn(filter)).as(description + " column reader").isEqualTo(expected);
    }

    private static Stream<Arguments> stringPredicates() {
        Set<String> members = Set.of("alpha", "echo", "zulu");
        return Stream.of(
                Arguments.of("eq", FilterPredicate.eq("label", "bravo"),
                        (Predicate<String>) "bravo"::equals),
                Arguments.of("not eq", FilterPredicate.notEq("label", "bravo"),
                        (Predicate<String>) value -> !value.equals("bravo")),
                Arguments.of("less than", FilterPredicate.lt("label", "delta"),
                        (Predicate<String>) value -> value.compareTo("delta") < 0),
                Arguments.of("less than or equal", FilterPredicate.ltEq("label", "delta"),
                        (Predicate<String>) value -> value.compareTo("delta") <= 0),
                Arguments.of("greater than", FilterPredicate.gt("label", "charlie"),
                        (Predicate<String>) value -> value.compareTo("charlie") > 0),
                Arguments.of("greater than or equal", FilterPredicate.gtEq("label", "charlie"),
                        (Predicate<String>) value -> value.compareTo("charlie") >= 0),
                Arguments.of("in", FilterPredicate.in("label", "alpha", "echo", "zulu"),
                        (Predicate<String>) members::contains),
                Arguments.of("not in",
                        FilterPredicate.not(FilterPredicate.in("label", "alpha", "echo", "zulu")),
                        (Predicate<String>) value -> !members.contains(value)));
    }

    private static List<String> readRows(FilterPredicate filter) throws Exception {
        List<String> matched = new ArrayList<>();
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(CROSS_CHUNK));
             RowReader rows = file.buildRowReader().filter(filter).build()) {
            while (rows.hasNext()) {
                rows.next();
                matched.add(rows.getString("label"));
            }
        }
        return matched;
    }

    private static List<String> readColumn(FilterPredicate filter) throws Exception {
        List<String> matched = new ArrayList<>();
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(CROSS_CHUNK));
             ColumnReader labels = file.buildColumnReader("label").filter(filter).build()) {
            while (labels.nextBatch()) {
                matched.addAll(List.of(labels.getStrings()));
            }
        }
        return matched;
    }

    private static List<String> allLabels() throws Exception {
        List<String> labels = new ArrayList<>();
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(CROSS_CHUNK));
             RowReader rows = file.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                labels.add(rows.getString("label"));
            }
        }
        return labels;
    }
}
