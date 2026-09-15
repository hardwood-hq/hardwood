/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.BinaryBatchValues;

import static org.assertj.core.api.Assertions.assertThat;

/// Production-path coverage for dictionary-space binary equality.
///
/// The matcher unit tests in
/// [dev.hardwood.internal.predicate.ColumnBatchMatcherTest] drive hand-built
/// batches; these read real files, so they also cover the retention plumbing
/// and the `-1` sentinels the reader writes.
class DictionarySpaceEvaluationTest {

    /// 4096 rows, `code` a dictionary-encoded `FIXED_LEN_BYTE_ARRAY(4)` cycling
    /// `aa00`, `aa03`, `aa06`, `aa09`.
    private static final Path FLBA = Paths.get("src/test/resources/dict_flba_pushdown.parquet");

    /// 200 rows in two row groups — so two column chunks and two dictionaries —
    /// with disjoint value pools: rows 0-99 from `{alpha, bravo, charlie}` and
    /// rows 100-199 from `{delta, echo, foxtrot}`. The file is below the row
    /// reader's batch floor, so one batch straddles both chunks.
    private static final Path CROSS_CHUNK = Paths.get("src/test/resources/dict_cross_chunk.parquet");

    /// A filtered `ColumnReader` retains entry IDs for a non-string column.
    /// Unlike a `UTF8` column, this fixed-length binary fixture never retains
    /// IDs for `String` interning, so only the dictionary-space filter
    /// requirement can switch them on. The batch is inspected after selection,
    /// which additionally pins that compaction carries the provenance through.
    @Test
    void fixedLengthPredicateBatchRetainsDictionaryIds() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(FLBA));
             ColumnReader codes = file.buildColumnReader("code")
                     .filter(FilterPredicate.eq("code", "aa06"))
                     .build()) {
            assertThat(codes.nextBatch()).isTrue();
            assertThat(codes.getRecordCount()).isEqualTo(1024);

            BinaryBatchValues values =
                    (BinaryBatchValues) codes.currentFlatBatch().values;
            assertThat(values.dictionary).isNotNull();
            assertThat(values.dictIndices).hasSize(1024);

            assertThat(codes.nextBatch()).isFalse();
        }
    }

    /// The cross-chunk fixture must decode as a single straddling batch, or the
    /// parity tests below would silently stop covering the fallback.
    @Test
    void crossChunkFixtureDecodesAsOneStraddlingBatch() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(CROSS_CHUNK));
             ColumnReader labels = file.buildColumnReader("label").build()) {
            assertThat(labels.nextBatch()).isTrue();
            assertThat(labels.getRecordCount()).isEqualTo(200);
            assertThat(labels.nextBatch()).isFalse();
        }
    }

    /// A batch adopts only the first chunk's dictionary, so rows from the
    /// second chunk carry the `-1` sentinel and must be answered by comparing
    /// packed bytes. `alpha` exercises the dictionary-ID path, `delta` the
    /// fallback, and `zulu` a value absent from both chunks. Mis-resolving
    /// chunk 1's ordinals against chunk 0's dictionary would match the wrong
    /// rows, which the disjoint pools make observable.
    @ParameterizedTest
    @ValueSource(strings = {"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "zulu"})
    void rowReaderCrossChunkEqualityMatchesEveryOccurrence(String needle) throws Exception {
        List<String> expected = allLabels().stream().filter(needle::equals).toList();

        List<String> matched = new ArrayList<>();
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(CROSS_CHUNK));
             RowReader rows = file.buildRowReader()
                     .filter(FilterPredicate.eq("label", needle))
                     .build()) {
            while (rows.hasNext()) {
                rows.next();
                matched.add(rows.getString("label"));
            }
        }

        assertThat(matched).isEqualTo(expected);
    }

    /// The exact `ColumnReader` path evaluates the same matcher from the
    /// consumer thread through `SelectionEngine`, so it needs its own straddle
    /// coverage.
    @ParameterizedTest
    @ValueSource(strings = {"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "zulu"})
    void columnReaderCrossChunkEqualityMatchesEveryOccurrence(String needle) throws Exception {
        List<String> expected = allLabels().stream().filter(needle::equals).toList();

        List<String> matched = new ArrayList<>();
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(CROSS_CHUNK));
             ColumnReader labels = file.buildColumnReader("label")
                     .filter(FilterPredicate.eq("label", needle))
                     .build()) {
            while (labels.nextBatch()) {
                matched.addAll(List.of(labels.getStrings()));
            }
        }

        assertThat(matched).isEqualTo(expected);
    }

    /// The oracle: every `label` in file order, read without a filter.
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
