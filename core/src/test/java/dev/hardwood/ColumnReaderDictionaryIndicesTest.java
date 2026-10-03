/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.BinaryDictionary;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The dictionary ids [ColumnReader] exposes: each value resolves through its id to the same
/// bytes as through its binary range, batches end at row-group boundaries so that each draws on
/// one dictionary, and a batch holding any value stored outside the dictionary exposes none.
class ColumnReaderDictionaryIndicesTest {

    /// Two row groups of 100 rows with disjoint pools: row `i` holds entry `i % 3` of
    /// `alpha`/`bravo`/`charlie` below row 100 and of `delta`/`echo`/`foxtrot` from it on.
    private static final Path CROSS_CHUNK = Paths.get("src/test/resources/dict_cross_chunk.parquet");

    /// One row group: 2,000 rows of three repeated values and 500 distinct ones in dictionary
    /// pages, then 1,500 distinct values the writer stored plain once the dictionary was full.
    private static final Path PLAIN_FALLBACK = Paths.get("src/test/resources/dict_plain_fallback.parquet");

    @Test
    void eachValueResolvesThroughItsIdToItsBytes() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(CROSS_CHUNK));
             ColumnReader col = reader.buildColumnReader("label").batchSize(64).build()) {

            List<String> viaIds = new ArrayList<>();
            List<String> viaRanges = new ArrayList<>();
            while (col.nextBatch()) {
                int[] ids = col.getDictionaryIndices();
                assertThat(ids).hasSize(col.getValueCount());
                for (int i = 0; i < col.getValueCount(); i++) {
                    viaIds.add(col.getBinaryDictionary().getString(ids[i]));
                    viaRanges.add(range(col.getBinaryValues(), col.getBinaryStarts()[i], col.getBinaryEnds()[i]));
                }
            }
            assertThat(viaIds).hasSize(200).isEqualTo(viaRanges);
            assertThat(viaIds.subList(0, 3)).containsExactly("alpha", "bravo", "charlie");
            assertThat(viaIds.subList(100, 103)).containsExactly("echo", "foxtrot", "delta");
        }
    }

    @Test
    void batchesEndAtRowGroupBoundariesAndShareOnlyTheirDictionaryArrays() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(CROSS_CHUNK));
             ColumnReader col = reader.buildColumnReader("label").batchSize(64).build()) {

            List<Integer> counts = new ArrayList<>();
            List<BinaryDictionary> dictionaries = new ArrayList<>();
            List<int[]> ids = new ArrayList<>();
            while (col.nextBatch()) {
                counts.add(col.getRecordCount());
                dictionaries.add(col.getBinaryDictionary());
                ids.add(col.getDictionaryIndices());
            }
            assertThat(counts).containsExactly(64, 36, 64, 36);
            assertThat(ids.get(1)).as("ids are fresh per batch").isNotSameAs(ids.get(0));
            assertThat(dictionaries.get(1)).isSameAs(dictionaries.get(0));
            assertThat(dictionaries.get(3)).isSameAs(dictionaries.get(2));
            assertThat(dictionaries.get(2)).isNotSameAs(dictionaries.get(0));
        }
    }

    @Test
    void aBatchHoldingPlainValuesExposesNoIds() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(PLAIN_FALLBACK));
             ColumnReader col = reader.buildColumnReader("label").batchSize(500).build()) {

            List<Boolean> hasIds = new ArrayList<>();
            int row = 0;
            while (col.nextBatch()) {
                int[] ids = col.getDictionaryIndices();
                hasIds.add(ids != null);
                if (ids == null) {
                    assertThat(col.getBinaryDictionary()).isNull();
                }
                for (int i = 0; i < col.getValueCount(); i++, row++) {
                    String expected = row < 2_000 ? new String[]{ "alpha", "bravo", "charlie" }[row % 3]
                            : "distinct-%06d".formatted(row - 2_000);
                    String actual = ids != null
                            ? col.getBinaryDictionary().getString(ids[i])
                            : range(col.getBinaryValues(), col.getBinaryStarts()[i], col.getBinaryEnds()[i]);
                    assertThat(actual).isEqualTo(expected);
                }
            }
            assertThat(hasIds).containsExactly(true, true, true, true, true, false, false, false);
        }
    }

    @Test
    void nullsDoNotWithholdIds() throws Exception {
        Path file = Paths.get("src/test/resources/dict_nested_repeats.parquet");
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             ColumnReader col = reader.columnReader("info.name")) {

            assertThat(col.nextBatch()).isTrue();
            assertThat(col.getLeafValidity().isNull(0)).isTrue();
            int[] ids = col.getDictionaryIndices();
            assertThat(ids).isNotNull();
            assertThat(col.getBinaryDictionary().getString(ids[1])).isEqualTo("beta");
            assertThat(col.getBinaryDictionary().getString(ids[2])).isEqualTo("gamma");
        }
    }

    @Test
    void aRepeatedLeafExposesOneIdPerValue() throws Exception {
        Path file = Paths.get("src/test/resources/dict_nested_repeats.parquet");
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             ColumnReader col = reader.columnReader("tags.list.element")) {

            assertThat(col.nextBatch()).isTrue();
            int[] ids = col.getDictionaryIndices();
            String[] strings = col.getStrings();
            assertThat(ids).hasSize(col.getValueCount());
            for (int i = 0; i < col.getValueCount(); i++) {
                assertThat(col.getBinaryDictionary().getString(ids[i])).isEqualTo(strings[i]);
            }
        }
    }

    @Test
    void theDictionaryReadsEachEntryOnceAsText() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(CROSS_CHUNK));
             ColumnReader col = reader.columnReader("label")) {

            assertThat(col.nextBatch()).isTrue();
            BinaryDictionary dictionary = col.getBinaryDictionary();
            assertThat(dictionary.size()).isEqualTo(3);
            assertThat(dictionary.getString(0)).isEqualTo("alpha");
            assertThat(dictionary.getBinary(1)).isEqualTo("bravo".getBytes(StandardCharsets.UTF_8));
            assertThatThrownBy(() -> dictionary.getBinary(3))
                    .isInstanceOf(IndexOutOfBoundsException.class)
                    .hasMessage("Dictionary entry 3 out of bounds for 3 entries");
        }
    }

    @Test
    void aDictionaryOfANonTextColumnRefusesStrings() throws Exception {
        Path file = Paths.get("src/test/resources/dict_flba_pushdown.parquet");
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             ColumnReader col = reader.columnReader("code")) {

            assertThat(col.nextBatch()).isTrue();
            BinaryDictionary dictionary = col.getBinaryDictionary();
            assertThatThrownBy(() -> dictionary.getString(0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[dict_flba_pushdown.parquet] Column 'code' is FIXED_LEN_BYTE_ARRAY, which cannot be read as a string");
        }
    }

    @Test
    void aColumnWithoutADictionaryExposesNoIds() throws Exception {
        Path file = Paths.get("src/test/resources/plain_uncompressed_with_nulls.parquet");
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             ColumnReader col = reader.columnReader("name")) {

            assertThat(col.nextBatch()).isTrue();
            assertThat(col.getDictionaryIndices()).isNull();
            assertThat(col.getBinaryDictionary()).isNull();
        }
    }

    @Test
    void aFixedWidthColumnRefusesTheDictionaryAccessors() throws Exception {
        Path file = Paths.get("src/test/resources/filter_pushdown_int.parquet");
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             ColumnReader col = reader.columnReader("id")) {

            assertThat(col.nextBatch()).isTrue();
            assertThatThrownBy(col::getDictionaryIndices)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("[filter_pushdown_int.parquet] Column 'id' is INT64, not byte[]");
            assertThatThrownBy(col::getBinaryDictionary)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("[filter_pushdown_int.parquet] Column 'id' is INT64, not byte[]");
        }
    }

    /// Both inputs are named `<memory>` and both consist of row group 0, yet each copy's rows
    /// form their own batches, each with its own dictionary.
    @Test
    void inputsSharingANameAndRowGroupIndexGetTheirOwnBatches() throws Exception {
        byte[] file = writeStations(100, 1_000);
        try (ParquetFileReader reader = ParquetFileReader.openAll(List.of(
                     InputFile.of(ByteBuffer.wrap(file)), InputFile.of(ByteBuffer.wrap(file))));
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("label"))
                     .batchSize(1_000).build()) {

            ColumnReader col = columns.getColumnReader("label");
            List<Integer> counts = new ArrayList<>();
            List<BinaryDictionary> dictionaries = new ArrayList<>();
            while (columns.nextBatch()) {
                counts.add(col.getRecordCount());
                assertThat(col.getDictionaryIndices()).isNotNull();
                dictionaries.add(col.getBinaryDictionary());
            }
            assertThat(counts).containsExactly(100, 100);
            assertThat(dictionaries.get(1)).isNotSameAs(dictionaries.get(0));
        }
    }

    @Test
    void aFixedLengthColumnExposesIds() throws Exception {
        Path file = Paths.get("src/test/resources/dict_flba_pushdown.parquet");
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             ColumnReader col = reader.columnReader("code")) {

            int values = 0;
            while (col.nextBatch()) {
                int[] ids = col.getDictionaryIndices();
                assertThat(ids).isNotNull();
                for (int i = 0; i < col.getValueCount(); i++, values++) {
                    assertThat(col.getBinaryDictionary().getBinary(ids[i])).isEqualTo(
                            Arrays.copyOfRange(col.getBinaryValues(), col.getBinaryStarts()[i], col.getBinaryEnds()[i]));
                }
            }
            assertThat(values).isEqualTo(4_096);
        }
    }

    /// The ids describe the values a filter kept.
    @Test
    void aFilteredBatchExposesTheIdsOfTheValuesItKept() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(CROSS_CHUNK));
             ColumnReader col = reader.buildColumnReader("label")
                     .filter(FilterPredicate.eq("label", "bravo")).build()) {

            int kept = 0;
            while (col.nextBatch()) {
                int[] ids = col.getDictionaryIndices();
                for (int i = 0; i < col.getValueCount(); i++, kept++) {
                    assertThat(col.getBinaryDictionary().getString(ids[i])).isEqualTo("bravo");
                }
            }
            assertThat(kept).isEqualTo(33);
        }
    }

    /// A flat column, a repeated column and a filter-only column read together over several row
    /// groups: their batches stay row-aligned and each draws on one dictionary per column.
    @Test
    void flatRepeatedAndFilterOnlyColumnsStayAlignedAcrossRowGroups() throws Exception {
        byte[] file = writeStations(1_000, 300);
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("label", "tags.list.element"))
                     .filter(FilterPredicate.gt("id", 50L)).batchSize(128).build()) {

            ColumnReader label = columns.getColumnReader("label");
            ColumnReader tags = columns.getColumnReader("tags.list.element");
            int rows = 0;
            Set<BinaryDictionary> labelDictionaries = Collections.newSetFromMap(new IdentityHashMap<>());
            Set<BinaryDictionary> tagDictionaries = Collections.newSetFromMap(new IdentityHashMap<>());
            while (columns.nextBatch()) {
                assertThat(label.getDictionaryIndices()).isNotNull();
                assertThat(tags.getDictionaryIndices()).isNotNull();
                assertResolvesLikeStrings(label);
                assertResolvesLikeStrings(tags);
                labelDictionaries.add(label.getBinaryDictionary());
                tagDictionaries.add(tags.getBinaryDictionary());
                rows += columns.getRecordCount();
            }
            assertThat(rows).isEqualTo(949);
            // One object per row group's dictionary, across filtered and repeated batches.
            assertThat(labelDictionaries).hasSize(4);
            assertThat(tagDictionaries).hasSize(4);
        }
    }

    private static void assertResolvesLikeStrings(ColumnReader col) {
        int[] ids = col.getDictionaryIndices();
        String[] strings = col.getStrings();
        for (int i = 0; i < col.getValueCount(); i++) {
            assertThat(col.getBinaryDictionary().getString(ids[i])).isEqualTo(strings[i]);
        }
    }

    /// `rows` rows in row groups of `rowGroupRows`: `id` = row, `label` cycling three stations,
    /// `tags` one or two of them.
    private static byte[] writeStations(int rows, long rowGroupRows) throws IOException {
        String[] pool = { "Hamburg", "Oslo", "Abha" };
        FileSchema schema = FileSchema.builder("stations")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("label", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, new LogicalType.StringType())
                .list("tags", RepetitionType.REQUIRED,
                        el -> el.primitive(PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, new LogicalType.StringType()))
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema,
                WriterConfig.builder().rowGroupTargetRows(rowGroupRows).build())) {
            for (int r = 0; r < rows; r++) {
                long id = r;
                String label = pool[r % 3];
                String first = pool[(r + 1) % 3];
                boolean two = r % 2 == 0;
                writer.rowWriter().writeRow(row -> row.setLong("id", id).setString("label", label)
                        .setList("tags", list -> {
                            list.addString(first);
                            if (two) {
                                list.addString(label);
                            }
                        }));
            }
        }
        return InMemoryFiles.toByteArray(out);
    }

    private static String range(byte[] bytes, int start, int end) {
        return new String(bytes, start, end - start, StandardCharsets.UTF_8);
    }
}
