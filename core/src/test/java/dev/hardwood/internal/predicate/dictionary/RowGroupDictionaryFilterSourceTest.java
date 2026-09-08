/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate.dictionary;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.Dictionary;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PageEncodingStats;
import dev.hardwood.metadata.PageType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Locating a column chunk's dictionary page from column metadata alone.
///
/// `dictionary_page_offset` is optional in parquet.thrift and its absence is ordinary — parquet-mr
/// 1.12 and Trino before 427 both omit it — in which case the dictionary page is the chunk's first
/// page, the fallback parquet-java's ColumnChunkMetaData.getStartingPos() makes.
/// The offsets locate the page and size the opening read; the page header states its own length and
/// is the authority on it. Offsets that cannot be read at all are rejected.
class RowGroupDictionaryFilterSourceTest {

    /// One row group, 10 000 rows; `category` cycles 10 distinct values, so its dictionary page is
    /// far smaller than the probe.
    private static final Path SMALL_DICTIONARY = Paths.get("src/test/resources/column_index_pushdown_dict.parquet");

    /// One row group, 2 000 rows. `label` omits `dictionary_page_offset`
    /// with `data_page_offset` naming the dictionary page.
    private static final Path MISSING_PAGE_OFFSET = Paths.get("src/test/resources/dict_missing_page_offset.parquet");

    private static final int DICTIONARY_COLUMN = 1;

    /// Every value of the fixture's dictionary column is dictionary-encoded, so the chunk really is
    /// eligible; substituting `encoding_stats` is what makes each ineligible shape reachable
    /// without a fixture per shape. A chunk reported ineligible yields no dictionary, and a `null`
    /// dictionary is what stops [DictionaryFilterSupport] from proving anything absent — so these
    /// pin the one guarantee that keeps push-down from dropping row groups that hold matching rows.
    @Test
    void aChunkWhoseEncodingStatsAreAbsentIsIneligible() throws Exception {
        withFixture(SMALL_DICTIONARY, fixture ->
                assertThat(fixture.sourceWithEncodingStats(List.of()).forColumn(DICTIONARY_COLUMN))
                        .isNull());
    }

    @Test
    void aChunkWithANonDictionaryDataPageIsIneligible() throws Exception {
        // The writer-fallback shape: the dictionary filled up part-way through the chunk and the
        // remaining pages went out plain, so the dictionary describes only a prefix of the values.
        withFixture(SMALL_DICTIONARY, fixture ->
                assertThat(fixture.sourceWithEncodingStats(List.of(
                        new PageEncodingStats(PageType.DICTIONARY_PAGE, Encoding.PLAIN, 1),
                        new PageEncodingStats(PageType.DATA_PAGE, Encoding.RLE_DICTIONARY, 3),
                        new PageEncodingStats(PageType.DATA_PAGE, Encoding.PLAIN, 2)))
                        .forColumn(DICTIONARY_COLUMN))
                        .isNull());
    }

    @Test
    void aChunkCountingAPageTypeThisReleaseCannotClassifyIsIneligible() throws Exception {
        // An unrecognized page type may hold values in any encoding, so it cannot be assumed not
        // to contradict the dictionary.
        withFixture(SMALL_DICTIONARY, fixture ->
                assertThat(fixture.sourceWithEncodingStats(List.of(
                        new PageEncodingStats(PageType.DICTIONARY_PAGE, Encoding.PLAIN, 1),
                        new PageEncodingStats(PageType.DATA_PAGE, Encoding.RLE_DICTIONARY, 3),
                        new PageEncodingStats(PageType.UNKNOWN, Encoding.RLE_DICTIONARY, 1)))
                        .forColumn(DICTIONARY_COLUMN))
                        .isNull());
    }

    @Test
    void aChunkWithNoDataPageIsIneligible() throws Exception {
        // A dictionary page alone proves nothing about values no data page claims to hold.
        withFixture(SMALL_DICTIONARY, fixture ->
                assertThat(fixture.sourceWithEncodingStats(List.of(
                        new PageEncodingStats(PageType.DICTIONARY_PAGE, Encoding.PLAIN, 1)))
                        .forColumn(DICTIONARY_COLUMN))
                        .isNull());
    }

    @Test
    void wellFormedOffsetsReadTheDictionary() throws Exception {
        withFixture(SMALL_DICTIONARY, fixture ->
                assertThat(entryCount(fixture.source().forColumn(DICTIONARY_COLUMN))).isEqualTo(10));
    }

    @Test
    void offsetsPointingAtTheSameByteStillLocateTheDictionary() throws Exception {
        withFixture(SMALL_DICTIONARY, fixture -> {
            Dictionary expected = fixture.source().forColumn(DICTIONARY_COLUMN);
            long chunkStart = fixture.dictionaryPageOffset();

            Dictionary sameByte = fixture.sourceWithOffsets(chunkStart, chunkStart)
                    .forColumn(DICTIONARY_COLUMN);

            assertSameEntries(sameByte, expected);
        });
    }

    @Test
    void aChunkWithoutADictionaryPageOffsetIsStillPruned() throws Exception {
        // The offset is optional; parquet-mr 1.12 and Trino before 427 omit it on
        // dictionary-encoded chunks. The page is then the chunk's first, and its header sizes
        // the read.
        withFixture(MISSING_PAGE_OFFSET, fixture -> {
            assertThat(fixture.metaData().dictionaryPageOffset())
                    .isNull();
            assertThat(entryCount(fixture.source().forColumn(DICTIONARY_COLUMN))).isEqualTo(50);
        });
    }

    @Test
    void aSiblingColumnWithConventionalOffsetsStillPrunes() throws Exception {
        // The same file's 'id' column keeps its offsets, so one chunk's defect does not disable
        // push-down for the rest of the row group.
        withFixture(MISSING_PAGE_OFFSET, fixture ->
                assertThat(fixture.source().forColumn(0)).isNotNull());
    }

    @Test
    void aNonDictionaryPageAtTheDeclaredOffsetYieldsNoDictionary() throws Exception {
        // data_page_offset naming a real data page: the implicit reading finds no dictionary
        // page header there, so there is nothing to prune with.
        withFixture(SMALL_DICTIONARY, fixture ->
                assertThat(fixture.sourceWithOffsets(null, fixture.dataPageOffset())
                        .forColumn(DICTIONARY_COLUMN)).isNull());
    }

    @Test
    void dictionaryPageAfterFirstDataPageIsRejected() throws Exception {
        withFixture(SMALL_DICTIONARY, fixture -> {
            RowGroupDictionaryFilterSource source = fixture.sourceWithOffsets(4096L, 1024L);

            assertThatThrownBy(() -> source.forColumn(DICTIONARY_COLUMN))
                    .isInstanceOf(ParquetReadException.class)
                    .hasMessage("[column_index_pushdown_dict.parquet] row group 0, column category,"
                            + " dictionary page at byte 4096 (0x001000) — Malformed Parquet"
                            + " metadata: the dictionary page lies after the first data page at offset 1024");
        });
    }

    @Test
    void aDictionaryPageStartingBeyondItsChunkIsRejected() throws Exception {
        withFixture(SMALL_DICTIONARY, fixture -> {
            RowGroupDictionaryFilterSource source = fixture.sourceWithChunkSize(0L);

            assertThatThrownBy(() -> source.forColumn(DICTIONARY_COLUMN))
                    .isInstanceOf(ParquetReadException.class)
                    .hasMessage("[column_index_pushdown_dict.parquet] row group 0, column category,"
                            + " dictionary page at byte 95903 (0x01769f) — Malformed Parquet"
                            + " metadata: the chunk holding the dictionary page ends at offset 95903, so it"
                            + " holds no bytes at all");
        });
    }

    @Test
    void aHeaderDeclaringMoreBytesThanTheChunkHoldsIsRejected() throws Exception {
        // The header is valid and states a page longer than the bytes the chunk declares.
        // Truncating the read to the chunk would hand the parser a buffer shorter than the body
        // that same header declares, failing later and less clearly.
        withFixture(SMALL_DICTIONARY, fixture -> {
            RowGroupDictionaryFilterSource source = fixture.sourceWithChunkSize(50L);

            assertThatThrownBy(() -> source.forColumn(DICTIONARY_COLUMN))
                    .isInstanceOf(ParquetReadException.class)
                    .hasMessage("[column_index_pushdown_dict.parquet] row group 0, column category,"
                            + " dictionary page at byte 95903 (0x01769f) — Malformed Parquet"
                            + " metadata: the dictionary page header declares 106 bytes but only 50 remain"
                            + " in the chunk");
        });
    }

    private static int entryCount(Dictionary dictionary) {
        assertThat(dictionary).isInstanceOf(Dictionary.ByteArrayDictionary.class);
        return ((Dictionary.ByteArrayDictionary) dictionary).values().length;
    }

    private static void assertSameEntries(Dictionary actual, Dictionary expected) {
        assertThat(actual).isNotNull();
        assertThat(((Dictionary.ByteArrayDictionary) actual).values())
                .isDeepEqualTo(((Dictionary.ByteArrayDictionary) expected).values());
    }

    private static void withFixture(Path path, FixtureTest test) throws Exception {
        InputFile inputFile = InputFile.of(path);
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile);
             HardwoodContextImpl context = HardwoodContextImpl.create()) {
            test.run(new Fixture(inputFile,
                    reader.getFileMetaData().rowGroups().getFirst(),
                    FileSchema.fromSchemaElements(reader.getFileMetaData().schema()),
                    context));
        }
    }

    @FunctionalInterface
    private interface FixtureTest {
        void run(Fixture fixture) throws Exception;
    }

    /// One fixture's row group, with helpers that rewrite the dictionary column's offsets so a
    /// writer convention can be reproduced without a fixture per convention.
    private record Fixture(InputFile inputFile, RowGroup rowGroup, FileSchema schema,
                           HardwoodContextImpl context) {

        long dictionaryPageOffset() {
            return metaData().dictionaryPageOffset();
        }

        long dataPageOffset() {
            return metaData().dataPageOffset();
        }

        RowGroupDictionaryFilterSource source() {
            return new RowGroupDictionaryFilterSource(inputFile, rowGroup, 0, schema, context);
        }

        RowGroupDictionaryFilterSource sourceWithOffsets(Long dictionaryPageOffset, long dataPageOffset) {
            return sourceWith(dictionaryPageOffset, dataPageOffset, metaData().totalCompressedSize());
        }

        RowGroupDictionaryFilterSource sourceWithEncodingStats(List<PageEncodingStats> encodingStats) {
            ColumnMetaData m = metaData();
            return sourceWith(new ColumnMetaData(m.type(), m.encodings(), m.pathInSchema(), m.codec(),
                    m.numValues(), m.totalUncompressedSize(), m.totalCompressedSize(),
                    m.keyValueMetadata(), m.dataPageOffset(), m.dictionaryPageOffset(), m.statistics(),
                    m.geospatialStatistics(), m.bloomFilterOffset(), m.bloomFilterLength(),
                    encodingStats, m.sizeStatistics()));
        }

        RowGroupDictionaryFilterSource sourceWithChunkSize(long totalCompressedSize) {
            return sourceWith(metaData().dictionaryPageOffset(), metaData().dataPageOffset(),
                    totalCompressedSize);
        }

        RowGroupDictionaryFilterSource sourceWith(Long dictionaryPageOffset, long dataPageOffset,
                                                  long totalCompressedSize) {
            ColumnMetaData m = metaData();
            return sourceWith(new ColumnMetaData(m.type(), m.encodings(), m.pathInSchema(), m.codec(),
                    m.numValues(), m.totalUncompressedSize(), totalCompressedSize,
                    m.keyValueMetadata(), dataPageOffset, dictionaryPageOffset, m.statistics(),
                    m.geospatialStatistics(), m.bloomFilterOffset(), m.bloomFilterLength(),
                    m.encodingStats(), m.sizeStatistics()));
        }

        ColumnMetaData metaData() {
            return rowGroup.columns().get(DICTIONARY_COLUMN).metaData();
        }

        private RowGroupDictionaryFilterSource sourceWith(ColumnMetaData patched) {
            ColumnChunk original = rowGroup.columns().get(DICTIONARY_COLUMN);
            ColumnChunk replacement = new ColumnChunk(patched, original.offsetIndexOffset(),
                    original.offsetIndexLength(), original.columnIndexOffset(),
                    original.columnIndexLength(), "");
            List<ColumnChunk> columns = rowGroup.columns().stream()
                    .map(chunk -> chunk == original ? replacement : chunk)
                    .toList();
            return new RowGroupDictionaryFilterSource(inputFile,
                    new RowGroup(columns, rowGroup.totalByteSize(), rowGroup.numRows()), 0,
                    schema, context);
        }
    }
}
