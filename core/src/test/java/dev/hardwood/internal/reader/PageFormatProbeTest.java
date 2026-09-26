/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import dev.hardwood.InputFile;
import dev.hardwood.internal.thrift.ThriftCompactConstants;
import dev.hardwood.internal.thrift.ThriftCompactWriter;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PageType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Verifies [PageFormatProbe.firstDataPageType] against fixtures whose
/// writer-version is known at generation time. The probe's result feeds the
/// row-group-wide mask gate in [RowGroupIterator]; its correctness is the
/// load-bearing piece of the nested-v1 fallback path that this codebase has
/// no dedicated fixture for.
class PageFormatProbeTest {

    /// `inline_page_stats.parquet` is generated with `data_page_version='1.0'`.
    private static final Path V1_FIXTURE =
            Paths.get("src/test/resources/inline_page_stats.parquet");

    /// `misaligned_pages_no_index.parquet` is generated with `data_page_version='2.0'`.
    private static final Path V2_FIXTURE =
            Paths.get("src/test/resources/misaligned_pages_no_index.parquet");

    @Test
    void testProbeReturnsDataPageForV1Fixture() throws Exception {
        try (InputFile file = InputFile.of(V1_FIXTURE)) {
            file.open();
            FileMetaData meta = ParquetMetadataReader.readMetadata(file);
            RowGroup rg = meta.rowGroups().get(0);
            for (int c = 0; c < rg.columns().size(); c++) {
                PageType type = PageFormatProbe.firstDataPageType(file, rg.columns().get(c));
                assertThat(type)
                        .as("column %d (path=%s)", c, rg.columns().get(c).metaData().pathInSchema())
                        .isEqualTo(PageType.DATA_PAGE);
            }
        }
    }

    @Test
    void testProbeReturnsDataPageV2ForV2Fixture() throws Exception {
        try (InputFile file = InputFile.of(V2_FIXTURE)) {
            file.open();
            FileMetaData meta = ParquetMetadataReader.readMetadata(file);
            RowGroup rg = meta.rowGroups().get(0);
            for (int c = 0; c < rg.columns().size(); c++) {
                PageType type = PageFormatProbe.firstDataPageType(file, rg.columns().get(c));
                assertThat(type)
                        .as("column %d (path=%s)", c, rg.columns().get(c).metaData().pathInSchema())
                        .isEqualTo(PageType.DATA_PAGE_V2);
            }
        }
    }

    /// The `tags` leaf of each fixture is dictionary-encoded, and its footer either leaves the
    /// dictionary page's header out of `data_page_offset` (so the offset points into the
    /// dictionary page body) or omits `dictionary_page_offset` and names the dictionary page as
    /// `data_page_offset`. The header at `data_page_offset` is not a data page's, so the probe
    /// falls back to walking from the chunk start and finds the first data page past the
    /// dictionary page: three reads, at `data_page_offset`, the dictionary page and the data page.
    /// `nested_dict_v2.parquet` states both offsets correctly and is answered by the first read.
    @ParameterizedTest
    @CsvSource({
            "nested_dict_understated_offset_v1.parquet, DATA_PAGE, 3",
            "nested_dict_understated_offset_v2.parquet, DATA_PAGE_V2, 3",
            "nested_dict_no_dict_offset_v2.parquet, DATA_PAGE_V2, 3",
            "nested_dict_v2.parquet, DATA_PAGE_V2, 1"
    })
    void testProbeFindsFirstDataPagePastDictionaryPage(String fixture, PageType expected, int reads)
            throws Exception {
        try (CountingInputFile file = new CountingInputFile(InputFile.of(Paths.get("src/test/resources", fixture)))) {
            file.open();
            FileMetaData meta = ParquetMetadataReader.readMetadata(file);
            RowGroup rg = meta.rowGroups().get(0);
            int readsBefore = file.readCount();
            assertThat(PageFormatProbe.firstDataPageType(file, rg.columns().get(1))).isEqualTo(expected);
            assertThat(file.readCount() - readsBefore).as("reads").isEqualTo(reads);
        }
    }

    /// A chunk holding only an empty dictionary page: the walk steps over it by its header
    /// alone, reaches the chunk end and reports that no data page follows.
    @Test
    void testProbeRejectsAChunkWithoutADataPage() throws Exception {
        byte[] dictionaryPage = dictionaryPageHeader(0);
        try (InputFile file = InputFile.of(ByteBuffer.wrap(dictionaryPage))) {
            file.open();

            assertThatThrownBy(() -> PageFormatProbe.firstDataPageType(file, chunk(dictionaryPage.length)))
                    .isExactlyInstanceOf(ParquetReadException.class)
                    .hasMessage("Column chunk at offset 0 has no data page");
        }
    }

    /// A dictionary page declaring more bytes than the chunk holds is a malformed page, not a
    /// chunk without data pages.
    @Test
    void testProbeRejectsAPageRunningPastTheChunkEnd() throws Exception {
        byte[] header = dictionaryPageHeader(100);
        int chunkLength = header.length + 10;
        try (InputFile file = InputFile.of(ByteBuffer.allocate(chunkLength).put(header).rewind())) {
            file.open();

            assertThatThrownBy(() -> PageFormatProbe.firstDataPageType(file, chunk(chunkLength)))
                    .isExactlyInstanceOf(ParquetReadException.class)
                    .hasMessage("Page at offset 0 ends at offset " + (header.length + 100)
                            + ", past the end of the column chunk at offset " + chunkLength);
        }
    }

    /// The header of a dictionary page whose body is `compressedSize` bytes.
    private static byte[] dictionaryPageHeader(int compressedSize) {
        ThriftCompactWriter writer = new ThriftCompactWriter();
        writer.writeFieldBegin(1, ThriftCompactConstants.FieldType.I32);
        writer.writeI32(2); // Thrift PageType.DICTIONARY_PAGE
        writer.writeFieldBegin(2, ThriftCompactConstants.FieldType.I32);
        writer.writeI32(compressedSize);
        writer.writeFieldBegin(3, ThriftCompactConstants.FieldType.I32);
        writer.writeI32(compressedSize);
        writer.writeFieldStop();
        return writer.toByteArray();
    }

    /// A column chunk of `totalCompressedSize` bytes at offset 0 declaring its dictionary page there.
    private static ColumnChunk chunk(long totalCompressedSize) {
        ColumnMetaData metaData = new ColumnMetaData(PhysicalType.BYTE_ARRAY, List.of(Encoding.PLAIN),
                FieldPath.of("col"), CompressionCodec.UNCOMPRESSED, 10, totalCompressedSize, totalCompressedSize,
                Map.of(), 0, 0L, null, null, null, null, List.of(), null);
        return new ColumnChunk(metaData, null, null, null, null, null);
    }
}
