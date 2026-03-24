/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

public class PageScannerTest {

    @Test
    void testOffsetIndexProducesIdenticalResultsToSequentialScan() throws Exception {
        Path file = Paths.get("src/test/resources/page_index_test.parquet");
        FileMetaData fileMetaData;
        FileSchema schema;

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            fileMetaData = reader.getFileMetaData();
            schema = reader.getFileSchema();
        }

        try (HardwoodContextImpl context = HardwoodContextImpl.create();
             InputFile inputFile = InputFile.of(file)) {
            inputFile.open();

            RowGroup rowGroup = fileMetaData.rowGroups().get(0);
            RowGroupIndexBuffers indexBuffers = RowGroupIndexBuffers.fetch(inputFile, rowGroup);

            for (int colIdx = 0; colIdx < rowGroup.columns().size(); colIdx++) {
                ColumnChunk columnChunk = rowGroup.columns().get(colIdx);
                ColumnSchema columnSchema = schema.getColumn(colIdx);

                // Verify offset index is present
                assertThat(columnChunk.offsetIndexOffset())
                        .as("Column '%s' should have offset index", columnSchema.name())
                        .isNotNull();

                PageScanner scanner = createScanner(inputFile, context, columnChunk, columnSchema,
                        indexBuffers.forColumn(colIdx), 0);

                // Get pages via both methods
                List<PageInfo> sequential = scanner.scanPagesSequential();
                List<PageInfo> indexed = scanner.scanPagesFromIndex();

                // Same number of pages
                assertThat(indexed).as("Page count for column '%s'", columnSchema.name())
                        .hasSameSizeAs(sequential);

                // Decode all pages and verify identical data
                for (int p = 0; p < sequential.size(); p++) {
                    PageInfo seqInfo = sequential.get(p);
                    PageInfo idxInfo = indexed.get(p);

                    PageReader seqPageReader = new PageReader(
                            seqInfo.columnMetaData(), seqInfo.columnSchema(),
                            context.decompressorFactory());
                    PageReader idxPageReader = new PageReader(
                            idxInfo.columnMetaData(), idxInfo.columnSchema(),
                            context.decompressorFactory());

                    Page seqPage = seqPageReader.decodePage(seqInfo.pageData(), seqInfo.dictionary());
                    Page idxPage = idxPageReader.decodePage(idxInfo.pageData(), idxInfo.dictionary());

                    assertThat(idxPage.size())
                            .as("Page %d size for column '%s'", p, columnSchema.name())
                            .isEqualTo(seqPage.size());

                    assertThat(idxPage.definitionLevels())
                            .as("Page %d definition levels for column '%s'", p, columnSchema.name())
                            .isEqualTo(seqPage.definitionLevels());

                    assertThat(idxPage.repetitionLevels())
                            .as("Page %d repetition levels for column '%s'", p, columnSchema.name())
                            .isEqualTo(seqPage.repetitionLevels());

                    assertPageValuesEqual(seqPage, idxPage, p, columnSchema.name());
                }
            }
        }
    }

    @Test
    void testScanPagesAutoSelectsIndexPath() throws Exception {
        Path file = Paths.get("src/test/resources/page_index_test.parquet");
        FileMetaData fileMetaData;
        FileSchema schema;

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            fileMetaData = reader.getFileMetaData();
            schema = reader.getFileSchema();
        }

        try (HardwoodContextImpl context = HardwoodContextImpl.create();
             InputFile inputFile = InputFile.of(file)) {
            inputFile.open();

            RowGroup rowGroup = fileMetaData.rowGroups().get(0);
            RowGroupIndexBuffers indexBuffers = RowGroupIndexBuffers.fetch(inputFile, rowGroup);

            ColumnChunk columnChunk = rowGroup.columns().get(0);
            ColumnSchema columnSchema = schema.getColumn(0);

            // Verify the auto-selection works (file has offset index)
            assertThat(columnChunk.offsetIndexOffset()).isNotNull();

            PageScanner scanner = createScanner(inputFile, context, columnChunk, columnSchema,
                    indexBuffers.forColumn(0), 0);
            List<PageInfo> autoPages = scanner.scanPages();
            List<PageInfo> indexPages = scanner.scanPagesFromIndex();

            assertThat(autoPages).hasSameSizeAs(indexPages);
        }
    }

    @Test
    void testSequentialFallbackForFilesWithoutIndex() throws Exception {
        Path file = Paths.get("src/test/resources/plain_uncompressed.parquet");
        FileMetaData fileMetaData;
        FileSchema schema;

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            fileMetaData = reader.getFileMetaData();
            schema = reader.getFileSchema();
        }

        try (HardwoodContextImpl context = HardwoodContextImpl.create();
             InputFile inputFile = InputFile.of(file)) {
            inputFile.open();

            RowGroup rowGroup = fileMetaData.rowGroups().get(0);

            ColumnChunk columnChunk = rowGroup.columns().get(0);
            ColumnSchema columnSchema = schema.getColumn(0);

            // Verify no offset index
            assertThat(columnChunk.offsetIndexOffset()).isNull();

            PageScanner scanner = createScanner(inputFile, context, columnChunk, columnSchema, null, 0);
            List<PageInfo> pages = scanner.scanPages();

            assertThat(pages).isNotEmpty();
        }
    }

    /// Creates a PageScanner by pre-fetching the column chunk data from the InputFile.
    static PageScanner createScanner(InputFile inputFile, HardwoodContextImpl context,
            ColumnChunk columnChunk, ColumnSchema columnSchema,
            ColumnIndexBuffers indexBuffers, int rowGroupIndex) throws IOException {
        ColumnMetaData meta = columnChunk.metaData();
        Long dictOffset = meta.dictionaryPageOffset();
        long chunkStart = (dictOffset != null && dictOffset > 0) ? dictOffset : meta.dataPageOffset();
        int chunkLen = Math.toIntExact(meta.totalCompressedSize());
        ByteBuffer chunkData = inputFile.readRange(chunkStart, chunkLen);
        return new PageScanner(columnSchema, columnChunk, context,
                chunkData, chunkStart, indexBuffers, rowGroupIndex, inputFile.name());
    }

    private void assertPageValuesEqual(Page expected, Page actual, int pageIndex, String columnName) {
        String desc = String.format("Page %d values for column '%s'", pageIndex, columnName);

        if (expected instanceof Page.LongPage seqLong && actual instanceof Page.LongPage idxLong) {
            assertThat(idxLong.values()).as(desc).isEqualTo(seqLong.values());
        }
        else if (expected instanceof Page.IntPage seqInt && actual instanceof Page.IntPage idxInt) {
            assertThat(idxInt.values()).as(desc).isEqualTo(seqInt.values());
        }
        else if (expected instanceof Page.DoublePage seqDouble && actual instanceof Page.DoublePage idxDouble) {
            assertThat(idxDouble.values()).as(desc).isEqualTo(seqDouble.values());
        }
        else if (expected instanceof Page.FloatPage seqFloat && actual instanceof Page.FloatPage idxFloat) {
            assertThat(idxFloat.values()).as(desc).isEqualTo(seqFloat.values());
        }
        else if (expected instanceof Page.BooleanPage seqBool && actual instanceof Page.BooleanPage idxBool) {
            assertThat(idxBool.values()).as(desc).isEqualTo(seqBool.values());
        }
        else if (expected instanceof Page.ByteArrayPage seqBytes && actual instanceof Page.ByteArrayPage idxBytes) {
            assertThat(idxBytes.values()).as(desc).isEqualTo(seqBytes.values());
        }
        else {
            assertThat(actual.getClass()).as(desc + " type mismatch").isEqualTo(expected.getClass());
        }
    }
}
