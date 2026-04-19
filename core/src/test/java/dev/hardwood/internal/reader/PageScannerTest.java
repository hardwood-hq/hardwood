/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Tests that indexed and sequential fetch plans produce identical decoded pages.
///
/// Uses a test file with both OffsetIndex and data pages. Creates both plan types
/// for each column and verifies that decoded page content matches exactly.
public class PageScannerTest {

    @Test
    void indexedAndSequentialPlansProduceIdenticalPages() throws Exception {
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
            ProjectedSchema projected = ProjectedSchema.create(schema, ColumnProjection.all());

            for (int colIdx = 0; colIdx < rowGroup.columns().size(); colIdx++) {
                ColumnChunk columnChunk = rowGroup.columns().get(colIdx);
                ColumnSchema columnSchema = schema.getColumn(colIdx);

                assertThat(columnChunk.offsetIndexOffset())
                        .as("Column '%s' should have offset index", columnSchema.name())
                        .isNotNull();

                // Build indexed plan via RowGroupIterator
                RowGroupIterator indexedIterator = createIterator(inputFile, schema, fileMetaData, context);
                FetchPlan indexedPlan = indexedIterator.getColumnPlan(
                        indexedIterator.getWorkItems().get(0), colIdx);

                // Build sequential plan (bypasses OffsetIndex)
                SequentialFetchPlan sequentialPlan = SequentialFetchPlan.build(
                        inputFile, columnSchema, columnChunk,
                        context, 0, inputFile.name(), 0);

                // Collect pages from both plans
                List<PageInfo> indexedPages = collectPages(indexedPlan.pages());
                List<PageInfo> sequentialPages = collectPages(sequentialPlan.pages());

                assertThat(indexedPages).as("Page count for column '%s'", columnSchema.name())
                        .hasSameSizeAs(sequentialPages);

                // Decode and compare
                for (int p = 0; p < indexedPages.size(); p++) {
                    PageInfo idxInfo = indexedPages.get(p);
                    PageInfo seqInfo = sequentialPages.get(p);

                    PageDecoder idxDecoder = new PageDecoder(
                            idxInfo.columnMetaData(), idxInfo.columnSchema(),
                            context.decompressorFactory());
                    PageDecoder seqDecoder = new PageDecoder(
                            seqInfo.columnMetaData(), seqInfo.columnSchema(),
                            context.decompressorFactory());

                    Page idxPage = idxDecoder.decodePage(idxInfo.pageData(), idxInfo.dictionary());
                    Page seqPage = seqDecoder.decodePage(seqInfo.pageData(), seqInfo.dictionary());

                    assertThat(idxPage.size())
                            .as("Page %d size for column '%s'", p, columnSchema.name())
                            .isEqualTo(seqPage.size());

                    assertThat(idxPage.definitionLevels())
                            .as("Page %d definition levels for column '%s'", p, columnSchema.name())
                            .isEqualTo(seqPage.definitionLevels());

                    assertPageValuesEqual(seqPage, idxPage, p, columnSchema.name());
                }

                indexedIterator.close();
            }
        }
    }

    @Test
    void sequentialFallbackForFilesWithoutIndex() throws Exception {
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

            assertThat(columnChunk.offsetIndexOffset()).isNull();

            SequentialFetchPlan plan = SequentialFetchPlan.build(
                    inputFile, columnSchema, columnChunk,
                    context, 0, inputFile.name(), 0);

            List<PageInfo> pages = collectPages(plan.pages());
            assertThat(pages).isNotEmpty();
        }
    }

    @Test
    void sequentialScanWithMaxRowsStopsEarly() throws Exception {
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

            assertThat(columnChunk.offsetIndexOffset()).isNull();

            List<PageInfo> fullPages = collectPages(SequentialFetchPlan.build(
                    inputFile, columnSchema, columnChunk,
                    context, 0, inputFile.name(), 0).pages());

            // Requesting a single row must not scan past the first data page.
            List<PageInfo> limitedPages = collectPages(SequentialFetchPlan.build(
                    inputFile, columnSchema, columnChunk,
                    context, 0, inputFile.name(), 1).pages());

            assertThat(limitedPages).hasSize(1);
            assertThat(limitedPages.size()).isLessThanOrEqualTo(fullPages.size());
        }
    }

    private static RowGroupIterator createIterator(InputFile inputFile, FileSchema schema,
                                                    FileMetaData metaData,
                                                    HardwoodContextImpl context) {
        RowGroupIterator iterator = new RowGroupIterator(
                List.of(inputFile), context, 0);
        iterator.setFirstFile(schema, metaData.rowGroups());
        iterator.initialize(
                ProjectedSchema.create(schema, ColumnProjection.all()), null);
        return iterator;
    }

    private static List<PageInfo> collectPages(Iterator<PageInfo> iter) {
        List<PageInfo> pages = new ArrayList<>();
        while (iter.hasNext()) {
            pages.add(iter.next());
        }
        return pages;
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
