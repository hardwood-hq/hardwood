/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Tests that indexed and sequential fetch plans produce identical decoded pages.
///
/// Uses a test file with both OffsetIndex and data pages. Creates both plan types
/// for each column and verifies that decoded page content matches exactly.
public class FetchPlanEquivalenceTest {

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

                    ReferencePageDecoder idxDecoder = new ReferencePageDecoder(idxInfo.columnSchema(),
                            idxInfo.columnMetaData().codec(), context.decompressorFactory());
                    ReferencePageDecoder seqDecoder = new ReferencePageDecoder(seqInfo.columnSchema(),
                            seqInfo.columnMetaData().codec(), context.decompressorFactory());

                    ReferencePageDecoder.DecodedPage idxPage = idxDecoder.decode(idxInfo.pageData(),
                            idxInfo.dictionary());
                    ReferencePageDecoder.DecodedPage seqPage = seqDecoder.decode(seqInfo.pageData(),
                            seqInfo.dictionary());

                    assertThat(idxPage.numValues())
                            .as("Page %d size for column '%s'", p, columnSchema.name())
                            .isEqualTo(seqPage.numValues());

                    assertThat(idxPage.definitionLevels())
                            .as("Page %d definition levels for column '%s'", p, columnSchema.name())
                            .isEqualTo(seqPage.definitionLevels());

                    assertThat(idxPage.values())
                            .as("Page %d values for column '%s'", p, columnSchema.name())
                            .isEqualTo(seqPage.values());
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
                                                    HardwoodContextImpl context) throws IOException {
        RowGroupIterator iterator = new RowGroupIterator(
                List.of(inputFile), context, 0);
        iterator.setFirstFile(schema, metaData.rowGroups());
        iterator.initialize(
                ProjectedSchema.create(schema, ColumnProjection.all()), null);
        return iterator;
    }

    private static List<PageInfo> collectPages(PageIterator iter) throws IOException {
        List<PageInfo> pages = new ArrayList<>();
        while (iter.hasNext()) {
            pages.add(iter.next());
        }
        return pages;
    }
}
