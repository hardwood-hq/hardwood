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
import java.util.BitSet;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

class RowGroupIndexBuffersTest {

    private static final Path PAGE_INDEX_FILE = Paths.get("src/test/resources/page_index_test.parquet");
    private static final Path PLAIN_FILE = Paths.get("src/test/resources/plain_uncompressed.parquet");

    @Test
    void fetchesEveryColumnsIndexesInOneRequestPerStructure() throws Exception {
        CountingInputFile countingFile = new CountingInputFile(InputFile.of(PAGE_INDEX_FILE));
        countingFile.open();

        FileMetaData meta = ParquetMetadataReader.readMetadata(countingFile);
        RowGroup rowGroup = meta.rowGroups().get(0);

        int readsBefore = countingFile.readCount();
        RowGroupIndexBuffers buffers = RowGroupIndexBuffers.fetch(countingFile, rowGroup);
        int readsForFetch = countingFile.readCount() - readsBefore;

        // One request per structure
        assertThat(readsForFetch).isEqualTo(2);

        // All 3 columns (id, value, category) should have offset index buffers
        for (int i = 0; i < rowGroup.columns().size(); i++) {
            ColumnIndexBuffers colBuffers = buffers.forColumn(i);
            assertThat(colBuffers).as("Column %d", i).isNotNull();
            assertThat(colBuffers.offsetIndex())
                    .as("Column %d offset index", i).isNotNull();
        }
    }

    @Test
    void fetchesOnlyTheRequestedSlicesPerStructure() throws Exception {
        CountingInputFile countingFile = new CountingInputFile(InputFile.of(PAGE_INDEX_FILE));
        countingFile.open();
        FileMetaData meta = ParquetMetadataReader.readMetadata(countingFile);
        RowGroup rowGroup = meta.rowGroups().get(0);
        ColumnChunk first = rowGroup.columns().get(0);
        ColumnChunk last = rowGroup.columns().get(2);

        int readsBefore = countingFile.readCount();
        RowGroupIndexBuffers buffers = RowGroupIndexBuffers.fetch(countingFile, rowGroup,
                BitSet.valueOf(new long[] { 0b101 }), BitSet.valueOf(new long[] { 0b001 }));

        // One request per structure, each spanning only the requested slices.
        assertThat(countingFile.reads().subList(readsBefore, countingFile.readCount()))
                .extracting(CountingInputFile.Read::offset, CountingInputFile.Read::length)
                .containsExactlyInAnyOrder(
                        tuple(first.columnIndexOffset(), first.columnIndexLength()),
                        tuple(first.offsetIndexOffset(), Math.toIntExact(last.offsetIndexOffset()
                                + last.offsetIndexLength() - first.offsetIndexOffset())));

        assertThat(buffers.forColumn(0).offsetIndex()).isNotNull();
        assertThat(buffers.forColumn(0).columnIndex()).isNotNull();
        assertThat(buffers.forColumn(2).offsetIndex()).isNotNull();
        assertThat(buffers.forColumn(2).columnIndex()).isNull();
    }

    @Test
    void aColumnTheReadDidNotAskForIsRejected() throws Exception {
        CountingInputFile countingFile = new CountingInputFile(InputFile.of(PAGE_INDEX_FILE));
        countingFile.open();
        FileMetaData meta = ParquetMetadataReader.readMetadata(countingFile);
        RowGroup rowGroup = meta.rowGroups().get(0);

        RowGroupIndexBuffers buffers = RowGroupIndexBuffers.fetch(countingFile, rowGroup,
                BitSet.valueOf(new long[] { 0b001 }), new BitSet());

        assertThatThrownBy(() -> buffers.forColumn(1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Page index of column 1 was not fetched for this read");
    }

    @Test
    void returnsNullForFileWithoutIndexes() throws Exception {
        CountingInputFile countingFile = new CountingInputFile(InputFile.of(PLAIN_FILE));
        countingFile.open();

        FileMetaData meta = ParquetMetadataReader.readMetadata(countingFile);
        RowGroup rowGroup = meta.rowGroups().get(0);

        int readsBefore = countingFile.readCount();
        RowGroupIndexBuffers buffers = RowGroupIndexBuffers.fetch(countingFile, rowGroup);
        int readsForFetch = countingFile.readCount() - readsBefore;

        // No indexes to fetch — should not issue any readRange() calls
        assertThat(readsForFetch).isEqualTo(0);

        // Every column was asked for and has no index
        for (int i = 0; i < rowGroup.columns().size(); i++) {
            assertThat(buffers.forColumn(i).offsetIndex()).as("Column %d offset index", i).isNull();
            assertThat(buffers.forColumn(i).columnIndex()).as("Column %d column index", i).isNull();
        }
    }

}
