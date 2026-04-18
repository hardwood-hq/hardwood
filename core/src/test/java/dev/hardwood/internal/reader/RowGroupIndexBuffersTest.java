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

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;

import static org.assertj.core.api.Assertions.assertThat;

class RowGroupIndexBuffersTest {

    private static final Path PAGE_INDEX_FILE = Paths.get("src/test/resources/page_index_test.parquet");
    private static final Path PLAIN_FILE = Paths.get("src/test/resources/plain_uncompressed.parquet");

    @Test
    void fetchesAllColumnIndexesInOneRead() throws Exception {
        CountingInputFile countingFile = new CountingInputFile(InputFile.of(PAGE_INDEX_FILE));
        countingFile.open();

        FileMetaData meta = ParquetMetadataReader.readMetadata(countingFile, null, null);
        RowGroup rowGroup = meta.rowGroups().get(0);

        int readsBefore = countingFile.readCount();
        RowGroupIndexBuffers buffers = RowGroupIndexBuffers.fetch(countingFile, rowGroup);
        int readsForFetch = countingFile.readCount() - readsBefore;

        // All column indexes should be fetched in exactly 1 readRange() call
        assertThat(readsForFetch).isEqualTo(1);

        // All 3 columns (id, value, category) should have offset index buffers
        for (int i = 0; i < rowGroup.columns().size(); i++) {
            ColumnIndexBuffers colBuffers = buffers.forColumn(i);
            assertThat(colBuffers).as("Column %d", i).isNotNull();
            assertThat(colBuffers.offsetIndex())
                    .as("Column %d offset index", i).isNotNull();
        }
    }

    @Test
    void returnsNullForFileWithoutIndexes() throws Exception {
        CountingInputFile countingFile = new CountingInputFile(InputFile.of(PLAIN_FILE));
        countingFile.open();

        FileMetaData meta = ParquetMetadataReader.readMetadata(countingFile, null, null);
        RowGroup rowGroup = meta.rowGroups().get(0);

        int readsBefore = countingFile.readCount();
        RowGroupIndexBuffers buffers = RowGroupIndexBuffers.fetch(countingFile, rowGroup);
        int readsForFetch = countingFile.readCount() - readsBefore;

        // No indexes to fetch — should not issue any readRange() calls
        assertThat(readsForFetch).isEqualTo(0);

        // All columns should return null (no indexes available)
        for (int i = 0; i < rowGroup.columns().size(); i++) {
            assertThat(buffers.forColumn(i))
                    .as("Column %d should have no index buffers", i).isNull();
        }
    }

}
