/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright The original authors
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.morling.hardwood.parquet.ParquetFileReader;
import dev.morling.hardwood.parquet.column.ColumnReader;
import dev.morling.hardwood.parquet.metadata.ColumnChunk;
import dev.morling.hardwood.parquet.metadata.FileMetaData;
import dev.morling.hardwood.parquet.metadata.RowGroup;
import dev.morling.hardwood.parquet.schema.Column;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for reading dictionary encoded Parquet files.
 */
class DictionaryEncodingTest {

    @Test
    void testReadDictionaryEncodedFile() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/dictionary_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(parquetFile)) {
            assertThat(reader).isNotNull();

            FileMetaData metadata = reader.getFileMetaData();
            assertThat(metadata).isNotNull();
            assertThat(metadata.numRows()).isEqualTo(5);
            assertThat(metadata.rowGroups()).hasSize(1);

            // Verify schema
            assertThat(reader.getSchema().getColumnCount()).isEqualTo(2);

            // Read row group
            RowGroup rowGroup = metadata.rowGroups().get(0);
            assertThat(rowGroup.columns()).hasSize(2);

            // Read and verify 'id' column (PLAIN encoded)
            ColumnChunk idColumnChunk = rowGroup.columns().get(0);
            Column idColumn = reader.getSchema().getColumn(0);
            assertThat(idColumn.name()).isEqualTo("id");

            ColumnReader idReader = new ColumnReader(reader.getFile(), idColumn, idColumnChunk);
            List<Object> idValues = idReader.readAll();
            assertThat(idValues).hasSize(5);
            assertThat(idValues).containsExactly(1L, 2L, 3L, 4L, 5L);

            // Read and verify 'category' column (DICTIONARY encoded)
            ColumnChunk categoryColumnChunk = rowGroup.columns().get(1);
            Column categoryColumn = reader.getSchema().getColumn(1);
            assertThat(categoryColumn.name()).isEqualTo("category");

            // Verify dictionary encoding is used
            assertThat(categoryColumnChunk.metaData().encodings())
                    .contains(dev.morling.hardwood.parquet.Encoding.RLE_DICTIONARY);

            ColumnReader categoryReader = new ColumnReader(reader.getFile(), categoryColumn, categoryColumnChunk);
            List<Object> categoryValues = categoryReader.readAll();
            assertThat(categoryValues).hasSize(5);

            // Verify the exact values: ['A', 'B', 'A', 'C', 'B']
            assertThat(new String((byte[]) categoryValues.get(0))).isEqualTo("A");
            assertThat(new String((byte[]) categoryValues.get(1))).isEqualTo("B");
            assertThat(new String((byte[]) categoryValues.get(2))).isEqualTo("A");
            assertThat(new String((byte[]) categoryValues.get(3))).isEqualTo("C");
            assertThat(new String((byte[]) categoryValues.get(4))).isEqualTo("B");
        }
    }
}
