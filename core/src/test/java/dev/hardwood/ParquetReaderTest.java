/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.BitSet;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

class ParquetReaderTest {

    @Test
    void testReadPlainParquet() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(parquetFile)) {
            // Verify file metadata
            FileMetaData metadata = reader.getFileMetaData();
            assertThat(metadata).isNotNull();
            assertThat(metadata.version()).isEqualTo(2);
            assertThat(metadata.numRows()).isEqualTo(3);
            assertThat(metadata.rowGroups()).hasSize(1);

            // Verify schema
            FileSchema schema = reader.getFileSchema();
            assertThat(schema).isNotNull();
            assertThat(schema.getColumnCount()).isEqualTo(2);

            // Verify column names and types
            ColumnSchema idColumn = schema.getColumn(0);
            assertThat(idColumn.name()).isEqualTo("id");
            assertThat(idColumn.type()).isEqualTo(PhysicalType.INT64);
            assertThat(idColumn.repetitionType()).isEqualTo(RepetitionType.REQUIRED);

            ColumnSchema valueColumn = schema.getColumn(1);
            assertThat(valueColumn.name()).isEqualTo("value");
            assertThat(valueColumn.type()).isEqualTo(PhysicalType.INT64);
            assertThat(valueColumn.repetitionType()).isEqualTo(RepetitionType.REQUIRED);

            // Read and verify 'id' column using batch API
            try (ColumnReader idReader = reader.createColumnReader("id")) {
                assertThat(idReader.nextBatch()).isTrue();
                assertThat(idReader.getRecordCount()).isEqualTo(3);
                assertThat(idReader.getValueCount()).isEqualTo(3);

                long[] idValues = idReader.getLongs();
                assertThat(idValues[0]).isEqualTo(1L);
                assertThat(idValues[1]).isEqualTo(2L);
                assertThat(idValues[2]).isEqualTo(3L);

                // No nulls for required column
                assertThat(idReader.getElementNulls()).isNull();

                // Flat column
                assertThat(idReader.getNestingDepth()).isEqualTo(0);

                assertThat(idReader.nextBatch()).isFalse();
            }

            // Read and verify 'value' column using batch API
            try (ColumnReader valueReader = reader.createColumnReader("value")) {
                assertThat(valueReader.nextBatch()).isTrue();
                assertThat(valueReader.getRecordCount()).isEqualTo(3);

                long[] valueValues = valueReader.getLongs();
                assertThat(valueValues[0]).isEqualTo(100L);
                assertThat(valueValues[1]).isEqualTo(200L);
                assertThat(valueValues[2]).isEqualTo(300L);

                assertThat(valueReader.nextBatch()).isFalse();
            }
        }
    }

    @Test
    void testReadPlainParquetWithNulls() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed_with_nulls.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(parquetFile)) {
            // Verify file metadata
            FileMetaData metadata = reader.getFileMetaData();
            assertThat(metadata).isNotNull();
            assertThat(metadata.version()).isEqualTo(2);
            assertThat(metadata.numRows()).isEqualTo(3);
            assertThat(metadata.rowGroups()).hasSize(1);

            // Verify schema
            FileSchema schema = reader.getFileSchema();
            assertThat(schema).isNotNull();
            assertThat(schema.getColumnCount()).isEqualTo(2);

            // Verify column names and types
            ColumnSchema idColumn = schema.getColumn(0);
            assertThat(idColumn.name()).isEqualTo("id");
            assertThat(idColumn.type()).isEqualTo(PhysicalType.INT64);
            assertThat(idColumn.repetitionType()).isEqualTo(RepetitionType.REQUIRED);

            ColumnSchema nameColumn = schema.getColumn(1);
            assertThat(nameColumn.name()).isEqualTo("name");
            assertThat(nameColumn.type()).isEqualTo(PhysicalType.BYTE_ARRAY);
            assertThat(nameColumn.repetitionType()).isEqualTo(RepetitionType.OPTIONAL);

            // Read and verify 'id' column (all non-null)
            try (ColumnReader idReader = reader.createColumnReader("id")) {
                assertThat(idReader.nextBatch()).isTrue();
                assertThat(idReader.getRecordCount()).isEqualTo(3);

                long[] idValues = idReader.getLongs();
                assertThat(idValues[0]).isEqualTo(1L);
                assertThat(idValues[1]).isEqualTo(2L);
                assertThat(idValues[2]).isEqualTo(3L);

                assertThat(idReader.nextBatch()).isFalse();
            }

            // Read and verify 'name' column (with one null)
            try (ColumnReader nameReader = reader.createColumnReader("name")) {
                assertThat(nameReader.nextBatch()).isTrue();
                assertThat(nameReader.getRecordCount()).isEqualTo(3);

                byte[][] nameValues = nameReader.getBinaries();
                BitSet nulls = nameReader.getElementNulls();

                // Verify: 'alice', null, 'charlie'
                assertThat(nulls).isNotNull();
                assertThat(nulls.get(0)).isFalse();
                assertThat(new String(nameValues[0])).isEqualTo("alice");
                assertThat(nulls.get(1)).isTrue(); // null
                assertThat(nulls.get(2)).isFalse();
                assertThat(new String(nameValues[2])).isEqualTo("charlie");

                assertThat(nameReader.nextBatch()).isFalse();
            }
        }
    }

    @Test
    void testReadSnappyCompressedParquet() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_snappy.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(parquetFile)) {
            // Verify file metadata
            FileMetaData metadata = reader.getFileMetaData();
            assertThat(metadata).isNotNull();
            assertThat(metadata.version()).isEqualTo(2);
            assertThat(metadata.numRows()).isEqualTo(3);
            assertThat(metadata.rowGroups()).hasSize(1);

            // Verify schema
            FileSchema schema = reader.getFileSchema();
            assertThat(schema).isNotNull();
            assertThat(schema.getColumnCount()).isEqualTo(2);

            // Read and verify 'id' column - should be SNAPPY compressed
            assertThat(metadata.rowGroups().get(0).columns().get(0).metaData().codec())
                    .isEqualTo(CompressionCodec.SNAPPY);

            try (ColumnReader idReader = reader.createColumnReader("id")) {
                assertThat(idReader.nextBatch()).isTrue();
                assertThat(idReader.getRecordCount()).isEqualTo(3);

                long[] idValues = idReader.getLongs();
                assertThat(idValues[0]).isEqualTo(1L);
                assertThat(idValues[1]).isEqualTo(2L);
                assertThat(idValues[2]).isEqualTo(3L);

                assertThat(idReader.nextBatch()).isFalse();
            }

            // Read and verify 'value' column - should be SNAPPY compressed
            assertThat(metadata.rowGroups().get(0).columns().get(1).metaData().codec())
                    .isEqualTo(CompressionCodec.SNAPPY);

            try (ColumnReader valueReader = reader.createColumnReader("value")) {
                assertThat(valueReader.nextBatch()).isTrue();
                assertThat(valueReader.getRecordCount()).isEqualTo(3);

                long[] valueValues = valueReader.getLongs();
                assertThat(valueValues[0]).isEqualTo(100L);
                assertThat(valueValues[1]).isEqualTo(200L);
                assertThat(valueValues[2]).isEqualTo(300L);

                assertThat(valueReader.nextBatch()).isFalse();
            }
        }
    }

    @Test
    void testColumnReaderByIndex() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(parquetFile)) {
            // Read column by index
            try (ColumnReader idReader = reader.createColumnReader(0)) {
                assertThat(idReader.getColumnSchema().name()).isEqualTo("id");
                assertThat(idReader.nextBatch()).isTrue();
                assertThat(idReader.getRecordCount()).isEqualTo(3);

                long[] values = idReader.getLongs();
                assertThat(values[0]).isEqualTo(1L);
                assertThat(values[1]).isEqualTo(2L);
                assertThat(values[2]).isEqualTo(3L);

                assertThat(idReader.nextBatch()).isFalse();
            }
        }
    }

}
