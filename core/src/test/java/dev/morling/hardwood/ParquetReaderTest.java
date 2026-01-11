/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.morling.hardwood.metadata.ColumnChunk;
import dev.morling.hardwood.metadata.CompressionCodec;
import dev.morling.hardwood.metadata.FileMetaData;
import dev.morling.hardwood.metadata.PhysicalType;
import dev.morling.hardwood.metadata.RepetitionType;
import dev.morling.hardwood.metadata.RowGroup;
import dev.morling.hardwood.reader.ColumnReader;
import dev.morling.hardwood.reader.ParquetFileReader;
import dev.morling.hardwood.schema.ColumnSchema;
import dev.morling.hardwood.schema.FileSchema;

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

            // Read row group
            RowGroup rowGroup = metadata.rowGroups().get(0);
            assertThat(rowGroup.numRows()).isEqualTo(3);
            assertThat(rowGroup.columns()).hasSize(2);

            // Read and verify 'id' column
            ColumnChunk idColumnChunk = rowGroup.columns().get(0);
            assertThat(idColumnChunk.metaData().codec())
                    .isEqualTo(CompressionCodec.UNCOMPRESSED);
            assertThat(idColumnChunk.metaData().numValues()).isEqualTo(3);

            ColumnReader idReader = reader.getColumnReader(idColumn, idColumnChunk);
            List<Object> idValues = idReader.readAll();
            assertThat(idValues).hasSize(3);
            assertThat(idValues).containsExactly(1L, 2L, 3L);

            // Read and verify 'value' column
            ColumnChunk valueColumnChunk = rowGroup.columns().get(1);
            assertThat(valueColumnChunk.metaData().codec())
                    .isEqualTo(CompressionCodec.UNCOMPRESSED);
            assertThat(valueColumnChunk.metaData().numValues()).isEqualTo(3);

            ColumnReader valueReader = reader.getColumnReader(valueColumn, valueColumnChunk);
            List<Object> valueValues = valueReader.readAll();
            assertThat(valueValues).hasSize(3);
            assertThat(valueValues).containsExactly(100L, 200L, 300L);
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

            // Read row group
            RowGroup rowGroup = metadata.rowGroups().get(0);
            assertThat(rowGroup.numRows()).isEqualTo(3);
            assertThat(rowGroup.columns()).hasSize(2);

            // Read and verify 'id' column (all non-null)
            ColumnChunk idColumnChunk = rowGroup.columns().get(0);
            assertThat(idColumnChunk.metaData().codec())
                    .isEqualTo(CompressionCodec.UNCOMPRESSED);
            assertThat(idColumnChunk.metaData().numValues()).isEqualTo(3);

            ColumnReader idReader = reader.getColumnReader(idColumn, idColumnChunk);
            List<Object> idValues = idReader.readAll();
            assertThat(idValues).hasSize(3);
            assertThat(idValues).containsExactly(1L, 2L, 3L);

            // Read and verify 'name' column (with one null)
            ColumnChunk nameColumnChunk = rowGroup.columns().get(1);
            assertThat(nameColumnChunk.metaData().codec())
                    .isEqualTo(CompressionCodec.UNCOMPRESSED);
            assertThat(nameColumnChunk.metaData().numValues()).isEqualTo(3);

            ColumnReader nameReader = reader.getColumnReader(nameColumn, nameColumnChunk);
            List<Object> nameValues = nameReader.readAll();
            assertThat(nameValues).hasSize(3);

            // Verify the exact values: 'alice', null, 'charlie'
            assertThat(nameValues.get(0)).isInstanceOf(byte[].class);
            assertThat(new String((byte[]) nameValues.get(0))).isEqualTo("alice");
            assertThat(nameValues.get(1)).isNull();
            assertThat(nameValues.get(2)).isInstanceOf(byte[].class);
            assertThat(new String((byte[]) nameValues.get(2))).isEqualTo("charlie");
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

            // Verify column names and types
            ColumnSchema idColumn = schema.getColumn(0);
            assertThat(idColumn.name()).isEqualTo("id");
            assertThat(idColumn.type()).isEqualTo(PhysicalType.INT64);
            assertThat(idColumn.repetitionType()).isEqualTo(RepetitionType.REQUIRED);

            ColumnSchema valueColumn = schema.getColumn(1);
            assertThat(valueColumn.name()).isEqualTo("value");
            assertThat(valueColumn.type()).isEqualTo(PhysicalType.INT64);
            assertThat(valueColumn.repetitionType()).isEqualTo(RepetitionType.REQUIRED);

            // Read row group
            RowGroup rowGroup = metadata.rowGroups().get(0);
            assertThat(rowGroup.numRows()).isEqualTo(3);
            assertThat(rowGroup.columns()).hasSize(2);

            // Read and verify 'id' column - should be SNAPPY compressed
            ColumnChunk idColumnChunk = rowGroup.columns().get(0);
            assertThat(idColumnChunk.metaData().codec())
                    .isEqualTo(CompressionCodec.SNAPPY);
            assertThat(idColumnChunk.metaData().numValues()).isEqualTo(3);

            ColumnReader idReader = reader.getColumnReader(idColumn, idColumnChunk);
            List<Object> idValues = idReader.readAll();
            assertThat(idValues).hasSize(3);
            assertThat(idValues).containsExactly(1L, 2L, 3L);

            // Read and verify 'value' column - should be SNAPPY compressed
            ColumnChunk valueColumnChunk = rowGroup.columns().get(1);
            assertThat(valueColumnChunk.metaData().codec())
                    .isEqualTo(CompressionCodec.SNAPPY);
            assertThat(valueColumnChunk.metaData().numValues()).isEqualTo(3);

            ColumnReader valueReader = reader.getColumnReader(valueColumn, valueColumnChunk);
            List<Object> valueValues = valueReader.readAll();
            assertThat(valueValues).hasSize(3);
            assertThat(valueValues).containsExactly(100L, 200L, 300L);
        }
    }
}
