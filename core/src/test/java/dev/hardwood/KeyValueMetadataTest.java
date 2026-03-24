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
import java.util.Map;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Tests for key-value metadata extraction from Parquet file footers.
class KeyValueMetadataTest {

    @Test
    void pyArrowFileContainsArrowSchemaMetadata() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            FileMetaData metadata = reader.getFileMetaData();
            Map<String, String> kvMetadata = metadata.keyValueMetadata();

            assertThat(kvMetadata).isNotNull();
            assertThat(kvMetadata).containsKey("ARROW:schema");
            assertThat(kvMetadata.get("ARROW:schema")).isNotEmpty();
        }
    }

    @Test
    void keyValueMetadataIsUnmodifiable() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            Map<String, String> kvMetadata = reader.getFileMetaData().keyValueMetadata();

            assertThatThrownBy(() -> kvMetadata.put("test", "value"))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Test
    void keyValueMetadataWithCustomEntries() throws Exception {
        // kv_metadata_test.parquet has custom key-value metadata:
        // app.version=1.2.3, writer.tool=hardwood-test, empty.value=''
        Path parquetFile = Paths.get("src/test/resources/kv_metadata_test.parquet");
        if (!parquetFile.toFile().exists()) {
            // File must be generated via simple-datagen.py; skip if not available
            return;
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            Map<String, String> kvMetadata = reader.getFileMetaData().keyValueMetadata();

            assertThat(kvMetadata).containsEntry("app.version", "1.2.3");
            assertThat(kvMetadata).containsEntry("writer.tool", "hardwood-test");
            assertThat(kvMetadata).containsEntry("empty.value", "");
            // Also contains ARROW:schema from pyarrow
            assertThat(kvMetadata).containsKey("ARROW:schema");
        }
    }

    @Test
    void columnLevelKeyValueMetadata() throws Exception {
        // column_kv_metadata_test.parquet has column-level kv metadata:
        //   id column: col.origin=primary-key
        //   name column: col.encoding=utf-8, col.source=user-input
        Path parquetFile = Paths.get("src/test/resources/column_kv_metadata_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            RowGroup rowGroup = reader.getFileMetaData().rowGroups().get(0);

            ColumnMetaData idColumn = rowGroup.columns().get(0).metaData();
            assertThat(idColumn.keyValueMetadata()).containsEntry("col.origin", "primary-key");
            assertThat(idColumn.keyValueMetadata()).hasSize(1);

            ColumnMetaData nameColumn = rowGroup.columns().get(1).metaData();
            assertThat(nameColumn.keyValueMetadata()).containsEntry("col.encoding", "utf-8");
            assertThat(nameColumn.keyValueMetadata()).containsEntry("col.source", "user-input");
            assertThat(nameColumn.keyValueMetadata()).hasSize(2);
        }
    }

    @Test
    void columnLevelKeyValueMetadataIsEmptyWhenAbsent() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            RowGroup rowGroup = reader.getFileMetaData().rowGroups().get(0);
            for (var chunk : rowGroup.columns()) {
                assertThat(chunk.metaData().keyValueMetadata()).isEmpty();
            }
        }
    }

    @Test
    void otherMetadataFieldsStillWork() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            FileMetaData metadata = reader.getFileMetaData();

            assertThat(metadata.version()).isEqualTo(2);
            assertThat(metadata.numRows()).isEqualTo(3);
            assertThat(metadata.rowGroups()).hasSize(1);
            assertThat(metadata.createdBy()).contains("parquet-cpp-arrow");
        }
    }
}
