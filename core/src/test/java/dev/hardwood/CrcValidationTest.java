/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CrcValidationTest {

    @Test
    void testReadFileWithCrc() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_with_crc.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(3);

            try (ColumnReader idReader = reader.createColumnReader("id")) {
                assertThat(idReader.nextBatch()).isTrue();
                long[] values = idReader.getLongs();
                assertThat(values[0]).isEqualTo(1L);
                assertThat(values[1]).isEqualTo(2L);
                assertThat(values[2]).isEqualTo(3L);
                assertThat(idReader.nextBatch()).isFalse();
            }

            try (ColumnReader valueReader = reader.createColumnReader("value")) {
                assertThat(valueReader.nextBatch()).isTrue();
                long[] values = valueReader.getLongs();
                assertThat(values[0]).isEqualTo(100L);
                assertThat(values[1]).isEqualTo(200L);
                assertThat(values[2]).isEqualTo(300L);
                assertThat(valueReader.nextBatch()).isFalse();
            }
        }
    }

    @Test
    void testCorruptedDataDetected() throws Exception {
        Path source = Paths.get("src/test/resources/plain_with_crc.parquet");
        byte[] bytes = Files.readAllBytes(source);

        // Read metadata to find the corruption offset
        long corruptOffset;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(bytes)))) {
            ColumnMetaData meta = reader.getFileMetaData().rowGroups().get(0).columns().get(0).metaData();
            corruptOffset = meta.dataPageOffset() + meta.totalCompressedSize() - 1;
        }

        // Flip a byte to corrupt the data page
        bytes[(int) corruptOffset] ^= 0xFF;

        ByteBuffer corrupted = ByteBuffer.wrap(bytes);
        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(corrupted))) {
                try (ColumnReader colReader = reader.createColumnReader("id")) {
                    colReader.nextBatch();
                }
            }
        }).hasRootCauseInstanceOf(IOException.class)
          .rootCause().hasMessageContaining("CRC mismatch");
    }

    @Test
    void testReadDictionaryFileWithCrc() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/dictionary_with_crc.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(5);

            try (ColumnReader categoryReader = reader.createColumnReader("category")) {
                assertThat(categoryReader.nextBatch()).isTrue();
                String[] values = categoryReader.getStrings();
                assertThat(values[0]).isEqualTo("A");
                assertThat(values[1]).isEqualTo("B");
                assertThat(values[2]).isEqualTo("A");
                assertThat(values[3]).isEqualTo("C");
                assertThat(values[4]).isEqualTo("B");
                assertThat(categoryReader.nextBatch()).isFalse();
            }
        }
    }

    @Test
    void testCorruptedDictionaryPageDetected() throws Exception {
        Path source = Paths.get("src/test/resources/dictionary_with_crc.parquet");
        byte[] bytes = Files.readAllBytes(source);

        // Read metadata to find the dictionary page corruption offset
        long corruptOffset;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(bytes)))) {
            ColumnMetaData meta = reader.getFileMetaData().rowGroups().get(0).columns().get(1).metaData();
            corruptOffset = meta.dictionaryPageOffset() + meta.totalCompressedSize() - 1;
        }

        // Flip a byte to corrupt the dictionary page
        bytes[(int) corruptOffset] ^= 0xFF;

        ByteBuffer corrupted = ByteBuffer.wrap(bytes);
        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(corrupted))) {
                try (ColumnReader colReader = reader.createColumnReader("category")) {
                    colReader.nextBatch();
                }
            }
        }).hasRootCauseInstanceOf(IOException.class)
          .rootCause().hasMessageContaining("CRC mismatch");
    }

    @Test
    void testFilesWithoutCrc() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(3);

            try (ColumnReader idReader = reader.createColumnReader("id")) {
                assertThat(idReader.nextBatch()).isTrue();
                long[] values = idReader.getLongs();
                assertThat(values[0]).isEqualTo(1L);
                assertThat(values[1]).isEqualTo(2L);
                assertThat(values[2]).isEqualTo(3L);
                assertThat(idReader.nextBatch()).isFalse();
            }
        }
    }
}
