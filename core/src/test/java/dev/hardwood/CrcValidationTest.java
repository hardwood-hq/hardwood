/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

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

        try (ParquetFileReader reader = ParquetFileReader.open(parquetFile)) {
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
        Path corrupted = Files.createTempFile("corrupted_crc_", ".parquet");
        try {
            Files.copy(source, corrupted, StandardCopyOption.REPLACE_EXISTING);

            // Derive the corruption offset from metadata: corrupt the last byte
            // of the first column chunk, which is always within page data.
            long corruptOffset;
            try (ParquetFileReader reader = ParquetFileReader.open(source)) {
                ColumnMetaData meta = reader.getFileMetaData().rowGroups().get(0).columns().get(0).metaData();
                corruptOffset = meta.dataPageOffset() + meta.totalCompressedSize() - 1;
            }

            flipByte(corrupted, corruptOffset);

            assertThatThrownBy(() -> {
                try (ParquetFileReader reader = ParquetFileReader.open(corrupted)) {
                    try (ColumnReader colReader = reader.createColumnReader("id")) {
                        colReader.nextBatch();
                    }
                }
            }).hasRootCauseInstanceOf(IOException.class)
              .rootCause().hasMessageContaining("CRC mismatch");
        }
        finally {
            Files.deleteIfExists(corrupted);
        }
    }

    @Test
    void testReadDictionaryFileWithCrc() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/dictionary_with_crc.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(parquetFile)) {
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
        Path corrupted = Files.createTempFile("corrupted_dict_crc_", ".parquet");
        try {
            Files.copy(source, corrupted, StandardCopyOption.REPLACE_EXISTING);

            // Corrupt the last byte of the dictionary column chunk (column 1),
            // which covers the dictionary page data.
            long corruptOffset;
            try (ParquetFileReader reader = ParquetFileReader.open(source)) {
                ColumnMetaData meta = reader.getFileMetaData().rowGroups().get(0).columns().get(1).metaData();
                corruptOffset = meta.dictionaryPageOffset() + meta.totalCompressedSize() - 1;
            }

            flipByte(corrupted, corruptOffset);

            assertThatThrownBy(() -> {
                try (ParquetFileReader reader = ParquetFileReader.open(corrupted)) {
                    try (ColumnReader colReader = reader.createColumnReader("category")) {
                        colReader.nextBatch();
                    }
                }
            }).hasRootCauseInstanceOf(IOException.class)
              .rootCause().hasMessageContaining("CRC mismatch");
        }
        finally {
            Files.deleteIfExists(corrupted);
        }
    }

    @Test
    void testFilesWithoutCrc() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(parquetFile)) {
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

    private static void flipByte(Path file, long offset) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(offset);
            byte original = raf.readByte();
            raf.seek(offset);
            raf.writeByte(original ^ 0xFF);
        }
    }
}
