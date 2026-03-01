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

import org.junit.jupiter.api.Test;

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
            Files.copy(source, corrupted, java.nio.file.StandardCopyOption.REPLACE_EXISTING);

            // Corrupt a byte in the page data region of the first column.
            // Column 0's chunk starts at file offset 4 with a 67-byte header,
            // so the CRC-covered page data occupies offsets 71-94.
            try (RandomAccessFile raf = new RandomAccessFile(corrupted.toFile(), "rw")) {
                long corruptOffset = 80;
                raf.seek(corruptOffset);
                byte original = raf.readByte();
                raf.seek(corruptOffset);
                raf.writeByte(original ^ 0xFF);
            }

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
}
