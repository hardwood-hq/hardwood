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

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.compression.libdeflate.LibdeflateLoader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for GZIP decompression.
 * <p>
 * On Java 21, uses Java's built-in Inflater for GZIP decompression.
 * On Java 25+, uses libdeflate via FFM for faster decompression.
 * <p>
 * Set system property {@code libdeflate.required=true} to assert that libdeflate
 * is available. This is used in CI on Java 25 to ensure libdeflate is properly
 * installed and working.
 */
class GzipDecompressionTest {

    private static final boolean LIBDEFLATE_REQUIRED =
            Boolean.getBoolean("libdeflate.required");

    @Test
    void readGzipCompressedFile() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/gzip_compressed.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            assertThat(fileReader.getFileSchema().getColumnCount()).isEqualTo(3);
            assertThat(fileReader.getFileMetaData().numRows()).isEqualTo(5);

            try (RowReader rowReader = fileReader.createRowReader()) {
                // Row 0
                assertThat(rowReader.hasNext()).isTrue();
                rowReader.next();
                assertThat(rowReader.getInt("id")).isEqualTo(1);
                assertThat(rowReader.getString("name")).isEqualTo("Alice");
                assertThat(rowReader.getLong("value")).isEqualTo(100);

                // Row 1
                assertThat(rowReader.hasNext()).isTrue();
                rowReader.next();
                assertThat(rowReader.getInt("id")).isEqualTo(2);
                assertThat(rowReader.getString("name")).isEqualTo("Bob");
                assertThat(rowReader.getLong("value")).isEqualTo(200);

                // Row 2
                assertThat(rowReader.hasNext()).isTrue();
                rowReader.next();
                assertThat(rowReader.getInt("id")).isEqualTo(3);
                assertThat(rowReader.getString("name")).isEqualTo("Charlie");
                assertThat(rowReader.getLong("value")).isEqualTo(300);

                // Row 3
                assertThat(rowReader.hasNext()).isTrue();
                rowReader.next();
                assertThat(rowReader.getInt("id")).isEqualTo(4);
                assertThat(rowReader.getString("name")).isEqualTo("Diana");
                assertThat(rowReader.getLong("value")).isEqualTo(400);

                // Row 4
                assertThat(rowReader.hasNext()).isTrue();
                rowReader.next();
                assertThat(rowReader.getInt("id")).isEqualTo(5);
                assertThat(rowReader.getString("name")).isEqualTo("Eve");
                assertThat(rowReader.getLong("value")).isEqualTo(500);

                // No more rows
                assertThat(rowReader.hasNext()).isFalse();
            }
        }
    }

    @Test
    void libdeflateIsAvailableWhenRequired() {
        if (LIBDEFLATE_REQUIRED) {
            assertThat(LibdeflateLoader.isAvailable())
                    .as("libdeflate is required but not available")
                    .isTrue();
        }
    }
}
