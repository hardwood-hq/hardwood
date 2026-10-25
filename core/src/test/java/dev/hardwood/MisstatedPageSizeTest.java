/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Reads a file of each codec whose one data page header understates or overstates the page's
/// uncompressed size, from a mapped file, whose pages reach the decompressor in direct buffers,
/// and from the heap. Each read fails with [ParquetReadException], with the decompressor's
/// writes held to the declared size and its reads to the page.
class MisstatedPageSizeTest {

    @TempDir
    static Path dir;

    static Stream<Arguments> misstatedPages() {
        return Stream.of(
                Arguments.of(CompressionCodec.GZIP, MisstatedPageFile.UNDERSTATED,
                        "GZIP data decompresses to more than the expected 4000 bytes"),
                Arguments.of(CompressionCodec.GZIP, MisstatedPageFile.OVERSTATED,
                        "Decompressed size mismatch: expected 8100, got 8000"),
                Arguments.of(CompressionCodec.SNAPPY, MisstatedPageFile.UNDERSTATED,
                        "Snappy decompression size mismatch: expected 4000, got 8000"),
                Arguments.of(CompressionCodec.SNAPPY, MisstatedPageFile.OVERSTATED,
                        "Snappy decompression size mismatch: expected 8100, got 8000"),
                Arguments.of(CompressionCodec.ZSTD, MisstatedPageFile.UNDERSTATED,
                        "ZSTD decompression failed: Destination buffer is too small"),
                Arguments.of(CompressionCodec.ZSTD, MisstatedPageFile.OVERSTATED,
                        "ZSTD decompression size mismatch: expected 8100, got 8000"),
                Arguments.of(CompressionCodec.LZ4_RAW, MisstatedPageFile.UNDERSTATED,
                        "LZ4_RAW decompression failed: malformed block"),
                Arguments.of(CompressionCodec.LZ4_RAW, MisstatedPageFile.OVERSTATED,
                        "LZ4_RAW decompression size mismatch: expected 8100, got 8000"),
                Arguments.of(CompressionCodec.BROTLI, MisstatedPageFile.UNDERSTATED,
                        "Brotli decompression produced more than the expected 4000 bytes"),
                Arguments.of(CompressionCodec.BROTLI, MisstatedPageFile.OVERSTATED,
                        "Brotli decompression size mismatch: expected 8100, got 8000"));
    }

    @ParameterizedTest(name = "{0} declaring {1}")
    @MethodSource("misstatedPages")
    void aMappedPageFails(CompressionCodec codec, int declared, String message) throws Exception {
        Path file = dir.resolve(codec.name().toLowerCase() + "_" + declared + ".parquet");
        Files.write(file, MisstatedPageFile.build(codec, declared));

        assertReadFails(InputFile.of(file), "[" + file.getFileName() + ": row group 0, column 'v', page 0] " + message);
    }

    @ParameterizedTest(name = "{0} declaring {1}")
    @MethodSource("misstatedPages")
    void aHeapPageFails(CompressionCodec codec, int declared, String message) throws Exception {
        InputFile file = InputFile.of(ByteBuffer.wrap(MisstatedPageFile.build(codec, declared)));

        assertReadFails(file, "[<memory>: row group 0, column 'v', page 0] " + message);
    }

    private static void assertReadFails(InputFile file, String message) {
        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(file);
                 ColumnReader column = reader.columnReader("v")) {
                while (column.nextBatch()) {
                    // read to the misstated page
                }
            }
        })
                .isInstanceOf(ParquetReadException.class)
                .hasMessage(message);
    }
}
