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
import java.util.Arrays;
import java.util.stream.Stream;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Reads a file of each codec whose one data page header understates or overstates the page's
/// uncompressed size, from a mapped file, whose pages reach the decompressor in direct buffers,
/// and from the heap. Each read fails with [ParquetReadException], with the decompressor's
/// writes held to the declared size and its reads to the page.
class MisstatedPageSizeTest {

    /// 1000 `INT64` values in one `PLAIN` page
    private static final int VALUES = 1_000;
    private static final int UNCOMPRESSED_SIZE = VALUES * Long.BYTES;
    /// Both encode in the same number of varint bytes as the true size, so the header keeps its
    /// length
    private static final int UNDERSTATED = UNCOMPRESSED_SIZE / 2;
    private static final int OVERSTATED = UNCOMPRESSED_SIZE + 100;

    @TempDir
    static Path dir;

    static Stream<Arguments> misstatedPages() {
        return Stream.of(
                Arguments.of(CompressionCodec.GZIP, UNDERSTATED,
                        "GZIP data decompresses to more than the expected 4000 bytes"),
                Arguments.of(CompressionCodec.GZIP, OVERSTATED,
                        "Decompressed size mismatch: expected 8100, got 8000"),
                Arguments.of(CompressionCodec.SNAPPY, UNDERSTATED,
                        "Snappy decompression size mismatch: expected 4000, got 8000"),
                Arguments.of(CompressionCodec.SNAPPY, OVERSTATED,
                        "Snappy decompression size mismatch: expected 8100, got 8000"),
                Arguments.of(CompressionCodec.ZSTD, UNDERSTATED,
                        "ZSTD decompression failed: Destination buffer is too small"),
                Arguments.of(CompressionCodec.ZSTD, OVERSTATED,
                        "ZSTD decompression size mismatch: expected 8100, got 8000"),
                Arguments.of(CompressionCodec.LZ4_RAW, UNDERSTATED,
                        "LZ4_RAW decompression failed: malformed block"),
                Arguments.of(CompressionCodec.LZ4_RAW, OVERSTATED,
                        "LZ4_RAW decompression size mismatch: expected 8100, got 8000"),
                Arguments.of(CompressionCodec.BROTLI, UNDERSTATED,
                        "Brotli decompression produced more than the expected 4000 bytes"),
                Arguments.of(CompressionCodec.BROTLI, OVERSTATED,
                        "Brotli decompression size mismatch: expected 8100, got 8000"));
    }

    @ParameterizedTest(name = "{0} declaring {1}")
    @MethodSource("misstatedPages")
    void aMappedPageFails(CompressionCodec codec, int declared, String message) throws Exception {
        Path file = dir.resolve(codec.name().toLowerCase() + "_" + declared + ".parquet");
        Files.write(file, misstated(codec, declared));

        assertReadFails(InputFile.of(file), "[" + file.getFileName() + ": row group 0, column 'v', page 0] " + message);
    }

    @ParameterizedTest(name = "{0} declaring {1}")
    @MethodSource("misstatedPages")
    void aHeapPageFails(CompressionCodec codec, int declared, String message) throws Exception {
        InputFile file = InputFile.of(ByteBuffer.wrap(misstated(codec, declared)));

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

    /// A file of `codec` whose data page header declares `declared` uncompressed bytes.
    private static byte[] misstated(CompressionCodec codec, int declared) throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();
        long[] values = new long[VALUES];
        for (int i = 0; i < VALUES; i++) {
            values[i] = i * 31L;
        }
        WriterConfig config = WriterConfig.builder()
                .codec(codec)
                .encoding(ColumnEncoding.PLAIN)
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.longs("v", values));
        }
        byte[] file = InMemoryFiles.toByteArray(out);

        long pageOffset;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            pageOffset = reader.getFileMetaData().rowGroups().get(0).columns().get(0).metaData().dataPageOffset();
        }
        // The page header opens with field 1 (type, i32), then field 2 (uncompressed_page_size, i32)
        int at = Math.toIntExact(pageOffset);
        assertThat(file[at]).as("field 1 header").isEqualTo((byte) 0x15);
        at += 2;
        assertThat(file[at]).as("field 2 header").isEqualTo((byte) 0x15);
        at++;
        byte[] actual = zigzagVarint(UNCOMPRESSED_SIZE);
        byte[] replacement = zigzagVarint(declared);
        assertThat(ByteBuffer.wrap(file, at, actual.length)).isEqualTo(ByteBuffer.wrap(actual));
        assertThat(replacement).hasSameSizeAs(actual);
        System.arraycopy(replacement, 0, file, at, replacement.length);
        return file;
    }

    private static byte[] zigzagVarint(int value) {
        int zigzag = (value << 1) ^ (value >> 31);
        ByteBuffer out = ByteBuffer.allocate(5);
        while ((zigzag & ~0x7F) != 0) {
            out.put((byte) ((zigzag & 0x7F) | 0x80));
            zigzag >>>= 7;
        }
        out.put((byte) zigzag);
        return Arrays.copyOf(out.array(), out.position());
    }
}
