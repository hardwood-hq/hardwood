/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import dev.hardwood.internal.compression.Decompressor;
import dev.hardwood.internal.compression.libdeflate.LibdeflateDecompressor;
import dev.hardwood.internal.compression.libdeflate.LibdeflateLoader;
import dev.hardwood.internal.compression.libdeflate.LibdeflatePool;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Integration tests for GZIP decompression.
///
/// On Java 21, uses Java's built-in Inflater for GZIP decompression.
/// On Java 25+, uses libdeflate via FFM for faster decompression.
///
/// Set system property `libdeflate.required=true` to assert that libdeflate
/// is available. This is used in CI on Java 25 to ensure libdeflate is properly
/// installed and working.
class GzipDecompressionTest {

    private static final HardwoodContextImpl CONTEXT = HardwoodContextImpl.create(1);

    private static final boolean LIBDEFLATE_REQUIRED =
            Boolean.getBoolean("libdeflate.required");

    @Test
    void readGzipCompressedFile() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/gzip_compressed.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            assertThat(fileReader.getFileSchema().getColumnCount()).isEqualTo(3);
            assertThat(fileReader.getFileMetaData().numRows()).isEqualTo(5);

            try (RowReader rowReader = fileReader.rowReader()) {
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

    /// libdeflate reads a page from position to limit and leaves both as they were, whether the page
    /// is a heap slice with an array offset or sits inside a larger direct buffer.
    @Test
    void libdeflateLeavesThePageBufferAsItWas() throws Exception {
        Assumptions.assumeTrue(LibdeflateLoader.isAvailable(), "libdeflate not available on this system");
        byte[] original = new byte[4_000];
        for (int i = 0; i < original.length; i++) {
            original[i] = (byte) ('a' + i % 13);
        }
        ByteArrayOutputStream gzip = new ByteArrayOutputStream();
        try (GZIPOutputStream out = new GZIPOutputStream(gzip)) {
            out.write(original);
        }
        byte[] page = gzip.toByteArray();
        byte[] backing = new byte[page.length + 20];
        Arrays.fill(backing, (byte) 0x55);
        System.arraycopy(page, 0, backing, 7, page.length);
        ByteBuffer slicedHeap = ByteBuffer.wrap(backing).position(3).slice().position(4).limit(4 + page.length);
        ByteBuffer positionedDirect = ByteBuffer.allocateDirect(backing.length).put(backing).position(7)
                .limit(7 + page.length);
        LibdeflatePool pool = new LibdeflatePool();
        try {
            Decompressor decompressor = new LibdeflateDecompressor(pool);
            for (ByteBuffer buffer : new ByteBuffer[]{ slicedHeap, positionedDirect }) {
                int position = buffer.position();
                for (int attempt = 0; attempt < 2; attempt++) {
                    byte[] dest = new byte[original.length];
                    decompress(decompressor, buffer, original.length, dest);

                    assertThat(dest).as("%s, attempt %d", buffer, attempt).isEqualTo(original);
                    assertThat(buffer.position()).as("%s position", buffer).isEqualTo(position);
                    assertThat(buffer.limit()).as("%s limit", buffer).isEqualTo(position + page.length);
                }
            }
        }
        finally {
            pool.clear();
        }
    }

    /// Decompresses on a context's pool thread, which holds the buffers a decompressor takes.
    private static byte[] decompress(Decompressor decompressor, ByteBuffer compressed, int uncompressedSize,
                                     byte[] dest) {
        return CONTEXT.decode(buffers -> decompressor.decompress(compressed, uncompressedSize, dest, buffers));
    }

    private static byte[] body(int length, int seed) {
        byte[] body = new byte[length];
        for (int i = 0; i < length; i++) {
            body[i] = (byte) ('a' + (i * seed) % 13);
        }
        return body;
    }

    private static byte[] member(byte[] body) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (GZIPOutputStream out = new GZIPOutputStream(bytes)) {
            out.write(body);
        }
        return bytes.toByteArray();
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.writeBytes(part);
        }
        return out.toByteArray();
    }

    /// The rule `GzipPageTailTest` pins for the `Inflater` path, with the same messages: members are
    /// read until the declared size is produced and the bytes after it are ignored; a member that
    /// inflates past the declared size, or whose CRC32 or length trailer disagrees with its output,
    /// fails.
    @Test
    void libdeflateReadsAPageTailAsTheInflaterDoes() throws Exception {
        Assumptions.assumeTrue(LibdeflateLoader.isAvailable(), "libdeflate not available on this system");
        byte[] body = body(4_000, 1);
        byte[] garbage = new byte[16];
        Arrays.fill(garbage, (byte) 0x55);
        byte[] first = body(2_000, 1);
        byte[] second = body(2_000, 3);
        byte[] twoMembers = concat(member(first), member(second));
        LibdeflatePool pool = new LibdeflatePool();
        try {
            Decompressor decompressor = new LibdeflateDecompressor(pool);

            assertThat(decompress(decompressor, ByteBuffer.wrap(concat(member(body), garbage)), body.length,
                    new byte[body.length])).as("bytes after a complete page").isEqualTo(body);
            assertThat(decompress(decompressor, ByteBuffer.wrap(concat(member(body), member(second))),
                    body.length, new byte[body.length])).as("a member after a complete page").isEqualTo(body);
            assertThat(decompress(decompressor, ByteBuffer.wrap(twoMembers), 4_000, new byte[4_000]))
                    .as("members until the declared size").isEqualTo(concat(first, second));
            assertThatThrownBy(() -> decompress(decompressor, ByteBuffer.wrap(twoMembers), 3_000,
                    new byte[3_000]))
                    .isInstanceOf(ParquetReadException.class)
                    .hasMessage("GZIP data decompresses to more than the expected 3000 bytes");
            assertThatThrownBy(() -> decompress(decompressor, ByteBuffer.wrap(member(body)), 5_000,
                    new byte[5_000]))
                    .isInstanceOf(ParquetReadException.class)
                    .hasMessage("Decompressed size mismatch: expected 5000, got 4000");
            // A trailer that disagrees with the output: libdeflate does not say which field
            for (int fromEnd : new int[]{ 8, 4 }) {
                byte[] corrupt = member(body);
                corrupt[corrupt.length - fromEnd] ^= 0x01;
                assertThatThrownBy(() -> decompress(decompressor, ByteBuffer.wrap(corrupt), body.length,
                        new byte[body.length]))
                        .as("trailer byte %d from the end", fromEnd)
                        .isInstanceOf(ParquetReadException.class)
                        .hasMessage("libdeflate decompression failed: Bad data (invalid or corrupt compressed data)");
            }
        }
        finally {
            pool.clear();
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
