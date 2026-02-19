/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression.libdeflate;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.zip.GZIPOutputStream;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.internal.compression.GzipDecompressor;
import dev.hardwood.metadata.CompressionCodec;

import static org.assertj.core.api.Assertions.assertThat;

class LibdeflateDecompressorIT {

    private static LibdeflatePool pool;

    @BeforeAll
    static void checkAvailability() {
        Assumptions.assumeTrue(
                LibdeflateLoader.isAvailable(),
                "libdeflate not available on this system");
        pool = new LibdeflatePool();
    }

    @org.junit.jupiter.api.AfterAll
    static void cleanup() {
        if (pool != null) {
            pool.clear();
        }
    }

    @Test
    void decompressSimpleData() throws Exception {
        byte[] original = "Hello, libdeflate!".getBytes();
        byte[] compressed = gzipCompress(original);

        LibdeflateDecompressor decompressor = new LibdeflateDecompressor(pool);
        MappedByteBuffer buffer = wrapAsMappedBuffer(compressed);

        byte[] result = decompressor.decompress(buffer, original.length);

        assertThat(result).isEqualTo(original);
    }

    @Test
    void decompressLargeData() throws Exception {
        byte[] original = new byte[1024 * 1024];
        Random random = new Random(42);
        random.nextBytes(original);
        byte[] compressed = gzipCompress(original);

        LibdeflateDecompressor decompressor = new LibdeflateDecompressor(pool);
        MappedByteBuffer buffer = wrapAsMappedBuffer(compressed);

        byte[] result = decompressor.decompress(buffer, original.length);

        assertThat(result).isEqualTo(original);
    }

    @Test
    void matchesJavaImplementation() throws Exception {
        byte[] original = "Test data for comparison between libdeflate and Java implementation".getBytes();
        byte[] compressed = gzipCompress(original);

        MappedByteBuffer buffer1 = wrapAsMappedBuffer(compressed);
        MappedByteBuffer buffer2 = wrapAsMappedBuffer(compressed);

        byte[] libdeflateResult = new LibdeflateDecompressor(pool).decompress(buffer1, original.length);
        byte[] javaResult = new GzipDecompressor().decompress(buffer2, original.length);

        assertThat(libdeflateResult).isEqualTo(javaResult);
    }

    @Test
    void decompressConcatenatedGzipMembers() throws Exception {
        byte[] part1 = "First GZIP member".getBytes();
        byte[] part2 = "Second GZIP member".getBytes();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
            gzip.write(part1);
        }
        try (GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
            gzip.write(part2);
        }
        byte[] concatenated = baos.toByteArray();

        byte[] expectedOutput = new byte[part1.length + part2.length];
        System.arraycopy(part1, 0, expectedOutput, 0, part1.length);
        System.arraycopy(part2, 0, expectedOutput, part1.length, part2.length);

        MappedByteBuffer buffer = wrapAsMappedBuffer(concatenated);
        byte[] result = new LibdeflateDecompressor(pool).decompress(buffer, expectedOutput.length);

        assertThat(result).isEqualTo(expectedOutput);
    }

    @Test
    void factorySelectsLibdeflateWhenAvailable() {
        DecompressorFactory factory = new DecompressorFactory(pool);
        var decompressor = factory.getDecompressor(CompressionCodec.GZIP);

        assertThat(decompressor).isInstanceOf(LibdeflateDecompressor.class);
    }

    private static byte[] gzipCompress(byte[] data) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
            gzip.write(data);
        }
        return baos.toByteArray();
    }

    private static MappedByteBuffer wrapAsMappedBuffer(byte[] data) {
        // MappedByteBuffer is a direct buffer, so we create one and copy data into it
        // This simulates the memory-mapped file access pattern
        ByteBuffer direct = ByteBuffer.allocateDirect(data.length);
        direct.put(data);
        direct.flip();

        // We can't directly create a MappedByteBuffer, but we can use sun.misc.Unsafe
        // or just cast for testing. For test purposes, we'll use a workaround.
        // The decompressor only uses position(), remaining(), and get() methods
        // which work on any ByteBuffer. Cast will fail, so we need another approach.

        // Actually, we need to create a file and map it. For simplicity in tests,
        // let's just test that the code compiles and works with the actual implementation
        // by reading from a temporary file.

        return createMappedBuffer(data);
    }

    private static MappedByteBuffer createMappedBuffer(byte[] data) {
        try {
            Path tempFile = Files.createTempFile("libdeflate-test", ".gz");
            Files.write(tempFile, data);
            try (java.nio.channels.FileChannel channel = java.nio.channels.FileChannel.open(
                    tempFile, StandardOpenOption.READ)) {
                MappedByteBuffer buffer = channel.map(java.nio.channels.FileChannel.MapMode.READ_ONLY, 0, data.length);
                Files.delete(tempFile);
                return buffer;
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to create mapped buffer", e);
        }
    }
}
