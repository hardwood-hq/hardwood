/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression.lz4;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import dev.hardwood.internal.compression.Decompressor;
import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.metadata.CompressionCodec;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import static org.assertj.core.api.Assertions.assertThat;

class Lz4DecompressorIT {

    private static LZ4Factory lz4Factory;

    @BeforeAll
    static void checkAvailability() {
        Assumptions.assumeTrue(
                Lz4Loader.isAvailable(),
                "lz4-java not available on the classpath");
        lz4Factory = LZ4Factory.fastestInstance();
    }

    @Test
    void decompressRawLz4() throws Exception {
        byte[] original = "Hello, LZ4 raw compression!".getBytes();
        byte[] compressed = lz4Factory.fastCompressor().compress(original);

        Decompressor decompressor = factory().getDecompressor(CompressionCodec.LZ4);
        byte[] result = decompressor.decompress(createMappedBuffer(compressed), original.length);

        assertThat(result).isEqualTo(original);
    }

    @Test
    void decompressHadoopFormat() throws Exception {
        byte[] original = "Hello, Hadoop LZ4 format!".getBytes();
        LZ4Compressor compressor = lz4Factory.fastCompressor();
        byte[] compressedBlock = compressor.compress(original);

        // Build Hadoop-style frame: [4-byte uncompressed_len, BE][4-byte compressed_len, BE][data]
        ByteBuffer hadoop = ByteBuffer.allocate(8 + compressedBlock.length).order(ByteOrder.BIG_ENDIAN);
        hadoop.putInt(original.length);
        hadoop.putInt(compressedBlock.length);
        hadoop.put(compressedBlock);

        Decompressor decompressor = factory().getDecompressor(CompressionCodec.LZ4);
        byte[] result = decompressor.decompress(createMappedBuffer(hadoop.array()), original.length);

        assertThat(result).isEqualTo(original);
    }

    @Test
    void decompressHadoopFormatMultipleBlocks() throws Exception {
        byte[] part1 = "First block of data".getBytes();
        byte[] part2 = "Second block of data".getBytes();
        LZ4Compressor compressor = lz4Factory.fastCompressor();
        byte[] compressed1 = compressor.compress(part1);
        byte[] compressed2 = compressor.compress(part2);

        ByteBuffer hadoop = ByteBuffer.allocate(16 + compressed1.length + compressed2.length)
                .order(ByteOrder.BIG_ENDIAN);
        hadoop.putInt(part1.length);
        hadoop.putInt(compressed1.length);
        hadoop.put(compressed1);
        hadoop.putInt(part2.length);
        hadoop.putInt(compressed2.length);
        hadoop.put(compressed2);

        byte[] expected = new byte[part1.length + part2.length];
        System.arraycopy(part1, 0, expected, 0, part1.length);
        System.arraycopy(part2, 0, expected, part1.length, part2.length);

        Decompressor decompressor = factory().getDecompressor(CompressionCodec.LZ4);
        byte[] result = decompressor.decompress(createMappedBuffer(hadoop.array()), expected.length);

        assertThat(result).isEqualTo(expected);
    }

    @Test
    void decompressLargeData() throws Exception {
        byte[] original = new byte[1024 * 1024];
        new Random(42).nextBytes(original);
        byte[] compressed = lz4Factory.fastCompressor().compress(original);

        Decompressor decompressor = factory().getDecompressor(CompressionCodec.LZ4);
        byte[] result = decompressor.decompress(createMappedBuffer(compressed), original.length);

        assertThat(result).isEqualTo(original);
    }

    @Test
    void decompressLz4Raw() throws Exception {
        byte[] original = "Hello, LZ4_RAW compression!".getBytes();
        byte[] compressed = lz4Factory.fastCompressor().compress(original);

        Decompressor decompressor = factory().getDecompressor(CompressionCodec.LZ4_RAW);
        byte[] result = decompressor.decompress(createMappedBuffer(compressed), original.length);

        assertThat(result).isEqualTo(original);
    }

    @Test
    void decompressLz4RawLargeData() throws Exception {
        byte[] original = new byte[1024 * 1024];
        new Random(42).nextBytes(original);
        byte[] compressed = lz4Factory.fastCompressor().compress(original);

        Decompressor decompressor = factory().getDecompressor(CompressionCodec.LZ4_RAW);
        byte[] result = decompressor.decompress(createMappedBuffer(compressed), original.length);

        assertThat(result).isEqualTo(original);
    }

    @Test
    void factorySelectsCorrectDecompressors() {
        DecompressorFactory factory = factory();

        assertThat(factory.getDecompressor(CompressionCodec.LZ4)).isInstanceOf(Lz4Decompressor.class);
        assertThat(factory.getDecompressor(CompressionCodec.LZ4_RAW)).isInstanceOf(Lz4RawDecompressor.class);
    }

    private static DecompressorFactory factory() {
        return new DecompressorFactory(null);
    }

    private static MappedByteBuffer createMappedBuffer(byte[] data) {
        try {
            Path tempFile = Files.createTempFile("lz4-test", ".lz4");
            Files.write(tempFile, data);
            try (FileChannel channel = FileChannel.open(tempFile, StandardOpenOption.READ)) {
                MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, data.length);
                Files.delete(tempFile);
                return buffer;
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to create mapped buffer", e);
        }
    }
}
