/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression.lz4;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import dev.hardwood.internal.TestDecodeBuffers;
import dev.hardwood.internal.compression.CompressorFactory;
import dev.hardwood.internal.compression.Decompressor;
import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The LZ4 decompressors hold a page to its buffer's limit, so bytes past it (the next page of an
/// unsliced chunk) never complete a block.
class Lz4PageBoundsTest {

    private static byte[] body(int seed) {
        byte[] body = new byte[4_000];
        Random random = new Random(seed);
        for (int i = 0; i < body.length; i++) {
            body[i] = (byte) (random.nextInt(16) + 'a');
        }
        return body;
    }

    private static byte[] compress(byte[] body) {
        return new CompressorFactory().getCompressor(CompressionCodec.LZ4_RAW).compress(body, 0, body.length);
    }

    @ParameterizedTest
    @EnumSource(value = CompressionCodec.class, names = {"LZ4", "LZ4_RAW"})
    void decodesTwoPagesOfOneUnslicedBuffer(CompressionCodec codec) {
        byte[] first = body(1);
        byte[] second = body(2);
        byte[] firstPage = compress(first);
        byte[] secondPage = compress(second);
        ByteBuffer chunk = ByteBuffer.allocate(firstPage.length + secondPage.length).put(firstPage).put(secondPage);
        Decompressor decompressor = new DecompressorFactory(null).getDecompressor(codec);

        // The plain call returns the thread's buffer, so each result is copied before the next call
        byte[] decodedFirst = Arrays.copyOf(decompressor.decompress(
                chunk.duplicate().position(0).limit(firstPage.length), first.length, new byte[first.length], TestDecodeBuffers.create()), first.length);
        byte[] decodedSecond = Arrays.copyOf(decompressor.decompress(
                chunk.duplicate().position(firstPage.length), second.length, new byte[second.length], TestDecodeBuffers.create()), second.length);

        assertThat(decodedFirst).isEqualTo(first);
        assertThat(decodedSecond).isEqualTo(second);
    }

    @ParameterizedTest
    @EnumSource(value = CompressionCodec.class, names = {"LZ4", "LZ4_RAW"})
    void refusesABlockThatEndsPastThePageLimit(CompressionCodec codec) {
        byte[] body = body(1);
        byte[] page = compress(body);
        // The block's last five bytes sit past the limit, where an unsliced chunk holds them
        ByteBuffer truncated = ByteBuffer.wrap(page).limit(page.length - 5);
        Decompressor decompressor = new DecompressorFactory(null).getDecompressor(codec);

        String message = codec == CompressionCodec.LZ4
                ? "LZ4 decompression failed (tried both raw and Hadoop formats): raw: LZ4 raw block is malformed; "
                        + "Hadoop: "
                        + "Invalid LZ4 Hadoop block sizes: uncompressed=-251658364, compressed=1818388327"
                : "LZ4_RAW decompression failed: malformed block";
        assertThatThrownBy(() -> decompressor.decompress(truncated, body.length, new byte[body.length], TestDecodeBuffers.create()))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage(message);
    }
}
