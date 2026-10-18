/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.xerial.snappy.Snappy;

import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// [Decompressor#decompress(ByteBuffer, int, byte[])], which writes into a buffer the caller owns:
/// exactly the declared bytes, from a heap or a direct page, and nothing past them.
class DecompressorCallerBufferTest {

    private static final byte SENTINEL = 0x55;

    private static byte[] body() {
        byte[] body = new byte[4_000];
        Random random = new Random(11);
        for (int i = 0; i < body.length; i++) {
            body[i] = (byte) (random.nextInt(16) + 'a');
        }
        return body;
    }

    @ParameterizedTest
    @EnumSource(value = CompressionCodec.class,
            names = {"UNCOMPRESSED", "GZIP", "SNAPPY", "ZSTD", "LZ4_RAW", "BROTLI"})
    void writesTheDeclaredBytesOfAHeapPageIntoTheCallersBuffer(CompressionCodec codec) {
        byte[] body = body();
        ByteBuffer compressed = ByteBuffer.wrap(new CompressorFactory().getCompressor(codec).compress(body, 0,
                body.length));

        assertDecompressesInto(codec, compressed, body);
    }

    @ParameterizedTest
    @EnumSource(value = CompressionCodec.class,
            names = {"UNCOMPRESSED", "GZIP", "SNAPPY", "ZSTD", "LZ4_RAW", "BROTLI"})
    void writesTheDeclaredBytesOfADirectPageIntoTheCallersBuffer(CompressionCodec codec) {
        byte[] body = body();
        byte[] bytes = new CompressorFactory().getCompressor(codec).compress(body, 0, body.length);
        ByteBuffer compressed = ByteBuffer.allocateDirect(bytes.length).put(bytes).flip();

        assertDecompressesInto(codec, compressed, body);
    }

    /// A page in the middle of a heap buffer: the decompressor reads from the buffer's position.
    @ParameterizedTest
    @EnumSource(value = CompressionCodec.class, names = {"UNCOMPRESSED", "SNAPPY", "ZSTD"})
    void readsAHeapPageFromItsPosition(CompressionCodec codec) {
        byte[] body = body();
        byte[] bytes = new CompressorFactory().getCompressor(codec).compress(body, 0, body.length);
        byte[] backing = new byte[bytes.length + 20];
        System.arraycopy(bytes, 0, backing, 7, bytes.length);
        ByteBuffer compressed = ByteBuffer.wrap(backing, 7, bytes.length);

        assertDecompressesInto(codec, compressed, body);
    }

    /// Snappy decompresses a heap page natively into the caller's buffer, which bounds nothing;
    /// the stream's declared length is checked first, so a stream longer than the page declares
    /// fails without a byte written.
    @Test
    void refusesASnappyHeapStreamLongerThanThePageDeclares() throws Exception {
        byte[] body = body();
        ByteBuffer compressed = ByteBuffer.wrap(Snappy.compress(body));
        byte[] dest = new byte[body.length + 64];
        Arrays.fill(dest, SENTINEL);
        Decompressor decompressor = new DecompressorFactory(null).getDecompressor(CompressionCodec.SNAPPY);

        assertThatThrownBy(() -> decompressor.decompress(compressed, 1_000, dest))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Snappy decompression size mismatch: expected 1000, got " + body.length);
        assertThat(dest).containsOnly(SENTINEL);
    }

    @Test
    void refusesASnappyHeapStreamShorterThanThePageDeclares() throws Exception {
        byte[] body = body();
        ByteBuffer compressed = ByteBuffer.wrap(Snappy.compress(body));
        byte[] dest = new byte[body.length + 64];
        Decompressor decompressor = new DecompressorFactory(null).getDecompressor(CompressionCodec.SNAPPY);

        assertThatThrownBy(() -> decompressor.decompress(compressed, body.length + 10, dest))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Snappy decompression size mismatch: expected " + (body.length + 10) + ", got "
                        + body.length);
    }

    private static void assertDecompressesInto(CompressionCodec codec, ByteBuffer compressed, byte[] body) {
        byte[] dest = new byte[body.length + 64];
        Arrays.fill(dest, SENTINEL);
        Decompressor decompressor = new DecompressorFactory(null).getDecompressor(codec);

        byte[] result = decompressor.decompress(compressed, body.length, dest);

        assertThat(result).isSameAs(dest);
        assertThat(Arrays.copyOf(dest, body.length)).as("%s", codec).isEqualTo(body);
        assertThat(Arrays.copyOfRange(dest, body.length, dest.length)).as("%s past the page", codec)
                .containsOnly(SENTINEL);
    }
}
