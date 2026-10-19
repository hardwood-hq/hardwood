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
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.metadata.CompressionCodec;

import static org.assertj.core.api.Assertions.assertThat;

/// A [Decompressor] reads its input and leaves it as it was: the same buffer decompresses again,
/// whichever codec and whichever kind of buffer holds the page.
class DecompressorInputTest {

    private static final CompressionCodec[] CODECS = {
            CompressionCodec.UNCOMPRESSED, CompressionCodec.GZIP, CompressionCodec.SNAPPY,
            CompressionCodec.ZSTD, CompressionCodec.LZ4, CompressionCodec.LZ4_RAW, CompressionCodec.BROTLI };

    static Stream<Arguments> codecsAndBuffers() {
        return Arrays.stream(CODECS)
                .flatMap(codec -> Arrays.stream(PageBuffer.values()).map(buffer -> Arguments.of(codec, buffer)));
    }

    private static byte[] body() {
        byte[] body = new byte[4_000];
        Random random = new Random(13);
        for (int i = 0; i < body.length; i++) {
            body[i] = (byte) (random.nextInt(16) + 'a');
        }
        return body;
    }

    /// `LZ4` reads raw LZ4 blocks first; the write side only produces them as `LZ4_RAW`.
    private static byte[] compress(CompressionCodec codec, byte[] body) {
        CompressionCodec writeCodec = codec == CompressionCodec.LZ4 ? CompressionCodec.LZ4_RAW : codec;
        return new CompressorFactory().getCompressor(writeCodec).compress(body, 0, body.length);
    }

    @ParameterizedTest
    @MethodSource("codecsAndBuffers")
    void decompressesTheSameBufferTwice(CompressionCodec codec, PageBuffer buffer) {
        byte[] body = body();
        ByteBuffer compressed = buffer.wrap(compress(codec, body));
        int position = compressed.position();
        int limit = compressed.limit();
        Decompressor decompressor = new DecompressorFactory(null).getDecompressor(codec);

        for (int attempt = 0; attempt < 2; attempt++) {
            byte[] result = decompressor.decompress(compressed, body.length);

            assertThat(Arrays.copyOf(result, body.length)).as("%s %s, attempt %d", codec, buffer, attempt)
                    .isEqualTo(body);
            assertThat(compressed.position()).as("%s %s position", codec, buffer).isEqualTo(position);
            assertThat(compressed.limit()).as("%s %s limit", codec, buffer).isEqualTo(limit);
        }
    }

    @ParameterizedTest
    @MethodSource("codecsAndBuffers")
    void decompressesTheSameBufferTwiceIntoTheCallersBuffer(CompressionCodec codec, PageBuffer buffer) {
        byte[] body = body();
        ByteBuffer compressed = buffer.wrap(compress(codec, body));
        int position = compressed.position();
        int limit = compressed.limit();
        Decompressor decompressor = new DecompressorFactory(null).getDecompressor(codec);

        for (int attempt = 0; attempt < 2; attempt++) {
            byte[] dest = new byte[body.length];

            decompressor.decompress(compressed, body.length, dest);

            assertThat(dest).as("%s %s, attempt %d", codec, buffer, attempt).isEqualTo(body);
            assertThat(compressed.position()).as("%s %s position", codec, buffer).isEqualTo(position);
            assertThat(compressed.limit()).as("%s %s limit", codec, buffer).isEqualTo(limit);
        }
    }

    /// `LZ4` in Hadoop's framing, several blocks to a page, through both entry points.
    @ParameterizedTest
    @EnumSource(PageBuffer.class)
    void decompressesTheSameHadoopLz4BufferTwice(PageBuffer buffer) {
        byte[] body = body();
        ByteBuffer compressed = buffer.wrap(Lz4HadoopFrames.frame(body, 1_500));
        int position = compressed.position();
        int limit = compressed.limit();
        Decompressor decompressor = new DecompressorFactory(null).getDecompressor(CompressionCodec.LZ4);

        for (int attempt = 0; attempt < 2; attempt++) {
            byte[] result = decompressor.decompress(compressed, body.length);
            assertThat(Arrays.copyOf(result, body.length)).as("%s, attempt %d", buffer, attempt).isEqualTo(body);

            byte[] dest = new byte[body.length];
            decompressor.decompress(compressed, body.length, dest);
            assertThat(dest).as("%s into the caller's buffer, attempt %d", buffer, attempt).isEqualTo(body);

            assertThat(compressed.position()).as("%s position", buffer).isEqualTo(position);
            assertThat(compressed.limit()).as("%s limit", buffer).isEqualTo(limit);
        }
    }
}
