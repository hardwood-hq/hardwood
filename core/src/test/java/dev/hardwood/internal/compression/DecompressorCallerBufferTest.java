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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.xerial.snappy.Snappy;

import com.github.luben.zstd.ZstdCompressCtx;

import dev.hardwood.internal.TestDecodeBuffers;
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

    /// A buffer of exactly the page's size: nothing is written past it, so nothing fails on it.
    @ParameterizedTest
    @EnumSource(value = CompressionCodec.class,
            names = {"UNCOMPRESSED", "GZIP", "SNAPPY", "ZSTD", "LZ4_RAW", "BROTLI"})
    void fillsABufferOfExactlyThePagesSize(CompressionCodec codec) {
        byte[] body = body();
        ByteBuffer compressed = ByteBuffer.wrap(new CompressorFactory().getCompressor(codec).compress(body, 0,
                body.length));
        byte[] dest = new byte[body.length];
        Decompressor decompressor = new DecompressorFactory(null).getDecompressor(codec);

        assertThat(decompressor.decompress(compressed, body.length, dest, TestDecodeBuffers.create())).isSameAs(dest).isEqualTo(body);
    }

    /// `LZ4` is read in its raw block form and, failing that, in Hadoop's framing; Hardwood writes
    /// neither, so both streams are built here. The Hadoop page holds several blocks, the shape its
    /// framing exists for.
    @Test
    void writesAnLz4PageOfEitherFramingIntoTheCallersBuffer() {
        byte[] body = body();
        byte[] raw = new CompressorFactory().getCompressor(CompressionCodec.LZ4_RAW).compress(body, 0, body.length);
        byte[] hadoop = Lz4HadoopFrames.frame(body, 1_500);

        assertDecompressesInto(CompressionCodec.LZ4, ByteBuffer.wrap(raw), body);
        assertDecompressesInto(CompressionCodec.LZ4, ByteBuffer.wrap(hadoop), body);
        assertDecompressesInto(CompressionCodec.LZ4, PageBuffer.DIRECT.wrap(hadoop), body);
        byte[] dest = new byte[body.length];
        assertThat(new DecompressorFactory(null).getDecompressor(CompressionCodec.LZ4)
                .decompress(ByteBuffer.wrap(hadoop), body.length, dest, TestDecodeBuffers.create())).isSameAs(dest).isEqualTo(body);
    }

    static Stream<Arguments> streamsOfAnotherLength() {
        byte[] body = body();
        CompressorFactory compressors = new CompressorFactory();
        byte[] gzip = compressors.getCompressor(CompressionCodec.GZIP).compress(body, 0, body.length);
        byte[] lz4Raw = compressors.getCompressor(CompressionCodec.LZ4_RAW).compress(body, 0, body.length);
        byte[] brotli = compressors.getCompressor(CompressionCodec.BROTLI).compress(body, 0, body.length);
        byte[] zstd = compressors.getCompressor(CompressionCodec.ZSTD).compress(body, 0, body.length);
        byte[] zstdWithoutContentSize;
        try (ZstdCompressCtx ctx = new ZstdCompressCtx()) {
            // As a streaming writer leaves it, so the frame does not say how far it decodes
            zstdWithoutContentSize = ctx.setContentSize(false).compress(body);
        }
        byte[] hadoop = Lz4HadoopFrames.frame(body, 1_500);
        return Stream.of(
                Arguments.of("GZIP longer", CompressionCodec.GZIP, gzip, 1_000,
                        "GZIP data decompresses to more than the expected 1000 bytes"),
                Arguments.of("GZIP shorter", CompressionCodec.GZIP, gzip, 5_000,
                        "Decompressed size mismatch: expected 5000, got 4000"),
                Arguments.of("LZ4_RAW longer", CompressionCodec.LZ4_RAW, lz4Raw, 1_000,
                        "LZ4_RAW decompression failed: malformed block"),
                Arguments.of("LZ4_RAW shorter", CompressionCodec.LZ4_RAW, lz4Raw, 5_000,
                        "LZ4_RAW decompression size mismatch: expected 5000, got 4000"),
                Arguments.of("LZ4 raw longer", CompressionCodec.LZ4, lz4Raw, 1_000,
                        "LZ4 decompression failed (tried both raw and Hadoop formats): raw: LZ4 raw block is malformed; Hadoop: "
                                + "Invalid LZ4 Hadoop block sizes: uncompressed=-251685780, compressed=1818715243"),
                Arguments.of("LZ4 raw shorter", CompressionCodec.LZ4, lz4Raw, 5_000,
                        "LZ4 decompression failed (tried both raw and Hadoop formats): raw: LZ4 raw block decompresses to 4000 bytes, not the expected 5000; Hadoop: "
                                + "Invalid LZ4 Hadoop block sizes: uncompressed=-251685780, compressed=1818715243"),
                Arguments.of("LZ4 Hadoop longer", CompressionCodec.LZ4, hadoop, 2_000,
                        "LZ4 decompression failed (tried both raw and Hadoop formats): raw: LZ4 raw block is malformed; Hadoop: "
                                + "LZ4 Hadoop block ends at byte 3000, past the expected 2000 bytes"),
                Arguments.of("LZ4 Hadoop shorter", CompressionCodec.LZ4, hadoop, 5_000,
                        "LZ4 decompression failed (tried both raw and Hadoop formats): raw: LZ4 raw block is malformed; Hadoop: "
                                + "LZ4 Hadoop decompression size mismatch: expected 5000, got 4000"),
                Arguments.of("BROTLI longer", CompressionCodec.BROTLI, brotli, 1_000,
                        "Brotli decompression produced more than the expected 1000 bytes"),
                Arguments.of("BROTLI shorter", CompressionCodec.BROTLI, brotli, 5_000,
                        "Brotli decompression size mismatch: expected 5000, got 4000"),
                Arguments.of("ZSTD longer", CompressionCodec.ZSTD, zstd, 1_000,
                        "ZSTD decompression failed: Destination buffer is too small"),
                Arguments.of("ZSTD shorter", CompressionCodec.ZSTD, zstd, 5_000,
                        "ZSTD decompression size mismatch: expected 5000, got 4000"),
                Arguments.of("ZSTD longer, no content size", CompressionCodec.ZSTD, zstdWithoutContentSize, 1_000,
                        "ZSTD decompression failed: Destination buffer is too small"),
                Arguments.of("ZSTD shorter, no content size", CompressionCodec.ZSTD, zstdWithoutContentSize, 5_000,
                        "ZSTD decompression size mismatch: expected 5000, got 4000"))
                .flatMap(arguments -> Stream.of(PageBuffer.values()).map(pageBuffer -> {
                    Object[] values = arguments.get();
                    return Arguments.of(values[0] + ", " + pageBuffer, values[1], pageBuffer.wrap((byte[]) values[2]),
                            values[3], values[4]);
                }));
    }

    /// A stream that decodes to more or fewer bytes than the page declares is refused, and nothing
    /// is written past the declared size, even where the caller's buffer, sized for an earlier
    /// page, would hold the whole stream.
    @ParameterizedTest(name = "{0}")
    @MethodSource("streamsOfAnotherLength")
    void refusesAStreamOfAnotherLengthThanThePageDeclares(String label, CompressionCodec codec, ByteBuffer page,
                                                          int declared, String message) {
        byte[] dest = new byte[declared + 8_192];
        Arrays.fill(dest, SENTINEL);
        Decompressor decompressor = new DecompressorFactory(null).getDecompressor(codec);

        assertThatThrownBy(() -> decompressor.decompress(page, declared, dest, TestDecodeBuffers.create()))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage(message);
        assertThat(Arrays.copyOfRange(dest, declared, dest.length)).as("past the declared size")
                .containsOnly(SENTINEL);
    }

    /// Snappy decompresses a page natively, into the caller's buffer from the heap and into a
    /// direct buffer otherwise, and neither bounds the write; the stream's declared length is
    /// checked first, so a stream longer than the page declares fails without a byte written.
    @ParameterizedTest
    @EnumSource(PageBuffer.class)
    void refusesASnappyStreamLongerThanThePageDeclares(PageBuffer pageBuffer) throws Exception {
        byte[] body = body();
        ByteBuffer compressed = pageBuffer.wrap(Snappy.compress(body));
        byte[] dest = new byte[body.length + 64];
        Arrays.fill(dest, SENTINEL);
        Decompressor decompressor = new DecompressorFactory(null).getDecompressor(CompressionCodec.SNAPPY);

        assertThatThrownBy(() -> decompressor.decompress(compressed, 1_000, dest, TestDecodeBuffers.create()))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Snappy decompression size mismatch: expected 1000, got " + body.length);
        assertThat(dest).containsOnly(SENTINEL);
    }

    @ParameterizedTest
    @EnumSource(PageBuffer.class)
    void refusesASnappyStreamShorterThanThePageDeclares(PageBuffer pageBuffer) throws Exception {
        byte[] body = body();
        ByteBuffer compressed = pageBuffer.wrap(Snappy.compress(body));
        byte[] dest = new byte[body.length + 64];
        Decompressor decompressor = new DecompressorFactory(null).getDecompressor(CompressionCodec.SNAPPY);

        assertThatThrownBy(() -> decompressor.decompress(compressed, body.length + 10, dest, TestDecodeBuffers.create()))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Snappy decompression size mismatch: expected " + (body.length + 10) + ", got "
                        + body.length);
    }

    /// ZSTD decompresses a heap page natively into the caller's buffer, bounded to the declared
    /// size, so a stream longer than the page declares fails without a byte written past it.
    @Test
    void refusesAZstdHeapStreamLongerThanThePageDeclares() {
        byte[] body = body();
        ByteBuffer compressed = ByteBuffer.wrap(new CompressorFactory().getCompressor(CompressionCodec.ZSTD)
                .compress(body, 0, body.length));
        byte[] dest = new byte[body.length + 64];
        Arrays.fill(dest, SENTINEL);
        Decompressor decompressor = new DecompressorFactory(null).getDecompressor(CompressionCodec.ZSTD);

        assertThatThrownBy(() -> decompressor.decompress(compressed, 1_000, dest, TestDecodeBuffers.create()))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("ZSTD decompression failed: Destination buffer is too small");
        assertThat(Arrays.copyOfRange(dest, 1_000, dest.length)).containsOnly(SENTINEL);
    }

    private static void assertDecompressesInto(CompressionCodec codec, ByteBuffer compressed, byte[] body) {
        byte[] dest = new byte[body.length + 64];
        Arrays.fill(dest, SENTINEL);
        Decompressor decompressor = new DecompressorFactory(null).getDecompressor(codec);

        byte[] result = decompressor.decompress(compressed, body.length, dest, TestDecodeBuffers.create());

        assertThat(result).isSameAs(dest);
        assertThat(Arrays.copyOf(dest, body.length)).as("%s", codec).isEqualTo(body);
        assertThat(Arrays.copyOfRange(dest, body.length, dest.length)).as("%s past the page", codec)
                .containsOnly(SENTINEL);
    }
}
