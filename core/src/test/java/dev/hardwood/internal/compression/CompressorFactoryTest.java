/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.metadata.CompressionCodec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Every codec the writer produces, compressed here and handed straight to the [Decompressor]
/// the read path resolves for the same codec. Agreement between the two sides is the floor:
/// what makes a compressed page readable *elsewhere* is the interop gate and the differential
/// suite, which read the produced files with parquet-java and DuckDB.
///
/// The two codecs that are refused are asserted alongside them, because a refusal that quietly
/// became a "not yet" is what this stage set out to remove.
class CompressorFactoryTest {

    private final CompressorFactory compressors = new CompressorFactory();
    private final DecompressorFactory decompressors = new DecompressorFactory(null);

    /// Every codec [CompressorFactory] produces, derived by subtracting the two it refuses
    /// rather than listed, so a codec added to [CompressionCodec] joins this sweep instead of
    /// being written and never compressed here.
    static Stream<CompressionCodec> writableCodecs() {
        return Stream.of(CompressionCodec.values())
                .filter(codec -> codec != CompressionCodec.LZ4 && codec != CompressionCodec.LZO);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("writableCodecs")
    void roundTripsACompressibleBody(CompressionCodec codec) throws IOException {
        byte[] body = compressible(64 * 1024);

        byte[] compressed = compressors.getCompressor(codec).compress(body, 0, body.length);

        assertThat(decompress(codec, compressed, body.length)).as("%s round trip", codec).isEqualTo(body);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("writableCodecs")
    void roundTripsAnIncompressibleBody(CompressionCodec codec) throws IOException {
        // Random bytes are the case where a codec's output can exceed its input, so this is what
        // holds each compressor's output buffer to the library's own bound rather than to the
        // uncompressed size.
        byte[] body = new byte[64 * 1024];
        new Random(20260820L).nextBytes(body);

        byte[] compressed = compressors.getCompressor(codec).compress(body, 0, body.length);

        assertThat(decompress(codec, compressed, body.length))
                .as("%s round trip of incompressible bytes", codec).isEqualTo(body);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("writableCodecs")
    void compressesOnlyTheRequestedSlice(CompressionCodec codec) throws IOException {
        // The writer hands the compressor one page's body inside the chunk's buffer, so a
        // compressor that ignored the offset or the length would still round-trip against
        // itself while writing the neighbouring pages' bytes into this one.
        byte[] body = compressible(8 * 1024);
        byte[] buffer = new byte[body.length + 2 * 1024];
        Arrays.fill(buffer, (byte) 0x5a);
        System.arraycopy(body, 0, buffer, 1024, body.length);

        byte[] compressed = compressors.getCompressor(codec).compress(buffer, 1024, body.length);

        assertThat(decompress(codec, compressed, body.length))
                .as("%s compressed the requested slice only", codec).isEqualTo(body);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("writableCodecs")
    void shrinksAHighlyCompressibleBody(CompressionCodec codec) throws IOException {
        // A single repeated byte is compressible by any codec by a wide margin, so a compressor
        // that returned its input unchanged — the one failure a round trip cannot see — fails
        // here instead. UNCOMPRESSED is the deliberate exception: storing the body verbatim is
        // what it is for.
        byte[] body = new byte[64 * 1024];
        Arrays.fill(body, (byte) 7);

        byte[] compressed = compressors.getCompressor(codec).compress(body, 0, body.length);

        if (codec == CompressionCodec.UNCOMPRESSED) {
            assertThat(compressed).as("UNCOMPRESSED stores the body verbatim").isEqualTo(body);
        }
        else {
            assertThat(compressed.length).as("%s compressed size", codec).isLessThan(body.length / 10);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("writableCodecs")
    void namesItsCodec(CompressionCodec codec) {
        assertThat(compressors.getCompressor(codec).getName()).as("%s compressor name", codec)
                .isEqualTo(codec.name());
    }

    @Test
    void refusesTheCodecsItWillNeverWrite() {
        // Both messages state why the codec is not written rather than implying a later
        // increment, and LZ4's points at the codec that replaced it.
        assertThatThrownBy(() -> compressors.getCompressor(CompressionCodec.LZ4))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("LZ4 uses the Hadoop framing the Parquet format has deprecated, so it is not "
                         + "written; use LZ4_RAW instead. Files already written with LZ4 are still read.")
                ;
        assertThatThrownBy(() -> compressors.getCompressor(CompressionCodec.LZO))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("LZO compression is not supported: there is no maintained JVM implementation "
                         + "under a licence this project can depend on. The read path refuses it for the "
                         + "same reason.");
    }

    @Test
    void refusesLz4WhileTheReadPathStillAcceptsIt() {
        // The asymmetry is deliberate: files written before the format deprecated the Hadoop
        // framing exist, so refusing to write it must not become refusing to read it. LZO has no
        // such split — it is refused in both directions.
        assertThat(decompressors.getDecompressor(CompressionCodec.LZ4).getName()).isEqualTo("LZ4");
        assertThatThrownBy(() -> decompressors.getDecompressor(CompressionCodec.LZO))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /// A body with the structure a page of values has — a small set of values recurring — so
    /// every codec has something to find in it.
    private static byte[] compressible(int size) {
        byte[] body = new byte[size];
        for (int i = 0; i < size; i++) {
            body[i] = (byte) (i % 17);
        }
        return body;
    }

    private byte[] decompress(CompressionCodec codec, byte[] compressed, int uncompressedSize) throws IOException {
        byte[] decompressed = decompressors.getDecompressor(codec)
                .decompress(ByteBuffer.wrap(compressed), uncompressedSize);
        // The decompressors hand back a reusable thread-local buffer that may be longer than the
        // body, so the comparison is over the body's own bytes.
        return Arrays.copyOf(decompressed, uncompressedSize);
    }
}
