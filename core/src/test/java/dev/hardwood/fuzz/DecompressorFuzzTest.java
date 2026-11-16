/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.fuzz;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.params.provider.MethodSource;

import com.code_intelligence.jazzer.junit.FuzzTest;

import dev.hardwood.internal.TestDecodeBuffers;
import dev.hardwood.internal.compression.Decompressor;
import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.internal.compression.libdeflate.LibdeflateLoader;
import dev.hardwood.internal.compression.libdeflate.LibdeflatePool;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.reader.ParquetReadException;

/// Decompresses arbitrary bytes with each decompressor a read uses, from a heap buffer or a direct
/// one, as a mapped file or a remote read hands it over. libdeflate's `GZIP` decompressor is
/// among them only where its Java 22 classes are loaded, which they are from the packaged jar
/// alone, so where the target runs under failsafe (`TESTING.md` § Fuzzing). Several decompressors
/// run native code, where a malformed stream can do worse than throw. A decompression either
/// returns or fails with [ParquetReadException], writes nothing past the declared size, and leaves
/// the compressed buffer's position and limit as they were; any other throwable, a write into the
/// guard bytes past the declared size, or a moved position or limit is a finding.
///
/// An input is a selector byte, the declared size in three bytes, little-endian, and the
/// compressed bytes. The selector's low bits are the codec's ordinal, [#LIBDEFLATE_BIT] picks
/// libdeflate's `GZIP` decompressor, and [#DIRECT_BIT] a direct buffer. The layout is the same in
/// every JVM, so an input checked in from one run replays against the same decompressor in
/// another; an input selecting a decompressor this JVM lacks is passed over.
class DecompressorFuzzTest {

    static final int HEADER_BYTES = 4;
    static final int DIRECT_BIT = 0x80;
    static final int LIBDEFLATE_BIT = 0x40;

    /// Bytes past the declared size, each [#GUARD_VALUE], that a decompression must leave as they are.
    private static final int GUARD_BYTES = 64;
    private static final byte GUARD_VALUE = 0x5A;

    /// The decompressors by selector without [#DIRECT_BIT], `null` where this JVM has none.
    private static final Decompressor[] DECOMPRESSORS = decompressors();

    @MethodSource("dev.hardwood.fuzz.Seeds#compressed")
    @FuzzTest
    void decompress(byte[] input) {
        if (input.length < HEADER_BYTES) {
            return;
        }
        Decompressor decompressor = DECOMPRESSORS[input[0] & ~DIRECT_BIT & 0xFF];
        if (decompressor == null) {
            return;
        }
        int size = (input[1] & 0xFF) | (input[2] & 0xFF) << 8 | (input[3] & 0xFF) << 16;
        ByteBuffer compressed = (input[0] & DIRECT_BIT) != 0
                ? ByteBuffer.allocateDirect(input.length - HEADER_BYTES)
                : ByteBuffer.allocate(input.length - HEADER_BYTES);
        compressed.put(input, HEADER_BYTES, input.length - HEADER_BYTES).flip();
        int position = compressed.position();
        int limit = compressed.limit();
        byte[] dest = new byte[size + GUARD_BYTES];
        Arrays.fill(dest, size, dest.length, GUARD_VALUE);
        try {
            decompressor.decompress(compressed, size, dest, TestDecodeBuffers.create());
        }
        catch (ParquetReadException e) {
            // not a stream of `size` bytes
        }
        if (compressed.position() != position || compressed.limit() != limit) {
            throw new AssertionError(decompressor.getName() + " moved the compressed buffer from position "
                    + position + ", limit " + limit + " to position " + compressed.position() + ", limit "
                    + compressed.limit());
        }
        for (int i = size; i < dest.length; i++) {
            if (dest[i] != GUARD_VALUE) {
                throw new AssertionError(decompressor.getName() + " wrote byte " + i + " past the declared size of "
                        + size);
            }
        }
    }

    /// The decompressors by selector without [#DIRECT_BIT]: each codec's at its ordinal, and
    /// libdeflate's `GZIP` decompressor at the `GZIP` ordinal with [#LIBDEFLATE_BIT].
    private static Decompressor[] decompressors() {
        Decompressor[] decompressors = new Decompressor[DIRECT_BIT];
        DecompressorFactory java = new DecompressorFactory(null);
        for (CompressionCodec codec : CompressionCodec.values()) {
            decompressors[codec.ordinal()] = decompressor(java, codec);
        }
        if (LibdeflateLoader.isAvailable()) {
            decompressors[LIBDEFLATE_BIT | CompressionCodec.GZIP.ordinal()] =
                    decompressor(new DecompressorFactory(new LibdeflatePool()), CompressionCodec.GZIP);
        }
        return decompressors;
    }

    /// `codec`'s decompressor from `factory`, `null` for `LZO` or a codec whose library is absent.
    private static Decompressor decompressor(DecompressorFactory factory, CompressionCodec codec) {
        try {
            return factory.getDecompressor(codec);
        }
        catch (UnsupportedOperationException e) {
            return null;
        }
    }

    /// The selectors, without [#DIRECT_BIT], of the decompressors this JVM has for `codec`.
    static List<Integer> selectors(CompressionCodec codec) {
        List<Integer> selectors = new ArrayList<>();
        for (int selector : new int[]{ codec.ordinal(), LIBDEFLATE_BIT | codec.ordinal() }) {
            if (DECOMPRESSORS[selector] != null) {
                selectors.add(selector);
            }
        }
        return selectors;
    }
}
