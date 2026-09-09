/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Bytes that do not decode are the file being wrong, whatever the codec.
///
/// A [Decompressor] turns one buffer into another and reaches nothing, so it cannot fail at I/O
/// and does not declare [java.io.IOException]. That matters to a caller rather than only to a
/// signature: an `IOException` says another attempt may succeed, and a page whose compressed bytes
/// are corrupt will fail the same way every time.
///
/// Every codec is asserted rather than one, because each wraps a different library and the type
/// each of those raises is not something this project chooses.
class DecompressorContractTest {

    /// Long enough that no codec can mistake it for a valid empty stream, and fixed so a failure
    /// reproduces.
    private static byte[] garbage() {
        byte[] bytes = new byte[256];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (i * 31 + 7);
        }
        return bytes;
    }

    /// A decoder that ran and produced the wrong number of bytes is a different failure from one
    /// that refused the stream, and says so. [BrotliDecompressor] is where the two are easiest to
    /// confuse: its size checks sit next to a catch wide enough to swallow them, which would
    /// report a length disagreement as the generic refusal with its own message demoted to a
    /// cause.
    @Test
    void aLengthTheStreamDoesNotBackIsReportedAsItself() {
        byte[] body = new byte[512];
        byte[] compressed = new BrotliCompressor().compress(body, 0, body.length);
        Decompressor decompressor = new DecompressorFactory(null).getDecompressor(CompressionCodec.BROTLI);

        assertThatThrownBy(() -> decompressor.decompress(ByteBuffer.wrap(compressed), body.length + 1024))
                .as("the stream decoded; what disagrees is the length, and that is what to report")
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Brotli decompression size mismatch: expected 1536, got 512");
    }

    @ParameterizedTest
    @EnumSource(value = CompressionCodec.class,
            names = {"GZIP", "SNAPPY", "ZSTD", "LZ4", "LZ4_RAW", "BROTLI"})
    void bytesThatDoNotDecodeRaiseParquetReadException(CompressionCodec codec) {
        Decompressor decompressor = new DecompressorFactory(null).getDecompressor(codec);

        assertThatThrownBy(() -> decompressor.decompress(ByteBuffer.wrap(garbage()), 1024))
                .as("%s: corrupt bytes are the file's fault, and no retry changes them", codec)
                .isInstanceOf(ParquetReadException.class)
                .isNotInstanceOf(java.io.IOException.class);
    }
}
