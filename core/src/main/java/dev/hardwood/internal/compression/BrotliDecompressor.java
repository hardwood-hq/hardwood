/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.aayushatharva.brotli4j.decoder.BrotliInputStream;

import dev.hardwood.reader.ParquetReadException;

/// Decompressor for Brotli compressed data.
public class BrotliDecompressor implements Decompressor {

    @Override
    public byte[] decompress(ByteBuffer compressed, int uncompressedSize) {
        return decompress(compressed, uncompressedSize, new byte[uncompressedSize]);
    }

    @Override
    public byte[] decompress(ByteBuffer compressed, int uncompressedSize, byte[] dest) {
        BrotliLoader.ensureLoaded();

        // BrotliInputStream decodes through DecoderJNI directly. The one-shot
        // Decoder.decompress()/DirectDecompress path returns its output wrapped in a
        // Netty ByteBuf, which would otherwise force io.netty:netty-buffer onto the
        // classpath just to unwrap the bytes.
        int decompressedLength;
        boolean trailingBytes;
        try (BrotliInputStream in = new BrotliInputStream(inputStream(compressed))) {
            decompressedLength = in.readNBytes(dest, 0, uncompressedSize);
            trailingBytes = in.read() != -1;
        }
        catch (IOException | RuntimeException e) {
            // The source is a byte array, so an IOException here is the decoder
            // rejecting the stream rather than anything reaching a file.
            throw new ParquetReadException("Brotli decompression failed", e);
        }

        // Outside the catch, and deliberately: the stream decoded, so a length that disagrees
        // with the header is what the bytes say rather than the decoder refusing them. Raised
        // inside, these would be caught by the arm above and reported as the generic failure
        // with their own message demoted to a cause.
        if (decompressedLength != uncompressedSize) {
            throw new ParquetReadException(
                    "Brotli decompression size mismatch: expected " + uncompressedSize +
                            ", got " + decompressedLength);
        }
        if (trailingBytes) {
            throw new ParquetReadException(
                    "Brotli decompression produced more than the expected " + uncompressedSize + " bytes");
        }

        return dest;
    }

    /// Brotli4j has no direct ByteBuffer API: a heap buffer is read where it lies, a direct one is
    /// extracted to a byte array first.
    private static ByteArrayInputStream inputStream(ByteBuffer compressed) {
        if (compressed.hasArray()) {
            return new ByteArrayInputStream(compressed.array(), compressed.arrayOffset() + compressed.position(),
                    compressed.remaining());
        }
        byte[] compressedBytes = new byte[compressed.remaining()];
        compressed.duplicate().get(compressedBytes);
        return new ByteArrayInputStream(compressedBytes);
    }

    @Override
    public String getName() {
        return "BROTLI";
    }
}
