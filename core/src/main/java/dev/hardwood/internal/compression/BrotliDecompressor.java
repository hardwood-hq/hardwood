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

import com.aayushatharva.brotli4j.Brotli4jLoader;
import com.aayushatharva.brotli4j.decoder.BrotliInputStream;

/// Decompressor for Brotli compressed data.
public class BrotliDecompressor implements Decompressor {

    private static volatile boolean initialized = false;

    private static synchronized void ensureInitialized() throws IOException {
        if (!initialized) {
            try {
                Brotli4jLoader.ensureAvailability();
                initialized = true;
            }
            catch (UnsatisfiedLinkError e) {
                throw new IOException("Failed to load Brotli native library: " + e.getMessage(), e);
            }
        }
    }

    @Override
    public byte[] decompress(ByteBuffer compressed, int uncompressedSize) throws IOException {
        ensureInitialized();

        // Brotli4j has no direct ByteBuffer API, so extract to a byte array.
        byte[] compressedBytes = new byte[compressed.remaining()];
        compressed.duplicate().get(compressedBytes);

        // BrotliInputStream decodes through DecoderJNI directly. The one-shot
        // Decoder.decompress()/DirectDecompress path returns its output wrapped in a
        // Netty ByteBuf, which would otherwise force io.netty:netty-buffer onto the
        // classpath just to unwrap the bytes.
        byte[] decompressed;
        try (BrotliInputStream in = new BrotliInputStream(new ByteArrayInputStream(compressedBytes))) {
            decompressed = in.readNBytes(uncompressedSize);
            if (decompressed.length != uncompressedSize) {
                throw new IOException(
                        "Brotli decompression size mismatch: expected " + uncompressedSize +
                                ", got " + decompressed.length);
            }
            if (in.read() != -1) {
                throw new IOException(
                        "Brotli decompression produced more than the expected " + uncompressedSize + " bytes");
            }
        }
        catch (RuntimeException e) {
            throw new IOException("Brotli decompression failed", e);
        }

        return decompressed;
    }

    @Override
    public String getName() {
        return "BROTLI";
    }
}
