/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.compression;

import java.io.IOException;

import com.aayushatharva.brotli4j.Brotli4jLoader;
import com.aayushatharva.brotli4j.decoder.Decoder;
import com.aayushatharva.brotli4j.decoder.DecoderJNI;
import com.aayushatharva.brotli4j.decoder.DirectDecompress;

/**
 * Decompressor for Brotli compressed data.
 */
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
    public byte[] decompress(byte[] compressed, int uncompressedSize) throws IOException {
        ensureInitialized();

        try {
            DirectDecompress result = Decoder.decompress(compressed);

            if (result.getResultStatus() != DecoderJNI.Status.DONE) {
                throw new IOException("Brotli decompression failed: " + result.getResultStatus());
            }

            byte[] decompressed = result.getDecompressedData();

            if (decompressed.length != uncompressedSize) {
                throw new IOException(
                        "Brotli decompression size mismatch: expected " + uncompressedSize +
                                ", got " + decompressed.length);
            }

            return decompressed;
        }
        catch (IOException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IOException("Brotli decompression failed", e);
        }
    }

    @Override
    public String getName() {
        return "BROTLI";
    }
}
