/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.compression;

import java.io.IOException;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * Decompressor for LZ4_RAW compressed data (standard LZ4 block format).
 * <p>
 * This is used for the LZ4_RAW codec which uses standard LZ4 block compression
 * without any framing or headers.
 */
public class Lz4RawDecompressor implements Decompressor {

    private final LZ4FastDecompressor decompressor;

    public Lz4RawDecompressor() {
        this.decompressor = LZ4Factory.fastestInstance().fastDecompressor();
    }

    @Override
    public byte[] decompress(byte[] compressed, int uncompressedSize) throws IOException {
        try {
            byte[] uncompressed = new byte[uncompressedSize];
            int compressedLength = decompressor.decompress(compressed, 0, uncompressed, 0, uncompressedSize);

            // Verify all compressed bytes were consumed
            if (compressedLength != compressed.length) {
                throw new IOException(
                        "LZ4_RAW decompression did not consume all input: expected " + compressed.length +
                                " bytes, consumed " + compressedLength);
            }

            return uncompressed;
        }
        catch (Exception e) {
            throw new IOException("LZ4_RAW decompression failed: " + e.getMessage(), e);
        }
    }

    @Override
    public String getName() {
        return "LZ4_RAW";
    }
}
