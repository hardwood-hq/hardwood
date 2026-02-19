/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.io.IOException;
import java.nio.MappedByteBuffer;

import com.github.luben.zstd.Zstd;

/**
 * Decompressor for ZSTD compressed data.
 */
public class ZstdDecompressor implements Decompressor {

    @Override
    public byte[] decompress(MappedByteBuffer compressed, int uncompressedSize) throws IOException {
        // Decompress directly from MappedByteBuffer to byte[] - no copying
        byte[] uncompressed = new byte[uncompressedSize];
        int actualSize = Zstd.decompress(uncompressed, compressed);

        if (actualSize != uncompressedSize) {
            throw new IOException(
                    "ZSTD decompression size mismatch: expected " + uncompressedSize + ", got " + actualSize);
        }

        return uncompressed;
    }

    @Override
    public String getName() {
        return "ZSTD";
    }
}
