/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.compression;

import java.io.IOException;

import com.github.luben.zstd.Zstd;

/**
 * Decompressor for ZSTD compressed data.
 */
public class ZstdDecompressor implements Decompressor {

    @Override
    public byte[] decompress(byte[] compressed, int uncompressedSize) throws IOException {
        // ZSTD decompression
        byte[] uncompressed = new byte[uncompressedSize];
        long actualSize = Zstd.decompressByteArray(uncompressed, 0, uncompressedSize,
                compressed, 0, compressed.length);

        // Check for decompression errors
        if (Zstd.isError(actualSize)) {
            throw new IOException("ZSTD decompression failed: " + Zstd.getErrorName(actualSize));
        }

        // Verify the uncompressed size matches expectations
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
