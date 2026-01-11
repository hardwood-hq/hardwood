/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.compression;

import java.io.IOException;

import org.xerial.snappy.Snappy;

/**
 * Decompressor for Snappy compressed data.
 */
public class SnappyDecompressor implements Decompressor {

    @Override
    public byte[] decompress(byte[] compressed, int uncompressedSize) throws IOException {
        // Snappy decompression
        byte[] uncompressed = new byte[uncompressedSize];
        int actualSize = Snappy.uncompress(compressed, 0, compressed.length, uncompressed, 0);

        // Verify the uncompressed size matches expectations
        if (actualSize != uncompressedSize) {
            throw new IOException(
                    "Snappy decompression size mismatch: expected " + uncompressedSize + ", got " + actualSize);
        }

        return uncompressed;
    }

    @Override
    public String getName() {
        return "SNAPPY";
    }
}
