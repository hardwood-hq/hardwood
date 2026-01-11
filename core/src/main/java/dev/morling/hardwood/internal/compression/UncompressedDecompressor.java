/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.compression;

/**
 * Decompressor for uncompressed data (passthrough).
 */
public class UncompressedDecompressor implements Decompressor {

    @Override
    public byte[] decompress(byte[] compressed, int uncompressedSize) {
        // No decompression needed - just return the data as-is
        return compressed;
    }

    @Override
    public String getName() {
        return "UNCOMPRESSED";
    }
}
