/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import org.xerial.snappy.Snappy;

/**
 * Decompressor for Snappy compressed data.
 */
public class SnappyDecompressor implements Decompressor {

    @Override
    public byte[] decompress(MappedByteBuffer compressed, int uncompressedSize) throws IOException {
        // Snappy requires both buffers to be direct for ByteBuffer API, so allocate direct output
        ByteBuffer output = ByteBuffer.allocateDirect(uncompressedSize);
        int actualSize = Snappy.uncompress(compressed, output);

        if (actualSize != uncompressedSize) {
            throw new IOException(
                    "Snappy decompression size mismatch: expected " + uncompressedSize + ", got " + actualSize);
        }

        // Copy from direct buffer to byte[]
        byte[] uncompressed = new byte[uncompressedSize];
        output.rewind();
        output.get(uncompressed);
        return uncompressed;
    }

    @Override
    public String getName() {
        return "SNAPPY";
    }
}
