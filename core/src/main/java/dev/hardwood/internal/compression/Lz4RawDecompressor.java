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
    public byte[] decompress(MappedByteBuffer compressed, int uncompressedSize) throws IOException {
        try {
            // Decompress directly from MappedByteBuffer - no copying
            byte[] uncompressed = new byte[uncompressedSize];
            ByteBuffer dest = ByteBuffer.wrap(uncompressed);

            int compressedLength = compressed.remaining();
            int consumedLength = decompressor.decompress(compressed, 0, dest, 0, uncompressedSize);

            if (consumedLength != compressedLength) {
                throw new IOException(
                        "LZ4_RAW decompression did not consume all input: expected " + compressedLength +
                                " bytes, consumed " + consumedLength);
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
