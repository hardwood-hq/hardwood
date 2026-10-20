/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.nio.ByteBuffer;

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.reader.ParquetReadException;

/// Decompressor for uncompressed data (passthrough).
public class UncompressedDecompressor implements Decompressor {

    @Override
    public byte[] decompress(ByteBuffer compressed, int uncompressedSize, byte[] dest, DecodeBuffers buffers) {
        int storedSize = compressed.remaining();
        requireStoredSize(storedSize, uncompressedSize);
        compressed.get(compressed.position(), dest, 0, storedSize);
        return dest;
    }

    /// Checks that an uncompressed page stores as many bytes as its header declares.
    ///
    /// @throws ParquetReadException if the sizes differ
    public static void requireStoredSize(int storedSize, int uncompressedSize) {
        if (storedSize != uncompressedSize) {
            throw new ParquetReadException(
                    "Uncompressed page size mismatch: expected " + uncompressedSize + ", got " + storedSize);
        }
    }

    @Override
    public String getName() {
        return "UNCOMPRESSED";
    }
}
