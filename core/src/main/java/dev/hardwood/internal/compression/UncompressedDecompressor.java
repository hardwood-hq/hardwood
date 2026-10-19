/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.nio.ByteBuffer;

import dev.hardwood.reader.ParquetReadException;

/// Decompressor for uncompressed data (passthrough).
public class UncompressedDecompressor implements Decompressor {

    @Override
    public byte[] decompress(ByteBuffer compressed, int uncompressedSize, byte[] dest) {
        int storedSize = compressed.remaining();
        if (storedSize != uncompressedSize) {
            throw new ParquetReadException(
                    "Uncompressed page size mismatch: expected " + uncompressedSize + ", got " + storedSize);
        }
        compressed.get(compressed.position(), dest, 0, storedSize);
        return dest;
    }

    @Override
    public String getName() {
        return "UNCOMPRESSED";
    }
}
