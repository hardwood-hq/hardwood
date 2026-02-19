/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.nio.MappedByteBuffer;

/**
 * Decompressor for uncompressed data (passthrough).
 */
public class UncompressedDecompressor implements Decompressor {

    @Override
    public byte[] decompress(MappedByteBuffer compressed, int uncompressedSize) {
        byte[] data = new byte[compressed.remaining()];
        compressed.get(data);
        return data;
    }

    @Override
    public String getName() {
        return "UNCOMPRESSED";
    }
}
