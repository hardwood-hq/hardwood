/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.nio.ByteBuffer;

/// Decompressor for uncompressed data (passthrough).
public class UncompressedDecompressor implements Decompressor {

    private static final ThreadLocal<byte[]> OUTPUT_BUFFER = new ThreadLocal<>();

    @Override
    public byte[] decompress(ByteBuffer compressed, int uncompressedSize) {
        byte[] data = borrowOutputBuffer(compressed.remaining());
        compressed.get(data, 0, compressed.remaining());
        return data;
    }

    private static byte[] borrowOutputBuffer(int minSize) {
        byte[] buf = OUTPUT_BUFFER.get();
        if (buf == null || buf.length < minSize) {
            buf = new byte[minSize];
            OUTPUT_BUFFER.set(buf);
        }
        return buf;
    }

    @Override
    public String getName() {
        return "UNCOMPRESSED";
    }
}
