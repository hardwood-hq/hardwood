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

import com.github.luben.zstd.Zstd;

/// Decompressor for ZSTD compressed data.
public class ZstdDecompressor implements Decompressor {

    private static final ThreadLocal<byte[]> OUTPUT_BUFFER = new ThreadLocal<>();

    @Override
    public byte[] decompress(ByteBuffer compressed, int uncompressedSize) throws IOException {
        byte[] output = borrowOutputBuffer(uncompressedSize);
        int actualSize = Zstd.decompress(output, compressed);

        if (actualSize != uncompressedSize) {
            throw new IOException(
                    "ZSTD decompression size mismatch: expected " + uncompressedSize + ", got " + actualSize);
        }

        return output;
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
        return "ZSTD";
    }
}
