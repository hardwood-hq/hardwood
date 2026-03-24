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

import org.xerial.snappy.Snappy;

/// Decompressor for Snappy compressed data.
public class SnappyDecompressor implements Decompressor {

    private static final ThreadLocal<ByteBuffer> DIRECT_BUFFER = new ThreadLocal<>();
    private static final ThreadLocal<byte[]> OUTPUT_BUFFER = new ThreadLocal<>();

    @Override
    public byte[] decompress(ByteBuffer compressed, int uncompressedSize) throws IOException {
        ByteBuffer directOutput = borrowDirectBuffer(uncompressedSize);
        int actualSize = Snappy.uncompress(compressed, directOutput);

        if (actualSize != uncompressedSize) {
            throw new IOException(
                    "Snappy decompression size mismatch: expected " + uncompressedSize + ", got " + actualSize);
        }

        byte[] output = borrowOutputBuffer(uncompressedSize);
        directOutput.rewind();
        directOutput.get(output, 0, uncompressedSize);
        return output;
    }

    private static ByteBuffer borrowDirectBuffer(int minSize) {
        ByteBuffer buf = DIRECT_BUFFER.get();
        if (buf == null || buf.capacity() < minSize) {
            buf = ByteBuffer.allocateDirect(minSize);
            DIRECT_BUFFER.set(buf);
        }
        buf.clear();
        return buf;
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
        return "SNAPPY";
    }
}
