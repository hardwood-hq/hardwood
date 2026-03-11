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

    private static final ThreadLocal<ByteBuffer> DIRECT_INPUT_BUFFER = new ThreadLocal<>();
    private static final ThreadLocal<ByteBuffer> DIRECT_OUTPUT_BUFFER = new ThreadLocal<>();
    private static final ThreadLocal<byte[]> OUTPUT_BUFFER = new ThreadLocal<>();

    @Override
    public byte[] decompress(ByteBuffer compressed, int uncompressedSize) throws IOException {
        ByteBuffer directInput = toDirectBuffer(compressed, DIRECT_INPUT_BUFFER);
        ByteBuffer directOutput = borrowDirectBuffer(uncompressedSize);
        int actualSize = Snappy.uncompress(directInput, directOutput);

        if (actualSize != uncompressedSize) {
            throw new IOException(
                    "Snappy decompression size mismatch: expected " + uncompressedSize + ", got " + actualSize);
        }

        byte[] output = borrowOutputBuffer(uncompressedSize);
        directOutput.rewind();
        directOutput.get(output, 0, uncompressedSize);
        return output;
    }

    private static ByteBuffer toDirectBuffer(ByteBuffer src, ThreadLocal<ByteBuffer> cache) {
        if (src.isDirect()) {
            return src;
        }
        int size = src.remaining();
        ByteBuffer direct = borrowDirectBuffer(size, cache);
        direct.put(src);
        direct.flip();
        return direct;
    }

    private static ByteBuffer borrowDirectBuffer(int minSize) {
        return borrowDirectBuffer(minSize, DIRECT_OUTPUT_BUFFER);
    }

    private static ByteBuffer borrowDirectBuffer(int minSize, ThreadLocal<ByteBuffer> cache) {
        ByteBuffer buf = cache.get();
        if (buf == null || buf.capacity() < minSize) {
            buf = ByteBuffer.allocateDirect(minSize);
            cache.set(buf);
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
