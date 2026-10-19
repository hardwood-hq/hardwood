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

import dev.hardwood.reader.ParquetReadException;

/// Decompressor for Snappy compressed data.
public class SnappyDecompressor implements Decompressor {

    private static final ThreadLocal<ByteBuffer> DIRECT_OUTPUT_BUFFER = new ThreadLocal<>();
    private static final ThreadLocal<byte[]> OUTPUT_BUFFER = new ThreadLocal<>();

    @Override
    public byte[] decompress(ByteBuffer compressed, int uncompressedSize) {
        return decompress(compressed, uncompressedSize, OutputBuffers.borrow(OUTPUT_BUFFER, uncompressedSize));
    }

    /// Decompresses into `dest`: heap to heap where the compressed page is on the heap, otherwise
    /// through direct buffers for the native codec and copied into `dest` once.
    @Override
    public byte[] decompress(ByteBuffer compressed, int uncompressedSize, byte[] dest) {
        int actualSize;
        try {
            // Declared by the codec, which uncompresses a buffer and reaches nothing.
            if (compressed.hasArray()) {
                byte[] input = compressed.array();
                int offset = compressed.arrayOffset() + compressed.position();
                int length = compressed.remaining();
                // The stream's own length, checked before the native call writes into `dest`
                int declared = Snappy.uncompressedLength(input, offset, length);
                if (declared != uncompressedSize) {
                    throw new ParquetReadException("Snappy decompression size mismatch: expected "
                            + uncompressedSize + ", got " + declared);
                }
                actualSize = Snappy.uncompress(input, offset, length, dest, 0);
            }
            else {
                ByteBuffer directOutput = borrowDirectBuffer(uncompressedSize);
                actualSize = Snappy.uncompress(DirectBuffers.ensureDirect(compressed), directOutput);
                if (actualSize == uncompressedSize) {
                    directOutput.rewind();
                    directOutput.get(dest, 0, uncompressedSize);
                }
            }
        }
        catch (IOException e) {
            throw new ParquetReadException("Snappy decompression failed", e);
        }

        if (actualSize != uncompressedSize) {
            throw new ParquetReadException(
                    "Snappy decompression size mismatch: expected " + uncompressedSize + ", got " + actualSize);
        }
        return dest;
    }

    private static ByteBuffer borrowDirectBuffer(int minSize) {
        ByteBuffer buf = DIRECT_OUTPUT_BUFFER.get();
        if (buf == null || buf.capacity() < minSize) {
            buf = ByteBuffer.allocateDirect(minSize);
            DIRECT_OUTPUT_BUFFER.set(buf);
        }
        buf.clear();
        return buf;
    }

    @Override
    public String getName() {
        return "SNAPPY";
    }
}
