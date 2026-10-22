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

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.reader.ParquetReadException;

/// Decompressor for Snappy compressed data.
public class SnappyDecompressor implements Decompressor {

    private static final DecodeBuffers.Slot<ByteBuffer> DIRECT_OUTPUT_BUFFER = DecodeBuffers.slot();

    /// Decompresses into `dest`: heap to heap where the compressed page is on the heap, otherwise
    /// through direct buffers for the native codec and copied into `dest` once.
    @Override
    public byte[] decompress(ByteBuffer compressed, int uncompressedSize, byte[] dest, DecodeBuffers buffers) {
        int actualSize;
        try {
            // Declared by the codec, which uncompresses a buffer and reaches nothing.
            if (compressed.hasArray()) {
                actualSize = uncompressHeap(compressed.array(), compressed.arrayOffset() + compressed.position(),
                        compressed.remaining(), uncompressedSize, dest);
            }
            else if (uncompressedSize > DecodeBuffers.RETAINED_BYTES) {
                // A page above what a set of buffers keeps goes heap to heap: a direct buffer of
                // its own would be freed only when a collection finds it
                byte[] input = new byte[compressed.remaining()];
                compressed.duplicate().get(input);
                actualSize = uncompressHeap(input, 0, input.length, uncompressedSize, dest);
            }
            else {
                ByteBuffer directOutput = borrowDirectBuffer(buffers, uncompressedSize);
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

    private static int uncompressHeap(byte[] input, int offset, int length, int uncompressedSize, byte[] dest)
            throws IOException {
        // The stream's own length, checked before the native call writes into `dest`
        int declared = Snappy.uncompressedLength(input, offset, length);
        if (declared != uncompressedSize) {
            throw new ParquetReadException("Snappy decompression size mismatch: expected "
                    + uncompressedSize + ", got " + declared);
        }
        return Snappy.uncompress(input, offset, length, dest, 0);
    }

    private static ByteBuffer borrowDirectBuffer(DecodeBuffers buffers, int minSize) {
        ByteBuffer buf = buffers.get(DIRECT_OUTPUT_BUFFER);
        if (buf == null || buf.capacity() < minSize) {
            buf = ByteBuffer.allocateDirect(minSize);
            buffers.set(DIRECT_OUTPUT_BUFFER, buf);
        }
        buf.clear();
        return buf;
    }

    @Override
    public String getName() {
        return "SNAPPY";
    }
}
