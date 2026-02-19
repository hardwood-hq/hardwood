/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * Decompressor for GZIP-compressed data using Inflater directly for maximum performance.
 * <p>
 * This implementation bypasses GZIPInputStream to avoid intermediate buffering and copies,
 * decompressing directly into a pre-allocated output buffer.
 */
public class GzipDecompressor implements Decompressor {

    private static final int GZIP_MAGIC = 0x8b1f;
    private static final int FHCRC = 2;
    private static final int FEXTRA = 4;
    private static final int FNAME = 8;
    private static final int FCOMMENT = 16;

    @Override
    public byte[] decompress(MappedByteBuffer compressed, int uncompressedSize) throws IOException {
        byte[] result = new byte[uncompressedSize];
        int totalDecompressed = 0;

        // Handle concatenated GZIP members
        while (totalDecompressed < uncompressedSize && compressed.hasRemaining()) {
            int headerEnd = skipGzipHeader(compressed);

            Inflater inflater = new Inflater(true); // true = nowrap (raw deflate)
            try {
                // slice() with indices is absolute - must add current position
                MappedByteBuffer dataSlice = compressed.slice(compressed.position() + headerEnd, compressed.remaining() - headerEnd);
                inflater.setInput(dataSlice);

                while (totalDecompressed < uncompressedSize) {
                    int decompressed = inflater.inflate(result, totalDecompressed, uncompressedSize - totalDecompressed);
                    if (decompressed == 0) {
                        if (inflater.finished()) {
                            break;
                        }
                        if (inflater.needsInput()) {
                            throw new IOException("Truncated GZIP data");
                        }
                        if (inflater.needsDictionary()) {
                            throw new IOException("GZIP stream requires dictionary");
                        }
                    }
                    totalDecompressed += decompressed;
                }
                // Move past consumed input + 8-byte trailer for next member
                int consumed = dataSlice.capacity() - inflater.getRemaining() + 8;
                compressed.position(compressed.position() + headerEnd + consumed);
            }
            catch (DataFormatException e) {
                throw new IOException("GZIP decompression failed", e);
            }
            finally {
                inflater.end();
            }
        }

        if (totalDecompressed != uncompressedSize) {
            throw new IOException("Decompressed size mismatch: expected " + uncompressedSize +
                    " but got " + totalDecompressed);
        }

        return result;
    }

    private int skipGzipHeader(MappedByteBuffer buffer) throws IOException {
        int start = buffer.position();
        if (buffer.remaining() < 10) {
            throw new IOException("GZIP data too short for header");
        }

        // Check magic number
        int magic = (buffer.get(start) & 0xff) | ((buffer.get(start + 1) & 0xff) << 8);
        if (magic != GZIP_MAGIC) {
            throw new IOException("Not in GZIP format");
        }

        // Check compression method (must be 8 = deflate)
        if (buffer.get(start + 2) != 8) {
            throw new IOException("Unsupported compression method: " + buffer.get(start + 2));
        }

        int flags = buffer.get(start + 3) & 0xff;
        int offset = 10; // Skip fixed header

        // Skip extra field if present
        if ((flags & FEXTRA) != 0) {
            if (offset + 2 > buffer.remaining()) {
                throw new IOException("Truncated GZIP extra field");
            }
            int extraLen = (buffer.get(start + offset) & 0xff) | ((buffer.get(start + offset + 1) & 0xff) << 8);
            offset += 2 + extraLen;
        }

        // Skip file name if present
        if ((flags & FNAME) != 0) {
            while (offset < buffer.remaining() && buffer.get(start + offset) != 0) {
                offset++;
            }
            offset++; // Skip null terminator
        }

        // Skip comment if present
        if ((flags & FCOMMENT) != 0) {
            while (offset < buffer.remaining() && buffer.get(start + offset) != 0) {
                offset++;
            }
            offset++; // Skip null terminator
        }

        // Skip header CRC if present
        if ((flags & FHCRC) != 0) {
            offset += 2;
        }

        if (offset >= buffer.remaining()) {
            throw new IOException("GZIP header extends beyond data");
        }

        return offset;
    }

    @Override
    public String getName() {
        return "GZIP";
    }
}
