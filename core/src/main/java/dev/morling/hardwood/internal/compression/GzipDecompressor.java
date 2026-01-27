/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.compression;

import java.io.IOException;
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
    public byte[] decompress(byte[] compressed, int uncompressedSize) throws IOException {
        byte[] result = new byte[uncompressedSize];
        int totalDecompressed = 0;
        int inputOffset = 0;

        // Handle concatenated GZIP members
        while (totalDecompressed < uncompressedSize && inputOffset < compressed.length) {
            int headerEnd = skipGzipHeader(compressed, inputOffset);

            Inflater inflater = new Inflater(true); // true = nowrap (raw deflate)
            try {
                inflater.setInput(compressed, headerEnd, compressed.length - headerEnd);
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
                inputOffset = compressed.length - inflater.getRemaining() + 8;
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

    private int skipGzipHeader(byte[] data, int start) throws IOException {
        if (data.length - start < 10) {
            throw new IOException("GZIP data too short for header");
        }

        // Check magic number
        int magic = (data[start] & 0xff) | ((data[start + 1] & 0xff) << 8);
        if (magic != GZIP_MAGIC) {
            throw new IOException("Not in GZIP format");
        }

        // Check compression method (must be 8 = deflate)
        if (data[start + 2] != 8) {
            throw new IOException("Unsupported compression method: " + data[start + 2]);
        }

        int flags = data[start + 3] & 0xff;
        int offset = start + 10; // Skip fixed header

        // Skip extra field if present
        if ((flags & FEXTRA) != 0) {
            if (offset + 2 > data.length) {
                throw new IOException("Truncated GZIP extra field");
            }
            int extraLen = (data[offset] & 0xff) | ((data[offset + 1] & 0xff) << 8);
            offset += 2 + extraLen;
        }

        // Skip file name if present
        if ((flags & FNAME) != 0) {
            while (offset < data.length && data[offset] != 0) {
                offset++;
            }
            offset++; // Skip null terminator
        }

        // Skip comment if present
        if ((flags & FCOMMENT) != 0) {
            while (offset < data.length && data[offset] != 0) {
                offset++;
            }
            offset++; // Skip null terminator
        }

        // Skip header CRC if present
        if ((flags & FHCRC) != 0) {
            offset += 2;
        }

        if (offset >= data.length) {
            throw new IOException("GZIP header extends beyond data");
        }

        return offset;
    }

    @Override
    public String getName() {
        return "GZIP";
    }
}
