/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.wasm;

import java.io.IOException;

/// Minimal decoder for the Snappy raw block format (the format Parquet uses for the SNAPPY
/// codec). Pure Java, no `sun.misc.Unsafe`, so it runs under GraalVM Web Image where the
/// `Unsafe`-based codec libraries do not.
///
/// The format is a little-endian base-128 varint holding the uncompressed length, followed by
/// a stream of elements. Each element starts with a tag byte whose low two bits select a
/// literal run or a back-reference copy.
final class SnappyBlock {

    private SnappyBlock() {
    }

    /// Decode `input` into `output`. Returns the number of bytes written.
    static int decompress(byte[] input, byte[] output) throws IOException {
        int ip = 0;

        // Uncompressed-length varint (validated against output capacity below).
        int expected = 0;
        int shift = 0;
        while (true) {
            if (ip >= input.length) {
                throw new IOException("truncated Snappy length header");
            }
            int b = input[ip++] & 0xff;
            expected |= (b & 0x7f) << shift;
            if ((b & 0x80) == 0) {
                break;
            }
            shift += 7;
        }
        if (expected > output.length) {
            throw new IOException("Snappy length " + expected + " exceeds output buffer " + output.length);
        }

        int op = 0;
        while (ip < input.length) {
            int tag = input[ip++] & 0xff;
            switch (tag & 0x03) {
                case 0 -> {
                    // Literal: length-1 in the upper 6 bits, or in the following 1–4 bytes.
                    int length = tag >>> 2;
                    if (length >= 60) {
                        int extra = length - 59;
                        length = 0;
                        for (int i = 0; i < extra; i++) {
                            length |= (input[ip++] & 0xff) << (8 * i);
                        }
                    }
                    length += 1;
                    System.arraycopy(input, ip, output, op, length);
                    ip += length;
                    op += length;
                }
                case 1 -> {
                    // Copy with 1-byte offset: 3-bit length, 11-bit offset.
                    int length = ((tag >>> 2) & 0x07) + 4;
                    int offset = ((tag >>> 5) & 0x07) << 8 | (input[ip++] & 0xff);
                    op = copy(output, op, offset, length);
                }
                case 2 -> {
                    // Copy with 2-byte offset.
                    int length = (tag >>> 2) + 1;
                    int offset = (input[ip++] & 0xff) | (input[ip++] & 0xff) << 8;
                    op = copy(output, op, offset, length);
                }
                default -> {
                    // Copy with 4-byte offset.
                    int length = (tag >>> 2) + 1;
                    int offset = (input[ip++] & 0xff)
                            | (input[ip++] & 0xff) << 8
                            | (input[ip++] & 0xff) << 16
                            | (input[ip++] & 0xff) << 24;
                    op = copy(output, op, offset, length);
                }
            }
        }
        return op;
    }

    /// Copy `length` bytes from `offset` back in `output`, one byte at a time so overlapping
    /// runs (offset < length) replicate correctly.
    private static int copy(byte[] output, int op, int offset, int length) throws IOException {
        if (offset <= 0 || offset > op) {
            throw new IOException("invalid Snappy copy offset " + offset + " at " + op);
        }
        int src = op - offset;
        for (int i = 0; i < length; i++) {
            output[op++] = output[src++];
        }
        return op;
    }
}
