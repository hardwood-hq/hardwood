/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

/// Decodes the deprecated `BIT_PACKED` encoding a `DATA_PAGE` may declare for its repetition or
/// definition levels.
///
/// Unlike the bit-packed runs of the RLE/bit-packing hybrid, which [RleBitPackingHybridDecoder]
/// reads, each value's bits run from its most significant one down, and they fill each byte from
/// its most significant bit. The section has no header and no length prefix: `count` values at
/// `bitWidth` bits occupy [BitPacker#packedLength] bytes, the last one padded with zeros.
public final class BitPackedLevelDecoder {

    private BitPackedLevelDecoder() {
    }

    /// Decodes `count` levels of `bitWidth` bits starting at `data[offset]` into `out[0, count)`.
    ///
    /// @param bitWidth bits per level, 1–31; the caller has checked that the section's
    ///        [BitPacker#packedLength] bytes lie within `data`
    public static void decode(byte[] data, int offset, int bitWidth, int[] out, int count) {
        int mask = (1 << bitWidth) - 1;
        // Holds fewer than bitWidth + 8 unconsumed bits; bits above them are masked off
        long buffer = 0;
        int buffered = 0;
        int pos = offset;
        for (int i = 0; i < count; i++) {
            while (buffered < bitWidth) {
                buffer = buffer << 8 | (data[pos++] & 0xFF);
                buffered += 8;
            }
            buffered -= bitWidth;
            out[i] = (int) (buffer >>> buffered) & mask;
        }
    }
}
