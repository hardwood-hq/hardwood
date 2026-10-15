/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

/// Packs fixed-width values LSB-first, the bit layout shared by the RLE/bit-packing hybrid's
/// eight-value groups, DELTA_BINARY_PACKED's 32-value miniblocks and ALP's vectors.
///
/// Value `i` of a group occupies bits `[i·bitWidth, (i+1)·bitWidth)` counted from the least
/// significant bit of the first byte, which is what [RleBitPackingHybridDecoder],
/// [DeltaBinaryPackedDecoder] and [AlpDecoder] unpack. Every caller packs a whole number of
/// bytes, so no caller has a partial trailing byte to carry: eight values and 32 values at any
/// width are each a multiple of eight bits, and [AlpEncoder] pads a short last vector with zeros
/// to a multiple of eight values, keeping only the bytes the format counts.
public final class BitPacker {

    private BitPacker() {
    }

    /// Rejects a group this packer cannot write whole.
    ///
    /// Only whole bytes are flushed, so a group whose bits do not divide by eight would leave up
    /// to seven of them in the accumulator: the packed values would be short by a byte and
    /// unreadable, with nothing to say so. Every caller packs a multiple of eight bits at every
    /// width — that is a property of their group sizes, not of this method — so the check costs
    /// nothing and turns a future caller's silent truncation into a failure at the call.
    private static void checkGroup(int count, int bitWidth, int maxBitWidth) {
        if (bitWidth < 0 || bitWidth > maxBitWidth) {
            throw new IllegalArgumentException(
                    "bitWidth must be between 0 and " + maxBitWidth + " but was " + bitWidth);
        }
        if ((long) count * bitWidth % Byte.SIZE != 0) {
            throw new IllegalArgumentException("Packing " + count + " values at " + bitWidth
                    + " bits does not fill a whole number of bytes; this packer writes whole bytes only");
        }
    }

    /// The bytes `count` values at `bitWidth` bits occupy.
    ///
    /// @param count how many values are packed
    /// @param bitWidth bits per value
    /// @return the exact byte count
    public static int packedLength(int count, int bitWidth) {
        return (count * bitWidth + 7) / 8;
    }

    /// Packs `count` `int` values into `out`, starting at `outOffset`.
    ///
    /// A width of at most 32 keeps the whole of one value plus the at most seven bits carried
    /// over from the last inside a `long`, so this needs none of the boundary handling the
    /// `long` form does.
    ///
    /// @param values the values to pack
    /// @param offset index of the first value
    /// @param count how many values to pack
    /// @param bitWidth bits per value, 0–32
    /// @param out the destination, which must have [#packedLength] bytes free at `outOffset`
    /// @param outOffset where to write the first byte
    /// @return the number of bytes written
    public static int pack(int[] values, int offset, int count, int bitWidth, byte[] out, int outOffset) {
        checkGroup(count, bitWidth, Integer.SIZE);
        if (bitWidth == 0) {
            return 0;
        }
        long mask = bitWidth == 32 ? 0xFFFFFFFFL : (1L << bitWidth) - 1;
        long accumulator = 0;
        int bits = 0;
        int pos = outOffset;
        for (int i = 0; i < count; i++) {
            accumulator |= (values[offset + i] & mask) << bits;
            bits += bitWidth;
            while (bits >= 8) {
                out[pos++] = (byte) accumulator;
                accumulator >>>= 8;
                bits -= 8;
            }
        }
        return pos - outOffset;
    }

    /// Packs `count` `long` values into `out`, starting at `outOffset`.
    ///
    /// Widths above 57 can carry more of a value than the accumulator has room for once the
    /// leftover bits of the previous value are in it, so a value that does not fit is split: the
    /// low bits complete the accumulator, which is flushed whole, and the rest starts the next
    /// one. An `INT64` column whose deltas span the type reaches width 64, which is why this
    /// exists rather than the `int` form serving both.
    ///
    /// @param values the values to pack
    /// @param offset index of the first value
    /// @param count how many values to pack
    /// @param bitWidth bits per value, 0–64
    /// @param out the destination, which must have [#packedLength] bytes free at `outOffset`
    /// @param outOffset where to write the first byte
    /// @return the number of bytes written
    public static int pack(long[] values, int offset, int count, int bitWidth, byte[] out, int outOffset) {
        checkGroup(count, bitWidth, Long.SIZE);
        if (bitWidth == 0) {
            return 0;
        }
        long mask = bitWidth == 64 ? -1L : (1L << bitWidth) - 1;
        long accumulator = 0;
        int bits = 0;
        int pos = outOffset;
        for (int i = 0; i < count; i++) {
            long value = values[offset + i] & mask;
            // Java discards the bits shifted past bit 63, so this places exactly the low
            // `room` bits of the value whether or not the whole of it fits.
            accumulator |= value << bits;
            int room = 64 - bits;
            if (bitWidth > room) {
                for (int b = 0; b < Long.BYTES; b++) {
                    out[pos++] = (byte) accumulator;
                    accumulator >>>= 8;
                }
                accumulator = value >>> room;
                bits = bitWidth - room;
            }
            else {
                bits += bitWidth;
            }
            while (bits >= 8) {
                out[pos++] = (byte) accumulator;
                accumulator >>>= 8;
                bits -= 8;
            }
        }
        return pos - outOffset;
    }
}
