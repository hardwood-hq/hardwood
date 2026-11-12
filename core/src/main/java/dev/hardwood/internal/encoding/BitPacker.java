/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/// Packs fixed-width values LSB-first, the bit layout shared by the RLE/bit-packing hybrid's
/// eight-value groups, DELTA_BINARY_PACKED's 32-value miniblocks and ALP's vectors.
///
/// Value `i` of a group occupies bits `[i·bitWidth, (i+1)·bitWidth)` counted from the least
/// significant bit of the first byte, which is what [RleBitPackingHybridDecoder],
/// [DeltaBinaryPackedDecoder] and [AlpDecoder] unpack. A group whose bits end inside a byte
/// writes that byte too, its bits above the group's left zero.
///
/// Values collect in a `long` that is stored whole, little-endian, each time it fills; only the
/// last fewer than eight bytes are written one at a time.
public final class BitPacker {

    private static final VarHandle LONG_LE =
            MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    private BitPacker() {
    }

    /// Rejects a width the value type cannot hold.
    private static void checkWidth(int bitWidth, int maxBitWidth) {
        if (bitWidth < 0 || bitWidth > maxBitWidth) {
            throw new IllegalArgumentException(
                    "bitWidth must be between 0 and " + maxBitWidth + " but was " + bitWidth);
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
    /// @param values the values to pack
    /// @param offset index of the first value
    /// @param count how many values to pack
    /// @param bitWidth bits per value, 0–32
    /// @param out the destination, which must have [#packedLength] bytes free at `outOffset`
    /// @param outOffset where to write the first byte
    /// @return the number of bytes written
    public static int pack(int[] values, int offset, int count, int bitWidth, byte[] out, int outOffset) {
        checkWidth(bitWidth, Integer.SIZE);
        if (bitWidth == 0) {
            return 0;
        }
        long mask = bitWidth == 32 ? 0xFFFFFFFFL : (1L << bitWidth) - 1;
        long accumulator = 0;
        int bits = 0;
        int pos = outOffset;
        for (int i = 0; i < count; i++) {
            long value = values[offset + i] & mask;
            accumulator |= value << bits;
            bits += bitWidth;
            if (bits >= Long.SIZE) {
                LONG_LE.set(out, pos, accumulator);
                pos += Long.BYTES;
                bits -= Long.SIZE;
                accumulator = carry(value, bitWidth, bits);
            }
        }
        return writeTail(accumulator, bits, out, pos) - outOffset;
    }

    /// Packs `count` `long` values into `out`, starting at `outOffset`.
    ///
    /// An `INT64` column whose deltas span the type reaches width 64, which is why this exists
    /// rather than the `int` form serving both.
    ///
    /// @param values the values to pack
    /// @param offset index of the first value
    /// @param count how many values to pack
    /// @param bitWidth bits per value, 0–64
    /// @param out the destination, which must have [#packedLength] bytes free at `outOffset`
    /// @param outOffset where to write the first byte
    /// @return the number of bytes written
    public static int pack(long[] values, int offset, int count, int bitWidth, byte[] out, int outOffset) {
        checkWidth(bitWidth, Long.SIZE);
        if (bitWidth == 0) {
            return 0;
        }
        long mask = bitWidth == 64 ? -1L : (1L << bitWidth) - 1;
        long accumulator = 0;
        int bits = 0;
        int pos = outOffset;
        for (int i = 0; i < count; i++) {
            long value = values[offset + i] & mask;
            accumulator |= value << bits;
            bits += bitWidth;
            if (bits >= Long.SIZE) {
                LONG_LE.set(out, pos, accumulator);
                pos += Long.BYTES;
                bits -= Long.SIZE;
                accumulator = carry(value, bitWidth, bits);
            }
        }
        return writeTail(accumulator, bits, out, pos) - outOffset;
    }

    /// The high `overflow` bits of a `bitWidth`-bit value, the part a full accumulator had no room
    /// for. Java discards the bits shifted past bit 63, so the accumulator took exactly the rest.
    /// None when the value ended the accumulator exactly, where a shift by `bitWidth` would wrap
    /// at width 64.
    private static long carry(long value, int bitWidth, int overflow) {
        return overflow == 0 ? 0 : value >>> (bitWidth - overflow);
    }

    /// Writes the accumulator's `bits` remaining bits byte by byte, the last byte zero-padded, so
    /// the destination needs no room past the packed length.
    ///
    /// @return the position after the last byte written
    private static int writeTail(long accumulator, int bits, byte[] out, int pos) {
        for (; bits > 0; bits -= Byte.SIZE) {
            out[pos++] = (byte) accumulator;
            accumulator >>>= Byte.SIZE;
        }
        return pos;
    }
}
