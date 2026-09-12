/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

import dev.hardwood.internal.conversion.LogicalTypeConverter;

/// Byte array comparison in the orders a binary column sorts in: unsigned lexicographic (for
/// BYTE_ARRAY), signed two's complement (for DECIMAL columns of either byte-array type), and the
/// instant a legacy `INT96` timestamp encodes.
///
/// Both the reader's predicate evaluation and the writer's statistics collection compare through
/// here, so a chunk's bounds and a predicate over them cannot come to disagree.
public final class BinaryComparator {

    private static final VarHandle LONG_LE =
            MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle INT_LE =
            MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    private static final long NANOS_PER_DAY = 86_400_000_000_000L;

    private BinaryComparator() {
    }

    /// Compare two byte arrays lexicographically (unsigned).
    /// This matches Parquet's binary comparison semantics for BYTE_ARRAY statistics.
    ///
    /// @return negative if a < b, zero if equal, positive if a > b
    public static int compareUnsigned(byte[] a, byte[] b) {
        return Arrays.compareUnsigned(a, b);
    }

    /// Compare two byte arrays as big-endian two's complement signed values, the order a
    /// `DECIMAL`'s unscaled value sorts in.
    ///
    /// The two may differ in length. A `FIXED_LEN_BYTE_ARRAY` decimal pads every value to the
    /// column width, so its values always match; a `BYTE_ARRAY` decimal stores each value in the
    /// fewest bytes that hold it, so `127` is one byte and `128` is two. Length is not magnitude
    /// under that encoding — a byte-wise comparison would rank `0x7F` above `0x00 0x80` — so the
    /// shorter value is sign-extended to the longer before the bytes are compared.
    ///
    /// An empty array is the value zero.
    ///
    /// @return negative if a < b, zero if equal, positive if a > b
    public static int compareSigned(byte[] a, byte[] b) {
        boolean aNegative = a.length > 0 && a[0] < 0;
        boolean bNegative = b.length > 0 && b[0] < 0;
        if (aNegative != bNegative) {
            return aNegative ? -1 : 1;
        }
        if (a.length == b.length) {
            if (a.length == 0) {
                return 0;
            }
            // Same sign and same width: the leading bytes carry the same weight, so the whole
            // string compares unsigned.
            return Arrays.compareUnsigned(a, b);
        }
        int length = Math.max(a.length, b.length);
        int aPad = aNegative ? 0xFF : 0x00;
        int bPad = bNegative ? 0xFF : 0x00;
        int aOffset = length - a.length;
        int bOffset = length - b.length;
        for (int i = 0; i < length; i++) {
            int aByte = i < aOffset ? aPad : a[i - aOffset] & 0xFF;
            int bByte = i < bOffset ? bPad : b[i - bOffset] & 0xFF;
            if (aByte != bByte) {
                return aByte - bByte;
            }
        }
        return 0;
    }

    /// Compare two legacy `INT96` timestamps by the instants they encode.
    ///
    /// Each is twelve little-endian bytes: a signed 64-bit count of nanoseconds of the day, then a
    /// signed 32-bit Julian day. The format does not bound the nanoseconds by one day, so the same
    /// instant can be stored as day `d` with `n` nanoseconds or as day `d - 1` with `n` plus a
    /// day's worth. Both values are brought to whole days and a remainder within one day before
    /// they compare, which orders every encoding of an instant as that instant. No sum overflows:
    /// a nanoseconds field moves the day by at most 106,752 days either way.
    ///
    /// @return negative if a < b, zero if both encode the same instant, positive if a > b
    /// @throws IllegalArgumentException if either value is not twelve bytes
    public static int compareInt96(byte[] a, byte[] b) {
        requireInt96(a);
        requireInt96(b);
        long aNanos = (long) LONG_LE.get(a, 0);
        long bNanos = (long) LONG_LE.get(b, 0);
        long aDay = (int) INT_LE.get(a, Long.BYTES) + Math.floorDiv(aNanos, NANOS_PER_DAY);
        long bDay = (int) INT_LE.get(b, Long.BYTES) + Math.floorDiv(bNanos, NANOS_PER_DAY);
        int byDay = Long.compare(aDay, bDay);
        return byDay != 0
                ? byDay
                : Long.compare(Math.floorMod(aNanos, NANOS_PER_DAY), Math.floorMod(bNanos, NANOS_PER_DAY));
    }

    private static void requireInt96(byte[] value) {
        if (value.length != LogicalTypeConverter.INT96_BYTES) {
            throw new IllegalArgumentException("An INT96 value is " + LogicalTypeConverter.INT96_BYTES
                    + " bytes, not " + value.length);
        }
    }
}
