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

import dev.hardwood.internal.conversion.FixedWidths;
import dev.hardwood.internal.schema.ByteColumnOrder;

/// Byte array comparison in the orders a binary column sorts in: unsigned lexicographic (for
/// BYTE_ARRAY), big-endian signed two's complement (for DECIMAL columns of either byte-array type),
/// little-endian signed two's complement (for a `FIXED_LEN_BYTE_ARRAY(12)` `TIMESTAMP`), the
/// instant a legacy `INT96` timestamp encodes, and the day-then-nanoseconds order of
/// `INT96_TIMESTAMP_ORDER`.
///
/// Both the reader's predicate evaluation and the writer's statistics collection compare through
/// here, so a chunk's bounds and a predicate over them cannot come to disagree. The writer's
/// per-value fast path also lives here: [#prefix] reads the first eight bytes a bound is
/// compared on, and [#compareUnsignedAfterPrefix] settles the values those bytes tie on.
public final class BinaryComparator {

    private static final VarHandle LONG_LE =
            MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle INT_LE =
            MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle LONG_BE =
            MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    /// `PREFIX_MASKS[k]` keeps the first `k` bytes of a big-endian `long` and clears the rest.
    private static final long[] PREFIX_MASKS = prefixMasks();

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

    /// Compare the slice `a[aFrom, aTo)` against all of `b` lexicographically (unsigned).
    ///
    /// @return negative if the slice < b, zero if equal, positive if the slice > b
    public static int compareUnsigned(byte[] a, int aFrom, int aTo, byte[] b) {
        return Arrays.compareUnsigned(a, aFrom, aTo, b, 0, b.length);
    }

    /// The first up-to-eight bytes of `array[offset, offset + length)` as a big-endian `long`,
    /// zero-padded where the value is shorter. Values whose prefixes differ are ordered by their
    /// prefixes alone, which lets the writer's statistics settle most comparisons without reading
    /// the values again.
    public static long prefix(byte[] array, int offset, int length) {
        if (length >= Long.BYTES) {
            return (long) LONG_BE.get(array, offset);
        }
        if (offset <= array.length - Long.BYTES) {
            return (long) LONG_BE.get(array, offset) & PREFIX_MASKS[length];
        }
        long prefix = 0L;
        for (int i = 0; i < length; i++) {
            prefix |= Byte.toUnsignedLong(array[offset + i]) << (Long.SIZE - Byte.SIZE * (i + 1));
        }
        return prefix;
    }

    /// Compares two unsigned lexicographic values whose [#prefix]es are equal. A value that fits
    /// within the prefix is decided by its length: if both fit, the shorter sorts first; if only
    /// one does, it sorts first. Otherwise the bytes after the prefix decide, compared in place
    /// where the remainder is short.
    ///
    /// @return negative if `a` < `b`, zero if equal, positive if `a` > `b`
    public static int compareUnsignedAfterPrefix(byte[] a, int aOffset, int aLength,
                                                 byte[] b, int bOffset, int bLength) {
        boolean aShort = aLength <= Long.BYTES;
        boolean bShort = bLength <= Long.BYTES;
        if (aShort && bShort) {
            return Integer.compare(aLength, bLength);
        }
        if (aShort) {
            return -1;
        }
        if (bShort) {
            return 1;
        }
        int aTail = aLength - Long.BYTES;
        int bTail = bLength - Long.BYTES;
        // A short remainder is cheaper to compare in place than through Arrays.compareUnsigned,
        // whose entry point costs more than the one to eight bytes most keys leave here. The
        // zero padding only ties where one remainder is a prefix of the other, which the lengths
        // then settle.
        if (aTail <= Long.BYTES && bTail <= Long.BYTES) {
            int difference = Long.compareUnsigned(prefix(a, aOffset + Long.BYTES, aTail),
                    prefix(b, bOffset + Long.BYTES, bTail));
            return difference != 0 ? difference : Integer.compare(aTail, bTail);
        }
        return Arrays.compareUnsigned(a, aOffset + Long.BYTES, aOffset + aLength,
                b, bOffset + Long.BYTES, bOffset + bLength);
    }

    private static long[] prefixMasks() {
        long[] masks = new long[Long.BYTES + 1];
        for (int length = 1; length <= Long.BYTES; length++) {
            masks[length] = -1L << (Long.SIZE - Byte.SIZE * length);
        }
        return masks;
    }

    /// Compare the slice `a[aFrom, aTo)` against all of `b` as big-endian two's complement values,
    /// sign-extending the shorter to the longer exactly as [#compareSigned(byte[], byte[])] does: a
    /// `BYTE_ARRAY` `DECIMAL` stores each value in the fewest bytes that hold it, so the widths
    /// legitimately differ. An empty slice is the value zero.
    ///
    /// @return negative if the slice < b, zero if equal, positive if the slice > b
    public static int compareSigned(byte[] a, int aFrom, int aTo, byte[] b) {
        int aLength = aTo - aFrom;
        boolean aNegative = aLength > 0 && a[aFrom] < 0;
        boolean bNegative = b.length > 0 && b[0] < 0;
        if (aNegative != bNegative) {
            return aNegative ? -1 : 1;
        }
        if (aLength == b.length) {
            if (aLength == 0) {
                return 0;
            }
            return Arrays.compareUnsigned(a, aFrom, aTo, b, 0, b.length);
        }
        int length = Math.max(aLength, b.length);
        int aPad = aNegative ? 0xFF : 0x00;
        int bPad = bNegative ? 0xFF : 0x00;
        int aOffset = length - aLength;
        int bOffset = length - b.length;
        for (int i = 0; i < length; i++) {
            int aByte = i < aOffset ? aPad : a[aFrom + i - aOffset] & 0xFF;
            int bByte = i < bOffset ? bPad : b[i - bOffset] & 0xFF;
            if (aByte != bByte) {
                return aByte - bByte;
            }
        }
        return 0;
    }

    /// Compare the slice `a[aFrom, aTo)` against all of `b` in `order`.
    ///
    /// @return negative if the slice < b, zero if equal, positive if the slice > b
    ///
    /// A `FLOAT16` is compared as the half it encodes, by `Float16Predicate` and the writer's own
    /// collector, and a column without an order is compared in none, so neither is a slice order.
    ///
    /// @throws IllegalArgumentException if `order` is [ByteColumnOrder#HALF_FLOAT] or
    ///         [ByteColumnOrder#NONE]
    public static int compare(byte[] a, int aFrom, int aTo, byte[] b, ByteColumnOrder order) {
        return switch (order) {
            case BYTES -> compareUnsigned(a, aFrom, aTo, b);
            case SIGNED_BIG_ENDIAN -> compareSigned(a, aFrom, aTo, b);
            case SIGNED_LITTLE_ENDIAN -> compareSignedLittleEndian(a, aFrom, aTo, b);
            case INT96_INSTANT -> compareInt96(a, aFrom, aTo, b);
            case HALF_FLOAT, NONE -> throw new IllegalArgumentException("No slice is compared in " + order);
        };
    }

    /// The order `comparison` compares slices in.
    ///
    /// The byte-array matchers read the order from here, so a new
    /// [ResolvedPredicate.BinaryPredicate.Comparison] is answered once, in a switch with no
    /// `default`, and every comparison names an order that has a slice comparison.
    public static ByteColumnOrder order(ResolvedPredicate.BinaryPredicate.Comparison comparison) {
        return switch (comparison) {
            case BYTE_STRING, STORED_BYTES -> ByteColumnOrder.BYTES;
            case FIXED_DECIMAL, VARIABLE_DECIMAL -> ByteColumnOrder.SIGNED_BIG_ENDIAN;
            case FIXED_TIMESTAMP -> ByteColumnOrder.SIGNED_LITTLE_ENDIAN;
            case INT96_INSTANT -> ByteColumnOrder.INT96_INSTANT;
        };
    }

    /// Whether the slice `a[aFrom, aTo)` holds exactly the bytes of `b`.
    ///
    /// Sound as an equality test only where the column encodes a value as exactly one byte string —
    /// see [ResolvedPredicate.BinaryPredicate.Comparison#byteExact()]. Where it does not, equality
    /// must go through [#compare(byte[], int, int, byte[], boolean)] instead, because a padded
    /// spelling of the literal's value holds different bytes.
    public static boolean sliceEquals(byte[] a, int aFrom, int aTo, byte[] b) {
        return Arrays.equals(a, aFrom, aTo, b, 0, b.length);
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
        return compareSigned(a, 0, a.length, b);
    }

    /// Compare two little-endian two's complement values of equal width, the order a
    /// `FIXED_LEN_BYTE_ARRAY(12)` `TIMESTAMP` sorts in.
    ///
    /// The last byte is the most significant and carries the sign, so it compares signed; the
    /// remaining bytes compare unsigned, from the last towards the first.
    ///
    /// @return negative if a < b, zero if equal, positive if a > b
    /// @throws IllegalArgumentException if the two differ in width, which no column of the type
    ///         stores
    public static int compareSignedLittleEndian(byte[] a, byte[] b) {
        return compareSignedLittleEndian(a, 0, a.length, b);
    }

    /// Compare the slice `a[aFrom, aTo)` against all of `b` as little-endian two's complement
    /// values of equal width, exactly as [#compareSignedLittleEndian(byte[], byte[])] does.
    ///
    /// @return negative if the slice < b, zero if equal, positive if the slice > b
    /// @throws IllegalArgumentException if the two differ in width, which no column of the type
    ///         stores
    public static int compareSignedLittleEndian(byte[] a, int aFrom, int aTo, byte[] b) {
        int length = aTo - aFrom;
        if (length != b.length) {
            throw new IllegalArgumentException("A little-endian signed comparison takes values of one width, not "
                    + length + " and " + b.length + " bytes");
        }
        int last = length - 1;
        if (last < 0) {
            return 0;
        }
        if (a[aFrom + last] != b[last]) {
            return Byte.compare(a[aFrom + last], b[last]);
        }
        for (int i = last - 1; i >= 0; i--) {
            if (a[aFrom + i] != b[i]) {
                return Integer.compare(a[aFrom + i] & 0xFF, b[i] & 0xFF);
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
        return compareInt96(a, 0, a.length, b);
    }

    /// Compare the slice `a[aFrom, aTo)` against all of `b` by the instants they encode, exactly as
    /// [#compareInt96(byte[], byte[])] does.
    ///
    /// @return negative if the slice < b, zero if both encode the same instant, positive if a > b
    /// @throws IllegalArgumentException if either value is not twelve bytes
    public static int compareInt96(byte[] a, int aFrom, int aTo, byte[] b) {
        requireInt96(aTo - aFrom);
        requireInt96(b.length);
        long aNanos = (long) LONG_LE.get(a, aFrom);
        long bNanos = (long) LONG_LE.get(b, 0);
        long aDay = (int) INT_LE.get(a, aFrom + Long.BYTES) + Math.floorDiv(aNanos, NANOS_PER_DAY);
        long bDay = (int) INT_LE.get(b, Long.BYTES) + Math.floorDiv(bNanos, NANOS_PER_DAY);
        int byDay = Long.compare(aDay, bDay);
        return byDay != 0
                ? byDay
                : Long.compare(Math.floorMod(aNanos, NANOS_PER_DAY), Math.floorMod(bNanos, NANOS_PER_DAY));
    }

    /// Compare two legacy `INT96` timestamps in `INT96_TIMESTAMP_ORDER`, the order a writer
    /// declaring it records bounds in: the Julian day as a signed 32-bit integer, then the
    /// nanoseconds of the day as a signed 64-bit integer, each as stored.
    ///
    /// Neither field is normalized, so this orders by instant only where both nanoseconds fields
    /// lie within one day ([#int96NanosWithinDay]); [#compareInt96(byte[], byte[])] orders by
    /// instant throughout. Two values compare equal exactly when their bytes are equal.
    ///
    /// @return negative if a < b, zero if both hold the same bytes, positive if a > b
    /// @throws IllegalArgumentException if either value is not twelve bytes
    public static int compareInt96TimestampOrder(byte[] a, byte[] b) {
        requireInt96(a.length);
        requireInt96(b.length);
        int byDay = Integer.compare((int) INT_LE.get(a, Long.BYTES), (int) INT_LE.get(b, Long.BYTES));
        return byDay != 0
                ? byDay
                : Long.compare((long) LONG_LE.get(a, 0), (long) LONG_LE.get(b, 0));
    }

    /// Whether a twelve-byte legacy `INT96` timestamp holds nanoseconds of the day in
    /// `[0, 1 day)`, where its day and nanoseconds order as the instant they encode.
    ///
    /// @throws IllegalArgumentException if the value is not twelve bytes
    public static boolean int96NanosWithinDay(byte[] value) {
        requireInt96(value.length);
        long nanos = (long) LONG_LE.get(value, 0);
        return nanos >= 0 && nanos < NANOS_PER_DAY;
    }

    private static void requireInt96(int width) {
        if (width != FixedWidths.INT96) {
            throw new IllegalArgumentException("An INT96 value is " + FixedWidths.INT96
                    + " bytes, not " + width);
        }
    }
}
