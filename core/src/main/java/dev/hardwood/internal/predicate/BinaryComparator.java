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
/// BYTE_ARRAY), big-endian signed two's complement (for DECIMAL columns of either byte-array type),
/// little-endian signed two's complement (for a `FIXED_LEN_BYTE_ARRAY(12)` `TIMESTAMP`), and the
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

    /// Compare the slice `a[aFrom, aTo)` against all of `b` lexicographically (unsigned).
    ///
    /// @return negative if the slice < b, zero if equal, positive if the slice > b
    public static int compareUnsigned(byte[] a, int aFrom, int aTo, byte[] b) {
        return Arrays.compareUnsigned(a, aFrom, aTo, b, 0, b.length);
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

    /// Compare the slice `a[aFrom, aTo)` against all of `b` in the column's order: signed when
    /// `signed`, otherwise unsigned.
    ///
    /// @return negative if the slice < b, zero if equal, positive if the slice > b
    public static int compare(byte[] a, int aFrom, int aTo, byte[] b, boolean signed) {
        return signed ? compareSigned(a, aFrom, aTo, b) : compareUnsigned(a, aFrom, aTo, b);
    }

    /// The order [#compare(byte[], int, int, byte[], boolean)] compares slices in for a comparison.
    public enum SliceOrder {
        /// Unsigned lexicographic.
        UNSIGNED,
        /// Big-endian two's complement, sign-extending the shorter slice.
        SIGNED,
        /// No slice comparison implements the order, so a predicate in it cannot be decided over
        /// slices.
        NONE
    }

    /// The slice order `comparison` compares in, or [SliceOrder#NONE] where no slice comparison
    /// implements it.
    ///
    /// Both the batch filter compiler's eligibility check and the byte-array matchers read the
    /// order from here, so a new [ResolvedPredicate.BinaryPredicate.Comparison] is answered once:
    /// the switch has no `default`, and [SliceOrder#NONE] keeps a predicate off the batch path.
    public static SliceOrder sliceOrder(ResolvedPredicate.BinaryPredicate.Comparison comparison) {
        return switch (comparison) {
            case BYTE_STRING, STORED_BYTES -> SliceOrder.UNSIGNED;
            case FIXED_DECIMAL, VARIABLE_DECIMAL -> SliceOrder.SIGNED;
            case FIXED_TIMESTAMP, INT96_INSTANT -> SliceOrder.NONE;
        };
    }

    /// The `signed` argument [#compare(byte[], int, int, byte[], boolean)] takes to compare slices
    /// in `comparison`'s order, resolved once so a per-row loop does not switch over the order.
    ///
    /// @throws IllegalArgumentException for a comparison whose [#sliceOrder] is [SliceOrder#NONE]
    public static boolean signedSliceOrder(ResolvedPredicate.BinaryPredicate.Comparison comparison) {
        return switch (sliceOrder(comparison)) {
            case UNSIGNED -> false;
            case SIGNED -> true;
            case NONE -> throw new IllegalArgumentException(
                    "No slice comparison compares in the " + comparison + " order");
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
        if (a.length != b.length) {
            throw new IllegalArgumentException("A little-endian signed comparison takes values of one width, not "
                    + a.length + " and " + b.length + " bytes");
        }
        int last = a.length - 1;
        if (last < 0) {
            return 0;
        }
        if (a[last] != b[last]) {
            return Byte.compare(a[last], b[last]);
        }
        for (int i = last - 1; i >= 0; i--) {
            if (a[i] != b[i]) {
                return Integer.compare(a[i] & 0xFF, b[i] & 0xFF);
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
