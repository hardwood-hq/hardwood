/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate.matcher.binaries;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import dev.hardwood.internal.predicate.BinaryComparator;
import dev.hardwood.internal.reader.BinaryBatchValues;

/// Byte equality of each row against a set of members, with rows of at most eight bytes compared as
/// one big-endian `long`.
///
/// A value of eight bytes or fewer fits in a `long` once the bytes past its length are masked off,
/// so it equals a member of the same length exactly when the two masked `long`s are equal: one load
/// and one compare per member, no branch. That is the case [java.util.Arrays#equals] is slowest on —
/// below eight bytes it compares byte by byte — while from eight bytes up it compares a word at a
/// time, so rows longer than eight bytes keep [BinaryComparator#sliceEquals] against the longer
/// members. A row's length picks the side, and a row only ever equals members of its own length.
///
/// Reading eight bytes at a value's start runs past a value shorter than eight bytes. Inside `bytes`
/// that only reads the next value or unused capacity, which the mask discards; but the last values
/// of a batch can end within eight bytes of the array's end, so a short row starting there is
/// compared byte by byte instead.
///
/// Sound only where the column holds a value as exactly one byte string —
/// [dev.hardwood.internal.predicate.ResolvedPredicate.BinaryPredicate.Comparison#byteExact()].
/// Validity is not applied: null slots produce bits the caller masks out.
final class ShortValueEquality {

    private static final VarHandle LONG_BE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    /// `PREFIX_MASKS[k]` keeps the first `k` bytes of a big-endian `long` and clears the rest.
    private static final long[] PREFIX_MASKS = prefixMasks();

    private final byte[][] shortMembers;
    private final int[] shortLengths;
    private final long[] shortValues;
    private final byte[][] longMembers;

    private ShortValueEquality(byte[][] shortMembers, byte[][] longMembers) {
        this.shortMembers = shortMembers;
        this.shortLengths = new int[shortMembers.length];
        this.shortValues = new long[shortMembers.length];
        for (int m = 0; m < shortMembers.length; m++) {
            shortLengths[m] = shortMembers[m].length;
            shortValues[m] = asLong(shortMembers[m]);
        }
        this.longMembers = longMembers;
    }

    /// Whether any member is at most eight bytes long. Without one, every row would take the byte-wise
    /// comparison and this class would only add work.
    static boolean hasShortMember(byte[]... members) {
        for (byte[] member : members) {
            if (member.length <= Long.BYTES) {
                return true;
            }
        }
        return false;
    }

    /// Equality against `members`, at least one of which must be at most eight bytes long.
    static ShortValueEquality of(byte[]... members) {
        int shortCount = 0;
        for (byte[] member : members) {
            if (member.length <= Long.BYTES) {
                shortCount++;
            }
        }
        if (shortCount == 0) {
            throw new IllegalArgumentException("At least one member must be at most eight bytes long");
        }
        byte[][] shortMembers = new byte[shortCount][];
        byte[][] longMembers = new byte[members.length - shortCount][];
        int s = 0;
        int l = 0;
        for (byte[] member : members) {
            if (member.length <= Long.BYTES) {
                shortMembers[s++] = member;
            }
            else {
                longMembers[l++] = member;
            }
        }
        return new ShortValueEquality(shortMembers, longMembers);
    }

    /// Writes one bit per row into `outWords`, set iff the row's bytes equal a member.
    void test(BinaryBatchValues vals, int recordCount, long[] outWords) {
        byte[] bytes = vals.bytes;
        int[] offsets = vals.offsets;
        // The last position an eight-byte read fits at; negative for an array under eight bytes.
        int lastLongStart = bytes.length - Long.BYTES;
        int activeWords = (recordCount + 63) >>> 6;

        for (int w = 0; w < activeWords; w++) {
            int base = w << 6;
            int rows = Math.min(64, recordCount - base);
            long word = 0L;
            for (int b = 0; b < rows; b++) {
                int i = base + b;
                int start = offsets[i];
                word |= (testValue(bytes, start, offsets[i + 1], lastLongStart) ? 1L : 0L) << b;
            }
            outWords[w] = word;
        }
    }

    /// Whether one byte slice equals any member.
    boolean testValue(byte[] bytes, int from, int to) {
        return testValue(bytes, from, to, bytes.length - Long.BYTES);
    }

    private boolean testValue(byte[] bytes, int from, int to, int lastLongStart) {
        int length = to - from;
        if (length > Long.BYTES) {
            return containsAny(longMembers, bytes, from, to);
        }
        if (from <= lastLongStart) {
            long value = (long) LONG_BE.get(bytes, from) & PREFIX_MASKS[length];
            boolean hit = false;
            for (int m = 0; m < shortLengths.length; m++) {
                hit |= (length == shortLengths[m]) & (value == shortValues[m]);
            }
            return hit;
        }
        return containsAny(shortMembers, bytes, from, to);
    }

    /// Clears every bit whose row is null. Bits past `recordCount` are left as they are: the consumer
    /// never reads past a batch's record count.
    static void keepPresent(long[] outWords, long[] validity, int recordCount) {
        if (validity == null) {
            return;
        }
        int activeWords = (recordCount + 63) >>> 6;
        for (int w = 0; w < activeWords; w++) {
            outWords[w] &= validity[w];
        }
    }

    /// Inverts the words covering the first `recordCount` bits.
    static void invert(long[] outWords, int recordCount) {
        int activeWords = (recordCount + 63) >>> 6;
        for (int w = 0; w < activeWords; w++) {
            outWords[w] = ~outWords[w];
        }
    }

    private static boolean containsAny(byte[][] members, byte[] bytes, int from, int to) {
        for (byte[] member : members) {
            if (BinaryComparator.sliceEquals(bytes, from, to, member)) {
                return true;
            }
        }
        return false;
    }

    private static long asLong(byte[] member) {
        long value = 0L;
        for (int k = 0; k < member.length; k++) {
            value |= (member[k] & 0xFFL) << (56 - (k << 3));
        }
        return value;
    }

    private static long[] prefixMasks() {
        long[] masks = new long[Long.BYTES + 1];
        for (int k = 1; k <= Long.BYTES; k++) {
            masks[k] = -1L << (Long.SIZE - (k << 3));
        }
        return masks;
    }
}
