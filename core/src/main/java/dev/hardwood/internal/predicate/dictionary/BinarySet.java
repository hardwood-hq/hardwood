/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate.dictionary;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

import dev.hardwood.internal.predicate.BinaryComparator;

/// A fixed set of byte strings, built once from a predicate's literals and probed once per
/// dictionary entry. Open addressing with linear probing over a power-of-two table filled to at
/// most a quarter; a slot holds the index of a key and the key's hash, so a probe compares bytes
/// only when the hashes agree.
///
/// Before hashing an entry, its length is checked against the lengths the keys have: an entry no
/// key is as long as matches none, and the check is one bit test.
///
/// The hash folds the string's words into an accumulator seeded with the length, one multiply per
/// word, and finishes with [LongSet#mix]. It covers every byte, so two strings hash alike only by
/// chance, and a stored hash guards each byte comparison; the hash only places keys, so it needs
/// no agreement with any other hash of the same bytes.
final class BinarySet {

    private static final int MIN_SLOTS = 16;
    private static final int EMPTY = -1;

    private static final VarHandle LONG_LE =
            MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle INT_LE =
            MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    /// Odd multiplier folding one word into the accumulator (the golden ratio's 64-bit fraction).
    private static final long FOLD = 0x9E3779B97F4A7C15L;

    private final byte[][] keys;
    private final int[] slots;
    private final long[] hashes;
    private final int shift;
    private final long lengths;

    BinarySet(byte[][] keys) {
        this.keys = keys;
        int length = Math.max(MIN_SLOTS, Integer.highestOneBit(Math.multiplyExact(keys.length, 4) - 1) << 1);
        slots = new int[length];
        hashes = new long[length];
        Arrays.fill(slots, EMPTY);
        shift = Long.SIZE - Integer.numberOfTrailingZeros(length);
        int mask = length - 1;
        long lengthBits = 0;
        for (int index = 0; index < keys.length; index++) {
            byte[] key = keys[index];
            lengthBits |= lengthBit(key.length);
            long hash = hash(key, 0, key.length);
            int i = (int) (hash >>> shift);
            while (slots[i] != EMPTY) {
                i = (i + 1) & mask;
            }
            slots[i] = index;
            hashes[i] = hash;
        }
        lengths = lengthBits;
    }

    /// Whether the bytes `[from, to)` of `bytes` are a key.
    boolean contains(byte[] bytes, int from, int to) {
        if ((lengths & lengthBit(to - from)) == 0) {
            return false;
        }
        long hash = hash(bytes, from, to);
        int mask = slots.length - 1;
        int i = (int) (hash >>> shift);
        int index;
        while ((index = slots[i]) != EMPTY) {
            if (hashes[i] == hash && BinaryComparator.sliceEquals(bytes, from, to, keys[index])) {
                return true;
            }
            i = (i + 1) & mask;
        }
        return false;
    }

    /// Hash of the bytes `[from, to)`. From eight bytes up, the slice is read as little-endian
    /// words, the last of which ends at `to` and overlaps the one before it where the length is
    /// not a multiple of eight, so no byte is read one at a time and none outside the slice. A
    /// shorter slice is one word built from its first and last four bytes, or from its first,
    /// middle and last byte; the length in the seed tells `aa` from `a`.
    private static long hash(byte[] bytes, int from, int to) {
        int length = to - from;
        long h = length * FOLD;
        if (length >= Long.BYTES) {
            int last = to - Long.BYTES;
            for (int p = from; p < last; p += Long.BYTES) {
                h = fold(h, (long) LONG_LE.get(bytes, p));
            }
            h = fold(h, (long) LONG_LE.get(bytes, last));
        }
        else if (length >= Integer.BYTES) {
            long head = (int) INT_LE.get(bytes, from) & 0xFFFFFFFFL;
            long tail = (int) INT_LE.get(bytes, to - Integer.BYTES) & 0xFFFFFFFFL;
            h = fold(h, head | tail << 32);
        }
        else if (length > 0) {
            long word = (bytes[from] & 0xFFL)
                    | (bytes[from + length / 2] & 0xFFL) << 8
                    | (bytes[to - 1] & 0xFFL) << 16;
            h = fold(h, word);
        }
        return LongSet.mix(h);
    }

    private static long fold(long h, long word) {
        return Long.rotateLeft((h ^ word) * FOLD, 29);
    }

    /// Bit `length` for lengths below 64; bit 63 stands for every longer length.
    private static long lengthBit(int length) {
        return 1L << Math.min(length, Long.SIZE - 1);
    }
}
