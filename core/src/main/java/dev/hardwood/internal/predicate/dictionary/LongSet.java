/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate.dictionary;

import java.util.Arrays;

/// A fixed set of `long` keys, built once from a predicate's literals and probed once per
/// dictionary entry. Open addressing with linear probing over a power-of-two table filled to at
/// most a quarter, so a miss, the common probe, ends at the first or second slot.
///
/// Empty slots hold a marker that is not a key, so no probe needs a special case: a probe for the
/// marker itself finds the marker and reports no key.
final class LongSet {

    private static final int MIN_SLOTS = 16;

    private final long[] slots;
    private final int shift;
    private final long empty;

    LongSet(long[] keys) {
        int length = Math.max(MIN_SLOTS, Integer.highestOneBit(Math.multiplyExact(keys.length, 4) - 1) << 1);
        slots = new long[length];
        shift = Long.SIZE - Integer.numberOfTrailingZeros(length);
        empty = freeValue(keys);
        Arrays.fill(slots, empty);
        int mask = length - 1;
        for (long key : keys) {
            int i = (int) (mix(key) >>> shift);
            while (slots[i] != empty) {
                i = (i + 1) & mask;
            }
            slots[i] = key;
        }
    }

    boolean contains(long key) {
        int mask = slots.length - 1;
        int i = (int) (mix(key) >>> shift);
        while (true) {
            long slot = slots[i];
            if (slot == empty) {
                return false;
            }
            if (slot == key) {
                return true;
            }
            i = (i + 1) & mask;
        }
    }

    /// Stafford's variant 13 of the SplitMix64 finalizer: a bijection whose high bits, which
    /// pick the slot, depend on every input bit. [BinarySet] finishes its hash with it too.
    static long mix(long value) {
        long z = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }

    /// A value that is not among `keys`: `0` when it is free, else the first free value along a
    /// deterministic sequence. Each candidate costs one pass over `keys`.
    private static long freeValue(long[] keys) {
        long candidate = 0;
        while (contains(keys, candidate)) {
            candidate = mix(candidate) + 1;
        }
        return candidate;
    }

    private static boolean contains(long[] keys, long value) {
        for (long key : keys) {
            if (key == value) {
                return true;
            }
        }
        return false;
    }
}
