/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

/// Where a fixed-width key starts probing in the open-addressed tables of [DictionaryEncoder] and
/// [LongDictionaryEncoder].
///
/// Fibonacci hashing: the key times `2^64 / φ`, of which the top `log2(capacity)` bits are the
/// slot. Every bit of the key reaches the top of the product, while its low bits depend only on
/// the key's low bits. Keys that agree in their low bits, such as decimal doubles, whose
/// mantissas repeat, or multiples of a power of two, would share a slot if the table were
/// indexed by the low bits.
final class DictionarySlots {

    private static final long GOLDEN_RATIO = 0x9E3779B97F4A7C15L;

    private DictionarySlots() {
    }

    /// The shift that leaves the slot bits of a table of `capacity` slots, a power of two.
    static int shift(int capacity) {
        return Long.SIZE - Integer.numberOfTrailingZeros(capacity);
    }

    /// The slot `key` starts probing at, in a table whose [#shift] is `shift`.
    static int home(long key, int shift) {
        return (int) ((key * GOLDEN_RATIO) >>> shift);
    }
}
