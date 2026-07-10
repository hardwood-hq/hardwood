/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.util.Arrays;

/// Builds a column-chunk dictionary for `INT32` values, assigning indices in first-seen
/// order: the first distinct value is index `0`, the next new value index `1`, and so on.
///
/// Backed by an open-addressed value → index hash table (primitive, no boxing); the distinct
/// values are also kept in index order so the dictionary page can be `PLAIN`-encoded directly
/// from [#values]. The writer interns each present value through [#indexOf] / [#add] and
/// consults [#byteSize] to decide when to fall back to `PLAIN`.
public final class DictionaryEncoder {

    private static final int EMPTY = -1;

    // Open-addressed hash table with linear probing. slotIndex[s] is the dictionary index
    // stored in slot s, or EMPTY when the slot is free; slotValue[s] is that entry's value.
    private int[] slotValue;
    private int[] slotIndex;
    private int mask;
    private int threshold; // resize when size exceeds this (~75% load)

    // Distinct values in index order — the dictionary body.
    private int[] values;
    private int size;

    public DictionaryEncoder() {
        allocateTable(64);
        this.values = new int[16];
    }

    /// The index assigned to `value`, or `-1` if it has not been seen. Does not assign.
    public int indexOf(int value) {
        int slot = hash(value) & mask;
        while (slotIndex[slot] != EMPTY) {
            if (slotValue[slot] == value) {
                return slotIndex[slot];
            }
            slot = (slot + 1) & mask;
        }
        return EMPTY;
    }

    /// Assigns and returns the next index for `value`, which the caller has confirmed absent
    /// via [#indexOf]. Appends it to the dictionary body.
    public int add(int value) {
        if (size == values.length) {
            values = Arrays.copyOf(values, values.length * 2);
        }
        int index = size;
        values[size++] = value;
        insert(value, index);
        if (size > threshold) {
            resizeTable();
        }
        return index;
    }

    /// The number of distinct values assigned so far.
    public int size() {
        return size;
    }

    /// The byte size of the dictionary body if `PLAIN`-encoded now (four bytes per `INT32`).
    public long byteSize() {
        return (long) size * Integer.BYTES;
    }

    /// The distinct values in index order; only indices `[0, size)` are meaningful.
    public int[] values() {
        return values;
    }

    private void insert(int value, int index) {
        int slot = hash(value) & mask;
        while (slotIndex[slot] != EMPTY) {
            slot = (slot + 1) & mask;
        }
        slotValue[slot] = value;
        slotIndex[slot] = index;
    }

    private void resizeTable() {
        int[] oldValue = slotValue;
        int[] oldIndex = slotIndex;
        allocateTable(oldValue.length * 2);
        for (int s = 0; s < oldIndex.length; s++) {
            if (oldIndex[s] != EMPTY) {
                insert(oldValue[s], oldIndex[s]);
            }
        }
    }

    private void allocateTable(int capacity) {
        this.slotValue = new int[capacity];
        this.slotIndex = new int[capacity];
        Arrays.fill(slotIndex, EMPTY);
        this.mask = capacity - 1;
        this.threshold = capacity - (capacity >> 2); // 75%
    }

    /// Fibonacci hashing spreads sequential and clustered `INT32` keys across the table.
    private static int hash(int value) {
        return value * 0x9E3779B1;
    }
}
