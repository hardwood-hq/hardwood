/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.util.Arrays;

/// Builds a column-chunk dictionary for 64-bit values, assigning indices in first-seen
/// order: the first distinct value is index `0`, the next new value index `1`, and so on.
/// The `INT64` counterpart of [DictionaryEncoder]; `DOUBLE` columns intern through the same
/// table on the value's raw bit pattern.
///
/// Backed by an open-addressed value → index hash table (primitive, no boxing); the distinct
/// values are also kept in index order so the dictionary page can be `PLAIN`-encoded directly
/// from [#values].
public final class LongDictionaryEncoder {

    private static final int EMPTY = -1;

    private long[] slotValue;
    private int[] slotIndex;
    private int mask;
    private int threshold;

    private long[] values;
    private int size;

    public LongDictionaryEncoder() {
        allocateTable(64);
        this.values = new long[16];
    }

    /// The index assigned to `value`, or `-1` if it has not been seen. Does not assign.
    public int indexOf(long value) {
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
    public int add(long value) {
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

    /// The byte size of the dictionary body if `PLAIN`-encoded now (eight bytes per value).
    public long byteSize() {
        return (long) size * Long.BYTES;
    }

    /// The distinct values in index order; only indices `[0, size)` are meaningful.
    public long[] values() {
        return values;
    }

    private void insert(long value, int index) {
        int slot = hash(value) & mask;
        while (slotIndex[slot] != EMPTY) {
            slot = (slot + 1) & mask;
        }
        slotValue[slot] = value;
        slotIndex[slot] = index;
    }

    private void resizeTable() {
        long[] oldValue = slotValue;
        int[] oldIndex = slotIndex;
        allocateTable(oldValue.length * 2);
        for (int s = 0; s < oldIndex.length; s++) {
            if (oldIndex[s] != EMPTY) {
                insert(oldValue[s], oldIndex[s]);
            }
        }
    }

    private void allocateTable(int capacity) {
        this.slotValue = new long[capacity];
        this.slotIndex = new int[capacity];
        Arrays.fill(slotIndex, EMPTY);
        this.mask = capacity - 1;
        this.threshold = capacity - (capacity >> 2); // 75%
    }

    /// Fibonacci hashing over the mixed high and low words spreads sequential and clustered
    /// 64-bit keys across the table.
    private static int hash(long value) {
        return (int) ((value ^ (value >>> 32)) * 0x9E3779B1L);
    }
}
