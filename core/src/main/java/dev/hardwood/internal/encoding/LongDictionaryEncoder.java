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
    private int shift;
    private int threshold;

    private long[] values;
    private int size;

    public LongDictionaryEncoder() {
        allocateTable(64);
        this.values = new long[16];
    }

    /// The index assigned to `value`, or `-1` if it has not been seen. Does not assign.
    public int indexOf(long value) {
        int slot = DictionarySlots.home(value, shift);
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

    /// Empties the dictionary for a new column chunk, keeping the hash table and value array
    /// it has grown to. Only the slots are cleared: entries past the new size are unreachable
    /// until they are overwritten, since every lookup goes through the table.
    public void clear() {
        Arrays.fill(slotIndex, EMPTY);
        size = 0;
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
        int slot = DictionarySlots.home(value, shift);
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
        this.shift = DictionarySlots.shift(capacity);
        this.threshold = capacity - (capacity >> 2); // 75%
    }

    /// The mean number of slots a lookup of a held value visits, from each entry's distance to
    /// the slot it starts probing at; how well the placement spreads the values held.
    double meanProbes() {
        long probes = 0;
        for (int s = 0; s < slotIndex.length; s++) {
            if (slotIndex[s] != EMPTY) {
                probes += ((s - DictionarySlots.home(slotValue[s], shift)) & mask) + 1;
            }
        }
        return (double) probes / size;
    }

    /// The bytes this dictionary retains: the distinct values, and the open-addressing table that
    /// finds them. Charged from [#size] rather than from the table's allocated length, so that a
    /// cleared dictionary charges nothing. A slot is a `long` value beside an `int` index, and the
    /// table resizes at a 75% load, so two slots per entry bounds it.
    public long retainedBytes() {
        return (long) size * (Long.BYTES + 2 * (Long.BYTES + Integer.BYTES));
    }
}
