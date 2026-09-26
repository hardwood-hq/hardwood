/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.util.Arrays;

/// Builds a column-chunk dictionary for binary values (`BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY`),
/// assigning indices in first-seen order. The binary counterpart of [DictionaryEncoder]; the
/// table keys on byte-array **content** (content hash, content equality).
///
/// The distinct values are kept in index order so the dictionary page can be `PLAIN`-encoded
/// directly from [#values]. [#contentBytes] is the running sum of the distinct values' lengths,
/// which the caller combines with the per-entry framing overhead to size the dictionary body it
/// would write, and so to decide whether writing one pays.
public final class BinaryDictionaryEncoder {

    private static final int EMPTY = -1;

    // Open-addressed hash table with linear probing. slotIndex[s] is the dictionary index in
    // slot s, or EMPTY when free; the value lives at values[slotIndex[s]]. slotHash[s] is that
    // value's hash, kept beside it so that probing past an occupied slot compares two ints
    // rather than two byte arrays — a full array comparison against an entry whose hash never
    // matched is the probe's cost and none of its answer.
    private int[] slotIndex;
    private int[] slotHash;
    private int mask;
    private int threshold;

    private byte[][] values;
    private int size;
    private long contentBytes;

    public BinaryDictionaryEncoder() {
        allocateTable(64);
        this.values = new byte[16][];
    }

    /// The index `value` is assigned, assigning the next one if it has not been seen.
    ///
    /// Lookup and assignment are one operation because they are one hash. Asking first and
    /// adding after hashes every value that turns out to be new a second time, which for a column
    /// of largely distinct values is every value.
    public int intern(byte[] value) {
        int hash = hash(value);
        int slot = hash & mask;
        while (slotIndex[slot] != EMPTY) {
            if (slotHash[slot] == hash && Arrays.equals(values[slotIndex[slot]], value)) {
                return slotIndex[slot];
            }
            slot = (slot + 1) & mask;
        }
        if (size == values.length) {
            values = Arrays.copyOf(values, values.length * 2);
        }
        int index = size;
        // Copy: the caller's array is a window over the batch it came from, which the writer must
        // not still be referencing when the dictionary page is encoded at row-group flush.
        values[size++] = Arrays.copyOf(value, value.length);
        contentBytes += value.length;
        slotIndex[slot] = index;
        slotHash[slot] = hash;
        if (size > threshold) {
            resizeTable();
        }
        return index;
    }

    /// Empties the dictionary for a new column chunk, keeping the hash table and the value array
    /// it has grown to but releasing the values themselves. A chunk that gives up its dictionary
    /// has just resolved every one of those values into its own store, so holding the entries
    /// until each slot is overwritten would keep two copies of the chunk's distinct values.
    public void clear() {
        Arrays.fill(slotIndex, EMPTY);
        Arrays.fill(values, 0, size, null);
        size = 0;
        contentBytes = 0;
    }

    /// The number of distinct values assigned so far.
    public int size() {
        return size;
    }

    /// The running sum of the distinct values' content lengths (no framing overhead).
    public long contentBytes() {
        return contentBytes;
    }

    /// The distinct values in index order; only indices `[0, size)` are meaningful.
    public byte[][] values() {
        return values;
    }

    private void insert(int hash, int index) {
        int slot = hash & mask;
        while (slotIndex[slot] != EMPTY) {
            slot = (slot + 1) & mask;
        }
        slotIndex[slot] = index;
        slotHash[slot] = hash;
    }

    /// Doubles the table and re-places what it holds. The hashes move with the entries rather
    /// than being recomputed, which would mean hashing every distinct value of the chunk again
    /// on every resize.
    private void resizeTable() {
        int[] oldIndex = slotIndex;
        int[] oldHash = slotHash;
        allocateTable(oldIndex.length * 2);
        for (int s = 0; s < oldIndex.length; s++) {
            if (oldIndex[s] != EMPTY) {
                insert(oldHash[s], oldIndex[s]);
            }
        }
    }

    private void allocateTable(int capacity) {
        this.slotIndex = new int[capacity];
        this.slotHash = new int[capacity];
        Arrays.fill(slotIndex, EMPTY);
        this.mask = capacity - 1;
        this.threshold = capacity - (capacity >> 2); // 75%
    }

    private static int hash(byte[] value) {
        return Arrays.hashCode(value) * 0x9E3779B1;
    }

    /// The bytes this dictionary retains: the distinct values' own bytes and the `byte[]` holding
    /// each, the value array's references, and the open-addressing table that finds them. Charged
    /// from [#size] rather than from the table's allocated length, so that a cleared dictionary
    /// charges nothing.
    public long retainedBytes() {
        return contentBytes + (long) size * (ARRAY_HEADER_BYTES + REFERENCE_BYTES
                + 2 * (Integer.BYTES + Integer.BYTES));
    }

    /// A `byte[]`'s object header and length, which a dictionary of many short values pays more
    /// for than for the values themselves.
    private static final int ARRAY_HEADER_BYTES = 16;

    /// A reference in the value array, under compressed oops.
    private static final int REFERENCE_BYTES = 4;
}
