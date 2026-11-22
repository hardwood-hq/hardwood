/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.util.Arrays;

import dev.hardwood.metadata.FieldPath;

/// Per-decode cache of the `path_in_schema` of each column, so a footer materializes one
/// [FieldPath] per column rather than one per column chunk.
///
/// A footer repeats a column's path in every row group's chunk of that column — on a file of
/// 100,000 columns and ten row groups, a million paths of which only 100,000 are distinct, each
/// otherwise costing a `String`, its bytes, a list and a `FieldPath`.
///
/// The cache is keyed by position, the way [dev.hardwood.internal.reader.batch.Dictionary] keys its
/// interned strings by dictionary index: chunks appear in schema order within a row group, so
/// the n-th path of one row group is the n-th path of the next. That is a convention rather
/// than a rule the format enforces, so it is a **prediction**, checked by comparing the cached
/// entry against the encoded bytes about to be read — a hit skips the decode entirely, a miss
/// decodes and re-primes that position. Content-keyed interning was measured against this and
/// lost: hashing every path and probing a table of them costs more than the allocation it saves.
///
/// A repeat is recognised without copying the path out: the cache remembers where the previous
/// row group's bytes sit in the footer (offset and length) and compares them in place, 8 bytes
/// at a time — on a mapped file the one-byte-at-a-time loop it replaces was about 2.4× the cost
/// of just copying the path bytes out. Keeping the offset instead of a `byte[]` copy also saves
/// about 2 MB of retained bytes per decode at 100,000 columns.
///
/// A cache instance belongs to one [ThriftCompactReader] and is used by a single thread.
final class RepeatedPathCache {

    private static final int INITIAL_CAPACITY = 16;

    /// The offset and length of the encoded `list<string>` bytes of the path at each position,
    /// as they appear in the footer, so a repeat is recognised without decoding it. A length of
    /// zero means the slot was never primed: every encoded path is at least its list-header
    /// byte, so a real entry never has length zero.
    private int[] offsets = new int[INITIAL_CAPACITY];
    private int[] lengths = new int[INITIAL_CAPACITY];
    private FieldPath[] paths = new FieldPath[INITIAL_CAPACITY];
    private int position;

    /// Starts the column chunks of a row group, so the next path read is predicted from the
    /// first column of the previous row group.
    void startRowGroup() {
        position = 0;
    }

    /// The [FieldPath] of the `path_in_schema` list the reader is positioned on, consuming it
    /// either way.
    ///
    /// @param reader positioned on the list header of the path
    FieldPath next(ThriftCompactReader reader) {
        int slot = position++;
        if (slot < lengths.length && lengths[slot] != 0
                && reader.matchesRange(offsets[slot], lengths[slot])) {
            reader.skipBytes(lengths[slot]);
            return paths[slot];
        }
        int start = reader.position();
        FieldPath path = new FieldPath(reader.readStringList());
        store(slot, start, reader.position() - start, path);
        return path;
    }

    private void store(int slot, int offset, int length, FieldPath path) {
        if (slot >= lengths.length) {
            int capacity = Math.max(slot + 1, lengths.length * 2);
            offsets = Arrays.copyOf(offsets, capacity);
            lengths = Arrays.copyOf(lengths, capacity);
            paths = Arrays.copyOf(paths, capacity);
        }
        offsets[slot] = offset;
        lengths[slot] = length;
        paths[slot] = path;
    }
}
