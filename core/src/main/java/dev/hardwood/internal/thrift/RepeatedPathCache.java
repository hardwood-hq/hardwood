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
/// A cache instance belongs to one [ThriftCompactReader] and is used by a single thread.
final class RepeatedPathCache {

    private static final int INITIAL_CAPACITY = 16;

    /// The encoded `list<string>` bytes of the path at each position, as they appear in the
    /// footer, so a repeat is recognised without decoding it.
    private byte[][] encoded = new byte[INITIAL_CAPACITY][];
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
        byte[] predicted = slot < encoded.length ? encoded[slot] : null;
        if (predicted != null && reader.matchesAt(predicted)) {
            reader.skipBytes(predicted.length);
            return paths[slot];
        }
        int start = reader.position();
        FieldPath path = new FieldPath(reader.readStringList());
        store(slot, reader.copyRange(start, reader.position() - start), path);
        return path;
    }

    private void store(int slot, byte[] encodedPath, FieldPath path) {
        if (slot >= encoded.length) {
            int capacity = Math.max(slot + 1, encoded.length * 2);
            encoded = Arrays.copyOf(encoded, capacity);
            paths = Arrays.copyOf(paths, capacity);
        }
        encoded[slot] = encodedPath;
        paths[slot] = path;
    }
}
