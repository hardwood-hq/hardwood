/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.util.Arrays;
import java.util.List;

import dev.hardwood.metadata.PageEncodingStats;

/// Per-decode cache of the `encoding_stats` of each column, so a footer materializes one
/// list per column rather than one per column chunk.
///
/// A column's `encoding_stats` is byte-identical in every row group in the typical file
/// (for example `[DATA_PAGE, PLAIN, 1]`), so a wide footer holds one copy per chunk
/// instead of one per column. The cache applies the same positional prediction as
/// [RepeatedPathCache]: chunks appear in schema order within a row group, so the n-th
/// stats list of one row group is the n-th of the next. That is a prediction, checked by
/// comparing the cached entry against the encoded bytes about to be read — a hit skips
/// the decode entirely, a miss decodes and re-primes that position. A chunk that omits
/// the field does not advance the position, so a later chunk may be compared against a
/// different column's entry; that only costs a miss, never a wrong answer, because a hit
/// requires byte-identical content, which decodes to an equal list.
///
/// The cache is typed (`List<PageEncodingStats>[]`), not `Object[]` plus a cast: the
/// `checkcast` on a hit loads the class word of a list allocated a row group earlier, a
/// cache miss per chunk.
///
/// A cache instance belongs to one [ThriftCompactReader] and is used by a single thread.
final class RepeatedEncodingStatsCache {

    private static final int INITIAL_CAPACITY = 16;

    /// The encoded `list<PageEncodingStats>` bytes of the stats at each position, as they
    /// appear in the footer, so a repeat is recognised without decoding it.
    private byte[][] encoded = new byte[INITIAL_CAPACITY][];
    private List<PageEncodingStats>[] stats = new List[INITIAL_CAPACITY];
    private int position;

    /// Starts the column chunks of a row group, so the next stats list read is predicted
    /// from the first column of the previous row group.
    void startRowGroup() {
        position = 0;
    }

    /// The `encoding_stats` list the reader is positioned on, consuming it either way.
    ///
    /// @param reader positioned on the list header of the stats
    List<PageEncodingStats> next(ThriftCompactReader reader) {
        int slot = position++;
        List<PageEncodingStats> predicted = slot < stats.length ? stats[slot] : null;
        byte[] predictedBytes = slot < encoded.length ? encoded[slot] : null;
        if (predicted != null && predictedBytes != null && reader.matchesAt(predictedBytes)) {
            reader.skipBytes(predictedBytes.length);
            return predicted;
        }
        int start = reader.position();
        List<PageEncodingStats> decoded = PageEncodingStatsReader.read(reader);
        store(slot, reader.copyRange(start, reader.position() - start), decoded);
        return decoded;
    }

    private void store(int slot, byte[] encodedStats, List<PageEncodingStats> decoded) {
        if (slot >= encoded.length) {
            int capacity = Math.max(slot + 1, encoded.length * 2);
            encoded = Arrays.copyOf(encoded, capacity);
            stats = Arrays.copyOf(stats, capacity);
        }
        encoded[slot] = encodedStats;
        stats[slot] = decoded;
    }
}
