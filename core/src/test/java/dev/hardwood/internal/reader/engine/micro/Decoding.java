/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine.micro;

import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.IntStream;

/// The decoding stage: the walked pages waiting for their decode, the decode slots, and every
/// column's decoded rows. Its share of the engine's poll, [#poll], takes the pages walked, which
/// the poll passes in, and decodes the waiting page of the lowest row while a slot is free; it
/// reads no other stage. The batches are built from its rows ([#decodedBefore],
/// [#takeRows]).
final class Decoding {

    private final Slots slots;
    /// walked pages waiting for their decode, lowest row first across all columns
    private final TreeSet<Page> waiting = new TreeSet<>(
            Comparator.comparingLong(Page::firstRow).thenComparingInt(Page::column));
    private final List<DecodedRows> decoded;

    Decoding(int columns, int maxDecodes) {
        this.slots = new Slots(maxDecodes);
        this.decoded = IntStream.range(0, columns).mapToObj(c -> new DecodedRows()).toList();
    }

    // --- the rule ---

    void poll(List<Page> walked, List<Command> out) {
        waiting.addAll(walked);
        while (slots.free() && !waiting.isEmpty()) {
            slots.take();
            out.add(new Command.Decode(waiting.pollFirst()));
        }
    }

    /// Whether every column has decoded every row before `row`.
    boolean decodedBefore(long row) {
        for (DecodedRows column : decoded) {
            if (!column.decodedBefore(row)) {
                return false;
            }
        }
        return true;
    }

    /// Takes rows `[first, end)` of every column, every one of them decoded.
    long[][] takeRows(long first, long end) {
        long[][] values = new long[decoded.size()][];
        for (int c = 0; c < decoded.size(); c++) {
            values[c] = decoded.get(c).takeRows(first, end);
        }
        return values;
    }

    // --- facts ---

    void decoded(Page page, long[] values) {
        slots.give();
        decoded.get(page.column()).decoded(page, values);
    }

    void decodeFailed() {
        slots.give();
    }

    @Override
    public String toString() {
        return "decoding[slots=" + slots + " waiting=" + waiting.size() + "]";
    }
}
