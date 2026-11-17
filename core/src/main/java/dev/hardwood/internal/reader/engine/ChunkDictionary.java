/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.util.function.LongConsumer;

import dev.hardwood.internal.reader.batch.Dictionary;

/// A column chunk's dictionary in a read: its values, the [Output] of its dictionary page's decode
/// or ended from the start where the unit's dictionary pruning decoded them, which the chunk's data
/// pages wait on. A numeric column's batches hold the values their pages took from it, not the
/// dictionary, so the context's ledger holds its entries until every data page of the chunk has
/// decoded ([#holdUntilDecoded]); a binary column's batches refer to its entries, which ride on
/// them instead ([BatchPublisher#ride]). Used under the read's lock.
final class ChunkDictionary {

    private final Output<Dictionary> values;
    /// the chunk's data pages added whose decode has not ended
    private int pages;
    /// whether the chunk's data pages are all added
    private boolean sealed;
    /// the bytes held for its entries until its pages have decoded, and what returns them
    private long held;
    private LongConsumer release;

    ChunkDictionary() {
        this(new Output<>());
    }

    private ChunkDictionary(Output<Dictionary> values) {
        this.values = values;
    }

    /// A dictionary the unit's dictionary pruning decoded, whose values are there from the start.
    static ChunkDictionary preloaded(Dictionary values) {
        return new ChunkDictionary(Output.of(values));
    }

    /// Its dictionary page's decode's output: its values, or the decode's failure.
    Output<Dictionary> output() {
        return values;
    }

    /// A data page of the chunk is added, which decodes with the dictionary.
    void pageAdded() {
        pages++;
    }

    /// The decode of a data page of the chunk has ended, or the page will not decode.
    void pageEnded() {
        pages--;
        returnHeld();
    }

    /// Every data page of the chunk is added.
    void seal() {
        sealed = true;
        returnHeld();
    }

    /// Holds `bytes` charged for its entries until every data page of the chunk has decoded, then
    /// hands them to `release`.
    void holdUntilDecoded(long bytes, LongConsumer release) {
        this.held = bytes;
        this.release = release;
        returnHeld();
    }

    private void returnHeld() {
        if (sealed && pages == 0 && held > 0) {
            release.accept(held);
            held = 0;
        }
    }

}
