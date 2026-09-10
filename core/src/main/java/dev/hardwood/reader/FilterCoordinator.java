/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.IOException;

import dev.hardwood.internal.reader.RecordFilterTally;
import dev.hardwood.internal.reader.RowGroupIterator;

/// Drives the filtered column-reader path (#624). It advances every reader of
/// the augmented projection (payload columns plus the predicate columns) in
/// lockstep, asks the [SelectionEngine] for the matching records of the aligned
/// batch, and compacts each exposed payload reader down to those records.
///
/// A monotonically increasing `generation` lets sibling readers advanced
/// individually (e.g. `a.nextBatch() & b.nextBatch()`) share a single advance:
/// the first reader to reach the current generation triggers the work; the rest
/// observe the already-produced batch.
final class FilterCoordinator {

    /// Every reader of the augmented projection (payload + predicate columns),
    /// indexed by augmented projected column index. All are advanced and closed
    /// together; the [SelectionEngine] reads predicate values from them.
    private final ColumnReader[] allReaders;
    /// The exposed subset, compacted to the matching records each batch.
    private final ColumnReader[] payloadReaders;
    private final SelectionEngine engine;
    /// The iterator all readers decode through. A filtered single-column read is
    /// handed one reader out of the projection and never sees the enclosing group,
    /// so tearing the projection down has to release the iterator too.
    private final RowGroupIterator rowGroupIterator;
    /// Per-file record-filter counts for JFR. Every batch this path produces has
    /// passed through the selection, so the counts are complete for the read.
    private final RecordFilterTally tally = new RecordFilterTally();

    private long generation;
    private boolean hasBatch;
    private int recordCount;
    private boolean closed;

    FilterCoordinator(ColumnReader[] allReaders, ColumnReader[] payloadReaders, SelectionEngine engine,
                      RowGroupIterator rowGroupIterator) {
        this.allReaders = allReaders;
        this.payloadReaders = payloadReaders;
        this.engine = engine;
        this.rowGroupIterator = rowGroupIterator;
    }

    long generation() {
        return generation;
    }

    boolean hasBatch() {
        return hasBatch;
    }

    int recordCount() {
        return recordCount;
    }

    /// Advances to the next aligned batch, computes the selection, and compacts
    /// the payload readers. Returns `false` (without incrementing the
    /// generation) once the input is exhausted.
    boolean advance() throws IOException {
        if (!allReaders[0].rawNextBatch()) {
            hasBatch = false;
            recordCount = 0;
            // Drain the remaining readers so the shared iterator finalizes cleanly.
            for (int i = 1; i < allReaders.length; i++) {
                allReaders[i].rawNextBatch();
            }
            return false;
        }
        int firstCount = allReaders[0].rawRecordCount();
        for (int i = 1; i < allReaders.length; i++) {
            if (!allReaders[i].rawNextBatch()) {
                throw new IllegalStateException(
                        "ColumnReader '" + allReaders[i].getColumnSchema().name()
                                + "' exhausted before peer column '"
                                + allReaders[0].getColumnSchema().name()
                                + "' — readers from the same projection must advance in lockstep");
            }
            int count = allReaders[i].rawRecordCount();
            if (count != firstCount) {
                throw new IllegalStateException(
                        "ColumnReader batch sizes diverged: column '"
                                + allReaders[0].getColumnSchema().name() + "' has " + firstCount
                                + " records, column '" + allReaders[i].getColumnSchema().name()
                                + "' has " + count);
            }
        }

        // Compute the selection from the raw (pre-compaction) batches, then
        // compact the exposed payload readers. Order matters: a payload column
        // that is also the predicate column must be read before it is compacted.
        // `kept` is the engine's reusable buffer; all payload readers consume it
        // here, before the next advance overwrites it.
        int matchCount = engine.computeSelection(firstCount);
        int[] kept = engine.selection();
        for (ColumnReader reader : payloadReaders) {
            reader.applySelection(kept, matchCount);
        }

        recordCount = matchCount < 0 ? firstCount : matchCount;
        tally.switchFile(allReaders[0].currentFileName());
        tally.recordBatch(firstCount, recordCount);
        hasBatch = true;
        generation++;
        return true;
    }

    void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        tally.close();
        // Released even when a reader's teardown fails: this coordinator will not
        // run its close body again, so skipping the release would leave the work
        // list reachable for the parent reader's whole lifetime.
        try (rowGroupIterator) {
            for (ColumnReader reader : allReaders) {
                reader.rawClose();
            }
        }
    }
}
