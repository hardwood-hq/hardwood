/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

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

    private long generation;
    private boolean hasBatch;
    private int recordCount;
    private boolean closed;

    FilterCoordinator(ColumnReader[] allReaders, ColumnReader[] payloadReaders, SelectionEngine engine) {
        this.allReaders = allReaders;
        this.payloadReaders = payloadReaders;
        this.engine = engine;
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
    boolean advance() {
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
        hasBatch = true;
        generation++;
        return true;
    }

    void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (ColumnReader reader : allReaders) {
            reader.rawClose();
        }
    }
}
