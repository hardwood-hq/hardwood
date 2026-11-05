/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine.micro;

import java.util.ArrayDeque;

/// The consumer's side: which batch is built next, those built that the consumer has not taken,
/// and from them the consumer's position and demand.
final class Output {

    private final int batchRows;
    private final long totalRows;
    private final long batchCount;
    /// the next batch to build
    private long nextBatch;
    private final ArrayDeque<Batch> built = new ArrayDeque<>();

    Output(int batchRows, long totalRows) {
        this.batchRows = batchRows;
        this.totalRows = totalRows;
        this.batchCount = (totalRows + batchRows - 1) / batchRows;
    }

    // --- questions ---

    /// Whether every batch is built.
    boolean allBuilt() {
        return nextBatch == batchCount;
    }

    /// The first row of the next batch to build.
    long nextFirst() {
        return nextBatch * batchRows;
    }

    /// The end of the next batch to build.
    long nextEnd() {
        return Math.min(nextFirst() + batchRows, totalRows);
    }

    /// The first row the consumer has not taken. A batch built and not taken still counts, so a
    /// consumer that stops taking holds this row where it is.
    long consumerRow() {
        return (nextBatch - built.size()) * batchRows;
    }

    /// The consumer's demand, from the rows it has taken.
    Demand demand() {
        return Demand.at(consumerRow());
    }

    /// Whether a batch is built and not taken.
    boolean hasBuilt() {
        return !built.isEmpty();
    }

    boolean done() {
        return allBuilt() && built.isEmpty();
    }

    // --- facts ---

    /// `batch`, the next, is built.
    void built(Batch batch) {
        if (batch.firstRow() != nextFirst()) {
            throw new IllegalStateException("batch at " + batch.firstRow() + " built out of order, next is " + nextFirst());
        }
        built.add(batch);
        nextBatch++;
    }

    /// The consumer takes the next batch built, `null` if none is.
    Batch take() {
        return built.poll();
    }

    @Override
    public String toString() {
        return "built=" + built.size() + " consumerRow=" + consumerRow();
    }
}
