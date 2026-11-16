/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.pipeline;

import dev.hardwood.internal.reader.batch.FlatBatch;
import dev.hardwood.internal.reader.batch.NestedBatch;

/// A column's batch under construction: the work of one decoded column for one batch of the batch
/// plan, as its publish order sees it. A [Cell] fills a flat column's [FlatBatch], a
/// [NestedCell] a [NestedBatch] of a column decoded through levels. The cell ends at publish; the
/// column's batch it filled goes on to the consumer.
///
/// @param <B> the type of the column's batch
public interface BatchCell<B> {

    /// The read's row of the batch's first record.
    long firstRow();

    /// The index of its batch among the read's batches, set as the cell is opened.
    long batchIndex();

    void batchIndex(long index);

    /// Whether the cell is completed and waits to be published; read and set under the column
    /// lock.
    boolean isCompleted();

    void markCompleted();

    /// The column's batch, once the cell is completed.
    B columnBatch();

    /// Drops what the cell references once it is published. A page usually spans several cells
    /// and reaches each through its segments until its last cell is published; a published cell
    /// that kept the column's batch would keep its arrays alive as long as the page.
    void release();
}
