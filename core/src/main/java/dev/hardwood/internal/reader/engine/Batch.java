/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.util.ArrayList;
import java.util.List;

import dev.hardwood.internal.reader.pipeline.BatchCell;

/// One batch of a read's batch plan, which may span row groups: per column, the cell its
/// placement opened for it and whether the cell is complete. Written under the read's lock.
final class Batch {

    /// the key of its first row, and the key past its last row of the row groups batched so far
    private final long key;
    private long endKey;
    /// the read's row it starts at, which a filter makes fewer than its key
    private final long row;
    private final BatchCell<?>[] cells;
    private final boolean[] complete;
    private int columnsLeft;
    /// the ranges its pages were decoded from, held until every column has published it
    private final List<RangeFetch> held = new ArrayList<>(0);

    Batch(long key, long row, int columns) {
        this.key = key;
        this.row = row;
        this.endKey = key;
        this.cells = new BatchCell<?>[columns];
        this.complete = new boolean[columns];
        this.columnsLeft = columns;
    }

    /// The key of its first row.
    long key() {
        return key;
    }

    /// The key past its last row of the row groups batched so far.
    long endKey() {
        return endKey;
    }

    /// A row group the plan continues it in extends it to `endKey`.
    void extendTo(long endKey) {
        this.endKey = endKey;
    }

    /// The read's row it starts at.
    long row() {
        return row;
    }

    /// The cell `column` opened for it, `null` where none is open.
    BatchCell<?> cell(int column) {
        return cells[column];
    }

    /// `column`'s placement opened `cell` for it.
    void open(int column, BatchCell<?> cell) {
        cells[column] = cell;
    }

    /// Whether `column`'s cell for it is complete, or the column is not read in it.
    boolean complete(int column) {
        return complete[column];
    }

    /// `column`'s cell for it is complete, or the column is not read in it.
    void completed(int column) {
        complete[column] = true;
    }

    /// Whether `column` is not read in it: complete without a cell.
    boolean skips(int column) {
        return complete[column] && cells[column] == null;
    }

    /// It holds `ranges` until every column has published it.
    void hold(List<RangeFetch> ranges) {
        held.addAll(ranges);
    }

    /// `column` is done with it; returns whether it was the last column, after which the ranges it
    /// holds are returned.
    boolean passed(int column) {
        cells[column] = null;
        return --columnsLeft == 0;
    }

    /// The ranges it holds.
    List<RangeFetch> held() {
        return held;
    }
}
