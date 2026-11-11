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
    final long key;
    long endKey;
    /// the read's row it starts at, which a filter makes fewer than its key
    final long row;
    final BatchCell<?>[] cells;
    final boolean[] complete;
    int columnsLeft;
    /// the ranges its pages were decoded from, held until every column has published it
    final List<RangeFetch> held = new ArrayList<>(0);

    Batch(long key, long row, int columns) {
        this.key = key;
        this.row = row;
        this.endKey = key;
        this.cells = new BatchCell<?>[columns];
        this.complete = new boolean[columns];
        this.columnsLeft = columns;
    }
}
