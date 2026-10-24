/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.pipeline;

import java.util.ArrayList;
import java.util.List;

import dev.hardwood.internal.reader.batch.NestedBatch;

/// The work of one [ScheduledNestedColumn] for one planned batch: the [NestedSegment]s of the
/// measured pages placed into it, in page order.
///
/// A cell goes through these steps, each once:
///
/// 1. **Opened** by the [Placer] at its batch's first record.
/// 2. **Segments placed**, in page order, each from a page already measured.
/// 3. **Sealed** when the batch holds every record it will hold: for a column with repetition
///    once the next record start, the end of the row group or the end of the read shows its last
///    record is complete.
/// 4. **Filled**, once it is sealed, by a thread that takes it off the column's sealed cells,
///    which allocates its arrays at their final size from its segments' measures, copies every
///    segment from its page ([NestedFill]) and drops the segments.
/// 5. **Completed** by that thread, and published in plan order.
///
/// The segments and totals are guarded by the column lock until the cell is sealed; the thread
/// that takes it off the sealed cells then owns it until it is completed.
final class NestedCell implements ScheduledCell<NestedBatch> {

    final String fileName;
    final boolean alwaysMatches;
    private final long firstRow;

    private final List<NestedSegment> segments = new ArrayList<>(2);
    private int records;
    private int values;
    /// The page of the last segment placed, which a failure while filling the cell is placed with.
    private PageTask lastPage;

    private NestedBatch batch;
    /// Guarded by the column lock: whether the cell is completed and waits to be published.
    private boolean completed;

    NestedCell(String fileName, boolean alwaysMatches, long firstRow) {
        this.fileName = fileName;
        this.alwaysMatches = alwaysMatches;
        this.firstRow = firstRow;
    }

    /// Places the page's records `[recordStart, recordEnd)`, which hold its values
    /// `[valueStart, valueEnd)`, into this cell after every segment placed so far.
    void add(NestedPageTask page, int recordStart, int recordEnd, int valueStart, int valueEnd) {
        segments.add(new NestedSegment(page, recordStart, recordEnd, valueStart, valueEnd, records, values));
        records += recordEnd - recordStart;
        values += valueEnd - valueStart;
        lastPage = page;
    }

    List<NestedSegment> segments() {
        return segments;
    }

    int records() {
        return records;
    }

    int values() {
        return values;
    }

    PageTask lastPage() {
        return lastPage;
    }

    /// Drops the segments, and with them the pages, once the cell is filled.
    void dropSegments() {
        segments.clear();
        lastPage = null;
    }

    /// Sets the batch the cell's fill produced.
    void filled(NestedBatch batch) {
        this.batch = batch;
    }

    @Override
    public long firstRow() {
        return firstRow;
    }

    @Override
    public boolean isCompleted() {
        return completed;
    }

    @Override
    public void markCompleted() {
        completed = true;
    }

    @Override
    public NestedBatch batch() {
        return batch;
    }

    @Override
    public void release() {
        segments.clear();
        lastPage = null;
        batch = null;
    }
}
