/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

/// Places one flat column's pages, in page order, into the [Cell]s of the read's batch plan.
///
/// The placer follows the plan's [BatchPlan.RowGroupBatches]: it splits each page's kept records
/// into [Segment]s, one per batch they fall into, opens a cell at a batch's first record and seals
/// it when the batch holds every record the plan puts in it. It checks the column against the
/// plan, and a column that holds more or fewer rows than the plan takes from a row group fails
/// with a "Batch plan mismatch".
///
/// Records past the row cap are not placed: once the cell the cap ends is sealed, the placer
/// stops and reports the end of the read.
///
/// The placer decides; its [Sink] fills, completes and publishes, none of which the placer waits
/// for. Every method returns whether the column goes on: `false` once the read ended. The placer is
/// not thread-safe: the column's fetcher calls it under the column lock, one page at a time in
/// page order.
final class Placer {

    /// What the placer's decisions act on.
    interface Sink {

        /// Takes a cell the placer opened.
        void opened(Cell cell);

        /// Takes a cell that holds every record it will hold.
        void sealed(Cell cell);

        /// Ends the read: the row cap or the end of the stream was reached, and every cell is
        /// sealed.
        void endRead();
    }

    private final Sink sink;
    private final int batchCapacity;
    /// Whether the cells are [Cell#measured].
    private final boolean measured;

    /// The batches of the row group being placed, `null` before the first page.
    private BatchPlan.RowGroupBatches rowGroupBatches;
    /// The segment of [#rowGroupBatches] the next records go to.
    private int segment;
    /// Records the current plan segment still takes.
    private int segmentRowsLeft;
    /// Whether the column is not read in the current row group (a boundary marker stands for it).
    private boolean rowGroupSkipped;
    /// Whether the plan left a batch open across a row group the column is not read in. Only a
    /// row group that closes that batch may follow with records.
    private boolean openBatchSkipped;
    /// The cell of the batch holding the last record placed, `null` when that batch is sealed.
    private Cell openCell;
    private boolean stopped;

    /// @param batchCapacity the read's batch capacity, which every row group's batches must have
    /// @param measured whether the cells are sized from their pages' measures ([Cell#measured])
    Placer(Sink sink, int batchCapacity, boolean measured) {
        this.sink = sink;
        this.batchCapacity = batchCapacity;
        this.measured = measured;
    }

    /// Places the next page of the column: enters its row group if it starts one, then places its
    /// records. A boundary marker places nothing.
    ///
    /// @return whether the column goes on
    boolean place(PageTask page) {
        if (stopped) {
            return false;
        }
        if (page.batches != rowGroupBatches && !enterRowGroup(page.batches, page.isBoundaryMarker())) {
            return false;
        }
        if (page.isBoundaryMarker()) {
            return true;
        }
        int records = page.records();
        int r = 0;
        while (r < records) {
            if (segmentRowsLeft == 0) {
                nextSegment();
            }
            Cell cell = openCell != null ? openCell : open(page.firstRow + r);
            int count = Math.min(segmentRowsLeft, records - r);
            page.segments.add(cell.add(page, r, r + count));
            r += count;
            segmentRowsLeft -= count;
            if (segmentRowsLeft == 0 && rowGroupBatches.closes(segment) && !closeSegmentBatch()) {
                return false;
            }
        }
        return true;
    }

    /// Ends the stream of pages: checks the last row group, and ends the read.
    void end() {
        if (!stopped && finishRowGroup()) {
            endRead();
        }
    }

    /// Seals the open cell, if any, without checking it against the plan, and stops: the read
    /// failed between two work items, where every column's batch ends at the same row.
    void sealOpenCell() {
        if (!stopped) {
            sealOpen();
        }
        stopped = true;
    }

    /// Stops without sealing the open cell, and returns it: the read failed inside a work item,
    /// at a row the other columns read past, so the cell's partial batch is not published.
    ///
    /// @return the open cell, `null` if there is none
    Cell abandonOpenCell() {
        Cell cell = stopped ? null : openCell;
        openCell = null;
        stopped = true;
        return cell;
    }

    /// Opens the cell of the current plan segment's batch, whose first record is at the read's row
    /// `firstRow`. A cell takes the plan's records, or a full batch when the plan leaves the batch
    /// open past this row group.
    private Cell open(long firstRow) {
        boolean closes = rowGroupBatches.closes(segment);
        Cell cell = new Cell(rowGroupBatches.fileName(), rowGroupBatches.alwaysMatches(),
                closes ? segmentRowsLeft : Cell.UNKNOWN_RECORDS, closes ? segmentRowsLeft : batchCapacity,
                firstRow, measured);
        sink.opened(cell);
        openCell = cell;
        return cell;
    }

    /// Moves to the next row group's batches: checks the column placed every record the plan takes
    /// from the row group it leaves, seals a batch the plan closes at that row group's end or before
    /// the new row group, and takes the new row group's batches.
    ///
    /// @param skipped whether the column is not read in the row group (a boundary marker): it
    ///        places no records there, and no consumer takes its batches for them
    private boolean enterRowGroup(BatchPlan.RowGroupBatches batches, boolean skipped) {
        if (!finishRowGroup()) {
            return false;
        }
        if (batches.workItemIndex() < 0) {
            throw new IllegalStateException("Batch plan mismatch: the column reached a row group past the row cap that ended the read");
        }
        if (batches.capacity() != batchCapacity) {
            throw new IllegalStateException("Batch plan mismatch: the plan's batch capacity is "
                    + batches.capacity() + ", the column's " + batchCapacity);
        }
        if (batches.closesOpenBatch()) {
            openBatchSkipped = false;
            sealOpen();
        }
        else if (skipped ? openCell != null : openBatchSkipped) {
            throw new IllegalStateException("Batch plan mismatch: a batch spans a row group the column is not read in and one it is");
        }
        rowGroupBatches = batches;
        rowGroupSkipped = skipped;
        int segmentCount = batches.segmentCount();
        if (skipped) {
            segment = Math.max(0, segmentCount - 1);
            segmentRowsLeft = 0;
            openBatchSkipped = batches.lastOpen();
            if (batches.endsRead()) {
                endRead();
                return false;
            }
            return true;
        }
        segment = 0;
        segmentRowsLeft = segmentCount > 0 ? batches.segmentRows(0) : 0;
        return true;
    }

    /// Ends the current row group: fails unless the column placed every record the plan takes
    /// from it.
    private boolean finishRowGroup() {
        BatchPlan.RowGroupBatches batches = rowGroupBatches;
        if (batches == null || rowGroupSkipped) {
            return true;
        }
        int last = batches.segmentCount() - 1;
        if (segment < last || segmentRowsLeft != 0) {
            long placed = batches.rowsBefore(segment) + batches.segmentRows(segment) - segmentRowsLeft;
            throw new IllegalStateException("Batch plan mismatch: the column holds " + placed + " of the "
                    + batches.plannedRows() + " rows the plan takes from the row group");
        }
        return true;
    }

    /// Seals the batch the current plan segment closes, and ends the read when the row cap ends it
    /// there.
    private boolean closeSegmentBatch() {
        sealOpen();
        if (rowGroupBatches.endsRead() && segment == rowGroupBatches.segmentCount() - 1) {
            endRead();
            return false;
        }
        return true;
    }

    /// Moves to the row group's next plan segment once the current one is complete.
    ///
    /// @throws IllegalStateException if the plan takes no more records from the row group
    private void nextSegment() {
        if (segment + 1 >= rowGroupBatches.segmentCount()) {
            throw new IllegalStateException("Batch plan mismatch: the column holds more than the "
                    + rowGroupBatches.plannedRows() + " rows the plan takes from the row group");
        }
        segment++;
        segmentRowsLeft = rowGroupBatches.segmentRows(segment);
    }

    /// Seals the open cell, if any.
    private void sealOpen() {
        Cell cell = openCell;
        if (cell != null) {
            openCell = null;
            sink.sealed(cell);
        }
    }

    /// Seals the open cell, if any, and ends the read.
    private void endRead() {
        sealOpen();
        stopped = true;
        sink.endRead();
    }
}
