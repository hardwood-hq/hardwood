/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import dev.hardwood.reader.ParquetReadException;

/// Places one column's pages, in page order, into the cells of the read's batch plan.
///
/// The placer follows the plan's [BatchPlan.RowGroupBatches]: it splits each page's kept records
/// into segments, one per batch they fall into, opens a cell at a batch's first record and seals
/// it when the batch holds every record it will hold. It checks the column against the plan, and
/// a column that holds more or fewer rows than the plan takes from a row group fails with a
/// "Batch plan mismatch".
///
/// A record never spans two batches. A column without repetition seals a batch as soon as its
/// last planned record is placed, and is placed from its pages' metadata ([#place(PageTask)]). A
/// column with repetition is placed from its pages' measures ([#place(PageTask, PageMeasure)]) and
/// seals a batch at the next record start, at the end of the row group or at the end of the read,
/// since a record's last values may lie on a later page: the values a page begins with before its
/// first record start go, as a segment of no records, into the cell holding the previous page's
/// last record. A column chunk whose first page begins inside a record fails, placed at that page.
///
/// Records past the row cap are not placed: once the cell the cap ends is sealed, the placer
/// stops and reports the end of the read.
///
/// The placer decides; its [Sink] fills, completes and publishes, none of which the placer waits
/// for. Every method returns whether the column goes on: `false` once the read ended. The placer is
/// not thread-safe: the column calls it under the column lock, one page at a time in page order.
///
/// @param <C> the cell type
final class Placer<C> {

    /// What the placer's decisions act on.
    interface Sink<C> {

        /// Opens the cell of a batch whose first record is the read's row `firstRow`.
        ///
        /// @param plannedRecords the records the plan puts in the batch when the plan closes it in
        ///        the row group it opens in, [Cell#UNKNOWN_RECORDS] otherwise
        /// @param capacity the records a cell of a column without repetition takes: the planned
        ///        records, or the batch capacity when the plan leaves the batch open
        C open(String fileName, boolean alwaysMatches, int plannedRecords, int capacity, long firstRow);

        /// Places the page's kept records `[recordStart, recordEnd)`, which hold its kept values
        /// `[valueStart, valueEnd)`, into `cell` after every segment placed so far.
        void add(C cell, PageTask page, int recordStart, int recordEnd, int valueStart, int valueEnd);

        /// Takes a cell that holds every record it will hold.
        void sealed(C cell);

        /// Ends the read: the row cap or the end of the stream was reached, and every cell is
        /// sealed.
        void endRead();
    }

    private final Sink<C> sink;
    private final int batchCapacity;

    /// The batches of the row group being placed, `null` before the first page.
    private BatchPlan.RowGroupBatches rowGroupBatches;
    /// The segment of [#rowGroupBatches] the next records go to.
    private int segment;
    /// Records the current plan segment still takes.
    private int segmentRowsLeft;
    /// Records of the current row group placed so far.
    private long rowsPlaced;
    /// Whether the column is not read in the current row group (a boundary marker stands for it).
    private boolean rowGroupSkipped;
    /// Whether the plan left a batch open across a row group the column is not read in. Only a
    /// row group that closes that batch may follow with records.
    private boolean openBatchSkipped;
    /// Whether a page with values has been placed in the current column chunk.
    private boolean chunkStarted;
    /// The cell of the batch holding the last record placed, `null` when that batch is sealed.
    private C openCell;
    private boolean stopped;

    /// @param batchCapacity the read's batch capacity, which every row group's batches must have
    Placer(Sink<C> sink, int batchCapacity) {
        this.sink = sink;
        this.batchCapacity = batchCapacity;
    }

    /// Places the next page of a column without repetition, whose kept records its metadata
    /// gives, one value each: enters its row group if it starts one, then places its records. A
    /// boundary marker places nothing.
    ///
    /// @return whether the column goes on
    boolean place(PageTask page) {
        if (!enter(page)) {
            return false;
        }
        return page.isBoundaryMarker() || placeRecords(page, page.records(), null);
    }

    /// Places the next page of a column decoded through levels, from its measure: one record per
    /// value for a column without repetition, records at its repetition levels' zeros otherwise.
    ///
    /// @param repeated whether the column has repetition
    /// @return whether the column goes on
    /// @throws ParquetReadException if the page begins its column chunk inside a record
    boolean place(PageTask page, PageMeasure measure, boolean repeated) {
        if (!enter(page)) {
            return false;
        }
        if (page.isBoundaryMarker()) {
            return true;
        }
        return repeated ? placeRecordsWithRepetition(page, measure) : placeRecords(page, measure.records(), measure);
    }

    /// Enters the page's row group if it starts one.
    private boolean enter(PageTask page) {
        if (stopped) {
            return false;
        }
        return page.batches == rowGroupBatches || enterRowGroup(page.batches, page.isBoundaryMarker());
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
    C abandonOpenCell() {
        C cell = stopped ? null : openCell;
        openCell = null;
        stopped = true;
        return cell;
    }

    /// The read's row the column's next record is at: the plan's rows before the current row
    /// group and the records placed in it, every planned record of a row group the column is not
    /// read in counted. `0` before the first page.
    long nextRow() {
        BatchPlan.RowGroupBatches batches = rowGroupBatches;
        if (batches == null) {
            return 0;
        }
        return batches.readRowsBefore() + (rowGroupSkipped ? batches.plannedRows() : rowsPlaced);
    }

    /// Places `records` records of a column without repetition. Record `r` holds value
    /// `measure.recordStart(r)`, or value `r` when there is no measure.
    private boolean placeRecords(PageTask page, int records, PageMeasure measure) {
        int r = 0;
        while (r < records) {
            if (segmentRowsLeft == 0) {
                nextSegment();
            }
            C cell = openCell != null ? openCell : open();
            int count = Math.min(segmentRowsLeft, records - r);
            int valueStart = measure != null ? measure.recordStart(r) : r;
            int valueEnd = measure != null ? measure.recordStart(r + count) : r + count;
            add(cell, page, r, r + count, valueStart, valueEnd);
            r += count;
            if (segmentRowsLeft == 0 && rowGroupBatches.closes(segment) && !closeSegmentBatch()) {
                return false;
            }
        }
        return true;
    }

    /// Places the records of a measured page of a column with repetition. The batch of a
    /// complete plan segment is sealed at the next record start.
    private boolean placeRecordsWithRepetition(PageTask page, PageMeasure measure) {
        if (measure.values() == 0) {
            return true;
        }
        if (measure.leadingValues() > 0) {
            if (!chunkStarted) {
                throw new ParquetReadException("Invalid column chunk: first repetition level must be 0 but was "
                        + measure.firstRepetitionLevel());
            }
            // The values continue the last record placed, which the open cell holds: a cell of a
            // column with repetition is sealed only at a record start or at the end of its chunk.
            if (openCell == null) {
                throw new IllegalStateException("A page continues a record no open batch holds");
            }
            sink.add(openCell, page, 0, 0, 0, measure.leadingValues());
        }
        chunkStarted = true;
        int records = measure.records();
        int r = 0;
        while (r < records) {
            if (segmentRowsLeft == 0 && !closeSegmentAtRecordStart()) {
                return false;
            }
            C cell = openCell != null ? openCell : open();
            int count = Math.min(segmentRowsLeft, records - r);
            add(cell, page, r, r + count, measure.recordStart(r), measure.recordStart(r + count));
            r += count;
        }
        return true;
    }

    private void add(C cell, PageTask page, int recordStart, int recordEnd, int valueStart, int valueEnd) {
        sink.add(cell, page, recordStart, recordEnd, valueStart, valueEnd);
        int count = recordEnd - recordStart;
        segmentRowsLeft -= count;
        rowsPlaced += count;
    }

    /// Opens the cell of the current plan segment's batch at the column's next record. A cell
    /// takes the plan's records, or a full batch when the plan leaves the batch open past this
    /// row group.
    private C open() {
        boolean closes = rowGroupBatches.closes(segment);
        C cell = sink.open(rowGroupBatches.fileName(), rowGroupBatches.alwaysMatches(),
                closes ? segmentRowsLeft : Cell.UNKNOWN_RECORDS, closes ? segmentRowsLeft : batchCapacity,
                nextRow());
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
        // A batch of a column with repetition that the row cap ends waits for its last record to
        // complete, which the first page past the cap shows.
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
        rowsPlaced = 0;
        chunkStarted = false;
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
    /// from it, then seals a batch the plan closes at its end that is still open, which is a
    /// batch of a column with repetition waiting for its last record to complete.
    private boolean finishRowGroup() {
        BatchPlan.RowGroupBatches batches = rowGroupBatches;
        if (batches == null || rowGroupSkipped) {
            return true;
        }
        int last = batches.segmentCount() - 1;
        if (segment < last || segmentRowsLeft != 0) {
            throw new IllegalStateException("Batch plan mismatch: the column holds " + rowsPlaced + " of the "
                    + batches.plannedRows() + " rows the plan takes from the row group");
        }
        if (last >= 0 && batches.closes(last) && openCell != null) {
            return closeSegmentBatch();
        }
        return true;
    }

    /// At a record start with the current plan segment complete: seals its batch where the plan
    /// closes it and moves to the next segment.
    private boolean closeSegmentAtRecordStart() {
        if (rowGroupBatches.closes(segment) && openCell != null && !closeSegmentBatch()) {
            return false;
        }
        nextSegment();
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
        C cell = openCell;
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
