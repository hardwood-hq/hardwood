/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.ArrayDeque;

import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.schema.ColumnSchema;

/// A [ScheduledColumn] of a column decoded through levels into [NestedBatch]es: a leaf with
/// repetition, or with a `STRUCT` or `REPEATED` layer above it, and every column of a nested row
/// read.
///
/// Where such a page's records start, and how many values each holds, only its levels say, so a
/// page is placed once its task has decoded and measured it:
///
/// - The fetcher queues each page for the scheduler and behind the pages not yet placed. It keeps
///   at most [#MAX_UNPLACED] pages queued or decoding and not placed. A page whose metadata gives
///   no record count is queued at the column's last known row, a lower bound of its first, so it
///   is never submitted late.
/// - A page task decodes its page into arrays of the page's own, trims it to its mask's records,
///   measures it ([PageMeasure]), and places every measured page at the head of the unplaced pages
///   into the [NestedCell]s of the read's batch plan ([Placer]), in page order.
/// - A cell placement seals is filled once the placing thread has released the lock, by that
///   thread or another that finds it first among the sealed cells: every page placed into a sealed
///   cell is measured, so the cell's arrays are allocated at their final size
///   and every segment is copied from its page ([NestedFill]). That thread completes the cell and
///   publishes what can be published. A page keeps its arrays until every cell it is placed into
///   is filled.
public final class ScheduledNestedColumn extends ScheduledColumn<NestedBatch> {

    /// Pages the column keeps queued or decoding and not yet placed.
    static final int MAX_UNPLACED = ColumnWorker.MAX_INFLIGHT_PAGES;

    /// The fetcher's failure, behind the pages it queued before it.
    private record FetchFailure(Exception failure, boolean betweenWorkItems) {
    }

    private final NestedLevelComputer.Layers layers;
    private final NestedColumnWorker.IndexMode indexMode;
    private final boolean fixedListFastPathEnabled;
    /// Whether the column has repetition, so that a batch is sealed at the next record start.
    private final boolean repeated;
    /// Raised once a reader needed the views of deferred dictionary values; the column builds them
    /// as its cells complete from then on.
    private final BinaryBatchValues.ViewDemand viewDemand = new BinaryBatchValues.ViewDemand();

    // === Guarded by `lock` ===
    private final Placer<NestedCell> placer;
    /// Pages in page order not yet placed, then [#END] or the [FetchFailure] ending the stream.
    private final ArrayDeque<Object> unplaced = new ArrayDeque<>();
    /// Cells sealed and not yet filled, which the thread that sealed them fills once it has
    /// released the lock.
    private final ArrayDeque<NestedCell> sealed = new ArrayDeque<>();
    /// Set once the placer placed its last page: the read ended or failed.
    private volatile boolean placementEnded;
    /// The last page placed, which a failure at the end of the stream is placed with.
    private PageTask lastPlaced;

    // === Fetcher only ===
    private BatchPlan.RowGroupBatches rowGroup;
    /// Kept records of the current row group the fetcher queued, `-1` once a page's metadata gave
    /// no record count.
    private long rowInRowGroup;

    /// Creates a scheduled nested column and adds it to `scheduler`, which starts it.
    ///
    /// @param layers the column's layers ([NestedLevelComputer#computeLayers])
    /// @param indexMode what the column's batches carry for their reader
    public ScheduledNestedColumn(ReadScheduler scheduler, PageSource pageSource, BatchExchange<NestedBatch> exchange,
                                 ColumnSchema column, int batchCapacity, DecompressorFactory decompressorFactory,
                                 NestedLevelComputer.Layers layers, NestedColumnWorker.IndexMode indexMode,
                                 boolean fixedListFastPathEnabled) {
        super(scheduler, pageSource, exchange, column, decompressorFactory);
        this.layers = layers;
        this.indexMode = indexMode;
        this.fixedListFastPathEnabled = fixedListFastPathEnabled;
        this.repeated = column.maxRepetitionLevel() > 0;
        this.placer = new Placer<>(new PlacerSink(), batchCapacity);
    }

    // ==================== Fetcher ====================

    @Override
    PageDecoder newPageDecoder(PageInfo info) {
        return new PageDecoder(info.columnMetaData(), info.columnSchema(), decompressorFactory,
                fixedListFastPathEnabled);
    }

    @Override
    PageTask newPageTask(PageInfo info, PageDecoder decoder, long firstRow) {
        return new NestedPageTask(info, pageSource.getCurrentFileName(), pageSource.getCurrentRowGroupIndex(),
                pageSource.getCurrentPageIndex(), pageSource.getCurrentBatches(), decoder, firstRow);
    }

    @Override
    boolean queue(PageInfo info, PageDecoder decoder) {
        BatchPlan.RowGroupBatches batches = pageSource.getCurrentBatches();
        if (batches != rowGroup) {
            rowGroup = batches;
            rowInRowGroup = 0;
        }
        lock.lock();
        try {
            long firstRow = rowInRowGroup >= 0
                    ? batches.readRowsBefore() + rowInRowGroup
                    : Math.max(batches.readRowsBefore(), placer.nextRow());
            NestedPageTask page = (NestedPageTask) newPageTask(info, decoder, firstRow);
            if (info.isBoundaryMarker()) {
                page.measured = true;
                rowInRowGroup = batches.plannedRows();
            }
            else {
                ready.addLast(page);
                rowInRowGroup = rowInRowGroup >= 0 && info.records() != PageInfo.UNKNOWN_RECORDS
                        ? rowInRowGroup + info.records()
                        : -1;
            }
            unplaced.addLast(page);
            if (rowInRowGroup >= 0) {
                nextRow = Math.max(nextRow, batches.readRowsBefore() + rowInRowGroup);
            }
            placeMeasured();
        }
        finally {
            lock.unlock();
        }
        fillSealed();
        return !placementEnded;
    }

    @Override
    boolean mayQueue() {
        lock.lock();
        try {
            return ready.size() < FETCH_AHEAD && unplaced.size() < MAX_UNPLACED;
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    boolean fetchStopped() {
        return placementEnded;
    }

    @Override
    long unplacedRow() {
        for (Object head : unplaced) {
            if (head instanceof NestedPageTask page) {
                if (!page.isBoundaryMarker()) {
                    return page.firstRow;
                }
            }
            else {
                break;
            }
        }
        return Long.MAX_VALUE;
    }

    @Override
    void pagesEnded() {
        endUnplaced(END);
    }

    @Override
    void fetchFailed(Exception failure, boolean betweenWorkItems) {
        endUnplaced(new FetchFailure(failure, betweenWorkItems));
    }

    /// Ends the unplaced pages with `end`, which the placement reaches once it has placed them.
    private void endUnplaced(Object end) {
        lock.lock();
        try {
            unplaced.addLast(end);
            placeMeasured();
        }
        finally {
            lock.unlock();
        }
        fillSealed();
    }

    // ==================== Placement (column lock) ====================

    /// Places the measured pages at the head of the unplaced pages, in page order, and ends the
    /// stream once they reach its end.
    private void placeMeasured() {
        int before = unplaced.size();
        Object head;
        while (!done && !placementEnded && (head = unplaced.peekFirst()) != null) {
            if (head instanceof NestedPageTask page) {
                if (!page.measured) {
                    break;
                }
                unplaced.pollFirst();
                place(page);
            }
            else {
                unplaced.pollFirst();
                placementEnded = true;
                end(head);
            }
        }
        if (placementEnded) {
            // Nothing is placed after the end: the pages left over past a row cap are dropped,
            // and those not submitted yet are not decoded
            unplaced.clear();
            ready.clear();
            lastPlaced = null;
        }
        if (!fetchEnded) {
            nextRow = Math.max(nextRow, placer.nextRow());
        }
        updatePublishedRow();
        if (unplaced.size() < before) {
            // The fetcher may wait for room among the unplaced pages
            unparkFetcher();
        }
    }

    private void place(NestedPageTask page) {
        try {
            if (!placer.place(page, page.measure, repeated)) {
                placementEnded = true;
            }
            page.endRow[0] = placer.nextRow();
            lastPlaced = page;
        }
        catch (RuntimeException e) {
            placementEnded = true;
            pageFailed(e, page);
        }
    }

    /// Ends the placement at the end of the stream or at the fetcher's failure.
    private void end(Object end) {
        if (end instanceof FetchFailure(Exception failure, boolean betweenWorkItems)) {
            NestedCell abandoned = null;
            if (betweenWorkItems) {
                placer.sealOpenCell();
            }
            else {
                abandoned = placer.abandonOpenCell();
            }
            endWithFailure(failure, abandoned);
            return;
        }
        try {
            placer.end();
        }
        catch (RuntimeException e) {
            if (lastPlaced != null) {
                pageFailed(e, lastPlaced);
            }
            else {
                signalError(ColumnWorker.asReadFailure(e));
            }
        }
    }

    /// What the placer's decisions do to the column; runs under the column lock.
    private final class PlacerSink implements Placer.Sink<NestedCell> {

        @Override
        public NestedCell open(String fileName, boolean alwaysMatches, int plannedRecords, int capacity,
                               long firstRow) {
            NestedCell cell = new NestedCell(fileName, alwaysMatches, firstRow);
            publishOrder.addLast(cell);
            return cell;
        }

        @Override
        public void add(NestedCell cell, PageTask page, int recordStart, int recordEnd, int valueStart,
                        int valueEnd) {
            cell.add((NestedPageTask) page, recordStart, recordEnd, valueStart, valueEnd);
        }

        @Override
        public void sealed(NestedCell cell) {
            sealed.addLast(cell);
        }

        @Override
        public void endRead() {
            publishOrder.addLast(END);
            publishReady();
        }
    }

    // ==================== Page task ====================

    /// Decodes and measures a page, places what the measure lets the column place, and fills the
    /// cells that placement sealed.
    @Override
    void runPage(PageTask task) {
        NestedPageTask page = (NestedPageTask) task;
        try {
            if (done) {
                return;
            }
            PageInfo info = page.info;
            Page decoded = info.isNullPlaceholder()
                    ? page.decoder.nullPage(info.placeholderNumValues())
                    : page.decoder.decodePage(info.pageData(), info.dictionary(), page.scratch);
            PageRowMask mask = info.mask();
            if (!mask.isAll()) {
                decoded = PageTrimmer.trim(decoded, mask);
            }
            page.page = decoded;
            page.measure = PageMeasure.of(decoded, info.records(), page.scratch);
            lock.lock();
            try {
                page.measured = true;
                placeMeasured();
            }
            finally {
                lock.unlock();
            }
            fillSealed();
        }
        catch (Exception e) {
            pageFailed(e, page);
        }
        catch (Error err) {
            signalError(err);
            throw err;
        }
        finally {
            taskEnded();
        }
    }

    /// Fills, completes and publishes the cells sealed so far, outside the lock.
    private void fillSealed() {
        while (true) {
            NestedCell cell;
            lock.lock();
            try {
                cell = done ? null : sealed.pollFirst();
            }
            finally {
                lock.unlock();
            }
            if (cell == null) {
                return;
            }
            try {
                cell.filled(NestedFill.fill(cell, exchange, column, layers, indexMode, viewDemand));
                // The cell holds what it was filled from no longer: a cell waiting for room in the
                // exchange would otherwise keep its pages' arrays alive
                cell.dropSegments();
            }
            catch (RuntimeException e) {
                // The cell cannot complete, and a failure queued behind it would never be raised
                pageFailed(e, cell.lastPage());
                return;
            }
            completed(cell);
        }
    }
}
