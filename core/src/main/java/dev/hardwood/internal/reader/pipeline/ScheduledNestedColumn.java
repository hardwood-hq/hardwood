/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.pipeline;

import java.util.ArrayDeque;
import java.util.concurrent.ArrayBlockingQueue;

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.batch.BinaryBatchValues;
import dev.hardwood.internal.reader.batch.NestedBatch;
import dev.hardwood.internal.reader.batch.NestedIndexMode;
import dev.hardwood.internal.reader.batch.NestedLevelComputer;
import dev.hardwood.internal.reader.decode.PageDecoder;
import dev.hardwood.internal.reader.decode.PageInfo;
import dev.hardwood.internal.reader.decode.PageTrimmer;
import dev.hardwood.internal.reader.plan.BatchPlan;
import dev.hardwood.internal.reader.plan.PageSource;
import dev.hardwood.internal.reader.select.PageRowMask;
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
/// - A page task reads its page in two passes: it is decompressed into its own scratch and only its
///   levels and ids are decoded, its values left in place for the fill; a masked page keeps the
///   levels of its kept records and where their values lie
///   ([dev.hardwood.internal.reader.decode.KeptRuns]). The task measures the page ([PageMeasure])
///   and places every measured page at the head of the unplaced pages into the [NestedCell]s of the
///   read's batch plan ([Placer]), in page order. A page borrows its scratch from the column's pool
///   and returns it once every cell it is placed into is filled.
/// - A cell placement seals is filled on the decode pool once the placing task has released the
///   lock, by that task or another that finds it first among the sealed cells: every page placed
///   into a sealed cell is measured, so the cell's arrays are allocated at their final size and
///   every segment is written from its page ([NestedFill]). That task completes the cell and
///   publishes what can be published. The arrays are new for every batch: values a reader
///   returned refer to them.
/// - The fetcher places too: a row group the column is not read in, the end of the stream and its
///   own failure, once every page before them is placed. It fills nothing. A cell its placement
///   seals is filled by a [SealedFillTask], which it queues ahead of its queued pages, so a cell
///   is filled on the pool even where no page task is left to fill it.
public final class ScheduledNestedColumn extends ScheduledColumn<NestedBatch> {

    /// Pages the column keeps queued or decoding and not yet placed.
    static final int MAX_UNPLACED = ScheduledColumn.MAX_INFLIGHT_PAGES;

    /// Scratch a column keeps for its next pages once their pages' cells are all filled. Fixed,
    /// and not tied to [#MAX_UNPLACED]: a scratch is sized by the largest page it held, so a
    /// larger pool keeps many times a large-page file's live scratch alive and saves little
    /// allocation.
    static final int SCRATCH_POOL = 4;

    /// Scratch of pages whose cells are all filled, for the column's next pages.
    private final ArrayBlockingQueue<PageDecoder.LevelScratch> scratchPool = newScratchPool();

    /// A pool for the scratch of a column's pages whose cells are all filled.
    static ArrayBlockingQueue<PageDecoder.LevelScratch> newScratchPool() {
        return new ArrayBlockingQueue<>(SCRATCH_POOL);
    }

    /// Told of every cell as it is filled, on the filling thread; for tests of where cells are
    /// filled. `null` for none.
    static volatile FillObserver fillObserver;

    /// Sees each cell the column fills, before its fill.
    interface FillObserver {

        void filling(ScheduledNestedColumn column, NestedCell cell);
    }

    /// The fetcher's failure, behind the pages it queued before it.
    private record FetchFailure(Exception failure, boolean betweenWorkItems) {
    }

    private final NestedLevelComputer.Layers layers;
    private final NestedIndexMode indexMode;
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
    /// Cells sealed and not yet filled, which the page task that sealed them fills once it has
    /// released the lock, or a [SealedFillTask] where the fetcher sealed them.
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
                                 NestedLevelComputer.Layers layers, NestedIndexMode indexMode,
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
                fixedListFastPathEnabled, pageSource.workingSet(info.columnSchema()));
    }

    @Override
    PageTask newPageTask(PageInfo info, PageDecoder decoder, long firstRow) {
        PageDecoder.LevelScratch scratch = scratchPool.poll();
        return new NestedPageTask(info, pageSource.getCurrentFileName(), pageSource.getCurrentRowGroupIndex(),
                pageSource.getCurrentPageIndex(), pageSource.getCurrentBatches(), decoder, firstRow,
                scratch != null ? scratch : new PageDecoder.LevelScratch());
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
            placeAndQueueSealedFill();
        }
        finally {
            lock.unlock();
        }
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
    /// The fetcher submits the queue's change as it exits.
    private void endUnplaced(Object end) {
        lock.lock();
        try {
            unplaced.addLast(end);
            placeAndQueueSealedFill();
        }
        finally {
            lock.unlock();
        }
    }

    /// Places the measured pages on the fetcher (lock held) and, if that sealed a cell, queues a
    /// [SealedFillTask] ahead of the queued pages for the sealed cells not yet filled, unless one
    /// is queued already. Every queued page starts at or after the cells placed so far, so the
    /// queue stays in row order. A cell a page task seals needs no such task: the page task fills
    /// the sealed cells once it has placed.
    private void placeAndQueueSealedFill() {
        int sealedBefore = sealed.size();
        placeMeasured();
        NestedCell first = sealed.peekFirst();
        if (sealed.size() > sealedBefore && !done && !(ready.peekFirst() instanceof SealedFillTask)) {
            ready.addFirst(new SealedFillTask(first.firstRow()));
        }
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
            // and those not submitted yet are not decoded. A queued SealedFillTask stays: the
            // cells the fetcher sealed before have no other task to fill them
            unplaced.clear();
            ready.removeIf(task -> !(task instanceof SealedFillTask));
            lastPlaced = null;
        }
        if (!fetchEnded) {
            nextRow = Math.max(nextRow, placer.nextRow());
        }
        updatePublishedRow();
        if (unplaced.size() < before) {
            // The fetch may have stopped for room among the unplaced pages
            scheduleFetch();
        }
    }

    private void place(NestedPageTask page) {
        try {
            boolean goesOn = placer.place(page, page.measure, repeated);
            // Placement of the page has ended: every cell it reaches holds its segment, and none of
            // them is filled before the lock is released
            if (page.unfilledSegments.get() == 0) {
                releaseScratch(page);
            }
            if (!goesOn) {
                placementEnded = true;
            }
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
            NestedCell abandoned = betweenWorkItems ? placer.sealOpenCell(failure, column.fieldPath().toString()) : placer.abandonOpenCell();
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
                signalError(ScheduledColumn.asReadFailure(e));
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
            NestedPageTask nestedPage = (NestedPageTask) page;
            nestedPage.unfilledSegments.incrementAndGet();
            cell.add(nestedPage, recordStart, recordEnd, valueStart, valueEnd);
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
    /// cells sealed so far; a [SealedFillTask] only fills them.
    @Override
    void runPage(PageTask task, DecodeBuffers buffers) {
        try {
            if (done) {
                return;
            }
            if (task instanceof NestedPageTask page) {
                measureAndPlace(page, buffers);
            }
            fillSealed(buffers);
        }
        catch (Exception e) {
            pageFailed(e, task);
        }
        catch (Error err) {
            signalError(err);
            throw err;
        }
        finally {
            taskEnded(task);
        }
    }

    private void measureAndPlace(NestedPageTask page, DecodeBuffers buffers) {
        measure(page, buffers);
        lock.lock();
        try {
            page.measured = true;
            placeMeasured();
        }
        finally {
            lock.unlock();
        }
    }

    /// Decodes a page's levels into its scratch, its values left in place, trims it to its mask
    /// and measures it; the page is then ready to be placed.
    static void measure(NestedPageTask page, DecodeBuffers buffers) {
        PageInfo info = page.info;
        PageRowMask mask = info.mask();
        PageDecoder.InPlaceNestedPage inPlace = info.isNullPlaceholder()
                ? page.decoder.nullsInPlace(info.placeholderNumValues(), page.scratch, true)
                : page.decoder.decodeNestedInPlace(info.dataPage(), info.dictionary(), page.scratch, buffers);
        if (!mask.isAll()) {
            inPlace = PageTrimmer.trim(inPlace, mask, page.scratch);
        }
        page.inPlace = inPlace;
        page.measure = PageMeasure.of(inPlace, info.records(), page.scratch);
    }

    /// Returns a page's scratch to the column's pool; the page reads it no more.
    private void releaseScratch(NestedPageTask page) {
        releaseScratch(page, scratchPool);
    }

    /// Returns a page's scratch to `pool`, where it has room; the page reads it no more.
    static void releaseScratch(NestedPageTask page, ArrayBlockingQueue<PageDecoder.LevelScratch> pool) {
        page.inPlace = null;
        page.measure = null;
        page.scratch.trim();
        pool.offer(page.scratch);
    }

    /// Fills a sealed `cell` ([NestedFill]) and drops its segments, returning the scratch of each
    /// page whose cells are then all filled to `pool`.
    static void fill(NestedCell cell, BatchExchange<NestedBatch> exchange, ColumnSchema column,
                     NestedLevelComputer.Layers layers, NestedIndexMode indexMode,
                     BinaryBatchValues.ViewDemand viewDemand, DecodeBuffers buffers,
                     ArrayBlockingQueue<PageDecoder.LevelScratch> pool) {
        cell.filled(NestedFill.fill(cell, exchange, column, layers, indexMode, viewDemand, buffers));
        for (NestedSegment segment : cell.segments()) {
            if (segment.page.unfilledSegments.decrementAndGet() == 0) {
                releaseScratch(segment.page, pool);
            }
        }
        // The cell holds what it was filled from no longer: a cell waiting for room in the
        // exchange would otherwise keep its pages' arrays alive
        cell.dropSegments();
    }

    /// Fills, completes and publishes the cells sealed so far, outside the lock.
    private void fillSealed(DecodeBuffers buffers) {
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
            FillObserver observer = fillObserver;
            if (observer != null) {
                observer.filling(this, cell);
            }
            try {
                fill(cell, exchange, column, layers, indexMode, viewDemand, buffers, scratchPool);
            }
            catch (RuntimeException e) {
                // The cell cannot complete, and a failure queued behind it would never be raised
                pageFailed(e, cell.lastPage());
                return;
            }
            completed(cell, buffers);
        }
    }
}
