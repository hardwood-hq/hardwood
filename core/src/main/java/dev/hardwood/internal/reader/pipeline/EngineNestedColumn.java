/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.pipeline;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
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
import dev.hardwood.internal.reader.memory.WorkingSet;
import dev.hardwood.internal.reader.plan.BatchPlan;
import dev.hardwood.schema.ColumnSchema;

/// One column decoded through levels of a read the read engine drives: a leaf with repetition, or
/// with a `STRUCT` or `REPEATED` layer above it, read into [NestedBatch]es.
///
/// Where such a page's records start only its levels say, so a page is placed once a job has
/// decoded and measured it ([ScheduledNestedColumn#measure]):
///
/// - The column takes the pages its walk hands out behind the pages not yet placed. A page whose
///   metadata gives no record count is ordered at the column's last known row, a lower bound of
///   its first.
/// - Each page decodes and is measured on a job of its own; as its result is applied, the measured
///   pages at the head of the pages not yet placed are placed into the [NestedCell]s of the read's
///   batch plan ([Placer]), in page order. A page waits for no page ahead of it beyond that order.
/// - The cells a placement seals, including those the end of a row group or of the read seals,
///   are filled on a job ([NestedFill]), which completes them; they are published in plan order.
/// - A page borrows its scratch from the column's pool and returns it once every cell it is placed
///   into is filled.
///
/// Placement, sealing and publishing run under the engine's lock; decoding, measuring and filling
/// run on jobs, each touching only the page or the cells it was given.
public final class EngineNestedColumn implements EngineColumn {

    /// The end of the column's pages, behind the pages taken before it: the read's end or a
    /// failure, or the failure of its walk (`walk`), which abandons the open cell.
    private record End(Throwable failure, boolean walk) {
    }

    private final ColumnSchema column;
    private final BatchExchange<NestedBatch> exchange;
    private final EnginePublisher<NestedBatch> publisher;
    private final DecompressorFactory decompressorFactory;
    private final WorkingSet.Column workingSet;
    /// Whether a [dev.hardwood.MetadataSource] supplied the footers, which a failure hints at.
    private final boolean footersSupplied;
    private final NestedLevelComputer.Layers layers;
    private final NestedIndexMode indexMode;
    private final boolean fixedListFastPathEnabled;
    /// Whether the column has repetition, so that a batch is sealed at the next record start.
    private final boolean repeated;
    /// Raised once a reader needed the views of deferred dictionary values; the column builds them
    /// as its cells fill from then on.
    private final BinaryBatchValues.ViewDemand viewDemand = new BinaryBatchValues.ViewDemand();
    /// Scratch of pages whose cells are all filled, for the column's next pages; jobs return to it.
    private final ArrayBlockingQueue<PageDecoder.LevelScratch> scratchPool = ScheduledNestedColumn.newScratchPool();
    private final Placer<NestedCell> placer;
    /// Pages in page order not yet placed, then the [End] of the column's pages.
    private final ArrayDeque<Object> unplaced = new ArrayDeque<>();
    /// The pages with records among [#unplaced].
    private int unplacedPages;
    /// Cells sealed and not yet handed to a job to fill.
    private final List<Object> sealed = new ArrayList<>();
    /// Set once the placer placed its last page.
    private boolean placementEnded;
    /// The last page placed, which a failure at the end of the pages is placed with.
    private PageTask lastPlaced;

    private BatchPlan.RowGroupBatches rowGroup;
    /// Records of the current row group taken so far, `-1` once a page's metadata gave no record
    /// count.
    private long rowInRowGroup;
    private PageDecoder decoder;

    /// @param layers the column's layers ([NestedLevelComputer#computeLayers])
    /// @param indexMode what the column's batches carry for their reader
    public EngineNestedColumn(ColumnSchema column, BatchExchange<NestedBatch> exchange, int batchCapacity,
                              DecompressorFactory decompressorFactory, WorkingSet.Column workingSet,
                              boolean footersSupplied, NestedLevelComputer.Layers layers,
                              NestedIndexMode indexMode, boolean fixedListFastPathEnabled) {
        this.column = column;
        this.exchange = exchange;
        this.publisher = new EnginePublisher<>(exchange);
        this.decompressorFactory = decompressorFactory;
        this.workingSet = workingSet;
        this.footersSupplied = footersSupplied;
        this.layers = layers;
        this.indexMode = indexMode;
        this.fixedListFastPathEnabled = fixedListFastPathEnabled;
        this.repeated = column.maxRepetitionLevel() > 0;
        this.placer = new Placer<>(new Sink(), batchCapacity);
    }

    public BatchExchange<NestedBatch> exchange() {
        return exchange;
    }

    /// Takes a page of work item `batches` behind the pages not yet placed; returns the page to
    /// decode, `null` for a boundary marker, which is placed in its turn without a decode, and once
    /// the placement has ended.
    @Override
    public Page place(PageInfo info, BatchPlan.RowGroupBatches batches, String fileName, int rowGroupIndex,
                      int pageIndex) {
        if (placementEnded) {
            return null;
        }
        if (batches != rowGroup) {
            rowGroup = batches;
            rowInRowGroup = 0;
        }
        boolean marker = info.isBoundaryMarker();
        if (!marker && (decoder == null || !decoder.decodesChunk(info.columnMetaData()))) {
            decoder = new PageDecoder(info.columnMetaData(), info.columnSchema(), decompressorFactory,
                    fixedListFastPathEnabled, workingSet);
        }
        PageDecoder.LevelScratch scratch = marker ? null : scratchPool.poll();
        NestedPageTask page = new NestedPageTask(info, fileName, rowGroupIndex, pageIndex, batches, decoder, nextRow(),
                marker || scratch != null ? scratch : new PageDecoder.LevelScratch());
        unplaced.addLast(page);
        if (marker) {
            page.measured = true;
            rowInRowGroup = batches.plannedRows();
        }
        else {
            unplacedPages++;
            rowInRowGroup = rowInRowGroup >= 0 && info.records() != PageInfo.UNKNOWN_RECORDS
                    ? rowInRowGroup + info.records()
                    : -1;
        }
        placeMeasured();
        return marker ? null : new Page(page);
    }

    @Override
    public int pagesHeld() {
        return unplacedPages;
    }

    @Override
    public boolean placementEnded() {
        return placementEnded;
    }

    /// Queues the end of the column's pages behind the pages not yet placed: once they are, every
    /// cell is sealed and the read's end, or `failure`, queued behind them.
    @Override
    public void end(Throwable failure) {
        unplaced.addLast(new End(failure, false));
        placeMeasured();
    }

    @Override
    public void walkFailed(Throwable failure) {
        unplaced.addLast(new End(failure, true));
        placeMeasured();
    }

    @Override
    public long nextRow() {
        if (rowGroup == null) {
            return 0;
        }
        return rowInRowGroup >= 0
                ? rowGroup.readRowsBefore() + rowInRowGroup
                : Math.max(rowGroup.readRowsBefore(), placer.nextRow());
    }

    /// The first cell's row, or else the first unplaced page's, or else the next row.
    @Override
    public long firstUnpublishedRow() {
        ScheduledCell<NestedBatch> head = publisher.head();
        if (head != null) {
            return head.firstRow();
        }
        return unplaced.peekFirst() instanceof NestedPageTask page ? page.firstRow : nextRow();
    }

    /// Decodes and measures `page`, on a job; returns it for [#decoded].
    @Override
    public Object decode(Page page, DecodeBuffers buffers) throws Exception {
        NestedPageTask task = (NestedPageTask) page.task;
        try {
            ScheduledNestedColumn.measure(task, buffers);
        }
        catch (RuntimeException e) {
            throw placed(e, task.fileName, task.rowGroup, task.pageIndex);
        }
        return task;
    }

    /// Marks a page measured and places the measured pages at the head of the pages not yet placed.
    @Override
    public void decoded(Object result) throws Exception {
        ((NestedPageTask) result).measured = true;
        placeMeasured();
    }

    /// Places the measured pages at the head of the pages not yet placed, in page order, and ends
    /// the placement once they reach the end of the pages.
    private void placeMeasured() {
        Object head;
        while (!placementEnded && (head = unplaced.peekFirst()) != null) {
            if (head instanceof NestedPageTask page) {
                if (!page.measured) {
                    return;
                }
                unplaced.pollFirst();
                if (!page.isBoundaryMarker()) {
                    unplacedPages--;
                }
                placeMeasured(page);
            }
            else {
                unplaced.pollFirst();
                placementEnded = true;
                End end = (End) head;
                if (end.walk()) {
                    abandonAt(end.failure());
                }
                else {
                    endPlacement(end.failure());
                }
            }
        }
        if (placementEnded) {
            // Nothing is placed after the end, nor decoded
            unplaced.clear();
            unplacedPages = 0;
        }
    }

    private void placeMeasured(NestedPageTask page) {
        boolean goesOn;
        try {
            goesOn = placer.place(page, page.measure, repeated);
        }
        catch (RuntimeException e) {
            throw rethrowable(placed(e, page.fileName, page.rowGroup, page.pageIndex));
        }
        // Every cell the page reaches holds its segment, and none of them is filled before the
        // engine's lock is released
        if (page.scratch != null && page.unfilledSegments.get() == 0) {
            ScheduledNestedColumn.releaseScratch(page, scratchPool);
        }
        placementEnded |= !goesOn;
        lastPlaced = page;
    }

    /// Ends the column with its walk's `failure` behind the cells before the open one, which is
    /// abandoned.
    private void abandonAt(Throwable failure) {
        NestedCell abandoned = placer.abandonOpenCell();
        if (abandoned != null) {
            publisher.remove(abandoned);
        }
        publisher.endWith(failure);
        publisher.endRead();
    }

    private void endPlacement(Throwable failure) {
        publisher.endWith(failure);
        try {
            placer.end();
        }
        catch (RuntimeException e) {
            throw lastPlaced == null
                    ? e
                    : rethrowable(placed(e, lastPlaced.fileName, lastPlaced.rowGroup, lastPlaced.pageIndex));
        }
    }

    /// `placed`, a placed runtime failure, as the runtime failure it is; a placed failure keeps
    /// the type of the failure it places.
    private static RuntimeException rethrowable(Exception placed) {
        if (placed instanceof RuntimeException runtime) {
            return runtime;
        }
        throw new IllegalStateException("A placement failure lost its type", placed);
    }

    @Override
    public List<Object> takeSealed() {
        List<Object> cells = new ArrayList<>(sealed);
        sealed.clear();
        return cells;
    }

    /// Fills the cells [#takeSealed] took, on a job; returns them.
    @Override
    public List<Object> finishSealed(List<Object> cells, DecodeBuffers buffers) throws Exception {
        for (Object c : cells) {
            NestedCell cell = (NestedCell) c;
            try {
                ScheduledNestedColumn.fill(cell, exchange, column, layers, indexMode, viewDemand, buffers,
                        scratchPool);
            }
            catch (SegmentFillException e) {
                throw placed(e.failure(), e.page.fileName, e.page.rowGroup, e.page.pageIndex);
            }
            catch (RuntimeException e) {
                PageTask page = cell.lastPage();
                throw page == null ? e : placed(e, page.fileName, page.rowGroup, page.pageIndex);
            }
        }
        return cells;
    }

    @Override
    public void completed(List<Object> cells) {
        for (Object cell : cells) {
            ((NestedCell) cell).markCompleted();
        }
        publish();
    }

    @Override
    public Exception placed(Exception e, String fileName, int rowGroup, int page) {
        return ScheduledColumn.placeFailure(e, column, footersSupplied, fileName, rowGroup, page);
    }

    @Override
    public void publish() {
        publisher.publish();
    }

    @Override
    public boolean waitingForRoom() {
        return publisher.waitingForRoom();
    }

    @Override
    public boolean ended() {
        return publisher.ended();
    }

    @Override
    public void stop() {
        publisher.stop();
    }

    @Override
    public void fail(Throwable failure) {
        publisher.fail(failure);
    }

    @Override
    public String describe() {
        NestedCell head = (NestedCell) publisher.head();
        Object unplacedHead = unplaced.peekFirst();
        return "nested nextRow=" + nextRow() + " unplaced=" + unplaced.size()
                + (unplacedHead instanceof NestedPageTask page
                        ? " unplacedHead[firstRow=" + page.firstRow + " measured=" + page.measured + "]"
                        : " unplacedHead=" + unplacedHead)
                + " cells=" + publisher.size() + " sealed=" + sealed.size()
                + (head != null ? " head[firstRow=" + head.firstRow() + " records=" + head.records()
                        + " completed=" + head.isCompleted() + "]" : "")
                + " placementEnded=" + placementEnded + " ended=" + publisher.ended();
    }

    /// What the placer's decisions do to the column; under the engine's lock.
    private final class Sink implements Placer.Sink<NestedCell> {

        @Override
        public NestedCell open(String fileName, boolean alwaysMatches, int plannedRecords, int capacity,
                               long firstRow) {
            NestedCell cell = new NestedCell(fileName, alwaysMatches, firstRow);
            publisher.add(cell);
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
            sealed.add(cell);
        }

        @Override
        public void endRead() {
            publisher.endRead();
        }
    }
}
