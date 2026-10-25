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
import java.util.Arrays;
import java.util.List;

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.batch.BinaryBatchValues;
import dev.hardwood.internal.reader.decode.BinaryPageValues;
import dev.hardwood.internal.reader.decode.FlatPageValues;
import dev.hardwood.internal.reader.decode.PageDecoder;
import dev.hardwood.internal.reader.decode.PageInfo;
import dev.hardwood.internal.reader.memory.WorkingSet;
import dev.hardwood.internal.reader.plan.BatchPlan;
import dev.hardwood.schema.ColumnSchema;

/// One flat decoded column of a read the read engine drives: it places the pages its walk hands
/// out into the cells of the batch plan ([Placer]), decodes and fills them on jobs, and publishes
/// its completed cells in plan order to its [BatchExchange]. A binary column's pages copy their
/// segments' plain bytes into chunks of their own, or write dictionary ids ([BinaryFill]).
///
/// Placement, marking cells completed and publishing run under the engine's lock, which also
/// guards the cells' bookkeeping; decoding, filling and completing a cell's batch run on jobs,
/// outside it. A job reports the cells it completed, which the engine marks under its lock.
public final class EngineColumn {

    private static final Object END = new Object();

    private final ColumnSchema column;
    private final BatchExchange<BatchExchange.Batch> exchange;
    private final DecompressorFactory decompressorFactory;
    private final WorkingSet.Column workingSet;
    /// Whether a [dev.hardwood.MetadataSource] supplied the footers, which a failure hints at.
    private final boolean footersSupplied;
    private final int maxDefinitionLevel;
    /// Whether the column is binary, its cells filled through [BinaryFill].
    private final boolean binary;
    /// Raised once a reader needed the views of deferred dictionary values; the column builds them
    /// as its cells complete from then on.
    private final BinaryBatchValues.ViewDemand viewDemand = new BinaryBatchValues.ViewDemand();
    private final Placer<Cell> placer;
    /// The cells in plan order, then [#END] once the read has ended.
    private final ArrayDeque<Object> publishOrder = new ArrayDeque<>();
    /// Cells whose seal came after their last fill, which a job completes.
    private final List<Cell> sealedComplete = new ArrayList<>();

    private BatchPlan.RowGroupBatches rowGroup;
    /// Kept records of the current row group placed so far.
    private long rowInRowGroup;
    private PageDecoder decoder;
    private boolean ended;
    /// The failure the column ends with behind its cells, `null` for the read's end.
    private Throwable endFailure;
    /// Whether a completed cell waits for room in the exchange.
    private boolean waitingForRoom;

    /// A placed page, for the engine to order and decode: the read's row of its first record.
    public static final class Page {

        private final PageTask task;

        private Page(PageTask task) {
            this.task = task;
        }

        public long firstRow() {
            return task.firstRow;
        }

        /// The buffers the page's task takes, which the read admits before it starts
        /// ([WorkingSet#admit]).
        public long taskBytes() {
            return task.info == null ? 0 : task.info.taskBytes();
        }
    }

    public EngineColumn(ColumnSchema column, BatchExchange<BatchExchange.Batch> exchange, int batchCapacity,
                        DecompressorFactory decompressorFactory, WorkingSet.Column workingSet,
                        boolean footersSupplied) {
        this.column = column;
        this.footersSupplied = footersSupplied;
        this.exchange = exchange;
        this.decompressorFactory = decompressorFactory;
        this.workingSet = workingSet;
        this.maxDefinitionLevel = column.maxDefinitionLevel();
        this.binary = switch (column.type()) {
            case INT32, INT64, FLOAT, DOUBLE, BOOLEAN -> false;
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> true;
        };
        this.placer = new Placer<>(new Sink(), batchCapacity);
    }

    /// Places a page of work item `batches`; returns the page to decode, `null` when it has no
    /// record to decode. Under the engine's lock.
    public Page place(PageInfo info, BatchPlan.RowGroupBatches batches, String fileName, int rowGroupIndex,
                      int pageIndex) {
        if (batches != rowGroup) {
            rowGroup = batches;
            rowInRowGroup = 0;
        }
        if (!info.isBoundaryMarker() && (decoder == null || !decoder.decodesChunk(info.columnMetaData()))) {
            decoder = new PageDecoder(info.columnMetaData(), info.columnSchema(), decompressorFactory, true, workingSet);
        }
        PageTask task = new PageTask(info, fileName, rowGroupIndex, pageIndex, batches, decoder,
                batches.readRowsBefore() + rowInRowGroup);
        placer.place(task);
        rowInRowGroup = info.isBoundaryMarker() ? batches.plannedRows() : rowInRowGroup + info.records();
        return task.segments.isEmpty() ? null : new Page(task);
    }

    /// Ends the column's pages: every cell is sealed and the read's end, or `failure` where it is
    /// not `null`, queued behind them. Under the engine's lock.
    public void end(Throwable failure) {
        endFailure = failure;
        placer.end();
    }

    /// The read's row of the next record the column places.
    public long nextRow() {
        return rowGroup == null ? 0 : rowGroup.readRowsBefore() + rowInRowGroup;
    }

    /// The read's row of the first record not published: the first cell's, or the next row.
    public long firstUnpublishedRow() {
        Object head = publishOrder.peekFirst();
        return head instanceof Cell cell ? cell.firstRow : nextRow();
    }

    /// The cells sealed after their last fill, for a job to complete ([#complete]). Under the
    /// engine's lock.
    public List<Object> takeSealed() {
        List<Object> sealed = new ArrayList<>(sealedComplete);
        sealedComplete.clear();
        return sealed;
    }

    /// Decodes `page` and fills its segments, on a job; returns the cells it completed.
    ///
    /// @throws Exception the failure, placed at the page's file, row group and ordinal
    public List<Object> decode(Page page, DecodeBuffers buffers) throws Exception {
        try {
            return decodeUnplaced(page, buffers);
        }
        catch (RuntimeException e) {
            throw placed(e, page.task.fileName, page.task.rowGroup, page.task.pageIndex);
        }
    }

    /// `e`, placed at the column and `fileName`, `rowGroup` and `page` it was met at.
    public Exception placed(Exception e, String fileName, int rowGroup, int page) {
        return ScheduledColumn.placeFailure(e, column, footersSupplied, fileName, rowGroup, page);
    }

    private List<Object> decodeUnplaced(Page page, DecodeBuffers buffers) {
        PageTask task = page.task;
        PageInfo info = task.info;
        PageDecoder.LevelScratch scratch = ScheduledFlatColumn.scratch(buffers);
        try {
            return binary ? decodeBinary(task, info, scratch, buffers) : decodeFixed(task, info, scratch, buffers);
        }
        finally {
            scratch.trim();
        }
    }

    private List<Object> decodeFixed(PageTask task, PageInfo info, PageDecoder.LevelScratch scratch,
                                     DecodeBuffers buffers) {
        FlatPageValues values = info.isNullPlaceholder()
                ? task.decoder.nullsInPlace(info.placeholderNumValues(), scratch, false).flat()
                : task.decoder.decodeFlat(info.dataPage(), info.dictionary(), scratch, buffers);
        ScheduledFlatColumn.checkRows(info, values.size());
        List<Object> completed = new ArrayList<>(1);
        for (Segment segment : task.segments) {
            Cell cell = segment.cell;
            FlatFill.fill(segment, values, cell.allocate(exchange, column).values, maxDefinitionLevel);
            filled(cell, completed);
        }
        return completed;
    }

    private List<Object> decodeBinary(PageTask task, PageInfo info, PageDecoder.LevelScratch scratch,
                                      DecodeBuffers buffers) {
        BinaryPageValues values = info.isNullPlaceholder()
                ? task.decoder.nullsInPlace(info.placeholderNumValues(), scratch, false).binary()
                : task.decoder.decodeFlatBinary(info.dataPage(), info.dictionary(), scratch, buffers);
        ScheduledFlatColumn.checkRows(info, values.size());
        List<Object> completed = new ArrayList<>(1);
        for (Segment segment : task.segments) {
            Cell cell = segment.cell;
            BinaryFill.fill(segment, values, (BinaryBatchValues) cell.allocate(exchange, column).values,
                    maxDefinitionLevel);
            filled(cell, completed);
        }
        return completed;
    }

    /// Counts one of `cell`'s tokens given up, and completes it into `completed` at its last.
    private void filled(Cell cell, List<Object> completed) {
        if (cell.pending.decrementAndGet() == 0) {
            completed.add(completeBatch(cell));
        }
    }

    /// Completes the batches of cells [#takeSealed] took, on a job; returns them.
    public static List<Object> complete(EngineColumn owner, List<Object> cells) {
        for (Object cell : cells) {
            owner.completeBatch((Cell) cell);
        }
        return cells;
    }

    private Cell completeBatch(Cell cell) {
        BatchExchange.Batch batch = cell.allocate(exchange, column);
        int records = cell.records();
        batch.recordCount = records;
        FlatFill.backfillValidity(cell);
        long[] validity = cell.validity();
        int words = (records + 63) >>> 6;
        batch.validity = validity == null || validity.length == words ? validity : Arrays.copyOf(validity, words);
        batch.fileName = cell.fileName;
        batch.filterAlwaysMatches = cell.alwaysMatches;
        if (batch.values instanceof BinaryBatchValues values) {
            BinaryFill.finish(cell, values, viewDemand);
            // Views are built here once a reader needed them.
            if (viewDemand.requested) {
                values.ensureViews();
            }
        }
        return cell;
    }

    /// Marks the cells a job completed, and publishes what that lets the column publish. Under the
    /// engine's lock.
    public void completed(List<Object> cells) {
        for (Object cell : cells) {
            ((Cell) cell).markCompleted();
        }
        publish();
    }

    /// Publishes the completed cells at the head of the plan order, as far as the exchange takes
    /// them, and finishes the exchange at the read's end. Under the engine's lock; also run when
    /// the consumer makes room.
    public void publish() {
        while (!publishOrder.isEmpty()) {
            Object head = publishOrder.peekFirst();
            if (head == END) {
                publishOrder.pollFirst();
                ended = true;
                if (endFailure != null) {
                    exchange.signalError(endFailure);
                }
                else {
                    exchange.finish();
                }
                return;
            }
            Cell cell = (Cell) head;
            if (!cell.isCompleted()) {
                return;
            }
            waitingForRoom = !exchange.tryPublish(cell.batch());
            if (waitingForRoom) {
                return;
            }
            publishOrder.pollFirst();
            cell.release();
        }
    }

    /// Whether a completed cell waits for the consumer to make room in the exchange.
    public boolean waitingForRoom() {
        return waitingForRoom;
    }

    /// The column's state, for a stall report.
    public String describe() {
        Object head = publishOrder.peekFirst();
        return "nextRow=" + nextRow() + " cells=" + publishOrder.size() + " sealed=" + sealedComplete.size()
                + (head instanceof Cell cell ? " head[firstRow=" + cell.firstRow + " records=" + cell.records()
                        + " pending=" + cell.pending.get() + " completed=" + cell.isCompleted() + "]" : " head=" + head)
                + " ended=" + ended;
    }

    /// Whether the column has published its last batch and finished.
    public boolean ended() {
        return ended;
    }

    /// Stops the column as its read closes: nothing more is published, and a consumer waiting for
    /// a batch finds the end.
    public void stop() {
        if (!ended) {
            ended = true;
            publishOrder.clear();
            exchange.finish();
        }
    }

    /// Ends the column with `failure`.
    public void fail(Throwable failure) {
        ended = true;
        publishOrder.clear();
        exchange.signalError(failure);
    }

    /// What the placer's decisions do to the column; under the engine's lock.
    private final class Sink implements Placer.Sink<Cell> {

        @Override
        public Cell open(String fileName, boolean alwaysMatches, int plannedRecords, int capacity, long firstRow) {
            Cell cell = new Cell(fileName, alwaysMatches, plannedRecords, capacity, firstRow);
            publishOrder.addLast(cell);
            return cell;
        }

        @Override
        public void add(Cell cell, PageTask page, int recordStart, int recordEnd, int valueStart, int valueEnd) {
            page.segments.add(cell.add(page, recordStart, recordEnd));
        }

        @Override
        public void sealed(Cell cell) {
            if (cell.pending.decrementAndGet() == 0) {
                sealedComplete.add(cell);
            }
        }

        @Override
        public void endRead() {
            publishOrder.addLast(END);
        }
    }
}
