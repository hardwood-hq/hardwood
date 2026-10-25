/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.pipeline;

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
/// out into the cells of the batch plan ([Placer]) as it takes them, decodes and fills them on
/// jobs, and publishes its completed cells in plan order to its [BatchExchange]. A binary column's
/// pages copy their segments' plain bytes into chunks of their own, or write dictionary ids
/// ([BinaryFill]).
///
/// Placement, marking cells completed and publishing run under the engine's lock, which also
/// guards the cells' bookkeeping; decoding, filling and completing a cell's batch run on jobs,
/// outside it. A job reports the cells it completed, which the engine marks under its lock.
public final class EngineFlatColumn implements EngineColumn {

    private final ColumnSchema column;
    private final BatchExchange<BatchExchange.Batch> exchange;
    private final EnginePublisher<BatchExchange.Batch> publisher;
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
    /// Cells whose seal came after their last fill, which a job completes.
    private final List<Cell> sealedComplete = new ArrayList<>();

    private BatchPlan.RowGroupBatches rowGroup;
    /// Kept records of the current row group placed so far.
    private long rowInRowGroup;
    private PageDecoder decoder;
    /// Pages placed and not decoded.
    private int undecoded;

    public EngineFlatColumn(ColumnSchema column, BatchExchange<BatchExchange.Batch> exchange, int batchCapacity,
                        DecompressorFactory decompressorFactory, WorkingSet.Column workingSet,
                        boolean footersSupplied) {
        this.column = column;
        this.footersSupplied = footersSupplied;
        this.exchange = exchange;
        this.publisher = new EnginePublisher<>(exchange);
        this.decompressorFactory = decompressorFactory;
        this.workingSet = workingSet;
        this.maxDefinitionLevel = column.maxDefinitionLevel();
        this.binary = switch (column.type()) {
            case INT32, INT64, FLOAT, DOUBLE, BOOLEAN -> false;
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> true;
        };
        this.placer = new Placer<>(new Sink(), batchCapacity);
    }

    public BatchExchange<BatchExchange.Batch> exchange() {
        return exchange;
    }

    /// Places a page of work item `batches`; returns the page to decode, `null` when it has no
    /// record to decode.
    @Override
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
        if (task.segments.isEmpty()) {
            return null;
        }
        undecoded++;
        return new Page(task);
    }

    @Override
    public int pagesHeld() {
        return undecoded;
    }

    /// Ends the column's pages: every cell is sealed and the read's end, or `failure` where it is
    /// not `null`, queued behind them.
    @Override
    public void end(Throwable failure) {
        publisher.endWith(failure);
        placer.end();
    }

    /// The read's row of the next record the column places.
    @Override
    public long nextRow() {
        return rowGroup == null ? 0 : rowGroup.readRowsBefore() + rowInRowGroup;
    }

    /// The read's row of the first record not published: the first cell's, or the next row.
    @Override
    public long firstUnpublishedRow() {
        ScheduledCell<BatchExchange.Batch> head = publisher.head();
        return head != null ? head.firstRow() : nextRow();
    }

    /// The cells sealed after their last fill, for a job to complete ([#finishSealed]).
    @Override
    public List<Object> takeSealed() {
        List<Object> sealed = new ArrayList<>(sealedComplete);
        sealedComplete.clear();
        return sealed;
    }

    /// Decodes `page` and fills its segments, on a job; returns the cells it completed.
    ///
    /// @throws Exception the failure, placed at the page's file, row group and ordinal
    @Override
    public Object decode(Page page, DecodeBuffers buffers) throws Exception {
        try {
            return decodeUnplaced(page, buffers);
        }
        catch (RuntimeException e) {
            throw placed(e, page.task.fileName, page.task.rowGroup, page.task.pageIndex);
        }
    }

    @Override
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
    @Override
    public List<Object> finishSealed(List<Object> cells, DecodeBuffers buffers) {
        for (Object cell : cells) {
            completeBatch((Cell) cell);
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

    /// Marks the cells of a decoded page completed, and publishes what that lets the column
    /// publish.
    @Override
    public void decoded(Object result) {
        undecoded--;
        @SuppressWarnings("unchecked")
        List<Object> cells = (List<Object>) result;
        completed(cells);
    }

    /// Marks the cells a job completed, and publishes what that lets the column publish.
    @Override
    public void completed(List<Object> cells) {
        for (Object cell : cells) {
            ((Cell) cell).markCompleted();
        }
        publish();
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
    public String describe() {
        Cell head = (Cell) publisher.head();
        return "nextRow=" + nextRow() + " undecoded=" + undecoded + " cells=" + publisher.size()
                + " sealed=" + sealedComplete.size()
                + (head != null ? " head[firstRow=" + head.firstRow + " records=" + head.records()
                        + " pending=" + head.pending.get() + " completed=" + head.isCompleted() + "]" : "")
                + " ended=" + publisher.ended();
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

    /// What the placer's decisions do to the column; under the engine's lock.
    private final class Sink implements Placer.Sink<Cell> {

        @Override
        public Cell open(String fileName, boolean alwaysMatches, int plannedRecords, int capacity, long firstRow) {
            Cell cell = new Cell(fileName, alwaysMatches, plannedRecords, capacity, firstRow);
            publisher.add(cell);
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
            publisher.endRead();
        }
    }
}
