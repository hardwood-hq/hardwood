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
import dev.hardwood.internal.predicate.ColumnBatchMatcher;
import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.batch.BinaryBatchValues;
import dev.hardwood.internal.reader.batch.BitmapWords;
import dev.hardwood.internal.reader.decode.BinaryPageValues;
import dev.hardwood.internal.reader.decode.FlatPageValues;
import dev.hardwood.internal.reader.decode.PageDecoder;
import dev.hardwood.internal.reader.decode.PageInfo;
import dev.hardwood.internal.reader.memory.WorkingSet;
import dev.hardwood.internal.reader.plan.BatchPlan;
import dev.hardwood.internal.reader.select.PageRowMask;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

/// One flat decoded column of a read ([dev.hardwood.internal.reader.engine.RowGroupIterator]): it
/// places the pages its walk hands out into the cells of the batch plan ([Placer]) as it takes them, decodes and fills them on
/// jobs, and publishes its completed cells in plan order to its [BatchExchange]. A binary column's
/// pages copy their segments' plain bytes into chunks of their own, or write dictionary ids
/// ([BinaryFill]). A column with a filter fragment writes each batch's matches as the batch
/// completes.
///
/// Placement, marking cells completed and publishing run under the read's lock, which also
/// guards the cells' bookkeeping; decoding, filling and completing a cell's batch run on jobs,
/// outside it. A job reports the cells it completed, which the read marks under its lock.
public final class FlatColumn implements DecodedColumn {

    private static final DecodeBuffers.Slot<PageDecoder.LevelScratch> SCRATCH = DecodeBuffers.slot();

    private final ColumnSchema column;
    private final BatchExchange<BatchExchange.Batch> exchange;
    private final CellPublisher<BatchExchange.Batch> publisher;
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
    /// The column's filter fragment, run on each batch as it completes; `null` for none.
    private final ColumnBatchMatcher columnFilter;
    private final Placer<Cell> placer;
    /// Cells whose seal came after their last fill, which a job completes.
    private final List<Cell> sealedComplete = new ArrayList<>();
    /// Set once the placer ended the read at its row cap.
    private boolean placementEnded;

    private BatchPlan.RowGroupBatches rowGroup;
    /// Kept records of the current row group placed so far.
    private long rowInRowGroup;
    private PageDecoder decoder;
    /// Pages placed and not decoded.
    private int undecoded;
    /// The last page placed, which a failure at the end of the pages is placed with.
    private PageTask lastPlaced;

    /// @param columnFilter the column's filter fragment, which writes each batch's `matches` as the
    ///        batch completes; `null` for none
    public FlatColumn(ColumnSchema column, BatchExchange<BatchExchange.Batch> exchange, int batchCapacity,
                            DecompressorFactory decompressorFactory, WorkingSet.Column workingSet,
                            boolean footersSupplied, ColumnBatchMatcher columnFilter) {
        this.column = column;
        this.columnFilter = columnFilter;
        this.footersSupplied = footersSupplied;
        this.exchange = exchange;
        this.publisher = new CellPublisher<>(exchange);
        this.decompressorFactory = decompressorFactory;
        this.workingSet = workingSet;
        this.maxDefinitionLevel = column.maxDefinitionLevel();
        this.binary = switch (column.type()) {
            case INT32, INT64, FLOAT, DOUBLE, BOOLEAN -> false;
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> true;
        };
        this.placer = new Placer<>(new Sink(), batchCapacity);
    }

    /// Whether a reader asked for the views of the column's deferred dictionary values, for tests.
    BinaryBatchValues.ViewDemand viewDemand() {
        return viewDemand;
    }

    public BatchExchange<BatchExchange.Batch> exchange() {
        return exchange;
    }

    /// Places a page of work item `batches`; returns the page to decode, `null` when it has no
    /// record to decode or the placement has ended.
    @Override
    public Page place(PageInfo info, BatchPlan.RowGroupBatches batches, String fileName, int rowGroupIndex,
                      int pageIndex) throws PlaceFailure {
        if (placementEnded) {
            return null;
        }
        if (batches != rowGroup) {
            rowGroup = batches;
            rowInRowGroup = 0;
        }
        if (!info.isBoundaryMarker() && (decoder == null || !decoder.decodesChunk(info.columnMetaData()))) {
            decoder = new PageDecoder(info.columnMetaData(), info.columnSchema(), decompressorFactory, true, workingSet);
        }
        PageTask task = new PageTask(info, fileName, rowGroupIndex, pageIndex, batches, decoder,
                batches.readRowsBefore() + rowInRowGroup);
        try {
            placementEnded = !placer.place(task);
        }
        catch (RuntimeException e) {
            // The segments placed before the failure are filled, so that their cells complete
            Page placed = null;
            if (!task.segments.isEmpty()) {
                undecoded++;
                placed = new Page(task);
            }
            throw new PlaceFailure(placed(e, fileName, rowGroupIndex, pageIndex), placed);
        }
        lastPlaced = task;
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

    @Override
    public boolean placementEnded() {
        return placementEnded;
    }

    /// A flat page is placed before it is decoded, so every page taken fills a cell.
    @Override
    public boolean decodes(Page page) {
        return true;
    }

    /// Ends the column's pages: every cell is sealed and the read's end queued behind them; or,
    /// with `failure`, a failure between two work items, the open cell sealed, or abandoned where
    /// the column is short of the plan ([Placer#sealOpenCell]), and `failure` queued behind them.
    @Override
    public void end(Throwable failure) {
        if (failure == null) {
            publisher.endWith(null);
            try {
                placer.end();
            }
            catch (RuntimeException e) {
                // the column is short of the plan at the read's end
                abandon(placer.abandonOpenCell());
                publisher.endWith(lastPlaced == null ? e : placed(e, lastPlaced.fileName, lastPlaced.rowGroup,
                        lastPlaced.pageIndex));
                publisher.endRead();
            }
            return;
        }
        abandon(placer.sealOpenCell(failure, column.fieldPath().toString()));
        publisher.endWith(failure);
        publisher.endRead();
    }

    /// Takes out `abandoned`, if any, which its pages in flight still fill: they must not complete
    /// it.
    private void abandon(Cell abandoned) {
        if (abandoned != null) {
            abandoned.pending.incrementAndGet();
            publisher.remove(abandoned);
        }
    }

    @Override
    public void walkFailed(Throwable failure) {
        Cell abandoned = placer.abandonOpenCell();
        if (abandoned != null) {
            publisher.remove(abandoned);
        }
        publisher.endWith(failure);
        publisher.endRead();
    }

    /// The read's row of the next record the column places.
    @Override
    public long nextRow() {
        return rowGroup == null ? 0 : rowGroup.readRowsBefore() + rowInRowGroup;
    }

    /// The read's row of the first record not published: the first cell's, or the next row.
    @Override
    public long firstUnpublishedRow() {
        BatchCell<BatchExchange.Batch> head = publisher.head();
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
        return DecodedColumn.placeFailure(e, column, footersSupplied, fileName, rowGroup, page);
    }

    private List<Object> decodeUnplaced(Page page, DecodeBuffers buffers) {
        PageTask task = page.task;
        PageInfo info = task.info;
        PageDecoder.LevelScratch scratch = scratch(buffers);
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
        checkRows(info, values.size());
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
        checkRows(info, values.size());
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
    ///
    /// @throws Exception the failure, placed at the last page of the cell it was met at
    @Override
    public List<Object> finishSealed(List<Object> cells, DecodeBuffers buffers) throws Exception {
        for (Object c : cells) {
            Cell cell = (Cell) c;
            PageTask page = cell.lastPage();
            try {
                completeBatch(cell);
            }
            catch (RuntimeException e) {
                throw page == null ? e : placed(e, page.fileName, page.rowGroup, page.pageIndex);
            }
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
            // Views are built here once a reader needed them, or when the filter reads every one.
            if (viewDemand.requested || (columnFilter != null && columnFilter.readsEveryValueView())) {
                values.ensureViews();
            }
        }
        if (columnFilter != null) {
            testFilter(batch, records);
        }
        return cell;
    }

    /// Writes the batch's matches for the column's filter fragment: all ones when statistics
    /// proved every record matches, else the matcher's verdict, while the values are in this job's
    /// cache. A matcher keeps scratch between calls, and cells complete on several jobs at once, so
    /// its calls are serialized.
    private void testFilter(BatchExchange.Batch batch, int records) {
        if (batch.filterAlwaysMatches) {
            BitmapWords.fillAllOnes(batch.matches, records);
            return;
        }
        synchronized (columnFilter) {
            columnFilter.test(batch, batch.matches);
        }
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
                + " placementEnded=" + placementEnded + " ended=" + publisher.ended();
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

    /// What the placer's decisions do to the column; under the read's lock.
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

    /// The task's scratch in `buffers`.
    public static PageDecoder.LevelScratch scratch(DecodeBuffers buffers) {
        PageDecoder.LevelScratch scratch = buffers.get(SCRATCH);
        if (scratch == null) {
            scratch = new PageDecoder.LevelScratch();
            buffers.set(SCRATCH, scratch);
        }
        return scratch;
    }

    /// Checks that a decoded page holds the rows its placement took from its metadata: every row
    /// of an unmasked page, and at least the rows up to its mask's last for a masked one.
    ///
    /// @throws ParquetReadException if it does not
    private static void checkRows(PageInfo info, int values) {
        PageRowMask mask = info.mask();
        if (mask.isAll() ? values != info.records() : values < mask.end(mask.intervalCount() - 1)) {
            throw new ParquetReadException("Page holds " + values + " values, but its metadata gives it "
                    + (mask.isAll() ? info.records() : "at least " + mask.end(mask.intervalCount() - 1)) + " rows");
        }
    }
}
