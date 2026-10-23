/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.internal.predicate.ColumnBatchMatcher;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

/// A [ScheduledColumn] of a flat column: a leaf without repetition and without layers.
///
/// - The fetcher places each page into the [Cell]s of the read's batch plan ([Placer]) from its
///   metadata before it queues it.
/// - A page task decodes its page and fills its segments, allocating a cell on the first fill
///   that reaches it; the task that fills a cell's last segment completes it and publishes what
///   can be published. A binary column's task copies its segments' plain bytes into chunks of
///   their own ([BinaryFill]).
public final class ScheduledFlatColumn extends ScheduledColumn<BatchExchange.Batch> {

    /// The task's scratch for a page, whose values are filled within its task.
    private static final DecodeBuffers.Slot<PageDecoder.LevelScratch> SCRATCH = DecodeBuffers.slot();

    /// The column's filter fragment, run on each batch as it completes; `null` for none.
    private final ColumnBatchMatcher columnFilter;
    /// Whether the column is binary, its cells filled through [BinaryFill].
    private final boolean binary;
    /// Raised once a reader needed the views of deferred dictionary values; a binary column builds
    /// them as its cells complete from then on.
    private final BinaryBatchValues.ViewDemand viewDemand = new BinaryBatchValues.ViewDemand();

    /// The column's view demand, for tests: whether a reader asked for its views, and raising it
    /// before the read starts.
    BinaryBatchValues.ViewDemand viewDemand() {
        return viewDemand;
    }

    // === Guarded by `lock` ===
    private final Placer<Cell> placer;
    /// Cells whose seal came after their last fill, which a [SealedFillTask] completes.
    private final ArrayDeque<Cell> sealedComplete = new ArrayDeque<>();

    // === Fetcher only ===
    private BatchPlan.RowGroupBatches rowGroup;
    /// Kept records of the current row group the column placed so far.
    private long rowInRowGroup;

    /// Creates a scheduled flat column and adds it to `scheduler`, which starts it.
    ///
    /// @param columnFilter the column's filter fragment, which writes each batch's `matches` as
    ///        the batch completes; `null` for none
    public ScheduledFlatColumn(ReadScheduler scheduler, PageSource pageSource, BatchExchange<BatchExchange.Batch> exchange,
                               ColumnSchema column, int batchCapacity, DecompressorFactory decompressorFactory,
                               ColumnBatchMatcher columnFilter) {
        super(scheduler, pageSource, exchange, column, decompressorFactory);
        this.columnFilter = columnFilter;
        this.binary = switch (column.type()) {
            case INT32, INT64, FLOAT, DOUBLE, BOOLEAN -> false;
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> true;
        };
        this.placer = new Placer<>(new PlacerSink(), batchCapacity);
    }

    // ==================== Fetcher ====================

    /// Places a page and queues it for the scheduler, unless it has no records to decode.
    @Override
    boolean queue(PageInfo info, PageDecoder decoder) {
        BatchPlan.RowGroupBatches batches = pageSource.getCurrentBatches();
        if (batches != rowGroup) {
            rowGroup = batches;
            rowInRowGroup = 0;
        }
        PageTask page = newPageTask(info, decoder, batches.readRowsBefore() + rowInRowGroup);
        boolean goesOn;
        lock.lock();
        try {
            try {
                goesOn = placer.place(page);
            }
            finally {
                // A placement that failed part way placed segments into cells it may have sealed:
                // the page's task fills them, so that the cells before the failure complete and
                // the failure is raised behind them.
                if (!page.segments.isEmpty() && !done) {
                    ready.addLast(page);
                }
            }
            if (info.isBoundaryMarker()) {
                rowInRowGroup = batches.plannedRows();
            }
            else {
                rowInRowGroup += info.records();
            }
            nextRow = batches.readRowsBefore() + rowInRowGroup;
            updatePublishedRow();
            queueSealedFill();
        }
        finally {
            lock.unlock();
        }
        return goesOn;
    }

    /// The fetcher submits the queue's change as it exits.
    @Override
    void pagesEnded() {
        lock.lock();
        try {
            placer.end();
            queueSealedFill();
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    void fetchFailed(Exception failure, boolean betweenWorkItems) {
        lock.lock();
        try {
            Cell abandoned = betweenWorkItems ? placer.sealOpenCell(failure, column.fieldPath().toString()) : placer.abandonOpenCell();
            if (abandoned != null) {
                // Its pages still fill it; they must not complete it.
                abandoned.pending.incrementAndGet();
            }
            endWithFailure(failure, abandoned);
            queueSealedFill();
        }
        finally {
            lock.unlock();
        }
    }

    /// Queues a [SealedFillTask] ahead of the queued pages for the cells the fetcher's placement
    /// sealed after their last fill (lock held), unless one is queued already. Every page of such
    /// a cell has run, and every queued page starts after it, so the queue stays in row order.
    private void queueSealedFill() {
        Cell first = sealedComplete.peekFirst();
        if (first != null && !done && !(ready.peekFirst() instanceof SealedFillTask)) {
            ready.addFirst(new SealedFillTask(first.firstRow));
        }
    }

    /// Completes the cells the fetcher's placement sealed after their last fill, on a
    /// [SealedFillTask], outside the lock: completing a cell runs the column's filter fragment.
    private void completeSealed(DecodeBuffers task) {
        while (true) {
            Cell complete;
            lock.lock();
            try {
                complete = sealedComplete.pollFirst();
            }
            finally {
                lock.unlock();
            }
            if (complete == null) {
                return;
            }
            try {
                completeAndPublish(complete, task);
            }
            catch (RuntimeException e) {
                // The cell cannot complete, and a failure queued behind it would never be raised
                pageFailed(e, complete.lastPage());
                return;
            }
        }
    }

    /// What the placer's decisions do to the column; runs under the column lock.
    private final class PlacerSink implements Placer.Sink<Cell> {

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
                sealedComplete.addLast(cell);
            }
        }

        @Override
        public void endRead() {
            publishOrder.addLast(END);
            publishReady();
        }
    }

    // ==================== Page task ====================

    /// Decodes a page and fills its segments, completing each cell whose last segment it fills.
    @Override
    void runPage(PageTask page, DecodeBuffers buffers) {
        try {
            if (done) {
                return;
            }
            if (page instanceof SealedFillTask) {
                completeSealed(buffers);
                return;
            }
            if (binary) {
                runBinaryPage(page, buffers);
                return;
            }
            PageDecoder.LevelScratch scratch = scratch(buffers);
            PageInfo info = page.info;
            FlatPageValues values = info.isNullPlaceholder()
                    ? page.decoder.nullsInPlace(info.placeholderNumValues(), scratch).flat()
                    : page.decoder.decodeFlat(info.pageData(), info.dictionary(), scratch, buffers);
            checkRows(info, values.size());
            for (int i = 0; i < page.segments.size(); i++) {
                if (done) {
                    return;
                }
                Segment segment = page.segments.get(i);
                Cell cell = segment.cell;
                FlatFill.fill(segment, values, cell.allocate(exchange, column).values, maxDefinitionLevel);
                filled(cell, buffers);
            }
        }
        catch (Exception e) {
            pageFailed(e, page);
        }
        catch (Error err) {
            signalError(err);
            throw err;
        }
        finally {
            PageDecoder.LevelScratch scratch = buffers.get(SCRATCH);
            if (scratch != null) {
                scratch.trim();
            }
            taskEnded(page);
        }
    }

    /// Decodes a page of a binary column and fills its segments, completing each cell whose last
    /// segment it fills.
    private void runBinaryPage(PageTask page, DecodeBuffers buffers) {
        PageDecoder.LevelScratch scratch = scratch(buffers);
        PageInfo info = page.info;
        BinaryPageValues values = info.isNullPlaceholder()
                ? page.decoder.nullsInPlace(info.placeholderNumValues(), scratch).binary()
                : page.decoder.decodeFlatBinary(info.pageData(), info.dictionary(), scratch, buffers);
        checkRows(info, values.size());
        List<Segment> segments = page.segments;
        for (int i = 0; i < segments.size(); i++) {
            if (done) {
                return;
            }
            Segment segment = segments.get(i);
            Cell cell = segment.cell;
            BinaryFill.fill(segment, values, (BinaryBatchValues) cell.allocate(exchange, column).values,
                    maxDefinitionLevel);
            filled(cell, buffers);
        }
    }

    /// The task's scratch in `buffers`.
    static PageDecoder.LevelScratch scratch(DecodeBuffers buffers) {
        PageDecoder.LevelScratch scratch = buffers.get(SCRATCH);
        if (scratch == null) {
            scratch = new PageDecoder.LevelScratch();
            buffers.set(SCRATCH, scratch);
        }
        return scratch;
    }

    /// Counts one of the cell's tokens given up, and completes the cell at its last.
    private void filled(Cell cell, DecodeBuffers task) {
        if (cell.pending.decrementAndGet() == 0) {
            completeAndPublish(cell, task);
        }
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

    /// Completes a cell whose segments are all filled and which is sealed, and publishes what
    /// its completion lets the column publish.
    private void completeAndPublish(Cell cell, DecodeBuffers task) {
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
        completed(cell, task);
    }

    /// Writes the batch's matches for the column's filter fragment: all ones when statistics
    /// proved every record matches, else the matcher's verdict, while the values are in this
    /// task's cache. A matcher keeps scratch between calls, and cells complete on several tasks
    /// at once, so its calls are serialized.
    private void testFilter(BatchExchange.Batch batch, int records) {
        if (batch.filterAlwaysMatches) {
            BitmapWords.fillAllOnes(batch.matches, records);
            return;
        }
        synchronized (columnFilter) {
            columnFilter.test(batch, batch.matches);
        }
    }
}
