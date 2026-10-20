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
import java.util.concurrent.ArrayBlockingQueue;

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
///   can be published. A binary column's task first measures the bytes of each of its segments,
///   and sizes each cell whose last measure that is ([BinaryFill]).
public final class ScheduledFlatColumn extends ScheduledColumn<BatchExchange.Batch> {

    /// The task's scratch for a page of a column that is not binary, whose values are filled
    /// within its task.
    private static final DecodeBuffers.Slot<PageDecoder.LevelScratch> SCRATCH = DecodeBuffers.slot();

    /// The column's filter fragment, run on each batch as it completes; `null` for none.
    private final ColumnBatchMatcher columnFilter;
    /// Whether the column is binary, its cells [Cell#measured].
    private final boolean binary;
    /// Raised once a reader needed the views of deferred dictionary values; a binary column builds
    /// them as its cells complete from then on.
    private final BinaryBatchValues.ViewDemand viewDemand = new BinaryBatchValues.ViewDemand();

    /// The column's view demand, for tests: whether a reader asked for its views, and raising it
    /// before the read starts.
    BinaryBatchValues.ViewDemand viewDemand() {
        return viewDemand;
    }

    /// Scratch of binary pages whose segments are all filled, for the column's next binary pages.
    /// Polled and offered without the lock, by page tasks and by the threads that size cells.
    private final ArrayBlockingQueue<PageDecoder.LevelScratch> binaryScratch =
            new ArrayBlockingQueue<>(4 * ScheduledColumn.MAX_INFLIGHT_PAGES);

    // === Guarded by `lock` ===
    private final Placer<Cell> placer;
    /// Cells whose seal came after their last fill, which a [SealedFillTask] completes.
    private final ArrayDeque<Cell> sealedComplete = new ArrayDeque<>();
    /// A measured cell whose seal came after its last measure, which a [SealedFillTask] fails the
    /// read with; `null` while none is.
    private Cell sealedMeasured;

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
    /// sealed after their last fill or measure (lock held), unless one is queued already. Every
    /// page of such a cell has run, and every queued page starts after it, so the queue stays in
    /// row order.
    private void queueSealedFill() {
        Cell first = sealedMeasured != null ? sealedMeasured : sealedComplete.peekFirst();
        if (first != null && !done && !(ready.peekFirst() instanceof SealedFillTask)) {
            ready.addFirst(new SealedFillTask(first.firstRow));
        }
    }

    /// Completes the cells the fetcher's placement sealed after their last fill, on a
    /// [SealedFillTask], outside the lock: completing a cell runs the column's filter fragment. A
    /// measured cell sealed after its last measure fails the read instead
    /// ([#sealedAfterItsMeasures]).
    private void completeSealed(DecodeBuffers task) {
        while (true) {
            Cell measured;
            Cell complete = null;
            lock.lock();
            try {
                measured = sealedMeasured;
                sealedMeasured = null;
                if (measured == null) {
                    complete = sealedComplete.pollFirst();
                }
            }
            finally {
                lock.unlock();
            }
            if (measured == null && complete == null) {
                return;
            }
            Cell cell = measured != null ? measured : complete;
            try {
                if (measured != null) {
                    throw sealedAfterItsMeasures(measured);
                }
                completeAndPublish(complete, task);
            }
            catch (RuntimeException e) {
                // The cell cannot complete, and a failure queued behind it would never be raised
                pageFailed(e, cell.lastPage());
                return;
            }
        }
    }

    /// The failure of a measured cell sealed after every segment placed into it was measured. A
    /// cell is sealed by the placement of its last page, before that page is measured, unless its
    /// batch spans the end of a row group, which a read with a binary column never plans
    /// ([BatchPlan#endsBatchesAtRowGroupBoundaries]): such a batch would draw on two column chunks'
    /// dictionaries.
    private static IllegalStateException sealedAfterItsMeasures(Cell cell) {
        return new IllegalStateException("Batch plan mismatch: the batch of a binary column at row " + cell.firstRow
                + " is sealed after its last page, but a read with a binary column ends every batch at the end of its row group");
    }

    /// What the placer's decisions do to the column; runs under the column lock.
    private final class PlacerSink implements Placer.Sink<Cell> {

        @Override
        public Cell open(String fileName, boolean alwaysMatches, int plannedRecords, int capacity, long firstRow) {
            Cell cell = new Cell(fileName, alwaysMatches, plannedRecords, capacity, firstRow, binary);
            publishOrder.addLast(cell);
            return cell;
        }

        @Override
        public void add(Cell cell, PageTask page, int recordStart, int recordEnd, int valueStart, int valueEnd) {
            page.segments.add(cell.add(page, recordStart, recordEnd));
        }

        @Override
        public void sealed(Cell cell) {
            if (cell.measured && cell.unsized.decrementAndGet() == 0 && sealedMeasured == null) {
                sealedMeasured = cell;
            }
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
            taskEnded();
        }
    }

    /// Decodes a page of a binary column, measures the bytes of its segments, sizes the cells whose
    /// last measure that is, and fills its segments. Every segment is measured before any is
    /// filled, so that a cell this page's measure sizes is sized before the page fills it.
    private void runBinaryPage(PageTask page, DecodeBuffers buffers) {
        // The page's own scratch: a segment that parks reads the page's bytes after this task
        PageDecoder.LevelScratch scratch = binaryScratch.poll();
        page.ownScratch = scratch != null ? scratch : new PageDecoder.LevelScratch();
        try {
            fillBinaryPage(page, page.ownScratch, buffers);
        }
        finally {
            release(page);
        }
    }

    private static PageDecoder.LevelScratch scratch(DecodeBuffers buffers) {
        PageDecoder.LevelScratch scratch = buffers.get(SCRATCH);
        if (scratch == null) {
            scratch = new PageDecoder.LevelScratch();
            buffers.set(SCRATCH, scratch);
        }
        return scratch;
    }

    /// Gives up a hold on a flat binary page's scratch, returning it to the column's pool at the last.
    private void release(PageTask page) {
        if (page.holds.decrementAndGet() == 0) {
            PageDecoder.LevelScratch scratch = page.ownScratch;
            page.ownScratch = null;
            binaryScratch.offer(scratch);
        }
    }

    private void fillBinaryPage(PageTask page, PageDecoder.LevelScratch scratch, DecodeBuffers buffers) {
        PageInfo info = page.info;
        BinaryPageValues values = info.isNullPlaceholder()
                ? page.decoder.nullsInPlace(info.placeholderNumValues(), scratch).binary()
                : page.decoder.decodeFlatBinary(info.pageData(), info.dictionary(), scratch, buffers);
        checkRows(info, values.size());
        List<Segment> segments = page.segments;
        for (int i = 0; i < segments.size(); i++) {
            Segment segment = segments.get(i);
            segment.bytes = BinaryFill.measure(segment, values);
            if (segment.cell.unsized.decrementAndGet() == 0) {
                size(segment.cell, buffers);
            }
        }
        for (int i = 0; i < segments.size(); i++) {
            if (done) {
                return;
            }
            Segment segment = segments.get(i);
            Cell cell = segment.cell;
            BinaryBatchValues out = (BinaryBatchValues) cell.allocate(exchange, column).values;
            // A segment that parks holds the page's bytes until the sizing fills it
            page.holds.incrementAndGet();
            if (BinaryFill.fill(segment, values, out, maxDefinitionLevel)) {
                page.holds.decrementAndGet();
                filled(cell, buffers);
            }
        }
    }

    /// Sizes a measured cell, writes the bytes of the segments that parked before it was sized,
    /// and gives up the sizing's token, completing the cell if that was the last.
    private void size(Cell cell, DecodeBuffers task) {
        BinaryBatchValues out = (BinaryBatchValues) cell.allocate(exchange, column).values;
        List<Segment> parked = BinaryFill.size(cell, out);
        if (parked != null) {
            for (int i = 0; i < parked.size(); i++) {
                Segment segment = parked.get(i);
                BinaryFill.unpark(segment, out, maxDefinitionLevel);
                release(segment.page);
                filled(cell, task);
            }
        }
        filled(cell, task);
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
