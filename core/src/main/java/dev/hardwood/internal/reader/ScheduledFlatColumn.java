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

    private static final ThreadLocal<PageDecoder.LevelScratch> SCRATCH =
            ThreadLocal.withInitial(PageDecoder.LevelScratch::new);

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
    /// Cells whose seal came after their last fill, which the fetcher completes once it has
    /// released the lock.
    private final ArrayDeque<Cell> sealedComplete = new ArrayDeque<>();
    /// Measured cells whose seal came after their last measure, which the fetcher sizes once it
    /// has released the lock.
    private final ArrayDeque<Cell> sealedSizable = new ArrayDeque<>();

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
        }
        finally {
            lock.unlock();
        }
        completeSealed();
        return goesOn;
    }

    @Override
    void pagesEnded() {
        lock.lock();
        try {
            placer.end();
        }
        finally {
            lock.unlock();
        }
        completeSealed();
    }

    @Override
    void fetchFailed(Exception failure, boolean betweenWorkItems) {
        lock.lock();
        try {
            Cell abandoned = null;
            if (betweenWorkItems) {
                placer.sealOpenCell();
            }
            else {
                abandoned = placer.abandonOpenCell();
                if (abandoned != null) {
                    // Its pages still fill it; they must not complete it.
                    abandoned.pending.incrementAndGet();
                }
            }
            endWithFailure(failure, abandoned);
        }
        finally {
            lock.unlock();
        }
        completeSealed();
    }

    /// Sizes and completes the cells the fetcher's placement sealed after their last measure or
    /// fill, outside the lock: sizing a cell allocates its bytes, and completing it runs the
    /// column's filter fragment.
    private void completeSealed() {
        while (true) {
            Cell sizable;
            Cell complete = null;
            lock.lock();
            try {
                sizable = sealedSizable.pollFirst();
                if (sizable == null) {
                    complete = sealedComplete.pollFirst();
                }
            }
            finally {
                lock.unlock();
            }
            if (sizable == null && complete == null) {
                return;
            }
            try {
                if (sizable != null) {
                    size(sizable);
                }
                else {
                    completeAndPublish(complete);
                }
            }
            catch (RuntimeException e) {
                // The cell cannot complete, and a failure queued behind it would never be raised
                pageFailed(e, (sizable != null ? sizable : complete).lastPage());
                return;
            }
        }
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
            if (cell.measured && cell.unsized.decrementAndGet() == 0) {
                sealedSizable.addLast(cell);
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
    void runPage(PageTask page) {
        try {
            if (done) {
                return;
            }
            PageDecoder.LevelScratch scratch = SCRATCH.get();
            if (binary) {
                runBinaryPage(page);
                return;
            }
            PageInfo info = page.info;
            FlatPageValues values = info.isNullPlaceholder()
                    ? page.decoder.nullsInPlace(info.placeholderNumValues(), scratch).flat()
                    : page.decoder.decodeFlat(info.pageData(), info.dictionary(), scratch);
            checkRows(info, values.size());
            for (int i = 0; i < page.segments.size(); i++) {
                if (done) {
                    return;
                }
                Segment segment = page.segments.get(i);
                Cell cell = segment.cell;
                FlatFill.fill(segment, values, cell.allocate(exchange, column).values, maxDefinitionLevel);
                filled(cell);
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
    private void runBinaryPage(PageTask page) {
        // The page's own scratch: a segment that parks reads the page's bytes after this task
        PageDecoder.LevelScratch scratch = binaryScratch.poll();
        page.ownScratch = scratch != null ? scratch : new PageDecoder.LevelScratch();
        try {
            fillBinaryPage(page, page.ownScratch);
        }
        finally {
            release(page);
        }
    }

    /// Gives up a hold on a flat binary page's scratch, returning it to the column's pool at the last.
    private void release(PageTask page) {
        if (page.holds.decrementAndGet() == 0) {
            PageDecoder.LevelScratch scratch = page.ownScratch;
            page.ownScratch = null;
            binaryScratch.offer(scratch);
        }
    }

    private void fillBinaryPage(PageTask page, PageDecoder.LevelScratch scratch) {
        PageInfo info = page.info;
        BinaryPageValues values = info.isNullPlaceholder()
                ? page.decoder.nullsInPlace(info.placeholderNumValues(), scratch).binary()
                : page.decoder.decodeFlatBinary(info.pageData(), info.dictionary(), scratch);
        checkRows(info, values.size());
        List<Segment> segments = page.segments;
        for (int i = 0; i < segments.size(); i++) {
            Segment segment = segments.get(i);
            segment.bytes = BinaryFill.measure(segment, values);
            if (segment.cell.unsized.decrementAndGet() == 0) {
                size(segment.cell);
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
                filled(cell);
            }
        }
    }

    /// Sizes a measured cell, writes the bytes of the segments that parked before it was sized,
    /// and gives up the sizing's token, completing the cell if that was the last.
    private void size(Cell cell) {
        BinaryBatchValues out = (BinaryBatchValues) cell.allocate(exchange, column).values;
        List<Segment> parked = BinaryFill.size(cell, out);
        if (parked != null) {
            for (int i = 0; i < parked.size(); i++) {
                Segment segment = parked.get(i);
                BinaryFill.unpark(segment, out, maxDefinitionLevel);
                release(segment.page);
                filled(cell);
            }
        }
        filled(cell);
    }

    /// Counts one of the cell's tokens given up, and completes the cell at its last.
    private void filled(Cell cell) {
        if (cell.pending.decrementAndGet() == 0) {
            completeAndPublish(cell);
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
    private void completeAndPublish(Cell cell) {
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
        completed(cell);
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
