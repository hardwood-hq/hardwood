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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.internal.predicate.ColumnBatchMatcher;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

/// A flat column whose pages a [ReadScheduler] submits to the decode pool, in row order with the
/// read's other scheduled columns, and whose page tasks decode straight into the column's batches.
///
/// - The **fetcher**, a virtual thread, pulls the column's pages from its [PageSource], which does
///   every `InputFile` access, places each into the [Cell]s of the read's batch plan ([Placer])
///   and queues it for the scheduler. It keeps at most [#FETCH_AHEAD] pages queued.
/// - A **page task**, on the decode pool, decodes its page and fills its segments, allocating a
///   cell on the first fill that reaches it; the task that fills a cell's last segment completes
///   it and publishes what can be published. A binary column's task first measures the bytes of
///   each of its segments, and sizes each cell whose last measure that is ([BinaryFill]).
/// - **Publishing** takes the completed cells at the head of the column's publish order into its
///   [BatchExchange], without waiting. A cell that finds no room stays at the head, and the
///   consumer publishes it when it takes a batch ([BatchExchange#setRoomListener]).
///
/// The column lock guards the placer, the queue of placed pages and the publish order. It is held
/// for bookkeeping only: no decoding, filling or I/O runs under it.
public final class ScheduledColumn implements ColumnPipeline {

    /// Pages the fetcher keeps placed and queued for the scheduler.
    private static final int FETCH_AHEAD = 2;

    /// Stands in the publish order for the end of the read.
    private static final Object END = new Object();

    private static final ThreadLocal<PageDecoder.LevelScratch> SCRATCH =
            ThreadLocal.withInitial(PageDecoder.LevelScratch::new);

    private final ColumnSchema column;
    private final PageSource pageSource;
    private final BatchExchange<BatchExchange.Batch> exchange;
    private final DecompressorFactory decompressorFactory;
    private final int maxDefinitionLevel;
    private final ReadScheduler scheduler;
    /// The column's filter fragment, run on each batch as it completes; `null` for none.
    private final ColumnBatchMatcher columnFilter;
    /// Whether the column is binary, its cells [Cell#measured].
    private final boolean binary;
    /// Raised once a reader needed the views of deferred dictionary values; a binary column builds
    /// them as its cells complete from then on.
    private final BinaryBatchValues.ViewDemand viewDemand = new BinaryBatchValues.ViewDemand();

    // === Guarded by `lock` ===
    private final ReentrantLock lock = new ReentrantLock();
    private final Placer placer;
    /// Pages placed and not yet submitted, in page order.
    private final ArrayDeque<PageTask> ready = new ArrayDeque<>();
    /// Cells in plan order, then [#END] or the failure that ends the read.
    private final ArrayDeque<Object> publishOrder = new ArrayDeque<>();
    /// Cells whose seal came after their last fill, which the fetcher completes once it has
    /// released the lock.
    private final ArrayDeque<Cell> sealedComplete = new ArrayDeque<>();
    /// Measured cells whose seal came after their last measure, which the fetcher sizes once it
    /// has released the lock.
    private final ArrayDeque<Cell> sealedSizable = new ArrayDeque<>();

    // === Read by the scheduler ===
    /// The read's row the column's next page starts at or after.
    private volatile long nextRow;
    /// The read's row of the column's first record not yet published: the first row of the cell
    /// at the head of the publish order, or [#nextRow] when every cell placed is published.
    private volatile long publishedRow;
    /// The read's row after each page submitted whose rows are not all published, in page order.
    private final ArrayDeque<Long> submittedEnds = new ArrayDeque<>();

    // === Owned by the scheduler, read and written only by the thread submitting ===
    /// Raised when the column's state changed since the scheduler last indexed it.
    final AtomicBoolean changePending = new AtomicBoolean();
    /// The column's position among the read's columns, which orders columns with equal keys.
    int scheduleId;
    /// Where the scheduler indexed the column: [ReadScheduler] keeps it in at most one of its
    /// queued and pending sets, by the key last read here.
    boolean indexedQueued;
    boolean indexedPending;
    boolean indexedPublished;
    long indexedHeadRow;
    long indexedNextRow;
    long indexedPublishedRow;

    // === Pipeline control ===
    /// Set when the column stops: closed, its last batch published, or failed.
    private volatile boolean done;
    /// Page tasks submitted and not finished, which [#close()] waits for.
    private final AtomicInteger tasks = new AtomicInteger();
    /// Raised when a publish found no room, so the consumer publishes when it takes a batch.
    private final AtomicBoolean publishWanted = new AtomicBoolean();
    private volatile Thread fetcherThread;
    private volatile Thread closingThread;

    // === Fetcher only ===
    private PageDecoder pageDecoder;
    private BatchPlan.RowGroupBatches rowGroup;
    /// Kept records of the current row group the column placed so far.
    private long rowInRowGroup;

    /// Creates a scheduled column and adds it to `scheduler`, which starts it.
    ///
    /// @param columnFilter the column's filter fragment, which writes each batch's `matches` as
    ///        the batch completes; `null` for none
    public ScheduledColumn(ReadScheduler scheduler, PageSource pageSource, BatchExchange<BatchExchange.Batch> exchange,
                           ColumnSchema column, int batchCapacity, DecompressorFactory decompressorFactory,
                           ColumnBatchMatcher columnFilter) {
        this.scheduler = scheduler;
        this.columnFilter = columnFilter;
        this.pageSource = pageSource;
        this.exchange = exchange;
        this.column = column;
        this.decompressorFactory = decompressorFactory;
        this.maxDefinitionLevel = column.maxDefinitionLevel();
        this.binary = switch (column.type()) {
            case INT32, INT64, FLOAT, DOUBLE, BOOLEAN -> false;
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> true;
        };
        this.placer = new Placer(new PlacerSink(), batchCapacity, binary);
        exchange.setRoomListener(this::roomPossible);
        scheduler.add(this);
    }

    /// Starts the fetcher. Called by the scheduler.
    void start() {
        fetcherThread = Thread.ofVirtual().unstarted(this::runFetcher);
        fetcherThread.start();
    }

    /// Stops every column of the read ([ReadScheduler#stop()]) and waits until this one is
    /// quiescent: its fetcher has exited and every page task submitted for it has finished, so
    /// that the caller can release what the `InputFile` owns. Nothing is interrupted.
    ///
    /// A read's columns are closed together, one after another. Stopping them all on the first
    /// close lets the waits of every column overlap: a column still running while an earlier one
    /// is awaited would go on submitting and decoding pages, and its fetcher would only be woken
    /// once its own close is reached.
    @Override
    public void requestStop() {
        scheduler.stop();
    }

    @Override
    public void close() {
        scheduler.stop();
        Thread fetcher = fetcherThread;
        boolean interrupted = fetcher != null && ColumnWorker.joinUninterruptibly(fetcher);
        // Bounded wait, re-checked: the last task to finish unparks this thread.
        while (tasks.get() > 0) {
            LockSupport.parkNanos(this, ColumnWorker.WAKE_CHECK_NANOS);
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /// Stops the column without waiting for it: no publish runs after it, its consumer sees the
    /// end of the stream, its fetcher is woken to exit, and the calling thread is the one its last
    /// page task wakes. Called by [ReadScheduler#stop()], on the thread that closes the read.
    void signalStop() {
        closingThread = Thread.currentThread();
        stop();
        exchange.finish();
        unparkFetcher();
    }

    /// Sets `done` under the column lock, so that no publish runs after it.
    private void stop() {
        lock.lock();
        try {
            done = true;
        }
        finally {
            lock.unlock();
        }
    }

    boolean isDone() {
        return done;
    }

    // ==================== Scheduler side ====================

    /// The column's next queued page, `null` if none is queued.
    PageTask peekReady() {
        lock.lock();
        try {
            return ready.peekFirst();
        }
        finally {
            lock.unlock();
        }
    }

    long nextRow() {
        return nextRow;
    }

    long publishedRow() {
        return publishedRow;
    }

    /// The column's pages submitted whose rows are not all published: the pages its read-ahead
    /// holds.
    int pagesAhead() {
        lock.lock();
        try {
            long published = publishedRow;
            while (!submittedEnds.isEmpty() && submittedEnds.peekFirst() <= published) {
                submittedEnds.pollFirst();
            }
            return submittedEnds.size();
        }
        finally {
            lock.unlock();
        }
    }

    /// Takes the column's next queued page off the queue and counts it as a task. The task is
    /// counted before `done` is read, so [#close()] either sees the count or this sees `done`.
    ///
    /// @return the page, or `null` if the column is done
    PageTask takeReady() {
        tasks.incrementAndGet();
        PageTask page;
        lock.lock();
        try {
            page = done ? null : ready.pollFirst();
            if (page != null) {
                submittedEnds.addLast(page.firstRow + page.records());
            }
        }
        finally {
            lock.unlock();
        }
        if (page == null) {
            // Not submitted, so not counted by the scheduler.
            uncountTask();
        }
        unparkFetcher();
        return page;
    }

    /// Ends the read with the decode pool's refusal of a page task, which a closed context gives.
    void rejected(RejectedExecutionException e) {
        taskEnded();
        signalError(ColumnWorker.contextClosed(e));
    }

    // ==================== Fetcher ====================

    private void runFetcher() {
        try {
            while (!done) {
                PageInfo info = pageSource.next();
                if (info == null) {
                    lock.lock();
                    try {
                        placer.end();
                    }
                    finally {
                        lock.unlock();
                    }
                    completeSealed();
                    break;
                }
                if (!info.isBoundaryMarker()
                        && (pageDecoder == null || !pageDecoder.isCompatibleWith(info.columnMetaData()))) {
                    pageDecoder = new PageDecoder(info.columnMetaData(), info.columnSchema(), decompressorFactory);
                }
                if (!place(info)) {
                    break;
                }
                scheduler.submit(this);
                while (!done && queued() >= FETCH_AHEAD) {
                    LockSupport.parkNanos(this, ColumnWorker.WAKE_CHECK_NANOS);
                }
            }
        }
        catch (Exception e) {
            endWithFailure(ColumnWorker.placeFailure(e, column, pageSource.footersSupplied(),
                    pageSource.getCurrentFileName(), pageSource.getCurrentRowGroupIndex(),
                    pageSource.getCurrentPageIndex()), pageSource.isBetweenWorkItems());
        }
        catch (Error err) {
            signalError(err);
            throw err;
        }
        finally {
            nextRow = Long.MAX_VALUE;
            lock.lock();
            try {
                updatePublishedRow();
            }
            finally {
                lock.unlock();
            }
            scheduler.submit(this);
        }
    }

    /// Places a page and queues it for the scheduler, unless it has no records to decode.
    ///
    /// @return whether the column goes on
    private boolean place(PageInfo info) {
        BatchPlan.RowGroupBatches batches = pageSource.getCurrentBatches();
        if (batches != rowGroup) {
            rowGroup = batches;
            rowInRowGroup = 0;
        }
        PageTask page = new PageTask(info, pageSource.getCurrentFileName(), pageSource.getCurrentRowGroupIndex(),
                pageSource.getCurrentPageIndex(), batches, pageDecoder, batches.readRowsBefore() + rowInRowGroup);
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

    private int queued() {
        lock.lock();
        try {
            return ready.size();
        }
        finally {
            lock.unlock();
        }
    }

    /// Ends the stream with the fetcher's failure, raised once every cell before it is published.
    /// A failure between work items publishes the open cell's partial batch first, as every
    /// column's batch ends there; one inside a work item abandons it.
    private void endWithFailure(Exception failure, boolean betweenWorkItems) {
        lock.lock();
        try {
            if (betweenWorkItems) {
                placer.sealOpenCell();
            }
            else {
                Cell abandoned = placer.abandonOpenCell();
                if (abandoned != null) {
                    publishOrder.remove(abandoned);
                    // Its pages still fill it; they must not complete it.
                    abandoned.pending.incrementAndGet();
                }
            }
            publishOrder.addLast(failure);
            publishReady();
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
                PageTask page = (sizable != null ? sizable : complete).lastPage();
                signalError(ColumnWorker.placeFailure(e, column, pageSource.footersSupplied(), page.fileName,
                        page.rowGroup, page.pageIndex));
                return;
            }
        }
    }

    /// What the placer's decisions do to the column; runs under the column lock.
    private final class PlacerSink implements Placer.Sink {

        @Override
        public void opened(Cell cell) {
            publishOrder.addLast(cell);
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
    void runPage(PageTask page) {
        try {
            if (done) {
                return;
            }
            PageDecoder.LevelScratch scratch = SCRATCH.get();
            if (binary) {
                runBinaryPage(page, scratch);
                return;
            }
            PageInfo info = page.info;
            FlatPageValues values = null;
            Page decoded = null;
            if (info.isNullPlaceholder()) {
                decoded = page.decoder.nullPage(info.placeholderNumValues());
            }
            else {
                values = page.decoder.decodeFlat(info.pageData(), info.dictionary(), scratch);
                if (values == null) {
                    decoded = page.decoder.decodePage(info.pageData(), info.dictionary(), scratch);
                }
            }
            checkRows(info, values != null ? values.size() : decoded.size());
            for (int i = 0; i < page.segments.size(); i++) {
                if (done) {
                    return;
                }
                Segment segment = page.segments.get(i);
                Cell cell = segment.cell;
                FlatFill.fill(segment, values, decoded, cell.allocate(exchange, column).values, maxDefinitionLevel);
                filled(cell);
            }
        }
        catch (Exception e) {
            signalError(ColumnWorker.placeFailure(e, column, pageSource.footersSupplied(), page.fileName,
                    page.rowGroup, page.pageIndex));
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
    private void runBinaryPage(PageTask page, PageDecoder.LevelScratch scratch) {
        PageInfo info = page.info;
        BinaryPageValues values = info.isNullPlaceholder()
                ? null
                : page.decoder.decodeFlatBinary(info.pageData(), info.dictionary(), scratch);
        if (values == null) {
            Page decoded = info.isNullPlaceholder()
                    ? page.decoder.nullPage(info.placeholderNumValues())
                    : page.decoder.decodePage(info.pageData(), info.dictionary(), scratch);
            values = BinaryPageValues.of(decoded, scratch);
        }
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
            if (BinaryFill.fill(segment, values, out, maxDefinitionLevel)) {
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
                BinaryFill.unpark(parked.get(i), out);
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
        lock.lock();
        try {
            cell.completed = true;
            publishReady();
        }
        finally {
            lock.unlock();
        }
    }

    /// Writes the batch's matches for the column's filter fragment: all ones when statistics
    /// proved every record matches, else the matcher's verdict, while the values are in this
    /// task's cache. A matcher keeps scratch between calls, and cells complete on several tasks
    /// at once, so its calls are serialized.
    private void testFilter(BatchExchange.Batch batch, int records) {
        if (batch.filterAlwaysMatches) {
            FlatColumnWorker.fillAllOnes(batch.matches, records);
            return;
        }
        synchronized (columnFilter) {
            columnFilter.test(batch, batch.matches);
        }
    }

    // ==================== Publish (column lock) ====================

    /// Publishes the completed cells at the head of the publish order, and raises the item ending
    /// the read once every cell before it is published.
    private void publishReady() {
        Object head;
        while (!done && (head = publishOrder.peekFirst()) != null) {
            if (head == END) {
                publishOrder.pollFirst();
                finishRead();
                return;
            }
            if (head instanceof Throwable failure) {
                publishOrder.pollFirst();
                signalError(failure);
                return;
            }
            Cell cell = (Cell) head;
            if (!cell.completed) {
                break;
            }
            if (!exchange.tryPublish(cell.batch())) {
                publishWanted.set(true);
                // Room may have come free between the try and the flag: either the retry sees it,
                // or the consumer's next take sees the flag.
                if (!exchange.tryPublish(cell.batch())) {
                    break;
                }
            }
            publishOrder.pollFirst();
            cell.release();
        }
        updatePublishedRow();
        scheduler.submit(this);
    }

    /// Run by the consumer after it took a batch: publishes the cells that waited for room.
    /// A publish under the lock that left room continues its own loop.
    private void roomPossible() {
        if (!lock.isHeldByCurrentThread() && publishWanted.compareAndSet(true, false)) {
            lock.lock();
            try {
                publishReady();
            }
            finally {
                lock.unlock();
            }
        }
    }

    /// Recomputes [#publishedRow] after the publish order or [#nextRow] changed (lock held).
    private void updatePublishedRow() {
        Object head = publishOrder.peekFirst();
        publishedRow = head instanceof Cell cell ? cell.firstRow : nextRow;
    }

    private void finishRead() {
        done = true;
        exchange.finish();
        unparkFetcher();
        scheduler.submit(this);
    }

    /// Ends the column's read with `t`: the scheduler failed.
    void fail(Throwable t) {
        signalError(t);
    }

    private void signalError(Throwable t) {
        done = true;
        exchange.signalError(t);
        unparkFetcher();
        scheduler.submit(this);
    }

    private void taskEnded() {
        uncountTask();
        scheduler.pageEnded(this);
    }

    private void uncountTask() {
        if (tasks.decrementAndGet() == 0) {
            Thread closing = closingThread;
            if (closing != null) {
                LockSupport.unpark(closing);
            }
        }
    }

    private void unparkFetcher() {
        Thread t = fetcherThread;
        if (t != null) {
            LockSupport.unpark(t);
        }
    }
}
