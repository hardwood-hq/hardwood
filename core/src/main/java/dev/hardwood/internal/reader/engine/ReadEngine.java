/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.predicate.ColumnBatchMatcher;
import dev.hardwood.internal.predicate.PageDropPredicates;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.batch.NestedIndexMode;
import dev.hardwood.internal.reader.batch.NestedLevelComputer;
import dev.hardwood.internal.reader.fetch.CoalescingPolicy;
import dev.hardwood.internal.reader.memory.InternalProperty;
import dev.hardwood.internal.reader.memory.WorkingSet;
import dev.hardwood.internal.reader.plan.BatchPlan;
import dev.hardwood.internal.reader.step.ReadQueue;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// Runs one read of a [dev.hardwood.reader.ParquetFileReader], for its column reads and its row
/// reads, started by its [PreparedRead] once the reader's columns exist: builds the read's parts, which plan, fetch and decode the row groups of its files and
/// publish them into its columns' exchanges, hands the consumer's polls to them, and closes them.
/// The parts each own their state:
///
/// - [FooterReader] reads the footers of the files after the first and lists them in order.
/// - [RowGroupPlanner] moves each row group's [Unit] through its states: lists it, probes it,
///   plans its pages and ranges once its page index has arrived, batches the units in order,
///   and commits what pruning reports once the consumer reaches it.
/// - [RangeFetcher] fetches each range the read requests ([RangeFetch]) and releases it once its
///   last reader is done.
/// - [HeaderScanner] scans each column chunk without an OffsetIndex header by header.
/// - [PageTracker] tracks each page of a batched unit ([TrackedPage]) until its bytes, its dictionary and
///   its placement are there, and starts its decode then.
/// - [CellWriter] writes each column's pages into the cells of the batch plan: places them in page
///   order, decodes and fills them, and completes the cells into batches.
/// - [ColumnContext] gives every part each decoded column's schema, working set and failure context.
/// - [BatchPublisher] publishes each column's batches in row order, and its end or the read's failure.
/// - [JobScheduler] decides which job starts next, by one priority within three bounds.
/// - [ReadDriver] runs the jobs and applies their results under the read's one lock.
///
/// A failure, of a footer, a file's listing, a row group's planning or a page, is placed at its
/// row in the read's one [ReadFailure], an item the parts report to; `reconcile()` settles it, and
/// each column publishes every batch before it and then ends with it.
///
/// A read with a row limit lists row groups until the limit is covered and truncates the pages past
/// it; once every column has placed its last row, the read plans nothing more.
public final class ReadEngine implements Closeable {

    public static final String FETCH_AHEAD_BYTES_PROPERTY = "hardwood.internal.fetchAheadBytes";

    /// Batch byte budgets a read decodes at most ahead of its first unpublished row (READ_PIPELINE.md,
    /// the look-ahead).
    public static final int CAP_BUDGETS = 8;

    private final ReadDriver driver;
    private final BatchPublisher publisher;
    private final RowGroupPlanner planner;
    private final FooterReader footers;
    private final CellWriter cells;
    private final ReadFailure readFailure = new ReadFailure();
    /// each decoded column's exchange, in decoded order
    private final List<BatchExchange<?>> exchanges;
    private final Runnable closeListener;

    /// Starts `read`'s decoded columns; see [PreparedRead#start]. Closes what it started where its
    /// first file's listing fails, before the failure propagates.
    ReadEngine(PreparedRead read, ProjectedSchema decoded, List<NestedLevelComputer.Layers> layers,
               NestedIndexMode indexMode, boolean fixedListFastPathEnabled, int batchSize, long budgetRows,
               List<BatchExchange<?>> exchanges, ColumnBatchMatcher[] filters, Runnable closeListener) {
        this.closeListener = closeListener;
        HardwoodContextImpl context = read.context();
        FileSchema schema = read.schema();
        ResolvedPredicate filter = read.filter();
        long limit = read.limit();
        // a context of zero threads runs the read's CPU jobs on a queue of its own
        ReadQueue readQueue = context.hasPool() ? null : new ReadQueue(context);
        int n = decoded.getProjectedColumnCount();
        ColumnSchema[] schemas = new ColumnSchema[n];
        boolean[] filterOnly = new boolean[n];
        List<BatchExchange<?>> started = new ArrayList<>(n);
        for (int c = 0; c < n; c++) {
            schemas[c] = schema.getColumn(decoded.toOriginalIndex(c));
            filterOnly[c] = read.projection().isFilterOnly(c);
            String name = schemas[c].name();
            started.add(exchanges != null ? exchanges.get(c) : layers.get(c) != null
                    ? BatchExchange.detachingNested(name, batchSize) : BatchExchange.detachingFlat(name, batchSize));
        }
        this.exchanges = List.copyOf(started);
        driver = new ReadDriver(context, readQueue, defaultFetchAheadBytes(CoalescingPolicy.maxSpanBytes()),
                this::reconcile, t -> readFailure.failAt(0, t));
        JobScheduler scheduler = driver.scheduler();
        RangeFetcher fetcher = new RangeFetcher(scheduler);
        publisher = new BatchPublisher(fetcher, readFailure, started, batchSize, CAP_BUDGETS * budgetRows);
        ColumnContext columns = new ColumnContext(schemas, layers, new WorkingSet(n), read.files().footersSupplied());
        cells = new CellWriter(scheduler, publisher, readFailure, columns, started, indexMode, fixedListFastPathEnabled,
                batchSize, context.decompressorFactory(), filters);
        PageTracker tracker = new PageTracker(scheduler, fetcher, publisher, readFailure, cells, columns, context);
        HeaderScanner scanner = new HeaderScanner(scheduler, fetcher, tracker, cells, columns, readFailure,
                read.pruning().prunesByMetadata() ? PageDropPredicates.byColumn(filter) : Map.of());
        planner = new RowGroupPlanner(scheduler, fetcher, publisher, readFailure, tracker, scanner, columns,
                new BatchPlan(batchSize, BatchPlan.endsBatchesAtRowGroupBoundaries(decoded.getProjectedColumns()),
                        filter != null, limit == 0 ? 0 : limit + read.leadingRows()), schemas, filterOnly,
                read.pruning(), limit > 0 ? limit : Long.MAX_VALUE, filter != null, read.skip(), read.maskedRows());
        footers = new FooterReader(read.files(), scheduler, planner, readFailure, read.compatibility(), filter,
                limit > 0, driver::update);
        for (int c = 0; c < n; c++) {
            int column = c;
            if (readQueue != null) {
                started.get(c).setQueue(readQueue);
            }
            started.get(c).setRoomListener(() -> driver.update(() -> publisher.publishable(column)));
        }
        try {
            driver.locked(() -> footers.start(read.file(), schema, read.firstKept(), read.firstOrdinals()));
        }
        catch (RuntimeException | Error e) {
            closeAfter(e);
            throw e;
        }
    }

    /// F for a read whose largest request is `maxSpanBytes`: twice that unless overridden through
    /// [#FETCH_AHEAD_BYTES_PROPERTY]. A value that is not a non-negative byte count is rejected
    /// rather than replaced by the default.
    static long defaultFetchAheadBytes(int maxSpanBytes) {
        return InternalProperty.longValue(FETCH_AHEAD_BYTES_PROPERTY, 2L * maxSpanBytes, 0,
                "a non-negative byte count");
    }

    /// Each decoded column's exchange, in decoded order.
    public List<BatchExchange<?>> exchanges() {
        return exchanges;
    }

    /// Ends or seals the columns' placement where the read has reached its end or a failure
    /// between units, publishes what can be, and says where the consumer stands; under the lock.
    private JobScheduler.Position reconcile() {
        long batchedTo = planner.batchedKey();
        if (planner.allBatched() && !readFailure.failed()) {
            cells.end(planner.endKey());
        }
        // the failure stands once no work at or before its row is left; only then do the columns
        // seal or abandon their open cells at it, which can move it to a lower row
        while (readFailure.failed() && !readFailure.settled()) {
            if (!readFailure.settle(driver.scheduler().pendingThrough(readFailure.row()))) {
                break;
            }
            // every column meets it now
            publisher.allPublishable();
            cells.sealAt(readFailure.cause(), readFailure.row(), batchedTo >= readFailure.row());
        }
        publisher.publish(batchedTo, planner.allBatched());
        JobScheduler.Position at = publisher.position(batchedTo);
        if (cells.allPlacementEnded()) {
            // no column takes a further page: nothing of a unit not batched starts, neither its
            // probe, plan and index fetches nor a footer past it
            at = new JobScheduler.Position(at.need(), at.reach(), Math.min(at.end(), batchedTo - 1));
        }
        // footers start ahead of the file the read's data has reached, which F bounds, or of the
        // consumer's need where that is further: the data never waits for a footer F would let
        // it fetch
        footers.advance(publisher.firstUnpublished(batchedTo), Math.max(at.need(), driver.scheduler().fetchedTo() + 1));
        if (at.end() != Long.MAX_VALUE) {
            footers.endAt(at.end());
        }
        planner.report(publisher.firstUnpublished(batchedTo));
        return at;
    }

    /// Stops the read and waits until no job of it is in flight, then releases what it holds.
    /// Interrupts nothing. Idempotent.
    @Override
    public void close() {
        driver.locked(publisher::stop);
        driver.close();
        // under the lock, beside another read's load ending, which then starts nothing
        driver.locked(footers::close);
        closeListener.run();
    }

    /// Closes the read after `failure` ended its start or the build of its reader, adding a failure
    /// of the close to `failure` as suppressed.
    public void closeAfter(Throwable failure) {
        try {
            close();
        }
        catch (RuntimeException | Error closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    /// Waits until no job of the read is in flight, running the read's queue meanwhile on a context
    /// of zero threads, for tests.
    public void awaitQuiescent() {
        driver.awaitIdle();
    }
}
