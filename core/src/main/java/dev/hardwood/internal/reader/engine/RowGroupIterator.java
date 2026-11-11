/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import dev.hardwood.InputFile;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.predicate.ColumnBatchMatcher;
import dev.hardwood.internal.predicate.PageDropPredicates;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.batch.NestedIndexMode;
import dev.hardwood.internal.reader.batch.NestedLevelComputer;
import dev.hardwood.internal.reader.fetch.CoalescingPolicy;
import dev.hardwood.internal.reader.footer.FileColumnOrdinals;
import dev.hardwood.internal.reader.memory.InternalProperty;
import dev.hardwood.internal.reader.memory.WorkingSet;
import dev.hardwood.internal.reader.plan.BatchPlan;
import dev.hardwood.internal.reader.plan.FileMetadataCache;
import dev.hardwood.internal.reader.plan.RowGroupPruning;
import dev.hardwood.internal.reader.plan.SchemaCompatibility;
import dev.hardwood.internal.reader.step.ReadQueue;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.internal.schema.ReadProjection;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// Runs one read of a [dev.hardwood.reader.ParquetFileReader], for its column reads and its row
/// reads: builds the read's parts, which plan, fetch and decode the row groups of its files and
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
/// - [PagePlacer] places each column's pages in page order into the cells of the batch plan.
/// - [BatchPublisher] publishes each column's batches in row order and places the read's failure.
/// - [JobScheduler] decides which job starts next, by one priority within three bounds.
/// - [ReadDriver] runs the jobs and applies their results under the read's one lock.
///
/// A failure, of a footer, a file's listing, a row group's planning or a page, is placed at its
/// row: each column publishes every batch before it and then ends with it.
///
/// A read with a row limit lists row groups until the limit is covered and truncates the pages past
/// it; once every column has placed its last row, the read plans nothing more.
public final class RowGroupIterator implements Closeable {

    public static final String FETCH_AHEAD_BYTES_PROPERTY = "hardwood.internal.fetchAheadBytes";

    /// Batch byte budgets a read decodes at most ahead of its first unpublished row (READ_PIPELINE.md,
    /// the look-ahead).
    public static final int CAP_BUDGETS = 8;

    private final FileMetadataCache files;
    private final HardwoodContextImpl context;
    private final Runnable closeListener;
    private FileSchema schema;
    /// the file, as the reader holds it: with its tail
    private InputFile file;
    private ReadDriver driver;
    private BatchPublisher publisher;
    private RowGroupPlanner planner;
    private FooterReader footers;
    private PagePlacer placer;
    private ResolvedPredicate filter;
    /// the read's row limit, `0` for none: under a filter it counts matching rows
    private long limit;
    private long skip;
    /// the rows of the row group the skip lands in that precede it, and whether that row group's
    /// plan masks them out; where it cannot, the reader drops them ([#leadingRows])
    private long leading;
    private boolean masked;
    private ReadProjection projection;
    private RowGroupPruning pruning;
    private SchemaCompatibility compatibility;
    private FileColumnOrdinals firstOrdinals;
    /// the first file's row groups its statistics keep
    private RowGroupPruning.FilteredRowGroups firstKept;

    /// @throws IllegalStateException on a thread of `context`'s own pool, whose read would wait for
    ///         the jobs it holds up
    public RowGroupIterator(FileMetadataCache files, HardwoodContextImpl context,
                            Consumer<RowGroupIterator> closeListener) {
        context.requireOffPool();
        this.files = files;
        this.context = context;
        this.closeListener = () -> closeListener.accept(this);
    }

    /// F for a read whose largest request is `maxSpanBytes`: twice that unless overridden through
    /// [#FETCH_AHEAD_BYTES_PROPERTY]. A value that is not a non-negative byte count is rejected
    /// rather than replaced by the default.
    static long defaultFetchAheadBytes(int maxSpanBytes) {
        return InternalProperty.longValue(FETCH_AHEAD_BYTES_PROPERTY, 2L * maxSpanBytes, 0,
                "a non-negative byte count");
    }

    /// Sets up the read's planning, before its columns start.
    ///
    /// @param rowGroups the first file's row groups, `null` for all of them
    /// @param filter the read's resolved filter, `null` for none
    /// @param metadataFiltering whether the filter takes metadata-derived shortcuts
    /// @param limit the read's row limit, `0` for none; under a filter it counts matching rows, and
    ///        bounds the listing only over row groups statistics prove to match
    /// @param skip the read's leading rows it does not return, `0` for none; a read with a filter
    ///        skips none, its reader skipping matching rows
    /// @throws IOException the failure of a footer of a file the skip reaches, read on the caller's
    ///         thread
    public void initialize(FileSchema schema, List<RowGroup> rowGroups, ReadProjection projection,
                           ResolvedPredicate filter, boolean metadataFiltering, long limit, long skip)
            throws IOException {
        this.schema = schema;
        List<RowGroup> first = rowGroups != null ? rowGroups : files.getFileMetaData(0).rowGroups();
        this.file = files.preparedInputFile(0);
        SchemaCompatibility.validateFirstFile(file, schema, projection.decoded(), first);
        this.filter = filter;
        this.projection = projection;
        this.limit = limit;
        this.skip = skip;
        this.pruning = new RowGroupPruning(context, filter, metadataFiltering);
        this.compatibility = new SchemaCompatibility(schema, projection.decoded(), filter);
        this.firstOrdinals = FileColumnOrdinals.identity(schema.getColumnCount(), filter,
                files.getFile(0).boundsReadability());
        this.firstKept = pruning.filterRowGroups(first, file, firstOrdinals);
        landSkip(first);
    }

    /// Finds the row group the skip lands in, past the row groups it drops whole, and the rows of
    /// it before the skip's end; reads the footers of the files it reaches on the caller's thread.
    /// The plan of a row group of the first file masks those rows out where every nested decoded
    /// column has an OffsetIndex, which a page mask needs.
    private void landSkip(List<RowGroup> first) throws IOException {
        long left = skip;
        for (int i = 0; i < files.inputFiles().size() && left > 0; i++) {
            for (RowGroup rg : i == 0 ? first : files.getFileMetaData(i).rowGroups()) {
                if (left < rg.numRows()) {
                    leading = left;
                    masked = i == 0 && takesMask(rg);
                    return;
                }
                left -= rg.numRows();
            }
        }
    }

    private boolean takesMask(RowGroup rg) {
        ProjectedSchema decoded = projection.decoded();
        for (int c = 0; c < decoded.getProjectedColumnCount(); c++) {
            ColumnSchema column = schema.getColumn(decoded.toOriginalIndex(c));
            if (column.maxRepetitionLevel() > 0 && rg.columns().get(column.columnIndex()).offsetIndexOffset() == null) {
                return false;
            }
        }
        return true;
    }

    /// The leading rows the reader drops itself before it returns a row: those of the row group the
    /// skip lands in that precede it, where its plan cannot mask them out; `0` otherwise. The read
    /// returns them, and counts them against its limit.
    public long leadingRows() {
        return masked ? 0 : leading;
    }

    public String referenceFileName() {
        return files.inputFiles().getFirst().name();
    }

    /// Starts the read's decoded columns, publishing into detaching exchanges without filter
    /// fragments, as a column read without batch matchers does, and returns each one's exchange,
    /// in decoded order.
    ///
    /// @param layers the layers of each decoded column decoded through levels, `null` for a flat
    ///        one, in decoded order
    /// @param indexMode what the batches of a column decoded through levels carry for their reader
    public List<BatchExchange<?>> start(ProjectedSchema decoded, List<NestedLevelComputer.Layers> layers,
                                        NestedIndexMode indexMode, boolean fixedListFastPathEnabled, int batchSize,
                                        long budgetRows) {
        return start(decoded, layers, indexMode, fixedListFastPathEnabled, batchSize, budgetRows, null, null);
    }

    /// Starts the read's decoded columns and returns each one's exchange, in decoded order.
    ///
    /// @param layers the layers of each decoded column decoded through levels, `null` for a flat
    ///        one, in decoded order
    /// @param indexMode what the batches of a column decoded through levels carry for their reader
    /// @param budgetRows the rows of the read's batch byte budget, which bound its look-ahead
    /// @param exchanges the exchange each decoded column publishes into, in decoded order: a flat
    ///        column's of [BatchExchange.Batch], one decoded through levels of [NestedBatch];
    ///        `null` for detaching exchanges
    /// @param filters the filter fragment of each flat decoded column, `null` for a column without
    ///        one; `null` for none
    public List<BatchExchange<?>> start(ProjectedSchema decoded, List<NestedLevelComputer.Layers> layers,
                                        NestedIndexMode indexMode, boolean fixedListFastPathEnabled, int batchSize,
                                        long budgetRows, List<BatchExchange<?>> exchanges,
                                        ColumnBatchMatcher[] filters) {
        // a context of zero threads runs the read's CPU jobs on a queue of its own
        ReadQueue readQueue = context.hasPool() ? null : new ReadQueue(context);
        int n = decoded.getProjectedColumnCount();
        ColumnSchema[] schemas = new ColumnSchema[n];
        boolean[] filterOnly = new boolean[n];
        List<BatchExchange<?>> started = new ArrayList<>(n);
        for (int c = 0; c < n; c++) {
            schemas[c] = schema.getColumn(decoded.toOriginalIndex(c));
            filterOnly[c] = projection.isFilterOnly(c);
            String name = schemas[c].name();
            started.add(exchanges != null ? exchanges.get(c) : layers.get(c) != null
                    ? BatchExchange.detachingNested(name, batchSize) : BatchExchange.detachingFlat(name, batchSize));
        }
        driver = new ReadDriver(context, readQueue, defaultFetchAheadBytes(CoalescingPolicy.maxSpanBytes()),
                this::settle, t -> publisher.failAt(0, t));
        JobScheduler scheduler = driver.scheduler();
        RangeFetcher fetcher = new RangeFetcher(scheduler);
        publisher = new BatchPublisher(fetcher, started, batchSize, CAP_BUDGETS * budgetRows);
        placer = new PagePlacer(scheduler, publisher, schemas, started, layers, indexMode, fixedListFastPathEnabled,
                batchSize, context.decompressorFactory(), files.footersSupplied(),
                new WorkingSet(n), filters);
        PageTracker tracker = new PageTracker(scheduler, fetcher, publisher, placer, context);
        HeaderScanner scanner = new HeaderScanner(scheduler, fetcher, tracker, placer, publisher,
                pruning.prunesByMetadata() ? PageDropPredicates.byColumn(filter) : Map.of());
        planner = new RowGroupPlanner(scheduler, fetcher, publisher, tracker, scanner, placer, new BatchPlan(batchSize,
                BatchPlan.endsBatchesAtRowGroupBoundaries(decoded.getProjectedColumns()), filter != null,
                limit == 0 ? 0 : limit + leadingRows()), schemas, filterOnly, pruning,
                limit > 0 ? limit : Long.MAX_VALUE, filter != null, skip, masked ? leading : 0);
        footers = new FooterReader(files, scheduler, planner, publisher, compatibility, filter, limit > 0,
                driver::update);
        for (int c = 0; c < n; c++) {
            int column = c;
            if (readQueue != null) {
                started.get(c).setQueue(readQueue);
            }
            started.get(c).setRoomListener(() -> driver.update(() -> publisher.publishable(column)));
        }
        driver.locked(() -> footers.start(file, schema, firstKept, firstOrdinals));
        return started;
    }

    /// Ends or seals the columns' placement where the read has reached its end or a failure
    /// between units, publishes what can be, and says where the consumer stands; under the lock.
    private JobScheduler.Position settle() {
        long batchedTo = planner.batchedKey();
        if (planner.allBatched() && !publisher.failed()) {
            placer.end(planner.endKey());
        }
        // the failure stands once no work at or before its row is left; only then do the columns
        // seal or abandon their open cells at it, which can move it to a lower row
        while (publisher.failed() && !publisher.settled()) {
            publisher.settle(driver.scheduler().pendingThrough(publisher.failedAt()));
            if (!publisher.settled()) {
                break;
            }
            placer.sealAt(publisher.failure(), publisher.failedAt(), batchedTo >= publisher.failedAt());
        }
        publisher.publish(batchedTo, planner.allBatched());
        JobScheduler.Position at = publisher.position(batchedTo);
        if (placer.allPlacementEnded()) {
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
        if (driver != null) {
            driver.locked(publisher::stop);
            driver.close();
            // under the lock, beside another read's load ending, which then starts nothing
            driver.locked(footers::close);
        }
        closeListener.run();
    }

    /// Waits until no job of the read is in flight, running the read's queue meanwhile on a context
    /// of zero threads, for tests.
    public void awaitQuiescent() {
        if (driver != null) {
            driver.awaitIdle();
        }
    }
}
