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
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import dev.hardwood.InputFile;
import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.io.HeldBytes;
import dev.hardwood.internal.io.ReadRanges;
import dev.hardwood.internal.predicate.ColumnBatchMatcher;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.batch.NestedBatch;
import dev.hardwood.internal.reader.batch.NestedIndexMode;
import dev.hardwood.internal.reader.batch.NestedLevelComputer;
import dev.hardwood.internal.reader.decode.PageInfo;
import dev.hardwood.internal.reader.fetch.CoalescingPolicy;
import dev.hardwood.internal.reader.fetch.FetchPlan;
import dev.hardwood.internal.reader.fetch.PageWalk;
import dev.hardwood.internal.reader.fetch.Section;
import dev.hardwood.internal.reader.fetch.Sections;
import dev.hardwood.internal.reader.fetch.Span;
import dev.hardwood.internal.reader.fetch.TailedInputFile;
import dev.hardwood.internal.reader.memory.InternalProperty;
import dev.hardwood.internal.reader.memory.ReadAheadBuffer;
import dev.hardwood.internal.reader.memory.WorkingSet;
import dev.hardwood.internal.reader.pipeline.DecodedColumn;
import dev.hardwood.internal.reader.pipeline.FlatColumn;
import dev.hardwood.internal.reader.pipeline.NestedColumn;
import dev.hardwood.internal.reader.plan.BatchPlan;
import dev.hardwood.internal.reader.plan.FileMetadataCache;
import dev.hardwood.internal.reader.plan.ReadPlanning;
import dev.hardwood.internal.reader.plan.WorkItem;
import dev.hardwood.internal.reader.step.Need;
import dev.hardwood.internal.reader.step.ReadExecutor;
import dev.hardwood.internal.reader.step.ReadQueue;
import dev.hardwood.internal.reader.step.ReadTasks;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.internal.schema.ReadProjection;
import dev.hardwood.internal.util.Lazy;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// One read with a single decision point: every piece of the read's state is held here, under one
/// lock, and [#poll] is the only place that decides what happens next. The work leaves as jobs
/// (a planning stage, a walk's advance, a page's decode, a request's read), which run outside the
/// lock on the read's [ReadExecutor] or, for a read, on a virtual thread, and whose results are
/// applied under the lock ([#update]), each batch of them followed by a poll. A job returns its
/// result as a [Fact], data that names what happened, as does a callback from outside the read;
/// [#take] is the one place a fact changes the read's state. A planning stage, and a walk that
/// reaches the part of a chunk a row limit left out, add their requests through this read's
/// [Sections] as they run, under the lock. On a read of several decode threads
/// a job's thread does not wait for the lock held by another: it queues its result, which the
/// thread that holds the lock applies.
///
/// A read of several files lists them in order, each once its footer is loaded. The footers are
/// jobs of the read like its requests: the next file's footer after the one the read has reached
/// starts at once, those further ahead start in position order behind the earlier files' data,
/// charged to R, at most `filesAhead` files ahead. A file's tail and footer are dropped once every
/// column has passed a work item of a later file. A planning failure, of a footer, a file's listing
/// or a work item's stage, is sticky at its file or work item: each column publishes every batch
/// before it and then ends with it.
///
/// A filtered read decides, before its columns start, whether it has a work item its Bloom filters
/// and dictionaries keep ([#hasLiveWorkItem]): it plans and probes the work items in order up to the
/// first one kept, and no further. The other questions a reader asks before the columns start, the
/// physical skip's residue ([#firstRowGroupSkip]) and whether every work item takes a page mask
/// ([#canFastSkipAllRowGroups]), are answered the same way: the caller waits while the read's jobs
/// list and plan as far as the answer needs.
///
/// A read with a row limit lists work items until the limit is covered, truncates the pages past
/// it, and starts no speculative request (F is zero): it plans the unit the columns are in and the
/// next, and reads ahead only the next file's footer. A column whose placement reaches the limit
/// takes no further page, and once every column's has, the read plans nothing more.
///
/// Every read of a [dev.hardwood.reader.ParquetFileReader] runs here: its column reads and its row
/// reads.
public final class RowGroupIterator implements Closeable, Sections {

    public static final String FETCH_AHEAD_BYTES_PROPERTY = "hardwood.internal.fetchAheadBytes";

    private static final Lazy<System.Logger> LOG = Lazy.of(() -> System.getLogger(RowGroupIterator.class.getName()));

    private final FileMetadataCache files;
    private final HardwoodContextImpl context;
    private final Consumer<RowGroupIterator> closeListener;
    private final ReadExecutor executor;
    private final ReadTasks tasks;
    private final ReadAheadBuffer readAhead;
    private final int maxSpan = CoalescingPolicy.maxSpanBytes();
    /// F: the speculative bytes the read holds, zero for a read with a row limit.
    private long fetchAheadBytes;
    private final long indexWindowBytes = InternalProperty.longValue("hardwood.internal.indexWindowBytes",
            16L * 1024 * 1024, 1, "a positive byte count");

    /// Pages a column holds at most: not decoded for a flat column, not placed for one decoded
    /// through levels.
    private static final int MAX_PAGES_HELD = 8;
    /// Batch byte budgets a read decodes at most ahead of its first unpublished row (READ_PIPELINE.md,
    /// the cap).
    public static final int CAP_BUDGETS = 8;
    /// The admission of a page task the working set refused.
    private static final long REFUSED = Long.MIN_VALUE;

    private final ReentrantLock lock = new ReentrantLock();
    /// Facts queued for the thread that holds the lock ([#update]).
    private final ConcurrentLinkedQueue<Fact> pending = new ConcurrentLinkedQueue<>();
    /// Whether a thread waits for the lock to apply its fact: on a read of one decode thread,
    /// which no other job thread contends with, and whose job, applying its own result, goes on to
    /// the job its poll emits without a hand-off.
    private final boolean waitForLock;
    /// Runs [#update] with [PlanNeedMet]: a need a stage job lacked was met.
    private final Runnable planNeedMet = () -> update(new PlanNeedMet());

    private FileSchema referenceSchema;
    private ReadPlanning planning;
    /// Whether the read has a filter, under which its Bloom filters and dictionaries may drop work
    /// items and its batches close where the always-match flag changes.
    private boolean filtered;
    /// The read's decoded columns, each of which passes every unit.
    private int columnCount;
    private WorkingSet workingSet;
    private int unitCount;
    private int fileCount;
    /// The files after the one the read has reached whose footers start ahead of it.
    private int filesAhead;
    /// Whether every file's bytes are in memory, so that no data request waits to start and F does
    /// not bound planning.
    private boolean allInMemory;
    /// The charge to R of a footer started ahead of the file after the one the read has reached.
    private final int footerCharge = TailedInputFile.tailBytes();

    // ==================== State, guarded by the lock ====================

    /// The next unit to plan, the stage need it waits for, and whether a stage job runs.
    private int nextPlan;
    private Need planNeed;
    private boolean planRunning;
    /// The units planned, the columns still to pass each, and each one's file.
    private final List<Boolean> planned = new ArrayList<>();
    private final List<Integer> unitHolders = new ArrayList<>();
    private final List<Integer> unitFiles = new ArrayList<>();

    /// Each file's footer.
    private FileState[] footers;
    /// The next file to list, and whether every file is listed (or planning failed).
    private int nextList = 1;
    private boolean listed;
    /// The last file the read has reached, the next file whose footer is to be requested, the
    /// last file whose footer has been started at once, and the first file not dropped.
    private int reachedFile = -1;
    private int nextFooter = 1;
    private int startedThrough;
    private int doneBefore;
    /// The furthest unit a column has entered, or [#hasLiveWorkItem] has reached.
    private int furthestUnit;
    /// The unit a question asked before the columns start probes, `-1` while none is asked, what
    /// the question is, and its answer once decided.
    private int probe = -1;
    private Probe probeMode;
    private Boolean probeAnswer;
    /// Footers waiting for another read's load of them.
    private int pendingFooters;
    /// The failure that ends planning, sticky at unit `failedFrom`, and the work item it is
    /// reported against, `null` for a file's.
    private Throwable planFailure;
    private int failedFrom = Integer.MAX_VALUE;
    private WorkItem failedWorkItem;

    /// The data requests of each work item not every column has passed: a work item's are forgotten
    /// once every column has passed it ([#forget]).
    private final Map<Integer, List<Request>> byUnit = new HashMap<>();
    /// The requests and footers that start ahead of need, in position order: by file, then by work
    /// item, then by offset, a file's footer before its data.
    private final PriorityQueue<Positioned> queued = new PriorityQueue<>();
    /// The bytes of the data requests inserted and not started.
    private long queuedBytes;
    private long speculativeBytes;
    private boolean waitingForRoom;
    /// Requests demanded since the last poll: a walk or planning stopped on them.
    private final List<Request> demands = new ArrayList<>();

    private Column[] columns;
    /// The columns whose placement has not ended at the row cap, each of which holds the units
    /// listed from the one it is in on.
    private int placingColumns;
    private int batchCapacity;
    /// The cap: the rows a page may start past the read's first unpublished row.
    private long capRows;
    /// Pages placed and waiting for their decode, in row order across columns.
    private final TreeMap<Key, Ready> ready = new TreeMap<>();
    /// Every open column's first unpublished row, as a multiset: its least is the read's.
    private final TreeMap<Long, Integer> unpublished = new TreeMap<>();
    /// The columns whose walk may be able to go on, refilled by what unblocks one: a request
    /// arriving, a decode ending, a unit planned, the read-ahead moving.
    private final ArrayDeque<Column> runnable = new ArrayDeque<>();
    /// The columns whose walk waits for the read-ahead to move.
    private final List<Column> capBlocked = new ArrayList<>();
    /// The first unit a column has not passed.
    private int heldFrom;
    private long readySeq;
    private int decodesInFlight;
    private int maxDecodes;
    /// Whether the page at the head of [#ready] waits for a task of the read to end, or for room in
    /// R, before its task is admitted ([WorkingSet#admit]).
    private boolean waitingForTask;
    /// Re-runs the poll once a task of the read ended or R released room after a page was refused.
    private final Runnable taskEndedWake = () -> update(new TaskEnded());

    /// Jobs emitted and not yet applied.
    private int inFlight;
    /// Whether an update applied in place asks for another poll.
    private boolean repoll;
    /// The bytes the read released from R under the lock, returned once it lets go of it.
    private long toRelease;
    private boolean closed;
    private Throwable failure;
    /// An `Error` a footer read failed with that no listing has raised yet, which the read's close
    /// raises.
    private Error untakenError;
    /// The requests read ahead whose read failed with an `Error`: the read raises it where it
    /// reaches the request, and its close raises the first nothing took.
    private final List<Request> erred = new ArrayList<>();
    /// The `Error`s of [#erred], which a walk's job tells from an `Error` of its own outside the
    /// lock.
    private final Set<Throwable> requestErrors = ConcurrentHashMap.newKeySet();
    private List<Runnable> out;

    private record Key(long row, int column, long seq) implements Comparable<Key> {
        @Override
        public int compareTo(Key o) {
            int c = Long.compare(row, o.row);
            if (c == 0) {
                c = Integer.compare(column, o.column);
            }
            return c != 0 ? c : Long.compare(seq, o.seq);
        }
    }

    private record Ready(int column, DecodedColumn.Page page) {
    }

    /// A question a reader asks before the read's columns start, which the read's jobs answer
    /// while the caller waits.
    private enum Probe {
        /// Whether a work item of at least one row survives its Bloom filters and dictionaries:
        /// probes them in order up to the first kept.
        LIVE,
        /// Whether every work item kept takes a page mask: probes the page index and the mask of
        /// every work item.
        MASKS,
        /// Whether the first work item is listed, or the read has none: lists files up to it.
        LISTED
    }

    /// A request's or footer's position in the read's order.
    private abstract static class Positioned implements Comparable<Positioned> {
        final int file;
        final int unit;
        final long position;

        Positioned(int file, int unit, long position) {
            this.file = file;
            this.unit = unit;
            this.position = position;
        }

        @Override
        public int compareTo(Positioned o) {
            int c = Integer.compare(file, o.file);
            if (c == 0) {
                c = Integer.compare(unit, o.unit);
            }
            return c != 0 ? c : Long.compare(position, o.position);
        }
    }

    /// One file's footer, positioned before the file's data.
    private static final class FileState extends Positioned {
        /// Whether the footer waits in [#queued].
        boolean queued;
        /// Whether the footer's claim was made.
        boolean started;
        boolean charged;
        boolean retried;
        /// Whether another read's load of it is waited for.
        boolean pending;
        /// Whether the footer is loaded or failed, so that the file can be listed.
        boolean loaded;
        /// Whether its read failed twice ahead of demand: the claim was given up, and the read
        /// claims the footer again once it lists the file.
        boolean failedAhead;

        FileState(int file) {
            super(file, -1, 0);
        }

        @Override
        public String toString() {
            return "footer " + file + "[queued=" + queued + " started=" + started + " pending=" + pending
                    + " loaded=" + loaded + "]";
        }
    }

    /// One decoded column: its column side and its walk over the current unit.
    private static final class Column {
        final DecodedColumn side;
        final int index;
        /// The column's first unpublished row as [#unpublished] holds it, `-1` while it holds none.
        long rowKey = -1;
        /// Whether the column is queued in [#runnable].
        boolean candidate;
        int unit;
        PageWalk walk;
        boolean walking;
        /// The section the walk stopped on, or another need it waits for.
        Need waitingOn;
        boolean needMet = true;
        boolean ended;
        /// Whether the column's placement has ended at the row cap.
        boolean placed;
        /// Whether the column has passed the unit it is in and every unit after it, as its
        /// placement ended at the row cap or the column ended.
        boolean passedAll;
        /// The failure its walk met, applied once every page taken before it is decoded.
        Throwable walkFailure;
        /// Whether the walk's failure ends the column behind its open cell, sealed, rather than
        /// abandoning it: the failure was met between two batches.
        boolean sealAtFailure;
        /// Whether the column has placed a page of the unit it is in.
        boolean unitTaken;

        Column(DecodedColumn side, int index) {
            this.side = side;
            this.index = index;
        }
    }

    public RowGroupIterator(FileMetadataCache files, HardwoodContextImpl context,
                            Consumer<RowGroupIterator> closeListener) {
        context.requireOffPool();
        this.files = files;
        this.context = context;
        this.closeListener = closeListener;
        this.executor = ReadExecutor.forContext(context);
        this.tasks = executor.tasks();
        this.readAhead = context.readAheadBuffer();
        this.waitForLock = executor.decodeThreads() <= 1;
    }

    /// F for a read whose largest request is `maxSpanBytes`: twice that unless overridden through
    /// [#FETCH_AHEAD_BYTES_PROPERTY]. A value that is not a non-negative byte count is rejected
    /// rather than replaced by the default.
    private static long defaultFetchAheadBytes(int maxSpanBytes) {
        return InternalProperty.longValue(FETCH_AHEAD_BYTES_PROPERTY, 2L * maxSpanBytes, 0,
                "a non-negative byte count");
    }

    /// Sets up the read's planning and lists its first file's work items.
    ///
    /// @param filter the read's resolved filter, `null` for none
    /// @param metadataFiltering whether the filter takes metadata-derived shortcuts
    /// @param maxRows the read's row budget, `0` for none; under a filter it bounds no listing
    /// @param physicalSkip the leading rows whose row groups the listing drops, `0` for none
    public void initialize(FileSchema schema, List<RowGroup> rowGroups, ReadProjection projection,
                           ResolvedPredicate filter, boolean metadataFiltering, long maxRows, long physicalSkip)
            throws IOException {
        this.referenceSchema = schema;
        this.filtered = filter != null;
        this.fetchAheadBytes = maxRows > 0 ? 0 : defaultFetchAheadBytes(maxSpan);
        this.columnCount = projection.decoded().getProjectedColumnCount();
        WorkingSet workingSet = new WorkingSet(columnCount, readAhead);
        this.planning = new ReadPlanning(files, this, context, schema, rowGroups, projection, filter,
                metadataFiltering, maxRows, physicalSkip, maxSpan, indexWindowBytes, workingSet);
        List<InputFile> inputFiles = files.inputFiles();
        fileCount = inputFiles.size();
        allInMemory = inputFiles.stream().allMatch(HeldBytes::inMemory);
        filesAhead = ReadPlanning.filesAhead(!allInMemory && maxRows <= 0, fileCount);
        ReadPlanning.FileListing first = planning.listFile(0, 0);
        lock.lock();
        try {
            footers = new FileState[fileCount];
            for (int i = 0; i < fileCount; i++) {
                footers[i] = new FileState(i);
            }
            footers[0].started = true;
            footers[0].loaded = true;
            appendUnits(0, planning.append(first));
            planning.reportFiles(0);
            listed = fileCount == 1 || !planning.budgetLeft();
        }
        finally {
            unlock();
        }
        this.workingSet = workingSet;
    }

    /// Appends `count` units of file `file`, with every column whose placement goes on still to
    /// pass each.
    private void appendUnits(int file, int count) {
        int holders = columns == null ? columnCount : placingColumns;
        for (int i = 0; i < count; i++) {
            planned.add(false);
            unitHolders.add(holders);
            unitFiles.add(file);
        }
        unitCount += count;
    }

    /// Whether the read has a row group of at least one row left once it reaches them. Without a
    /// filter: its first file has one, or else the files after it are listed, on the read's jobs
    /// while the caller waits, until one has. With one: the work items are listed and their Bloom
    /// filters and dictionaries probed in order, on the read's jobs while the caller waits, until
    /// one of at least one row is kept, and each drop is reported as the probe reaches it; the
    /// decisions are kept for the read's planning. Asked once, before the columns start.
    ///
    /// @throws IOException the failure that ended planning at or before the first work item kept,
    ///         as planning raised it
    public boolean hasLiveWorkItem() throws IOException {
        if (!filtered) {
            lock.lock();
            try {
                for (int i = 0; i < unitCount; i++) {
                    if (hasRows(i)) {
                        return true;
                    }
                }
            }
            finally {
                unlock();
            }
        }
        return ask(Probe.LIVE);
    }

    private boolean hasRows(int unit) {
        return planning.workItem(unit).rowGroup().numRows() > 0;
    }

    /// The leading rows of the first work item the reader discards: the residue of the physical
    /// skip within it, `0` without one. Lists files, on the read's jobs while the caller waits, as
    /// far as the first work item.
    ///
    /// @throws IOException the failure that ended planning before the first work item
    public long firstRowGroupSkip() throws IOException {
        boolean listedFirst;
        lock.lock();
        try {
            listedFirst = unitCount > 0 || listed;
        }
        finally {
            unlock();
        }
        if (!listedFirst && !ask(Probe.LISTED)) {
            return 0;
        }
        return planning.firstRowGroupSkip();
    }

    /// Whether every work item kept takes a page mask, so that a tail read can mask the leading rows
    /// of its first work item. Takes every work item through its page index and mask probe, on the
    /// read's jobs while the caller waits; the read's planning reuses the probes.
    ///
    /// @throws IllegalStateException if the read has more than one file
    /// @throws IOException the failure that ended planning at a work item
    public boolean canFastSkipAllRowGroups() throws IOException {
        if (fileCount != 1) {
            throw new IllegalStateException(
                    "canFastSkipAllRowGroups requires a single-file iterator, got " + fileCount + " files");
        }
        return ask(Probe.MASKS);
    }

    /// Sets the leading rows of the first work item its plans mask out, before the columns start.
    ///
    /// @throws IllegalStateException if a work item's batches are planned already
    public void setTailSkip(long tailSkip) {
        if (tailSkip < 0) {
            throw new IllegalArgumentException("tailSkip must be non-negative, got " + tailSkip);
        }
        planning.setTailSkip(tailSkip);
    }

    /// Asks `mode` of the read before its columns start: the read's jobs probe the work items in
    /// order while the caller waits off the pool. Answers `false` when the read closed meanwhile.
    ///
    /// @throws IOException the failure that ended planning at or before where the probe reached, as
    ///         planning raised it
    private boolean ask(Probe mode) throws IOException {
        lock.lock();
        try {
            probe = 0;
            probeMode = mode;
            probeAnswer = null;
        }
        finally {
            unlock();
        }
        update(new ProbeAsked());
        tasks.await(this::probeDecided, executor instanceof ReadQueue queue ? queue : null);
        Throwable failed;
        WorkItem failedAt;
        lock.lock();
        try {
            probe = -1;
            if (probeAnswer != null) {
                return probeAnswer;
            }
            failed = failure != null ? failure : planFailure;
            failedAt = failure != null ? null : failedWorkItem;
        }
        finally {
            unlock();
        }
        if (failed == null) {
            // closed while it probed
            return false;
        }
        throw probeFailure(failed, failedAt);
    }

    private boolean probeDecided() {
        lock.lock();
        try {
            return probeAnswer != null || failure != null || closed || probe >= failedFrom;
        }
        finally {
            unlock();
        }
    }

    /// The failure the probe met, as planning raised it; a runtime failure of a work item's stage
    /// placed at the work item.
    private static IOException probeFailure(Throwable failed, WorkItem failedAt) throws IOException {
        if (failed instanceof RuntimeException e && failedAt != null) {
            throw ExceptionContext.addReadContext(failedAt.inputFile().name(), failedAt.rowGroupIndex(), null, e);
        }
        if (failed instanceof IOException e) {
            throw e;
        }
        if (failed instanceof RuntimeException e) {
            throw e;
        }
        if (failed instanceof Error e) {
            throw e;
        }
        throw new IOException(failed);
    }

    public String referenceFileName() {
        return files.inputFiles().getFirst().name();
    }

    /// Starts the read's decoded columns, publishing into detaching exchanges without a row cap or
    /// filter fragments, as a column read does, and returns each, in decoded order.
    ///
    /// @param layers the layers of each decoded column decoded through levels, `null` for a flat
    ///        one, in decoded order
    /// @param indexMode what the batches of a column decoded through levels carry for their reader
    public List<DecodedColumn> start(ProjectedSchema decoded, List<NestedLevelComputer.Layers> layers,
                                    NestedIndexMode indexMode, boolean fixedListFastPathEnabled, int batchSize,
                                    long budgetRows) {
        return start(decoded, layers, indexMode, fixedListFastPathEnabled, batchSize, budgetRows, 0, null, null);
    }

    /// Starts the read's decoded columns and returns each, in decoded order.
    ///
    /// @param layers the layers of each decoded column decoded through levels, `null` for a flat
    ///        one, in decoded order
    /// @param indexMode what the batches of a column decoded through levels carry for their reader
    /// @param maxRows the row cap the read's batches end at, `0` for none; under a filter it counts
    ///        matching rows, and holds only while statistics prove every row reached matches
    /// @param exchanges the exchange each decoded column publishes into, in decoded order: a flat
    ///        column's of [BatchExchange.Batch], one decoded through levels of [NestedBatch];
    ///        `null` for detaching exchanges
    /// @param filters the filter fragment of each flat decoded column, `null` for a column without
    ///        one; `null` for none
    public List<DecodedColumn> start(ProjectedSchema decoded, List<NestedLevelComputer.Layers> layers,
                                    NestedIndexMode indexMode, boolean fixedListFastPathEnabled, int batchSize,
                                    long budgetRows, long maxRows, List<BatchExchange<?>> exchanges,
                                    ColumnBatchMatcher[] filters) {
        planning.setBatchPlan(new BatchPlan(batchSize,
                BatchPlan.endsBatchesAtRowGroupBoundaries(decoded.getProjectedColumns()), filtered, maxRows));
        List<DecodedColumn> sides = new ArrayList<>();
        lock.lock();
        try {
            this.batchCapacity = batchSize;
            this.capRows = CAP_BUDGETS * budgetRows;
            this.maxDecodes = Math.max(1, 2 * executor.decodeThreads());
            columns = new Column[decoded.getProjectedColumnCount()];
            placingColumns = columns.length;
            for (int i = 0; i < columns.length; i++) {
                ColumnSchema column = referenceSchema.getColumn(decoded.toOriginalIndex(i));
                NestedLevelComputer.Layers columnLayers = layers.get(i);
                BatchExchange<?> exchange;
                DecodedColumn side;
                if (columnLayers == null) {
                    BatchExchange<BatchExchange.Batch> flat = exchanges == null
                            ? BatchExchange.detachingFlat(column.name(), batchSize)
                            : cast(exchanges.get(i));
                    ColumnBatchMatcher filter = filters != null && i < filters.length ? filters[i] : null;
                    side = new FlatColumn(column, flat, batchSize, context.decompressorFactory(),
                            workingSet.column(i, column), planning.footersSupplied(), filter);
                    exchange = flat;
                }
                else {
                    // A nested batch's arrays are allocated for it when its cell is filled
                    BatchExchange<NestedBatch> nested = exchanges == null
                            ? BatchExchange.detachingNested(column.name(), batchSize)
                            : cast(exchanges.get(i));
                    side = new NestedColumn(column, nested, batchSize, context.decompressorFactory(),
                            workingSet.column(i, column), planning.footersSupplied(), columnLayers, indexMode,
                            fixedListFastPathEnabled);
                    exchange = nested;
                }
                if (executor instanceof ReadQueue queue) {
                    exchange.setQueue(queue);
                }
                Column c = new Column(side, i);
                columns[i] = c;
                exchange.setRoomListener(() -> update(new RoomMade(c)));
                sides.add(side);
            }
            for (Column c : columns) {
                refresh(c);
                markRunnable(c);
            }
        }
        finally {
            unlock();
        }
        update(new ColumnsStarted());
        return sides;
    }

    @SuppressWarnings("unchecked")
    private static <B> BatchExchange<B> cast(BatchExchange<?> exchange) {
        return (BatchExchange<B>) exchange;
    }

    // ==================== Driver ====================

    /// What the read takes in under the lock ([#take]): a job's result, or a callback from outside
    /// the read. Each record names what happened and carries the data its handler needs.
    private sealed interface Fact {
    }

    /// A job ended with `result`; its count in [ReadTasks] ends once the result is applied.
    private record JobEnded(Fact result) implements Fact {
    }

    /// A job failed with `failure`, or the context refused it.
    private record JobFailed(Throwable failure) implements Fact {
    }

    /// A stage job took unit `unit` through a planning stage, or stopped on `need`.
    private record PlanStepped(int unit, Need need) implements Fact {
    }

    /// A stage job took unit `unit` through the stage a question probes, or stopped on `need`.
    private record Probed(int unit, Need need) implements Fact {
    }

    /// A stage job of unit `unit` failed.
    private record StageFailed(int unit, Throwable failure) implements Fact {
    }

    /// File `file` was listed.
    private record FileListed(int file, ReadPlanning.FileListing listing) implements Fact {
    }

    /// The listing of the file whose units start at `from` failed.
    private record ListingFailed(int from, Throwable failure) implements Fact {
    }

    /// A footer's bytes were read.
    private record FooterRead(FileState footer) implements Fact {
    }

    /// A footer's read failed.
    private record FooterReadFailed(FileState footer, Exception failure) implements Fact {
    }

    /// A footer's read failed with an `Error`.
    private record FooterReadErred(FileState footer, Error error) implements Fact {
    }

    /// A footer was parsed, `error` the `Error` its parse failed with, `null` for none.
    private record FooterParsed(FileState footer, Error error) implements Fact {
    }

    /// Another read's load of a footer this read waited for completed.
    private record PendingFooterDone(FileState footer) implements Fact {
    }

    /// A walk advance of column `column` over unit `unit` took `pages`, and ended, stopped on
    /// `pending`, or ran out of room.
    private record Walked(int column, int unit, WorkItem workItem, BatchPlan.RowGroupBatches batches,
                          List<PageInfo> pages, List<Integer> ordinals, Need pending, boolean ended) implements Fact {
    }

    /// A walk advance of column `column` over unit `unit` took `pages` and then failed.
    private record WalkFailed(int column, int unit, WorkItem workItem, BatchPlan.RowGroupBatches batches,
                              List<PageInfo> pages, List<Integer> ordinals, Throwable failure) implements Fact {
    }

    /// A need a column's walk stopped on, other than one of the read's requests, was met.
    private record NeedMet(Column column) implements Fact {
    }

    /// A page of `column` was decoded.
    private record Decoded(Column column, Object decoded) implements Fact {
    }

    /// The decode of `page` of `column` failed.
    private record DecodeFailed(Column column, DecodedColumn.Page page, Exception failure) implements Fact {
    }

    /// The cells a placement of `column` sealed were finished.
    private record CellsFinished(Column column, List<Object> completed) implements Fact {
    }

    /// Finishing the cells a placement of `column` sealed failed.
    private record ColumnFailed(Column column, Exception failure) implements Fact {
    }

    /// A request's bytes arrived.
    private record RequestRead(Request request, ByteBuffer bytes) implements Fact {
    }

    /// A request's read failed.
    private record RequestFailed(Request request, Exception failure) implements Fact {
    }

    /// A request's read failed with an `Error`.
    private record RequestErred(Request request, Error error) implements Fact {
    }

    /// The consumer made room in `column`'s exchange.
    private record RoomMade(Column column) implements Fact {
    }

    /// R released room after a charge of the read did not fit.
    private record ReadAheadReleased() implements Fact {
    }

    /// A task of the read ended, or R released room, after a page's task was refused.
    private record TaskEnded() implements Fact {
    }

    /// A need a stage job lacked, other than one of the read's requests, was met.
    private record PlanNeedMet() implements Fact {
    }

    /// A reader asked a question of the read before its columns start.
    private record ProbeAsked() implements Fact {
    }

    /// The read's columns started.
    private record ColumnsStarted() implements Fact {
    }

    /// A job's work outside the lock, returning its result.
    private interface Work {
        Fact run(DecodeBuffers buffers) throws Exception;
    }

    /// Applies `fact`, polls, and runs what the poll emitted once the lock is released. A fact
    /// that a callback brings about on the thread applying another, such as a need met at once, is
    /// applied in place, and the poll runs again after it.
    ///
    /// On a read of several decode threads no thread waits for the lock to apply a fact: where
    /// another thread holds it, the fact is queued, and that thread applies it, with every other
    /// fact queued meanwhile, before it lets go of the lock. A job that ends while another thread
    /// polls goes on to its next job at once, and the polls of a read that many threads feed take
    /// the results in batches.
    private void update(Fact fact) {
        if (lock.isHeldByCurrentThread() && out != null) {
            apply(fact);
            repoll = true;
            return;
        }
        submit(fact);
    }

    /// Applies a job's result as [#update] does, and ends the job's count in [ReadTasks] once the
    /// result is applied and what its poll emitted is dispatched: a thread that finds the read idle
    /// finds the job's effects published.
    private void ended(Fact result) {
        submit(new JobEnded(result));
    }

    /// Applies `fact` at once where the lock is free, or once it is on a read that waits for it
    /// ([#waitForLock]), else queues it for the thread that holds the lock; a thread that holds it
    /// already applies it as it lets go of it ([#unlock]).
    private void submit(Fact fact) {
        if (!lock.isHeldByCurrentThread() && (waitForLock ? lockNow() : lock.tryLock())) {
            applyHeld(fact);
        }
        else {
            pending.add(fact);
        }
        drain();
    }

    /// Takes the lock, waiting for it.
    private boolean lockNow() {
        lock.lock();
        return true;
    }

    /// Applies the queued facts while the lock is free. A fact queued while another thread
    /// holds the lock is applied by that thread, which looks at the queue again once it has let go
    /// of the lock. Returns at once where the calling thread holds the lock.
    private void drain() {
        while (!lock.isHeldByCurrentThread() && !pending.isEmpty() && lock.tryLock()) {
            applyHeld(null);
        }
    }

    /// Applies `first`, where not `null`, and the queued facts under the lock the calling thread
    /// has just taken, and polls, until no fact is queued; then lets go of the lock, runs what the
    /// facts and the polls emitted, and ends the count of each job whose result it applied.
    private void applyHeld(Fact first) {
        List<Runnable> emitted = List.of();
        int endings = first instanceof JobEnded ? 1 : 0;
        try {
            // what the facts emit is run with what the poll emits
            out = new ArrayList<>();
            if (first != null) {
                apply(first);
            }
            // results queued while the poll ran are applied before the lock is let go of: their
            // jobs' threads have moved on, and would otherwise wait for the next acquisition
            do {
                Fact fact;
                while ((fact = pending.poll()) != null) {
                    if (fact instanceof JobEnded) {
                        endings++;
                    }
                    apply(fact);
                }
                do {
                    repoll = false;
                    emitted = pollFailing();
                }
                while (repoll);
            }
            while (!pending.isEmpty());
            checkStall(emitted);
        }
        finally {
            out = null;
            releaseLock();
            try {
                for (Runnable r : emitted) {
                    r.run();
                }
            }
            finally {
                for (int i = 0; i < endings; i++) {
                    tasks.ended();
                }
            }
        }
    }

    /// Polls, and fails the read where the poll throws, so that a rule of the engine that throws
    /// ends the read with that failure instead of leaving the consumer waiting. What the poll emitted
    /// before it threw is returned: those jobs are counted and must run.
    private List<Runnable> pollFailing() {
        try {
            return poll();
        }
        catch (RuntimeException | Error e) {
            fail(e);
            return out;
        }
    }

    /// Applies `fact`; a failure, an `Error` included, fails the read, so that the jobs the facts
    /// and polls emitted still run and the read's consumer does not wait for ever.
    private void apply(Fact fact) {
        try {
            take(fact);
        }
        catch (Exception | Error e) {
            fail(e);
        }
    }

    /// Takes in `fact`: the one place a job's result or a callback from outside the read changes
    /// the read's state, through the handler of what happened.
    private void take(Fact fact) throws Exception {
        switch (fact) {
            case JobEnded f -> {
                inFlight--;
                take(f.result());
            }
            case JobFailed f -> fail(f.failure());
            case PlanStepped f -> planStepped(f.unit(), f.need());
            case Probed f -> probed(f.unit(), f.need());
            case StageFailed f -> planFailed(f.unit(), f.failure(), planning.workItem(f.unit()));
            case FileListed f -> fileListed(f.file(), f.listing());
            case ListingFailed f -> planFailed(f.from(), f.failure(), null);
            case FooterRead f -> parseFooter(f.footer());
            case FooterReadFailed f -> footerReadFailed(f.footer(), f.failure());
            case FooterReadErred f -> footerReadErred(f.footer(), f.error());
            case FooterParsed f -> footerParsed(f.footer(), f.error());
            case PendingFooterDone f -> footerPendingDone(f.footer());
            case Walked f -> walked(f.column(), f.unit(), f.workItem(), f.batches(), f.pages(), f.ordinals(),
                    f.pending(), f.ended());
            case WalkFailed f -> walked(f.column(), f.unit(), f.workItem(), f.batches(), f.pages(), f.ordinals(),
                    f.failure());
            case NeedMet f -> needMet(f.column());
            case Decoded f -> decoded(f.column(), f.decoded());
            case DecodeFailed f -> decodeFailed(f.column(), f.page(), f.failure());
            case CellsFinished f -> cellsFinished(f.column(), f.completed());
            case ColumnFailed f -> columnFailed(f.column(), f.failure());
            case RequestRead f -> arrived(f.request(), f.bytes());
            case RequestFailed f -> requestFailed(f.request(), f.failure());
            case RequestErred f -> requestErred(f.request(), f.error());
            case RoomMade f -> roomMade(f.column());
            case ReadAheadReleased f -> waitingForRoom = false;
            case TaskEnded f -> waitingForTask = false;
            // nothing to take in: the poll that follows looks at the read again
            case PlanNeedMet f -> {
            }
            case ProbeAsked f -> {
            }
            case ColumnsStarted f -> {
            }
        }
    }

    /// Lets go of the lock, and then applies the facts other threads queued while it was held
    /// ([#drain]).
    private void unlock() {
        releaseLock();
        if (!pending.isEmpty()) {
            drain();
        }
    }

    /// Lets go of the lock, and returns to R what the read released under it once the lock is let
    /// go of entirely: R runs its waiters, other reads' among them, on the releasing thread, which
    /// must hold no lock they could wait for.
    private void releaseLock() {
        long released = 0;
        if (lock.getHoldCount() == 1) {
            released = toRelease;
            toRelease = 0;
        }
        lock.unlock();
        if (released > 0) {
            readAhead.release(released);
        }
    }

    /// Emits `work` as a job: counted until its result is applied.
    private void emit(Kind kind, Work work) {
        inFlight++;
        tasks.started();
        Runnable job = () -> run(work, null);
        Runnable dispatch = switch (kind) {
            case STEP -> () -> executor.executeStep(job);
            case PLANNING -> () -> executor.executePlanning(job);
            case DECODE -> () -> executor.executePage(buffers -> run(work, buffers));
            case IO -> () -> context.ioExecutor().execute(job);
        };
        out.add(() -> {
            try {
                dispatch.run();
            }
            catch (RejectedExecutionException e) {
                // a closed context refuses the job: it ends here, and fails the read
                ended(new JobFailed(HardwoodContextImpl.contextClosed(e)));
            }
        });
    }

    private enum Kind {
        STEP, PLANNING, DECODE, IO
    }

    private void run(Work work, DecodeBuffers buffers) {
        Fact result;
        try {
            result = work.run(buffers);
        }
        catch (Throwable t) {
            result = new JobFailed(t);
        }
        ended(result);
    }

    private void fail(Throwable t) {
        if (failure == null) {
            failure = t;
            if (columns != null) {
                for (Column c : columns) {
                    c.ended = true;
                    if (!c.side.ended()) {
                        c.side.fail(t);
                    }
                }
            }
        }
    }

    /// Fails the read where nothing is in flight, nothing was emitted, nothing outside the read is
    /// waited for, and a column has not ended: nothing could make progress any more, which only a
    /// rule of the read that misses a case brings about.
    private void checkStall(List<Runnable> emitted) {
        if (closed || failure != null || inFlight > 0 || !emitted.isEmpty() || waitingForRoom
                || waitingForTask || pendingFooters > 0 || planNeed != null && !(planNeed instanceof Request)) {
            return;
        }
        if (columns == null) {
            if (probe >= 0 && probeAnswer == null && probe < failedFrom) {
                fail(new IllegalStateException("Read stalled: " + describe()));
            }
            return;
        }
        boolean open = false;
        for (Column c : columns) {
            if (c.side.waitingForRoom() || c.waitingOn != null && !(c.waitingOn instanceof Request) && !c.needMet) {
                return;
            }
            open |= !c.side.ended();
        }
        if (open) {
            fail(new IllegalStateException("Read stalled: " + describe()));
        }
    }

    /// Where the read's planning stands against its columns, for tests.
    record PlanningPosition(int slowestUnit, int furthestUnit, int plannedThrough) {
    }

    /// Waits until no job of the read is in flight, running the read's queue meanwhile on a context
    /// of zero threads, for tests.
    void awaitQuiescent() {
        tasks.await(this::quiescent, executor instanceof ReadQueue queue ? queue : null);
    }

    PlanningPosition planningPosition() {
        lock.lock();
        try {
            return new PlanningPosition(consumerUnit(), furthestUnit, nextPlan - 1);
        }
        finally {
            unlock();
        }
    }

    private String describe() {
        StringBuilder b = new StringBuilder();
        b.append("nextPlan=").append(nextPlan).append('/').append(unitCount).append(" planRunning=").append(planRunning)
                .append(" planNeed=").append(planNeed).append(" ready=").append(ready.size())
                .append(ready.isEmpty() ? "" : " readyHead=" + ready.firstKey())
                .append(" decodesInFlight=").append(decodesInFlight).append(" waitingForTask=").append(waitingForTask)
                .append(" cap=")
                .append(firstUnpublishedRow() + capRows).append(" requests=").append(requestsHeld())
                .append(" queued=").append(queued.size()).append(" queuedHead=").append(queued.peek())
                .append(" nextList=").append(nextList).append('/').append(fileCount).append(" listed=").append(listed)
                .append(" reachedFile=").append(reachedFile).append(" failedFrom=").append(failedFrom)
                .append(" probe=").append(probe).append(' ').append(probeMode).append(" probeAnswer=")
                .append(probeAnswer).append(" placingColumns=").append(placingColumns);
        if (nextList < fileCount) {
            b.append(" nextListFooter=").append(footers[nextList]);
        }
        for (int i = 0; columns != null && i < columns.length; i++) {
            Column c = columns[i];
            b.append("\n  column ").append(i).append(": unit=").append(c.unit).append(" walk=").append(c.walk != null)
                    .append(" walking=").append(c.walking).append(" waitingOn=").append(c.waitingOn)
                    .append(" ended=").append(c.ended).append(" placed=").append(c.placed).append(' ')
                    .append(c.side.describe());
        }
        return b.toString();
    }

    // ==================== What happens next ====================

    private List<Runnable> poll() {
        if (!closed && failure == null) {
            if (columns != null) {
                // once every column's placement has ended at the row cap, the read reaches no
                // further file and plans nothing more
                if (placingColumns > 0) {
                    filesReached();
                    plan();
                }
                walk();
                startRequests();
                decode();
            }
            else if (probe >= 0 && probeAnswer == null) {
                filesReached();
                probe();
                startRequests();
            }
        }
        tasks.signal();
        return out;
    }

    /// The first unit a column has not passed.
    private int consumerUnit() {
        while (heldFrom < unitCount && unitHolders.get(heldFrom) == 0) {
            heldFrom++;
        }
        return heldFrom;
    }

    private long firstUnpublishedRow() {
        return unpublished.isEmpty() ? Long.MAX_VALUE : unpublished.firstKey();
    }

    /// Takes up the change of `c`'s first unpublished row, and the walks the read-ahead's move lets
    /// go on.
    private void refresh(Column c) {
        long row = c.side.ended() ? -1 : c.side.firstUnpublishedRow();
        if (row == c.rowKey) {
            return;
        }
        long before = firstUnpublishedRow();
        if (c.rowKey >= 0) {
            unpublished.computeIfPresent(c.rowKey, (k, n) -> n == 1 ? null : n - 1);
        }
        if (row >= 0) {
            unpublished.merge(row, 1, Integer::sum);
        }
        c.rowKey = row;
        if (firstUnpublishedRow() > before) {
            for (Column blocked : capBlocked) {
                markRunnable(blocked);
            }
            capBlocked.clear();
        }
    }

    private void markRunnable(Column c) {
        if (!c.candidate) {
            c.candidate = true;
            runnable.addLast(c);
        }
    }

    /// Whether planning has gone as far as the read plans ahead of its columns: past the unit the
    /// slowest column is in and the next, and, for a read of files not all in memory, as far as the
    /// requests planned and not started reach F. A read of files in memory plans one unit past the
    /// slowest column's on a pool, and on a context of zero threads no unit past the furthest a
    /// column has entered, so that its walks plan each unit as they reach it.
    private boolean plannedFarEnough() {
        if (!allInMemory) {
            return nextPlan > consumerUnit() + 1 && queuedBytes >= fetchAheadBytes;
        }
        return nextPlan > (executor instanceof ReadQueue ? furthestUnit : consumerUnit() + 1);
    }

    /// Plans the units the columns are in and the next, and further as [#plannedFarEnough] allows,
    /// one stage job at a time.
    private void plan() {
        if (planRunning || nextPlan >= failedFrom) {
            return;
        }
        if (nextPlan >= unitCount) {
            list();
            return;
        }
        if (planNeed != null) {
            if (planNeed instanceof Request r && !r.done()) {
                return;
            }
            planNeed = null;
        }
        if (plannedFarEnough()) {
            return;
        }
        planRunning = true;
        int index = nextPlan;
        emit(Kind.PLANNING, buffers -> {
            Need need;
            try {
                need = planning.plan(index);
            }
            catch (Exception | Error t) {
                return new StageFailed(index, t);
            }
            return new PlanStepped(index, need);
        });
    }

    /// Takes the unit a question probes through the stage it asks of, one stage job at a time,
    /// listing files as far as it, and leaves a planning failure at or before it for the caller.
    /// [Probe#LIVE] passes the units of no rows, answers `true` at the first other unit kept, which
    /// without a filter it takes through no stage, and `false` past the last unit;
    /// [Probe#MASKS] answers past the last unit; [Probe#LISTED] answers once a unit is listed, or
    /// `false` once every file is and none is.
    private void probe() {
        if (planRunning || probe >= failedFrom) {
            return;
        }
        if (probeMode == Probe.LIVE) {
            // a work item of no rows is not live; without a filter, any other is
            while (probe < unitCount && probe < failedFrom && !hasRows(probe)) {
                probe++;
                furthestUnit = Math.max(furthestUnit, probe);
            }
            if (probe >= failedFrom) {
                return;
            }
            if (probe < unitCount && !filtered) {
                probeAnswer = true;
                return;
            }
        }
        if (probeMode == Probe.LISTED || probe >= unitCount) {
            if (probe < unitCount) {
                probeAnswer = true;
            }
            else if (listed) {
                probeAnswer = probeMode == Probe.MASKS ? masksOpen() : false;
            }
            else {
                list();
            }
            return;
        }
        if (planNeed != null) {
            if (planNeed instanceof Request r && !r.done()) {
                return;
            }
            planNeed = null;
        }
        planRunning = true;
        int index = probe;
        boolean masks = probeMode == Probe.MASKS;
        emit(Kind.PLANNING, buffers -> {
            Need need;
            try {
                need = masks ? planning.masked(index) : planning.arrival(index);
            }
            catch (Exception | Error t) {
                return new StageFailed(index, t);
            }
            return new Probed(index, need);
        });
    }

    private void probed(int index, Need need) {
        planRunning = false;
        if (need == null && probeMode == Probe.MASKS) {
            probe++;
        }
        else if (need == null) {
            planning.report(index);
            if (planning.dropped(index)) {
                probe++;
                furthestUnit = Math.max(furthestUnit, probe);
            }
            else {
                probeAnswer = true;
            }
        }
        else {
            awaitPlanNeed(need);
        }
    }

    /// Whether every unit kept takes a page mask, once [Probe#MASKS] probed every unit.
    private boolean masksOpen() {
        for (int i = 0; i < unitCount; i++) {
            if (planning.maskClosed(i)) {
                return false;
            }
        }
        return true;
    }

    /// Waits for `need`, which a stage job lacked, before the next stage job: a request is demanded
    /// and the poll looks at it again once it arrives; another need re-runs the poll once met.
    private void awaitPlanNeed(Need need) {
        planNeed = need;
        if (need instanceof Request r) {
            demands.add(r);
        }
        else {
            need.whenMet(planNeedMet);
        }
    }

    /// Lists the next file once its footer is loaded, within the bound planning has
    /// ([#plannedFarEnough]).
    private void list() {
        if (!listed && footers[nextList].failedAhead) {
            FileState f = footers[nextList];
            f.failedAhead = false;
            startFooter(f, false);
        }
        if (listed || !footers[nextList].loaded || plannedFarEnough()) {
            return;
        }
        planRunning = true;
        int file = nextList;
        int before = unitCount;
        emit(Kind.PLANNING, buffers -> {
            ReadPlanning.FileListing listing;
            try {
                listing = planning.listFile(file, before);
            }
            catch (Exception | Error t) {
                return new ListingFailed(before, t);
            }
            return new FileListed(file, listing);
        });
    }

    private void fileListed(int file, ReadPlanning.FileListing listing) {
        planRunning = false;
        int before = unitCount;
        appendUnits(file, planning.append(listing));
        // a file the read reached before it was listed reports its pruning as it is listed
        planning.reportFiles(reachedFile);
        nextList++;
        listed = nextList >= fileCount || !planning.budgetLeft();
        wakeColumnsFrom(before);
    }

    /// Ends planning at unit `from` with `t`, a failure of the file listed there (`workItem`
    /// `null`) or of work item `from`'s stage: no file is listed and no unit planned from there on,
    /// and each column ends with the failure once it reaches the unit.
    private void planFailed(int from, Throwable t, WorkItem workItem) {
        planRunning = false;
        if (planFailure == null) {
            planFailure = t;
            failedFrom = from;
            failedWorkItem = workItem;
            listed = true;
        }
        wakeColumnsFrom(from);
    }

    /// Queues the columns that wait for a unit from `from` on to be listed or planned.
    private void wakeColumnsFrom(int from) {
        if (columns == null) {
            return;
        }
        for (Column c : columns) {
            if (c.walk == null && c.unit >= from) {
                markRunnable(c);
            }
        }
    }

    // ==================== Files ====================

    /// Takes up the move of the file the read has reached, the file of the furthest unit a column
    /// has entered or the next unit, or the file listed next where that is not listed: starts the
    /// footers up to the next file's at once and queues those of the files ahead it brings within
    /// reach. Past the end of a listing that has ended, at the last file or at the row budget, the
    /// read has reached every file, and starts only the footers it queued ahead. Drops the files
    /// before that of the last unit every column has passed.
    private void filesReached() {
        int consumer = consumerUnit();
        // before the columns start, the read has reached the unit the probe has
        int unit = columns == null ? furthestUnit : Math.max(furthestUnit, consumer + 1);
        int file = unit < unitCount ? unitFiles.get(unit) : listed ? fileCount : nextList;
        if (file == fileCount && unitCount > 0) {
            // the file of the last unit is reached before every file is
            reach(unitFiles.get(unitCount - 1));
        }
        reach(file);
        int done = consumer > 0 ? unitFiles.get(consumer - 1) : 0;
        dropFilesBefore(done);
    }

    private void reach(int file) {
        if (file > reachedFile) {
            reachedFile = file;
            planning.reportFiles(file);
            if (planFailure == null) {
                requestFooters();
            }
        }
    }

    private void requestFooters() {
        int now = Math.min(reachedFile + 1, fileCount - 1);
        for (int i = startedThrough + 1; i <= now; i++) {
            if (!footers[i].started && (reachedFile < fileCount || footers[i].queued)) {
                startFooter(footers[i], false);
            }
        }
        startedThrough = Math.max(startedThrough, now);
        int last = Math.min(reachedFile + filesAhead, fileCount - 1);
        for (int i = Math.max(nextFooter, now + 1); i <= last; i++) {
            FileState f = footers[i];
            if (!f.started) {
                f.queued = true;
                queued.add(f);
            }
        }
        nextFooter = Math.max(nextFooter, last + 1);
    }

    /// Claims `f`'s footer and starts what its load needs: nothing where it is loaded, a wait for
    /// another read's load, or its read and parse. A charge to R the footer took is kept only for
    /// a read of its own.
    private void startFooter(FileState f, boolean charged) {
        f.started = true;
        f.queued = false;
        switch (planning.claimFooter(f.file)) {
            case LOADED -> {
                f.loaded = true;
                returnCharge(charged);
            }
            case PENDING -> {
                f.pending = true;
                pendingFooters++;
                returnCharge(charged);
                out.add(() -> planning.whenFooterDone(f.file, () -> update(new PendingFooterDone(f))));
            }
            case CLAIMED -> {
                if (planning.footerReadsInPlace(f.file)) {
                    returnCharge(charged);
                    parseFooter(f);
                }
                else {
                    f.charged = charged;
                    readFooter(f);
                }
            }
        }
    }

    private void returnCharge(boolean charged) {
        if (charged) {
            toRelease += footerCharge;
        }
    }

    private void footerPendingDone(FileState f) {
        f.pending = false;
        pendingFooters--;
        if (!closed) {
            startFooter(f, false);
        }
    }

    private void readFooter(FileState f) {
        emit(Kind.IO, buffers -> {
            try {
                planning.readFooter(f.file);
            }
            catch (IOException | RuntimeException e) {
                return new FooterReadFailed(f, e);
            }
            catch (Error e) {
                return new FooterReadErred(f, e);
            }
            return new FooterRead(f);
        });
    }

    /// Parses `f`'s footer on a job, also once the read closed, so that the reader's other reads
    /// find its load completed.
    private void parseFooter(FileState f) {
        emit(Kind.PLANNING, buffers -> new FooterParsed(f, planning.parseFooter(f.file)));
    }

    private void footerParsed(FileState f, Error erred) {
        f.loaded = true;
        if (erred != null && untakenError == null) {
            // raised by the read's close unless the listing of the file raises it
            untakenError = erred;
        }
    }

    /// A footer read that failed the first time is started again at once, uncharged. A second
    /// failure of the footer the listing needs next is final, and fails the read where its listing
    /// reaches the file; a second failure ahead of demand gives up the claim, so that the file's
    /// entry fails no other read of the reader, and the footer is claimed again once the listing
    /// needs it. A read closed meanwhile leaves the load to its close, which gives it up. The
    /// load is completed or given up once the lock is let go of, since that runs what other reads
    /// wait for.
    private void footerReadFailed(FileState f, Exception e) {
        uncharge(f);
        LOG.get().log(System.Logger.Level.DEBUG, "Read ahead of demand failed: {0}", f, e);
        if (closed) {
            return;
        }
        if (!f.retried) {
            f.retried = true;
            readFooter(f);
        }
        else if (listed || f.file != nextList) {
            out.add(planning.abandonFooter(f.file));
            f.started = false;
            f.retried = false;
            f.failedAhead = true;
        }
        else {
            out.add(planning.failFooter(f.file, e));
            f.loaded = true;
        }
    }

    /// A footer read that failed with an `Error` is not read again: the error fails the read where
    /// its listing reaches the file, and is raised by the read's close if the read never does.
    private void footerReadErred(FileState f, Error e) {
        uncharge(f);
        out.add(planning.failFooter(f.file, e));
        f.loaded = true;
        untakenError = e;
    }

    private void uncharge(FileState f) {
        if (f.charged) {
            f.charged = false;
            toRelease += footerCharge;
        }
    }

    /// Drops the tails and footers of the files before `file`, which the read is done with, and
    /// returns their charges.
    private void dropFilesBefore(int file) {
        for (; doneBefore < file; doneBefore++) {
            uncharge(footers[doneBefore]);
            planning.dropFile(doneBefore);
        }
    }

    private void planStepped(int index, Need need) {
        planRunning = false;
        if (need == null) {
            planned.set(index, true);
            nextPlan++;
            if (unitHolders.get(index) == 0) {
                // every column's placement ended at the row cap while it was planned
                passed(index);
            }
            for (Column c : columns) {
                if (c.unit == index) {
                    markRunnable(c);
                }
            }
        }
        else {
            awaitPlanNeed(need);
        }
    }

    /// Runs each column's walk while its bytes are present, it has room for pages, and its next
    /// row is within the read-ahead; moves a column whose walk ended to its next unit.
    private void walk() {
        long limit = firstUnpublishedRow() + capRows + batchCapacity;
        for (int k = runnable.size(); k > 0; k--) {
            Column c = runnable.pollFirst();
            c.candidate = false;
            int i = c.index;
            if (c.ended || c.placed || c.walking || c.walkFailure != null) {
                continue;
            }
            if (c.walk == null) {
                if (c.unit >= failedFrom) {
                    if (planFailure == untakenError) {
                        // raised where the column reaches the failure, not by the close
                        untakenError = null;
                    }
                    end(c, placedPlanFailure(c));
                    continue;
                }
                if (c.unit >= unitCount) {
                    if (listed) {
                        end(c, null);
                    }
                    continue;
                }
                if (!planned.get(c.unit)) {
                    continue;
                }
                FetchPlan plan = planning.plans(c.unit)[i];
                planning.enterUnit(c.unit);
                planning.enter(i, c.unit);
                c.walk = plan.walk();
                c.unitTaken = false;
            }
            if (c.waitingOn instanceof Request r) {
                if (!r.done()) {
                    continue;
                }
            }
            else if (c.waitingOn != null && !c.needMet) {
                continue;
            }
            if (c.side.pagesHeld() >= MAX_PAGES_HELD) {
                continue;
            }
            if (c.side.nextRow() >= limit) {
                capBlocked.add(c);
                continue;
            }
            c.walking = true;
            c.waitingOn = null;
            int column = i;
            PageWalk walk = c.walk;
            int unit = c.unit;
            int room = MAX_PAGES_HELD - c.side.pagesHeld();
            WorkItem workItem = planning.workItem(unit);
            BatchPlan.RowGroupBatches batches = planning.batches(unit);
            emit(Kind.STEP, buffers -> {
                List<PageInfo> pages = new ArrayList<>();
                List<Integer> ordinals = new ArrayList<>();
                PageInfo page;
                try {
                    while (pages.size() < room && (page = walk.advance()) != null) {
                        pages.add(page);
                        ordinals.add(walk.currentPage());
                    }
                }
                catch (IOException | RuntimeException e) {
                    // the pages taken before the failure are placed ahead of it
                    Exception placed = c.side.placed(e, workItem.inputFile().name(), workItem.rowGroupIndex(),
                            walk.currentPage());
                    return new WalkFailed(column, unit, workItem, batches, pages, ordinals, placed);
                }
                catch (Error e) {
                    if (!requestErrors.contains(e)) {
                        throw e;
                    }
                    // a request's read failed with it: the column fails where it reached the bytes
                    return new WalkFailed(column, unit, workItem, batches, pages, ordinals, e);
                }
                Need pending = pages.size() < room ? walk.pending() : null;
                boolean ended = pages.size() < room && pending == null;
                return new Walked(column, unit, workItem, batches, pages, ordinals, pending, ended);
            });
        }
    }

    /// Ends `c` behind the cells it has placed: with the read's end, or with `failure`.
    private void end(Column c, Throwable failure) {
        c.ended = true;
        columnEnded(c);
        // the end seals the open cell, whose completion may then fall to a job
        c.side.end(failure);
        sealed(c);
        c.side.publish();
        refresh(c);
    }

    /// The failure that ended planning as column `c` reports it: placed at the work item it was
    /// met at, or, for a file's failure, as it stands.
    private Throwable placedPlanFailure(Column c) {
        if (!(planFailure instanceof Exception e)) {
            return planFailure;
        }
        return failedWorkItem == null
                ? c.side.placed(e, null, ExceptionContext.UNKNOWN_ROW_GROUP, ExceptionContext.UNKNOWN_PAGE)
                : c.side.placed(e, failedWorkItem.inputFile().name(), failedWorkItem.rowGroupIndex(),
                        ExceptionContext.UNKNOWN_PAGE);
    }

    /// Places the pages a walk took before it failed, and ends the column with `failure` once they
    /// are decoded ([#endAtWalkFailure]). A failure met before the walk took a page of a work item
    /// whose batches close the batch left open is met between two batches: the open cell, which
    /// every column ends at the same row, is sealed and published ahead of the failure.
    private void walked(int column, int unit, WorkItem workItem, BatchPlan.RowGroupBatches batches,
                        List<PageInfo> pages, List<Integer> ordinals, Throwable failure) {
        Column c = columns[column];
        c.walking = false;
        if (c.placed || c.ended) {
            stopWalk(c);
            return;
        }
        c.sealAtFailure = pages.isEmpty() && !c.unitTaken && batches.closesOpenBatch();
        if (!place(c, workItem, batches, pages, ordinals)) {
            return;
        }
        if (c.side.placementEnded()) {
            placementEnded(c);
            return;
        }
        walkFailed(c, failure);
    }

    private void walkFailed(Column c, Throwable failure) {
        c.walk = null;
        c.walkFailure = failure;
        endAtWalkFailure(c);
    }

    /// Ends `c` with its walk's failure once every page it took before the failure is decoded, so
    /// that a decode's own failure among them is raised first, as it would be had the walk not run
    /// ahead of them.
    private void endAtWalkFailure(Column c) {
        if (c.walkFailure == null || c.ended || c.side.pagesHeld() > 0) {
            return;
        }
        c.ended = true;
        columnEnded(c);
        if (c.sealAtFailure) {
            c.side.end(c.walkFailure);
        }
        else {
            c.side.walkFailed(c.walkFailure);
        }
        sealed(c);
        c.side.publish();
        refresh(c);
    }

    /// Places the pages a walk took; returns `false` where a placement failed, which ends the
    /// column as a walk's failure does, behind the segments placed before it.
    private boolean place(Column c, WorkItem workItem, BatchPlan.RowGroupBatches batches,
                          List<PageInfo> pages, List<Integer> ordinals) {
        c.unitTaken |= !pages.isEmpty();
        try {
            for (int p = 0; p < pages.size(); p++) {
                ready(c, c.side.place(pages.get(p), batches, workItem.inputFile().name(), workItem.rowGroupIndex(),
                        ordinals.get(p)));
            }
        }
        catch (DecodedColumn.PlaceFailure e) {
            ready(c, e.page());
            sealed(c);
            refresh(c);
            walkFailed(c, e.failure());
            return false;
        }
        sealed(c);
        refresh(c);
        return true;
    }

    private void ready(Column c, DecodedColumn.Page page) {
        if (page != null) {
            ready.put(new Key(page.firstRow(), c.index, readySeq++), new Ready(c.index, page));
        }
    }

    private void walked(int column, int unit, WorkItem workItem, BatchPlan.RowGroupBatches batches,
                        List<PageInfo> pages, List<Integer> ordinals, Need pending, boolean ended) {
        Column c = columns[column];
        c.walking = false;
        if (c.placed || c.ended) {
            // the placement ended at the row cap, or the column failed, while the walk ran
            stopWalk(c);
            return;
        }
        if (!place(c, workItem, batches, pages, ordinals)) {
            return;
        }
        if (c.side.placementEnded()) {
            placementEnded(c);
        }
        else if (ended) {
            markRunnable(c);
            c.walk = null;
            c.unit++;
            furthestUnit = Math.max(furthestUnit, c.unit);
            int left = unitHolders.get(unit) - 1;
            unitHolders.set(unit, left);
            if (left == 0) {
                planning.release(unit);
                forget(unit);
            }
        }
        else if (pending != null) {
            c.waitingOn = pending;
            if (pending instanceof Request r) {
                demands.add(r);
                if (r.done()) {
                    // arrived while the walk ran
                    markRunnable(c);
                }
                else {
                    r.waiting.add(c);
                }
            }
            else {
                c.needMet = false;
                pending.whenMet(() -> update(new NeedMet(c)));
            }
        }
        else {
            markRunnable(c);
        }
    }

    private void needMet(Column c) {
        c.needMet = true;
        markRunnable(c);
    }

    /// Takes up the end of `c`'s placement at the row cap: its walk stops, passing the sections it
    /// holds, once no advance of it runs, and the column passes the unit it is in and every unit
    /// listed after it. The read plans nothing more once no column places.
    private void placementEnded(Column c) {
        c.placed = true;
        if (!c.walking) {
            stopWalk(c);
        }
        passAll(c);
    }

    /// Takes up that `c` ended, with the read's end or a failure: as at the row cap, its walk
    /// stops once no advance of it runs, and it passes the unit it is in and every unit after it,
    /// so the other columns' planning and the release of what the units hold do not wait for it.
    private void columnEnded(Column c) {
        if (!c.walking) {
            stopWalk(c);
        }
        passAll(c);
    }

    /// Lets `c` pass the unit it is in and every unit listed after it, once.
    private void passAll(Column c) {
        if (c.passedAll) {
            return;
        }
        c.passedAll = true;
        placingColumns--;
        for (int unit = c.unit; unit < unitCount; unit++) {
            int left = unitHolders.get(unit) - 1;
            unitHolders.set(unit, left);
            if (left == 0) {
                passed(unit);
            }
        }
    }

    private void stopWalk(Column c) {
        if (c.walk != null) {
            c.walk.stop();
            c.walk = null;
        }
    }

    /// Takes up that every column has passed `unit` at the row cap: a planned unit's planning reads
    /// and index windows are released and its requests forgotten; a unit planned later is at the
    /// end of its planning ([#planStepped]).
    private void passed(int unit) {
        if (planned.get(unit)) {
            planning.release(unit);
            forget(unit);
        }
    }

    /// Forgets the requests of `unit`, which every column has passed: each is released whatever
    /// holders it has left, its bytes dropped and its charges returned.
    private void forget(int unit) {
        List<Request> unitRequests = byUnit.remove(unit);
        if (unitRequests != null) {
            for (Request r : unitRequests) {
                r.drop();
            }
        }
    }

    /// The requests the read holds, for tests: those of the units not every column has passed.
    int requestsHeld() {
        lock.lock();
        try {
            int held = 0;
            for (List<Request> unit : byUnit.values()) {
                held += unit.size();
            }
            return held;
        }
        finally {
            unlock();
        }
    }

    /// The bytes of the requests the read holds that have arrived, for tests.
    long dataBytesHeld() {
        lock.lock();
        try {
            long held = 0;
            for (List<Request> unit : byUnit.values()) {
                for (Request r : unit) {
                    if (r.bytes != null && !HeldBytes.inMemory(r.inputFile)) {
                        held += r.span.length();
                    }
                }
            }
            return held;
        }
        finally {
            unlock();
        }
    }

    /// Finishes, on a job, the cells the column's placement sealed and left to finish: a flat
    /// column's sealed after their last fill, which the job completes; the cells of a column decoded
    /// through levels, which the job fills.
    private void sealed(Column c) {
        List<Object> cells = c.side.takeSealed();
        if (!cells.isEmpty()) {
            emit(Kind.DECODE, buffers -> {
                List<Object> completed;
                try {
                    completed = c.side.finishSealed(cells, buffers);
                }
                catch (Exception failure) {
                    return new ColumnFailed(c, failure);
                }
                return new CellsFinished(c, completed);
            });
        }
    }

    private void cellsFinished(Column c, List<Object> completed) {
        c.side.completed(completed);
        refresh(c);
    }

    /// Takes up the room the consumer made in `c`'s exchange.
    private void roomMade(Column c) {
        c.side.publish();
        refresh(c);
    }

    /// Decodes ready pages in row order within the read-ahead cap, at most `maxDecodes` at once: a
    /// page past the cap still decodes when it starts at most one batch past its column's first
    /// unpublished row, which the page a nested column's open cell waits for to be sealed does. A
    /// page whose task the working set does not admit ([WorkingSet#admit]) holds the pages after it
    /// until a task of the read ends or R releases room.
    private void decode() {
        long cap = firstUnpublishedRow() + capRows;
        Iterator<Map.Entry<Key, Ready>> it = ready.entrySet().iterator();
        while (it.hasNext() && decodesInFlight < maxDecodes) {
            Map.Entry<Key, Ready> entry = it.next();
            Ready r = entry.getValue();
            Column c = columns[r.column()];
            if (c.side.ended() || !c.side.decodes(r.page())) {
                // the column failed, or its placement ended where it places no page decoded
                // after: its pages are not decoded
                it.remove();
                continue;
            }
            long row = entry.getKey().row();
            if (row >= cap && row > c.side.firstUnpublishedRow() + batchCapacity) {
                break;
            }
            long admission = admit(r.page());
            if (admission == REFUSED) {
                break;
            }
            it.remove();
            decodesInFlight++;
            emit(Kind.DECODE, buffers -> {
                Object decoded;
                try {
                    decoded = c.side.decode(r.page(), buffers);
                }
                catch (Exception failure) {
                    return new DecodeFailed(c, r.page(), failure);
                }
                finally {
                    // outside the lock: the release runs what waits for it, this read's poll among them
                    workingSet.taskEnded(admission);
                }
                return new Decoded(c, decoded);
            });
        }
    }

    private void decodeFailed(Column c, DecodedColumn.Page page, Exception failure) {
        decodesInFlight--;
        // a page the column's placement ended before holds no row of the read
        if (c.side.decodes(page)) {
            columnFailed(c, failure);
        }
    }

    /// Ends `c` at once with the failure of a decode or a fill, placed at its page: only the
    /// column whose page failed ends, so a reader that takes its columns in
    /// order raises the failure of the first column it takes, whichever failed first.
    private void columnFailed(Column c, Exception failure) {
        c.ended = true;
        columnEnded(c);
        if (!c.side.ended()) {
            c.side.fail(failure);
        }
        refresh(c);
    }

    /// The admission of a page's task, [#REFUSED] while it waits for a task of the read to end or
    /// for room in R, which then re-runs the poll.
    private long admit(DecodedColumn.Page page) {
        long bytes = page.taskBytes();
        if (bytes == 0) {
            return 0;
        }
        WorkingSet.Admission admitted = workingSet.admit(bytes);
        if (admitted.admitted()) {
            return admitted.admission();
        }
        if (!waitingForTask) {
            waitingForTask = true;
            admitted.refused().whenMet(taskEndedWake);
        }
        return REFUSED;
    }

    private void decoded(Column c, Object decoded) throws Exception {
        decodesInFlight--;
        c.side.decoded(decoded);
        sealed(c);
        refresh(c);
        if (!c.placed && c.side.placementEnded()) {
            placementEnded(c);
        }
        endAtWalkFailure(c);
        markRunnable(c);
    }

    /// Starts requests: demanded ones at once and uncharged, the next unit's first uncharged, and
    /// every other in position order while F and R have room.
    private void startRequests() {
        for (Request r : demands) {
            if (!r.started) {
                start(r);
            }
        }
        demands.clear();
        List<Request> next = byUnit.get(consumerUnit() + 1);
        if (next != null && !next.isEmpty() && !next.getFirst().started) {
            start(next.getFirst());
        }
        Positioned head;
        while ((head = queued.peek()) != null) {
            if (head instanceof Request r) {
                if (r.started || r.released) {
                    queued.poll();
                    continue;
                }
                if (speculativeBytes + r.span.length() > fetchAheadBytes || !charge(r.span.length())) {
                    return;
                }
                queued.poll();
                speculativeBytes += r.span.length();
                r.charged = true;
                start(r);
            }
            else {
                FileState f = (FileState) head;
                if (!f.queued || planFailure != null) {
                    queued.poll();
                    continue;
                }
                if (!charge(footerCharge)) {
                    return;
                }
                queued.poll();
                startFooter(f, true);
            }
        }
    }

    /// Takes `bytes` from R, or registers for its next release where they do not fit.
    private boolean charge(long bytes) {
        if (readAhead.tryCharge(bytes)) {
            return true;
        }
        if (!waitingForRoom) {
            waitingForRoom = true;
            readAhead.whenReleased(readAhead.releases(), () -> update(new ReadAheadReleased()));
        }
        return false;
    }

    private void start(Request r) {
        r.started = true;
        if (r.counted) {
            r.counted = false;
            queuedBytes -= r.span.length();
        }
        emit(Kind.IO, buffers -> {
            ByteBuffer bytes;
            try (FetchReason.Scope ignored = FetchReason.set(r.span.reason())) {
                bytes = ReadRanges.read(r.inputFile, r.span.start(), r.span.length());
            }
            catch (Exception e) {
                // a checked exception the file threw undeclared fails the request as one it declared
                return new RequestFailed(r, e);
            }
            catch (Error e) {
                return new RequestErred(r, e);
            }
            return new RequestRead(r, bytes);
        });
    }

    private void arrived(Request r, ByteBuffer bytes) {
        if (!r.released) {
            r.bytes = bytes;
        }
        wakeWaiting(r);
    }

    private void wakeWaiting(Request r) {
        for (Column c : r.waiting) {
            markRunnable(c);
        }
        r.waiting.clear();
    }

    /// A request that failed the first time is started again at once, as a demanded request, by
    /// the next poll; a second failure is final, and fails what reads it.
    private void requestFailed(Request r, Exception e) {
        uncharge(r);
        if (!awaited(r)) {
            // the walk or the stage that reaches the request reports its failure
            LOG.get().log(System.Logger.Level.DEBUG, "Read ahead of demand failed: {0}", r, e);
        }
        if (r.released) {
            return;
        }
        if (!r.retried) {
            r.retried = true;
            r.started = false;
            demands.add(r);
        }
        else {
            r.failure = new IOException(ExceptionContext.filePrefix(r.inputFile.name())
                    + "Failed to fetch data at offset " + r.span.start() + " (length " + r.span.length() + ")", e);
            wakeWaiting(r);
        }
    }

    /// A request whose read failed with an `Error` is not read again: the walk or the planning stage
    /// that reaches it, or that waits for it and runs again at once, raises the `Error` there, a
    /// walk as its column's failure and a stage as a planning failure of its work item; the read's
    /// close raises it if none does.
    private void requestErred(Request r, Error e) {
        uncharge(r);
        // known as a request's before a walk can find it on the request
        requestErrors.add(e);
        r.error = e;
        erred.add(r);
        wakeWaiting(r);
    }

    /// Whether a walk, or the planning stage the read runs next, stopped on `r`.
    private boolean awaited(Request r) {
        return !r.waiting.isEmpty() || planNeed == r;
    }

    private void uncharge(Request r) {
        if (r.charged) {
            r.charged = false;
            speculativeBytes -= r.span.length();
            toRelease += r.span.length();
        }
    }

    // ==================== Sections ====================

    @Override
    public List<? extends Section> sections(int workItemIndex) {
        lock.lock();
        try {
            return byUnit.get(workItemIndex);
        }
        finally {
            unlock();
        }
    }

    @Override
    public List<? extends Section> insert(int fileIndex, int workItemIndex, InputFile inputFile, List<Span> spans) {
        lock.lock();
        try {
            List<Request> existing = byUnit.get(workItemIndex);
            if (existing != null) {
                return existing;
            }
            List<Request> inserted = new ArrayList<>(spans.size());
            for (Span span : spans) {
                Request r = new Request(inputFile, span, fileIndex, workItemIndex);
                inserted.add(r);
                if (!r.started) {
                    queued.add(r);
                    r.counted = true;
                    queuedBytes += span.length();
                }
            }
            byUnit.put(workItemIndex, inserted);
            return inserted;
        }
        finally {
            unlock();
        }
    }

    /// A section a walk of work item `workItemIndex` reached past its plan's sections, the rest of a
    /// column chunk a row limit left out: not queued, since the walk stops on it and demands it.
    @Override
    public Section insertDemanded(int workItemIndex, InputFile inputFile, long start, int length, String reason) {
        lock.lock();
        try {
            return new Request(inputFile, new Span(start, length, reason, 1), -1, workItemIndex);
        }
        finally {
            unlock();
        }
    }

    @Override
    public Section planned(InputFile inputFile, long start, int length, String reason, int holders) {
        lock.lock();
        try {
            Request r = new Request(inputFile, new Span(start, length, reason, holders), -1, -1);
            // planning reads start when planning demands them, charged to nothing
            r.planning = true;
            return r;
        }
        finally {
            unlock();
        }
    }

    @Override
    public Section heldInPlace(InputFile inputFile, Span span) {
        Request r = new Request(inputFile, span, -1, -1);
        return r.started ? r : null;
    }

    /// One range of a file the read reads: a request, or bytes it holds sliced in place.
    private final class Request extends Positioned implements Section {

        final InputFile inputFile;
        final Span span;
        volatile ByteBuffer bytes;
        volatile IOException failure;
        /// The `Error` the request's read failed with, which is not read again, and whether a walk
        /// or a planning stage raised it.
        volatile Error error;
        volatile boolean errorTaken;
        boolean started;
        /// A read planning makes, started only when demanded.
        boolean planning;
        boolean retried;
        boolean charged;
        /// Whether the request's bytes count in [#queuedBytes].
        boolean counted;
        /// Whether every holder has passed the request.
        boolean released;
        int holders;
        /// The columns whose walk stopped on the request.
        final List<Column> waiting = new ArrayList<>(1);

        Request(InputFile inputFile, Span span, int file, int unit) {
            super(file, unit, span.start());
            this.inputFile = inputFile;
            this.span = span;
            this.holders = span.holders();
            ByteBuffer held = HeldBytes.inMemory(inputFile) ? sliceInPlace(inputFile, span)
                    : HeldBytes.slice(inputFile, span.start(), span.length());
            if (held != null) {
                bytes = held;
                started = true;
            }
        }

        private static ByteBuffer sliceInPlace(InputFile inputFile, Span span) {
            try {
                return ReadRanges.read(inputFile, span.start(), span.length());
            }
            catch (IOException e) {
                return null;
            }
        }

        @Override
        public long offset() {
            return span.start();
        }

        @Override
        public long end() {
            return span.start() + span.length();
        }

        @Override
        public ByteBuffer poll() throws IOException {
            ByteBuffer b = bytes;
            if (b == null && failure != null) {
                throw failure;
            }
            Error e = error;
            if (b == null && e != null) {
                errorTaken = true;
                throw e;
            }
            return b;
        }

        /// Whether the request's read has completed: its bytes arrived, or it failed for good.
        boolean done() {
            return bytes != null || failure != null || error != null;
        }

        @Override
        public ByteBuffer slice(ByteBuffer polled, long fileOffset, int length) {
            if (fileOffset < offset() || length < 0 || fileOffset + length > end()) {
                throw new IllegalArgumentException("Range [" + fileOffset + ", " + (fileOffset + length)
                        + ") lies outside the section [" + offset() + ", " + end() + ") (" + span.reason() + ")");
            }
            return polled.slice(Math.toIntExact(fileOffset - offset()), length);
        }

        @Override
        public void release() {
            lock.lock();
            try {
                if (--holders == 0) {
                    bytes = null;
                    released();
                }
            }
            finally {
                unlock();
            }
        }

        @Override
        public void demand() {
            lock.lock();
            try {
                demands.add(this);
            }
            finally {
                unlock();
            }
        }

        @Override
        public void discard() {
            lock.lock();
            try {
                drop();
            }
            finally {
                unlock();
            }
        }

        /// Releases the request whatever its holders, under the lock.
        void drop() {
            holders = 0;
            bytes = null;
            released();
        }

        /// Returns the request's charge, and takes it out of the bytes not started.
        private void released() {
            released = true;
            uncharge(this);
            if (counted) {
                counted = false;
                queuedBytes -= span.length();
            }
        }

        @Override
        public String toString() {
            return "Request[" + span.reason() + " at " + span.start() + " (" + span.length() + ") started=" + started
                    + " arrived=" + (bytes != null) + " holders=" + holders + "]";
        }

        @Override
        public void whenMet(Runnable resume) {
            throw new UnsupportedOperationException("The read tracks its requests itself");
        }
    }

    // ==================== Close ====================

    /// Stops the read and waits until no job of it is in flight, then releases what it holds.
    /// Interrupts nothing. Idempotent.
    ///
    /// @throws Error the `Error` a footer read or a request read ahead failed with that the read
    ///         never reached
    @Override
    public void close() {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            if (columns != null) {
                for (Column c : columns) {
                    c.ended = true;
                    c.side.stop();
                }
            }
        }
        finally {
            unlock();
        }
        tasks.await(this::quiescent, executor instanceof ReadQueue queue ? queue : null);
        Error untaken;
        Runnable abandon = null;
        lock.lock();
        try {
            untaken = untakenError;
            untakenError = null;
            for (Request r : erred) {
                if (untaken == null && !r.errorTaken && r.error != failure) {
                    untaken = r.error;
                }
                r.errorTaken = true;
            }
            for (List<Request> unit : byUnit.values()) {
                for (Request r : unit) {
                    uncharge(r);
                }
            }
            if (workingSet != null) {
                workingSet.cancelWait(taskEndedWake);
            }
            if (planning != null) {
                planning.discard();
                abandon = planning.abandonFooters();
            }
            if (footers != null) {
                dropFilesBefore(nextFooter);
            }
        }
        finally {
            unlock();
        }
        if (abandon != null) {
            // what other reads wait for runs outside the lock
            abandon.run();
        }
        closeListener.accept(this);
        if (untaken != null) {
            throw untaken;
        }
    }

    private boolean quiescent() {
        lock.lock();
        try {
            return inFlight == 0;
        }
        finally {
            unlock();
        }
    }
}
