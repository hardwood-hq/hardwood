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
import java.util.TreeMap;
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
import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.decode.PageInfo;
import dev.hardwood.internal.reader.fetch.CoalescingPolicy;
import dev.hardwood.internal.reader.fetch.FetchPlan;
import dev.hardwood.internal.reader.fetch.FetchSequence;
import dev.hardwood.internal.reader.fetch.PageWalk;
import dev.hardwood.internal.reader.fetch.Section;
import dev.hardwood.internal.reader.fetch.Sections;
import dev.hardwood.internal.reader.fetch.Span;
import dev.hardwood.internal.reader.fetch.TailedInputFile;
import dev.hardwood.internal.reader.memory.InternalProperty;
import dev.hardwood.internal.reader.memory.ReadAheadBuffer;
import dev.hardwood.internal.reader.memory.WorkingSet;
import dev.hardwood.internal.reader.pipeline.EngineColumn;
import dev.hardwood.internal.reader.plan.BatchPlan;
import dev.hardwood.internal.reader.plan.EnginePlanning;
import dev.hardwood.internal.reader.plan.FileMetadataCache;
import dev.hardwood.internal.reader.plan.RowGroupIterator.WorkItem;
import dev.hardwood.internal.reader.step.Need;
import dev.hardwood.internal.reader.step.ReadExecutor;
import dev.hardwood.internal.reader.step.ReadQueue;
import dev.hardwood.internal.reader.step.ReadTasks;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.internal.schema.ReadProjection;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// One read with a single decision point: every piece of the read's state is held here, under one
/// lock, and [#poll] is the only place that decides what happens next. The work leaves as jobs
/// (a planning stage, a walk's advance, a page's decode, a request's read), which run outside the
/// lock on the read's [ReadExecutor] or, for a read, on a virtual thread, and whose results are
/// applied under the lock ([#update]), each followed by a poll.
///
/// A read of several files lists them in order, each once its footer is loaded. The footers are
/// jobs of the read like its requests: the next file's footer after the one the read has reached
/// starts at once, those further ahead start in position order behind the earlier files' data,
/// charged to R, at most `filesAhead` files ahead. A file's tail and footer are dropped once every
/// column has passed a work item of a later file. A planning failure, of a footer, a file's listing
/// or a work item's stage, is sticky at its file or work item: each column publishes every batch
/// before it and then ends with it.
///
/// Interim name: the engine takes over [dev.hardwood.internal.reader.plan.RowGroupIterator]'s name
/// once the pipeline it replaces is gone. It reads flat columns without a filter or a row limit
/// ([#supports]).
public final class ReadEngine implements Closeable, Sections {

    /// Selects the engine for the reads it supports: `auto`; or never: `off`, the default.
    public static final String PROPERTY = "hardwood.internal.readEngine";

    private final FileMetadataCache files;
    private final HardwoodContextImpl context;
    private final Consumer<ReadEngine> closeListener;
    private final ReadExecutor executor;
    private final ReadTasks tasks;
    private final ReadAheadBuffer readAhead;
    private final int maxSpan = CoalescingPolicy.maxSpanBytes();
    /// F: the speculative bytes the read holds.
    private final long fetchAheadBytes = FetchSequence.fetchAheadBytes(maxSpan);
    private final long indexWindowBytes = InternalProperty.longValue("hardwood.internal.indexWindowBytes",
            16L * 1024 * 1024, 1, "a positive byte count");

    /// Batch byte budgets a read decodes at most ahead of its first unpublished row (READ_PIPELINE.md,
    /// the cap).
    private static final int CAP_BUDGETS = 8;
    /// The admission of a page task the working set refused.
    private static final long REFUSED = Long.MIN_VALUE;

    private final ReentrantLock lock = new ReentrantLock();
    /// Runs [#update] with nothing to apply: a release of R this read waited for.
    private final Runnable wake = () -> update(e -> {
    });

    private FileSchema referenceSchema;
    private EnginePlanning planning;
    private WorkingSet workingSet;
    private int unitCount;
    private int fileCount;
    /// The files after the one the read has reached whose footers start ahead of it.
    private int filesAhead;
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
    /// The furthest unit a column has entered.
    private int furthestUnit;
    /// Footers waiting for another read's load of them.
    private int pendingFooters;
    /// The failure that ends planning, sticky at unit `failedFrom`, and the work item it is
    /// reported against, `null` for a file's.
    private Throwable planFailure;
    private int failedFrom = Integer.MAX_VALUE;
    private WorkItem failedWorkItem;

    /// Every request, and the requests by work item.
    private final List<Request> requests = new ArrayList<>();
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
    private final Runnable taskEndedWake = () -> update(e -> e.waitingForTask = false);

    /// Jobs emitted and not yet applied.
    private int inFlight;
    /// Whether an update applied in place asks for another poll.
    private boolean repoll;
    /// The bytes the read released from R under the lock, returned once it lets go of it.
    private long toRelease;
    private boolean closed;
    private Throwable failure;
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

    private record Ready(int column, EngineColumn.Page page) {
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
        final EngineColumn side;
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
        /// Pages placed and not decoded.
        int queued;
        boolean ended;

        Column(EngineColumn side, int index) {
            this.side = side;
            this.index = index;
        }
    }

    public ReadEngine(FileMetadataCache files, HardwoodContextImpl context, Consumer<ReadEngine> closeListener) {
        context.requireOffPool();
        this.files = files;
        this.context = context;
        this.closeListener = closeListener;
        this.executor = ReadExecutor.forContext(context);
        this.tasks = executor.tasks();
        this.readAhead = context.readAheadBuffer();
    }

    /// Whether the engine reads a column read over `projected`: flat columns, no filter, the
    /// property on.
    public static boolean supports(ProjectedSchema projected, FileSchema schema, boolean filtered) {
        if (!"auto".equals(System.getProperty(PROPERTY, "off")) || filtered) {
            return false;
        }
        for (int i = 0; i < projected.getProjectedColumnCount(); i++) {
            ColumnSchema column = schema.getColumn(projected.toOriginalIndex(i));
            if (column.maxRepetitionLevel() > 0 || column.fieldPath().elements().size() > 1) {
                return false;
            }
        }
        return true;
    }

    /// Sets up the read's planning and lists its first file's work items.
    public void initialize(FileSchema schema, List<RowGroup> rowGroups, ReadProjection projection) throws IOException {
        this.referenceSchema = schema;
        WorkingSet workingSet = new WorkingSet(projection.decoded().getProjectedColumnCount(), readAhead);
        this.planning = new EnginePlanning(files, this, context, schema, rowGroups, projection, maxSpan,
                indexWindowBytes, workingSet);
        List<InputFile> inputFiles = files.inputFiles();
        fileCount = inputFiles.size();
        filesAhead = EnginePlanning.filesAhead(inputFiles.stream().allMatch(HeldBytes::inMemory), fileCount);
        EnginePlanning.FileListing first = planning.listFile(0, 0);
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
            lock.unlock();
        }
        this.workingSet = workingSet;
    }

    /// Appends `count` units of file `file`, with every column still to pass each once the columns
    /// have started.
    private void appendUnits(int file, int count) {
        for (int i = 0; i < count; i++) {
            planned.add(false);
            unitHolders.add(columns == null ? 0 : columns.length);
            unitFiles.add(file);
        }
        unitCount += count;
    }

    /// Whether the read may have a row group to read: its first file has one, or it reads further
    /// files, which it lists once it runs.
    public boolean hasLiveWorkItem() {
        return unitCount > 0 || fileCount > 1;
    }

    public String referenceFileName() {
        return files.inputFiles().getFirst().name();
    }

    /// Starts the read's decoded columns and returns the exchange of each, in decoded order.
    public List<BatchExchange<BatchExchange.Batch>> start(ProjectedSchema decoded, int batchSize, long budgetRows) {
        planning.setBatchPlan(new BatchPlan(batchSize,
                BatchPlan.endsBatchesAtRowGroupBoundaries(decoded.getProjectedColumns()), false, 0));
        List<BatchExchange<BatchExchange.Batch>> exchanges = new ArrayList<>();
        lock.lock();
        try {
            this.batchCapacity = batchSize;
            this.capRows = CAP_BUDGETS * budgetRows;
            this.maxDecodes = Math.max(1, 2 * executor.decodeThreads());
            columns = new Column[decoded.getProjectedColumnCount()];
            for (int i = 0; i < columns.length; i++) {
                ColumnSchema column = referenceSchema.getColumn(decoded.toOriginalIndex(i));
                BatchExchange<BatchExchange.Batch> exchange = BatchExchange.detachingFlat(column.name(), batchSize);
                if (executor instanceof ReadQueue queue) {
                    exchange.setQueue(queue);
                }
                EngineColumn side = new EngineColumn(column, exchange, batchSize, context.decompressorFactory(),
                        workingSet.column(i, column), planning.footersSupplied());
                Column c = new Column(side, i);
                columns[i] = c;
                exchange.setRoomListener(() -> update(e -> {
                    side.publish();
                    e.refresh(c);
                }));
                exchanges.add(exchange);
            }
            for (int i = 0; i < unitCount; i++) {
                unitHolders.set(i, columns.length);
            }
            for (Column c : columns) {
                refresh(c);
                markRunnable(c);
            }
        }
        finally {
            lock.unlock();
        }
        update(e -> {
        });
        return exchanges;
    }

    // ==================== Driver ====================

    /// A result to apply under the lock.
    private interface Update {
        void apply(ReadEngine engine) throws Exception;
    }

    /// A job's work outside the lock, returning its result.
    private interface Work {
        Update run(DecodeBuffers buffers) throws Exception;
    }

    /// Applies `u`, polls, and runs what the poll emitted once the lock is released. An update
    /// that a callback brings about on the thread applying another, such as a need met at once, is
    /// applied in place, and the poll runs again after it.
    private void update(Update u) {
        if (lock.isHeldByCurrentThread() && out != null) {
            apply(u);
            repoll = true;
            return;
        }
        List<Runnable> emitted;
        lock.lock();
        try {
            // what the update emits is run with what the poll emits
            out = new ArrayList<>();
            apply(u);
            do {
                repoll = false;
                emitted = poll();
            }
            while (repoll);
            checkStall(emitted);
        }
        finally {
            out = null;
            unlock();
        }
        for (Runnable r : emitted) {
            r.run();
        }
    }

    private void apply(Update u) {
        try {
            u.apply(this);
        }
        catch (Exception e) {
            fail(e);
        }
    }

    /// Lets go of the lock, and returns to R what the read released under it once the lock is let
    /// go of entirely: R runs its waiters, other reads' among them, on the releasing thread, which
    /// must hold no lock they could wait for.
    private void unlock() {
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
                try {
                    update(engine -> {
                        engine.inFlight--;
                        engine.fail(HardwoodContextImpl.contextClosed(e));
                    });
                }
                finally {
                    tasks.ended();
                }
            }
        });
    }

    private enum Kind {
        STEP, PLANNING, DECODE, IO
    }

    private void run(Work work, DecodeBuffers buffers) {
        Update result;
        try {
            result = work.run(buffers);
        }
        catch (Throwable t) {
            result = e -> e.fail(t);
        }
        Update applied = result;
        try {
            update(e -> {
                e.inFlight--;
                applied.apply(e);
            });
        }
        finally {
            tasks.ended();
        }
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
    /// rule of the engine that misses a case brings about.
    private void checkStall(List<Runnable> emitted) {
        if (closed || failure != null || columns == null || inFlight > 0 || !emitted.isEmpty() || waitingForRoom
                || waitingForTask || pendingFooters > 0 || planNeed != null && !(planNeed instanceof Request)) {
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
            fail(new IllegalStateException("Read engine stalled: " + describe()));
        }
    }

    private String describe() {
        StringBuilder b = new StringBuilder();
        b.append("nextPlan=").append(nextPlan).append('/').append(unitCount).append(" planRunning=").append(planRunning)
                .append(" planNeed=").append(planNeed).append(" ready=").append(ready.size())
                .append(ready.isEmpty() ? "" : " readyHead=" + ready.firstKey())
                .append(" decodesInFlight=").append(decodesInFlight).append(" waitingForTask=").append(waitingForTask)
                .append(" cap=")
                .append(firstUnpublishedRow() + capRows).append(" requests=").append(requests.size())
                .append(" queued=").append(queued.size()).append(" queuedHead=").append(queued.peek())
                .append(" nextList=").append(nextList).append('/').append(fileCount).append(" listed=").append(listed)
                .append(" reachedFile=").append(reachedFile).append(" failedFrom=").append(failedFrom);
        if (nextList < fileCount) {
            b.append(" nextListFooter=").append(footers[nextList]);
        }
        for (int i = 0; i < columns.length; i++) {
            Column c = columns[i];
            b.append("\n  column ").append(i).append(": unit=").append(c.unit).append(" walk=").append(c.walk != null)
                    .append(" walking=").append(c.walking).append(" waitingOn=").append(c.waitingOn)
                    .append(" queued=").append(c.queued).append(" ended=").append(c.ended).append(' ')
                    .append(c.side.describe());
        }
        return b.toString();
    }

    // ==================== What happens next ====================

    private List<Runnable> poll() {
        if (!closed && failure == null && columns != null) {
            filesReached();
            plan();
            walk();
            startRequests();
            decode();
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

    /// Plans the units the columns are in and the next, and further while the requests planned and
    /// not started stay below F, one stage job at a time.
    private void plan() {
        if (planRunning || nextPlan >= failedFrom) {
            return;
        }
        if (nextPlan >= unitCount) {
            list();
            return;
        }
        if (planNeed != null) {
            if (planNeed instanceof Request r && r.bytes == null && r.failure == null) {
                return;
            }
            planNeed = null;
        }
        if (nextPlan > consumerUnit() + 1 && queuedBytes >= fetchAheadBytes) {
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
                return e -> e.planFailed(index, t, e.planning.workItem(index));
            }
            return e -> e.planStepped(index, need);
        });
    }

    /// Lists the next file once its footer is loaded, within the bound planning has: the units the
    /// columns are in and the next, and further while the requests not started stay below F.
    private void list() {
        if (listed || !footers[nextList].loaded
                || nextPlan > consumerUnit() + 1 && queuedBytes >= fetchAheadBytes) {
            return;
        }
        planRunning = true;
        int file = nextList;
        int before = unitCount;
        emit(Kind.PLANNING, buffers -> {
            EnginePlanning.FileListing listing;
            try {
                listing = planning.listFile(file, before);
            }
            catch (Exception | Error t) {
                return e -> e.planFailed(before, t, null);
            }
            return e -> e.fileListed(file, listing);
        });
    }

    private void fileListed(int file, EnginePlanning.FileListing listing) {
        planRunning = false;
        int before = unitCount;
        appendUnits(file, planning.append(listing));
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
    /// reach. Drops the files before that of the last unit every column has passed.
    private void filesReached() {
        int consumer = consumerUnit();
        int unit = Math.max(furthestUnit, consumer + 1);
        int file = unit < unitCount ? unitFiles.get(unit) : listed ? fileCount - 1 : nextList;
        if (file > reachedFile) {
            reachedFile = file;
            planning.reportFiles(file);
            if (planFailure == null) {
                requestFooters();
            }
        }
        int done = consumer > 0 ? unitFiles.get(consumer - 1) : 0;
        dropFilesBefore(done);
    }

    private void requestFooters() {
        int now = Math.min(reachedFile + 1, fileCount - 1);
        for (int i = startedThrough + 1; i <= now; i++) {
            if (!footers[i].started) {
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
                out.add(() -> planning.whenFooterDone(f.file, () -> update(e -> e.footerPendingDone(f))));
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
                return engine -> engine.footerReadFailed(f, e);
            }
            return engine -> engine.parseFooter(f);
        });
    }

    /// Parses `f`'s footer on a job, also once the read closed, so that the reader's other reads
    /// find its load completed.
    private void parseFooter(FileState f) {
        emit(Kind.PLANNING, buffers -> {
            planning.parseFooter(f.file);
            return engine -> f.loaded = true;
        });
    }

    /// A footer read that failed the first time is started again at once, uncharged; a second
    /// failure is final, and fails the read where its listing reaches the file. A read closed
    /// meanwhile leaves the load to its close, which gives it up.
    private void footerReadFailed(FileState f, Exception e) {
        uncharge(f);
        if (closed) {
            return;
        }
        if (!f.retried) {
            f.retried = true;
            readFooter(f);
        }
        else {
            planning.failFooter(f.file, e);
            f.loaded = true;
        }
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
            for (Column c : columns) {
                if (c.unit == index) {
                    markRunnable(c);
                }
            }
        }
        else if (need instanceof Request r) {
            planNeed = r;
            demands.add(r);
        }
        else {
            planNeed = need;
            need.whenMet(wake);
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
            if (c.ended || c.walking) {
                continue;
            }
            if (c.walk == null) {
                if (c.unit >= failedFrom) {
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
                planning.enter(i, c.unit);
                c.walk = plan.walk();
            }
            if (c.waitingOn instanceof Request r) {
                if (r.bytes == null && r.failure == null) {
                    continue;
                }
            }
            else if (c.waitingOn != null && !c.needMet) {
                continue;
            }
            if (c.queued >= 8) {
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
            int room = 8 - c.queued;
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
                    throw c.side.placed(e, workItem.inputFile().name(), workItem.rowGroupIndex(), walk.currentPage());
                }
                Need pending = pages.size() < room ? walk.pending() : null;
                boolean ended = pages.size() < room && pending == null;
                return e -> e.walked(column, unit, workItem, batches, pages, ordinals, pending, ended);
            });
        }
    }

    /// Ends `c` behind the cells it has placed: with the read's end, or with `failure`.
    private void end(Column c, Throwable failure) {
        c.ended = true;
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

    private void walked(int column, int unit, WorkItem workItem, BatchPlan.RowGroupBatches batches,
                        List<PageInfo> pages, List<Integer> ordinals, Need pending, boolean ended) {
        Column c = columns[column];
        c.walking = false;
        for (int p = 0; p < pages.size(); p++) {
            EngineColumn.Page page = c.side.place(pages.get(p), batches, workItem.inputFile().name(),
                    workItem.rowGroupIndex(), ordinals.get(p));
            if (page != null) {
                c.queued++;
                ready.put(new Key(page.firstRow(), column, readySeq++), new Ready(column, page));
            }
        }
        sealed(c);
        refresh(c);
        if (ended) {
            markRunnable(c);
            c.walk = null;
            c.unit++;
            furthestUnit = Math.max(furthestUnit, c.unit);
            int left = unitHolders.get(unit) - 1;
            unitHolders.set(unit, left);
            if (left == 0) {
                planning.release(unit);
            }
        }
        else if (pending != null) {
            c.waitingOn = pending;
            if (pending instanceof Request r) {
                demands.add(r);
                if (r.bytes != null || r.failure != null) {
                    // arrived while the walk ran
                    markRunnable(c);
                }
                else {
                    r.waiting.add(c);
                }
            }
            else {
                c.needMet = false;
                pending.whenMet(() -> update(e -> {
                    c.needMet = true;
                    e.markRunnable(c);
                }));
            }
        }
        else {
            markRunnable(c);
        }
    }

    /// Completes, on a job, the cells the column's placement sealed after their last fill.
    private void sealed(Column c) {
        List<Object> cells = c.side.takeSealed();
        if (!cells.isEmpty()) {
            emit(Kind.DECODE, buffers -> {
                List<Object> completed = EngineColumn.complete(c.side, cells);
                return e -> {
                    c.side.completed(completed);
                    e.refresh(c);
                };
            });
        }
    }

    /// Decodes ready pages in row order within the read-ahead cap, at most `maxDecodes` at once: a
    /// page past the cap still decodes when it starts within one batch of its column's first
    /// unpublished row. A page whose task the working set does not admit ([WorkingSet#admit])
    /// holds the pages after it until a task of the read ends or R releases room.
    private void decode() {
        long cap = firstUnpublishedRow() + capRows;
        Iterator<Map.Entry<Key, Ready>> it = ready.entrySet().iterator();
        while (it.hasNext() && decodesInFlight < maxDecodes) {
            Map.Entry<Key, Ready> entry = it.next();
            Ready r = entry.getValue();
            Column c = columns[r.column()];
            long row = entry.getKey().row();
            if (row >= cap && row >= c.side.firstUnpublishedRow() + batchCapacity) {
                break;
            }
            long admission = admit(r.page());
            if (admission == REFUSED) {
                break;
            }
            it.remove();
            decodesInFlight++;
            emit(Kind.DECODE, buffers -> {
                List<Object> completed;
                try {
                    completed = c.side.decode(r.page(), buffers);
                }
                finally {
                    // outside the lock: the release runs what waits for it, this read's poll among them
                    workingSet.taskEnded(admission);
                }
                return e -> e.decoded(c, completed);
            });
        }
    }

    /// The admission of a page's task, [#REFUSED] while it waits for a task of the read to end or
    /// for room in R, which then re-runs the poll.
    private long admit(EngineColumn.Page page) {
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

    private void decoded(Column c, List<Object> completed) {
        decodesInFlight--;
        c.queued--;
        c.side.completed(completed);
        sealed(c);
        refresh(c);
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
            readAhead.whenReleased(readAhead.releases(), () -> update(e -> e.waitingForRoom = false));
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
            catch (IOException | RuntimeException e) {
                return engine -> engine.requestFailed(r, e);
            }
            return engine -> engine.arrived(r, bytes);
        });
    }

    private void arrived(Request r, ByteBuffer bytes) {
        r.bytes = bytes;
        wakeWaiting(r);
    }

    private void wakeWaiting(Request r) {
        for (Column c : r.waiting) {
            markRunnable(c);
        }
        r.waiting.clear();
    }

    /// A request that failed the first time is started again once needed; a second failure is
    /// final, and fails what reads it.
    private void requestFailed(Request r, Exception e) {
        uncharge(r);
        if (!r.retried) {
            r.retried = true;
            r.started = false;
            demands.add(r);
        }
        else {
            r.failure = new IOException(r.inputFile.name() + ": failed to fetch data at offset " + r.span.start()
                    + " (length " + r.span.length() + ")", e);
            wakeWaiting(r);
        }
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
            lock.unlock();
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
                    requests.add(r);
                    queued.add(r);
                    r.counted = true;
                    queuedBytes += span.length();
                }
            }
            byUnit.put(workItemIndex, inserted);
            return inserted;
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public Section insertDemanded(int workItemIndex, InputFile inputFile, long start, int length, String reason) {
        throw new UnsupportedOperationException("The read engine reads no row limit");
    }

    @Override
    public Section planned(InputFile inputFile, long start, int length, String reason, int holders) {
        lock.lock();
        try {
            Request r = new Request(inputFile, new Span(start, length, reason, holders), -1, -1);
            // planning reads start when planning demands them, charged to nothing
            r.planning = true;
            if (!r.started) {
                requests.add(r);
            }
            return r;
        }
        finally {
            lock.unlock();
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
            return b;
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
                lock.unlock();
            }
        }

        @Override
        public void discard() {
            lock.lock();
            try {
                holders = 0;
                bytes = null;
                released();
            }
            finally {
                unlock();
            }
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
            throw new UnsupportedOperationException("The read engine tracks its requests itself");
        }
    }

    // ==================== Close ====================

    /// Stops the read and waits until no job of it is in flight, then releases what it holds.
    /// Interrupts nothing. Idempotent.
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
            lock.unlock();
        }
        tasks.await(this::quiescent, executor instanceof ReadQueue queue ? queue : null);
        lock.lock();
        try {
            for (Request r : requests) {
                uncharge(r);
            }
            if (workingSet != null) {
                workingSet.cancelWait(taskEndedWake);
            }
            if (planning != null) {
                planning.discard();
                planning.abandonFooters();
            }
            if (footers != null) {
                dropFilesBefore(nextFooter);
            }
        }
        finally {
            unlock();
        }
        closeListener.accept(this);
    }

    private boolean quiescent() {
        lock.lock();
        try {
            return inFlight == 0;
        }
        finally {
            lock.unlock();
        }
    }
}
