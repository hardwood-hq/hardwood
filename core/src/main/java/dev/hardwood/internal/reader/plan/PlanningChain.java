/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.predicate.BoundsReadability;
import dev.hardwood.internal.predicate.LogContext;
import dev.hardwood.internal.predicate.PageFilterEvaluator;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.fetch.FetchPlan;
import dev.hardwood.internal.reader.fetch.FetchSequence;
import dev.hardwood.internal.reader.fetch.RangeRead;
import dev.hardwood.internal.reader.footer.FileColumnOrdinals;
import dev.hardwood.internal.reader.plan.FileMetadataCache.PreparedFile;
import dev.hardwood.internal.reader.plan.RowGroupIterator.RowGroupPlans;
import dev.hardwood.internal.reader.plan.RowGroupIterator.SharedRowGroupMetadata;
import dev.hardwood.internal.reader.plan.RowGroupIterator.WorkItem;
import dev.hardwood.internal.reader.select.RowRanges;
import dev.hardwood.internal.reader.step.Need;
import dev.hardwood.internal.reader.step.Parsed;
import dev.hardwood.internal.reader.step.ReadExecutor;
import dev.hardwood.internal.reader.step.ReadQueue;
import dev.hardwood.internal.schema.ReadProjection;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.FileSchema;
import jdk.jfr.Event;

/// The planning of one read: the work list, built a file at a time from each footer, and the plans
/// of each work item, built in work-list order once each.
///
/// The chain is one step on the read's executor, run under a claim: at the tail of its queue while
/// it plans ahead of the read, at its head while a column or a caller waits for it. Planning a work
/// item passes through stages (the file's footer and plan when it is the file's first, its Bloom
/// filters and dictionaries, its page index and page filter, its mask probe, its plans), each a
/// parse of the bytes the read holds that returns its result or what it needs: a footer still being
/// read, a window's entries, a range of the file ([Parsed]). On a need, the chain demands what it
/// needs from the read's [FetchSequence], registers to run again once it is met, and lets its
/// thread go, keeping the claim; it never waits, so it needs no thread of its own.
///
/// How far it plans: the work items the read's columns asked for ([#requested]), the read's next
/// unit ([#nextUnit]), and further while [FetchSequence#mayPlanAhead] allows. The work items it has
/// planned are a cursor, which columns wait on ([#isPlanned], [#whenPlanned]). A caller outside the
/// read that needs a work item taken part of the way before the read starts asks for it to be taken
/// through a [Stage] ([#awaitStage]) and waits for the chain to publish that stage's cursor past it.
///
/// Each work item listed has one state, its [Planning], from its listing until every column has
/// released it ([#release]), when it is dropped: the work list holds only the work items the read
/// has not passed. Its planned state is written once, by the chain, before it publishes the work
/// item planned, and read by the columns after that. A failure ends planning: a work item it hit,
/// and every one after it, take the same failure, as do the work items of the file whose planning
/// failed and the files after it.
///
/// The chain also decides which file the read has reached, from the furthest work item the read
/// has reached and the work item's file, or, for a work item not listed yet, the file listed next:
/// the read's [FetchSequence] starts the footers ahead of it, and the chain commits the
/// [dev.hardwood.jfr.RowGroupFilterEvent]s of the files up to it.
final class PlanningChain {

    private final List<InputFile> inputFiles;
    private final FileMetadataCache fileMetadataCache;
    private final ReadExecutor executor;
    private final FetchSequence sequence;
    private final FileSchema referenceSchema;
    private final List<RowGroup> firstFileRowGroups;
    private final ReadProjection projection;
    private final ResolvedPredicate filterPredicate;
    private final SchemaCompatibility schemaCompatibility;
    private final RowGroupPruning pruning;
    private final FetchPlanBuilder fetchPlanBuilder;
    private final long maxRows;
    /// Whether planning goes past the next unit, by F ([FetchSequence#mayPlanAhead]).
    private final boolean plansDeep;
    /// The most bytes one [IndexWindow] reads, gaps included, unless a single row group needs more.
    private final long indexWindowBytes;

    /// The work list: the state of each work item listed, by index, appended to a file at a time by
    /// the chain and dropped as every column releases a work item.
    private final Map<Integer, Planning> plannings = new ConcurrentHashMap<>();
    /// The work items listed. Written under `cursorLock`.
    private volatile int listedCount;
    /// The decoded columns, each of which takes and releases every work item it reads.
    private final int columnCount;
    /// The [dev.hardwood.jfr.RowGroupFilterEvent] of each file planned, by file index, until the
    /// read reaches the file.
    private final Map<Integer, Event> fileEvents = new ConcurrentSkipListMap<>();

    // The chain's own state, read and written only under its claim
    /// The next file to list; written under `cursorLock` too, which [#updateReached] reads it under.
    private int nextFileToPlan;
    private long planRowBudget;
    private long plannedRows;
    private long planSkipRemaining;
    /// Whether the read's batch plan has taken a work item, after which it can no longer be set.
    private boolean batchesPlanned;
    /// Whether the work item the chain plans is ahead of the columns and the next unit, which lists
    /// no file whose footer the sequence has not started.
    private boolean planningAhead;

    /// What the chain has published, replaced as a whole.
    private volatile Progress progress = new Progress(new int[] {-1, -1, -1}, false, null, Integer.MAX_VALUE, 0);
    /// Whether a column or caller took the failure that ended planning, so close does not raise it.
    /// Written by the column that took it, read by close once the columns have closed.
    private boolean failureTaken;

    /// The last work item callers outside the read asked to be taken through each stage before
    /// [Stage#PLANS], raised before they run the chain ([#advance]).
    private final AtomicIntegerArray targets = new AtomicIntegerArray(new int[] {-1, -1, -1});

    // Set before the read's columns ask for plans, read by the chain after
    private long tailSkip;
    private BatchPlan batchPlan;

    /// The claim: held while a step of the chain runs or waits for its need, and raised by every
    /// change that may let it go further, which the step looks at before it lets go.
    private final AtomicBoolean scheduled = new AtomicBoolean();
    private volatile boolean wanted;
    private volatile boolean closed;

    /// Guards the cursor: what columns asked for and wait on, and how far the read has reached.
    private final ReentrantLock cursorLock = new ReentrantLock();
    /// Guarded by `cursorLock`: the last work item a column asked to plan, `-1` before the read
    /// starts.
    private int requested = -1;
    /// Guarded by `cursorLock`: the next work item the chain plans, every one before it planned.
    private int planFrom;
    /// Guarded by `cursorLock`: whether the chain plans nothing more, at the end of the work list
    /// or after a failure.
    private boolean planningEnded;
    /// Guarded by `cursorLock`: the columns and callers waiting for a work item to be planned.
    private final List<PlanWaiter> planWaiters = new ArrayList<>();
    /// Guarded by `cursorLock`: the furthest work item the read has reached, as its next unit, a
    /// column's request or a caller's, and the last file the read has been told it reached.
    private int reachedItem = -1;
    private int toldFile = -1;

    /// What the chain has published: for each stage before [Stage#PLANS], the last work item it
    /// took through it for a caller; whether the work list is complete; the failure that ended
    /// planning with the first work item it fails; and the residue of the physical skip within the
    /// first work item.
    private record Progress(int[] reached, boolean listed, Throwable failure, int failedFrom,
                            long firstRowGroupSkip) {

        Progress withReached(Stage stage, int index) {
            int[] next = reached.clone();
            next[stage.ordinal()] = Math.max(next[stage.ordinal()], index);
            return new Progress(next, listed, failure, failedFrom, firstRowGroupSkip);
        }

        Progress withListed() {
            return new Progress(reached, true, failure, failedFrom, firstRowGroupSkip);
        }

        Progress withFailure(Throwable e, int from) {
            return failure != null ? this : new Progress(reached, listed, e, from, firstRowGroupSkip);
        }

        Progress withFirstRowGroupSkip(long skip) {
            return new Progress(reached, listed, failure, failedFrom, skip);
        }

        int reached(Stage stage) {
            return reached[stage.ordinal()];
        }
    }

    /// One work item's state: what its stages have decided so far and the bytes they read, written
    /// by the chain, and the columns that hold it and whether the read has entered it and reported
    /// its drop.
    static final class Planning {

        final WorkItem workItem;
        final PlanningReads pruningReads;
        final PlanningReads probeReads;
        final RowGroupPruning.Probe probe = new RowGroupPruning.Probe();
        RowGroupPruning.Arrival arrival;
        SharedRowGroupMetadata shared;
        MaskCapability mask;
        RowGroupPlans plans;
        /// The columns that took the work item and have not released it, `0` before the first takes it.
        final AtomicInteger holders = new AtomicInteger();
        final AtomicBoolean entered = new AtomicBoolean();
        final AtomicBoolean reported = new AtomicBoolean();

        Planning(WorkItem workItem, FetchSequence sequence) {
            this.workItem = workItem;
            String rowGroup = "rg=" + workItem.rowGroupIndex();
            this.pruningReads = new PlanningReads(sequence, workItem.inputFile(), rowGroup + " pruning", 1);
            this.probeReads = new PlanningReads(sequence, workItem.inputFile(), rowGroup + " indexes", 1);
        }
    }

    PlanningChain(List<InputFile> inputFiles, FileMetadataCache fileMetadataCache, ReadExecutor executor,
                  FetchSequence sequence, FileSchema referenceSchema, List<RowGroup> firstFileRowGroups,
                  ReadProjection projection, ResolvedPredicate filterPredicate,
                  SchemaCompatibility schemaCompatibility, RowGroupPruning pruning,
                  FetchPlanBuilder fetchPlanBuilder, long maxRows, long physicalSkip, long tailSkip,
                  boolean plansDeep, long indexWindowBytes) {
        this.inputFiles = inputFiles;
        this.fileMetadataCache = fileMetadataCache;
        this.executor = executor;
        this.sequence = sequence;
        this.referenceSchema = referenceSchema;
        this.firstFileRowGroups = firstFileRowGroups;
        this.projection = projection;
        this.filterPredicate = filterPredicate;
        this.schemaCompatibility = schemaCompatibility;
        this.pruning = pruning;
        this.fetchPlanBuilder = fetchPlanBuilder;
        this.maxRows = maxRows;
        this.plansDeep = plansDeep;
        this.indexWindowBytes = indexWindowBytes;
        this.planRowBudget = maxRows > 0 ? maxRows : Long.MAX_VALUE;
        this.planSkipRemaining = physicalSkip;
        this.tailSkip = tailSkip;
        this.columnCount = projection.decoded().getProjectedColumnCount();
    }

    // ==================== Running the chain ====================

    /// Runs the chain unless a step of it runs or waits, which then looks again. Called on every
    /// change that may let it go further: a column asking for a work item, the read entering a row
    /// group, an entry starting, a footer read ahead arriving, a caller raising a target.
    void advance() {
        if (closed) {
            return;
        }
        // Raised before the claim is tried: a step that releases the claim reads it after
        wanted = true;
        if (scheduled.compareAndSet(false, true)) {
            submit();
        }
    }

    /// Runs the chain in place, on the calling thread of the read's executor, unless a step of it
    /// runs or waits, which then looks again: a column's fetch step that needs a work item planned
    /// runs the chain itself rather than queue it. A read whose files are in memory then plans on
    /// the step, as its chain never waits.
    void advanceInPlace() {
        if (closed) {
            return;
        }
        wanted = true;
        if (scheduled.compareAndSet(false, true)) {
            run();
        }
    }

    /// Queues a step of the chain, which holds the claim: at the head of the read's executor while
    /// a column or a caller waits for it, at the tail otherwise.
    private void submit() {
        try {
            if (demanded() || callerWaits()) {
                executor.executeStep(this::run);
            }
            else {
                executor.executePlanning(this::run);
            }
        }
        catch (RejectedExecutionException e) {
            fail(HardwoodContextImpl.contextClosed(e), planFrom());
            scheduled.set(false);
            executor.tasks().signal();
        }
    }

    private boolean callerWaits() {
        Progress p = progress;
        for (Stage stage : CALLER_STAGES) {
            if (targets.get(stage.ordinal()) > p.reached(stage) && p.reached(stage) + 1 < p.failedFrom()) {
                return true;
            }
        }
        return false;
    }

    /// One step: plans until it needs what is not there or has nothing left to do. On a need it
    /// keeps the claim and runs again once the need is met; otherwise it lets the claim go and
    /// looks once more for a change that arrived meanwhile.
    ///
    /// A failure outside a stage, which [#stage] does not catch, ends planning at the next work
    /// item to plan and lets the claim go, so no column waits for planning and no close for the
    /// claim forever.
    private void run() {
        Need need = null;
        try {
            do {
                wanted = false;
                need = plan();
                if (need != null) {
                    return;
                }
                scheduled.set(false);
                executor.tasks().signal();
                // Released before the flag is read: a change after the step looked either raised
                // the flag before this read, or finds the claim free and runs the chain
            }
            while (wanted && !closed && scheduled.compareAndSet(false, true));
        }
        catch (RuntimeException | Error e) {
            need = null;
            try {
                fail(e, planFrom());
            }
            finally {
                scheduled.set(false);
                executor.tasks().signal();
            }
        }
        finally {
            if (need != null) {
                need.whenMet(this::submit);
            }
        }
    }

    /// Plans until a stage needs what is not there, which it returns, or nothing is left to plan:
    /// first what callers asked for, then the read's own work items.
    private Need plan() {
        while (!closed) {
            Progress p = progress;
            Need need = null;
            boolean progressed = false;
            for (Stage stage : CALLER_STAGES) {
                int next = p.reached(stage) + 1;
                if (next <= targets.get(stage.ordinal()) && next < p.failedFrom()) {
                    planningAhead = false;
                    need = stage(next, stage);
                    progressed = true;
                    break;
                }
            }
            if (!progressed) {
                int index = nextToPlan();
                if (index < 0) {
                    return null;
                }
                if (index >= p.failedFrom()) {
                    planningEnded();
                    continue;
                }
                need = stage(index, Stage.PLANS);
                if (need == NO_FOOTER_YET) {
                    // Runs again once the footer the sequence starts has been read
                    return null;
                }
            }
            if (need != null) {
                return need;
            }
        }
        planningEnded();
        return null;
    }

    /// The stages a caller or the read asks the chain to take a work item through, in order.
    enum Stage {
        /// Listed in the work list, its file planned, for [#workItemAt] and [#workItems].
        LISTED,
        /// Its Bloom filters and dictionaries, for [#arrivalAt].
        ARRIVAL,
        /// Its page index and mask probe, for [#awaitMasks] and [#sharedAt].
        MASK,
        /// Its plans, for the read's columns, which ask through the [FetchSequence].
        PLANS
    }

    private static final Stage[] CALLER_STAGES = {Stage.LISTED, Stage.ARRIVAL, Stage.MASK};

    /// What a stage planning ahead returns when the file it would list has no footer started yet:
    /// not a need the chain registers on, but the end of its step. The footer's arrival runs the
    /// chain again.
    private static final Need NO_FOOTER_YET = resume -> {
        throw new IllegalStateException("Not a need to wait for");
    };

    /// Takes work item `index` through `stage`, listing it first, and publishes what it reached;
    /// returns what a stage needs, `null` once done. A failure ends planning at the work item.
    private Need stage(int index, Stage stage) {
        Need need = listThrough(index);
        if (need != null) {
            // NO_FOOTER_YET included, which ends this step without a need
            return need;
        }
        if (index >= listedCount) {
            // Past the end of the work list, or of the files planned before a failure
            if (stage == Stage.PLANS) {
                planningEnded();
            }
            else {
                publish(progress.withReached(stage, targets.get(stage.ordinal())));
            }
            return null;
        }
        Planning planning = planning(index);
        try {
            need = switch (stage) {
                case LISTED -> null;
                case ARRIVAL -> arrival(planning);
                case MASK -> masked(planning);
                case PLANS -> plans(planning);
            };
            if (need == null) {
                if (stage == Stage.PLANS) {
                    planned(index);
                    executor.tasks().signal();
                }
                else {
                    publish(progress.withReached(stage, index));
                }
            }
            return need;
        }
        catch (IOException | RuntimeException | Error e) {
            fail(e, index);
            return null;
        }
    }

    /// Records `e` as the failure that ends planning at work item `from`, and resumes everything
    /// that waits for planning.
    private void fail(Throwable e, int from) {
        publish(progress.withFailure(e, from));
        sequence.stopFooters();
        planningEnded();
    }

    private void publish(Progress next) {
        progress = next;
        executor.tasks().signal();
    }

    // ==================== The work list ====================

    /// Plans files until the work list holds work item `index`, or none is left to plan; returns
    /// what a footer needs, `null` once done. Planning ahead, it lists a file only once the read's
    /// sequence has started the file's footer, within the files ahead and charged to R, and
    /// returns [#NO_FOOTER_YET] before.
    private Need listThrough(int index) {
        while (listedCount <= index && !progress.listed() && progress.failure() == null) {
            if (planningAhead && nextFileToPlan < inputFiles.size() && planRowBudget > 0
                    && !sequence.footerStarted(nextFileToPlan)) {
                return NO_FOOTER_YET;
            }
            Need need = planNextFile();
            if (need != null) {
                return need;
            }
        }
        return null;
    }

    /// Plans one more file, appending its surviving row groups to the work list, or records that
    /// none is left: every file done, or the row budget spent. Returns what the file's footer
    /// needs, `null` once done.
    ///
    /// A failure is sticky: the files after it are not planned, and the work items of the files
    /// before it are still handed out. Retrying the file is no option: planning it has already
    /// advanced the shared planning state, the next file to plan, the row budget and the rows
    /// still to skip among them.
    private Need planNextFile() {
        if (nextFileToPlan >= inputFiles.size() || planRowBudget <= 0) {
            publish(progress.withListed());
            updateReached();
            return null;
        }
        int fileIndex = nextFileToPlan;
        int plannedBefore = listedCount;
        try {
            FileMetadataCache.Loading loading = fileMetadataCache.fileForPlanning(fileIndex, sequence.tasks());
            if (loading.need() != null) {
                return loading.need();
            }
            list(planRowGroups(fileIndex, loading.file()));
        }
        catch (IOException | RuntimeException | Error e) {
            nextFileToPlan = inputFiles.size();
            fail(e, plannedBefore);
            return null;
        }
        updateReached();
        // The footers the planned file brings within reach
        sequence.pump();
        return null;
    }

    /// Appends one file's work items to the work list, and moves on to the next file.
    private void list(List<Planning> listed) {
        cursorLock.lock();
        try {
            for (Planning planning : listed) {
                plannings.put(planning.workItem.workItemIndex(), planning);
            }
            listedCount += listed.size();
            nextFileToPlan++;
        }
        finally {
            cursorLock.unlock();
        }
    }

    /// The work items of `prepared`'s row groups that pruning keeps, within the read's row budget,
    /// to append to the work list.
    private List<Planning> planRowGroups(int fileIndex, PreparedFile prepared) throws IOException {
        boolean hasFilter = filterPredicate != null;
        // Decided per file and in the file's own ordinals: the order its bounds were written in
        // is this file's to declare, not the reference file's (#1179).
        BoundsReadability boundsReadability = prepared.boundsReadability();
        FileColumnOrdinals columnOrdinals = fileIndex == 0
                ? FileColumnOrdinals.identity(referenceSchema.getColumnCount(), filterPredicate,
                        boundsReadability)
                : FileColumnOrdinals.of(
                        schemaCompatibility.validateSchemaCompatibility(prepared.inputFile(), prepared.schema()),
                        filterPredicate, boundsReadability);
        List<RowGroup> sourceRowGroups = fileIndex == 0 && firstFileRowGroups != null
                ? firstFileRowGroups : prepared.rowGroups();
        // Metadata pruning indexes every row group's chunk list by ordinal, so with
        // it active the cross-check has to cover the whole file before it runs.
        // Without it, the only row groups ever indexed are those that become work
        // items, and the ones physicalSkip / maxRows discard are never looked at.
        SchemaCompatibility.ChunkPathCheck chunkPaths = schemaCompatibility.chunkPathCheck(prepared.schema(),
                columnOrdinals);
        boolean pruningIndexesChunks = pruning.prunesByMetadata();
        if (pruningIndexesChunks) {
            for (int rgIndex = 0; rgIndex < sourceRowGroups.size(); rgIndex++) {
                chunkPaths.verify(sourceRowGroups.get(rgIndex), rgIndex, prepared.inputFile());
            }
        }
        RowGroupPruning.FilteredRowGroups filtered = pruning.filterRowGroups(
                sourceRowGroups, prepared.inputFile(), columnOrdinals);
        List<RowGroupPruning.FilteredRowGroup> rowGroups = filtered.rowGroups();
        if (filtered.event() != null) {
            fileEvents.put(fileIndex, filtered.event());
        }

        // Appended to the work list once the file's index windows are formed over them.
        List<WorkItem> planned = new ArrayList<>();
        for (int kept = 0; kept < rowGroups.size() && planRowBudget > 0; kept++) {
            RowGroupPruning.FilteredRowGroup decided = rowGroups.get(kept);
            RowGroup rg = decided.rowGroup();
            int rgIndex = decided.fileRowGroupIndex();
            long rgRows = rg.numRows();
            if (planSkipRemaining >= rgRows) {
                planSkipRemaining -= rgRows;
                continue;
            }

            long leadingSkip = planSkipRemaining;
            planSkipRemaining = 0;
            if (listedCount == 0 && planned.isEmpty()) {
                progress = progress.withFirstRowGroupSkip(leadingSkip);
            }

            // Not yet cross-checked when pruning was skipped: with pruning active the
            // loop above has already covered every row group in the file.
            if (!pruningIndexesChunks) {
                chunkPaths.verify(rg, rgIndex, prepared.inputFile());
            }

            planned.add(new WorkItem(
                    prepared.inputFile(),
                    rg,
                    prepared.schema(),
                    columnOrdinals,
                    fileIndex,
                    rgIndex,
                    listedCount + planned.size(),
                    plannedRows,
                    decided.alwaysMatches(),
                    decided.leafDecisions(),
                    decided.bloomFilterColumns(),
                    null));

            // maxRows limiting: deduct row count from budget.
            // With a filter active, actual match count is unpredictable,
            // so all row groups remain available.
            if (!hasFilter) {
                planRowBudget -= rgRows - leadingSkip;
            }
            plannedRows += rgRows - leadingSkip;
        }

        return withIndexWindows(planned);
    }

    /// The states of one file's planned work items, each with the [IndexWindow] it falls into,
    /// whose entries the sequence then holds.
    private List<Planning> withIndexWindows(List<WorkItem> planned) {
        if (planned.isEmpty()) {
            return List.of();
        }
        int count = planned.size();
        RowGroup[] rowGroups = new RowGroup[count];
        int[] rowGroupIndexes = new int[count];
        BitSet[] offsetIndexColumns = new BitSet[count];
        BitSet[] columnIndexColumns = new BitSet[count];
        BitSet[] bloomFilterColumns = new BitSet[count];
        for (int i = 0; i < count; i++) {
            WorkItem workItem = planned.get(i);
            rowGroups[i] = workItem.rowGroup();
            rowGroupIndexes[i] = workItem.rowGroupIndex();
            columnIndexColumns[i] = new BitSet();
            offsetIndexColumns[i] = new BitSet();
            bloomFilterColumns[i] = new BitSet();
            // A row group with a chunk in another file fails before its page index or bloom
            // filters are read (requireSameFile), so it asks for none: its offsets address that
            // other file.
            if (storedInThisFile(workItem.rowGroup())) {
                columnIndexColumns[i] = RowGroupPruning.columnIndexColumns(workItem, pruning.pageFiltering(workItem));
                offsetIndexColumns[i] = fetchPlanBuilder.offsetIndexColumns(workItem, columnIndexColumns[i]);
                bloomFilterColumns[i] = workItem.bloomFilterColumns();
            }
        }
        WorkItem first = planned.getFirst();
        List<IndexWindow> windows = IndexWindow.plan(indexWindowBytes, first.inputFile(),
                first.workItemIndex(), rowGroups, rowGroupIndexes, offsetIndexColumns,
                columnIndexColumns, bloomFilterColumns);
        List<Planning> listed = new ArrayList<>(count);
        int next = 0;
        for (IndexWindow window : windows) {
            window.insertEntries(sequence);
            for (int m = 0; m < window.memberCount(); m++) {
                listed.add(new Planning(planned.get(next++).withIndexWindow(window), sequence));
            }
        }
        return listed;
    }

    private static boolean storedInThisFile(RowGroup rowGroup) {
        for (ColumnChunk chunk : rowGroup.columns()) {
            if (!chunk.filePath().isEmpty()) {
                return false;
            }
        }
        return true;
    }

    // ==================== The stages of a work item ====================

    /// What the work item's Bloom filters and dictionaries decide; a row group they cannot drop is
    /// kept without a read.
    private Need arrival(Planning planning) throws IOException {
        if (planning.arrival != null) {
            return null;
        }
        WorkItem workItem = planning.workItem;
        if (!RowGroupPruning.droppableOnArrival(workItem)) {
            planning.arrival = RowGroupPruning.Arrival.KEPT;
            return null;
        }
        requireSameFile(workItem);
        try {
            ByteBuffer[] bloomFilterBytes = new ByteBuffer[workItem.rowGroup().columns().size()];
            if (!workItem.bloomFilterColumns().isEmpty()) {
                Need need = workItem.indexWindow().bloomFiltersNeed();
                if (need != null) {
                    return need;
                }
                bloomFilterBytes = workItem.indexWindow().bloomFiltersFor(workItem.workItemIndex());
            }
            planning.pruningReads.clearMissing();
            RowGroupPruning.Arrival arrival = pruning.probeOnArrival(workItem, bloomFilterBytes,
                    planning.pruningReads, planning.probe);
            // A filter or dictionary not yet read counted as absent: a drop stands, a keep is
            // probed again once it has arrived
            Parsed<?> missing = planning.pruningReads.missing();
            if (!arrival.dropped() && missing != null) {
                return planning.pruningReads.need(missing);
            }
            planning.arrival = arrival;
            return null;
        }
        catch (IOException e) {
            throw metadataFailure(workItem, e);
        }
    }

    /// The work item's index buffers and the rows its page filter keeps, after its arrival: a
    /// page-index window is read only once a member survives its Bloom filters and dictionaries.
    private Need shared(Planning planning) throws IOException {
        if (planning.shared != null) {
            return null;
        }
        Need need = arrival(planning);
        if (need != null) {
            return need;
        }
        WorkItem workItem = planning.workItem;
        if (planning.arrival.dropped()) {
            planning.shared = new SharedRowGroupMetadata(null, RowRanges.ALL, null, true);
            return null;
        }
        requireSameFile(workItem);
        try {
            need = workItem.indexWindow().indexNeed();
            if (need != null) {
                return need;
            }
            RowGroupIndexBuffers indexBuffers = workItem.indexWindow().buffersFor(workItem.workItemIndex());
            RowRanges matchingRows = RowRanges.ALL;
            if (pruning.pageFiltering(workItem)) {
                matchingRows = PageFilterEvaluator.computeMatchingRows(
                        workItem.columnOrdinals().filter(), workItem.rowGroup(), indexBuffers,
                        new LogContext(workItem.inputFile().name(), workItem.rowGroupIndex()),
                        workItem.columnOrdinals().boundsReadability());
            }
            planning.shared = new SharedRowGroupMetadata(indexBuffers, matchingRows, planning.arrival.dictionaries(),
                    false);
            return null;
        }
        catch (IOException e) {
            throw metadataFailure(workItem, e);
        }
    }

    /// The work item's page index read and, unless its Bloom filters or dictionaries dropped it,
    /// its mask probed.
    private Need masked(Planning planning) throws IOException {
        Need need = shared(planning);
        return need != null || planning.shared.dropped() ? need : mask(planning);
    }

    /// Whether every decoded column of the work item can take a page mask, by the page-format
    /// probe of each nested column without an offset index.
    private Need mask(Planning planning) throws IOException {
        if (planning.mask != null) {
            return null;
        }
        WorkItem workItem = planning.workItem;
        try {
            planning.probeReads.clearMissing();
            Parsed<Boolean> applicable = FetchPlanBuilder.masksApplicableForRowGroup(projection.decoded(),
                    workItem.rowGroup(), workItem.fileSchema(), workItem.columnOrdinals(), planning.probeReads);
            if (applicable.needsBytes()) {
                return planning.probeReads.need(applicable);
            }
            planning.mask = applicable.value() ? MaskCapability.YES : MaskCapability.NO;
            return null;
        }
        catch (IOException e) {
            throw metadataFailure(workItem, e);
        }
    }

    /// The fetch plans of every decoded column for the work item's row group, and then its batches.
    private Need plans(Planning planning) throws IOException {
        if (planning.plans != null) {
            return null;
        }
        WorkItem workItem = planning.workItem;
        long fileLength = workItem.inputFile().length();
        // Before the page index, which the plans need and these entries do not: they start while
        // it is read
        insertChunkEntries(workItem);
        Need need = shared(planning);
        if (need != null) {
            return need;
        }
        SharedRowGroupMetadata shared = planning.shared;
        int projectedCount = projection.decoded().getProjectedColumnCount();
        if (shared.dropped()) {
            FetchPlan[] empty = new FetchPlan[projectedCount];
            Arrays.fill(empty, FetchPlan.EMPTY);
            planning.plans = new RowGroupPlans(empty, planBatches(workItem, 0), List.of());
            return null;
        }
        RowGroup rowGroup = workItem.rowGroup();
        RowRanges matchingRows = shared.matchingRows();

        // The tail-read fast path's synthesized matching range, applied here rather than to the
        // shared metadata so that it stays independent of `tailSkip`
        if (matchingRows.isAll() && tailSkip > 0 && workItem.workItemIndex() == 0) {
            matchingRows = RowRanges.range(tailSkip, rowGroup.numRows());
        }

        // Per-page masks are honoured for this row group only when every decoded column is
        // mask-friendly; masking a subset of columns would leave sibling columns row-misaligned,
        // so a closed gate promotes `matchingRows` to ALL. Row ranges that are empty need no mask:
        // every column gets `FetchPlan.EMPTY`, so the gate is not probed for them.
        if (!matchingRows.isAll() && !matchingRows.isEmpty()) {
            need = mask(planning);
            if (need != null) {
                return need;
            }
            if (planning.mask == MaskCapability.NO) {
                matchingRows = RowRanges.ALL;
            }
        }

        // Whether a *predicate* narrowed the pages of this row group, which is what
        // PageFilterEvent reports, rather than the tail-read fast path's range or nothing at all
        // where a closed mask gate promoted the ranges back to ALL.
        boolean pageFilterApplied = !matchingRows.isAll() && !shared.matchingRows().isAll();

        FetchPlanBuilder.Plans plans = fetchPlanBuilder.columnPlans(workItem, shared, matchingRows,
                pageFilterApplied, perRgMaxRows(workItem), sequence, fileLength);

        // Last, once nothing above can fail: the batch plan takes each row group once.
        planning.plans = new RowGroupPlans(plans.columns(),
                planBatches(workItem, matchingRows.rowCount(rowGroup.numRows())), plans.pageFilterEvents());
        return null;
    }

    /// Inserts the entries of `workItem`'s row group from its chunks' extents, once, where every row
    /// of it is read and nothing can drop it, so they start before its page index is read.
    private void insertChunkEntries(WorkItem workItem) throws IOException {
        if (readsAllRows(workItem) && !RangeRead.inMemory(workItem.inputFile())
                && sequence.entries(workItem.workItemIndex()) == null) {
            sequence.insert(workItem.workItemIndex(), workItem.inputFile(),
                    fetchPlanBuilder.chunkSpans(workItem, workItem.inputFile().length()));
        }
    }

    /// Whether every row of `workItem`'s row group is read and no Bloom filter or dictionary can drop
    /// it, so its entries follow from its chunks' extents in the footer alone: no filter narrows its
    /// pages, no tail or row limit cuts it, and its chunks are in this file.
    private boolean readsAllRows(WorkItem workItem) {
        long perRgMaxRows = perRgMaxRows(workItem);
        return !RowGroupPruning.droppableOnArrival(workItem) && !pruning.pageFiltering(workItem)
                && !(tailSkip > 0 && workItem.workItemIndex() == 0)
                && (perRgMaxRows == 0 || perRgMaxRows >= workItem.rowGroup().numRows())
                && storedInThisFile(workItem.rowGroup());
    }

    /// Plans `workItem`'s batches from the rows its page masks keep, or returns `null` when the
    /// read plans no batches.
    private BatchPlan.RowGroupBatches planBatches(WorkItem workItem, long keptRows) {
        batchesPlanned = true;
        BatchPlan plan = batchPlan;
        if (plan == null) {
            return null;
        }
        return plan.plan(workItem.workItemIndex(), workItem.fileIndex(), workItem.inputFile().name(),
                workItem.filterAlwaysMatches(), keptRows);
    }

    /// Per-row-group remainder of the iterator-wide `maxRows` budget: `0` (no fetch-side
    /// truncation) when `maxRows` is unset or a filter is active, since `head(N)` then caps
    /// matching rows; otherwise what is left of it at this row group.
    private long perRgMaxRows(WorkItem workItem) {
        if (maxRows <= 0 || filterPredicate != null) {
            return 0;
        }
        long leadingSkip = workItem.workItemIndex() == 0 ? progress.firstRowGroupSkip() : 0;
        return Math.max(0, maxRows - workItem.rowsConsumedBefore() + leadingSkip);
    }

    private static IOException metadataFailure(WorkItem workItem, IOException e) {
        return new IOException(ExceptionContext.filePrefix(workItem.inputFile().name())
                + "Failed to fetch metadata for row group " + workItem.rowGroupIndex(), e);
    }

    /// Fails unless every chunk of the work item's row group stores its data in the file being
    /// read. It precedes every read the row group drives, whose offsets would address the other
    /// file for such a chunk.
    ///
    /// @throws UnsupportedOperationException if any chunk names another file
    private static void requireSameFile(WorkItem workItem) {
        List<ColumnChunk> columns = workItem.rowGroup().columns();
        for (int i = 0; i < columns.size(); i++) {
            try {
                columns.get(i).requireSameFile();
            }
            catch (UnsupportedOperationException e) {
                throw new UnsupportedOperationException(
                        ExceptionContext.filePrefix(workItem.inputFile().name())
                        + "Cannot read column " + i + " in row group "
                        + workItem.rowGroupIndex() + ": " + e.getMessage(), e);
            }
        }
    }

    // ==================== What the read and callers take ====================

    /// The work item at `index` once planned, `null` past the end of the work list.
    ///
    /// @throws IOException the failure that ended planning at or before it, as planning raised it
    WorkItem plannedWorkItem(int index) throws IOException {
        Progress p = progress;
        if (index >= p.failedFrom()) {
            failureTaken = true;
            throw rethrow(p.failure());
        }
        return index < listedCount ? planning(index).workItem : null;
    }

    /// The work item a failure planning its row group is reported against, `null` when there is
    /// none, as for a failure planning a file.
    WorkItem failedWorkItem(int index) {
        Progress p = progress;
        return p.failure() != null && index >= p.failedFrom() ? listedAt(index) : null;
    }

    /// The state of the listed work item at `index`.
    ///
    /// @throws IllegalStateException if every column has released it
    private Planning planning(int index) {
        Planning planning = plannings.get(index);
        if (planning == null) {
            throw new IllegalStateException("Work item " + index + " was released by every column");
        }
        return planning;
    }

    /// The plans of the work item at `index`, once planned.
    RowGroupPlans plans(int index) {
        return planning(index).plans;
    }

    /// What the work item's Bloom filters and dictionaries decided, `null` before its arrival.
    RowGroupPruning.Arrival arrival(int index) {
        Planning planning = plannings.get(index);
        return planning != null ? planning.arrival : null;
    }

    /// Records that a column takes the planned work item at `index`, which it releases once past
    /// it ([#release]).
    void take(int index) {
        planning(index).holders.compareAndSet(0, columnCount);
    }

    /// Records that a column has entered the work item at `index`; whether it is the first to.
    boolean enter(int index) {
        return planning(index).entered.compareAndSet(false, true);
    }

    /// Commits the drop of the work item at `index` by its Bloom filters or dictionaries, once,
    /// when the read reaches it, however far ahead the planning ran.
    void report(int index) {
        Planning planning = plannings.get(index);
        RowGroupPruning.Arrival arrival = planning != null ? planning.arrival : null;
        if (arrival != null && arrival.dropEvent() != null && planning.reported.compareAndSet(false, true)) {
            arrival.dropEvent().commit();
        }
    }

    /// Takes the work item at `index` through `stage`, on the caller's thread: runs the chain and
    /// waits until it has published the stage past it, the work list ended before it, or planning
    /// failed at or before it.
    ///
    /// @throws IOException the failure that ended planning at or before it
    private void awaitStage(int index, Stage stage) throws IOException {
        targets.accumulateAndGet(stage.ordinal(), index, Math::max);
        await(() -> progress.reached(stage) >= index || index >= progress.failedFrom());
        plannedWorkItem(index);
    }

    /// The work item at `index`, listing files as far as it, on the caller's thread; `null` past the
    /// end of the work list. The read has reached it.
    WorkItem workItemAt(int index) throws IOException {
        reached(index);
        targets.accumulateAndGet(Stage.LISTED.ordinal(), index, Math::max);
        await(() -> progress.reached(Stage.LISTED) >= index || index >= progress.failedFrom());
        // A listed work item is handed out whatever its own planning came to: only a failure
        // planning a file, which listed no work item from it on, fails here
        return index < listedCount ? planning(index).workItem : plannedWorkItem(index);
    }

    /// Starts the reads the planning of `workItem`, the read's next unit, needs first and that need
    /// no parse to locate: its window's Bloom filters where they may drop it, its window's page
    /// index where nothing can. The chain parses them when it reaches the work item, wherever it
    /// runs; this only starts them, on the calling column's thread.
    void startNextUnit(WorkItem workItem) throws IOException {
        if (workItem.indexWindow() == null || !storedInThisFile(workItem.rowGroup())) {
            return;
        }
        if (!RowGroupPruning.droppableOnArrival(workItem)) {
            workItem.indexWindow().indexNeed();
        }
        else if (!workItem.bloomFilterColumns().isEmpty()) {
            workItem.indexWindow().bloomFiltersNeed();
        }
    }

    /// Every work item, listing every file. The read has reached every file.
    List<WorkItem> workItems() throws IOException {
        reached(Integer.MAX_VALUE - 1);
        awaitStage(Integer.MAX_VALUE - 1, Stage.LISTED);
        List<WorkItem> workItems = new ArrayList<>(listedCount);
        for (int i = 0; i < listedCount; i++) {
            workItems.add(planning(i).workItem);
        }
        return workItems;
    }

    /// What the Bloom filters and dictionaries of the work item at `index` decide, on the caller's
    /// thread.
    RowGroupPruning.Arrival arrivalAt(int index) throws IOException {
        awaitStage(index, Stage.ARRIVAL);
        return arrival(index);
    }

    /// Probes the page masks of every work item, on the caller's thread; whether every one can take
    /// a mask.
    boolean awaitMasks() throws IOException {
        awaitStage(Integer.MAX_VALUE - 1, Stage.MASK);
        for (int i = 0; i < listedCount; i++) {
            Planning planning = planning(i);
            if (!planning.shared.dropped() && planning.mask == MaskCapability.NO) {
                return false;
            }
        }
        return true;
    }

    /// The shared metadata of the work item at `index`, its page index read, on the caller's thread.
    SharedRowGroupMetadata sharedAt(int index) throws IOException {
        awaitStage(index, Stage.MASK);
        return planning(index).shared;
    }

    /// Plans the work item at `index`, on the caller's thread, and returns its plans.
    RowGroupPlans awaitPlans(int index) throws IOException {
        requested(index);
        await(() -> isPlanned(index));
        plannedWorkItem(index);
        return plans(index);
    }

    /// The work item at `index` if the work list holds it, `null` otherwise.
    WorkItem listedAt(int index) {
        Planning planning = plannings.get(index);
        return planning != null ? planning.workItem : null;
    }

    /// Whether the work list holds every work item of the read.
    boolean listed() {
        Progress p = progress;
        return p.listed() && p.failure() == null;
    }

    /// The residue of the physical skip within the first work item, once the work list holds it.
    long firstRowGroupSkip() {
        return progress.firstRowGroupSkip();
    }

    // ==================== The cursor ====================

    /// Records that a column asks to plan the work item at `index`: its first entry starts as the
    /// next unit's does, since the column waits for the planning that inserts it, and the read has
    /// reached it.
    void requested(int index) {
        cursorLock.lock();
        try {
            if (index <= requested) {
                return;
            }
            requested = index;
        }
        finally {
            cursorLock.unlock();
        }
        sequence.nextUnit(index);
        reached(index);
    }

    /// Records that the work item at `index` is the read's next unit, since the first column has
    /// entered the one before it: its first entry starts without F and uncharged
    /// ([FetchSequence#nextUnit]), the chain plans it whatever F holds, and the read has reached it.
    void nextUnit(int index) {
        sequence.nextUnit(index);
        reached(index);
    }

    /// The next work item the chain may plan, `-1` when it may plan none now: none before the read
    /// starts, then the work items a column asked for and the next unit, and further as far as the
    /// sequence lets it plan ahead ([FetchSequence#mayPlanAhead]), which [#planningAhead] then
    /// records.
    private int nextToPlan() {
        int next;
        cursorLock.lock();
        try {
            if (requested < 0 || planningEnded) {
                return -1;
            }
            next = planFrom;
            planningAhead = false;
            if (next <= requested) {
                return next;
            }
        }
        finally {
            cursorLock.unlock();
        }
        if (sequence.withinNextUnit(next)) {
            return next;
        }
        planningAhead = true;
        return plansDeep && sequence.mayPlanAhead(next) ? next : -1;
    }

    /// Records that the chain has planned the work item at `index`, and resumes the waiters it
    /// meets.
    private void planned(int index) {
        List<Runnable> met = new ArrayList<>();
        cursorLock.lock();
        try {
            planFrom = Math.max(planFrom, index + 1);
            planWaiters.removeIf(waiter -> waiter.index() < planFrom && met.add(waiter.resume()));
        }
        finally {
            cursorLock.unlock();
        }
        met.forEach(Runnable::run);
    }

    /// Records that the chain plans nothing more, and resumes every waiter.
    private void planningEnded() {
        List<Runnable> met = new ArrayList<>();
        cursorLock.lock();
        try {
            planningEnded = true;
            planWaiters.forEach(waiter -> met.add(waiter.resume()));
            planWaiters.clear();
        }
        finally {
            cursorLock.unlock();
        }
        met.forEach(Runnable::run);
    }

    /// Whether a column waits for a work item the chain has not planned.
    private boolean demanded() {
        cursorLock.lock();
        try {
            return requested >= planFrom && !planningEnded;
        }
        finally {
            cursorLock.unlock();
        }
    }

    /// The next work item the chain plans.
    private int planFrom() {
        cursorLock.lock();
        try {
            return planFrom;
        }
        finally {
            cursorLock.unlock();
        }
    }

    /// Whether no step of the chain runs, is queued or waits for a need, for tests: the chain
    /// goes on only once something advances it after this returns.
    ///
    /// A step lets the claim go and takes it back where `wanted` was raised meanwhile, so the flag
    /// is read first: raised, the chain goes on; cleared, by a step holding the claim again.
    boolean atRest() {
        return !wanted && !scheduled.get();
    }

    /// Whether the chain has planned the work item at `index` or plans nothing more.
    boolean isPlanned(int index) {
        cursorLock.lock();
        try {
            return index < planFrom || planningEnded;
        }
        finally {
            cursorLock.unlock();
        }
    }

    /// What a column waits for until the work item at `index` is planned ([#isPlanned]).
    Need whenPlanned(int index) {
        return resume -> {
            cursorLock.lock();
            try {
                if (index >= planFrom && !planningEnded) {
                    planWaiters.add(new PlanWaiter(index, resume));
                    return;
                }
            }
            finally {
                cursorLock.unlock();
            }
            resume.run();
        };
    }

    private record PlanWaiter(int index, Runnable resume) {
    }

    /// Records that the read has reached the work item at `index`, and so its file.
    private void reached(int index) {
        cursorLock.lock();
        try {
            if (index <= reachedItem) {
                return;
            }
            reachedItem = index;
        }
        finally {
            cursorLock.unlock();
        }
        updateReached();
    }

    /// Tells the sequence the file the read has reached once it moves: the file of the furthest work
    /// item reached, or, for one not listed yet, the file listed next, and every file once the work
    /// list is complete. Commits the events of the files planned up to it, which a file listed
    /// since may add to.
    private void updateReached() {
        int told;
        boolean moved;
        cursorLock.lock();
        try {
            if (reachedItem < 0) {
                return;
            }
            int file;
            if (reachedItem < listedCount) {
                Planning planning = plannings.get(reachedItem);
                file = planning != null ? planning.workItem.fileIndex() : -1;
            }
            else {
                file = progress.listed() ? inputFiles.size() : nextFileToPlan;
            }
            moved = file > toldFile;
            toldFile = Math.max(toldFile, file);
            told = toldFile;
        }
        finally {
            cursorLock.unlock();
        }
        if (moved) {
            sequence.reachedFile(told);
        }
        reportFiles(told);
    }

    /// Runs the chain and waits until `done` holds: running the read's queue on a context of zero
    /// threads meanwhile, parking otherwise.
    private void await(BooleanSupplier done) {
        advance();
        executor.tasks().await(() -> done.getAsBoolean() || closed,
                executor instanceof ReadQueue queue ? queue : null);
    }

    /// Commits the [dev.hardwood.jfr.RowGroupFilterEvent]s of the files planned up to `fileIndex`,
    /// once the read has reached it.
    private void reportFiles(int fileIndex) {
        for (Map.Entry<Integer, Event> event : fileEvents.entrySet()) {
            if (event.getKey() > fileIndex) {
                break;
            }
            if (fileEvents.remove(event.getKey()) != null) {
                event.getValue().commit();
            }
        }
    }

    void setTailSkip(long tailSkip) {
        this.tailSkip = tailSkip;
    }

    void setBatchPlan(BatchPlan batchPlan) {
        this.batchPlan = batchPlan;
    }

    /// Whether a work item's batches were planned, after which the batch plan and tail skip are
    /// fixed.
    boolean batchesPlanned() {
        return batchesPlanned;
    }

    /// Records that a column is done with `workItem`; the last of the columns that took it drops
    /// its state and releases what its planning holds.
    ///
    /// @return whether this was the last
    boolean release(WorkItem workItem) {
        int index = workItem.workItemIndex();
        Planning planning = plannings.get(index);
        if (planning == null || planning.holders.getAndUpdate(h -> h > 0 ? h - 1 : h) != 1) {
            return false;
        }
        plannings.remove(index);
        planning.pruningReads.release();
        planning.probeReads.release();
        workItem.indexWindow().release(index);
        return true;
    }

    /// Stops the chain and waits for a step of it that runs or waits for its need, then drops the
    /// windows' entries. An `Error` that ended planning and that nothing took is returned for the
    /// caller to raise.
    Error close() {
        closed = true;
        planningEnded();
        executor.tasks().await(() -> !scheduled.get(), executor instanceof ReadQueue queue ? queue : null);
        for (Planning planning : plannings.values()) {
            planning.workItem.indexWindow().releaseAll();
            planning.pruningReads.discard();
            planning.probeReads.discard();
        }
        plannings.clear();
        return progress.failure() instanceof Error error && !failureTaken ? error : null;
    }

    /// Rethrows `failure` as the type it was raised as.
    private static IOException rethrow(Throwable failure) throws IOException {
        if (failure instanceof IOException io) {
            throw io;
        }
        if (failure instanceof RuntimeException re) {
            throw re;
        }
        if (failure instanceof Error err) {
            throw err;
        }
        throw new IOException(failure);
    }
}
