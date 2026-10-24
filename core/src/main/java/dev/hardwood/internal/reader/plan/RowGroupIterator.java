/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.io.Closeable;
import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.io.HeldBytes;
import dev.hardwood.internal.io.InputFileCloser;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.predicate.RowGroupFilterEvaluator;
import dev.hardwood.internal.predicate.dictionary.RowGroupDictionaryFilterSource;
import dev.hardwood.internal.reader.fetch.CoalescingPolicy;
import dev.hardwood.internal.reader.fetch.FetchPlan;
import dev.hardwood.internal.reader.fetch.FetchSequence;
import dev.hardwood.internal.reader.fetch.PrefetchTasks;
import dev.hardwood.internal.reader.footer.FileColumnOrdinals;
import dev.hardwood.internal.reader.memory.InternalProperty;
import dev.hardwood.internal.reader.memory.WorkingSet;
import dev.hardwood.internal.reader.select.RowRanges;
import dev.hardwood.internal.reader.step.Need;
import dev.hardwood.internal.reader.step.ReadExecutor;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.internal.schema.ReadProjection;
import dev.hardwood.jfr.PageFilterEvent;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.FileSchema;

/// Shared iterator over `(InputFile, RowGroup)` pairs across one or more files.
///
/// Holds the read's work list as its columns and callers see it, and its lifecycle: a column takes
/// each work item once the read's [PlanningChain] has planned it and releases it once past it, and
/// the read's close stops the chain and its [FetchSequence]. The schema checks
/// ([SchemaCompatibility]), pruning ([RowGroupPruning]) and the building of fetch plans
/// ([FetchPlanBuilder]) live in their own classes, which the chain runs.
///
/// Each [PageSource] maintains its own cursor into the work list.
public class RowGroupIterator implements Closeable {

    private final List<InputFile> inputFiles;
    private final FileMetadataCache fileMetadataCache;
    private final Consumer<RowGroupIterator> closeListener;

    /// [#close()] can be reached more than once, for instance through a column-reader scan that
    /// its group and every reader of that group close. Closing twice releases nothing twice.
    private volatile boolean closed;
    /// The reads the read has started, on the I/O executor; [#close()] waits for them.
    private final PrefetchTasks prefetchTasks;
    /// Runs the read's fetch steps, page tasks and planning: the context's pool, or a queue of the
    /// read's own on a context without one.
    private final ReadExecutor readExecutor;
    /// The read's data requests, planning reads and footer entries.
    private final FetchSequence sequence;
    /// S, the largest request, fixed for the read.
    private final int maxSpan;
    /// Whether the read plans its next unit ahead of its columns: not on a context of zero threads
    /// reading files all in memory, where that would only reorder its work.
    private final boolean plansAhead;
    /// Whether planning goes past the next unit, by F ([FetchSequence#mayPlanAhead]): a read of files
    /// not all in memory without a row limit.
    private final boolean plansDeep;
    private final HardwoodContextImpl context;
    private final long maxRows;
    private final long physicalSkip;
    private final long tailSkip;
    /// The most bytes one [IndexWindow] reads, gaps included, unless a single row group needs more.
    private final long indexWindowBytes = InternalProperty.longValue("hardwood.internal.indexWindowBytes",
            16L * 1024 * 1024, 1, "a positive byte count");

    // Set after first file
    private FileSchema referenceSchema;
    private List<RowGroup> firstFileRowGroups;
    /// The columns the read exposes and decodes; `null` until [#initialize].
    private ReadProjection projection;
    private ResolvedPredicate filterPredicate;
    private WorkingSet workingSet;
    /// The read's planning; `null` until [#initialize].
    private volatile PlanningChain chain;
    /// Whether [#planBatches] has been called.
    private boolean batchesSet;

    /// A single unit of work: one row group in one file.
    ///
    /// `rowsConsumedBefore` is the cumulative row count of all work items
    /// preceding this one in the work list — used to convert the iterator-wide
    /// `maxRows` budget into a per-row-group remainder when computing fetch
    /// plans. (Filter predicates invalidate this correlation, so callers must
    /// ignore it when a filter is active.)
    ///
    /// `leafDecisions` holds what statistics decided for each leaf of the filter,
    /// for the row group's bloom filters and dictionaries to sharpen once the read
    /// reaches it; `null` when no metadata filtering applies. `bloomFilterColumns`
    /// holds the file ordinals of the columns whose bloom filter a leaf would probe.
    ///
    /// `indexWindow` holds the row group's page-index and bloom-filter slices together with those
    /// of the work items planned beside it; `null` only while its file is planned.
    public record WorkItem(
            InputFile inputFile,
            RowGroup rowGroup,
            FileSchema fileSchema,
            FileColumnOrdinals columnOrdinals,
            int fileIndex,
            int rowGroupIndex,
            int workItemIndex,
            long rowsConsumedBefore,
            boolean filterAlwaysMatches,
            RowGroupFilterEvaluator.LeafDecisions leafDecisions,
            BitSet bloomFilterColumns,
            IndexWindow indexWindow
    ) {

        WorkItem withIndexWindow(IndexWindow window) {
            return new WorkItem(inputFile, rowGroup, fileSchema, columnOrdinals, fileIndex,
                    rowGroupIndex, workItemIndex, rowsConsumedBefore, filterAlwaysMatches,
                    leafDecisions, bloomFilterColumns, window);
        }
    }

    /// Shared metadata for one row group, reused across columns.
    ///
    /// `matchingRows` is filter-derived only: the tail-read fast path's synthesized
    /// `[tailSkip, numRows)` range is applied to the plans, so this record can be planned before
    /// `tailSkip` is known.
    ///
    /// `dictionaries` holds the dictionaries pruning read for this row group, for the fetch plans
    /// to decode with rather than read them again; `null` when pruning read none. A row group its
    /// bloom filters or dictionaries prove holds no match is `dropped`, and nothing else is read
    /// for it.
    public record SharedRowGroupMetadata(
            RowGroupIndexBuffers indexBuffers,
            RowRanges matchingRows,
            RowGroupDictionaryFilterSource dictionaries,
            boolean dropped
    ) {}

    /// The fetch plans of one row group, one per decoded column, its batches, `null` when the
    /// iterator plans no batches, and the [PageFilterEvent]s the read commits once it reaches the
    /// row group.
    record RowGroupPlans(FetchPlan[] columns, BatchPlan.RowGroupBatches batches,
                         List<PageFilterEvent> pageFilterEvents) {}

    /// Creates a RowGroupIterator over the files of `fileMetadataCache`.
    ///
    /// @param fileMetadataCache the files' metadata, which the parent reader owns and closes
    /// @param closeListener invoked with this iterator once [#close()] has released its caches, so
    ///        a parent can stop tracking an iterator its child reader has already torn down
    /// @param context the Hardwood context
    /// @param maxRows maximum rows to read (0 = unlimited)
    /// @param tailSkip leading rows of the first row group to skip via per-page
    ///        masking (0 = unused). Caller must guarantee every projected column
    ///        in every subset row group has an OffsetIndex; otherwise sequential
    ///        columns would emit unmaskable rows from offset 0 and break
    ///        cross-column alignment.
    /// @param physicalSkip leading physical rows to skip while building the
    ///        work list (0 = unused). Whole row groups are dropped; the residue
    ///        within the first kept row group is exposed via [#firstRowGroupSkip()].
    ///        Mutually exclusive with `tailSkip` — one masks the first row group's
    ///        leading rows, the other drops leading row groups, with no combined
    ///        semantics.
    public RowGroupIterator(FileMetadataCache fileMetadataCache, Consumer<RowGroupIterator> closeListener,
                            HardwoodContextImpl context, long maxRows, long tailSkip,
                            long physicalSkip) {
        context.requireOffPool();
        if (tailSkip < 0) {
            throw new IllegalArgumentException("tailSkip must be non-negative, got " + tailSkip);
        }
        if (physicalSkip < 0) {
            throw new IllegalArgumentException("physicalSkip must be non-negative, got " + physicalSkip);
        }
        if (tailSkip > 0 && physicalSkip > 0) {
            throw new IllegalArgumentException(
                    "tailSkip and physicalSkip are mutually exclusive, got tailSkip=" + tailSkip
                            + ", physicalSkip=" + physicalSkip);
        }
        this.fileMetadataCache = fileMetadataCache;
        this.closeListener = closeListener;
        this.inputFiles = fileMetadataCache.inputFiles();
        this.context = context;
        this.readExecutor = ReadExecutor.forContext(context);
        this.prefetchTasks = new PrefetchTasks(context.ioExecutor());
        this.maxSpan = CoalescingPolicy.maxSpanBytes();
        boolean inMemory = inputFiles.stream().allMatch(HeldBytes::inMemory);
        this.plansAhead = !inMemory || context.hasPool();
        this.plansDeep = maxRows <= 0 && !inMemory;
        this.sequence = new FetchSequence(prefetchTasks, context.readAheadBuffer(),
                FetchSequence.fetchAheadBytes(maxSpan), maxRows <= 0,
                this::advancePlanning, footers(fileMetadataCache), filesAhead(maxRows, inMemory),
                inputFiles.size());
        this.maxRows = maxRows;
        this.tailSkip = tailSkip;
        this.physicalSkip = physicalSkip;
    }

    /// The read's working set, which its fetch plans and page decoders charge; `null` until
    /// [#initialize(ReadProjection, ResolvedPredicate, boolean)].
    public WorkingSet workingSet() {
        return workingSet;
    }

    /// The read's executor, which the read's [dev.hardwood.internal.reader.pipeline.ReadScheduler]
    /// takes.
    public ReadExecutor readExecutor() {
        return readExecutor;
    }

    /// Rows to discard from the first kept row group after whole row groups
    /// before the physical skip target have been dropped from the work list.
    public long firstRowGroupSkip() throws IOException {
        if (physicalSkip == 0) {
            return 0;
        }
        // Written when the first work item is added, so listing as far as that item is enough —
        // listing the whole read would put every footer back on the critical path of any skipping
        // read.
        chain.workItemAt(0);
        return chain.firstRowGroupSkip();
    }

    /// Sets the reference schema and this iterator's row-group subset for the
    /// first file, skipping [#openFirst()]. Used when metadata has been read
    /// externally (e.g., by [dev.hardwood.reader.ParquetFileReader], which seeds
    /// the shared [FileMetadataCache] with that same footer).
    ///
    /// The row groups stay iterator-local: they are one reader's filtered view,
    /// not a property of the file, so they never reach the shared cache.
    ///
    /// @param schema the file schema from the first file
    /// @param rowGroups the (already filtered) row groups from the first file
    public void setFirstFile(FileSchema schema, List<RowGroup> rowGroups) {
        this.referenceSchema = schema;
        this.firstFileRowGroups = rowGroups;
    }

    /// Opens the first file and returns its schema.
    public FileSchema openFirst() throws IOException {
        referenceSchema = fileMetadataCache.getFile(0).schema();
        return referenceSchema;
    }

    /// Applies a read projection and optional filter, and sets up the read's planning, which plans
    /// nothing yet. The iterator plans [ReadProjection#decoded()], and knows which of its columns
    /// are filter-only.
    ///
    /// @param projection the exposed and decoded columns
    /// @param filter resolved predicate, or `null` for no filtering
    /// @param metadataFilteringEnabled when `false`, the filter takes no
    ///        metadata-derived shortcut — no row-group pruning from statistics,
    ///        bloom filters or dictionaries, no page-index or
    ///        inline-page-statistics skipping, no always-match decision — so the
    ///        predicate is evaluated against every decoded row
    /// @return the decoded projection
    public ProjectedSchema initialize(ReadProjection projection, ResolvedPredicate filter,
                                      boolean metadataFilteringEnabled) throws IOException {
        if (referenceSchema == null) {
            throw new IllegalStateException("openFirst() must be called before initialize()");
        }
        ProjectedSchema projected = projection.decoded();
        this.projection = projection;
        this.filterPredicate = filter;
        this.workingSet = new WorkingSet(projected.getProjectedColumnCount(), context.readAheadBuffer());
        SchemaCompatibility schemaCompatibility = new SchemaCompatibility(referenceSchema, projected, filter);
        schemaCompatibility.validateReferenceColumns(referenceFileName());
        this.chain = new PlanningChain(inputFiles, fileMetadataCache, readExecutor, sequence, referenceSchema,
                firstFileRowGroups, projection, filter, schemaCompatibility,
                new RowGroupPruning(context, filter, metadataFilteringEnabled),
                new FetchPlanBuilder(projection, filter, metadataFilteringEnabled, context, workingSet, maxSpan),
                maxRows, physicalSkip, tailSkip, plansDeep, indexWindowBytes);
        return projected;
    }

    /// Returns the ordered work list of (file, rowGroup) pairs, planning every file.
    public List<WorkItem> getWorkItems() throws IOException {
        return chain.workItems();
    }

    /// Returns the reference schema (from the first file).
    public FileSchema referenceSchema() {
        return referenceSchema;
    }

    /// Returns the shared metadata of the given work item, planning it as far as its page index on
    /// the calling thread.
    public SharedRowGroupMetadata getSharedMetadata(WorkItem workItem) throws IOException {
        return chain.sharedAt(workItem.workItemIndex());
    }

    /// Whether the read has a row group left once it reaches them: walks the work list from the
    /// first item and probes the bloom filters and dictionaries of each item they may drop, until
    /// one is not dropped. An item they cannot drop is live without a read.
    ///
    /// A reader asks this before it builds its pipeline, so a read whose every row group is
    /// dropped on arrival starts no column and allocates no batch. The decisions are kept, so the
    /// read's planning reuses those of the items walked here.
    public boolean hasLiveWorkItem() throws IOException {
        for (int i = 0; ; i++) {
            WorkItem workItem = workItemAt(i);
            if (workItem == null) {
                return false;
            }
            try {
                RowGroupPruning.Arrival arrival = chain.arrivalAt(i);
                chain.report(i);
                if (!arrival.dropped()) {
                    return true;
                }
            }
            catch (RuntimeException e) {
                throw ExceptionContext.addReadContext(workItem.inputFile().name(), workItem.rowGroupIndex(), null, e);
            }
        }
    }

    /// Sets the tail-skip budget for the first row group's fetch plans.
    ///
    /// Used by [dev.hardwood.reader.ParquetFileReader#buildTailRowReader] to defer the tail-skip
    /// decision until after the gate has been probed via [#canFastSkipAllRowGroups], before any
    /// column has consumed from the iterator.
    ///
    /// @throws IllegalStateException if a fetch plan has already been computed, since changing
    ///         the tail skip after the fact would yield inconsistent plans.
    public void setTailSkip(long tailSkip) {
        if (tailSkip < 0) {
            throw new IllegalArgumentException("tailSkip must be non-negative, got " + tailSkip);
        }
        if (chain.batchesPlanned()) {
            throw new IllegalStateException(
                    "setTailSkip must be called before any column requests its fetch plan");
        }
        chain.setTailSkip(tailSkip);
    }

    /// Probes the row-group-wide mask gate for every work item, returning `true` iff per-page
    /// masking is applicable across all of them. Used by the tail-read fast path; the read's
    /// planning reuses the probes.
    ///
    /// Tail reading is single-file only — this method asserts that
    /// invariant rather than silently ignoring non-first-file work items.
    ///
    /// @throws IllegalStateException if the iterator was constructed with
    ///         more than one input file
    public boolean canFastSkipAllRowGroups() throws IOException {
        if (inputFiles.size() != 1) {
            throw new IllegalStateException(
                    "canFastSkipAllRowGroups requires a single-file iterator, got "
                            + inputFiles.size() + " files");
        }
        return chain.awaitMasks();
    }

    /// Decides the read's batch boundaries ([BatchPlan]) as the read plans its row groups. Every
    /// decoded column of the read closes its batches where this plan does. Must be called once,
    /// before any column requests its fetch plan.
    ///
    /// @param batchCapacity the read's batch capacity, in rows
    /// @param maxRows the row cap the read's batches end at, `0` for none; under a filter it
    ///        counts matching rows
    /// @throws IllegalStateException if the batches are already planned, or a fetch plan was
    ///         computed before
    public void planBatches(int batchCapacity, long maxRows) {
        if (projection == null) {
            throw new IllegalStateException("initialize() must be called before planBatches()");
        }
        if (batchesSet || chain.batchesPlanned()) {
            throw new IllegalStateException(
                    "planBatches must be called once, before any column requests its fetch plan");
        }
        batchesSet = true;
        chain.setBatchPlan(new BatchPlan(batchCapacity,
                BatchPlan.endsBatchesAtRowGroupBoundaries(projection.decoded().getProjectedColumns()),
                filterPredicate != null, maxRows));
    }

    /// The batches of `workItem`'s row group. Valid between the column's [#getColumnPlan] for the
    /// row group and its [#releaseWorkItem].
    ///
    /// @throws IllegalStateException if the read plans no batches ([#planBatches])
    BatchPlan.RowGroupBatches getBatches(WorkItem workItem) throws IOException {
        rowGroupPlans(workItem);
        return plansOf(workItem).batches();
    }

    /// The plans of `workItem`, which [#plannedWorkItem] handed out, so taking them reads nothing
    /// and waits for nothing.
    ///
    /// @throws IllegalStateException if the read planned no batches
    RowGroupPlans plansOf(WorkItem workItem) {
        RowGroupPlans plans = chain.plans(workItem.workItemIndex());
        if (plans.batches() == null) {
            throw new IllegalStateException("No batch plan: planBatches() was not called for this read");
        }
        return plans;
    }

    /// Returns the [FetchPlan] for the given column in the given row group, planning it on the
    /// calling thread if the read's planning has not.
    ///
    /// @param workItem the work item identifying the row group
    /// @param projectedColumnIndex the projected column index
    /// @return a fetch plan for iterating pages with lazy byte fetching
    public FetchPlan getColumnPlan(WorkItem workItem, int projectedColumnIndex) throws IOException {
        return rowGroupPlans(workItem).columns()[projectedColumnIndex];
    }

    private RowGroupPlans rowGroupPlans(WorkItem workItem) throws IOException {
        int index = workItem.workItemIndex();
        if (!isPlanned(index)) {
            chain.awaitPlans(index);
        }
        plannedWorkItem(index);
        return chain.plans(index);
    }

    /// Whether the work item at `index` is planned, or planning has ended before it; asks the
    /// read's planning to plan it otherwise, which a fetch step then waits for ([#whenPlanned]).
    boolean isPlanned(int index) {
        chain.requested(index);
        if (context.runsInPlace()) {
            // On a thread of the read's executor, which is where the chain runs
            chain.advanceInPlace();
        }
        else {
            chain.advance();
        }
        return chain.isPlanned(index);
    }

    /// Whether the read's planning has planned the work item at `index`, without asking it to.
    boolean isPlannedAhead(int index) {
        return chain.isPlanned(index);
    }

    /// Whether the read's planning is at rest ([PlanningChain#atRest()]), for tests.
    boolean isPlanningAtRest() {
        return chain.atRest();
    }

    /// Whether the read holds the work item at `index`, for tests.
    boolean holdsWorkItem(int index) {
        return chain.listedAt(index) != null;
    }

    /// What a fetch step waits for until the work item at `index` is planned.
    Need whenPlanned(int index) {
        return chain.whenPlanned(index);
    }

    /// The work item at `index` once [#isPlanned], `null` past the end of the work list. A column
    /// taking it enters it ([#enter]).
    ///
    /// @throws IOException the failure that ended planning at or before it, as planning raised it
    WorkItem plannedWorkItem(int index) throws IOException {
        WorkItem workItem = chain.plannedWorkItem(index);
        if (workItem == null) {
            return null;
        }
        chain.take(index);
        enter(workItem);
        return workItem;
    }

    /// The work item a failure planning its row group is reported against, `null` when there is
    /// none, as for a failure planning a file.
    WorkItem failedWorkItem(int index) {
        return chain.failedWorkItem(index);
    }

    /// Records that a column has entered `workItem`, once per work item: commits the events its
    /// planning decided and makes the next work item the read's next unit ([PlanningChain#nextUnit]).
    private void enter(WorkItem workItem) {
        if (!chain.enter(workItem.workItemIndex())) {
            return;
        }
        chain.report(workItem.workItemIndex());
        RowGroupPlans plans = chain.plans(workItem.workItemIndex());
        if (plans != null) {
            for (PageFilterEvent event : plans.pageFilterEvents()) {
                event.commit();
            }
        }
        if (plansAhead) {
            chain.nextUnit(workItem.workItemIndex() + 1);
            WorkItem next = chain.listedAt(workItem.workItemIndex() + 1);
            if (next != null) {
                try {
                    chain.startNextUnit(next);
                }
                catch (IOException e) {
                    // Speculative: the chain meets the failure when it plans the work item
                }
            }
            chain.advance();
        }
    }

    /// Notifies the iterator that one projected column is done with the given work item. On the
    /// last column, the work item's planning, windows and entries are released.
    ///
    /// In-flight `PageInfo` slices and decode tasks keep their byte data alive via the slice's
    /// parent reference, so this only drops the iterator's references.
    public void releaseWorkItem(WorkItem workItem) {
        if (chain.release(workItem)) {
            sequence.forget(workItem.workItemIndex());
            // Every column has passed the work item, and so every file before its own: their
            // tails and footers are dropped
            sequence.filesDoneBefore(workItem.fileIndex());
        }
    }

    /// The work item at `index`, listing files as far as it on the calling thread; `null` once the
    /// read is done.
    ///
    /// A work item of a file planned before a failed one is still handed out: a column can be
    /// ahead of the others, and a column failed short of it would return fewer rows of those files
    /// than the others.
    public WorkItem workItemAt(int index) throws IOException {
        return chain.workItemAt(index);
    }

    /// Stops the read's planning, waits for the reads the read has started, then releases its
    /// windows and entries. The metadata cache and the files are the parent reader's
    /// ([dev.hardwood.reader.ParquetFileReader]), which closes them; the iterator tells the parent
    /// to stop tracking it, so a closed child reader's work list does not stay reachable for the
    /// parent's whole lifetime.
    ///
    /// The caller closes the columns first, which waits for their fetch steps.
    ///
    /// An `Error` that ended planning, or that a read failed with, and that nothing took is thrown
    /// once the iterator is closed.
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        // Starts nothing more, and releases what it holds of the read-ahead buffer; its reads in
        // flight are prefetch tasks, waited for below
        sequence.close();
        Throwable failure = chain != null ? chain.close() : null;
        try {
            prefetchTasks.awaitAll();
        }
        catch (Error e) {
            if (failure == null) {
                failure = e;
            }
            else {
                failure.addSuppressed(e);
            }
        }
        closeListener.accept(this);
        InputFileCloser.rethrow(failure);
    }

    /// Runs the planning chain, once the read has set it up: on an entry starting or a footer
    /// read ahead arriving.
    private void advancePlanning() {
        PlanningChain planning = chain;
        if (planning != null && !closed) {
            planning.advance();
        }
    }

    /// The read's footer entries, which its [FileMetadataCache] holds, read as tasks of the read: a
    /// footer read ahead tells the planning chain it has arrived.
    private FetchSequence.Footers footers(FileMetadataCache cache) {
        return new FetchSequence.Footers() {
            @Override
            public CompletableFuture<?> start(int fileIndex) {
                return cache.prefetch(fileIndex, prefetchTasks, RowGroupIterator.this::advancePlanning);
            }

            @Override
            public void drop(int fileIndex) {
                cache.drop(fileIndex);
            }
        };
    }

    /// The files after the one it has reached whose footers a read reads ahead: up to
    /// [FetchSequence#maxFilesAhead] for a multi-file read of files not all in memory whose read
    /// plans ahead, and otherwise only the next file: a read of files all in memory, whose reads
    /// wait for nothing to overlap, or with a row limit, which may need no further file and under
    /// a filter learns only downstream that it is satisfied.
    private int filesAhead(long maxRows, boolean inMemory) {
        if (inputFiles.size() < 2 || maxRows > 0 || !plansAhead || inMemory) {
            return 1;
        }
        return FetchSequence.maxFilesAhead();
    }

    /// Whether a [dev.hardwood.MetadataSource] supplied the footers of the read's files, so that a
    /// failure reading a file's data may be a footer of content the file no longer holds.
    public boolean footersSupplied() {
        return fileMetadataCache.footersSupplied();
    }

    /// The name of the first file, whose schema is the reference schema of the read: what a
    /// failure found in that schema, before any row group is read, names.
    public String referenceFileName() {
        return inputFiles.get(0).name();
    }
}
