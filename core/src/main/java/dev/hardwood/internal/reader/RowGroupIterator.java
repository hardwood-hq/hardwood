/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.predicate.BoundsReadability;
import dev.hardwood.internal.predicate.LogContext;
import dev.hardwood.internal.predicate.PageFilterEvaluator;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.predicate.RowGroupFilterEvaluator;
import dev.hardwood.internal.predicate.dictionary.RowGroupDictionaryFilterSource;
import dev.hardwood.internal.reader.FileMetadataCache.PreparedFile;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.internal.schema.ReadProjection;
import dev.hardwood.internal.util.Lazy;
import dev.hardwood.jfr.PageFilterEvent;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.FileSchema;
import jdk.jfr.Event;

/// Shared iterator over `(InputFile, RowGroup)` pairs across one or more files.
///
/// Holds the read's work list and its lifecycle: planning a file at a time, `maxRows` limiting
/// at the row-group level, planning ahead of the read, the per-row-group caches and close. The
/// read's data requests are the entries of its [FetchSequence], which planning a row group inserts. The schema
/// checks ([SchemaCompatibility]), pruning ([RowGroupPruning]) and the building of fetch plans
/// ([FetchPlanBuilder]) live in their own classes.
///
/// Each [PageSource] maintains its own cursor into the work list exposed by
/// this iterator. Shared per-row-group metadata (index buffers, matching rows)
/// is cached for the current file and reused across columns.
public class RowGroupIterator implements Closeable {

    private static final Lazy<System.Logger> LOG = Lazy.of(() -> System.getLogger(RowGroupIterator.class.getName()));

    private final List<InputFile> inputFiles;
    private final FileMetadataCache fileMetadataCache;
    private final Consumer<RowGroupIterator> closeListener;

    /// [#close()] can be reached more than once, for instance through a
    /// column-reader scan that its group and every reader of that group close.
    /// Closing twice releases nothing twice. Volatile because the prefetch tasks,
    /// which [#close()] waits for after setting it, read it.
    private volatile boolean closed;
    /// The speculative reads this iterator's read has started, on the I/O executor; [#close()]
    /// waits for them.
    private final PrefetchTasks prefetchTasks;
    /// The speculative planning this iterator's read has started, at the tail of its executor;
    /// [#close()] waits for it.
    private final PrefetchTasks planningTasks;
    /// Runs the read's fetch steps, page tasks and planning: the context's pool, or a queue of the
    /// read's own on a context without one. Its scheduler and its planning share it.
    private final ReadExecutor readExecutor;
    /// Where the read plans its work items and the next row group, fixed once for the read.
    private final PlanningMode planningMode;
    /// The read's data requests and footer entries.
    private final FetchSequence sequence;
    /// S, the largest request, fixed for the read.
    private final int maxSpan;
    /// Whether planning goes past the next unit, by F ([FetchSequence#nextToPlan]): a read of files not all in
    /// memory without a row limit, whose planning ahead is not only reordering its work.
    private final boolean plansDeep;
    /// The planning chain: held while a carrier runs it ([#advancePlanning]), and raised by every
    /// change that may let it go further, which the carrier looks at before it lets go.
    private final AtomicBoolean chainScheduled = new AtomicBoolean();
    private volatile boolean chainWanted;
    private final HardwoodContextImpl context;
    private final long maxRows;
    private final long physicalSkip;

    /// Number of leading rows of the first row group to skip. Non-zero only on
    /// the tail-read fast path; consumed by [#computeFetchPlans] to synthesize
    /// a `[tailSkip, numRows)` matching range so the existing per-page mask
    /// machinery drops the leading pages and trims the straddling page. May
    /// be promoted from the construction-time value of `0` via
    /// [#setTailSkip(long)] once the gate decision is in.
    private long tailSkip;
    private long firstRowGroupSkip;

    // Set after first file
    private FileSchema referenceSchema;
    /// The columns the read exposes and decodes; `null` until [#initialize]. Every plan spans
    /// its decoded columns.
    private ReadProjection projection;
    private WorkingSet workingSet;
    private ResolvedPredicate filterPredicate;
    /// Planning state, carried between files because a file is planned when the
    /// read reaches it rather than all at once.
    private int nextFileToPlan;
    private long planRowBudget;
    private long plannedRows;
    private long planSkipRemaining;
    /// The read's schema checks, pruning and fetch-plan building; `null` until [#initialize].
    private SchemaCompatibility schemaCompatibility;
    private RowGroupPruning pruning;
    private FetchPlanBuilder fetchPlanBuilder;
    /// The most bytes one [IndexWindow] fetches, gaps included, unless a single row group needs
    /// more.
    private final long indexWindowBytes =
            Long.getLong("hardwood.internal.indexWindowBytes", 16L * 1024 * 1024);
    /// The failure that ended planning, rethrown to every later request for a work item
    /// past the files planned before it. See [#planNextFile].
    private Throwable planningFailure;
    /// How many work items the files planned before the failed one make up. A failed file's
    /// work items are not handed out, whether or not any reached the work list.
    private int workItemsBeforeFailure;

    /// Guards planning and the work list. A lock rather than a monitor: a thread plans while
    /// holding it and waits for a footer's I/O, and the calling thread, which plans the first row
    /// groups, may be virtual; on JDK 21 a virtual thread blocked on a monitor, or holding one
    /// while it waits, holds its carrier.
    private final ReentrantLock planLock = new ReentrantLock();

    // Work list: all (file, rowGroup) pairs to process, built during initialize(). Guarded by planLock.
    private final List<WorkItem> workItems = new ArrayList<>();

    // Per-row-group shared metadata cache (keyed by work item index)
    private final OnceCache<SharedRowGroupMetadata> metadataCache = new OnceCache<>();

    /// What each row group's bloom filters and dictionaries decided once the read reached it,
    /// keyed by work item index, so the decision [#hasLiveWorkItem] takes on the calling thread
    /// is the one [#getSharedMetadata] finds on a fetch step.
    private final OnceCache<RowGroupPruning.Arrival> arrivalCache = new OnceCache<>();

    // Per-row-group fetch plans cache (keyed by work item index).
    private final OnceCache<RowGroupPlans> fetchPlanCache = new OnceCache<>();

    /// The read's batch boundaries, set by [#planBatches] before any fetch plan is computed;
    /// `null` for an iterator whose fetch plans no scheduled column drains.
    private volatile BatchPlan batchPlan;
    /// Whether any row group's fetch plans have been computed, after which the batch plan can no
    /// longer be set.
    private volatile boolean fetchPlansComputed;

    /// The fetch plans of one row group, one per decoded column, its batches, `null` when the
    /// iterator plans no batches, and the [PageFilterEvent]s the read commits once it reaches the
    /// row group.
    private record RowGroupPlans(FetchPlan[] columns, BatchPlan.RowGroupBatches batches,
                                 List<PageFilterEvent> pageFilterEvents) {}

    // Number of projected columns still referencing each work item. Initialized to
    // projectedColumnCount in initialize(); each PageSource calls releaseWorkItem
    // when it advances past a work item, and on zero we evict the metadata and
    // fetch-plan caches for that index, and forget its entries in the sequence.
    /// One counter per work item, added as the item is planned. Keyed rather
    /// than indexed because the work list grows, and read from every column's
    /// fetch steps without the planning lock.
    private final Map<Integer, AtomicInteger> workItemRefCounts = new ConcurrentHashMap<>();

    /// Work items whose first demand access has made the next row group the read's next unit, held
    /// until every column releases the item. The next unit is planned once per work item, whichever
    /// column reaches the item first and whether or not the planning ahead planned it.
    private final Set<Integer> nextRowGroupPrefetched = ConcurrentHashMap.newKeySet();

    /// Work items whose events the read has committed, on reaching them ([#report]); held until
    /// every column releases the item.
    private final Set<Integer> reported = ConcurrentHashMap.newKeySet();

    /// The [dev.hardwood.jfr.RowGroupFilterEvent] of each file planned, by file index, until the
    /// read reaches the file ([#reportFiles]). Guarded by [#planLock].
    private final TreeMap<Integer, Event> fileEvents = new TreeMap<>();

    /// The planning of each work item a column has reached, keyed by work item index, for a read
    /// that plans on virtual threads ([#planning]); evicted with the work item's other caches.
    private final Map<Integer, WorkItemPlanning> plannings = new ConcurrentHashMap<>();

    /// Where a read plans its work items and the next row group ahead of its columns, by whether
    /// every file of the read holds its bytes in memory and whether the context has a pool:
    ///
    /// | Files | Pool | Zero threads |
    /// |---|---|---|
    /// | not all in memory | [#VIRTUAL_THREAD] | [#ON_STEP_PLAN_AHEAD] |
    /// | all in memory | [#ON_STEP_PLAN_AHEAD] | [#ON_STEP] |
    private enum PlanningMode {
        /// A work item is planned on a virtual thread, since its planning waits for reads, and a
        /// fetch step that reaches it first yields on it ([#planning]); the next row group is
        /// planned on the virtual thread of its prefetch.
        VIRTUAL_THREAD,
        /// A work item is planned on the fetch step that reaches it: no read can wait, or the step
        /// runs on the thread driving a read of a context without a pool, which planning may
        /// block. The next row group is planned ahead as a task at the tail of the read's executor.
        ON_STEP_PLAN_AHEAD,
        /// A work item is planned on the fetch step that reaches it, and nothing is planned or
        /// prefetched ahead: on one thread, with no read that waits, that would only reorder the
        /// read's work.
        ON_STEP;

        static PlanningMode of(boolean inMemory, boolean hasPool) {
            if (inMemory) {
                return hasPool ? ON_STEP_PLAN_AHEAD : ON_STEP;
            }
            return hasPool ? VIRTUAL_THREAD : ON_STEP_PLAN_AHEAD;
        }

        /// Whether the read prefetches and plans the next row group ahead of its columns.
        boolean plansAhead() {
            return this != ON_STEP;
        }
    }

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
    /// `indexWindow` fetches the row group's page-index and bloom-filter slices
    /// together with those of the work items planned beside it; `null` only while
    /// its file is planned.
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

    /// Cached shared metadata for one row group, reused across columns.
    ///
    /// `matchingRows` is filter-derived only — the tail-read fast path's
    /// synthesized `[tailSkip, numRows)` range is applied later in
    /// [#computeFetchPlans] so this record can be populated before
    /// `tailSkip` is known (the tail-read fast path needs to consult
    /// `maskCapability` to decide whether `tailSkip` is viable in the
    /// first place).
    ///
    /// `maskCapability` is probed on first request, which comes only from
    /// a row group whose plans would apply a page mask or from the tail-read
    /// fast path, so an unmasked read issues no probe.
    ///
    /// `dictionaries` holds the dictionaries pruning read for this row group, for
    /// the fetch plans to decode with rather than read them again; `null` when
    /// pruning read none. A row group its bloom filters or dictionaries prove holds
    /// no match is `dropped`, and nothing else is read for it.
    public record SharedRowGroupMetadata(
            RowGroupIndexBuffers indexBuffers,
            RowRanges matchingRows,
            LazyMaskCapability maskCapability,
            RowGroupDictionaryFilterSource dictionaries,
            boolean dropped
    ) {}

    private List<RowGroup> firstFileRowGroups;

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
        this.planningMode = PlanningMode.of(inputFiles.stream().allMatch(RangeRead::inMemory), context.hasPool());
        this.context = context;
        this.readExecutor = ReadExecutor.forContext(context);
        this.prefetchTasks = new PrefetchTasks(context.ioExecutor());
        this.planningTasks = new PrefetchTasks(readExecutor);
        this.maxSpan = CoalescingPolicy.maxSpanBytes();
        this.plansDeep = maxRows <= 0 && planningMode.plansAhead()
                && !inputFiles.stream().allMatch(RangeRead::inMemory);
        this.sequence = new FetchSequence(prefetchTasks, context.readAheadBuffer(),
                FetchSequence.fetchAheadBytes(maxSpan), maxRows <= 0,
                this::advancePlanning, footers(fileMetadataCache), filesAhead(maxRows), inputFiles.size());
        this.maxRows = maxRows;
        this.tailSkip = tailSkip;
        this.physicalSkip = physicalSkip;
    }

    /// The read's working set, which its fetch plans and page decoders charge; `null` until
    /// [#initialize(ReadProjection, ResolvedPredicate, boolean)].
    public WorkingSet workingSet() {
        return workingSet;
    }

    /// The read's executor, which the read's [ReadScheduler] takes.
    public ReadExecutor readExecutor() {
        return readExecutor;
    }

    /// Rows to discard from the first kept row group after whole row groups
    /// before the physical skip target have been dropped from the work list.
    public long firstRowGroupSkip() throws IOException {
        if (physicalSkip == 0) {
            return 0;
        }
        // Written when the first work item is added, so planning as far as that item is
        // enough — planning the whole read would put every footer back on the critical
        // path of any skipping read.
        ensurePlannedThrough(0);
        return firstRowGroupSkip;
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
        PreparedFile prepared = fileMetadataCache.getFile(0);
        referenceSchema = prepared.schema();
        return referenceSchema;
    }

    /// Applies a read projection and optional filter, builds the full work list. The
    /// iterator plans [ReadProjection#decoded()], and knows which of its columns are
    /// filter-only.
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
        this.workingSet = new WorkingSet(projected.getProjectedColumnCount(), context.readAheadBuffer());
        this.filterPredicate = filter;
        this.schemaCompatibility = new SchemaCompatibility(referenceSchema, projected, filter);
        this.pruning = new RowGroupPruning(context, filter, metadataFilteringEnabled);
        this.fetchPlanBuilder = new FetchPlanBuilder(projection, filter, metadataFilteringEnabled, context,
                workingSet, maxSpan);

        schemaCompatibility.validateReferenceColumns(referenceFileName());

        // Planning starts here and proceeds a file at a time, driven by what the
        // read reaches. Nothing is planned yet.
        planRowBudget = maxRows > 0 ? maxRows : Long.MAX_VALUE;
        planSkipRemaining = physicalSkip;
        nextFileToPlan = 0;

        return projected;
    }

    /// Returns the ordered work list of (file, rowGroup) pairs.
    public List<WorkItem> getWorkItems() throws IOException {
        ensureFullyPlanned();
        planLock.lock();
        try {
            return List.copyOf(workItems);
        }
        finally {
            planLock.unlock();
        }
    }

    /// Returns the reference schema (from the first file).
    public FileSchema referenceSchema() {
        return referenceSchema;
    }

    /// Returns shared metadata for the given work item, computing it on first access.
    /// Thread-safe: the first column to request metadata for a row group computes it;
    /// subsequent columns reuse the cached result.
    ///
    /// @param workItem the work item to get metadata for
    /// @return shared metadata (index buffers, filter-derived matching row
    ///         ranges, and the row-group-wide mask-applicability decision,
    ///         probed on first request)
    public SharedRowGroupMetadata getSharedMetadata(WorkItem workItem) throws IOException {
        try {
            return computeSharedMetadata(workItem);
        }
        catch (UncheckedIOException e) {
            // The mapping function below cannot declare it; this frame can, so the
            // wrapper goes no further than the two of them.
            throw ExceptionContext.unwrap(e);
        }
    }

    private SharedRowGroupMetadata computeSharedMetadata(WorkItem workItem) {
        return metadataCache.computeIfAbsent(workItem.workItemIndex(), idx -> {
            try (FetchReason.Scope ignored = FetchReason.set(
                    "rg=" + workItem.rowGroupIndex() + " indexes")) {
                requireSameFile(workItem);
                RowGroupPruning.Arrival arrival = arrival(workItem);
                if (arrival.dropped()) {
                    return droppedMetadata();
                }
                RowGroupDictionaryFilterSource dictionaries = arrival.dictionaries();
                // After the bloom filters and dictionaries: a row group they drop alone in its
                // window costs no index read. The fetch takes no lock but the window's own.
                RowGroupIndexBuffers indexBuffers = workItem.indexWindow().buffersFor(idx);
                boolean pageFiltering = pruning.pageFiltering(workItem);

                RowRanges matchingRows = RowRanges.ALL;
                if (pageFiltering) {
                    // Parsing the filter columns' column indexes and evaluating each page is CPU
                    // work, so it runs on the pool, which bounds a read's CPU.
                    matchingRows = context.compute("page filter", () -> PageFilterEvaluator.computeMatchingRows(
                            workItem.columnOrdinals().filter(), workItem.rowGroup(), indexBuffers,
                            new LogContext(workItem.inputFile().name(), workItem.rowGroupIndex()),
                            workItem.columnOrdinals().boundsReadability()));
                }

                // Probed only when a plan would apply a page mask: an unfiltered scan
                // never asks, and needs no page-header read to decide nothing.
                LazyMaskCapability maskCapability = new LazyMaskCapability(
                        () -> probeMaskCapability(workItem));

                return new SharedRowGroupMetadata(indexBuffers, matchingRows, maskCapability,
                        dictionaries, false);
            }
            catch (IOException e) {
                throw new UncheckedIOException(
                        ExceptionContext.filePrefix(workItem.inputFile().name())
                        + "Failed to fetch metadata for row group " + workItem.rowGroupIndex(), e);
            }
        });
    }

    /// Whether the read has a row group left once it reaches them: walks the work list from the
    /// first item and probes the bloom filters and dictionaries of each item they may drop, until
    /// one is not dropped. An item they cannot drop is live without a read, and a live item's
    /// page index is left to the columns' fetchers.
    ///
    /// A reader asks this before it builds its pipeline, so a read whose every row group is
    /// dropped on arrival starts no column and allocates no batch. The decisions are
    /// cached, so the fetchers reuse those of the items walked here.
    public boolean hasLiveWorkItem() throws IOException {
        for (int i = 0; ; i++) {
            WorkItem workItem = workItemAt(i);
            if (workItem == null) {
                return false;
            }
            try {
                RowGroupPruning.Arrival arrival = arrival(workItem);
                report(workItem);
                if (!arrival.dropped()) {
                    return true;
                }
            }
            catch (UncheckedIOException e) {
                throw ExceptionContext.unwrap(e);
            }
            catch (RuntimeException e) {
                throw ExceptionContext.addReadContext(workItem.inputFile().name(), workItem.rowGroupIndex(), null, e);
            }
        }
    }

    /// What `workItem`'s bloom filters and dictionaries decide, probed on first request; a row
    /// group they cannot drop is kept without a read.
    private RowGroupPruning.Arrival arrival(WorkItem workItem) {
        if (!RowGroupPruning.droppableOnArrival(workItem)) {
            return RowGroupPruning.Arrival.KEPT;
        }
        return arrivalCache.computeIfAbsent(workItem.workItemIndex(), idx -> {
            try {
                requireSameFile(workItem);
                return pruning.probeOnArrival(workItem);
            }
            catch (IOException e) {
                throw new UncheckedIOException(
                        ExceptionContext.filePrefix(workItem.inputFile().name())
                        + "Failed to fetch metadata for row group " + workItem.rowGroupIndex(), e);
            }
        });
    }

    /// Runs the page-format probe behind the row-group-wide mask gate, see
    /// [FetchPlanBuilder#masksApplicableForRowGroup].
    private MaskCapability probeMaskCapability(WorkItem workItem) throws IOException {
        try (FetchReason.Scope ignored = FetchReason.set(
                "rg=" + workItem.rowGroupIndex() + " indexes")) {
            return FetchPlanBuilder.masksApplicableForRowGroup(
                    projection.decoded(), workItem.rowGroup(), workItem.fileSchema(),
                    workItem.columnOrdinals(), workItem.inputFile())
                    ? MaskCapability.YES : MaskCapability.NO;
        }
        catch (IOException e) {
            throw new IOException(
                    ExceptionContext.filePrefix(workItem.inputFile().name())
                    + "Failed to fetch metadata for row group " + workItem.rowGroupIndex(), e);
        }
    }

    /// The shared metadata of a row group its bloom filters or dictionaries dropped.
    private static SharedRowGroupMetadata droppedMetadata() {
        return new SharedRowGroupMetadata(null, RowRanges.ALL,
                LazyMaskCapability.of(MaskCapability.YES), null, true);
    }

    /// Fails unless every chunk of the work item's row group stores its data in the file being
    /// read.
    ///
    /// This is the first thing [#getSharedMetadata] does, so it precedes every read the row group
    /// drives: the page index fetched right below, the dictionary and bloom-filter reads that
    /// prune it, and the fetch plans built from it. Every one of those reads takes its offsets
    /// from the chunk metadata, which addresses the other file for such a chunk.
    ///
    /// @throws UnsupportedOperationException if any chunk names another file
    private static void requireSameFile(WorkItem workItem) {
        List<ColumnChunk> columns = workItem.rowGroup().columns();
        for (int i = 0; i < columns.size(); i++) {
            try {
                columns.get(i).requireSameFile();
            }
            catch (UnsupportedOperationException e) {
                // Named here rather than by the catch around the caller, which only sees
                // `IOException`: this is unchecked and travels past it, so the file it came
                // from has to be added where it is raised.
                throw new UnsupportedOperationException(
                        ExceptionContext.filePrefix(workItem.inputFile().name())
                        + "Cannot read column " + i + " in row group "
                        + workItem.rowGroupIndex() + ": " + e.getMessage(), e);
            }
        }
    }

    /// Sets the tail-skip budget for the first row group's fetch plans.
    ///
    /// Used by [dev.hardwood.reader.ParquetFileReader#buildTailRowReader] to
    /// defer the tail-skip decision until after the gate has been probed via
    /// [#canFastSkipAllRowGroups]. The value flows into
    /// [#computeFetchPlans] when it builds the per-row-group fetch plans —
    /// so callers must invoke this method before any column has consumed
    /// from the iterator (specifically before
    /// [#getColumnPlan(WorkItem, int)]).
    ///
    /// @throws IllegalStateException if a fetch plan has already been
    ///         computed for the first work item, since changing the tail
    ///         skip after the fact would yield inconsistent plans.
    public void setTailSkip(long tailSkip) {
        if (tailSkip < 0) {
            throw new IllegalArgumentException("tailSkip must be non-negative, got " + tailSkip);
        }
        if (!fetchPlanCache.isEmpty()) {
            throw new IllegalStateException(
                    "setTailSkip must be called before any column requests its fetch plan");
        }
        this.tailSkip = tailSkip;
    }

    /// Pre-probes the row-group-wide mask gate for every work item,
    /// returning `true` iff per-page masking is applicable across all of
    /// them. Used by the tail-read fast path: it probes each row group's
    /// cached [LazyMaskCapability], so [#computeFetchPlans] reuses the
    /// result rather than running a second probe.
    ///
    /// Tail reading is single-file only — this method asserts that
    /// invariant rather than silently ignoring non-first-file work items.
    /// If multi-file tail reading is added later, that work must explicitly
    /// thread per-row-group input files through the gate decision.
    ///
    /// @throws IllegalStateException if the iterator was constructed with
    ///         more than one input file
    public boolean canFastSkipAllRowGroups() throws IOException {
        if (inputFiles.size() != 1) {
            throw new IllegalStateException(
                    "canFastSkipAllRowGroups requires a single-file iterator, got "
                            + inputFiles.size() + " files");
        }
        // Whether every row group supports per-page masking is a question about all of
        // them, so this one plans the read out. Single-file only, so that is one footer.
        ensureFullyPlanned();
        for (WorkItem workItem : workItems) {
            if (getSharedMetadata(workItem).maskCapability().get() == MaskCapability.NO) {
                return false;
            }
        }
        return true;
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
        if (batchPlan != null || fetchPlansComputed) {
            throw new IllegalStateException(
                    "planBatches must be called once, before any column requests its fetch plan");
        }
        batchPlan = new BatchPlan(batchCapacity,
                BatchPlan.endsBatchesAtRowGroupBoundaries(projection.decoded().getProjectedColumns()),
                filterPredicate != null, maxRows);
    }

    /// The batches of `workItem`'s row group. Valid between the column's [#getColumnPlan] for the
    /// row group and its [#releaseWorkItem].
    ///
    /// @throws IllegalStateException if the read plans no batches ([#planBatches])
    BatchPlan.RowGroupBatches getBatches(WorkItem workItem) throws IOException {
        BatchPlan.RowGroupBatches batches = rowGroupPlans(workItem).batches();
        if (batches == null) {
            throw new IllegalStateException("No batch plan: planBatches() was not called for this read");
        }
        return batches;
    }

    /// Returns the [FetchPlan] for the given column in the given row group.
    /// Plans are computed once per row group (on first access) and cached.
    ///
    /// @param workItem the work item identifying the row group
    /// @param projectedColumnIndex the projected column index
    /// @return a fetch plan for iterating pages with lazy byte fetching
    public FetchPlan getColumnPlan(WorkItem workItem, int projectedColumnIndex) throws IOException {
        return rowGroupPlans(workItem).columns()[projectedColumnIndex];
    }

    private RowGroupPlans rowGroupPlans(WorkItem workItem) throws IOException {
        int workItemIndex = workItem.workItemIndex();
        sequence.requested(workItemIndex);
        RowGroupPlans plans = fetchPlanCache.get(workItemIndex);
        if (plans == null) {
            try {
                plans = fetchPlanCache.computeIfAbsent(workItemIndex, idx -> {
                    try {
                        return computeFetchPlans(workItem);
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }
            catch (UncheckedIOException e) {
                // Wrapped only to leave the mapping function, and undone in the frame
                // that can declare what happened.
                throw ExceptionContext.unwrap(e);
            }
        }
        // The first demand access reports the row group and makes the next one the read's next
        // unit, whether it planned this one or found the plans the planning ahead cached; that
        // planning does not come through here. This runs outside the computation: resolving the
        // next work item plans the next file when this one is exhausted, which reads a footer and,
        // under a filter, probes bloom filters, and every other column asking for this row group's
        // plans waits for the whole of the computation.
        if (nextRowGroupPrefetched.add(workItemIndex)) {
            report(workItem);
            for (PageFilterEvent event : plans.pageFilterEvents()) {
                event.commit();
            }
            if (planningMode.plansAhead()) {
                enterNextUnit(workItem);
            }
        }
        return plans;
    }

    /// Notifies the iterator that one projected column is done with the given
    /// work item. Decrements the per-work-item reference counter; when it reaches
    /// zero (i.e. all columns have advanced past this work item), the cached
    /// metadata and fetch plans for that work item are evicted, releasing
    /// references to any fetched chunk bytes they hold.
    ///
    /// In-flight `PageInfo` slices and decode tasks keep their byte data alive
    /// via the slice's parent reference, so eviction here only drops the strong
    /// cache reference; the underlying chunk memory is reclaimed by GC once
    /// downstream consumers finish processing.
    public void releaseWorkItem(WorkItem workItem) {
        AtomicInteger counter = workItemRefCounts.get(workItem.workItemIndex());
        if (counter == null) {
            return;
        }
        int idx = workItem.workItemIndex();
        int remaining = counter.decrementAndGet();
        if (remaining == 0) {
            sequence.forget(idx);
            // Every column has passed the work item, and so every file before its own
            sequence.filesDoneBefore(workItem.fileIndex());
            reported.remove(idx);
            metadataCache.remove(idx);
            arrivalCache.remove(idx);
            fetchPlanCache.remove(idx);
            nextRowGroupPrefetched.remove(idx);
            plannings.remove(idx);
            workItem.indexWindow().release(idx);
        }
    }

    /// Whether every projected column has released `workItem`. [#releaseWorkItem] counts down
    /// before it evicts the work item's caches, as [#close] sets `closed` before it clears them.
    private boolean isReleased(WorkItem workItem) {
        AtomicInteger counter = workItemRefCounts.get(workItem.workItemIndex());
        return counter != null && counter.get() == 0;
    }

    /// Makes the work item after `currentWorkItem` the read's next unit, once per work item, when
    /// the first column enters `currentWorkItem`: its first entry starts without F and uncharged
    /// ([FetchSequence#nextUnit]), and the planning chain plans it whatever F holds
    /// ([FetchSequence#nextToPlan]).
    ///
    /// The next row group is asked for through [#workItemAt], on the caller, so the last row group
    /// of a file resolves the next file rather than stopping at the boundary — which is the one
    /// step of a read this exists to cover — and that planning is done by the time the read
    /// reaches it. What the caller waits for there is the next file's footer, whose read
    /// the sequence has already started ([FetchSequence#reachedFile]), or which the caller reads in place for a file
    /// whose bytes are in memory ([FileMetadataCache#prefetch]) — and under a filter, where
    /// planning also probes that file's bloom filters, which nothing has started.
    ///
    /// A planning failure is not raised here. The iterator keeps it, and every column's own
    /// request for the next work item rethrows it, so each column fails when its read reaches
    /// the failed file, with the failure as planning raised it. Raised here, it would fail
    /// the current row group, which is readable, and be reported as a failure in it.
    ///
    /// Called once per work item, by the planning that computes or first finds its plans, after
    /// their `computeIfAbsent` returns, never from inside it: this blocks, and every other caller
    /// asking for the work item's plans waits for the whole computation.
    private void enterNextUnit(WorkItem currentWorkItem) {
        WorkItem nextWorkItem;
        try {
            nextWorkItem = workItemAt(currentWorkItem.workItemIndex() + 1);
        }
        catch (IOException | RuntimeException | Error e) {
            // The planning failure, kept by planNextFile for the next request to rethrow.
            return;
        }
        if (nextWorkItem == null) {
            return;
        }
        sequence.nextUnit(nextWorkItem.workItemIndex());
        // The next unit's entries and windows at once, on a virtual thread of their own: the chain
        // may be planning further ahead, or wait at the tail of the read's queue for its planning
        prefetchTasks.submit(() -> readAhead(nextWorkItem));
        advancePlanning();
    }

    /// Inserts `workItem`'s entries from its chunks' extents and reads its index window and Bloom
    /// filters, ahead of its planning, which needs the windows and inserts the entries otherwise.
    /// Speculative: a failure is left to the planning, which repeats the work and reports it.
    private void readAhead(WorkItem workItem) {
        if (closed || isReleased(workItem)) {
            return;
        }
        try (FetchReason.Scope ignored = FetchReason.set("prefetch rg=" + workItem.rowGroupIndex())) {
            insertChunkEntries(workItem);
            IndexWindow window = workItem.indexWindow();
            if (window != null) {
                window.buffersFor(workItem.workItemIndex());
                window.bloomFiltersFor(workItem.workItemIndex());
            }
        }
        catch (IllegalStateException e) {
            // Every column released the row group since the check above
        }
        catch (IOException | RuntimeException e) {
            LOG.get().log(System.Logger.Level.DEBUG,
                    "Prefetch failed for row group {0}", workItem.rowGroupIndex(), e);
        }
    }

    /// Runs the planning chain, unless it runs already, which then looks again: on a virtual thread
    /// of the read's prefetch tasks, which may wait for the planning's reads. Called on every change
    /// that may let it go further: the read entering a row group, an entry starting, a footer read
    /// ahead arriving.
    private void advancePlanning() {
        if (!planningMode.plansAhead() || closed) {
            return;
        }
        // Raised before the claim is tried: a chain that releases the claim reads it after
        chainWanted = true;
        if (chainScheduled.compareAndSet(false, true)) {
            chainCarrier().submit(this::runChain);
        }
    }

    /// Where the chain runs: a virtual thread of the read's prefetch tasks, or, for a read that
    /// plans on the step, a task at the tail of its executor, which plans files and row groups
    /// where every other planning of the read runs.
    private PrefetchTasks chainCarrier() {
        return planningMode == PlanningMode.VIRTUAL_THREAD ? prefetchTasks : planningTasks;
    }

    /// Plans work items ahead of the read, in work-list order, once each, while
    /// [FetchSequence#nextToPlan] allows. A read that plans on the step ([PlanningMode#ON_STEP_PLAN_AHEAD]) reads a work item's
    /// entries and index window on a virtual thread, which then plans it as a task at the tail of
    /// the read's executor and continues the chain there.
    private void runChain() {
        do {
            chainWanted = false;
            while (!closed) {
                int index = sequence.nextToPlan(plansDeep);
                if (index < 0 || !planned(index - 1)) {
                    // A row group still being planned on demand runs the chain again once it is
                    break;
                }
                WorkItem workItem = workItemAhead(index);
                if (workItem == null) {
                    break;
                }
                if (planningMode == PlanningMode.VIRTUAL_THREAD) {
                    // Planning waits for the row group's dictionaries, so it stays on the virtual
                    // thread; its CPU work runs on the pool
                    readAhead(workItem);
                    planAhead(workItem);
                    sequence.planned(index);
                    continue;
                }
                prefetchTasks.submit(() -> {
                    readAhead(workItem);
                    planningTasks.submit(() -> {
                        try {
                            planAhead(workItem);
                            sequence.planned(index);
                        }
                        finally {
                            chainScheduled.set(false);
                            advancePlanning();
                        }
                    });
                });
                return;
            }
            chainScheduled.set(false);
            // Released before the flag is read: a change after the chain looked either raised the
            // flag before this read, or finds the claim free and runs the chain
        }
        while (chainWanted && chainScheduled.compareAndSet(false, true));
    }

    /// Whether the work item at `index` has its plans, or has been released by every column, so the
    /// one after it may be planned: row groups are planned in work-list order. `true` before the
    /// first.
    boolean planned(int index) {
        if (index < 0 || fetchPlanCache.get(index) != null) {
            return true;
        }
        AtomicInteger counter = workItemRefCounts.get(index);
        return counter != null && counter.get() == 0;
    }

    /// The work item at `index` for the planning chain, planning files only as far as their
    /// footers have been read, so the chain waits for no footer, and the next unit, which the
    /// read resolves itself ([#enterNextUnit]); `null` when it is not planned yet, or past
    /// the end or a planning failure. A file whose footer arrives later is planned then: its
    /// arrival runs the chain again.
    private WorkItem workItemAhead(int index) {
        planLock.lock();
        try {
            while (workItems.size() <= index && planningFailure == null && nextFileToPlan < inputFiles.size()
                    && (RangeRead.inMemory(inputFiles.get(nextFileToPlan))
                            || fileMetadataCache.footerRead(nextFileToPlan))) {
                try {
                    if (!planNextFile()) {
                        break;
                    }
                }
                catch (IOException | RuntimeException | Error e) {
                    // Kept by planNextFile for the read to rethrow when it reaches the file
                    return null;
                }
            }
            if (planningFailure != null && index >= workItemsBeforeFailure) {
                return null;
            }
            return index < workItems.size() ? workItems.get(index) : null;
        }
        finally {
            planLock.unlock();
        }
    }

    /// Plans `nextWorkItem` ahead of its read, on the planning chain's carrier ([#runChain]).
    private void planAhead(WorkItem nextWorkItem) {
        fetchPlanCache.computeIfAbsent(
                nextWorkItem.workItemIndex(),
                idx -> {
                    // Every column may already have read past the row group and
                    // released it, or the iterator have been closed, evicting its plans.
                    // Planning it again would repeat its dictionary and index reads, and
                    // report a row group its dictionaries drop a second time.
                    if (closed || isReleased(nextWorkItem)) {
                        return null;
                    }
                    try {
                        return computeFetchPlans(nextWorkItem);
                    }
                    catch (IOException e) {
                        // Speculative: nothing is waiting on this. Returning
                        // null leaves the entry uncached, so the demand path
                        // plans the row group again and reports the failure
                        // to a caller that is waiting for it.
                        LOG.get().log(System.Logger.Level.DEBUG,
                                "Prefetch failed for row group {0}",
                                nextWorkItem.rowGroupIndex(), e);
                        return null;
                    }
                });
    }

    /// Commits the events of `workItem`'s row group that its planning decided, once, when the read
    /// reaches it, however far ahead the planning ran: the drop by its Bloom filters or
    /// dictionaries.
    private void report(WorkItem workItem) {
        if (!reported.add(workItem.workItemIndex())) {
            return;
        }
        RowGroupPruning.Arrival arrival = RowGroupPruning.droppableOnArrival(workItem)
                ? arrivalCache.get(workItem.workItemIndex()) : null;
        if (arrival != null && arrival.dropEvent() != null) {
            arrival.dropEvent().commit();
        }
    }

    /// Commits the [dev.hardwood.jfr.RowGroupFilterEvent]s of the files planned up to `fileIndex`,
    /// once the read has reached it. Called under [#planLock].
    private void reportFiles(int fileIndex) {
        while (!fileEvents.isEmpty() && fileEvents.firstKey() <= fileIndex) {
            fileEvents.pollFirstEntry().getValue().commit();
        }
    }

    /// Computes the fetch plans of every decoded column for `workItem`'s row group, and then
    /// the row group's batches. Row groups are planned in work-list order: the first column to
    /// reach a row group plans it, or the previous row group's prefetch, which starts only once
    /// that row group is planned.
    private RowGroupPlans computeFetchPlans(WorkItem workItem) throws IOException {
        fetchPlansComputed = true;
        long fileLength = workItem.inputFile().length();
        // Before the page index, which the plans need and these entries do not: they start while
        // it is read
        insertChunkEntries(workItem);
        SharedRowGroupMetadata shared = getSharedMetadata(workItem);
        int projectedCount = projection.decoded().getProjectedColumnCount();
        if (shared.dropped()) {
            FetchPlan[] empty = new FetchPlan[projectedCount];
            Arrays.fill(empty, FetchPlan.EMPTY);
            return new RowGroupPlans(empty, planBatches(workItem, 0), List.of());
        }
        RowGroup rowGroup = workItem.rowGroup();
        RowRanges matchingRows = shared.matchingRows();

        // Apply the tail-read fast path's synthesized matching range here
        // (rather than in `getSharedMetadata`) so the cached metadata stays
        // independent of `tailSkip`. That lets `canFastSkipAllRowGroups`
        // populate the cache before the tail-skip decision is made.
        if (matchingRows.isAll() && tailSkip > 0 && workItem.workItemIndex() == 0) {
            matchingRows = RowRanges.range(tailSkip, rowGroup.numRows());
        }

        // Per-page masks are honoured for this row group only when every
        // decoded column is mask-friendly (e.g., has an OffsetIndex, is flat,
        // or is nested with `DATA_PAGE_V2` pages). Masking a subset of
        // columns would leave sibling columns row-misaligned. When the gate
        // is closed we promote `matchingRows` to ALL so neither plan applies
        // a mask; row-group-level statistics still drop the group when
        // possible, and the row reader applies the residual filter to
        // surviving rows. Row ranges that are empty need no mask: every column gets
        // `FetchPlan.EMPTY` below, so the gate is not probed for them.
        if (!matchingRows.isAll() && !matchingRows.isEmpty()
                && shared.maskCapability().get() == MaskCapability.NO) {
            matchingRows = RowRanges.ALL;
        }

        // Whether a *predicate* narrowed the pages of this row group, which is what
        // PageFilterEvent reports. `shared.matchingRows()` is filter-derived, while
        // the local `matchingRows` also carries the tail-read fast path's synthesized
        // range — pages that range drops were dropped by `tail(N)`, not by the Column
        // Index. Both must be narrow: a closed mask gate promotes the local ranges
        // back to ALL, and then no page is skipped at all.
        boolean pageFilterApplied = !matchingRows.isAll() && !shared.matchingRows().isAll();

        // Convert the iterator-wide maxRows into a per-row-group remainder.
        // `PageLocation.firstRowIndex` and `SequentialFetchPlan.valuesRead`
        // are both row-group-local (reset to 0 each RG), so passing the global
        // maxRows would fail to truncate anything in non-first row groups and
        // over-fetch the last partially-needed RG. With a filter active `head(N)`
        // caps matching rows (SQL LIMIT), so no fetch-side truncation applies —
        // perRgMaxRows returns 0 and the matched-row cap is enforced at the reader.
        long perRgMaxRows = perRgMaxRows(workItem);

        // Parsing the page index and building the plans is CPU work that grows with the projected
        // columns and their pages, so it runs on the pool, which bounds a read's CPU.
        RowRanges planned = matchingRows;
        FetchPlanBuilder.Plans plans = context.compute("fetch plans", () -> fetchPlanBuilder.columnPlans(
                workItem, shared, planned, pageFilterApplied, perRgMaxRows, sequence, fileLength));

        // Last, once nothing above can fail: a speculative plan that fails is planned again on
        // demand, and the batch plan takes each row group once.
        return new RowGroupPlans(plans.columns(), planBatches(workItem, matchingRows.rowCount(rowGroup.numRows())),
                plans.pageFilterEvents());
    }

    /// Inserts the entries of `workItem`'s row group from its chunks' extents, once, where every row
    /// of it is read and nothing can drop it ([#readsAllRows]), so they start before its page index
    /// is read.
    private void insertChunkEntries(WorkItem workItem) throws IOException {
        if (readsAllRows(workItem) && !RangeRead.inMemory(workItem.inputFile())
                && sequence.entries(workItem.workItemIndex()) == null) {
            sequence.insert(workItem.workItemIndex(), workItem.inputFile(),
                    fetchPlanBuilder.chunkSpans(workItem, workItem.inputFile().length()));
        }
    }

    /// Whether every row of `workItem`'s row group is read and no Bloom filter or dictionary can drop
    /// it, so its entries follow from its chunks' extents in the footer alone ([FetchPlanBuilder#chunkSpans]):
    /// no filter narrows its pages, no tail or row limit cuts it, and its chunks are in this file.
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
        BatchPlan plan = batchPlan;
        if (plan == null) {
            return null;
        }
        return plan.plan(workItem.workItemIndex(), workItem.fileIndex(), workItem.inputFile().name(),
                workItem.filterAlwaysMatches(), keptRows);
    }

    /// Per-row-group remainder of the iterator-wide `maxRows` budget.
    ///
    /// Returns `0` (no fetch-side truncation) when `maxRows` is unset, or when a
    /// filter predicate is active: `head(N)` then caps *matching* rows (SQL
    /// LIMIT), so a matching row can sit past the first `N` scanned rows and
    /// every surviving page must remain fetchable. Statistics pushdown still
    /// prunes pages and row groups, and the matched-row cap is enforced at the
    /// reader. Without a filter, returns `max(0, maxRows - workItem.rowsConsumedBefore())`,
    /// which naturally trims the last partially-needed row group's fetch plan
    /// while being a no-op (all pages kept) for fully-needed earlier ones.
    private long perRgMaxRows(WorkItem workItem) {
        if (maxRows <= 0 || filterPredicate != null) {
            return 0;
        }
        long leadingSkip = workItem.workItemIndex() == 0 ? firstRowGroupSkip : 0;
        return Math.max(0, maxRows - workItem.rowsConsumedBefore() + leadingSkip);
    }

    /// Waits for the speculative tasks the read has started, then releases
    /// iterator-local caches. The metadata cache and the files are the parent reader's
    /// ([dev.hardwood.reader.ParquetFileReader]), which closes them; the iterator tells the
    /// parent to stop tracking it, so a closed child reader's work list does not stay
    /// reachable for the parent's whole lifetime.
    ///
    /// The caller closes the columns first, which waits for their fetch steps, so no step starts
    /// a prefetch task while this waits.
    ///
    /// An `Error` speculative work raised is thrown once the iterator is closed.
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        // Starts nothing more, and releases what it holds of the read-ahead buffer; its reads in
        // flight are prefetch tasks, waited for below
        sequence.close();
        Throwable failure = null;
        try {
            // Before the caches are cleared: a planning task fills its cache entry once it has
            // planned, and the files are closed only after this returns.
            // The reads first: a read submits its planning task, which may submit a read again
            // and finds the reads closed
            prefetchTasks.awaitAll();
            planningTasks.awaitAll();
        }
        catch (Error e) {
            failure = e;
        }
        try {
            releaseResources();
        }
        catch (RuntimeException | Error e) {
            if (failure == null) {
                failure = e;
            }
            else {
                failure.addSuppressed(e);
            }
        }
        InputFileCloser.rethrow(failure);
    }

    /// Releases the caches and tells the parent to stop tracking this iterator.
    private void releaseResources() {
        metadataCache.clear();
        arrivalCache.clear();
        fetchPlanCache.clear();
        nextRowGroupPrefetched.clear();
        plannings.clear();
        releaseIndexWindows();

        // The parent stops tracking this iterator, so that a closed child reader's work list does
        // not stay reachable for the parent's whole lifetime.
        closeListener.accept(this);
    }

    // ==================== Internal ====================

    /// Drops the page-index bytes of every window, including those of work items a read that
    /// stopped early never released. The work list stays reachable from the readers.
    private void releaseIndexWindows() {
        planLock.lock();
        try {
            IndexWindow previous = null;
            for (WorkItem workItem : workItems) {
                if (workItem.indexWindow() != previous) {
                    previous = workItem.indexWindow();
                    previous.releaseAll();
                }
            }
        }
        finally {
            planLock.unlock();
        }
    }

    /// Plans one more file, appending its surviving row groups to the work list.
    ///
    /// Returns `false` when there is nothing left to plan — every file done, or
    /// the row budget spent. Planning a file opens its footer, checks its schema
    /// against the reference, and with a filter probes its bloom filters, so a file
    /// is planned when the read reaches it rather than when the reader was built.
    /// See #1107. Dictionaries are read later, when the read reaches each row group.
    ///
    /// A failure is sticky: once planning a file has thrown, every later call rethrows it.
    /// Each column's work items are planned for it, and a column that found nothing left
    /// to plan would end its stream cleanly while the column that hit the failure has yet
    /// to report it, so the read could end empty without an exception (#1278). Retrying the
    /// file is no option either: planning it has already advanced the shared planning state,
    /// the next file to plan, the row budget and the rows still to skip among them.
    private boolean planNextFile() throws IOException {
        planLock.lock();
        try {
            rethrowPlanningFailure();
            int plannedBefore = workItems.size();
            try {
                return planFile();
            }
            catch (IOException | RuntimeException | Error e) {
                planningFailure = e;
                workItemsBeforeFailure = plannedBefore;
                // No file after the failed one is planned, so no footer is read for one
                sequence.stopFooters();
                throw e;
            }
        }
        finally {
            planLock.unlock();
        }
    }

    /// Rethrows the failure that ended planning, if any, as the type it was raised as.
    ///
    /// An `Error` is kept as well: the column's fetcher reports one to its exchange like any
    /// other failure, and a column handed `null` past it would end the read just as silently.
    ///
    /// The same instance reaches every column, and the column never alters it: naming
    /// where the read was, or retyping it, makes a new instance. The one change made in place
    /// is a later failure attached with `addSuppressed`, which is thread-safe.
    private void rethrowPlanningFailure() throws IOException {
        if (planningFailure instanceof IOException io) {
            throw io;
        }
        if (planningFailure instanceof RuntimeException re) {
            throw re;
        }
        if (planningFailure instanceof Error err) {
            throw err;
        }
    }

    private boolean planFile() throws IOException {
        if (nextFileToPlan >= inputFiles.size() || planRowBudget <= 0) {
            return false;
        }
        int fileIndex = nextFileToPlan++;
        PreparedFile prepared = getPreparedFile(fileIndex);
        // Pruning by statistics, the schema checks and the index windows grow with the file's row
        // groups, so they run on the pool, which bounds a read's CPU; the footer is all that waits
        try {
            context.compute("file plan", () -> {
                try {
                    return planRowGroups(fileIndex, prepared);
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
        catch (UncheckedIOException e) {
            // Wrapped only to leave the supplier, and undone in the frame that can declare it
            throw ExceptionContext.unwrap(e);
        }
        // The footers the planned file brings within reach
        sequence.pump();
        return true;
    }

    /// Appends the work items of `prepared`'s row groups that pruning keeps, within the read's row
    /// budget, to the work list. Called under [#planLock], on the pool.
    private Void planRowGroups(int fileIndex, PreparedFile prepared) throws IOException {
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
            if (workItems.isEmpty() && planned.isEmpty()) {
                firstRowGroupSkip = leadingSkip;
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
                    workItems.size() + planned.size(),
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

        appendWithIndexWindows(planned);
        return null;
    }

    /// Appends one file's planned work items to the work list, each with the [IndexWindow] it
    /// falls into.
    private void appendWithIndexWindows(List<WorkItem> planned) {
        if (planned.isEmpty()) {
            return;
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
        int next = 0;
        for (IndexWindow window : windows) {
            for (int m = 0; m < window.memberCount(); m++) {
                WorkItem workItem = planned.get(next++);
                workItemRefCounts.put(workItem.workItemIndex(),
                        new AtomicInteger(projection.decoded().getProjectedColumnCount()));
                workItems.add(workItem.withIndexWindow(window));
            }
        }
    }

    /// Whether every chunk of `rowGroup` stores its data in the file being read.
    private static boolean storedInThisFile(RowGroup rowGroup) {
        for (ColumnChunk chunk : rowGroup.columns()) {
            if (!chunk.filePath().isEmpty()) {
                return false;
            }
        }
        return true;
    }

    /// The planning of the work item at `index`: the work item with its fetch plans computed, or
    /// `null` once the read is done. A read that plans on the step ([PlanningMode]) plans on the
    /// calling thread, and the planning returned is complete. Any other read plans each work item once, on
    /// a virtual thread of the read's prefetch tasks, since planning waits for reads: the next
    /// file's footer, the row group's Bloom filters, dictionaries and page index; a fetch step that
    /// gets the planning before it completes yields on it.
    WorkItemPlanning planning(int index) {
        sequence.requested(index);
        if (planningMode != PlanningMode.VIRTUAL_THREAD) {
            WorkItemPlanning planning = new WorkItemPlanning();
            planning.run(p -> plan(index, p), prefetchTasks);
            return planning;
        }
        WorkItemPlanning planning = plannings.get(index);
        if (planning != null) {
            return planning;
        }
        WorkItemPlanning mine = new WorkItemPlanning();
        planning = plannings.putIfAbsent(index, mine);
        if (planning != null) {
            return planning;
        }
        prefetchTasks.submit(() -> mine.run(p -> plan(index, p), prefetchTasks),
                () -> mine.drop(new IOException("The read was closed before work item " + index + " was planned")));
        return mine;
    }

    /// The work item at `index` with its fetch plans computed, `null` once the read is done.
    private WorkItem plan(int index, WorkItemPlanning planning) throws IOException {
        WorkItem workItem = workItemAt(index);
        if (workItem != null) {
            planning.resolved(workItem);
            rowGroupPlans(workItem);
        }
        return workItem;
    }

    /// The work item at `index`, planning further files if the read has reached
    /// past what is planned. `null` once the read is done.
    ///
    /// Synchronized because every projected column walks this list through its
    /// plannings, and a planning past the planned end plans another file.
    /// Planning mutates shared state, so exactly one column may do it at a time;
    /// the others see the finished result, or the failure that ended planning.
    ///
    /// A work item of a file planned before the failed one is still handed out: the column
    /// whose prefetch planned the failed file can be ahead of the others, and a column
    /// failed short of it would return fewer rows of those files than the others.
    public WorkItem workItemAt(int index) throws IOException {
        planLock.lock();
        try {
            if (planningFailure == null) {
                ensurePlannedThrough(index);
            }
            else if (index >= workItemsBeforeFailure) {
                rethrowPlanningFailure();
            }
            WorkItem workItem = index < workItems.size() ? workItems.get(index) : null;
            if (workItem != null) {
                sequence.reachedFile(workItem.fileIndex());
            }
            // The read has reached the file of the work item, or the end
            reportFiles(workItem != null ? workItem.fileIndex() : Integer.MAX_VALUE);
            return workItem;
        }
        finally {
            planLock.unlock();
        }
    }

    /// Plans until `index` exists in the work list, or nothing is left to plan.
    private void ensurePlannedThrough(int index) throws IOException {
        planLock.lock();
        try {
            while (workItems.size() <= index) {
                // The read waits for this file: the footers after it are read ahead of it
                sequence.reachedFile(nextFileToPlan);
                if (!planNextFile()) {
                    break;
                }
            }
        }
        finally {
            planLock.unlock();
        }
    }

    /// Plans every remaining file. Needed by the questions that are about the
    /// whole read rather than the next row group — the full work list
    /// ([#getWorkItems]) and whether every row group supports per-page masking
    /// ([#canFastSkipAllRowGroups]).
    private void ensureFullyPlanned() throws IOException {
        planLock.lock();
        try {
            while (planNextFile()) {
                // planNextFile appends
            }
            reportFiles(Integer.MAX_VALUE);
        }
        finally {
            planLock.unlock();
        }
    }

    /// Gets or loads file-wide metadata. Projection-specific validation remains with the
    /// read ([SchemaCompatibility]).
    private PreparedFile getPreparedFile(int fileIndex) throws IOException {
        // The checked accessor, because the one caller — [#planNextFile] — can say so. Its
        // sibling exists for the callers that cannot: a mapping function, a `Runnable`, a
        // `CompletableFuture` body. Nothing about a footer read is different on this path,
        // only what the frame above it is allowed to declare.
        return fileMetadataCache.getFile(fileIndex);
    }

    /// The read's footer entries, which its [FileMetadataCache] holds: a footer read ahead tells the
    /// planning chain it has arrived.
    private FetchSequence.Footers footers(FileMetadataCache cache) {
        return new FetchSequence.Footers() {
            @Override
            public CompletableFuture<?> start(int fileIndex) {
                return cache.prefetch(fileIndex, RowGroupIterator.this::advancePlanning);
            }

            @Override
            public void drop(int fileIndex) {
                cache.dropTail(fileIndex);
            }
        };
    }

    /// The files after the one it has reached whose footers a read reads ahead: up to
    /// [FetchSequence#maxFilesAhead] for a multi-file read of files not all in memory whose read
    /// plans ahead, and otherwise only the next file: a read of files all in memory, whose reads
    /// wait for nothing to overlap, or with a row limit, which may need no further file and under
    /// a filter learns only downstream that it is satisfied.
    private int filesAhead(long maxRows) {
        if (inputFiles.size() < 2 || maxRows > 0 || !planningMode.plansAhead()
                || inputFiles.stream().allMatch(RangeRead::inMemory)) {
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
