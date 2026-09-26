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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.predicate.BoundsReadability;
import dev.hardwood.internal.predicate.FilterDecision;
import dev.hardwood.internal.predicate.LogContext;
import dev.hardwood.internal.predicate.PageDropPredicates;
import dev.hardwood.internal.predicate.PageFilterEvaluator;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.predicate.RowGroupBloomFilterSource;
import dev.hardwood.internal.predicate.RowGroupFilterEvaluator;
import dev.hardwood.internal.predicate.dictionary.RowGroupDictionaryFilterSource;
import dev.hardwood.internal.reader.FileMetadataCache.PreparedFile;
import dev.hardwood.internal.schema.FixedWidthValidator;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.internal.schema.ReadProjection;
import dev.hardwood.internal.thrift.OffsetIndexReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.jfr.PageFilterEvent;
import dev.hardwood.jfr.RowGroupDictionaryFilterEvent;
import dev.hardwood.jfr.RowGroupFilterEvent;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.metadata.PageType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.SchemaIncompatibleException;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// Shared iterator over `(InputFile, RowGroup)` pairs across one or more files.
///
/// Handles schema validation, row-group filtering by statistics,
/// `maxRows` limiting at the row-group level, and async prefetching of the next file.
///
/// Each [PageSource] maintains its own cursor into the work list exposed by
/// this iterator. Shared per-row-group metadata (index buffers, matching rows)
/// is cached for the current file and reused across columns.
public class RowGroupIterator implements Closeable {

    private static final System.Logger LOG = System.getLogger(RowGroupIterator.class.getName());

    private static final Consumer<RowGroupIterator> NO_CLOSE_LISTENER = iterator -> {
    };

    private final List<InputFile> inputFiles;
    private final FileMetadataCache fileMetadataCache;
    private final boolean ownsFileMetadataCache;
    private final Consumer<RowGroupIterator> closeListener;

    /// [#close()] can be reached more than once, for instance through a
    /// column-reader scan that its group and every reader of that group close.
    /// Closing twice releases nothing twice. Volatile because the prefetch tasks,
    /// which [#close()] waits for after setting it, read it.
    private volatile boolean closed;
    /// The speculative tasks this iterator's read has started; [#close()] waits for them.
    private final PrefetchTasks prefetchTasks = new PrefetchTasks();
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
    private ResolvedPredicate filterPredicate;
    /// Planning state, carried between files because a file is planned when the
    /// read reaches it rather than all at once.
    private int nextFileToPlan;
    private long planRowBudget;
    private long plannedRows;
    private long planSkipRemaining;
    private boolean metadataFilteringEnabled = true;
    /// The failure that ended planning, rethrown to every later request for a work item
    /// past the files planned before it. See [#planNextFile].
    private Throwable planningFailure;
    /// How many work items the files planned before the failed one make up. The failed
    /// file's own row groups, appended before it failed, lie past it and are not handed out.
    private int workItemsBeforeFailure;

    /// Reference schema leaf ordinals this read touches: every projected column plus
    /// every column the filter tests — a predicate column need not be projected, but
    /// pruning still indexes into each file's metadata for it. Exactly the set that
    /// is validated per file and that [FileColumnOrdinals] resolves an ordinal for.
    private BitSet touchedColumns;

    /// AND-necessary leaves per column index, derived once from `filterPredicate`.
    /// Feeds [SequentialFetchPlan]'s inline-stats page-drop check.
    private Map<Integer, List<ResolvedPredicate>> dropLeavesByColumn = Map.of();

    // Work list: all (file, rowGroup) pairs to process, built during initialize()
    private final List<WorkItem> workItems = new ArrayList<>();

    // Per-row-group shared metadata cache (keyed by work item index)
    private final ConcurrentHashMap<Integer, SharedRowGroupMetadata> metadataCache = new ConcurrentHashMap<>();

    // Per-row-group fetch plans cache (keyed by work item index).
    private final ConcurrentHashMap<Integer, FetchPlan[]> fetchPlanCache = new ConcurrentHashMap<>();

    // Number of projected columns still referencing each work item. Initialized to
    // projectedColumnCount in initialize(); each PageSource calls releaseWorkItem
    // when it advances past a work item, and on zero we evict the metadata and
    // fetch-plan caches for that index. Prevents unbounded retention of fetched
    // chunk bytes for the lifetime of the iterator (matters most for remote I/O,
    // where ChunkHandle.data is heap-allocated rather than an mmap slice).
    /// One counter per work item, added as the item is planned. Keyed rather
    /// than indexed because the work list grows, and read from every column's
    /// retriever thread without the planning lock.
    private final Map<Integer, AtomicInteger> workItemRefCounts = new ConcurrentHashMap<>();

    /// Work items whose first demand access has started the next row group's prefetch, held
    /// until every column releases the item. The next row group is prefetched once per work
    /// item, whichever column reaches the item first and whether or not the previous
    /// prefetch planned it.
    private final Set<Integer> nextRowGroupPrefetched = ConcurrentHashMap.newKeySet();

    /// A single unit of work: one row group in one file.
    ///
    /// `rowsConsumedBefore` is the cumulative row count of all work items
    /// preceding this one in the work list — used to convert the iterator-wide
    /// `maxRows` budget into a per-row-group remainder when computing fetch
    /// plans. (Filter predicates invalidate this correlation, so callers must
    /// ignore it when a filter is active.)
    ///
    /// `leafDecisions` holds what statistics and bloom filters decided for each
    /// leaf of the filter, for the row group's dictionaries to sharpen once the
    /// read reaches it; `null` when no metadata filtering applies.
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
            RowGroupFilterEvaluator.LeafDecisions leafDecisions
    ) {}

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
    /// pruning read none. A row group they prove holds no match is
    /// `droppedByDictionary`, and nothing else is read for it.
    public record SharedRowGroupMetadata(
            RowGroupIndexBuffers indexBuffers,
            RowRanges matchingRows,
            LazyMaskCapability maskCapability,
            RowGroupDictionaryFilterSource dictionaries,
            boolean droppedByDictionary
    ) {}

    private List<RowGroup> firstFileRowGroups;

    /// Creates a RowGroupIterator for the given files.
    ///
    /// @param inputFiles one or more input files (must not be empty)
    /// @param context the Hardwood context
    /// @param maxRows maximum rows to read (0 = unlimited)
    public RowGroupIterator(List<InputFile> inputFiles, HardwoodContextImpl context, long maxRows) {
        this(inputFiles, context, maxRows, 0);
    }

    /// Creates a RowGroupIterator with a tail-skip budget applied to the first
    /// row group.
    ///
    /// @param inputFiles one or more input files (must not be empty)
    /// @param context the Hardwood context
    /// @param maxRows maximum rows to read (0 = unlimited)
    /// @param tailSkip leading rows of the first row group to skip via per-page
    ///        masking (0 = unused). Caller must guarantee every projected column
    ///        in every subset row group has an OffsetIndex; otherwise sequential
    ///        columns would emit unmaskable rows from offset 0 and break
    ///        cross-column alignment.
    public RowGroupIterator(List<InputFile> inputFiles, HardwoodContextImpl context,
                            long maxRows, long tailSkip) {
        this(inputFiles, context, maxRows, tailSkip, 0);
    }

    /// Creates a RowGroupIterator with tail-skip and physical-skip budgets.
    ///
    /// @param inputFiles one or more input files (must not be empty)
    /// @param context the Hardwood context
    /// @param maxRows maximum rows to read (0 = unlimited)
    /// @param tailSkip leading rows of the first row group to skip via per-page
    ///        masking (0 = unused)
    /// @param physicalSkip leading physical rows to skip while building the
    ///        work list (0 = unused). Whole row groups are dropped; the residue
    ///        within the first kept row group is exposed via [#firstRowGroupSkip()].
    ///        Mutually exclusive with `tailSkip` — one masks the first row group's
    ///        leading rows, the other drops leading row groups, with no combined
    ///        semantics.
    public RowGroupIterator(List<InputFile> inputFiles, HardwoodContextImpl context,
                            long maxRows, long tailSkip, long physicalSkip) {
        this(new FileMetadataCache(inputFiles), true, NO_CLOSE_LISTENER, context,
                maxRows, tailSkip, physicalSkip);
    }

    /// Creates a RowGroupIterator sharing file metadata with its parent reader.
    ///
    /// `closeListener` is invoked with this iterator once [#close()] has released
    /// its caches, so the parent can stop tracking an iterator its child reader
    /// has already torn down.
    public RowGroupIterator(FileMetadataCache fileMetadataCache,
                            Consumer<RowGroupIterator> closeListener,
                            HardwoodContextImpl context,
                            long maxRows, long tailSkip, long physicalSkip) {
        this(fileMetadataCache, false, closeListener, context, maxRows, tailSkip, physicalSkip);
    }

    private RowGroupIterator(FileMetadataCache fileMetadataCache, boolean ownsFileMetadataCache,
                             Consumer<RowGroupIterator> closeListener,
                             HardwoodContextImpl context, long maxRows, long tailSkip,
                             long physicalSkip) {
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
        this.ownsFileMetadataCache = ownsFileMetadataCache;
        this.closeListener = closeListener;
        this.inputFiles = fileMetadataCache.inputFiles();
        this.context = context;
        this.maxRows = maxRows;
        this.tailSkip = tailSkip;
        this.physicalSkip = physicalSkip;
    }

    /// Returns the maximum rows limit (0 = unlimited).
    public long maxRows() {
        return maxRows;
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

    /// Applies column projection and optional filter, builds the full work list.
    /// Statistics-based filtering stays enabled.
    ///
    /// @param projection column projection
    /// @param filter resolved predicate, or `null` for no filtering
    /// @return the projected schema
    public ProjectedSchema initialize(ColumnProjection projection, ResolvedPredicate filter) throws IOException {
        return initialize(projection, filter, true);
    }

    /// Applies column projection and optional filter, builds the full work list.
    ///
    /// @param projection column projection
    /// @param filter resolved predicate, or `null` for no filtering
    /// @param metadataFilteringEnabled when `false`, the filter takes no
    ///        metadata-derived shortcut — no row-group pruning from statistics,
    ///        bloom filters or dictionaries, no page-index or
    ///        inline-page-statistics skipping, no always-match decision — so the
    ///        predicate is evaluated against every decoded row
    /// @return the projected schema
    public ProjectedSchema initialize(ColumnProjection projection, ResolvedPredicate filter,
                                      boolean metadataFilteringEnabled) throws IOException {
        return initialize(ProjectedSchema.create(referenceSchema, projection), filter,
                metadataFilteringEnabled);
    }

    /// Applies a pre-built projected schema and optional filter, builds the full work list.
    /// Statistics-based filtering stays enabled.
    ///
    /// @param projected pre-built projected schema
    /// @param filter resolved predicate, or `null` for no filtering
    /// @return the projected schema (same as input)
    public ProjectedSchema initialize(ProjectedSchema projected, ResolvedPredicate filter) throws IOException {
        return initialize(projected, filter, true);
    }

    /// Applies a pre-built projected schema and optional filter, builds the full work list.
    /// Every projected column is exposed.
    ///
    /// @param projected pre-built projected schema
    /// @param filter resolved predicate, or `null` for no filtering
    /// @param metadataFilteringEnabled when `false`, the filter takes no
    ///        metadata-derived shortcut, so the predicate is evaluated against
    ///        every decoded row (see the sibling three-arg overload)
    /// @return the projected schema (same as input)
    public ProjectedSchema initialize(ProjectedSchema projected, ResolvedPredicate filter,
                                      boolean metadataFilteringEnabled) throws IOException {
        return initialize(ReadProjection.of(projected), filter, metadataFilteringEnabled);
    }

    /// Applies a read projection and optional filter, builds the full work list. The
    /// iterator plans [ReadProjection#decoded()], and knows which of its columns are
    /// filter-only.
    ///
    /// @param projection the exposed and decoded columns
    /// @param filter resolved predicate, or `null` for no filtering
    /// @param metadataFilteringEnabled as for [#initialize(ProjectedSchema, ResolvedPredicate, boolean)]
    /// @return the decoded projection
    public ProjectedSchema initialize(ReadProjection projection, ResolvedPredicate filter,
                                      boolean metadataFilteringEnabled) throws IOException {
        if (referenceSchema == null) {
            throw new IllegalStateException("openFirst() must be called before initialize()");
        }
        ProjectedSchema projected = projection.decoded();
        this.metadataFilteringEnabled = metadataFilteringEnabled;
        this.projection = projection;
        this.filterPredicate = filter;
        this.touchedColumns = touchedColumns(projected, filter, referenceSchema.getColumnCount());
        this.dropLeavesByColumn = filter != null && metadataFilteringEnabled
                ? PageDropPredicates.byColumn(filter) : Map.of();

        validateReferenceColumns();

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
        synchronized (this) {
            return List.copyOf(workItems);
        }
    }

    /// Returns the projected schema.
    public ProjectedSchema projectedSchema() {
        return projection != null ? projection.decoded() : null;
    }

    /// Returns the reference schema (from the first file).
    public FileSchema referenceSchema() {
        return referenceSchema;
    }

    /// Returns the filter predicate, or `null` if none.
    public ResolvedPredicate filterPredicate() {
        return filterPredicate;
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
                RowGroupDictionaryFilterSource dictionaries = null;
                if (workItem.leafDecisions() != null) {
                    dictionaries = new RowGroupDictionaryFilterSource(workItem.inputFile(),
                            workItem.rowGroup(), workItem.fileSchema(), context);
                    FilterDecision decision;
                    try (FetchReason.Scope pruning = FetchReason.set(
                            "rg=" + workItem.rowGroupIndex() + " pruning")) {
                        decision = RowGroupFilterEvaluator.refineWithDictionaries(
                                workItem.columnOrdinals().filter(), workItem.rowGroup(),
                                workItem.leafDecisions(), dictionaries);
                    }
                    if (decision == FilterDecision.CANNOT_MATCH) {
                        emitDictionaryDropEvent(workItem);
                        return new SharedRowGroupMetadata(null, RowRanges.ALL,
                                LazyMaskCapability.of(MaskCapability.YES), null, true);
                    }
                }
                boolean pageFiltering = filterPredicate != null && metadataFilteringEnabled;
                RowGroupIndexBuffers indexBuffers = RowGroupIndexBuffers.fetch(
                        workItem.inputFile(), workItem.rowGroup(),
                        pageFiltering);

                RowRanges matchingRows = RowRanges.ALL;
                if (pageFiltering) {
                    matchingRows = PageFilterEvaluator.computeMatchingRows(
                            workItem.columnOrdinals().filter(), workItem.rowGroup(), indexBuffers,
                            new LogContext(workItem.inputFile().name(), workItem.rowGroupIndex()),
                            workItem.columnOrdinals().boundsReadability());
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

    /// Runs the page-format probe behind the row-group-wide mask gate, see
    /// [#masksApplicableForRowGroup].
    private MaskCapability probeMaskCapability(WorkItem workItem) throws IOException {
        try (FetchReason.Scope ignored = FetchReason.set(
                "rg=" + workItem.rowGroupIndex() + " indexes")) {
            return masksApplicableForRowGroup(
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

    /// Reports a row group its dictionaries dropped once the read reached it.
    private static void emitDictionaryDropEvent(WorkItem workItem) {
        RowGroupDictionaryFilterEvent event = new RowGroupDictionaryFilterEvent();
        event.file = workItem.inputFile().name();
        event.rowGroupIndex = workItem.rowGroupIndex();
        event.commit();
    }

    /// Fails unless every chunk of the work item's row group stores its data in the file being
    /// read.
    ///
    /// This is the first thing [#getSharedMetadata] does, so it precedes every read the row group
    /// drives: the page index fetched right below, the dictionary and bloom-filter reads that
    /// prune it, and the fetch plans built from it. The index region alone would already be wrong
    /// — [RowGroupIndexBuffers#fetch] spans the offsets of *all* the row group's columns, so one
    /// chunk pointing elsewhere misplaces the region for the rest.
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

    /// Returns the [FetchPlan] for the given column in the given row group.
    /// Plans are computed once per row group (on first access) and cached.
    ///
    /// @param workItem the work item identifying the row group
    /// @param projectedColumnIndex the projected column index
    /// @return a fetch plan for iterating pages with lazy byte fetching
    public FetchPlan getColumnPlan(WorkItem workItem, int projectedColumnIndex) throws IOException {
        int workItemIndex = workItem.workItemIndex();
        FetchPlan[] plans = fetchPlanCache.get(workItemIndex);
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
        // The first demand access prefetches the next row group, whether it planned this
        // one or found the plans the previous prefetch cached; the prefetch task does not
        // come through here, so prefetch stays one row group ahead. This runs outside the
        // mapping function: prefetching plans the next file when this one is exhausted,
        // which reads a footer and, under a filter, probes bloom filters, and a
        // ConcurrentHashMap holds a bin lock for the whole of a mapping function.
        if (nextRowGroupPrefetched.add(workItemIndex)) {
            prefetchNextRowGroup(workItem);
        }
        return plans[projectedColumnIndex];
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
            metadataCache.remove(idx);
            fetchPlanCache.remove(idx);
            nextRowGroupPrefetched.remove(idx);
        }
    }

    /// Whether every projected column has released `workItem`. [#releaseWorkItem] counts down
    /// before it evicts the work item's caches, as [#close] sets `closed` before it clears them.
    private boolean isReleased(WorkItem workItem) {
        AtomicInteger counter = workItemRefCounts.get(workItem.workItemIndex());
        return counter != null && counter.get() == 0;
    }

    /// Triggers async pre-computation and pre-fetch for the next row group, as a task
    /// [#close()] waits for. The plan computation reads the row group's page index,
    /// and under a filter its dictionaries; the pre-fetch then reads the first chunk.
    ///
    /// The next row group is asked for through [#workItemAt], so the last row group of a
    /// file prefetches into the next one rather than stopping at the boundary — which
    /// is the one step of a read this prefetch exists to cover.
    ///
    /// That call stays on the caller, so that planning the next file is done by the time the
    /// read reaches it. The caller is the retriever, and what it blocks on is the next file's
    /// footer, whose read [#triggerPrefetch] has already started — except under a filter,
    /// where planning also probes that file's bloom filters, which nothing has started.
    ///
    /// A planning failure is not raised here. The iterator keeps it, and every column's own
    /// request for the next work item rethrows it, so each column fails when its read reaches
    /// the failed file, with the failure as planning raised it. Raised here, it would fail
    /// the current row group, which is readable, and be reported as a failure in it.
    ///
    /// Called once per work item, by the first [#getColumnPlan] to reach it, after its
    /// `computeIfAbsent` returns, never from inside it: this blocks, and a mapping function
    /// holds a bin lock for its whole duration. The task below plans the next row group
    /// without going through [#getColumnPlan], so it starts no further prefetch.
    private void prefetchNextRowGroup(WorkItem currentWorkItem) {
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
        prefetchTasks.submit(() -> {
            try (FetchReason.Scope ignored = FetchReason.set(
                    "prefetch rg=" + nextWorkItem.rowGroupIndex())) {
                FetchPlan[] nextPlans = fetchPlanCache.computeIfAbsent(
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
                                LOG.log(System.Logger.Level.DEBUG,
                                        "Prefetch failed for row group {0}",
                                        nextWorkItem.rowGroupIndex(), e);
                                return null;
                            }
                        });
                if (nextPlans == null) {
                    return;
                }
                // Pre-fetch the first chunk of the first plan that reads one
                for (FetchPlan plan : nextPlans) {
                    if (!plan.isEmpty() && !(plan instanceof SkippedColumnFetchPlan)) {
                        plan.prefetch();
                        break;
                    }
                }
            }
        });
    }

    private FetchPlan[] computeFetchPlans(WorkItem workItem) throws IOException {
        SharedRowGroupMetadata shared = getSharedMetadata(workItem);
        int projectedCount = projection.decoded().getProjectedColumnCount();
        if (shared.droppedByDictionary()) {
            FetchPlan[] empty = new FetchPlan[projectedCount];
            Arrays.fill(empty, FetchPlan.EMPTY);
            return empty;
        }
        RowGroup rowGroup = workItem.rowGroup();
        RowRanges matchingRows = shared.matchingRows();
        InputFile inputFile = workItem.inputFile();

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
        // surviving rows.
        if (!matchingRows.isAll() && shared.maskCapability().get() == MaskCapability.NO) {
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

        FetchPlan[] plans = new FetchPlan[projectedCount];

        for (int projCol = 0; projCol < projectedCount; projCol++) {
            int originalIndex = projection.decoded().toOriginalIndex(projCol);
            int fileOrdinal = workItem.columnOrdinals().fileOrdinal(originalIndex);
            ColumnChunk columnChunk = rowGroup.columns().get(fileOrdinal);
            ColumnSchema columnSchema = workItem.fileSchema().getColumn(fileOrdinal);
            ColumnIndexBuffers colBuffers = shared.indexBuffers().forColumn(fileOrdinal);
            Dictionary preloadedDictionary = shared.dictionaries() == null
                    ? null : shared.dictionaries().loaded(fileOrdinal);

            if (skipsColumn(workItem, projCol)) {
                plans[projCol] = SkippedColumnFetchPlan.INSTANCE;
                continue;
            }

            if (colBuffers == null || colBuffers.offsetIndex() == null) {
                // No OffsetIndex — sequential lazy fetching. Per-page drops via
                // inline DataPageHeader.statistics and per-page row masks both
                // happen inside SequentialFetchPlan. Inline page statistics are bounds
                // like any other, so a column whose bounds this file records in an order
                // this reader cannot read hands over no leaves to drop pages with (#1179).
                List<ResolvedPredicate> leaves = workItem.columnOrdinals().boundsReadability().readable(fileOrdinal)
                        ? dropLeavesByColumn.getOrDefault(originalIndex, List.of())
                        : List.of();
                plans[projCol] = SequentialFetchPlan.build(
                        inputFile, columnSchema, columnChunk,
                        context, workItem.rowGroupIndex(), inputFile.name(),
                        perRgMaxRows, leaves, matchingRows, rowGroup.numRows(), preloadedDictionary,
                        shared.dictionaries() == null ? 0 : shared.dictionaries().loadedPageEnd(fileOrdinal),
                        prefetchTasks);
                continue;
            }

            try {
                OffsetIndex offsetIndex = OffsetIndexReader.read(
                        new ThriftCompactReader(colBuffers.offsetIndex()));
                List<PageLocation> allPages = offsetIndex.pageLocations();

                // Determine needed pages (filter + maxRows). Each entry pairs a
                // PageLocation with its PageRowMask so the assembler can keep only
                // the records inside the matching ranges.
                List<NeededPage> neededPages = computeNeededPages(
                        allPages, matchingRows, rowGroup.numRows());

                // Report the page-level filter's effect before `truncateToMaxRows`,
                // so a `head(N)` cap is not counted as pages the predicate skipped,
                // and before the empty-plan shortcut below, so the fully-pruned
                // column — the most effective case — is reported too.
                emitPageFilterEvent(inputFile.name(), workItem.rowGroupIndex(), columnSchema.name(),
                        pageFilterApplied, allPages.size(), neededPages.size());

                if (neededPages.isEmpty()) {
                    plans[projCol] = FetchPlan.EMPTY;
                    continue;
                }

                neededPages = truncateToMaxRows(neededPages, perRgMaxRows);

                // Coalesce needed pages within this column into page groups,
                // bridging small gaps but splitting on large ones.
                // The dictionary page is fetched with the first page group when coalescing
                // would merge the two, and on its own otherwise. A dictionary pruning has
                // read is not fetched at all.
                long firstDataPageOffset = allPages.get(0).offset();
                long dictStart = preloadedDictionary == null
                        ? DictionaryParser.dictionaryPageStart(columnChunk, firstDataPageOffset) : 0;
                boolean foldDictionary = dictStart > 0
                        && foldsDictionary(dictStart, firstDataPageOffset, neededPages.get(0).location());
                List<PageGroup> groups = coalescePages(neededPages, foldDictionary ? dictStart : 0);

                // Create ChunkHandles for each page group, linked for pre-fetch
                List<ChunkHandle> handles = new ArrayList<>(groups.size());
                int groupCount = groups.size();
                for (int g = 0; g < groupCount; g++) {
                    PageGroup group = groups.get(g);
                    String purpose = "rg=" + workItem.rowGroupIndex()
                            + " col=" + originalIndex
                            + " pageGroup=" + (g + 1) + "/" + groupCount;
                    handles.add(new ChunkHandle(inputFile, group.offset, group.length, purpose, prefetchTasks));
                }
                for (int i = 0; i < handles.size() - 1; i++) {
                    handles.get(i).setNextChunk(handles.get(i + 1));
                }
                ChunkHandle dictionaryHandle = null;
                if (dictStart > 0 && !foldDictionary) {
                    dictionaryHandle = new ChunkHandle(inputFile, dictStart,
                            Math.toIntExact(firstDataPageOffset - dictStart),
                            "rg=" + workItem.rowGroupIndex() + " col=" + originalIndex + " dictionary",
                            prefetchTasks);
                    dictionaryHandle.setNextChunk(handles.get(0));
                }

                plans[projCol] = IndexedFetchPlan.build(
                        neededPages, groups, handles, dictionaryHandle, dictStart,
                        firstDataPageOffset,
                        columnSchema, columnChunk,
                        context, workItem.rowGroupIndex(), inputFile.name(), preloadedDictionary);
            }
            catch (ParquetReadException e) {
                throw new ParquetReadException(ExceptionContext.filePrefix(inputFile.name())
                        + "Failed to compute fetch plan for column " + projCol
                        + " in row group " + workItem.rowGroupIndex() + ": " + e.getMessage(), e);
            }
        }

        coalesceAcrossColumns(plans, inputFile, workItem);

        return plans;
    }

    /// Whether the column at `projCol` is not read in `workItem`'s row group: the predicate
    /// references it, the projection does not, and statistics proved every row of the row
    /// group matches, so nothing evaluates its values there.
    ///
    /// This is exactly the condition under which the consumers take no batch of the column
    /// for the row group, so the two must not diverge: a column read where no consumer takes
    /// its batches would hand its rows to the next undecided step. Page masks and row caps on
    /// the sibling columns do not enter into it, since the skipped column assembles no rows.
    private boolean skipsColumn(WorkItem workItem, int projCol) {
        return workItem.filterAlwaysMatches() && projection.isFilterOnly(projCol);
    }

    /// Coalesces the *first* read of multiple columns within this row group
    /// into a smaller number of larger ranged GETs. See #374.
    ///
    /// Conservative scope: only coalesces plans that report
    /// [CoalescableFirstChunk#isCoalesceSafe] as true, i.e. those whose
    /// first chunk represents the entire byte range the column will read.
    /// Plans with page drops keep their per-column reads — coalescing
    /// across an intra-column drop would over-fetch the dropped bytes
    /// into the shared region *and* later re-fetch the column's
    /// non-first page groups via the per-column chain (double-fetch).
    /// IndexedFetchPlans with a single page group qualify;
    /// SequentialFetchPlans qualify when their chunk size covers the
    /// entire column chunk (`head(N)` truncation may shrink it below).
    private void coalesceAcrossColumns(FetchPlan[] plans, InputFile inputFile, WorkItem workItem) {
        // Collect (offset, length, plan-index) for each plan's first read.
        record Entry(int planIndex, long offset, int length) {}
        // What each plan left out fetches on its own. A region must not bridge a gap holding
        // any of it, or those bytes would be fetched twice: once in the region, once by the
        // plan's own handles.
        record Extent(long start, long end) {}
        List<Entry> entries = new ArrayList<>(plans.length);
        List<Extent> fetchedAlone = new ArrayList<>();
        for (int i = 0; i < plans.length; i++) {
            FetchPlan plan = plans[i];
            if (plan instanceof CoalescableFirstChunk c) {
                if (c.isCoalesceSafe()) {
                    entries.add(new Entry(i, c.firstChunkOffset(), c.firstChunkLength()));
                }
                else if (c.readEnd() > c.readStart()) {
                    fetchedAlone.add(new Extent(c.readStart(), c.readEnd()));
                }
            }
        }
        if (entries.size() < 2) {
            return;
        }
        entries.sort(Comparator.comparingLong(Entry::offset));

        // Greedy walk: accumulate entries into a region while the coalescing
        // policy merges each into it and the gap holds nothing fetched alone.
        List<List<Entry>> regionEntries = new ArrayList<>();
        List<Entry> current = new ArrayList<>();
        long currentEnd = -1;
        long currentStart = -1;
        for (Entry e : entries) {
            long gapEnd = e.offset();
            long gapStart = currentEnd;
            if (current.isEmpty()
                    || (CoalescingPolicy.merges(currentStart, currentEnd, e.offset(), e.offset() + e.length())
                            && fetchedAlone.stream().noneMatch(x -> x.start() < gapEnd && gapStart < x.end()))) {
                if (current.isEmpty()) {
                    currentStart = e.offset();
                }
                current.add(e);
                currentEnd = e.offset() + e.length();
            }
            else {
                regionEntries.add(current);
                current = new ArrayList<>();
                current.add(e);
                currentStart = e.offset();
                currentEnd = e.offset() + e.length();
            }
        }
        if (!current.isEmpty()) {
            regionEntries.add(current);
        }

        // Skip the rewrite if every "region" is just one column — coalescing
        // would buy nothing.
        boolean anyMerged = regionEntries.stream().anyMatch(g -> g.size() > 1);
        if (!anyMerged) {
            return;
        }

        // Build SharedRegions and rewrite each merged plan's first ChunkHandle
        // to slice from the region. Single-entry "regions" are left alone.
        SharedRegion[] regionsByPlan = new SharedRegion[plans.length];
        SharedRegion previous = null;
        for (List<Entry> group : regionEntries) {
            if (group.size() == 1) {
                continue;
            }
            long regionOffset = group.get(0).offset();
            long regionEnd = group.get(group.size() - 1).offset()
                    + group.get(group.size() - 1).length();
            int regionLength = Math.toIntExact(regionEnd - regionOffset);
            String purpose = "rg=" + workItem.rowGroupIndex()
                    + " region=" + group.get(0).planIndex() + ".." + group.get(group.size() - 1).planIndex();
            SharedRegion region = new SharedRegion(inputFile, regionOffset, regionLength, purpose, prefetchTasks);
            if (previous != null) {
                previous.setNextRegion(region);
            }
            previous = region;
            for (Entry e : group) {
                regionsByPlan[e.planIndex()] = region;
            }
        }

        for (int i = 0; i < plans.length; i++) {
            SharedRegion region = regionsByPlan[i];
            if (region == null) {
                continue;
            }
            ((CoalescableFirstChunk) plans[i]).attachSharedRegion(region, workItem.rowGroupIndex());
        }
    }

    /// Plans that expose their first read's byte range and accept a
    /// region-backed [ChunkHandle] for it.
    interface CoalescableFirstChunk {
        long firstChunkOffset();
        int firstChunkLength();

        /// Returns true when the plan's first chunk is the only chunk
        /// (i.e. no later page groups will be fetched per-column).
        /// When false, cross-column coalescing skips this plan: bridging
        /// to a neighbour column would over-fetch the bytes between this
        /// plan's first chunk and its later chunks (e.g. dropped pages)
        /// into the shared region, *and* the per-column chain would later
        /// re-fetch those same later chunks.
        boolean isCoalesceSafe();

        /// First byte of everything the plan may fetch.
        long readStart();

        /// End, exclusive, of everything the plan may fetch; [#readStart] when it fetches
        /// nothing.
        long readEnd();

        void attachSharedRegion(SharedRegion region, int rowGroupIndex);
    }

    /// A contiguous byte range covering one or more pages within a column.
    record PageGroup(long offset, int length, int firstPageIndex, int pageCount) {}

    /// A page needed for the current read paired with the [PageRowMask] selecting
    /// which records within the page the assembler should keep. The mask is
    /// [PageRowMask#ALL] when no filter is active or when the page falls
    /// entirely inside the matching ranges.
    ///
    /// `pageIndex` is the page's ordinal in the whole column chunk, not its position in
    /// this list: a filter drops pages, and a failure that named the position would send
    /// a reader to a page the offset index numbers differently.
    record NeededPage(PageLocation location, PageRowMask mask, int pageIndex) {}

    /// Whether the dictionary page, `[dictStart, firstDataPageOffset)`, is fetched with the first
    /// needed page rather than on its own.
    static boolean foldsDictionary(long dictStart, long firstDataPageOffset, PageLocation firstNeeded) {
        return CoalescingPolicy.merges(dictStart, firstDataPageOffset,
                firstNeeded.offset(), firstNeeded.offset() + firstNeeded.compressedPageSize());
    }

    /// Coalesces needed pages within a column into page groups under [CoalescingPolicy].
    /// The first group is extended back to `dictStart` to take in the dictionary
    /// page, unless `dictStart` is `0`.
    static List<PageGroup> coalescePages(List<NeededPage> neededPages, long dictStart) {
        List<PageGroup> groups = new ArrayList<>();
        PageLocation firstPage = neededPages.get(0).location();
        long groupStart = firstPage.offset();
        long groupEnd = groupStart + firstPage.compressedPageSize();
        int groupFirstPage = 0;
        int groupPageCount = 1;

        // Extend first group backwards to include dictionary prefix
        if (dictStart > 0 && dictStart < groupStart) {
            groupStart = dictStart;
        }

        for (int i = 1; i < neededPages.size(); i++) {
            PageLocation page = neededPages.get(i).location();

            if (CoalescingPolicy.merges(groupStart, groupEnd, page.offset(),
                    page.offset() + page.compressedPageSize())) {
                groupEnd = page.offset() + page.compressedPageSize();
                groupPageCount++;
            }
            else {
                groups.add(new PageGroup(groupStart,
                        Math.toIntExact(groupEnd - groupStart),
                        groupFirstPage, groupPageCount));
                groupStart = page.offset();
                groupEnd = groupStart + page.compressedPageSize();
                groupFirstPage = i;
                groupPageCount = 1;
            }
        }

        groups.add(new PageGroup(groupStart,
                Math.toIntExact(groupEnd - groupStart),
                groupFirstPage, groupPageCount));

        return groups;
    }

    /// Determines which pages are needed based on the filter's matching row ranges,
    /// pairing each kept page with the [PageRowMask] that selects which of its
    /// records the assembler should keep.
    private static List<NeededPage> computeNeededPages(List<PageLocation> allPages,
                                                       RowRanges matchingRows,
                                                       long rowGroupRowCount) {
        if (matchingRows.isAll()) {
            List<NeededPage> needed = new ArrayList<>(allPages.size());
            for (int i = 0; i < allPages.size(); i++) {
                needed.add(new NeededPage(allPages.get(i), PageRowMask.ALL, i));
            }
            return needed;
        }
        List<NeededPage> needed = new ArrayList<>();
        for (int i = 0; i < allPages.size(); i++) {
            long pageFirstRow = allPages.get(i).firstRowIndex();
            long pageLastRow = (i + 1 < allPages.size())
                    ? allPages.get(i + 1).firstRowIndex()
                    : rowGroupRowCount;
            PageRowMask mask = matchingRows.maskForPage(pageFirstRow, pageLastRow);
            if (mask != null) {
                needed.add(new NeededPage(allPages.get(i), mask, i));
            }
        }
        return needed;
    }

    /// Emits a [PageFilterEvent] for one column chunk whose pages were narrowed by
    /// Column Index push-down.
    ///
    /// Nothing is emitted unless a predicate actually narrowed this row group's pages:
    /// a read with no filter, a `tail(N)` whose synthesized range is the only thing
    /// narrowing the pages, and a row group whose mask gate is closed all skip nothing
    /// by push-down and so report nothing. The absence of the event is therefore the
    /// signal that no page was a candidate for skipping, and a `pagesSkipped` of 0
    /// means the filter ran and kept everything.
    private static void emitPageFilterEvent(String fileName, int rowGroupIndex, String column,
                                            boolean pageFilterApplied, int totalPages, int pagesKept) {
        if (!pageFilterApplied) {
            return;
        }
        PageFilterEvent event = new PageFilterEvent();
        event.file = fileName;
        event.rowGroupIndex = rowGroupIndex;
        event.column = column;
        event.totalPages = totalPages;
        event.pagesKept = pagesKept;
        event.pagesSkipped = totalPages - pagesKept;
        event.commit();
    }

    /// Whether per-page row masks may be applied by the plans of this row
    /// group. Returns `true` iff every decoded column (the projected columns
    /// and any filter-only column, see [ReadProjection]) is one of:
    ///
    /// - backed by an OffsetIndex (its plan is an [IndexedFetchPlan], which
    ///   honours masks);
    /// - flat (`maxRepetitionLevel == 0`), where a [SequentialFetchPlan]
    ///   needs no rep-level walk to translate per-page row masks into
    ///   per-page record counts;
    /// - nested with `DATA_PAGE_V2` data pages, whose repetition levels live
    ///   in an uncompressed prefix and can be walked without invoking the
    ///   codec.
    ///
    /// A nested column lacking an OffsetIndex with `DATA_PAGE` (v1) data
    /// pages closes the gate for the whole row group: counting records there
    /// would require decompressing the page body, defeating the
    /// skip-without-decompress optimisation. When the gate is closed,
    /// `matchingRows` is promoted to [RowRanges#ALL] for the row group so no
    /// plan applies a mask — preserving cross-column row alignment instead
    /// of masking only the columns we know how to handle.
    ///
    /// Performs at most one bounded page-header read per nested-without-
    /// OffsetIndex column to detect v1 vs v2; files where every decoded
    /// column has an OffsetIndex (parquet-mr default since 1.11) pay no I/O
    /// here.
    public static boolean masksApplicableForRowGroup(ProjectedSchema projectedSchema,
                                                      RowGroup rowGroup, FileSchema fileSchema,
                                                      FileColumnOrdinals columnOrdinals,
                                                      InputFile inputFile) throws IOException {
        int projectedCount = projectedSchema.getProjectedColumnCount();
        for (int p = 0; p < projectedCount; p++) {
            int fileOrdinal = columnOrdinals.fileOrdinal(projectedSchema.toOriginalIndex(p));
            ColumnChunk columnChunk = rowGroup.columns().get(fileOrdinal);
            if (columnChunk.offsetIndexOffset() != null) {
                continue;
            }
            ColumnSchema columnSchema = fileSchema.getColumn(fileOrdinal);
            if (columnSchema.maxRepetitionLevel() == 0) {
                continue;
            }
            if (PageFormatProbe.firstDataPageType(inputFile, columnChunk)
                    == PageType.DATA_PAGE_V2) {
                continue;
            }
            return false;
        }
        return true;
    }

    /// Truncates a page list to cover at most `maxRows` rows. `maxRows <= 0`
    /// means "no row bound" and returns every page unchanged — the same
    /// `0 = unlimited` convention used throughout the fetch path.
    private static List<NeededPage> truncateToMaxRows(List<NeededPage> pages, long maxRows) {
        if (maxRows <= 0) {
            return pages;
        }
        List<NeededPage> truncated = new ArrayList<>();
        for (NeededPage page : pages) {
            if (page.location().firstRowIndex() >= maxRows) {
                break;
            }
            truncated.add(page);
        }
        return truncated;
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

    /// Returns the context.
    public HardwoodContextImpl context() {
        return context;
    }

    /// Waits for the speculative tasks the read has started, then releases
    /// iterator-local caches. Standalone iterators also wait for in-flight metadata
    /// loads and close their input files; iterators owned by ParquetFileReader leave
    /// those shared resources to the parent and instead tell it to stop tracking this
    /// iterator, so a closed child reader's work list does not stay reachable for the
    /// parent's whole lifetime.
    ///
    /// The caller stops the column workers first, so no retriever starts a task while
    /// this waits.
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        // Before the caches are cleared: a planning task holds its cache entry's lock
        // for as long as it plans, and the files are closed only after this returns.
        prefetchTasks.awaitAll();
        metadataCache.clear();
        fetchPlanCache.clear();
        nextRowGroupPrefetched.clear();

        try {
            if (ownsFileMetadataCache) {
                fileMetadataCache.close();
                // Reported rather than logged: a close that fails has failed, and a
                // warning nobody reads is a silent failure. Every file is attempted, one
                // failure is raised and the rest suppressed beneath it, as
                // ParquetFileReader.close does.
                InputFileCloser.closeAll(inputFiles);
            }
        }
        finally {
            // The parent stops tracking this iterator whether or not closing its files
            // succeeded: a failed close is still a close, and leaving the iterator on the
            // parent's list would keep a torn-down work list reachable for the parent's
            // whole lifetime.
            closeListener.accept(this);
        }
    }

    // ==================== Internal ====================

    /// Plans one more file, appending its surviving row groups to the work list.
    ///
    /// Returns `false` when there is nothing left to plan — every file done, or
    /// the row budget spent. Planning a file opens its footer, checks its schema
    /// against the reference, and with a filter probes its bloom filters, so a file
    /// is planned when the read reaches it rather than when the reader was built.
    /// See #1107. Dictionaries are read later, when the read reaches each row group.
    ///
    /// A failure is sticky: once planning a file has thrown, every later call rethrows it.
    /// Each column plans on its own retriever thread, and a column that found nothing left
    /// to plan would end its stream cleanly while the column that hit the failure has yet
    /// to report it, so the read could end empty without an exception (#1278). Retrying the
    /// file is no option either, as the row groups it added before failing are in the work
    /// list already.
    private synchronized boolean planNextFile() throws IOException {
        rethrowPlanningFailure();
        int plannedBefore = workItems.size();
        try {
            return planFile();
        }
        catch (IOException | RuntimeException | Error e) {
            planningFailure = e;
            workItemsBeforeFailure = plannedBefore;
            throw e;
        }
    }

    /// Rethrows the failure that ended planning, if any, as the type it was raised as.
    ///
    /// An `Error` is kept as well: the column worker reports one to its exchange like any
    /// other failure, and a column handed `null` past it would end the read just as silently.
    ///
    /// The same instance reaches every column, and the column worker never alters it: naming
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
        boolean hasFilter = filterPredicate != null;
        int fileIndex = nextFileToPlan++;
        PreparedFile prepared = getPreparedFile(fileIndex);
        // Decided per file and in the file's own ordinals: the order its bounds were written in
        // is this file's to declare, not the reference file's (#1179).
        BoundsReadability boundsReadability = prepared.boundsReadability();
        FileColumnOrdinals columnOrdinals = fileIndex == 0
                ? FileColumnOrdinals.identity(referenceSchema.getColumnCount(), filterPredicate,
                        boundsReadability)
                : FileColumnOrdinals.of(
                        validateSchemaCompatibility(prepared.inputFile(), prepared.schema()),
                        filterPredicate, boundsReadability);
        List<RowGroup> sourceRowGroups = fileIndex == 0 && firstFileRowGroups != null
                ? firstFileRowGroups : prepared.rowGroups();
        // Metadata pruning indexes every row group's chunk list by ordinal, so with
        // it active the cross-check has to cover the whole file before it runs.
        // Without it, the only row groups ever indexed are those that become work
        // items, and the ones physicalSkip / maxRows discard are never looked at.
        ChunkPathCheck chunkPaths = chunkPathCheck(prepared.schema(), columnOrdinals);
        boolean pruningIndexesChunks = filterPredicate != null && metadataFilteringEnabled;
        if (pruningIndexesChunks) {
            for (int rgIndex = 0; rgIndex < sourceRowGroups.size(); rgIndex++) {
                chunkPaths.verify(sourceRowGroups.get(rgIndex), rgIndex, prepared.inputFile());
            }
        }
        List<FilteredRowGroup> rowGroups = filterRowGroups(
                sourceRowGroups, prepared.inputFile(), columnOrdinals);

        for (int kept = 0; kept < rowGroups.size() && planRowBudget > 0; kept++) {
            FilteredRowGroup decided = rowGroups.get(kept);
            RowGroup rg = decided.rowGroup();
            int rgIndex = decided.fileRowGroupIndex();
            long rgRows = rg.numRows();
            if (planSkipRemaining >= rgRows) {
                planSkipRemaining -= rgRows;
                continue;
            }

            long leadingSkip = planSkipRemaining;
            planSkipRemaining = 0;
            if (workItems.isEmpty()) {
                firstRowGroupSkip = leadingSkip;
            }

            // Not yet cross-checked when pruning was skipped: with pruning active the
            // loop above has already covered every row group in the file.
            if (!pruningIndexesChunks) {
                chunkPaths.verify(rg, rgIndex, prepared.inputFile());
            }

            workItemRefCounts.put(workItems.size(),
                    new AtomicInteger(projection.decoded().getProjectedColumnCount()));
            workItems.add(new WorkItem(
                    prepared.inputFile(),
                    rg,
                    prepared.schema(),
                    columnOrdinals,
                    fileIndex,
                    rgIndex,
                    workItems.size(),
                    plannedRows,
                    decided.alwaysMatches(),
                    decided.leafDecisions()));

            // maxRows limiting: deduct row count from budget.
            // With a filter active, actual match count is unpredictable,
            // so all row groups remain available.
            if (!hasFilter) {
                planRowBudget -= rgRows - leadingSkip;
            }
            plannedRows += rgRows - leadingSkip;
        }

        // Trigger prefetch of next file
        triggerPrefetch(fileIndex + 1);

        LOG.log(System.Logger.Level.DEBUG,
                "Planned file {0}: {1} row groups in the work list so far",
                fileIndex, workItems.size());
        return true;
    }

    /// The work item at `index`, planning further files if the read has reached
    /// past what is planned. `null` once the read is done.
    ///
    /// Synchronized because every projected column walks this list on its own
    /// retriever thread, and a step past the planned end plans another file.
    /// Planning mutates shared state, so exactly one column may do it at a time;
    /// the others see the finished result, or the failure that ended planning.
    ///
    /// A work item of a file planned before the failed one is still handed out: the column
    /// whose prefetch planned the failed file can be ahead of the others, and a column
    /// failed short of it would return fewer rows of those files than the others.
    public synchronized WorkItem workItemAt(int index) throws IOException {
        if (planningFailure == null) {
            ensurePlannedThrough(index);
        }
        else if (index >= workItemsBeforeFailure) {
            rethrowPlanningFailure();
        }
        return index < workItems.size() ? workItems.get(index) : null;
    }

    /// Plans until `index` exists in the work list, or nothing is left to plan.
    private synchronized void ensurePlannedThrough(int index) throws IOException {
        while (workItems.size() <= index && planNextFile()) {
            // planNextFile appends
        }
    }

    /// Plans every remaining file. Needed by the questions that are about the
    /// whole read rather than the next row group — the full work list
    /// ([#getWorkItems]) and whether every row group supports per-page masking
    /// ([#canFastSkipAllRowGroups]).
    private synchronized void ensureFullyPlanned() throws IOException {
        while (planNextFile()) {
            // planNextFile appends
        }
    }

    /// Whether a filter predicate is installed, so that row groups can reach the
    /// readers with [WorkItem#filterAlwaysMatches] unset and rows scanned therefore
    /// outnumber rows matched.
    ///
    /// Only meaningful after [#initialize].
    public boolean hasFilter() {
        return filterPredicate != null;
    }

    /// Gets or loads file-wide metadata. Projection-specific validation remains
    /// local to [#buildWorkList].
    private PreparedFile getPreparedFile(int fileIndex) throws IOException {
        // The checked accessor, because the one caller — [#planNextFile] — can say so. Its
        // sibling exists for the callers that cannot: a mapping function, a `Runnable`, a
        // `CompletableFuture` body. Nothing about a footer read is different on this path,
        // only what the frame above it is allowed to declare.
        return fileMetadataCache.getFile(fileIndex);
    }

    /// Triggers async loading of the file at the given index.
    private void triggerPrefetch(int fileIndex) {
        fileMetadataCache.prefetch(fileIndex);
    }

    /// Where each column this read touches sits in one file, and the path the chunk
    /// at that ordinal must carry. Both are properties of the file, while the chunk
    /// list that restates them belongs to each row group, so this is resolved once
    /// per file and each row group is scanned against it by [#verify].
    ///
    /// @param fileOrdinals this file's leaf ordinal per touched column
    /// @param schemaPaths the schema path each of those ordinals was resolved from
    private record ChunkPathCheck(int[] fileOrdinals, FieldPath[] schemaPaths) {

        /// Asserts that the column chunk found at a touched leaf's ordinal is the
        /// chunk for that leaf, by comparing the chunk's own `path_in_schema`
        /// against the schema path the ordinal was resolved from.
        ///
        /// The two are independent statements of the same fact — the footer's column
        /// list is positionally aligned with the flattened schema leaves — so a
        /// disagreement means the file's metadata is internally inconsistent and any
        /// bytes read for this column would decode under the wrong column's schema.
        /// Writers that omit the (Thrift-required) path leave nothing to compare, and
        /// are left to the type checks alone.
        ///
        /// @throws SchemaIncompatibleException if a chunk disagrees with the schema
        void verify(RowGroup rowGroup, int rowGroupIndex, InputFile inputFile) {
            List<ColumnChunk> chunks = rowGroup.columns();
            int chunkCount = chunks.size();
            for (int i = 0; i < fileOrdinals.length; i++) {
                int fileOrdinal = fileOrdinals[i];
                FieldPath schemaPath = schemaPaths[i];
                if (fileOrdinal >= chunkCount) {
                    throw new SchemaIncompatibleException(
                            ExceptionContext.filePrefix(inputFile.name())
                                    + "Row group " + rowGroupIndex + " lists " + chunkCount
                                    + " column chunks, but the schema declares '"
                                    + schemaPath + "' at index " + fileOrdinal);
                }
                FieldPath chunkPath = chunks.get(fileOrdinal).metaData().pathInSchema();
                if (chunkPath.isEmpty() || chunkPath.equals(schemaPath)) {
                    continue;
                }
                throw new SchemaIncompatibleException(
                        ExceptionContext.filePrefix(inputFile.name())
                                + "Row group " + rowGroupIndex + " lists column '" + chunkPath
                                + "' where the schema declares '" + schemaPath + "'");
            }
        }
    }

    /// Resolves the file-level side of the chunk-path cross-check for one file.
    private ChunkPathCheck chunkPathCheck(FileSchema fileSchema, FileColumnOrdinals columnOrdinals) {
        int touchedCount = touchedColumns.cardinality();
        int[] fileOrdinals = new int[touchedCount];
        FieldPath[] schemaPaths = new FieldPath[touchedCount];
        int touched = 0;
        for (int refOrdinal = touchedColumns.nextSetBit(0); refOrdinal >= 0;
                refOrdinal = touchedColumns.nextSetBit(refOrdinal + 1)) {
            int fileOrdinal = columnOrdinals.fileOrdinal(refOrdinal);
            fileOrdinals[touched] = fileOrdinal;
            schemaPaths[touched] = fileSchema.getColumn(fileOrdinal).fieldPath();
            touched++;
        }
        return new ChunkPathCheck(fileOrdinals, schemaPaths);
    }

    /// A row group surviving predicate push-down, and whether its statistics prove
    /// every row matches (so per-row filtering can be skipped for it).
    /// A row group that survived pruning, with the index it has **in the file** — not its
    /// position in this list, which pruning makes a different number. Every consumer means
    /// the file's: the fetch log, the JFR events, the row group a failure names.
    private record FilteredRowGroup(RowGroup rowGroup, boolean alwaysMatches,
            int fileRowGroupIndex, RowGroupFilterEvaluator.LeafDecisions leafDecisions) {}

    private List<FilteredRowGroup> filterRowGroups(List<RowGroup> rowGroups, InputFile inputFile,
                                                   FileColumnOrdinals columnOrdinals) throws IOException {
        // The metadata-filtering opt-out (#797) disables every metadata-driven prune,
        // dictionary membership included — with it off, no row group is dropped without
        // reading rows.
        if (filterPredicate == null || !metadataFilteringEnabled) {
            List<FilteredRowGroup> unpruned = new ArrayList<>(rowGroups.size());
            for (int rgIndex = 0; rgIndex < rowGroups.size(); rgIndex++) {
                unpruned.add(new FilteredRowGroup(rowGroups.get(rgIndex), false, rgIndex, null));
            }
            return unpruned;
        }
        List<FilteredRowGroup> filtered = new ArrayList<>(rowGroups.size());
        int fullyMatching = 0;
        for (int rgIndex = 0; rgIndex < rowGroups.size(); rgIndex++) {
            RowGroup rg = rowGroups.get(rgIndex);
            FilterDecision decision;
            RowGroupFilterEvaluator.LeafDecisions leafDecisions = new RowGroupFilterEvaluator.LeafDecisions();
            // The bloom-filter reads pruning issues are named as such, so a fetch log tells them
            // apart from the reads that decode the row group. Dictionaries are read once the read
            // reaches the row group, in `computeSharedMetadata`, and kept for decoding it.
            try (FetchReason.Scope ignored = FetchReason.set("rg=" + rgIndex + " pruning")) {
                decision = RowGroupFilterEvaluator.planRowGroup(columnOrdinals.filter(), rg,
                        new RowGroupBloomFilterSource(inputFile, rg),
                        new LogContext(inputFile.name(), rgIndex), columnOrdinals.boundsReadability(),
                        leafDecisions);
            }
            if (decision == FilterDecision.CANNOT_MATCH) {
                continue;
            }
            boolean alwaysMatches = decision == FilterDecision.ALWAYS_MATCHES;
            if (alwaysMatches) {
                fullyMatching++;
            }
            filtered.add(new FilteredRowGroup(rg, alwaysMatches, rgIndex, leafDecisions));
        }

        RowGroupFilterEvent event = new RowGroupFilterEvent();
        event.file = inputFile.name();
        event.totalRowGroups = rowGroups.size();
        event.rowGroupsKept = filtered.size();
        event.rowGroupsSkipped = rowGroups.size() - filtered.size();
        event.rowGroupsFullyMatching = fullyMatching;
        event.commit();

        return filtered;
    }

    /// Validates the reference schema's own statement of the columns this read touches,
    /// before any of them is planned for or decoded.
    ///
    /// [#validateSchemaCompatibility] cross-checks every file from the second onwards
    /// against the reference schema, but the first file *is* the reference schema and so
    /// is never cross-checked. What it declares therefore has to hold on its own terms:
    /// a fixed-width column whose width the footer omits cannot be decoded by any of the
    /// consumers downstream of here, and a read of a single file would otherwise reach
    /// them with nothing having said so.
    ///
    /// @throws SchemaIncompatibleException if a touched column cannot be decoded under
    ///         the schema the file declares for it
    private void validateReferenceColumns() {
        String fileName = inputFiles.get(0).name();
        for (int originalIndex = touchedColumns.nextSetBit(0); originalIndex >= 0;
                originalIndex = touchedColumns.nextSetBit(originalIndex + 1)) {
            FixedWidthValidator.validate(fileName, referenceSchema.getColumn(originalIndex));
        }
    }

    /// The reference leaf ordinals a read with this projection and filter touches.
    private static BitSet touchedColumns(ProjectedSchema projected, ResolvedPredicate filter,
                                         int referenceColumnCount) {
        BitSet touched = new BitSet(referenceColumnCount);
        int projectedColumnCount = projected.getProjectedColumnCount();
        for (int projectedIndex = 0; projectedIndex < projectedColumnCount; projectedIndex++) {
            touched.set(projected.toOriginalIndex(projectedIndex));
        }
        if (filter != null) {
            ResolvedPredicate.collectColumnIndices(filter, touched);
        }
        return touched;
    }

    /// Validates the columns this read touches ([#touchedColumns]) against the
    /// reference schema and returns where each of them sits in `fileSchema`.
    ///
    /// Untouched columns map to `-1`; a file is free to differ in columns nobody reads.
    ///
    /// @return this file's leaf ordinal per reference leaf ordinal, `-1` where unresolved
    /// @throws SchemaIncompatibleException if a touched column is missing or its leaf
    ///         differs in a way that changes how its pages decode
    private int[] validateSchemaCompatibility(InputFile inputFile, FileSchema fileSchema) {
        int referenceColumnCount = referenceSchema.getColumnCount();
        int[] fileOrdinals = new int[referenceColumnCount];
        Arrays.fill(fileOrdinals, -1);

        for (int originalIndex = touchedColumns.nextSetBit(0); originalIndex >= 0;
                originalIndex = touchedColumns.nextSetBit(originalIndex + 1)) {
            fileOrdinals[originalIndex] = validateColumn(inputFile, fileSchema, originalIndex);
        }
        return fileOrdinals;
    }

    /// Validates one reference column against its counterpart in `fileSchema`,
    /// resolved by field path.
    ///
    /// @return the column's leaf ordinal in `fileSchema`
    private int validateColumn(InputFile inputFile, FileSchema fileSchema, int originalIndex) {
        ColumnSchema refColumn = referenceSchema.getColumn(originalIndex);

        ColumnSchema fileColumn;
        try {
            fileColumn = fileSchema.getColumn(refColumn.fieldPath());
        }
        catch (IllegalArgumentException e) {
            throw new SchemaIncompatibleException(
                    ExceptionContext.filePrefix(inputFile.name())
                            + "Column '" + refColumn.fieldPath() + "' not found");
        }

        PhysicalType refType = refColumn.type();
        PhysicalType fileType = fileColumn.type();
        if (refType != fileType) {
            throw new SchemaIncompatibleException(
                    ExceptionContext.filePrefix(inputFile.name())
                            + "Column '" + refColumn.fieldPath() + "' has incompatible type"
                            + ": expected " + refType + " but found " + fileType);
        }

        LogicalType refLogical = refColumn.logicalType();
        LogicalType fileLogical = fileColumn.logicalType();
        if (!Objects.equals(refLogical, fileLogical)) {
            throw new SchemaIncompatibleException(
                    ExceptionContext.filePrefix(inputFile.name())
                            + "Column '" + refColumn.fieldPath() + "' has incompatible logical type"
                            + ": expected " + refLogical + " but found " + fileLogical);
        }

        RepetitionType refRep = refColumn.repetitionType();
        RepetitionType fileRep = fileColumn.repetitionType();
        if (refRep != fileRep) {
            throw new SchemaIncompatibleException(
                    ExceptionContext.filePrefix(inputFile.name())
                            + "Column '" + refColumn.fieldPath() + "' has incompatible repetition type"
                            + ": expected " + refRep + " but found " + fileRep);
        }

        // The FLBA width drives every decode of the column's bytes.
        Integer refLength = refColumn.typeLength();
        Integer fileLength = fileColumn.typeLength();
        if (!Objects.equals(refLength, fileLength)) {
            throw new SchemaIncompatibleException(
                    ExceptionContext.filePrefix(inputFile.name())
                            + "Column '" + refColumn.fieldPath() + "' has incompatible type length"
                            + ": expected " + refLength + " but found " + fileLength);
        }

        // The leaf's own repetition type matching does not imply its ancestors' do,
        // and both levels are read from the per-file leaf when a page is decoded but
        // from the reference leaf when records are assembled from it (ColumnWorker).
        // The same leaf path under an optional rather than a required ancestor group
        // misreads nulls; under a repeated rather than a non-repeated one it misreads
        // record boundaries.
        int refMaxDef = refColumn.maxDefinitionLevel();
        int fileMaxDef = fileColumn.maxDefinitionLevel();
        if (refMaxDef != fileMaxDef) {
            throw new SchemaIncompatibleException(
                    ExceptionContext.filePrefix(inputFile.name())
                            + "Column '" + refColumn.fieldPath()
                            + "' has incompatible maximum definition level"
                            + ": expected " + refMaxDef + " but found " + fileMaxDef);
        }

        int refMaxRep = refColumn.maxRepetitionLevel();
        int fileMaxRep = fileColumn.maxRepetitionLevel();
        if (refMaxRep != fileMaxRep) {
            throw new SchemaIncompatibleException(
                    ExceptionContext.filePrefix(inputFile.name())
                            + "Column '" + refColumn.fieldPath()
                            + "' has incompatible maximum repetition level"
                            + ": expected " + refMaxRep + " but found " + fileMaxRep);
        }

        return fileColumn.columnIndex();
    }
}
