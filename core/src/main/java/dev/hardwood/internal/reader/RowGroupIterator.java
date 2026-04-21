/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerArray;

import dev.hardwood.InputFile;
import dev.hardwood.internal.predicate.PageDropPredicates;
import dev.hardwood.internal.predicate.PageFilterEvaluator;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.predicate.RowGroupFilterEvaluator;
import dev.hardwood.internal.thrift.OffsetIndexReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.jfr.FileOpenedEvent;
import dev.hardwood.jfr.RowGroupFilterEvent;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

/// Shared iterator over `(InputFile, RowGroup)` pairs across one or more files.
///
/// Handles file lifecycle (open, metadata read, schema validation, close),
/// row-group filtering by statistics, `maxRows` limiting at the row-group level,
/// and async prefetching of the next file.
///
/// Each [PageSource] maintains its own cursor into the work list exposed by
/// this iterator. Shared per-row-group metadata (index buffers, matching rows)
/// is cached for the current file and reused across columns.
public class RowGroupIterator {

    private static final System.Logger LOG = System.getLogger(RowGroupIterator.class.getName());

    /// Maximum gap (in bytes) between pages that will be bridged when coalescing
    /// within a column. Pages separated by more than this gap get separate
    /// `readRange()` calls.
    private static final int PAGE_COALESCE_GAP_BYTES = 1024 * 1024;

    /// Maximum size (in bytes) of a single coalesced page group. Groups that
    /// would exceed this are split so that each `readRange()` stays bounded,
    /// enabling lazy pre-fetch overlap and early cancellation.
    private static final int MAX_COALESCED_BYTES =
            Integer.getInteger("hardwood.internal.maxCoalescedBytes", 128 * 1024 * 1024);

    private final List<InputFile> inputFiles;
    private final HardwoodContextImpl context;
    private final long maxRows;

    // Set after first file
    private FileSchema referenceSchema;
    private ProjectedSchema projectedSchema;
    private ResolvedPredicate filterPredicate;

    /// AND-necessary leaves per column index, derived once from `filterPredicate`.
    /// Feeds [SequentialFetchPlan]'s inline-stats page-drop check.
    private Map<Integer, List<ResolvedPredicate>> dropLeavesByColumn = Map.of();

    // Work list: all (file, rowGroup) pairs to process, built during initialize()
    private final List<WorkItem> workItems = new ArrayList<>();

    // Prefetch state
    private final ConcurrentHashMap<Integer, CompletableFuture<PreparedFile>> fileFutures = new ConcurrentHashMap<>();

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
    private AtomicIntegerArray workItemRefCounts;

    /// A single unit of work: one row group in one file.
    ///
    /// `rowsConsumedBefore` is the cumulative row count of all work items
    /// preceding this one in the work list — used to convert the iterator-wide
    /// `maxRows` budget into a per-row-group remainder when computing fetch
    /// plans. (Filter predicates invalidate this correlation, so callers must
    /// ignore it when a filter is active.)
    public record WorkItem(
            InputFile inputFile,
            RowGroup rowGroup,
            FileSchema fileSchema,
            int fileIndex,
            int rowGroupIndex,
            int workItemIndex,
            long rowsConsumedBefore
    ) {}

    /// Cached shared metadata for one row group, reused across columns.
    public record SharedRowGroupMetadata(
            RowGroupIndexBuffers indexBuffers,
            RowRanges matchingRows
    ) {}

    /// Result of opening and validating a file.
    private record PreparedFile(
            InputFile inputFile,
            FileMetaData metaData,
            FileSchema schema,
            List<RowGroup> rowGroups
    ) {}

    /// Creates a RowGroupIterator for the given files.
    ///
    /// @param inputFiles one or more input files (must not be empty)
    /// @param context the Hardwood context
    /// @param maxRows maximum rows to read (0 = unlimited)
    public RowGroupIterator(List<InputFile> inputFiles, HardwoodContextImpl context, long maxRows) {
        if (inputFiles.isEmpty()) {
            throw new IllegalArgumentException("At least one file must be provided");
        }
        this.inputFiles = new ArrayList<>(inputFiles);
        this.context = context;
        this.maxRows = maxRows;
    }

    /// Returns the maximum rows limit (0 = unlimited).
    public long maxRows() {
        return maxRows;
    }

    /// Sets the reference schema and pre-prepared first file, skipping [#openFirst()].
    /// Used when the file is already open and metadata has been read externally
    /// (e.g., by [dev.hardwood.reader.ParquetFileReader]).
    ///
    /// @param schema the file schema from the first file
    /// @param rowGroups the (already filtered) row groups from the first file
    public void setFirstFile(FileSchema schema, List<RowGroup> rowGroups) {
        this.referenceSchema = schema;
        InputFile first = inputFiles.get(0);
        PreparedFile prepared = new PreparedFile(first, null, schema, rowGroups);
        fileFutures.put(0, CompletableFuture.completedFuture(prepared));
    }

    /// Opens the first file and returns its schema.
    public FileSchema openFirst() throws IOException {
        InputFile first = inputFiles.get(0);
        first.open();
        PreparedFile prepared = openAndReadMetadata(first);
        referenceSchema = prepared.schema;
        fileFutures.put(0, CompletableFuture.completedFuture(prepared));
        return referenceSchema;
    }

    /// Applies column projection and optional filter, builds the full work list.
    ///
    /// @param projection column projection
    /// @param filter resolved predicate, or `null` for no filtering
    /// @return the projected schema
    public ProjectedSchema initialize(ColumnProjection projection, ResolvedPredicate filter) {
        return initialize(ProjectedSchema.create(referenceSchema, projection), filter);
    }

    /// Applies a pre-built projected schema and optional filter, builds the full work list.
    ///
    /// @param projected pre-built projected schema
    /// @param filter resolved predicate, or `null` for no filtering
    /// @return the projected schema (same as input)
    public ProjectedSchema initialize(ProjectedSchema projected, ResolvedPredicate filter) {
        if (referenceSchema == null) {
            throw new IllegalStateException("openFirst() must be called before initialize()");
        }
        this.projectedSchema = projected;
        this.filterPredicate = filter;
        this.dropLeavesByColumn = filter != null ? PageDropPredicates.byColumn(filter) : Map.of();

        buildWorkList();

        int columnCount = projectedSchema.getProjectedColumnCount();
        workItemRefCounts = new AtomicIntegerArray(workItems.size());
        for (int i = 0; i < workItems.size(); i++) {
            workItemRefCounts.set(i, columnCount);
        }

        // Trigger prefetch of second file
        triggerPrefetch(1);

        return projectedSchema;
    }

    /// Returns the ordered work list of (file, rowGroup) pairs.
    public List<WorkItem> getWorkItems() {
        return workItems;
    }

    /// Returns the projected schema.
    public ProjectedSchema projectedSchema() {
        return projectedSchema;
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
    /// @return shared metadata (index buffers and matching row ranges)
    public SharedRowGroupMetadata getSharedMetadata(WorkItem workItem) {
        return metadataCache.computeIfAbsent(workItem.workItemIndex(), idx -> {
            try {
                RowGroupIndexBuffers indexBuffers = RowGroupIndexBuffers.fetch(
                        workItem.inputFile(), workItem.rowGroup());

                RowRanges matchingRows = RowRanges.ALL;
                if (filterPredicate != null) {
                    matchingRows = PageFilterEvaluator.computeMatchingRows(
                            filterPredicate, workItem.rowGroup(), indexBuffers);
                }

                return new SharedRowGroupMetadata(indexBuffers, matchingRows);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to fetch metadata for row group "
                        + workItem.rowGroupIndex() + " in " + workItem.inputFile().name(), e);
            }
        });
    }

    /// Returns the [FetchPlan] for the given column in the given row group.
    /// Plans are computed once per row group (on first access) and cached.
    ///
    /// @param workItem the work item identifying the row group
    /// @param projectedColumnIndex the projected column index
    /// @return a fetch plan for iterating pages with lazy byte fetching
    public FetchPlan getColumnPlan(WorkItem workItem, int projectedColumnIndex) {
        FetchPlan[] plans = fetchPlanCache.computeIfAbsent(workItem.workItemIndex(),
                idx -> {
                    FetchPlan[] computed = computeFetchPlans(workItem);
                    prefetchNextRowGroup(workItem);
                    return computed;
                });
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
        if (workItemRefCounts == null) {
            return;
        }
        int idx = workItem.workItemIndex();
        int remaining = workItemRefCounts.decrementAndGet(idx);
        if (remaining == 0) {
            metadataCache.remove(idx);
            fetchPlanCache.remove(idx);
        }
    }

    /// Triggers async pre-computation and pre-fetch for the next row group.
    /// The plan computation is pure metadata work (no I/O). The pre-fetch
    /// kicks off the first chunk's `readRange()` asynchronously.
    private void prefetchNextRowGroup(WorkItem currentWorkItem) {
        int nextIndex = currentWorkItem.workItemIndex() + 1;
        if (nextIndex >= workItems.size()) {
            return;
        }
        WorkItem nextWorkItem = workItems.get(nextIndex);
        CompletableFuture.runAsync(() -> {
            FetchPlan[] nextPlans = fetchPlanCache.computeIfAbsent(
                    nextWorkItem.workItemIndex(),
                    idx -> computeFetchPlans(nextWorkItem));
            // Pre-fetch the first non-empty plan's chunk
            for (FetchPlan plan : nextPlans) {
                if (!plan.isEmpty()) {
                    plan.prefetch();
                    break;
                }
            }
        });
    }

    private FetchPlan[] computeFetchPlans(WorkItem workItem) {
        SharedRowGroupMetadata shared = getSharedMetadata(workItem);
        RowGroup rowGroup = workItem.rowGroup();
        RowRanges matchingRows = shared.matchingRows();
        InputFile inputFile = workItem.inputFile();
        int projectedCount = projectedSchema.getProjectedColumnCount();

        // Convert the iterator-wide maxRows into a per-row-group remainder.
        // `PageLocation.firstRowIndex` and `SequentialFetchPlan.valuesRead`
        // are both row-group-local (reset to 0 each RG), so passing the global
        // maxRows would fail to truncate anything in non-first row groups and
        // over-fetch the last partially-needed RG. With a filter active the
        // match count is unpredictable, so we fall back to the global value.
        long perRgMaxRows = perRgMaxRows(workItem);

        FetchPlan[] plans = new FetchPlan[projectedCount];

        for (int projCol = 0; projCol < projectedCount; projCol++) {
            int originalIndex = projectedSchema.toOriginalIndex(projCol);
            ColumnChunk columnChunk = rowGroup.columns().get(originalIndex);
            ColumnSchema columnSchema = workItem.fileSchema().getColumn(originalIndex);
            ColumnIndexBuffers colBuffers = shared.indexBuffers().forColumn(originalIndex);

            if (colBuffers == null || colBuffers.offsetIndex() == null) {
                // No OffsetIndex — sequential lazy fetching. Per-page drop via
                // inline DataPageHeader.statistics happens inside SequentialFetchPlan
                // for the AND-necessary leaves touching this column.
                List<ResolvedPredicate> leaves = dropLeavesByColumn.getOrDefault(originalIndex, List.of());
                plans[projCol] = SequentialFetchPlan.build(
                        inputFile, columnSchema, columnChunk,
                        context, workItem.rowGroupIndex(), inputFile.name(),
                        perRgMaxRows, leaves);
                continue;
            }

            try {
                OffsetIndex offsetIndex = OffsetIndexReader.read(
                        new ThriftCompactReader(colBuffers.offsetIndex()));
                List<PageLocation> allPages = offsetIndex.pageLocations();

                // Determine needed pages (filter + maxRows)
                List<PageLocation> neededPages = computeNeededPages(
                        allPages, matchingRows, rowGroup.numRows());

                if (neededPages.isEmpty()) {
                    plans[projCol] = FetchPlan.EMPTY;
                    continue;
                }

                if (perRgMaxRows > 0) {
                    neededPages = truncateToMaxRows(neededPages, perRgMaxRows);
                }

                // Coalesce needed pages within this column into page groups,
                // bridging small gaps but splitting on large ones.
                List<PageGroup> groups = coalescePages(neededPages, columnChunk,
                        allPages.get(0).offset());

                // Create ChunkHandles for each page group, linked for pre-fetch
                List<ChunkHandle> handles = new ArrayList<>(groups.size());
                for (PageGroup group : groups) {
                    handles.add(new ChunkHandle(inputFile, group.offset, group.length));
                }
                for (int i = 0; i < handles.size() - 1; i++) {
                    handles.get(i).setNextChunk(handles.get(i + 1));
                }

                plans[projCol] = IndexedFetchPlan.build(
                        neededPages, groups, handles,
                        allPages.get(0).offset(),
                        columnSchema, columnChunk,
                        context, workItem.rowGroupIndex(), inputFile.name());
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to compute fetch plan for column "
                        + projCol + " in row group " + workItem.rowGroupIndex(), e);
            }
        }

        return plans;
    }

    /// A contiguous byte range covering one or more pages within a column.
    record PageGroup(long offset, int length, int firstPageIndex, int pageCount) {}

    /// Coalesces needed pages within a column into page groups with gap tolerance.
    /// Includes the dictionary prefix in the first group if present.
    private static List<PageGroup> coalescePages(List<PageLocation> neededPages,
                                                  ColumnChunk columnChunk,
                                                  long firstDataPageOffset) {
        // Determine dictionary prefix (explicit or implicit)
        Long dictOffset = columnChunk.metaData().dictionaryPageOffset();
        long dictStart;
        if (dictOffset != null && dictOffset > 0 && dictOffset < firstDataPageOffset) {
            dictStart = dictOffset;
        }
        else if (firstDataPageOffset > columnChunk.metaData().dataPageOffset()) {
            // Implicit dictionary: writers that omit dictionaryPageOffset
            dictStart = columnChunk.metaData().dataPageOffset();
        }
        else {
            dictStart = 0;
        }

        List<PageGroup> groups = new ArrayList<>();
        long groupStart = neededPages.get(0).offset();
        long groupEnd = groupStart + neededPages.get(0).compressedPageSize();
        int groupFirstPage = 0;
        int groupPageCount = 1;

        // Extend first group backwards to include dictionary prefix
        if (dictStart > 0 && dictStart < groupStart) {
            groupStart = dictStart;
        }

        for (int i = 1; i < neededPages.size(); i++) {
            PageLocation page = neededPages.get(i);
            long gap = page.offset() - groupEnd;
            long newGroupSize = page.offset() + page.compressedPageSize() - groupStart;

            if (gap <= PAGE_COALESCE_GAP_BYTES && newGroupSize <= MAX_COALESCED_BYTES) {
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

    /// Determines which pages are needed based on the filter's matching row ranges.
    private static List<PageLocation> computeNeededPages(List<PageLocation> allPages,
                                                          RowRanges matchingRows,
                                                          long rowGroupRowCount) {
        if (matchingRows.isAll()) {
            return allPages;
        }
        List<PageLocation> needed = new ArrayList<>();
        for (int i = 0; i < allPages.size(); i++) {
            long pageFirstRow = allPages.get(i).firstRowIndex();
            long pageLastRow = (i + 1 < allPages.size())
                    ? allPages.get(i + 1).firstRowIndex()
                    : rowGroupRowCount;
            if (matchingRows.overlapsPage(pageFirstRow, pageLastRow)) {
                needed.add(allPages.get(i));
            }
        }
        return needed;
    }

    /// Truncates a page list to cover at most `maxRows` rows.
    private static List<PageLocation> truncateToMaxRows(List<PageLocation> pages, long maxRows) {
        List<PageLocation> truncated = new ArrayList<>();
        for (PageLocation page : pages) {
            if (page.firstRowIndex() >= maxRows) {
                break;
            }
            truncated.add(page);
        }
        return truncated;
    }

    /// Per-row-group remainder of the iterator-wide `maxRows` budget.
    ///
    /// Returns `0` when `maxRows` is unset (no limit). When a filter predicate
    /// is active the prior-row-count sum can't be correlated with matching
    /// rows, so the global `maxRows` is returned as a conservative bound.
    /// Otherwise returns `max(0, maxRows - workItem.rowsConsumedBefore())`,
    /// which naturally trims the last partially-needed row group's fetch plan
    /// while being a no-op (all pages kept) for fully-needed earlier ones.
    private long perRgMaxRows(WorkItem workItem) {
        if (maxRows <= 0) {
            return 0;
        }
        if (filterPredicate != null) {
            return maxRows;
        }
        return Math.max(0, maxRows - workItem.rowsConsumedBefore());
    }

    /// Returns the context.
    public HardwoodContextImpl context() {
        return context;
    }

    /// Waits for in-flight prefetches and closes all files.
    public void close() {
        for (CompletableFuture<PreparedFile> future : fileFutures.values()) {
            try {
                future.join();
            }
            catch (Exception ignored) {
            }
        }
        fileFutures.clear();
        metadataCache.clear();
        fetchPlanCache.clear();

        for (InputFile file : inputFiles) {
            try {
                file.close();
            }
            catch (IOException e) {
                LOG.log(System.Logger.Level.WARNING, "Failed to close file: " + file.name(), e);
            }
        }
    }

    // ==================== Internal ====================

    /// Builds the work list by iterating all files and row groups.
    private void buildWorkList() {
        long rowBudget = maxRows > 0 ? maxRows : Long.MAX_VALUE;
        long rowsConsumed = 0;
        boolean hasFilter = filterPredicate != null;

        for (int fileIndex = 0; fileIndex < inputFiles.size() && rowBudget > 0; fileIndex++) {
            PreparedFile prepared = getPreparedFile(fileIndex);
            List<RowGroup> rowGroups = filterRowGroups(prepared.rowGroups, prepared.inputFile.name());

            for (int rgIndex = 0; rgIndex < rowGroups.size() && rowBudget > 0; rgIndex++) {
                RowGroup rg = rowGroups.get(rgIndex);
                workItems.add(new WorkItem(
                        prepared.inputFile,
                        rg,
                        prepared.schema,
                        fileIndex,
                        rgIndex,
                        workItems.size(),
                        rowsConsumed));

                // maxRows limiting: deduct row count from budget.
                // With a filter active, actual match count is unpredictable,
                // so all row groups remain available.
                if (!hasFilter) {
                    rowBudget -= rg.numRows();
                }
                rowsConsumed += rg.numRows();
            }

            // Trigger prefetch of next file
            triggerPrefetch(fileIndex + 1);
        }

        LOG.log(System.Logger.Level.DEBUG, "Built work list: {0} row groups across {1} files",
                workItems.size(), inputFiles.size());
    }

    /// Gets or loads a prepared file, blocking if necessary.
    private PreparedFile getPreparedFile(int fileIndex) {
        CompletableFuture<PreparedFile> future = fileFutures.computeIfAbsent(
                fileIndex, this::loadFileAsync);
        return future.join();
    }

    /// Triggers async loading of the file at the given index.
    private void triggerPrefetch(int fileIndex) {
        if (fileIndex >= 0 && fileIndex < inputFiles.size()) {
            fileFutures.computeIfAbsent(fileIndex, this::loadFileAsync);
        }
    }

    private CompletableFuture<PreparedFile> loadFileAsync(int fileIndex) {
        return CompletableFuture.supplyAsync(() -> loadFile(fileIndex));
    }

    private PreparedFile loadFile(int fileIndex) {
        InputFile inputFile = inputFiles.get(fileIndex);
        try {
            inputFile.open();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to open file: " + inputFile.name(), e);
        }

        PreparedFile prepared = openAndReadMetadata(inputFile);

        // Validate schema compatibility (skip first file — it IS the reference)
        if (fileIndex > 0) {
            validateSchemaCompatibility(inputFile, prepared.schema);
        }

        return prepared;
    }

    private PreparedFile openAndReadMetadata(InputFile inputFile) {
        FileOpenedEvent event = new FileOpenedEvent();
        event.begin();

        try {
            FileMetaData metaData = ParquetMetadataReader.readMetadata(inputFile);
            FileSchema schema = FileSchema.fromSchemaElements(metaData.schema());

            event.file = inputFile.name();
            event.fileSize = inputFile.length();
            event.rowGroupCount = metaData.rowGroups().size();
            event.columnCount = schema.getColumnCount();
            event.commit();

            // Row groups are stored unfiltered; filtering happens in buildWorkList()
            return new PreparedFile(inputFile, metaData, schema, metaData.rowGroups());
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read metadata: " + inputFile.name(), e);
        }
    }

    private List<RowGroup> filterRowGroups(List<RowGroup> rowGroups, String fileName) {
        if (filterPredicate == null) {
            return rowGroups;
        }
        List<RowGroup> filtered = rowGroups.stream()
                .filter(rg -> !RowGroupFilterEvaluator.canDropRowGroup(filterPredicate, rg))
                .toList();

        RowGroupFilterEvent event = new RowGroupFilterEvent();
        event.file = fileName;
        event.totalRowGroups = rowGroups.size();
        event.rowGroupsKept = filtered.size();
        event.rowGroupsSkipped = rowGroups.size() - filtered.size();
        event.commit();

        return filtered;
    }

    private void validateSchemaCompatibility(InputFile inputFile, FileSchema fileSchema) {
        int projectedColumnCount = projectedSchema.getProjectedColumnCount();
        for (int projectedIndex = 0; projectedIndex < projectedColumnCount; projectedIndex++) {
            int originalIndex = projectedSchema.toOriginalIndex(projectedIndex);
            ColumnSchema refColumn = referenceSchema.getColumn(originalIndex);

            ColumnSchema fileColumn;
            try {
                fileColumn = fileSchema.getColumn(refColumn.fieldPath());
            }
            catch (IllegalArgumentException e) {
                throw new RowGroupIterator.SchemaIncompatibleException(
                        "Column '" + refColumn.fieldPath() + "' not found in file: " + inputFile.name());
            }

            PhysicalType refType = refColumn.type();
            PhysicalType fileType = fileColumn.type();
            if (refType != fileType) {
                throw new RowGroupIterator.SchemaIncompatibleException(
                        "Column '" + refColumn.fieldPath() + "' has incompatible type in file " + inputFile.name()
                                + ": expected " + refType + " but found " + fileType);
            }

            LogicalType refLogical = refColumn.logicalType();
            LogicalType fileLogical = fileColumn.logicalType();
            if (!Objects.equals(refLogical, fileLogical)) {
                throw new RowGroupIterator.SchemaIncompatibleException(
                        "Column '" + refColumn.fieldPath() + "' has incompatible logical type in file "
                                + inputFile.name() + ": expected " + refLogical + " but found " + fileLogical);
            }

            RepetitionType refRep = refColumn.repetitionType();
            RepetitionType fileRep = fileColumn.repetitionType();
            if (refRep != fileRep) {
                throw new RowGroupIterator.SchemaIncompatibleException(
                        "Column '" + refColumn.fieldPath() + "' has incompatible repetition type in file "
                                + inputFile.name() + ": expected " + refRep + " but found " + fileRep);
            }
        }
    }

    /// Thrown when a file's schema is incompatible with the reference schema.
    public static class SchemaIncompatibleException extends RuntimeException {
        public SchemaIncompatibleException(String message) {
            super(message);
        }
    }
}
