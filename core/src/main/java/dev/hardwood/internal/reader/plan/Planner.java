/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.io.HeldBytes;
import dev.hardwood.internal.predicate.BoundsReadability;
import dev.hardwood.internal.predicate.LogContext;
import dev.hardwood.internal.predicate.PageFilterEvaluator;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.fetch.CoalescingPolicy.Extent;
import dev.hardwood.internal.reader.fetch.FetchPlan;
import dev.hardwood.internal.reader.fetch.Sections;
import dev.hardwood.internal.reader.footer.FileColumnOrdinals;
import dev.hardwood.internal.reader.plan.FileMetadataCache.PreparedFile;
import dev.hardwood.internal.reader.plan.RowGroupIterator.RowGroupPlans;
import dev.hardwood.internal.reader.plan.RowGroupIterator.SharedRowGroupMetadata;
import dev.hardwood.internal.reader.plan.RowGroupIterator.WorkItem;
import dev.hardwood.internal.reader.select.RowRanges;
import dev.hardwood.internal.reader.step.ByteSource;
import dev.hardwood.internal.reader.step.Need;
import dev.hardwood.internal.reader.step.Parsed;
import dev.hardwood.internal.schema.ReadProjection;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.FileSchema;
import jdk.jfr.Event;

/// What planning a read decides, without driving it: a file's work items from its footer, and each
/// work item's stages (its Bloom filters and dictionaries, its page index and page filter, its mask
/// probe, its plans and batches). Each stage is a parse of the bytes the read holds that returns
/// what it lacks, a [Need], and is run again once that has arrived; [Sections] holds the bytes. The
/// caller drives it in work-list order and keeps the work list.
///
/// It keeps the listing's state, which each file's listing resumes: the row budget, the rows
/// planned and still to skip, and the physical skip's residue within the first work item.
final class Planner {

    /// A file's work items, and the [dev.hardwood.jfr.RowGroupFilterEvent] its pruning reports,
    /// `null` for none.
    record Listed(List<Planning> plannings, Event event) {
    }

    private final Sections sections;
    private final FileSchema referenceSchema;
    private final List<RowGroup> firstFileRowGroups;
    private final ReadProjection projection;
    private final ResolvedPredicate filterPredicate;
    private final SchemaCompatibility schemaCompatibility;
    private final RowGroupPruning pruning;
    private final FetchPlanBuilder fetchPlanBuilder;
    private final long maxRows;
    /// The most bytes one [IndexWindow] reads, gaps included, unless a single row group needs more.
    private final long indexWindowBytes;

    private long planRowBudget;
    private long plannedRows;
    private long planSkipRemaining;
    /// The residue of the physical skip within the first work item, written once as the first file
    /// is listed, before the listing is published.
    private volatile long firstRowGroupSkip;
    /// Whether the read's batch plan has taken a work item, after which it can no longer be set.
    private boolean batchesPlanned;

    // Set before the read's columns ask for plans, read by planning after
    private long tailSkip;
    private BatchPlan batchPlan;

    Planner(Sections sections, FileSchema referenceSchema, List<RowGroup> firstFileRowGroups,
            ReadProjection projection, ResolvedPredicate filterPredicate, SchemaCompatibility schemaCompatibility,
            RowGroupPruning pruning, FetchPlanBuilder fetchPlanBuilder, long maxRows, long physicalSkip,
            long tailSkip, long indexWindowBytes) {
        this.sections = sections;
        this.referenceSchema = referenceSchema;
        this.firstFileRowGroups = firstFileRowGroups;
        this.projection = projection;
        this.filterPredicate = filterPredicate;
        this.schemaCompatibility = schemaCompatibility;
        this.pruning = pruning;
        this.fetchPlanBuilder = fetchPlanBuilder;
        this.maxRows = maxRows;
        this.indexWindowBytes = indexWindowBytes;
        this.planRowBudget = maxRows > 0 ? maxRows : Long.MAX_VALUE;
        this.planSkipRemaining = physicalSkip;
        this.tailSkip = tailSkip;
    }

    /// Whether the row budget leaves rows to plan.
    boolean budgetLeft() {
        return planRowBudget > 0;
    }

    /// The residue of the physical skip within the first work item, once it is listed.
    long firstRowGroupSkip() {
        return firstRowGroupSkip;
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

    /// The work items of `prepared`'s row groups that pruning keeps, within the read's row budget,
    /// to append to the work list.
    Listed planRowGroups(int fileIndex, PreparedFile prepared, int listedBefore) throws IOException {
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
            if (listedBefore == 0 && planned.isEmpty()) {
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
                    listedBefore + planned.size(),
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

        return new Listed(withIndexWindows(planned), filtered.event());
    }

    /// The states of one file's planned work items, each with the [IndexWindow] it falls into,
    /// whose section requests the sequence then holds.
    private List<Planning> withIndexWindows(List<WorkItem> planned) throws IOException {
        if (planned.isEmpty()) {
            return List.of();
        }
        int count = planned.size();
        List<IndexWindow.Member> members = new ArrayList<>(count);
        for (WorkItem workItem : planned) {
            members.add(indexMember(workItem));
        }
        WorkItem first = planned.getFirst();
        List<IndexWindow> windows = IndexWindow.plan(indexWindowBytes, first.inputFile(), first.workItemIndex(),
                members);
        List<Planning> listed = new ArrayList<>(count);
        int next = 0;
        for (IndexWindow window : windows) {
            window.insertSections(sections);
            for (int m = 0; m < window.memberCount(); m++) {
                listed.add(new Planning(planned.get(next++).withIndexWindow(window), sections));
            }
        }
        return listed;
    }

    /// `workItem` as its index window sees it: the columns whose ColumnIndex its page filter reads,
    /// whose OffsetIndex its plans and page filter read, and whose bloom filter its arrival may
    /// probe. A row group with a chunk in another file fails before any of them is read
    /// ([#chunkExtents]), so it needs none: its offsets address that other file.
    private IndexWindow.Member indexMember(WorkItem workItem) {
        if (!storedInThisFile(workItem.rowGroup())) {
            return new IndexWindow.Member(workItem.rowGroup(), workItem.rowGroupIndex(), new BitSet(), new BitSet(),
                    new BitSet());
        }
        BitSet columnIndexColumns = RowGroupPruning.columnIndexColumns(workItem, pruning.pageFiltering(workItem));
        return new IndexWindow.Member(workItem.rowGroup(), workItem.rowGroupIndex(),
                fetchPlanBuilder.offsetIndexColumns(workItem, columnIndexColumns), columnIndexColumns,
                workItem.bloomFilterColumns());
    }

    static boolean storedInThisFile(RowGroup rowGroup) {
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
    Need arrival(Planning planning) throws IOException {
        if (planning.arrival != null) {
            return null;
        }
        WorkItem workItem = planning.workItem;
        if (!RowGroupPruning.droppableOnArrival(workItem)) {
            planning.arrival = RowGroupPruning.Arrival.KEPT;
            return null;
        }
        chunkExtents(planning);
        try {
            ByteSource bloomFilters = planning.reads;
            if (!workItem.bloomFilterColumns().isEmpty()) {
                Need need = workItem.indexWindow().bloomFiltersNeed();
                if (need != null) {
                    return need;
                }
                bloomFilters = workItem.indexWindow().bloomFilters(workItem.workItemIndex(), planning.reads);
            }
            planning.reads.clearMissing();
            RowGroupPruning.Arrival arrival = pruning.probeOnArrival(workItem, bloomFilters, planning.reads,
                    planning.probe);
            // A filter or dictionary not yet read counted as absent: a drop stands, a keep is
            // probed again once it has arrived
            Parsed<?> missing = planning.reads.missing();
            if (!arrival.dropped() && missing != null) {
                return planning.reads.need(missing, PlanningReads.PRUNING);
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
        chunkExtents(planning);
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
    Need masked(Planning planning) throws IOException {
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
        chunkExtents(planning);
        try {
            planning.reads.clearMissing();
            Parsed<Boolean> applicable = FetchPlanBuilder.masksApplicableForRowGroup(projection.decoded(),
                    workItem.rowGroup(), workItem.fileSchema(), workItem.columnOrdinals(), planning.reads);
            if (applicable.needsBytes()) {
                return planning.reads.need(applicable, PlanningReads.INDEXES);
            }
            planning.mask = applicable.value() ? MaskCapability.YES : MaskCapability.NO;
            return null;
        }
        catch (IOException e) {
            throw metadataFailure(workItem, e);
        }
    }

    /// The fetch plans of every decoded column for the work item's row group, and then its batches.
    Need plans(Planning planning) throws IOException {
        if (planning.plans != null) {
            return null;
        }
        WorkItem workItem = planning.workItem;
        chunkExtents(planning);
        // Before the page index, which the plans need and these section requests do not: they start
        // while it is read
        insertChunkSections(planning);
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
                pageFilterApplied, perRgMaxRows(workItem), sections, chunkExtents(planning));

        // Last, once nothing above can fail: the batch plan takes each row group once.
        planning.plans = new RowGroupPlans(plans.columns(),
                planBatches(workItem, matchingRows.rowCount(rowGroup.numRows())), plans.pageFilterEvents());
        return null;
    }

    /// Inserts the section requests of `workItem`'s row group from its chunks' extents, once, where
    /// every row of it is read and nothing can drop it, so they start before its page index is
    /// read.
    private void insertChunkSections(Planning planning) throws IOException {
        WorkItem workItem = planning.workItem;
        if (readsAllRows(workItem) && !HeldBytes.inMemory(workItem.inputFile())
                && sections.sections(workItem.workItemIndex()) == null) {
            sections.insert(workItem.fileIndex(), workItem.workItemIndex(), workItem.inputFile(),
                    fetchPlanBuilder.chunkSpans(workItem, chunkExtents(planning)));
        }
    }

    /// The work item's chunk extents, checked on the first call, which every stage makes before it
    /// reads anything its row group's footer entries address: that the chunks are in this file,
    /// and then that they lie within it and do not overlap.
    ///
    /// @throws UnsupportedOperationException if a chunk is stored in another file
    /// @throws ParquetReadException if a chunk lies outside the file or two overlap
    private Extent[] chunkExtents(Planning planning) throws IOException {
        if (planning.extents == null) {
            requireSameFile(planning.workItem);
            planning.extents = fetchPlanBuilder.extents(planning.workItem);
        }
        return planning.extents;
    }

    /// Whether every row of `workItem`'s row group is read and no Bloom filter or dictionary can
    /// drop it, so its section requests follow from its chunks' extents in the footer alone: no
    /// filter narrows its pages, no tail or row limit cuts it, and its chunks are in this file.
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
        long leadingSkip = workItem.workItemIndex() == 0 ? firstRowGroupSkip : 0;
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
}
