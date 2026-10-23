/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.predicate.BoundsReadability;
import dev.hardwood.internal.predicate.PageDropPredicates;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.RowGroupIterator.SharedRowGroupMetadata;
import dev.hardwood.internal.reader.RowGroupIterator.WorkItem;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.internal.schema.ReadProjection;
import dev.hardwood.internal.thrift.OffsetIndexReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.jfr.PageFilterEvent;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.metadata.PageType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// Builds the fetch plans of a row group's decoded columns: selects each column's pages by the
/// row group's matching rows and row limit, coalesces them into page groups, and coalesces the
/// first reads of the columns across columns ([#columnPlans]).
public final class FetchPlanBuilder {

    private final ReadProjection projection;
    private final HardwoodContextImpl context;
    private final PrefetchTasks prefetchTasks;
    private final WorkingSet workingSet;

    /// AND-necessary leaves per column index, derived once from the read's filter.
    /// Feeds [SequentialFetchPlan]'s inline-stats page-drop check.
    private final Map<Integer, List<ResolvedPredicate>> dropLeavesByColumn;

    FetchPlanBuilder(ReadProjection projection, ResolvedPredicate filter, boolean metadataFilteringEnabled,
                     HardwoodContextImpl context, PrefetchTasks prefetchTasks, WorkingSet workingSet) {
        this.projection = projection;
        this.context = context;
        this.prefetchTasks = prefetchTasks;
        this.workingSet = workingSet;
        this.dropLeavesByColumn = filter != null && metadataFilteringEnabled
                ? PageDropPredicates.byColumn(filter) : Map.of();
    }

    /// The file ordinals of the columns whose OffsetIndex the read of `workItem` needs: every
    /// decoded column the row group reads, to plan its page fetches, and every column in
    /// `columnIndexColumns`, to map its pages to rows. A skipped filter-only column needs none.
    BitSet offsetIndexColumns(WorkItem workItem, BitSet columnIndexColumns) {
        BitSet columns = (BitSet) columnIndexColumns.clone();
        ProjectedSchema decoded = projection.decoded();
        int projectedCount = decoded.getProjectedColumnCount();
        for (int projCol = 0; projCol < projectedCount; projCol++) {
            if (!skipsColumn(workItem, projCol)) {
                columns.set(workItem.columnOrdinals().fileOrdinal(decoded.toOriginalIndex(projCol)));
            }
        }
        return columns;
    }

    /// Builds the fetch plan of every decoded column of `workItem`'s row group from its offset
    /// indexes, then coalesces them across columns. Reads nothing: the plans fetch lazily.
    FetchPlan[] columnPlans(WorkItem workItem, SharedRowGroupMetadata shared, RowRanges matchingRows,
                                    boolean pageFilterApplied, long perRgMaxRows) {
        int projectedCount = projection.decoded().getProjectedColumnCount();
        RowGroup rowGroup = workItem.rowGroup();
        InputFile inputFile = workItem.inputFile();
        FetchPlan[] plans = new FetchPlan[projectedCount];

        for (int projCol = 0; projCol < projectedCount; projCol++) {
            int originalIndex = projection.decoded().toOriginalIndex(projCol);
            int fileOrdinal = workItem.columnOrdinals().fileOrdinal(originalIndex);
            ColumnChunk columnChunk = rowGroup.columns().get(fileOrdinal);
            ColumnSchema columnSchema = workItem.fileSchema().getColumn(fileOrdinal);
            if (skipsColumn(workItem, projCol)) {
                plans[projCol] = SkippedColumnFetchPlan.INSTANCE;
                continue;
            }

            ColumnIndexBuffers colBuffers = shared.indexBuffers().forColumn(fileOrdinal);
            Dictionary preloadedDictionary = shared.dictionaries() == null
                    ? null : shared.dictionaries().loaded(fileOrdinal);

            if (colBuffers.offsetIndex() == null) {
                // No row of the row group survived page filtering, so no page of this
                // column is read; an indexed column reaches the same plan below, after
                // reporting its skipped pages.
                if (matchingRows.isEmpty()) {
                    plans[projCol] = FetchPlan.EMPTY;
                    continue;
                }
                // No OffsetIndex — sequential lazy fetching. Per-page drops via
                // inline DataPageHeader.statistics and per-page row masks both
                // happen inside SequentialFetchPlan. Inline page statistics are bounds
                // like any other, so a column whose bounds this file records in an order
                // this reader cannot read hands over no leaves to drop pages with (#1179).
                // The leaves are in the reference file's ordinals, so the column's order
                // travels with them rather than as the file's readability.
                BoundsReadability readability = workItem.columnOrdinals().boundsReadability();
                List<ResolvedPredicate> leaves = readability.readable(fileOrdinal)
                        ? dropLeavesByColumn.getOrDefault(originalIndex, List.of())
                        : List.of();
                plans[projCol] = SequentialFetchPlan.build(
                        inputFile, columnSchema, columnChunk,
                        context, workItem.rowGroupIndex(), inputFile.name(),
                        perRgMaxRows, leaves, BoundsReadability.ofOneColumn(readability.int96TimestampOrder(fileOrdinal)),
                        matchingRows, rowGroup.numRows(), preloadedDictionary,
                        shared.dictionaries() == null ? 0 : shared.dictionaries().loadedPageEnd(fileOrdinal),
                        prefetchTasks, workingSet.column(projCol, columnSchema));
                continue;
            }

            try {
                OffsetIndex offsetIndex = OffsetIndexReader.read(
                        new ThriftCompactReader(colBuffers.offsetIndex()), columnChunk.metaData());
                List<PageLocation> allPages = offsetIndex.pageLocations();

                // Determine needed pages (filter + maxRows). Each entry pairs a
                // PageLocation with its PageRowMask so the column keeps only the
                // records inside the matching ranges.
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
                        context, workItem.rowGroupIndex(), inputFile.name(), preloadedDictionary,
                        workingSet.column(projCol, columnSchema));
            }
            catch (ParquetReadException e) {
                throw new ParquetReadException(ExceptionContext.readPrefix(inputFile.name(),
                        workItem.rowGroupIndex(), columnSchema.fieldPath().toString())
                        + "Failed to compute the fetch plan: " + e.getMessage(), e);
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
    boolean skipsColumn(WorkItem workItem, int projCol) {
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
    /// which records within the page the column keeps. The mask is
    /// [PageRowMask#ALL] when no filter is active or when the page falls
    /// entirely inside the matching ranges.
    ///
    /// `pageIndex` is the page's ordinal in the whole column chunk, not its position in
    /// this list: a filter drops pages, and a failure that named the position would send
    /// a reader to a page the offset index numbers differently.
    ///
    /// `rows` is the page's row span in the offset index, before the mask.
    record NeededPage(PageLocation location, PageRowMask mask, int pageIndex, int rows) {}

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
    /// records the column keeps.
    private static List<NeededPage> computeNeededPages(List<PageLocation> allPages,
                                                       RowRanges matchingRows,
                                                       long rowGroupRowCount) {
        if (matchingRows.isAll()) {
            List<NeededPage> needed = new ArrayList<>(allPages.size());
            for (int i = 0; i < allPages.size(); i++) {
                needed.add(new NeededPage(allPages.get(i), PageRowMask.ALL, i,
                        pageRows(allPages, i, rowGroupRowCount)));
            }
            return needed;
        }
        List<NeededPage> needed = new ArrayList<>();
        for (int i = 0; i < allPages.size(); i++) {
            long pageFirstRow = allPages.get(i).firstRowIndex();
            long pageLastRow = pageFirstRow + pageRows(allPages, i, rowGroupRowCount);
            PageRowMask mask = matchingRows.maskForPage(pageFirstRow, pageLastRow);
            if (mask != null) {
                needed.add(new NeededPage(allPages.get(i), mask, i, Math.toIntExact(pageLastRow - pageFirstRow)));
            }
        }
        return needed;
    }

    /// The rows of page `i` by the offset index: up to the next page's first row, or to the end
    /// of the row group for the last page.
    private static int pageRows(List<PageLocation> allPages, int i, long rowGroupRowCount) {
        long pageLastRow = (i + 1 < allPages.size())
                ? allPages.get(i + 1).firstRowIndex()
                : rowGroupRowCount;
        return Math.toIntExact(pageLastRow - allPages.get(i).firstRowIndex());
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
}
