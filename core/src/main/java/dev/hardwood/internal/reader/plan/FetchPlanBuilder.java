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
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.io.HeldBytes;
import dev.hardwood.internal.io.ReadRanges;
import dev.hardwood.internal.predicate.BoundsReadability;
import dev.hardwood.internal.predicate.PageDropPredicates;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.batch.Dictionary;
import dev.hardwood.internal.reader.decode.DictionaryParser;
import dev.hardwood.internal.reader.decode.PageFormatProbe;
import dev.hardwood.internal.reader.fetch.CoalescingPolicy;
import dev.hardwood.internal.reader.fetch.CoalescingPolicy.Extent;
import dev.hardwood.internal.reader.fetch.CoalescingPolicy.Range;
import dev.hardwood.internal.reader.fetch.EntryCursor;
import dev.hardwood.internal.reader.fetch.FetchPlan;
import dev.hardwood.internal.reader.fetch.FetchSequence;
import dev.hardwood.internal.reader.fetch.IndexedFetchPlan;
import dev.hardwood.internal.reader.fetch.SequentialFetchPlan;
import dev.hardwood.internal.reader.fetch.SkippedColumnFetchPlan;
import dev.hardwood.internal.reader.footer.FileColumnOrdinals;
import dev.hardwood.internal.reader.memory.WorkingSet;
import dev.hardwood.internal.reader.plan.RowGroupIterator.SharedRowGroupMetadata;
import dev.hardwood.internal.reader.plan.RowGroupIterator.WorkItem;
import dev.hardwood.internal.reader.select.PageRowMask;
import dev.hardwood.internal.reader.select.RowRanges;
import dev.hardwood.internal.reader.step.ByteSource;
import dev.hardwood.internal.reader.step.Parsed;
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

/// Builds the fetch plans of a row group's decoded columns ([#columnPlans]): selects each column's
/// pages by the row group's matching rows and row limit, and coalesces the needed ranges of every
/// column at once, under the gap G and the span S ([CoalescingPolicy]), into the row group's
/// entries of the read's [FetchSequence]. Each plan then takes its pages' bytes from the entries
/// that overlap its column chunk, located by offset.
///
/// A row group whose rows are all read and that no Bloom filter or dictionary can drop has its
/// entries built from its chunks' extents in the footer ([#chunkSpans]), before its page index is
/// read, so they can start ahead of it; its plans then locate their pages in those entries.
public final class FetchPlanBuilder {

    private final ReadProjection projection;
    private final HardwoodContextImpl context;
    private final WorkingSet workingSet;
    /// S, the largest request, fixed for the read.
    private final int maxSpan;

    /// AND-necessary leaves per column index, derived once from the read's filter.
    /// Feeds [SequentialFetchPlan]'s inline-stats page-drop check.
    private final Map<Integer, List<ResolvedPredicate>> dropLeavesByColumn;

    FetchPlanBuilder(ReadProjection projection, ResolvedPredicate filter, boolean metadataFilteringEnabled,
                     HardwoodContextImpl context, WorkingSet workingSet, int maxSpan) {
        this.projection = projection;
        this.context = context;
        this.workingSet = workingSet;
        this.maxSpan = maxSpan;
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

    /// The fetch plans of a row group's decoded columns, and the [PageFilterEvent]s their page
    /// selection reports, which the read commits once it reaches the row group.
    record Plans(FetchPlan[] columns, List<PageFilterEvent> pageFilterEvents) {
    }

    /// Builds the fetch plan of every decoded column of `workItem`'s row group from its offset
    /// indexes, inserting the row group's entries into `sequence` unless [#chunkSpans] did. Reads
    /// nothing: the entries start by the sequence's rules.
    ///
    /// @param chunkExtents the work item's validated chunk extents ([#extents])
    /// @throws ParquetReadException if a needed range lies outside its column chunk
    Plans columnPlans(WorkItem workItem, SharedRowGroupMetadata shared, RowRanges matchingRows,
                      boolean pageFilterApplied, long perRgMaxRows, FetchSequence sequence, Extent[] chunkExtents) {
        int projectedCount = projection.decoded().getProjectedColumnCount();
        RowGroup rowGroup = workItem.rowGroup();
        InputFile inputFile = workItem.inputFile();
        FetchPlan[] plans = new FetchPlan[projectedCount];
        List<PageFilterEvent> events = new ArrayList<>();
        ColumnRead[] reads = new ColumnRead[projectedCount];
        List<Range> ranges = new ArrayList<>();

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
                reads[projCol] = sequentialRead(workItem, projCol, columnSchema, columnChunk, preloadedDictionary,
                        shared.dictionaries() == null ? 0 : shared.dictionaries().loadedPageEnd(fileOrdinal),
                        perRgMaxRows, ranges);
                continue;
            }

            try {
                OffsetIndex offsetIndex = OffsetIndexReader.read(
                        new ThriftCompactReader(colBuffers.offsetIndex()), columnChunk.metaData());
                List<PageLocation> allPages = offsetIndex.pageLocations();

                // Determine needed pages (filter + maxRows). Each entry pairs a
                // PageLocation with its PageRowMask so the column keeps only the
                // records inside the matching ranges.
                List<IndexedFetchPlan.NeededPage> neededPages = computeNeededPages(
                        allPages, matchingRows, rowGroup.numRows());

                // Report the page-level filter's effect before `truncateToMaxRows`,
                // so a `head(N)` cap is not counted as pages the predicate skipped,
                // and before the empty-plan shortcut below, so the fully-pruned
                // column — the most effective case — is reported too.
                if (pageFilterApplied) {
                    events.add(pageFilterEvent(inputFile.name(), workItem.rowGroupIndex(), columnSchema.name(),
                            allPages.size(), neededPages.size()));
                }

                if (neededPages.isEmpty()) {
                    plans[projCol] = FetchPlan.EMPTY;
                    continue;
                }

                neededPages = truncateToMaxRows(neededPages, perRgMaxRows);

                // The dictionary page is needed unless pruning has read it.
                long firstDataPageOffset = allPages.get(0).offset();
                long dictStart = preloadedDictionary == null
                        ? DictionaryParser.dictionaryPageStart(columnChunk, firstDataPageOffset) : 0;
                if (dictStart > 0) {
                    ranges.add(new Range(projCol, dictStart, firstDataPageOffset));
                }
                for (IndexedFetchPlan.NeededPage page : neededPages) {
                    ranges.add(new Range(projCol, page.location().offset(),
                            page.location().offset() + page.location().compressedPageSize()));
                }
                reads[projCol] = new ColumnRead(projCol, columnSchema, columnChunk, neededPages, dictStart,
                        firstDataPageOffset, preloadedDictionary, 0, 0, false);
            }
            catch (ParquetReadException e) {
                throw new ParquetReadException(ExceptionContext.readPrefix(inputFile.name(),
                        workItem.rowGroupIndex(), columnSchema.fieldPath().toString())
                        + "Failed to compute the fetch plan: " + e.getMessage(), e);
            }
        }

        Extent[] extents = read(chunkExtents, reads);
        checkWithinExtents(workItem, ranges, reads);
        List<FetchSequence.Entry> entries = sequence.entries(workItem.workItemIndex());
        if (entries == null) {
            ranges.sort(Comparator.comparingLong(Range::start));
            entries = sequence.insert(workItem.workItemIndex(), inputFile,
                    CoalescingPolicy.coalesce(ranges, extents, names(workItem), workItem.rowGroupIndex(), maxSpan,
                            HeldBytes.heldFrom(inputFile)));
        }
        for (Extent extent : extents) {
            ColumnRead read = reads[extent.column()];
            List<FetchSequence.Entry> overlapping = overlapping(entries, extent.start(), extent.end());
            plans[extent.column()] = read.neededPages() != null
                    ? new IndexedFetchPlan(read.neededPages(), overlapping, read.dictionaryStart(),
                            read.firstDataPageOffset(), read.schema(), read.chunk(), context,
                            workItem.rowGroupIndex(), inputFile.name(), read.preloadedDictionary(),
                            workingSet.column(extent.column(), read.schema()))
                    : sequentialPlan(workItem, read, overlapping, matchingRows, perRgMaxRows, sequence);
        }
        return new Plans(plans, events);
    }

    /// The needs of a column without an OffsetIndex: its chunk from its first data page on, whole,
    /// or under a row limit the first part [SequentialFetchPlan#computeChunkSize] sizes, which it
    /// adds to `ranges` in pieces of at most S.
    private ColumnRead sequentialRead(WorkItem workItem, int projCol, ColumnSchema columnSchema,
                                      ColumnChunk columnChunk, Dictionary preloadedDictionary, long preloadedEnd,
                                      long perRgMaxRows, List<Range> ranges) {
        long start = columnChunk.chunkStartOffset();
        int length = SequentialFetchPlan.chunkLength(columnChunk, workItem.rowGroupIndex(),
                workItem.inputFile().name(), columnSchema);
        if (preloadedDictionary != null) {
            // The data pages start where the dictionary page ends, which pruning has read.
            Objects.checkFromToIndex(start, preloadedEnd, start + length);
            length -= Math.toIntExact(preloadedEnd - start);
            start = preloadedEnd;
        }
        int firstPart = Math.min(length, SequentialFetchPlan.computeChunkSize(length, columnChunk.metaData(),
                perRgMaxRows, maxSpan));
        for (long piece = start; piece < start + firstPart; piece += maxSpan) {
            ranges.add(new Range(projCol, piece, Math.min(start + firstPart, piece + maxSpan)));
        }
        return new ColumnRead(projCol, columnSchema, columnChunk, null, 0, 0, preloadedDictionary, start,
                length, firstPart < length);
    }

    /// The plan of a column without an OffsetIndex over `entries`, inserting the rest of its chunk
    /// on demand where a row limit left it out.
    private SequentialFetchPlan sequentialPlan(WorkItem workItem, ColumnRead read, List<FetchSequence.Entry> entries,
                                               RowRanges matchingRows, long perRgMaxRows, FetchSequence sequence) {
        int fileOrdinal = workItem.columnOrdinals().fileOrdinal(projection.decoded().toOriginalIndex(read.column()));
        // Inline page statistics are bounds like any other, so a column whose bounds this file
        // records in an order this reader cannot read hands over no leaves to drop pages with
        // (#1179). The leaves are in the reference file's ordinals, so the column's order travels
        // with them rather than as the file's readability.
        BoundsReadability readability = workItem.columnOrdinals().boundsReadability();
        List<ResolvedPredicate> leaves = readability.readable(fileOrdinal)
                ? dropLeavesByColumn.getOrDefault(projection.decoded().toOriginalIndex(read.column()), List.of())
                : List.of();
        String name = read.schema().name();
        EntryCursor.Rest rest = !read.restLeftOut() ? null
                : EntryCursor.rest(sequence, workItem.workItemIndex(), workItem.inputFile(), read.start() + read.length(),
                        "rg=" + workItem.rowGroupIndex() + " col=" + name, maxSpan);
        return new SequentialFetchPlan(read.start(), read.length(), read.schema(), read.chunk(), context,
                perRgMaxRows, workItem.rowGroupIndex(), workItem.inputFile().name(), leaves,
                BoundsReadability.ofOneColumn(readability.int96TimestampOrder(fileOrdinal)),
                matchingRows, workItem.rowGroup().numRows(), read.preloadedDictionary(),
                workingSet.column(read.column(), read.schema()), entries, rest);
    }

    /// The entries of a row group whose rows are all read and that no Bloom filter or dictionary
    /// can drop, from its chunks' extents in the footer: every decoded column the row group reads,
    /// whole, cut into pieces of at most S and coalesced under G and S. Its plans locate their
    /// pages in them once its page index is read.
    ///
    /// @param chunkExtents the work item's validated chunk extents ([#extents])
    List<FetchSequence.Span> chunkSpans(WorkItem workItem, Extent[] chunkExtents) {
        List<Range> ranges = new ArrayList<>();
        for (Extent extent : chunkExtents) {
            for (long piece = extent.start(); piece < extent.end(); piece += maxSpan) {
                ranges.add(new Range(extent.column(), piece, Math.min(extent.end(), piece + maxSpan)));
            }
        }
        return CoalescingPolicy.coalesce(ranges, chunkExtents, names(workItem), workItem.rowGroupIndex(), maxSpan,
                HeldBytes.heldFrom(workItem.inputFile()));
    }

    /// What one decoded column of a row group reads: an indexed column its needed pages and
    /// dictionary page, one without an OffsetIndex its chunk from `start` on, `length` bytes, of
    /// which a row limit may leave the rest out (`restLeftOut`).
    private record ColumnRead(int column, ColumnSchema schema, ColumnChunk chunk, List<IndexedFetchPlan.NeededPage> neededPages,
                              long dictionaryStart, long firstDataPageOffset, Dictionary preloadedDictionary,
                              long start, int length, boolean restLeftOut) {
    }

    /// The chunk extents of every decoded column `workItem`'s row group reads, in offset order,
    /// validated against the file and each other. Planning takes them once, before any stage
    /// reads the bytes of a chunk, so a chunk the footer misplaces fails as such whichever stage
    /// reaches it first.
    ///
    /// @throws ParquetReadException if one lies outside the file or two overlap
    Extent[] extents(WorkItem workItem) throws IOException {
        long fileLength = workItem.inputFile().length();
        List<Extent> extents = new ArrayList<>();
        int projectedCount = projection.decoded().getProjectedColumnCount();
        for (int projCol = 0; projCol < projectedCount; projCol++) {
            if (skipsColumn(workItem, projCol)) {
                continue;
            }
            int fileOrdinal = workItem.columnOrdinals().fileOrdinal(projection.decoded().toOriginalIndex(projCol));
            ColumnChunk chunk = workItem.rowGroup().columns().get(fileOrdinal);
            long start = chunk.chunkStartOffset();
            extents.add(new Extent(projCol, workItem.fileSchema().getColumn(fileOrdinal).fieldPath().toString(),
                    start, start + chunk.metaData().totalCompressedSize()));
        }
        extents.sort(Comparator.comparingLong(Extent::start));
        Extent previous = null;
        for (Extent extent : extents) {
            ReadRanges.requireInFile(workItem.inputFile().name(), workItem.rowGroupIndex(), extent.name(),
                    "column chunk", extent.start(), extent.end(), fileLength);
            if (extent.end() > extent.start()) {
                if (previous != null && previous.end() > extent.start()) {
                    throw new ParquetReadException(ExceptionContext.readPrefix(workItem.inputFile().name(),
                            workItem.rowGroupIndex(), extent.name()) + "The column chunk [" + extent.start() + ", "
                            + extent.end() + ") overlaps the column chunk of '" + previous.name() + "' ["
                            + previous.start() + ", " + previous.end() + ")");
                }
                previous = extent;
            }
        }
        return extents.toArray(new Extent[0]);
    }

    /// The extents of `chunkExtents` whose columns `reads` holds.
    private static Extent[] read(Extent[] chunkExtents, ColumnRead[] reads) {
        List<Extent> read = new ArrayList<>(chunkExtents.length);
        for (Extent extent : chunkExtents) {
            if (reads[extent.column()] != null) {
                read.add(extent);
            }
        }
        return read.toArray(new Extent[0]);
    }

    /// Checks that every needed range lies within its column's chunk.
    ///
    /// @throws ParquetReadException naming the column of the first range that does not
    private static void checkWithinExtents(WorkItem workItem, List<Range> ranges, ColumnRead[] reads) {
        for (Range range : ranges) {
            ColumnChunk chunk = reads[range.column()].chunk();
            long start = chunk.chunkStartOffset();
            long end = start + chunk.metaData().totalCompressedSize();
            if (range.start() < start || range.end() > end || range.end() < range.start()) {
                throw new ParquetReadException(ExceptionContext.readPrefix(workItem.inputFile().name(),
                        workItem.rowGroupIndex(), reads[range.column()].schema().fieldPath().toString())
                        + "The range [" + range.start() + ", " + range.end() + ") lies outside the column chunk ["
                        + start + ", " + end + ")");
            }
        }
    }

    /// The names of the decoded columns `workItem`'s row group reads, by decoded column.
    private String[] names(WorkItem workItem) {
        String[] names = new String[projection.decoded().getProjectedColumnCount()];
        for (int projCol = 0; projCol < names.length; projCol++) {
            int fileOrdinal = workItem.columnOrdinals().fileOrdinal(projection.decoded().toOriginalIndex(projCol));
            names[projCol] = workItem.fileSchema().getColumn(fileOrdinal).name();
        }
        return names;
    }

    /// The entries of `entries`, in offset order, that overlap `[start, end)`.
    private static List<FetchSequence.Entry> overlapping(List<FetchSequence.Entry> entries, long start, long end) {
        List<FetchSequence.Entry> overlapping = new ArrayList<>();
        for (FetchSequence.Entry entry : entries) {
            if (entry.offset() < end && entry.end() > start) {
                overlapping.add(entry);
            }
        }
        return overlapping;
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

    /// Determines which pages are needed based on the filter's matching row ranges,
    /// pairing each kept page with the [PageRowMask] that selects which of its
    /// records the column keeps.
    private static List<IndexedFetchPlan.NeededPage> computeNeededPages(List<PageLocation> allPages,
                                                       RowRanges matchingRows,
                                                       long rowGroupRowCount) {
        if (matchingRows.isAll()) {
            List<IndexedFetchPlan.NeededPage> needed = new ArrayList<>(allPages.size());
            for (int i = 0; i < allPages.size(); i++) {
                needed.add(new IndexedFetchPlan.NeededPage(allPages.get(i), PageRowMask.ALL, i,
                        pageRows(allPages, i, rowGroupRowCount)));
            }
            return needed;
        }
        List<IndexedFetchPlan.NeededPage> needed = new ArrayList<>();
        for (int i = 0; i < allPages.size(); i++) {
            long pageFirstRow = allPages.get(i).firstRowIndex();
            long pageLastRow = pageFirstRow + pageRows(allPages, i, rowGroupRowCount);
            PageRowMask mask = matchingRows.maskForPage(pageFirstRow, pageLastRow);
            if (mask != null) {
                needed.add(new IndexedFetchPlan.NeededPage(allPages.get(i), mask, i, Math.toIntExact(pageLastRow - pageFirstRow)));
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

    /// A [PageFilterEvent] for one column chunk whose pages were narrowed by Column Index
    /// push-down, for the read to commit once it reaches the row group.
    ///
    /// Nothing is reported unless a predicate actually narrowed this row group's pages:
    /// a read with no filter, a `tail(N)` whose synthesized range is the only thing
    /// narrowing the pages, and a row group whose mask gate is closed all skip nothing
    /// by push-down and so report nothing. The absence of the event is therefore the
    /// signal that no page was a candidate for skipping, and a `pagesSkipped` of 0
    /// means the filter ran and kept everything.
    private static PageFilterEvent pageFilterEvent(String fileName, int rowGroupIndex, String column,
                                                   int totalPages, int pagesKept) {
        PageFilterEvent event = new PageFilterEvent();
        event.file = fileName;
        event.rowGroupIndex = rowGroupIndex;
        event.column = column;
        event.totalPages = totalPages;
        event.pagesKept = pagesKept;
        event.pagesSkipped = totalPages - pagesKept;
        return event;
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
    public static Parsed<Boolean> masksApplicableForRowGroup(ProjectedSchema projectedSchema,
                                                             RowGroup rowGroup, FileSchema fileSchema,
                                                             FileColumnOrdinals columnOrdinals,
                                                             ByteSource bytes) throws IOException {
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
            Parsed<PageType> type = PageFormatProbe.firstDataPageType(bytes, columnChunk);
            if (type.needsBytes()) {
                return type.asNeed();
            }
            if (type.value() == PageType.DATA_PAGE_V2) {
                continue;
            }
            return Parsed.of(false);
        }
        return Parsed.of(true);
    }

    /// Truncates a page list to cover at most `maxRows` rows. `maxRows <= 0`
    /// means "no row bound" and returns every page unchanged — the same
    /// `0 = unlimited` convention used throughout the fetch path.
    private static List<IndexedFetchPlan.NeededPage> truncateToMaxRows(List<IndexedFetchPlan.NeededPage> pages, long maxRows) {
        if (maxRows <= 0) {
            return pages;
        }
        List<IndexedFetchPlan.NeededPage> truncated = new ArrayList<>();
        for (IndexedFetchPlan.NeededPage page : pages) {
            if (page.location().firstRowIndex() >= maxRows) {
                break;
            }
            truncated.add(page);
        }
        return truncated;
    }
}
