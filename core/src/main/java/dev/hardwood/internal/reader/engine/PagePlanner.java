/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.io.HeldBytes;
import dev.hardwood.internal.io.ReadRanges;
import dev.hardwood.internal.predicate.LogContext;
import dev.hardwood.internal.predicate.PageFilterEvaluator;
import dev.hardwood.internal.reader.batch.Dictionary;
import dev.hardwood.internal.reader.decode.DictionaryParser;
import dev.hardwood.internal.reader.fetch.CoalescingPolicy;
import dev.hardwood.internal.reader.plan.NeededPage;
import dev.hardwood.internal.reader.plan.RowGroupIndexBuffers;
import dev.hardwood.internal.reader.plan.RowGroupPruning;
import dev.hardwood.internal.reader.plan.UnitListing;
import dev.hardwood.internal.reader.plan.UnitPlan;
import dev.hardwood.internal.reader.select.RowRanges;
import dev.hardwood.internal.thrift.OffsetIndexReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.jfr.PageFilterEvent;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;
import jdk.jfr.Event;

/// Plans a unit's pages and ranges on its plan job, once its page index has arrived: each decoded
/// column's pages and dictionary page from its OffsetIndex, kept by the rows the ColumnIndex
/// leaves its filter and by the row limit, and the ranges they are fetched in, coalesced. Also
/// gives the planner a unit's checked chunk extents, on its probe job, and the columns a unit
/// skips. Holds no state of the read.
final class PagePlanner {

    private final ColumnSchema[] schemas;
    /// per decoded column, whether only the filter reads it
    private final boolean[] filterOnly;
    private final RowGroupPruning pruning;
    /// the leading rows of the first unit a tail read masks
    private final long maskedRows;
    private final int maxSpan = CoalescingPolicy.maxSpanBytes();

    PagePlanner(ColumnSchema[] schemas, boolean[] filterOnly, RowGroupPruning pruning, long maskedRows) {
        this.schemas = schemas;
        this.filterOnly = filterOnly;
        this.pruning = pruning;
        this.maskedRows = maskedRows;
    }

    /// Whether only the filter reads `column`.
    boolean filterOnly(int column) {
        return filterOnly[column];
    }

    /// Whether `column` is not read in `u`: a filter-only column where statistics proved every row
    /// matches, so nothing evaluates its values.
    boolean skips(UnitListing u, int column) {
        return filterOnly[column] && u.alwaysMatches();
    }

    /// `u`'s chunks' extents, checked to lie in its file.
    CoalescingPolicy.Extent[] extents(UnitListing u) throws IOException {
        u.requireSameFile();
        CoalescingPolicy.Extent[] extents = new CoalescingPolicy.Extent[schemas.length];
        for (int c = 0; c < schemas.length; c++) {
            ColumnChunk chunk = u.chunks()[c];
            extents[c] = new CoalescingPolicy.Extent(c, schemas[c].fieldPath().toString(), chunk.chunkStartOffset(),
                    chunk.chunkStartOffset() + chunk.metaData().totalCompressedSize());
        }
        return extents(u, extents);
    }

    private static CoalescingPolicy.Extent[] extents(UnitListing u, CoalescingPolicy.Extent[] extents) throws IOException {
        return checkedExtents(u.file().name(), u.file().length(), u.rowGroup(), extents);
    }

    /// The rows of `u` its filter keeps by the ColumnIndex, where it narrows them: all of them
    /// where a nested column without an OffsetIndex could not take a page mask.
    private RowRanges matching(Unit.AwaitingIndex p, List<RangeFetch> index) throws IOException {
        UnitListing u = p.listing();
        if (!pruning.pageFiltering(u)) {
            return RowRanges.ALL;
        }
        RowGroupIndexBuffers buffers = RowGroupIndexBuffers.slice(u.metaData(), p.listed().offsetIndexes(),
                p.listed().columnIndexes(),
                (offset, length) -> RangeFetch.read(index, offset, length));
        RowRanges matching = PageFilterEvaluator.computeMatchingRows(u.columnOrdinals().filter(), u.metaData(),
                buffers, new LogContext(u.file().name(), u.rowGroup()), u.columnOrdinals().boundsReadability());
        for (int c = 0; c < schemas.length && !matching.isAll() && !matching.isEmpty(); c++) {
            if (schemas[c].maxRepetitionLevel() > 0 && u.chunks()[c].offsetIndexOffset() == null) {
                return RowRanges.ALL;
            }
        }
        return matching;
    }

    /// Plans each decoded column's pages and dictionary page from its OffsetIndex, kept by the
    /// rows its filter matches, and the unit's ranges: its chunks' extents, or under page
    /// filtering the pages kept, adding to `events` what page filtering reports; on a job.
    @SuppressWarnings("unchecked")
    UnitPlan plan(Unit.AwaitingIndex p, List<RangeFetch> index, List<Event> events)
            throws IOException {
        UnitListing u = p.listing();
        u.requireSameFile();
        RowRanges matching = matching(p, index);
        // whether a predicate narrowed the pages, which is what a page filter event reports
        boolean pageFiltered = !matching.isAll();
        if (u.index() == 0 && maskedRows > 0 && matching.isAll()) {
            matching = RowRanges.range(maskedRows, u.metaData().numRows());
        }
        int n = schemas.length;
        List<NeededPage>[] pages = new List[n];
        long[] dictionaryStart = new long[n];
        long[] firstDataPage = new long[n];
        Dictionary[] preloaded = new Dictionary[n];
        long[] dictionaryEnd = new long[n];
        List<CoalescingPolicy.Need> needs = new ArrayList<>();
        // pages are read alone where a filter or the row limit leaves some of them out
        boolean pageRanges = !matching.isAll() || u.limit() < u.metaData().numRows();
        CoalescingPolicy.Extent[] extents = new CoalescingPolicy.Extent[n];
        String[] names = new String[n];
        for (int c = 0; c < n; c++) {
            ColumnChunk chunk = u.chunks()[c];
            String path = schemas[c].fieldPath().toString();
            names[c] = schemas[c].name();
            int ordinal = u.columnOrdinals().fileOrdinal(schemas[c].columnIndex());
            if (p.dictionaries() != null && p.dictionaries().loaded(ordinal) != null) {
                preloaded[c] = p.dictionaries().loaded(ordinal);
                dictionaryEnd[c] = p.dictionaries().loadedPageEnd(ordinal);
            }
            extents[c] = new CoalescingPolicy.Extent(c, path,
                    dictionaryEnd[c] > 0 ? dictionaryEnd[c] : chunk.chunkStartOffset(),
                    chunk.chunkStartOffset() + chunk.metaData().totalCompressedSize());
            if (skips(u, c)) {
                pages[c] = List.of();
                continue;
            }
            if (chunk.offsetIndexOffset() == null) {
                // scanned once its unit is batched ([HeaderScanner]), unless no row of it is kept
                pages[c] = matching.isEmpty() ? List.of() : null;
                continue;
            }
            try {
                ByteBuffer slice = RangeFetch.read(index, chunk.offsetIndexOffset(), chunk.offsetIndexLength());
                List<PageLocation> all = OffsetIndexReader.read(new ThriftCompactReader(slice), chunk, u.metaData().numRows())
                        .pageLocations();
                pages[c] = NeededPage.select(all, matching, u.metaData().numRows());
                if (u.limit() < u.metaData().numRows()) {
                    pages[c] = pages[c].stream().filter(page -> page.location().firstRowIndex() < u.limit()).toList();
                }
                if (pageFiltered) {
                    events.add(pageFilterEvent(u, schemas[c].name(), all.size(), pages[c].size()));
                }
                if (!all.isEmpty()) {
                    firstDataPage[c] = all.getFirst().offset();
                    dictionaryStart[c] = preloaded[c] != null ? 0
                            : DictionaryParser.dictionaryPageStart(chunk, firstDataPage[c]);
                    if (pageRanges && dictionaryStart[c] > 0 && !pages[c].isEmpty()) {
                        needs.add(new CoalescingPolicy.Need(c, dictionaryStart[c], firstDataPage[c]));
                    }
                }
                for (NeededPage page : pages[c]) {
                    long start = page.location().offset();
                    long end = start + page.location().compressedPageSize();
                    if (pageRanges) {
                        needs.add(new CoalescingPolicy.Need(c, start, end));
                    }
                }
            }
            catch (ParquetReadException e) {
                throw new ParquetReadException(ExceptionContext.readPrefix(u.file().name(), u.rowGroup(), path)
                        + "Failed to compute the fetch plan: " + e.getMessage(), e);
            }
        }
        extents = extents(u, extents);
        for (CoalescingPolicy.Extent extent : extents) {
            if (skips(u, extent.column()) || pageRanges && pages[extent.column()] != null) {
                continue;
            }
            long end = extent.end();
            if (pages[extent.column()] == null && u.limit() < u.metaData().numRows()) {
                // a chunk a scan reads under a row limit: its first part, the scan fetching the rest
                // as it reaches it
                end = Math.min(end, extent.start() + CoalescingPolicy.computeChunkSize(
                        Math.toIntExact(end - extent.start()), u.chunks()[extent.column()].metaData(), u.limit(), maxSpan));
            }
            for (long piece = extent.start(); piece < end; piece += maxSpan) {
                needs.add(new CoalescingPolicy.Need(extent.column(), piece, Math.min(end, piece + maxSpan)));
            }
        }
        return new UnitPlan(matching, pages, dictionaryStart, firstDataPage, preloaded, dictionaryEnd,
                CoalescingPolicy.coalesce(needs, names, u.rowGroup(), maxSpan, HeldBytes.heldFrom(u.file())));
    }

    private static PageFilterEvent pageFilterEvent(UnitListing u, String column, int totalPages, int pagesKept) {
        PageFilterEvent event = new PageFilterEvent();
        event.file = u.file().name();
        event.rowGroupIndex = u.rowGroup();
        event.column = column;
        event.totalPages = totalPages;
        event.pagesKept = pagesKept;
        event.pagesSkipped = totalPages - pagesKept;
        return event;
    }

    /// `extents`, the chunk extents of row group `rowGroupIndex` of a file of `fileLength` bytes,
    /// in offset order, validated against the file and each other.
    ///
    /// @throws ParquetReadException if one lies outside the file or two overlap
    private static CoalescingPolicy.Extent[] checkedExtents(String fileName, long fileLength, int rowGroupIndex,
                                                    CoalescingPolicy.Extent[] extents) {
        CoalescingPolicy.Extent[] sorted = extents.clone();
        Arrays.sort(sorted, Comparator.comparingLong(CoalescingPolicy.Extent::start));
        CoalescingPolicy.Extent previous = null;
        for (CoalescingPolicy.Extent extent : sorted) {
            ReadRanges.requireInFile(fileName, rowGroupIndex, extent.name(), "column chunk", extent.start(),
                    extent.end(), fileLength);
            if (extent.end() > extent.start()) {
                if (previous != null && previous.end() > extent.start()) {
                    throw new ParquetReadException(ExceptionContext.readPrefix(fileName, rowGroupIndex, extent.name())
                            + "The column chunk [" + extent.start() + ", " + extent.end()
                            + ") overlaps the column chunk of '" + previous.name() + "' [" + previous.start() + ", "
                            + previous.end() + ")");
                }
                previous = extent;
            }
        }
        return sorted;
    }
}
