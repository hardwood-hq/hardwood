/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.jfr.RowGroupScannedEvent;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.metadata.PageType;
import dev.hardwood.schema.ColumnSchema;

/// [FetchPlan] for columns with an OffsetIndex.
///
/// Pages are pre-computed at plan time from the OffsetIndex (with filter and
/// maxRows already applied). Byte data and dictionary parsing are deferred
/// until the walk is first advanced — no I/O happens at plan time.
final class IndexedFetchPlan implements FetchPlan, FetchPlanBuilder.CoalescableFirstChunk {


    private final List<FetchPlanBuilder.NeededPage> neededPages;
    private final List<FetchPlanBuilder.PageGroup> pageGroups;
    private List<ChunkHandle> chunkHandles;
    /// The dictionary page's own handle when it is too far ahead of the first needed page to be
    /// fetched with it, or `null` when the first page group holds it or the plan needs no fetch
    /// of it.
    private final ChunkHandle dictionaryHandle;
    /// Where the dictionary page starts, or `0` when the plan reads none: the chunk has no
    /// dictionary page, or pruning has read it.
    private final long dictionaryStart;
    private final long firstDataPageOffset; // from OffsetIndex (not necessarily first needed page)
    private final ColumnSchema columnSchema;
    private final ColumnChunk columnChunk;
    private final HardwoodContextImpl context;
    private final int rowGroupIndex;
    private final String fileName;
    /// The dictionary pruning has already read, or `null` when the plan reads it.
    private final Dictionary preloadedDictionary;
    /// Where the plan charges the dictionary it parses.
    private final WorkingSet.Column workingSet;

    private IndexedFetchPlan(List<FetchPlanBuilder.NeededPage> neededPages,
                              List<FetchPlanBuilder.PageGroup> pageGroups,
                              List<ChunkHandle> chunkHandles,
                              ChunkHandle dictionaryHandle,
                              long dictionaryStart,
                              long firstDataPageOffset,
                              ColumnSchema columnSchema, ColumnChunk columnChunk,
                              HardwoodContextImpl context,
                              int rowGroupIndex, String fileName,
                              Dictionary preloadedDictionary, WorkingSet.Column workingSet) {
        this.neededPages = neededPages;
        this.pageGroups = pageGroups;
        this.chunkHandles = chunkHandles;
        this.dictionaryHandle = dictionaryHandle;
        this.dictionaryStart = dictionaryStart;
        this.firstDataPageOffset = firstDataPageOffset;
        this.columnSchema = columnSchema;
        this.columnChunk = columnChunk;
        this.context = context;
        this.rowGroupIndex = rowGroupIndex;
        this.fileName = fileName;
        this.preloadedDictionary = preloadedDictionary;
        this.workingSet = workingSet;
    }

    @Override
    public boolean isEmpty() {
        return neededPages.isEmpty();
    }

    /// Fetches the first chunk on the calling thread, which is the speculative task that
    /// planned this row group, so waiting for that task covers this read. A failure leaves the
    /// handle to the demand path, which reads it again and reports the failure to a caller
    /// that is waiting for it.
    @Override
    public void prefetch() {
        if (!chunkHandles.isEmpty()) {
            // The dictionary is read first, and its handle prefetches the first page group.
            ChunkHandle first = dictionaryHandle != null ? dictionaryHandle : chunkHandles.get(0);
            first.prefetch();
        }
    }

    @Override
    public PageWalk walk() {
        return new IndexedPageWalk();
    }

    @Override
    public long firstChunkOffset() {
        return chunkHandles.isEmpty() ? 0 : chunkHandles.get(0).fileOffset();
    }

    @Override
    public int firstChunkLength() {
        return chunkHandles.isEmpty() ? 0 : chunkHandles.get(0).length();
    }

    /// Coalesce-safe iff the column has a single page group holding
    /// everything it reads. Multiple groups, or a dictionary page fetched on
    /// its own, mean a filter dropped pages, leaving intra-column gaps —
    /// bridging to neighbour columns would pull dropped bytes into the
    /// shared region and double-fetch the later page groups.
    @Override
    public boolean isCoalesceSafe() {
        return chunkHandles.size() == 1 && dictionaryHandle == null;
    }

    @Override
    public long readStart() {
        if (dictionaryHandle != null) {
            return dictionaryHandle.fileOffset();
        }
        return chunkHandles.isEmpty() ? 0 : chunkHandles.get(0).fileOffset();
    }

    @Override
    public long readEnd() {
        if (chunkHandles.isEmpty()) {
            return readStart();
        }
        ChunkHandle last = chunkHandles.get(chunkHandles.size() - 1);
        return last.fileOffset() + last.length();
    }

    /// Replaces the *first* chunk handle with a region-backed view.
    /// Subsequent page-group handles (when filter / maxRows / large
    /// chunks produced multiple groups) keep their per-column reads —
    /// cross-column coalescing only spans the first read of each column.
    @Override
    public void attachSharedRegion(SharedRegion region, int rowGroupIndex) {
        if (chunkHandles.isEmpty()) {
            return;
        }
        ChunkHandle original = chunkHandles.get(0);
        ChunkHandle replacement = new ChunkHandle(region,
                original.fileOffset(), original.length(),
                "rg=" + rowGroupIndex + " col='" + columnSchema.name()
                        + "' pageGroup=1 (region-backed)");
        if (chunkHandles.size() > 1) {
            replacement.setNextChunk(chunkHandles.get(1));
        }
        // Rebuild the immutable list with the replacement at index 0.
        List<ChunkHandle> rebuilt = new java.util.ArrayList<>(chunkHandles.size());
        rebuilt.add(replacement);
        for (int i = 1; i < chunkHandles.size(); i++) {
            rebuilt.add(chunkHandles.get(i));
        }
        this.chunkHandles = rebuilt;
    }

    /// Builds an [IndexedFetchPlan]. No I/O occurs — the plan is pure metadata.
    ///
    /// @param neededPages needed pages paired with their per-page row masks
    ///        (filter + maxRows applied)
    /// @param pageGroups coalesced page groups within this column
    /// @param chunkHandles one ChunkHandle per page group, linked for pre-fetch
    /// @param dictionaryHandle the dictionary page's own handle, linked to the first page
    ///        group's, or `null` when the first page group holds the dictionary page
    /// @param dictionaryStart where the dictionary page starts, or `0` when the plan reads none;
    ///        see [DictionaryParser#dictionaryPageStart]
    /// @param firstDataPageOffset absolute offset of the first data page in the
    ///        OffsetIndex (may differ from `neededPages.get(0)` when filtering)
    /// @param preloadedDictionary the column's dictionary if pruning has read it, in which case
    ///        the page groups leave the dictionary page out; `null` otherwise
    /// @param workingSet where the plan charges the dictionary it parses
    static IndexedFetchPlan build(List<FetchPlanBuilder.NeededPage> neededPages,
                                   List<FetchPlanBuilder.PageGroup> pageGroups,
                                   List<ChunkHandle> chunkHandles,
                                   ChunkHandle dictionaryHandle,
                                   long dictionaryStart,
                                   long firstDataPageOffset,
                                   ColumnSchema columnSchema, ColumnChunk columnChunk,
                                   HardwoodContextImpl context,
                                   int rowGroupIndex, String fileName,
                                   Dictionary preloadedDictionary, WorkingSet.Column workingSet) {
        return new IndexedFetchPlan(neededPages, pageGroups, chunkHandles, dictionaryHandle, dictionaryStart,
                firstDataPageOffset, columnSchema, columnChunk, context,
                rowGroupIndex, fileName, preloadedDictionary, workingSet);
    }

    /// Walk that parses the dictionary on first access and yields [PageInfo] objects whose bytes
    /// it takes from the page groups' [ChunkHandle]s. Its needs are known from the offset index:
    /// the dictionary's handle, then each page's group.
    private class IndexedPageWalk implements PageWalk {

        private Dictionary dictionary;
        private boolean dictionaryParsed;
        /// Which page this walk is on, for a failure to name.
        private int currentPage = ExceptionContext.UNKNOWN_PAGE;
        private boolean eventEmitted;
        private int index;
        private int currentGroupIndex;
        /// What the last advance stopped on: bytes, or a task of the read ending.
        private Need pending;

        @Override
        public int currentPage() {
            return currentPage;
        }

        @Override
        public Need pending() {
            return pending;
        }

        @Override
        public PageInfo advance() throws IOException {
            pending = null;
            if (index >= neededPages.size()) {
                return null;
            }

            if (!dictionaryParsed) {
                currentPage = ExceptionContext.DICTIONARY_PAGE;
                ByteBuffer dictRegion = dictionaryRegion();
                if (pending != null) {
                    return null;
                }
                if (dictRegion == null) {
                    dictionaryParsed = true;
                    dictionary = preloadedDictionary();
                }
                else {
                    ThriftCompactReader headerReader = new ThriftCompactReader(dictRegion, 0);
                    PageHeader header = PageHeaderReader.read(headerReader);
                    int headerSize = headerReader.getBytesRead();
                    WorkingSet.DictionaryParse parse = workingSet.parseDictionary(header.uncompressedPageSize(), () -> {
                        // Set the flag *before* parsing so a throw from the parse
                        // doesn't cause a retry loop. This is safe because a throw
                        // propagates to the column's fetch step (ScheduledColumn.runFetchStep), which
                        // calls signalError → done=true → the pipeline stops; advance() is
                        // never re-entered on the same walk.
                        dictionaryParsed = true;
                        // Not retitled on the way out. The parser says what is wrong with the
                        // dictionary — a checksum that disagrees, a header that will not parse —
                        // and naming the step that noticed would replace that with less.
                        return header.type() == PageType.DICTIONARY_PAGE
                                ? DictionaryParser.parsePage(header,
                                        dictRegion.slice(headerSize, header.compressedPageSize()), columnSchema,
                                        columnChunk.metaData(), context, workingSet)
                                : null;
                    });
                    if (parse.pending() != null) {
                        pending = parse.pending();
                        return null;
                    }
                    dictionary = parse.dictionary();
                }
                currentPage = ExceptionContext.UNKNOWN_PAGE;
            }

            // Advance to next page group if needed
            while (currentGroupIndex < pageGroups.size() - 1) {
                FetchPlanBuilder.PageGroup nextGroup = pageGroups.get(currentGroupIndex + 1);
                if (index >= nextGroup.firstPageIndex()) {
                    currentGroupIndex++;
                }
                else {
                    break;
                }
            }

            FetchPlanBuilder.NeededPage needed = neededPages.get(index);
            currentPage = needed.pageIndex();
            PageLocation loc = needed.location();
            ChunkHandle handle = chunkHandles.get(currentGroupIndex);
            ByteBuffer groupBytes = handle.poll();
            if (groupBytes == null) {
                pending = handle;
                return null;
            }
            index++;
            ByteBuffer pageData = handle.slice(groupBytes, loc.offset(), loc.compressedPageSize());
            // The header gives the buffers decoding the page takes, which the read admits before
            // it submits the page
            PageHeader header = PageHeaderReader.read(new ThriftCompactReader(pageData, 0));
            PageInfo page = PageInfo.dataPage(pageData, header, columnSchema, columnChunk.metaData(), dictionary,
                    needed.mask(), keptRecords(needed));

            if (!eventEmitted && index >= neededPages.size()) {
                emitEvent();
            }

            return page;
        }

        /// The bytes of the dictionary page and whatever lies between it and the first data page,
        /// `null` when the plan parses none or they are not present, which then is [#pending].
        private ByteBuffer dictionaryRegion() throws IOException {
            if (preloadedDictionary != null || dictionaryStart == 0) {
                return null;
            }
            int dictRegionSize = Math.toIntExact(firstDataPageOffset - dictionaryStart);
            ChunkHandle holder = dictionaryHandle != null ? dictionaryHandle : chunkHandles.get(0);
            ByteBuffer holderBytes = holder.poll();
            if (holderBytes == null) {
                pending = holder;
                return null;
            }
            return holder.slice(holderBytes, dictionaryStart, dictRegionSize);
        }

        /// The records `needed` contributes after its mask, for [PageInfo#records()]: its row span
        /// in the offset index for a column without repetition, whose records are its values; a
        /// column with repetition counts its records from its repetition levels once decoded.
        private int keptRecords(FetchPlanBuilder.NeededPage needed) {
            if (columnSchema.maxRepetitionLevel() > 0) {
                return PageInfo.UNKNOWN_RECORDS;
            }
            return needed.mask().isAll() ? needed.rows() : needed.mask().totalRecords();
        }


        /// The dictionary pruning parsed for the chunk, charged to the column as it takes it; `null`
        /// where pruning parsed none and the chunk has no dictionary page.
        private Dictionary preloadedDictionary() {
            if (preloadedDictionary != null) {
                workingSet.dictionary(preloadedDictionary);
            }
            return preloadedDictionary;
        }

        private void emitEvent() {
            eventEmitted = true;
            RowGroupScannedEvent event = new RowGroupScannedEvent();
            event.begin();
            event.file = fileName;
            event.rowGroupIndex = rowGroupIndex;
            event.column = columnSchema.name();
            event.pageCount = neededPages.size();
            event.scanStrategy = RowGroupScannedEvent.STRATEGY_OFFSET_INDEX;
            event.commit();
        }
    }
}
