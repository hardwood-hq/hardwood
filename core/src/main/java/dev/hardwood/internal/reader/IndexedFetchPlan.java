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
import java.util.NoSuchElementException;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.jfr.RowGroupScannedEvent;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.schema.ColumnSchema;

/// [FetchPlan] for columns with an OffsetIndex.
///
/// Pages are pre-computed at plan time from the OffsetIndex (with filter and
/// maxRows already applied). Byte data and dictionary parsing are deferred
/// until the iterator is first advanced — no I/O happens at plan time.
final class IndexedFetchPlan implements FetchPlan, RowGroupIterator.CoalescableFirstChunk {

    private static final System.Logger LOG =
            System.getLogger(IndexedFetchPlan.class.getName());

    private final List<RowGroupIterator.NeededPage> neededPages;
    private final List<RowGroupIterator.PageGroup> pageGroups;
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

    private IndexedFetchPlan(List<RowGroupIterator.NeededPage> neededPages,
                              List<RowGroupIterator.PageGroup> pageGroups,
                              List<ChunkHandle> chunkHandles,
                              ChunkHandle dictionaryHandle,
                              long dictionaryStart,
                              long firstDataPageOffset,
                              ColumnSchema columnSchema, ColumnChunk columnChunk,
                              HardwoodContextImpl context,
                              int rowGroupIndex, String fileName,
                              Dictionary preloadedDictionary) {
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
    }

    @Override
    public boolean isEmpty() {
        return neededPages.isEmpty();
    }

    /// Fetches the first chunk on the calling thread, which is the speculative task that
    /// planned this row group, so waiting for that task covers this read.
    @Override
    public void prefetch() {
        if (!chunkHandles.isEmpty()) {
            // The dictionary is read first, and its handle prefetches the first page group.
            ChunkHandle first = dictionaryHandle != null ? dictionaryHandle : chunkHandles.get(0);
            try {
                first.ensureFetched();
            }
            catch (IOException e) {
                // Speculative: nothing is waiting on this, and a failed prefetch
                // leaves the handle unfetched, so the demand path fetches it again
                // and reports the failure to a caller that is waiting for it.
                // DEBUG rather than WARN so a sustained backend outage does not
                // emit one line per chunk for failures that are about to be
                // reported properly.
                LOG.log(System.Logger.Level.DEBUG,
                        "Prefetch failed for the first chunk of column {0} in row group {1}"
                                + " of {2}",
                        columnSchema.name(), rowGroupIndex, fileName, e);
            }
        }
    }

    @Override
    public PageIterator pages() {
        return new IndexedPageIterator();
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
    static IndexedFetchPlan build(List<RowGroupIterator.NeededPage> neededPages,
                                   List<RowGroupIterator.PageGroup> pageGroups,
                                   List<ChunkHandle> chunkHandles,
                                   ChunkHandle dictionaryHandle,
                                   long dictionaryStart,
                                   long firstDataPageOffset,
                                   ColumnSchema columnSchema, ColumnChunk columnChunk,
                                   HardwoodContextImpl context,
                                   int rowGroupIndex, String fileName,
                                   Dictionary preloadedDictionary) {
        return new IndexedFetchPlan(neededPages, pageGroups, chunkHandles, dictionaryHandle, dictionaryStart,
                firstDataPageOffset, columnSchema, columnChunk, context,
                rowGroupIndex, fileName, preloadedDictionary);
    }

    /// Iterator that lazily parses the dictionary on first access and yields
    /// [PageInfo] objects with lazy byte resolution via [ChunkHandle].
    private class IndexedPageIterator implements PageIterator {

        private Dictionary dictionary;
        private boolean dictionaryParsed;
        /// Which page this walk is on, for a failure to name.
        private int currentPage = ExceptionContext.UNKNOWN_PAGE;
        private boolean eventEmitted;
        private int index;
        private int currentGroupIndex;

        @Override
        public int currentPage() {
            return currentPage;
        }

        @Override
        public boolean hasNext() {
            return index < neededPages.size();
        }

        @Override
        public PageInfo next() throws IOException {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            if (!dictionaryParsed) {
                // Set the flag *before* parsing so a throw from parseDictionary
                // doesn't cause a retry loop. This is safe because a throw
                // propagates to ColumnWorker.runRetriever's catch block, which
                // calls signalError → done=true → the pipeline stops; next() is
                // never re-entered on the same iterator.
                dictionaryParsed = true;
                currentPage = ExceptionContext.DICTIONARY_PAGE;
                dictionary = parseDictionary();
                currentPage = ExceptionContext.UNKNOWN_PAGE;
            }

            // Advance to next page group if needed
            while (currentGroupIndex < pageGroups.size() - 1) {
                RowGroupIterator.PageGroup nextGroup = pageGroups.get(currentGroupIndex + 1);
                if (index >= nextGroup.firstPageIndex()) {
                    currentGroupIndex++;
                }
                else {
                    break;
                }
            }

            RowGroupIterator.NeededPage needed = neededPages.get(index++);
            currentPage = needed.pageIndex();
            PageLocation loc = needed.location();
            ChunkHandle handle = chunkHandles.get(currentGroupIndex);
            ByteBuffer pageData = handle.slice(loc.offset(), loc.compressedPageSize());
            PageInfo page = new PageInfo(pageData, columnSchema, columnChunk.metaData(),
                    dictionary, needed.mask());

            if (!eventEmitted && !hasNext()) {
                emitEvent();
            }

            return page;
        }

        private Dictionary parseDictionary() throws IOException {
            if (preloadedDictionary != null) {
                return preloadedDictionary;
            }
            if (dictionaryStart == 0) {
                return null;
            }
            int dictRegionSize = Math.toIntExact(firstDataPageOffset - dictionaryStart);
            ChunkHandle holder = dictionaryHandle != null ? dictionaryHandle : chunkHandles.get(0);
            ByteBuffer dictRegion = holder.slice(dictionaryStart, dictRegionSize);

            // Not retitled on the way out. The parser says what is wrong with the
            // dictionary — a checksum that disagrees, a header that will not parse —
            // and naming the step that noticed would replace that with less.
            return DictionaryParser.parse(dictRegion, columnSchema, columnChunk.metaData(), context);
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
