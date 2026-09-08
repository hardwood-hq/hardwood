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
import java.util.concurrent.CompletableFuture;

import dev.hardwood.internal.ExceptionContext.ReadContext.Region;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.ReadScope;
import dev.hardwood.jfr.RowGroupScannedEvent;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
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
    private final long firstDataPageOffset; // from OffsetIndex (not necessarily first needed page)
    private final ColumnSchema columnSchema;
    private final ColumnChunk columnChunk;
    private final HardwoodContextImpl context;
    private final int rowGroupIndex;
    private final String fileName;

    private IndexedFetchPlan(List<RowGroupIterator.NeededPage> neededPages,
                              List<RowGroupIterator.PageGroup> pageGroups,
                              List<ChunkHandle> chunkHandles,
                              long firstDataPageOffset,
                              ColumnSchema columnSchema, ColumnChunk columnChunk,
                              HardwoodContextImpl context,
                              int rowGroupIndex, String fileName) {
        this.neededPages = neededPages;
        this.pageGroups = pageGroups;
        this.chunkHandles = chunkHandles;
        this.firstDataPageOffset = firstDataPageOffset;
        this.columnSchema = columnSchema;
        this.columnChunk = columnChunk;
        this.context = context;
        this.rowGroupIndex = rowGroupIndex;
        this.fileName = fileName;
    }

    @Override
    public boolean isEmpty() {
        return neededPages.isEmpty();
    }

    @Override
    public void prefetch() {
        if (!chunkHandles.isEmpty()) {
            // FetchReason.bind carries the caller's reason (e.g.
            // "prefetch rg=2") to the worker thread; otherwise the
            // underlying readRange would log as `unattributed`.
            ChunkHandle first = chunkHandles.get(0);
            CompletableFuture.runAsync(FetchReason.bind(() -> {
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
            }));
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

    /// Coalesce-safe iff the column has a single page group. Multiple
    /// groups mean a filter dropped pages, leaving intra-column gaps —
    /// bridging to neighbour columns would pull dropped bytes into the
    /// shared region and double-fetch the later page groups.
    @Override
    public boolean isCoalesceSafe() {
        return chunkHandles.size() == 1;
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
    /// @param firstDataPageOffset absolute offset of the first data page in the
    ///        OffsetIndex (may differ from `neededPages.get(0)` when filtering)
    static IndexedFetchPlan build(List<RowGroupIterator.NeededPage> neededPages,
                                   List<RowGroupIterator.PageGroup> pageGroups,
                                   List<ChunkHandle> chunkHandles,
                                   long firstDataPageOffset,
                                   ColumnSchema columnSchema, ColumnChunk columnChunk,
                                   HardwoodContextImpl context,
                                   int rowGroupIndex, String fileName) {
        return new IndexedFetchPlan(neededPages, pageGroups, chunkHandles,
                firstDataPageOffset, columnSchema, columnChunk, context,
                rowGroupIndex, fileName);
    }

    /// Iterator that lazily parses the dictionary on first access and yields
    /// [PageInfo] objects with lazy byte resolution via [ChunkHandle].
    private class IndexedPageIterator implements PageIterator {
        private Dictionary dictionary;
        private boolean dictionaryParsed;
        private boolean eventEmitted;
        private int index;
        private int currentGroupIndex;

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
                dictionary = parseDictionary();
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
            PageLocation loc = needed.location();
            ChunkHandle handle = chunkHandles.get(currentGroupIndex);
            ByteBuffer pageData = handle.slice(loc.offset(), loc.compressedPageSize());
            PageInfo page = new PageInfo(pageData, columnSchema, columnChunk.metaData(),
                    dictionary, needed.mask(), loc.offset());

            if (!eventEmitted && !hasNext()) {
                emitEvent();
            }

            return page;
        }

        private Dictionary parseDictionary() throws IOException {
            ColumnMetaData metaData = columnChunk.metaData();

            Long dictOffset = metaData.dictionaryPageOffset();
            long dictAreaStart;
            if (dictOffset != null && dictOffset > 0) {
                dictAreaStart = dictOffset;
            }
            else if (firstDataPageOffset > metaData.dataPageOffset()) {
                dictAreaStart = metaData.dataPageOffset();
            }
            else {
                return null;
            }

            if (dictAreaStart >= firstDataPageOffset) {
                return null;
            }

            int dictRegionSize = Math.toIntExact(firstDataPageOffset - dictAreaStart);
            ByteBuffer dictRegion = chunkHandles.get(0).slice(dictAreaStart, dictRegionSize);

            // The parser says what is wrong with the dictionary; where the
            // dictionary is, is this frame's to say, and a parse failure inside
            // leaves placed at the byte it stopped on.
            try (ReadScope.Scope page = ReadScope.region(Region.DICTIONARY_PAGE, dictAreaStart)) {
                return DictionaryParser.parse(dictRegion, columnSchema, metaData, context);
            }
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
