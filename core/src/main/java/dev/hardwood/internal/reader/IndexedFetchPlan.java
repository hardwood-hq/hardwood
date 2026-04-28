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
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

import dev.hardwood.internal.FetchReason;
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
final class IndexedFetchPlan implements FetchPlan {

    private final List<PageLocation> neededPages;
    private final List<RowGroupIterator.PageGroup> pageGroups;
    private final List<ChunkHandle> chunkHandles;
    private final long firstDataPageOffset; // from OffsetIndex (not necessarily first needed page)
    private final ColumnSchema columnSchema;
    private final ColumnChunk columnChunk;
    private final HardwoodContextImpl context;
    private final int rowGroupIndex;
    private final String fileName;

    private IndexedFetchPlan(List<PageLocation> neededPages,
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
            CompletableFuture.runAsync(FetchReason.bind(chunkHandles.get(0)::ensureFetched));
        }
    }

    @Override
    public Iterator<PageInfo> pages() {
        return new PageIterator();
    }

    /// Builds an [IndexedFetchPlan]. No I/O occurs — the plan is pure metadata.
    ///
    /// @param neededPages needed page locations (filter + maxRows applied)
    /// @param pageGroups coalesced page groups within this column
    /// @param chunkHandles one ChunkHandle per page group, linked for pre-fetch
    /// @param firstDataPageOffset absolute offset of the first data page in the
    ///        OffsetIndex (may differ from `neededPages.get(0)` when filtering)
    static IndexedFetchPlan build(List<PageLocation> neededPages,
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
    private class PageIterator implements Iterator<PageInfo> {
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
        public PageInfo next() {
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

            PageLocation loc = neededPages.get(index++);
            ChunkHandle handle = chunkHandles.get(currentGroupIndex);
            ByteBuffer pageData = handle.slice(loc.offset(), loc.compressedPageSize());
            PageInfo page = new PageInfo(pageData, columnSchema, columnChunk.metaData(), dictionary);

            if (!eventEmitted && !hasNext()) {
                emitEvent();
            }

            return page;
        }

        private Dictionary parseDictionary() {
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

            try {
                return DictionaryParser.parse(dictRegion, columnSchema, metaData, context);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to parse dictionary for column '"
                        + columnSchema.name() + "'", e);
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
