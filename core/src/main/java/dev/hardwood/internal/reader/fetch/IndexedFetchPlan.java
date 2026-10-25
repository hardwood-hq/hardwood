/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.fetch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.reader.batch.Dictionary;
import dev.hardwood.internal.reader.decode.DataPage;
import dev.hardwood.internal.reader.decode.DictionaryParser;
import dev.hardwood.internal.reader.decode.PageInfo;
import dev.hardwood.internal.reader.memory.WorkingSet;
import dev.hardwood.internal.reader.select.PageRowMask;
import dev.hardwood.internal.reader.step.Need;
import dev.hardwood.jfr.RowGroupScannedEvent;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.metadata.PageType;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

/// [FetchPlan] for columns with an OffsetIndex.
///
/// Pages are pre-computed at plan time from the OffsetIndex (with filter and maxRows already
/// applied). The bytes come from the row group's [Section]s that overlap the
/// chunk, located by offset; dictionary parsing is deferred until the walk is first advanced. No
/// I/O happens at plan time.
public final class IndexedFetchPlan implements FetchPlan {

    private final List<NeededPage> neededPages;
    /// The section requests overlapping the column chunk, in offset order.
    private final List<? extends Section> sections;
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

    /// Builds an [IndexedFetchPlan]. No I/O occurs — the plan is pure metadata.
    ///
    /// @param neededPages needed pages paired with their per-page row masks
    ///        (filter + maxRows applied)
    /// @param sections the section requests overlapping the column chunk, in offset order, which
    ///        hold every needed page and the dictionary page
    /// @param dictionaryStart where the dictionary page starts, or `0` when the plan reads none;
    ///        see [DictionaryParser#dictionaryPageStart]
    /// @param firstDataPageOffset absolute offset of the first data page in the
    ///        OffsetIndex (may differ from `neededPages.get(0)` when filtering)
    /// @param preloadedDictionary the column's dictionary if pruning has read it, in which case
    ///        the plan reads no dictionary page; `null` otherwise
    /// @param workingSet where the plan charges the dictionary it parses
    public IndexedFetchPlan(List<NeededPage> neededPages, List<? extends Section> sections,
                     long dictionaryStart, long firstDataPageOffset,
                     ColumnSchema columnSchema, ColumnChunk columnChunk,
                     HardwoodContextImpl context, int rowGroupIndex, String fileName,
                     Dictionary preloadedDictionary, WorkingSet.Column workingSet) {
        this.neededPages = neededPages;
        this.sections = sections;
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

    @Override
    public PageWalk walk() {
        return new IndexedPageWalk();
    }

    /// Walk that parses the dictionary on first access and yields [PageInfo] objects whose bytes it
    /// slices from the section requests that hold them. Its needs are known from the offset index:
    /// the dictionary page's section request, then each page's.
    private class IndexedPageWalk implements PageWalk {

        private final SectionCursor cursor = new SectionCursor(sections, null);
        private Dictionary dictionary;
        private boolean dictionaryParsed;
        /// Which page this walk is on, for a failure to name.
        private int currentPage = ExceptionContext.UNKNOWN_PAGE;
        private int index;
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
        public void stop() {
            cursor.releaseAll();
        }

        @Override
        public PageInfo advance() throws IOException {
            pending = null;
            if (index >= neededPages.size()) {
                cursor.releaseAll();
                return null;
            }

            if (!dictionaryParsed) {
                currentPage = ExceptionContext.DICTIONARY_PAGE;
                if (preloadedDictionary != null || dictionaryStart == 0) {
                    dictionaryParsed = true;
                    dictionary = DictionaryStep.preloaded(preloadedDictionary, workingSet);
                }
                else {
                    ByteBuffer dictRegion = cursor.read(dictionaryStart,
                            Math.toIntExact(firstDataPageOffset - dictionaryStart));
                    if (dictRegion == null) {
                        pending = cursor.pending();
                        return null;
                    }
                    DataPage page = DataPage.parse(dictRegion);
                    int pageLength = page.headerSize() + page.header().compressedPageSize();
                    if (page.header().type() == PageType.DICTIONARY_PAGE && pageLength > dictRegion.remaining()) {
                        throw new ParquetReadException("Malformed Parquet metadata: the dictionary page header declares "
                                + pageLength + " bytes but only " + dictRegion.remaining()
                                + " bytes lie before the first data page");
                    }
                    WorkingSet.DictionaryParse parse = DictionaryStep.parse(page, columnSchema, columnChunk.metaData(),
                            context, workingSet);
                    if (parse.pending() != null) {
                        pending = parse.pending();
                        return null;
                    }
                    dictionaryParsed = true;
                    dictionary = parse.dictionary();
                }
                currentPage = ExceptionContext.UNKNOWN_PAGE;
            }

            NeededPage needed = neededPages.get(index);
            currentPage = needed.pageIndex();
            PageLocation loc = needed.location();
            cursor.releaseBefore(loc.offset());
            ByteBuffer pageData = cursor.read(loc.offset(), loc.compressedPageSize());
            if (pageData == null) {
                pending = cursor.pending();
                return null;
            }
            index++;
            // The header gives the buffers decoding the page takes, which the read admits before
            // it submits the page
            PageInfo page = new PageInfo(DataPage.parse(pageData), columnSchema, columnChunk.metaData(), dictionary,
                    needed.mask(), PageInfo.keptRecords(columnSchema, needed.mask(), needed.rows()));

            if (index == neededPages.size()) {
                PageWalk.scanned(fileName, rowGroupIndex, columnSchema, neededPages.size(),
                        RowGroupScannedEvent.STRATEGY_OFFSET_INDEX);
            }

            return page;
        }

    }

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
    public record NeededPage(PageLocation location, PageRowMask mask, int pageIndex, int rows) {}
}
