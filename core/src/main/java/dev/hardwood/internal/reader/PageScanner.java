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
import java.util.ArrayList;
import java.util.List;

import dev.hardwood.internal.compression.Decompressor;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.thrift.OffsetIndexReader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.jfr.PageFilterEvent;
import dev.hardwood.jfr.RowGroupScannedEvent;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.schema.ColumnSchema;

/// Scans page boundaries in a single column chunk and creates PageInfo objects.
///
/// Supports two usage modes:
///
/// - **Batch mode**: [#scanPages()] returns all pages as a list (used when data is
///   already pre-fetched, e.g. local files or filtered page ranges).
/// - **Iterator mode**: [#hasNextPage()] / [#nextPage()] yield pages one at a time,
///   backed by a [WindowedChunkReader] that fetches data from remote backends on demand.
///
/// When an Offset Index is available in the file metadata, pages are located
/// by direct lookup instead of sequentially scanning all page headers.
///
/// The column chunk data and index buffers are provided by the caller, which
/// pre-fetches them via [ChunkRange] and [RowGroupIndexBuffers]
/// to minimize network round-trips on remote backends.
public class PageScanner {

    private final ColumnSchema columnSchema;
    private final ColumnChunk columnChunk;
    private final HardwoodContextImpl context;
    private final ByteBuffer chunkData;
    private final long chunkDataFileOffset;
    private WindowedChunkReader windowedReader;
    private final PageRangeData pageRangeData;
    private final ColumnIndexBuffers indexBuffers;
    private final int rowGroupIndex;
    private final String fileName;
    private final RowRanges matchingRows;

    // Row limit: stop yielding pages once enough rows are covered (0 = unlimited)
    private final long maxRows;

    // Iterator state for sequential scanning
    private int position;
    private long valuesRead;
    private Dictionary dictionary;
    private boolean iteratorInitialized;
    private boolean iteratorExhausted;
    private int iteratorPageCount;
    private List<PageInfo> preScannedPages; // OffsetIndex path: pre-scanned list
    private int preScannedIndex;

    /// Creates a PageScanner with pre-fetched chunk data.
    ///
    /// @param matchingRows row ranges that might match the filter, or `RowRanges.ALL` for no filtering
    /// @param maxRows maximum rows to yield (0 = unlimited)
    public PageScanner(ColumnSchema columnSchema, ColumnChunk columnChunk, HardwoodContextImpl context,
                       ByteBuffer chunkData, long chunkDataFileOffset, ColumnIndexBuffers indexBuffers,
                       int rowGroupIndex, String fileName, RowRanges matchingRows, long maxRows) {
        this.columnSchema = columnSchema;
        this.columnChunk = columnChunk;
        this.context = context;
        this.chunkData = chunkData;
        this.chunkDataFileOffset = chunkDataFileOffset;
        this.windowedReader = null;
        this.pageRangeData = null;
        this.indexBuffers = indexBuffers;
        this.rowGroupIndex = rowGroupIndex;
        this.fileName = fileName;
        this.matchingRows = matchingRows;
        this.maxRows = maxRows;
    }

    /// Creates a PageScanner with page-range buffers for selective I/O.
    ///
    /// @param matchingRows row ranges that might match the filter
    /// @param maxRows maximum rows to yield (0 = unlimited)
    public PageScanner(ColumnSchema columnSchema, ColumnChunk columnChunk, HardwoodContextImpl context,
                       PageRangeData pageRangeData, ColumnIndexBuffers indexBuffers,
                       int rowGroupIndex, String fileName, RowRanges matchingRows, long maxRows) {
        this.columnSchema = columnSchema;
        this.columnChunk = columnChunk;
        this.context = context;
        this.chunkData = null;
        this.chunkDataFileOffset = 0;
        this.windowedReader = null;
        this.pageRangeData = pageRangeData;
        this.indexBuffers = indexBuffers;
        this.rowGroupIndex = rowGroupIndex;
        this.fileName = fileName;
        this.matchingRows = matchingRows;
        this.maxRows = maxRows;
    }

    /// Creates a PageScanner with a [WindowedChunkReader] for lazy, incremental page fetching.
    ///
    /// @param matchingRows row ranges that might match the filter, or `RowRanges.ALL` for no filtering
    /// @param maxRows maximum rows to yield (0 = unlimited)
    public PageScanner(ColumnSchema columnSchema, ColumnChunk columnChunk, HardwoodContextImpl context,
                       WindowedChunkReader windowedReader, ColumnIndexBuffers indexBuffers,
                       int rowGroupIndex, String fileName, RowRanges matchingRows, long maxRows) {
        this.columnSchema = columnSchema;
        this.columnChunk = columnChunk;
        this.context = context;
        this.chunkData = null;
        this.chunkDataFileOffset = 0;
        this.windowedReader = windowedReader;
        this.pageRangeData = null;
        this.indexBuffers = indexBuffers;
        this.rowGroupIndex = rowGroupIndex;
        this.fileName = fileName;
        this.matchingRows = matchingRows;
        this.maxRows = maxRows;
    }

    /// Scan pages in this column chunk and return PageInfo objects.
    ///
    /// Automatically selects between index-based and sequential scanning
    /// depending on whether an Offset Index is available.
    ///
    /// @return list of PageInfo objects for data pages in this chunk
    public List<PageInfo> scanPages() throws IOException {
        if (columnChunk.offsetIndexOffset() != null) {
            return scanPagesFromIndex();
        }
        // Drain the iterator (JFR event emitted by hasNextPage() when exhausted)
        List<PageInfo> pages = new ArrayList<>();
        while (hasNextPage()) {
            pages.add(nextPage());
        }
        return pages;
    }

    // ==================== Iterator API ====================

    /// Returns the column schema for this scanner.
    public ColumnSchema columnSchema() {
        return columnSchema;
    }

    /// Returns the column metadata for this scanner.
    public ColumnMetaData columnMetaData() {
        return columnChunk.metaData();
    }

    /// Check if there are more data pages to scan.
    /// On the first call, initializes the iterator by parsing any dictionary page
    /// (sequential path) or pre-scanning all pages via OffsetIndex (index path).
    ///
    /// @return true if [#nextPage()] will return a page
    public boolean hasNextPage() throws IOException {
        if (iteratorExhausted) {
            return false;
        }
        // Stop early if we've yielded enough rows for the consumer's limit
        if (maxRows > 0 && valuesRead >= maxRows) {
            iteratorExhausted = true;
            emitRowGroupScannedEvent(iteratorPageCount);
            return false;
        }
        if (!iteratorInitialized) {
            initializeIterator();
        }
        // OffsetIndex path: iterate over pre-scanned list
        if (preScannedPages != null) {
            boolean hasMore = preScannedIndex < preScannedPages.size();
            if (!hasMore) {
                iteratorExhausted = true;
            }
            return hasMore;
        }
        // Sequential path: check value count and buffer position
        ColumnMetaData metaData = columnChunk.metaData();
        WindowedChunkReader reader = effectiveReader();
        boolean hasMore = valuesRead < metaData.numValues() && position < reader.totalLength();
        if (!hasMore) {
            iteratorExhausted = true;
            emitRowGroupScannedEvent(iteratorPageCount);
            // Validate value count when all pages have been read
            if (valuesRead != metaData.numValues()) {
                throw new IOException("Value count mismatch for column '" + columnSchema.name()
                        + "': metadata declares " + metaData.numValues()
                        + " values but pages contain " + valuesRead);
            }
        }
        return hasMore;
    }

    /// Returns the next data page. Must only be called when [#hasNextPage()] returns true.
    ///
    /// @return the next PageInfo
    public PageInfo nextPage() throws IOException {
        if (!hasNextPage()) {
            return null;
        }

        // OffsetIndex path: return from pre-scanned list
        if (preScannedPages != null) {
            return preScannedPages.get(preScannedIndex++);
        }

        WindowedChunkReader reader = effectiveReader();
        ColumnMetaData metaData = columnChunk.metaData();

        while (valuesRead < metaData.numValues() && position < reader.totalLength()) {
            // Read page header — need enough bytes for the Thrift header (typically 20-50 bytes)
            // Fetch a small peek first, then the full page
            int peekSize = Math.min(256, reader.totalLength() - position);
            ByteBuffer headerBuf = reader.slice(position, peekSize);
            ThriftCompactReader headerReader = new ThriftCompactReader(headerBuf);
            PageHeader header = PageHeaderReader.read(headerReader);
            int headerSize = headerReader.getBytesRead();

            int compressedSize = header.compressedPageSize();
            int totalPageSize = headerSize + compressedSize;

            if (header.type() == PageHeader.PageType.DICTIONARY_PAGE) {
                // Dictionary pages should have been handled in initializeIterator()
                // but handle gracefully if encountered mid-stream
                position += totalPageSize;
                continue;
            }

            if (header.type() == PageHeader.PageType.DATA_PAGE ||
                    header.type() == PageHeader.PageType.DATA_PAGE_V2) {
                ByteBuffer pageSlice = reader.slice(position, totalPageSize);
                PageInfo pageInfo = new PageInfo(pageSlice, columnSchema, metaData, dictionary);
                valuesRead += getValueCount(header);
                position += totalPageSize;
                iteratorPageCount++;
                return pageInfo;
            }

            // Skip unknown page types
            position += totalPageSize;
        }

        iteratorExhausted = true;
        return null;
    }

    /// Initializes the iterator. For the OffsetIndex path, pre-scans all pages via
    /// `scanPagesFromIndex()`. For the sequential path, scans past any dictionary page.
    private void initializeIterator() throws IOException {
        iteratorInitialized = true;

        // OffsetIndex path: pre-scan all pages (data is already selectively fetched)
        if (columnChunk.offsetIndexOffset() != null && (pageRangeData != null || chunkData != null)) {
            preScannedPages = scanPagesFromIndex();
            preScannedIndex = 0;
            return;
        }

        position = 0;
        valuesRead = 0;
        dictionary = null;

        WindowedChunkReader reader = effectiveReader();
        ColumnMetaData metaData = columnChunk.metaData();

        // Scan forward past dictionary page (if present)
        if (position < reader.totalLength()) {
            int peekSize = Math.min(256, reader.totalLength() - position);
            ByteBuffer headerBuf = reader.slice(position, peekSize);
            ThriftCompactReader headerReader = new ThriftCompactReader(headerBuf);
            PageHeader header = PageHeaderReader.read(headerReader);
            int headerSize = headerReader.getBytesRead();

            if (header.type() == PageHeader.PageType.DICTIONARY_PAGE) {
                int pageDataOffset = position + headerSize;
                int compressedSize = header.compressedPageSize();
                int numValues = header.dictionaryPageHeader().numValues();
                if (numValues < 0) {
                    throw new IOException("Invalid dictionary page for column '" + columnSchema.name()
                            + "': negative numValues (" + numValues + ")");
                }

                ByteBuffer compressedData = reader.slice(pageDataOffset, compressedSize);
                if (header.crc() != null) {
                    CrcValidator.assertCorrectCrc(header.crc(), compressedData, columnSchema.name());
                }
                int uncompressedSize = header.uncompressedPageSize();

                dictionary = parseDictionary(compressedData, numValues, uncompressedSize,
                        columnSchema, metaData.codec());

                position += headerSize + compressedSize;
            }
        }
    }

    /// Returns the windowed reader. When constructed with pre-fetched `chunkData`
    /// (no `WindowedChunkReader`), creates a wrapper that delegates to `chunkData` slices.
    private WindowedChunkReader effectiveReader() {
        if (windowedReader != null) {
            return windowedReader;
        }
        if (chunkData != null) {
            // Lazily wrap chunkData. No I/O — slice() is a zero-copy ByteBuffer operation.
            windowedReader = new WindowedChunkReader(null, 0, chunkData.limit()) {
                @Override
                public ByteBuffer slice(int relativeOffset, int length) {
                    return chunkData.slice(relativeOffset, length);
                }
            };
            return windowedReader;
        }
        throw new IllegalStateException(
                "No data source available — PageScanner requires either chunkData or a WindowedChunkReader");
    }

    /// Emits a JFR event summarizing the sequential scan.
    private void emitRowGroupScannedEvent(int pageCount) {
        RowGroupScannedEvent event = new RowGroupScannedEvent();
        event.file = fileName;
        event.rowGroupIndex = rowGroupIndex;
        event.column = columnSchema.name();
        event.pageCount = pageCount;
        event.scanStrategy = RowGroupScannedEvent.STRATEGY_SEQUENTIAL;
        event.commit();
    }

    /// Scan pages by sequentially reading all page headers through the column chunk.
    /// This always uses the sequential path regardless of OffsetIndex availability,
    /// making it suitable for testing both paths independently.
    ///
    /// @return list of PageInfo objects for data pages in this chunk
    List<PageInfo> scanPagesSequential() throws IOException {
        WindowedChunkReader reader = effectiveReader();
        ColumnMetaData metaData = columnChunk.metaData();
        long seqValuesRead = 0;
        int seqPosition = 0;
        Dictionary seqDictionary = null;

        List<PageInfo> pages = new ArrayList<>();

        while (seqValuesRead < metaData.numValues() && seqPosition < reader.totalLength()) {
            int peekSize = Math.min(256, reader.totalLength() - seqPosition);
            ByteBuffer headerBuf = reader.slice(seqPosition, peekSize);
            ThriftCompactReader headerReader = new ThriftCompactReader(headerBuf);
            PageHeader header = PageHeaderReader.read(headerReader);
            int headerSize = headerReader.getBytesRead();

            int compressedSize = header.compressedPageSize();
            int totalPageSize = headerSize + compressedSize;

            if (header.type() == PageHeader.PageType.DICTIONARY_PAGE) {
                int numValues = header.dictionaryPageHeader().numValues();
                if (numValues < 0) {
                    throw new IOException("Invalid dictionary page for column '" + columnSchema.name()
                            + "': negative numValues (" + numValues + ")");
                }
                int pageDataOffset = seqPosition + headerSize;
                ByteBuffer compressedData = reader.slice(pageDataOffset, compressedSize);
                if (header.crc() != null) {
                    CrcValidator.assertCorrectCrc(header.crc(), compressedData, columnSchema.name());
                }
                seqDictionary = parseDictionary(compressedData, numValues,
                        header.uncompressedPageSize(), columnSchema, metaData.codec());
            }
            else if (header.type() == PageHeader.PageType.DATA_PAGE ||
                     header.type() == PageHeader.PageType.DATA_PAGE_V2) {
                ByteBuffer pageSlice = reader.slice(seqPosition, totalPageSize);
                pages.add(new PageInfo(pageSlice, columnSchema, metaData, seqDictionary));
                seqValuesRead += getValueCount(header);
            }

            seqPosition += totalPageSize;
        }

        if (seqValuesRead != metaData.numValues()) {
            throw new IOException("Value count mismatch for column '" + columnSchema.name()
                    + "': metadata declares " + metaData.numValues()
                    + " values but pages contain " + seqValuesRead);
        }

        emitRowGroupScannedEvent(pages.size());
        return pages;
    }

    /// Scan pages using the Offset Index for direct page location lookup.
    ///
    /// Parses the Offset Index from the pre-fetched index buffers, then
    /// slices each data page directly from the pre-fetched chunk data.
    ///
    /// @return list of PageInfo objects for data pages in this chunk
    List<PageInfo> scanPagesFromIndex() throws IOException {
        RowGroupScannedEvent event = new RowGroupScannedEvent();
        event.begin();

        ColumnMetaData metaData = columnChunk.metaData();

        // Parse the OffsetIndex from pre-fetched index buffers
        ByteBuffer indexBuffer = indexBuffers.offsetIndex();
        OffsetIndex offsetIndex = OffsetIndexReader.read(new ThriftCompactReader(indexBuffer));

        if (offsetIndex.pageLocations().isEmpty()) {
            throw new IOException("Empty Offset Index for column '" + columnSchema.name()
                    + "': the Offset Index contains no page locations");
        }

        // Parse dictionary from the chunk data prefix (if present)
        long firstDataPageOffset = offsetIndex.pageLocations().get(0).offset();
        Long dictOffset = metaData.dictionaryPageOffset();
        long chunkStart;

        if (dictOffset != null && dictOffset > 0) {
            // Explicit dictionary offset
            chunkStart = dictOffset;
        }
        else if (firstDataPageOffset > metaData.dataPageOffset()) {
            // Implicit dictionary (writers that omit dictionary_page_offset)
            chunkStart = metaData.dataPageOffset();
        }
        else {
            chunkStart = (pageRangeData != null) ? firstDataPageOffset : chunkDataFileOffset;
        }

        Dictionary dictionary = null;
        if (chunkStart < firstDataPageOffset) {
            if (pageRangeData != null) {
                // Page-range mode: dictionary is in the first fetched range (via extendStart)
                int dictRegionSize = Math.toIntExact(firstDataPageOffset - chunkStart);
                ByteBuffer dictSlice = pageRangeData.sliceRegion(chunkStart, dictRegionSize);
                dictionary = parseDictionaryFromSlice(dictSlice, metaData);
            }
            else {
                dictionary = parseDictionaryFromBuffer(chunkData, chunkDataFileOffset,
                        chunkStart, firstDataPageOffset, metaData);
            }
        }

        // Filter page locations by matching row ranges (Column Index pushdown)
        List<PageLocation> allPages = offsetIndex.pageLocations();
        List<PageLocation> filteredPages = filterPageLocations(allPages);

        // Slice each data page from the appropriate data source
        List<PageInfo> pageInfos = new ArrayList<>(filteredPages.size());
        for (PageLocation loc : filteredPages) {
            ByteBuffer pageSlice;
            if (pageRangeData != null) {
                pageSlice = pageRangeData.slicePage(loc);
            }
            else {
                int relOffset = Math.toIntExact(loc.offset() - chunkDataFileOffset);
                pageSlice = chunkData.slice(relOffset, loc.compressedPageSize());
            }
            pageInfos.add(new PageInfo(pageSlice, columnSchema, metaData, dictionary));
        }

        event.file = fileName;
        event.rowGroupIndex = rowGroupIndex;
        event.column = columnSchema.name();
        event.pageCount = pageInfos.size();
        event.scanStrategy = RowGroupScannedEvent.STRATEGY_OFFSET_INDEX;
        event.commit();

        return pageInfos;
    }

    /// Filters page locations to only those overlapping with matching row ranges.
    /// Returns all pages if no row ranges filter is active.
    private List<PageLocation> filterPageLocations(List<PageLocation> pages) {
        if (matchingRows.isAll()) {
            return pages;
        }

        int totalPages = pages.size();
        List<PageLocation> filtered = new ArrayList<>();

        for (int i = 0; i < totalPages; i++) {
            long pageFirstRow = pages.get(i).firstRowIndex();
            long pageLastRow = (i + 1 < totalPages)
                    ? pages.get(i + 1).firstRowIndex()
                    : Long.MAX_VALUE;

            if (matchingRows.overlapsPage(pageFirstRow, pageLastRow)) {
                filtered.add(pages.get(i));
            }
        }

        int pagesSkipped = totalPages - filtered.size();
        if (pagesSkipped > 0) {
            PageFilterEvent filterEvent = new PageFilterEvent();
            filterEvent.file = fileName;
            filterEvent.rowGroupIndex = rowGroupIndex;
            filterEvent.column = columnSchema.name();
            filterEvent.totalPages = totalPages;
            filterEvent.pagesKept = filtered.size();
            filterEvent.pagesSkipped = pagesSkipped;
            filterEvent.commit();
        }

        return filtered;
    }

    /// Parses a dictionary page from a pre-sliced buffer (page-range mode).
    private Dictionary parseDictionaryFromSlice(ByteBuffer dictSlice, ColumnMetaData metaData) throws IOException {
        ThriftCompactReader probeReader = new ThriftCompactReader(dictSlice, 0);
        PageHeader header = PageHeaderReader.read(probeReader);

        if (header.type() != PageHeader.PageType.DICTIONARY_PAGE) {
            return null;
        }

        int headerSize = probeReader.getBytesRead();
        int compressedSize = header.compressedPageSize();
        ByteBuffer compressedData = dictSlice.slice(headerSize, compressedSize);
        if (header.crc() != null) {
            CrcValidator.assertCorrectCrc(header.crc(), compressedData, columnSchema.name());
        }

        return parseDictionary(compressedData,
                header.dictionaryPageHeader().numValues(),
                header.uncompressedPageSize(),
                columnSchema, metaData.codec());
    }

    /// Parses a dictionary page from a buffer. The dictionary region sits
    /// between `dictAreaStart` and `firstDataPageOffset`.
    private Dictionary parseDictionaryFromBuffer(ByteBuffer buffer, long bufferFileOffset,
            long dictAreaStart, long firstDataPageOffset, ColumnMetaData metaData) throws IOException {

        int dictRelOffset = Math.toIntExact(dictAreaStart - bufferFileOffset);
        int dictRegionSize = Math.toIntExact(firstDataPageOffset - dictAreaStart);
        ByteBuffer dictSlice = buffer.slice(dictRelOffset, dictRegionSize);

        ThriftCompactReader probeReader = new ThriftCompactReader(dictSlice, 0);
        PageHeader header = PageHeaderReader.read(probeReader);

        if (header.type() != PageHeader.PageType.DICTIONARY_PAGE) {
            return null;
        }

        int headerSize = probeReader.getBytesRead();
        int compressedSize = header.compressedPageSize();
        ByteBuffer compressedData = dictSlice.slice(headerSize, compressedSize);
        if (header.crc() != null) {
            CrcValidator.assertCorrectCrc(header.crc(), compressedData, columnSchema.name());
        }

        return parseDictionary(compressedData,
                header.dictionaryPageHeader().numValues(),
                header.uncompressedPageSize(),
                columnSchema, metaData.codec());
    }

    private Dictionary parseDictionary(ByteBuffer compressedData, int numValues,
            int uncompressedSize, ColumnSchema column, CompressionCodec codec) throws IOException {
        int compressedSize = compressedData.remaining();
        try {
            Decompressor decompressor = context.decompressorFactory().getDecompressor(codec);
            byte[] data = decompressor.decompress(compressedData, uncompressedSize);
            return Dictionary.parse(data, numValues, column.type(), column.typeLength());
        }
        catch (Exception e) {
            throw new IOException("Failed to parse dictionary for column '" + column.name()
                    + "' (type=" + column.type()
                    + ", numValues=" + numValues
                    + ", uncompressedSize=" + uncompressedSize
                    + ", compressedSize=" + compressedSize
                    + ", codec=" + codec + ")", e);
        }
    }

    private long getValueCount(PageHeader header) {
        return switch (header.type()) {
            case DATA_PAGE -> header.dataPageHeader().numValues();
            case DATA_PAGE_V2 -> header.dataPageHeaderV2().numValues();
            case DICTIONARY_PAGE -> 0;
            case INDEX_PAGE -> 0;
        };
    }
}
