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
import dev.hardwood.jfr.RowGroupScannedEvent;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.schema.ColumnSchema;

/// Scans page boundaries in a single column chunk and creates PageInfo objects.
///
/// Reads page headers and parses dictionary pages upfront, then creates
/// PageInfo records that can be used for on-demand page decoding.
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
    private final ColumnIndexBuffers indexBuffers;
    private final int rowGroupIndex;
    private final String fileName;

    /// Creates a PageScanner with pre-fetched chunk data and index buffers.
    ///
    /// @param columnSchema        the column schema
    /// @param columnChunk         the column chunk metadata
    /// @param context             the Hardwood context
    /// @param chunkData           pre-fetched bytes for this column chunk
    /// @param chunkDataFileOffset absolute file offset where `chunkData` starts
    /// @param indexBuffers        pre-fetched index buffers for this column
    /// @param rowGroupIndex       the row group index for JFR event reporting
    /// @param fileName            the file name for error messages and JFR events
    public PageScanner(ColumnSchema columnSchema, ColumnChunk columnChunk, HardwoodContextImpl context,
                       ByteBuffer chunkData, long chunkDataFileOffset, ColumnIndexBuffers indexBuffers,
                       int rowGroupIndex, String fileName) {
        this.columnSchema = columnSchema;
        this.columnChunk = columnChunk;
        this.context = context;
        this.chunkData = chunkData;
        this.chunkDataFileOffset = chunkDataFileOffset;
        this.indexBuffers = indexBuffers;
        this.rowGroupIndex = rowGroupIndex;
        this.fileName = fileName;
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
        return scanPagesSequential();
    }

    /// Scan pages by sequentially reading all page headers through the column chunk.
    ///
    /// @return list of PageInfo objects for data pages in this chunk
    List<PageInfo> scanPagesSequential() throws IOException {
        RowGroupScannedEvent event = new RowGroupScannedEvent();
        event.begin();

        ColumnMetaData metaData = columnChunk.metaData();

        List<PageInfo> pageInfos = new ArrayList<>();
        long valuesRead = 0;
        int position = 0;

        Dictionary dictionary = null;

        while (valuesRead < metaData.numValues() && position < chunkData.limit()) {
            ThriftCompactReader headerReader = new ThriftCompactReader(chunkData, position);
            PageHeader header = PageHeaderReader.read(headerReader);
            int headerSize = headerReader.getBytesRead();

            int pageDataOffset = position + headerSize;
            int compressedSize = header.compressedPageSize();
            int totalPageSize = headerSize + compressedSize;

            if (header.type() == PageHeader.PageType.DICTIONARY_PAGE) {
                int numValues = header.dictionaryPageHeader().numValues();
                if (numValues < 0) {
                    throw new IOException("Invalid dictionary page for column '" + columnSchema.name()
                            + "': negative numValues (" + numValues + ")");
                }

                ByteBuffer compressedData = chunkData.slice(pageDataOffset, compressedSize);
                if (header.crc() != null) {
                    CrcValidator.assertCorrectCrc(header.crc(), compressedData, columnSchema.name());
                }
                int uncompressedSize = header.uncompressedPageSize();

                dictionary = parseDictionary(compressedData, numValues, uncompressedSize,
                    columnSchema, metaData.codec());
            }
            else if (header.type() == PageHeader.PageType.DATA_PAGE ||
                     header.type() == PageHeader.PageType.DATA_PAGE_V2) {
                ByteBuffer pageSlice = chunkData.slice(position, totalPageSize);

                PageInfo pageInfo = new PageInfo(
                    pageSlice,
                    columnSchema,
                    metaData,
                    dictionary
                );
                pageInfos.add(pageInfo);

                valuesRead += getValueCount(header);
            }

            position += totalPageSize;
        }

        if (valuesRead != metaData.numValues()) {
            throw new IOException("Value count mismatch for column '" + columnSchema.name()
                    + "': metadata declares " + metaData.numValues()
                    + " values but pages contain " + valuesRead);
        }

        event.file = fileName;
        event.rowGroupIndex = rowGroupIndex;
        event.column = columnSchema.name();
        event.pageCount = pageInfos.size();
        event.scanStrategy = RowGroupScannedEvent.STRATEGY_SEQUENTIAL;
        event.commit();

        return pageInfos;
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
        long chunkStart = chunkDataFileOffset;

        // Detect implicit dictionary (writers that omit dictionary_page_offset)
        if ((dictOffset == null || dictOffset <= 0) && firstDataPageOffset > metaData.dataPageOffset()) {
            chunkStart = metaData.dataPageOffset();
        }

        Dictionary dictionary = null;
        if (chunkStart < firstDataPageOffset) {
            dictionary = parseDictionaryFromBuffer(chunkData, chunkDataFileOffset,
                    chunkStart, firstDataPageOffset, metaData);
        }

        // Slice each data page directly from the pre-fetched chunk data
        List<PageInfo> pageInfos = new ArrayList<>(offsetIndex.pageLocations().size());
        for (PageLocation loc : offsetIndex.pageLocations()) {
            int relOffset = Math.toIntExact(loc.offset() - chunkDataFileOffset);
            ByteBuffer pageSlice = chunkData.slice(relOffset, loc.compressedPageSize());
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
