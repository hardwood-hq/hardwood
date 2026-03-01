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
import dev.hardwood.internal.reader.event.RowGroupScannedEvent;
import dev.hardwood.internal.thrift.OffsetIndexReader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.schema.ColumnSchema;

/**
 * Scans page boundaries in a single column chunk and creates PageInfo objects.
 * <p>
 * Reads page headers and parses dictionary pages upfront, then creates
 * PageInfo records that can be used for on-demand page decoding.
 * </p>
 * <p>
 * When an Offset Index is available in the file metadata, pages are located
 * by direct lookup instead of sequentially scanning all page headers.
 * </p>
 */
public class PageScanner {

    private final ColumnSchema columnSchema;
    private final ColumnChunk columnChunk;
    private final HardwoodContextImpl context;
    private final ByteBuffer fileMapping;
    private final long fileMappingBaseOffset;
    private final String filePath;
    private final int rowGroupIndex;

    /**
     * Creates a PageScanner that uses a pre-mapped file buffer.
     *
     * @param columnSchema the column schema
     * @param columnChunk the column chunk metadata
     * @param context the Hardwood context
     * @param fileMapping pre-mapped buffer covering the data region
     * @param fileMappingBaseOffset the file offset where fileMapping starts
     */
    public PageScanner(ColumnSchema columnSchema, ColumnChunk columnChunk, HardwoodContextImpl context,
                       ByteBuffer fileMapping, long fileMappingBaseOffset) {
        this(columnSchema, columnChunk, context, fileMapping, fileMappingBaseOffset, null, -1);
    }

    /**
     * Creates a PageScanner that uses a pre-mapped file buffer with file context for JFR events.
     *
     * @param columnSchema the column schema
     * @param columnChunk the column chunk metadata
     * @param context the Hardwood context
     * @param fileMapping pre-mapped buffer covering the data region
     * @param fileMappingBaseOffset the file offset where fileMapping starts
     * @param filePath the file path for JFR event reporting (may be null)
     * @param rowGroupIndex the row group index for JFR event reporting
     */
    public PageScanner(ColumnSchema columnSchema, ColumnChunk columnChunk, HardwoodContextImpl context,
                       ByteBuffer fileMapping, long fileMappingBaseOffset,
                       String filePath, int rowGroupIndex) {
        this.columnSchema = columnSchema;
        this.columnChunk = columnChunk;
        this.context = context;
        this.fileMapping = fileMapping;
        this.fileMappingBaseOffset = fileMappingBaseOffset;
        this.filePath = filePath;
        this.rowGroupIndex = rowGroupIndex;
    }

    /**
     * Scan pages in this column chunk and return PageInfo objects.
     * <p>
     * Automatically selects between index-based and sequential scanning
     * depending on whether an Offset Index is available.
     * </p>
     *
     * @return list of PageInfo objects for data pages in this chunk
     */
    public List<PageInfo> scanPages() throws IOException {
        if (columnChunk.offsetIndexOffset() != null) {
            return scanPagesFromIndex();
        }
        return scanPagesSequential();
    }

    /**
     * Scan pages by sequentially reading all page headers through the column chunk.
     *
     * @return list of PageInfo objects for data pages in this chunk
     */
    List<PageInfo> scanPagesSequential() throws IOException {
        RowGroupScannedEvent event = new RowGroupScannedEvent();
        event.begin();

        ColumnMetaData metaData = columnChunk.metaData();

        Long dictOffset = metaData.dictionaryPageOffset();
        long chunkStartOffset = (dictOffset != null && dictOffset > 0)
                ? dictOffset
                : metaData.dataPageOffset();
        long chunkSize = metaData.totalCompressedSize();

        int sliceOffset = (int) (chunkStartOffset - fileMappingBaseOffset);
        ByteBuffer buffer;
        try {
            buffer = fileMapping.slice(sliceOffset, (int) chunkSize);
        }
        catch (IndexOutOfBoundsException e) {
            throw new IOException("Invalid column chunk bounds for '" + columnSchema.name()
                    + "': chunkStart=" + chunkStartOffset
                    + ", chunkSize=" + chunkSize
                    + ", dictOffset=" + dictOffset
                    + ", dataPageOffset=" + metaData.dataPageOffset()
                    + ", mappingBase=" + fileMappingBaseOffset
                    + ", mappingSize=" + fileMapping.capacity(), e);
        }

        List<PageInfo> pageInfos = new ArrayList<>();
        long valuesRead = 0;
        int position = 0;

        Dictionary dictionary = null;

        while (valuesRead < metaData.numValues() && position < buffer.limit()) {
            ThriftCompactReader headerReader = new ThriftCompactReader(buffer, position);
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

                ByteBuffer compressedData = buffer.slice(pageDataOffset, compressedSize);
                int uncompressedSize = header.uncompressedPageSize();

                dictionary = parseDictionary(compressedData, numValues, uncompressedSize,
                    columnSchema, metaData.codec());
            }
            else if (header.type() == PageHeader.PageType.DATA_PAGE ||
                     header.type() == PageHeader.PageType.DATA_PAGE_V2) {
                ByteBuffer pageSlice = buffer.slice(position, totalPageSize);

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

        event.file = filePath;
        event.rowGroupIndex = rowGroupIndex;
        event.column = columnSchema.name();
        event.pageCount = pageInfos.size();
        event.scanStrategy = RowGroupScannedEvent.STRATEGY_SEQUENTIAL;
        event.commit();

        return pageInfos;
    }

    /**
     * Scan pages using the Offset Index for direct page location lookup.
     * <p>
     * Reads the Offset Index from the file mapping, then locates each data page
     * by its recorded offset and size. If a dictionary page exists, only the
     * dictionary page header at chunk start is parsed.
     * </p>
     *
     * @return list of PageInfo objects for data pages in this chunk
     */
    List<PageInfo> scanPagesFromIndex() throws IOException {
        RowGroupScannedEvent event = new RowGroupScannedEvent();
        event.begin();

        ColumnMetaData metaData = columnChunk.metaData();

        // Read and parse the OffsetIndex
        int indexSliceOffset = (int) (columnChunk.offsetIndexOffset() - fileMappingBaseOffset);
        ByteBuffer indexBuffer = fileMapping.slice(indexSliceOffset, columnChunk.offsetIndexLength());
        ThriftCompactReader indexReader = new ThriftCompactReader(indexBuffer);
        OffsetIndex offsetIndex = OffsetIndexReader.read(indexReader);

        if (offsetIndex.pageLocations().isEmpty()) {
            throw new IOException("Empty Offset Index for column '" + columnSchema.name()
                    + "': the Offset Index contains no page locations");
        }

        // Parse dictionary page if present
        Dictionary dictionary = null;
        Long dictOffset = metaData.dictionaryPageOffset();
        if (dictOffset == null || dictOffset <= 0) {
            // Some writers (e.g. parquet-mr <= 1.12) omit dictionary_page_offset.
            // Probe the first page at data_page_offset — if it is a dictionary page,
            // parse it here so data pages can reference it.
            int probeOffset = (int) (metaData.dataPageOffset() - fileMappingBaseOffset);
            ThriftCompactReader probeReader = new ThriftCompactReader(fileMapping, probeOffset);
            PageHeader probeHeader = PageHeaderReader.read(probeReader);
            if (probeHeader.type() == PageHeader.PageType.DICTIONARY_PAGE) {
                dictOffset = metaData.dataPageOffset();
            }
        }
        if (dictOffset != null && dictOffset > 0) {
            int dictSliceOffset = (int) (dictOffset - fileMappingBaseOffset);
            // Read just the dictionary page header
            ThriftCompactReader dictHeaderReader = new ThriftCompactReader(fileMapping, dictSliceOffset);
            PageHeader dictHeader = PageHeaderReader.read(dictHeaderReader);
            int dictHeaderSize = dictHeaderReader.getBytesRead();

            int compressedSize = dictHeader.compressedPageSize();
            ByteBuffer compressedData = fileMapping.slice(dictSliceOffset + dictHeaderSize, compressedSize);
            int numValues = dictHeader.dictionaryPageHeader().numValues();
            int uncompressedSize = dictHeader.uncompressedPageSize();

            dictionary = parseDictionary(compressedData, numValues, uncompressedSize,
                    columnSchema, metaData.codec());
        }

        // Create PageInfo for each data page using offset index locations
        List<PageInfo> pageInfos = new ArrayList<>(offsetIndex.pageLocations().size());
        for (PageLocation loc : offsetIndex.pageLocations()) {
            int pageSliceOffset = (int) (loc.offset() - fileMappingBaseOffset);
            ByteBuffer pageSlice = fileMapping.slice(pageSliceOffset, loc.compressedPageSize());

            PageInfo pageInfo = new PageInfo(
                    pageSlice,
                    columnSchema,
                    metaData,
                    dictionary
            );
            pageInfos.add(pageInfo);
        }

        event.file = filePath;
        event.rowGroupIndex = rowGroupIndex;
        event.column = columnSchema.name();
        event.pageCount = pageInfos.size();
        event.scanStrategy = RowGroupScannedEvent.STRATEGY_OFFSET_INDEX;
        event.commit();

        return pageInfos;
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
