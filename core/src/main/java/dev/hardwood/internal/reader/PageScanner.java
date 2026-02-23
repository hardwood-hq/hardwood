/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;

import dev.hardwood.internal.compression.Decompressor;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.schema.ColumnSchema;

/**
 * Scans page boundaries in a single column chunk and creates PageInfo objects.
 * <p>
 * Reads page headers and parses dictionary pages upfront, then creates
 * PageInfo records that can be used for on-demand page decoding.
 * </p>
 */
public class PageScanner {

    private final ColumnSchema columnSchema;
    private final ColumnChunk columnChunk;
    private final HardwoodContextImpl context;
    private final MappedByteBuffer fileMapping;
    private final long fileMappingBaseOffset;

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
                       MappedByteBuffer fileMapping, long fileMappingBaseOffset) {
        this.columnSchema = columnSchema;
        this.columnChunk = columnChunk;
        this.context = context;
        this.fileMapping = fileMapping;
        this.fileMappingBaseOffset = fileMappingBaseOffset;
    }

    /**
     * Scan pages in this column chunk and return PageInfo objects.
     * <p>
     * Dictionary pages are parsed upfront and the parsed dictionary is shared
     * with all data page PageInfo objects. Each PageInfo receives a ByteBuffer
     * slice of the pre-mapped chunk, avoiding per-page memory mapping overhead.
     * </p>
     *
     * @return list of PageInfo objects for data pages in this chunk
     */
    public List<PageInfo> scanPages() throws IOException {
        ColumnMetaData metaData = columnChunk.metaData();

        Long dictOffset = metaData.dictionaryPageOffset();
        long chunkStartOffset = (dictOffset != null && dictOffset > 0)
                ? dictOffset
                : metaData.dataPageOffset();
        long chunkSize = metaData.totalCompressedSize();

        int sliceOffset = (int) (chunkStartOffset - fileMappingBaseOffset);
        MappedByteBuffer buffer;
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
                MappedByteBuffer compressedData = buffer.slice(pageDataOffset, compressedSize);
                int numValues = header.dictionaryPageHeader().numValues();
                int uncompressedSize = header.uncompressedPageSize();

                dictionary = parseDictionary(compressedData, numValues, uncompressedSize,
                    columnSchema, metaData.codec());
            }
            else if (header.type() == PageHeader.PageType.DATA_PAGE ||
                     header.type() == PageHeader.PageType.DATA_PAGE_V2) {
                MappedByteBuffer pageSlice = buffer.slice(position, totalPageSize);

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

        return pageInfos;
    }

    private Dictionary parseDictionary(MappedByteBuffer compressedData, int numValues,
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
