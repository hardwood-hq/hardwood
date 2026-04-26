/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dev.hardwood.InputFile;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.reader.Dictionary;
import dev.hardwood.internal.reader.DictionaryParser;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.internal.thrift.ColumnIndexReader;
import dev.hardwood.internal.thrift.OffsetIndexReader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnIndex;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

/// Snapshot of a Parquet file exposed to the dive screens.
///
/// Opens the file eagerly at construction, reading the footer and schema so any I/O
/// error surfaces before the TUI enters raw mode. The underlying [ParquetFileReader]
/// is held open for the session and closed via [AutoCloseable]. Aggregate facts
/// (compressed/uncompressed totals, ratio) are computed once and cached in [Facts].
public final class ParquetModel implements AutoCloseable {

    private final String displayPath;
    private final long fileSizeBytes;
    private final InputFile inputFile;
    private final ParquetFileReader reader;
    private final FileMetaData metadata;
    private final FileSchema schema;
    private final Facts facts;
    private int previewPageSize = 20;
    private static final int DICTIONARY_CACHE_CAPACITY = 4;
    private static final int DICTIONARY_READ_CAP_BYTES = 4 * 1024 * 1024;

    private final Map<ChunkKey, ColumnIndex> columnIndexCache = new HashMap<>();
    private final Map<ChunkKey, OffsetIndex> offsetIndexCache = new HashMap<>();
    private final Map<ChunkKey, List<PageHeader>> pageHeaderCache = new HashMap<>();
    private final java.util.LinkedHashMap<ChunkKey, Dictionary> dictionaryCache =
            new java.util.LinkedHashMap<>(DICTIONARY_CACHE_CAPACITY, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<ChunkKey, Dictionary> eldest) {
                    return size() > DICTIONARY_CACHE_CAPACITY;
                }
            };

    private ParquetModel(String displayPath, long fileSizeBytes, InputFile inputFile, ParquetFileReader reader) {
        this.displayPath = displayPath;
        this.fileSizeBytes = fileSizeBytes;
        this.inputFile = inputFile;
        this.reader = reader;
        this.metadata = reader.getFileMetaData();
        this.schema = reader.getFileSchema();
        this.facts = computeFacts();
    }

    public static ParquetModel open(InputFile inputFile, String displayPath) throws IOException {
        ParquetFileReader reader = ParquetFileReader.open(inputFile);
        try {
            return new ParquetModel(displayPath, inputFile.length(), inputFile, reader);
        }
        catch (RuntimeException e) {
            reader.close();
            throw e;
        }
    }

    public String displayPath() {
        return displayPath;
    }

    public long fileSizeBytes() {
        return fileSizeBytes;
    }

    public FileMetaData metadata() {
        return metadata;
    }

    public FileSchema schema() {
        return schema;
    }

    public Facts facts() {
        return facts;
    }

    public int rowGroupCount() {
        return metadata.rowGroups().size();
    }

    public int columnCount() {
        return schema.getColumnCount();
    }

    public RowGroup rowGroup(int index) {
        return metadata.rowGroups().get(index);
    }

    public ColumnChunk chunk(int rowGroupIndex, int columnIndex) {
        return rowGroup(rowGroupIndex).columns().get(columnIndex);
    }

    /// Reads the column index for a chunk, caching the result for the session.
    /// Returns `null` when the chunk has no column index.
    public ColumnIndex columnIndex(int rowGroupIndex, int columnIndex) {
        ChunkKey key = new ChunkKey(rowGroupIndex, columnIndex);
        if (columnIndexCache.containsKey(key)) {
            return columnIndexCache.get(key);
        }
        ColumnChunk cc = chunk(rowGroupIndex, columnIndex);
        Long offset = cc.columnIndexOffset();
        Integer length = cc.columnIndexLength();
        ColumnIndex result = null;
        if (offset != null && length != null && length > 0) {
            try {
                ByteBuffer buffer = inputFile.readRange(offset, length);
                result = ColumnIndexReader.read(new ThriftCompactReader(buffer));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        columnIndexCache.put(key, result);
        return result;
    }

    /// Reads the offset index for a chunk, caching the result for the session.
    /// Returns `null` when the chunk has no offset index.
    public OffsetIndex offsetIndex(int rowGroupIndex, int columnIndex) {
        ChunkKey key = new ChunkKey(rowGroupIndex, columnIndex);
        if (offsetIndexCache.containsKey(key)) {
            return offsetIndexCache.get(key);
        }
        ColumnChunk cc = chunk(rowGroupIndex, columnIndex);
        Long offset = cc.offsetIndexOffset();
        Integer length = cc.offsetIndexLength();
        OffsetIndex result = null;
        if (offset != null && length != null && length > 0) {
            try {
                ByteBuffer buffer = inputFile.readRange(offset, length);
                result = OffsetIndexReader.read(new ThriftCompactReader(buffer));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        offsetIndexCache.put(key, result);
        return result;
    }

    /// Walks a column chunk's byte range and returns its page headers (dictionary
    /// header first if present, then data pages). Cached after the first call.
    public List<PageHeader> pageHeaders(int rowGroupIndex, int columnIndex) {
        ChunkKey key = new ChunkKey(rowGroupIndex, columnIndex);
        List<PageHeader> cached = pageHeaderCache.get(key);
        if (cached != null) {
            return cached;
        }
        ColumnChunk cc = chunk(rowGroupIndex, columnIndex);
        ColumnMetaData cmd = cc.metaData();
        Long dictOffset = cmd.dictionaryPageOffset();
        long start = dictOffset != null && dictOffset > 0 ? dictOffset : cmd.dataPageOffset();
        long totalBytes = cmd.totalCompressedSize();
        List<PageHeader> headers = new ArrayList<>();
        try {
            ByteBuffer buffer = inputFile.readRange(start, Math.toIntExact(totalBytes));
            int position = 0;
            while (position < buffer.limit()) {
                ThriftCompactReader tcr = new ThriftCompactReader(buffer, position);
                PageHeader header = PageHeaderReader.read(tcr);
                headers.add(header);
                int headerSize = tcr.getBytesRead();
                position += headerSize + header.compressedPageSize();
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        List<PageHeader> result = List.copyOf(headers);
        pageHeaderCache.put(key, result);
        return result;
    }

    /// Loads the dictionary for a column chunk, caching the most recent entries to
    /// bound memory for wide BYTE_ARRAY dictionaries. Returns `null` if the chunk
    /// is not dictionary-encoded.
    public Dictionary dictionary(int rowGroupIndex, int columnIndex) {
        ChunkKey key = new ChunkKey(rowGroupIndex, columnIndex);
        Dictionary cached = dictionaryCache.get(key);
        if (cached != null) {
            return cached;
        }
        ColumnChunk cc = chunk(rowGroupIndex, columnIndex);
        ColumnMetaData cmd = cc.metaData();
        Long dictOffset = cmd.dictionaryPageOffset();
        long chunkStart = dictOffset != null && dictOffset > 0 ? dictOffset : cmd.dataPageOffset();
        int readSize = Math.toIntExact(Math.min(cmd.totalCompressedSize(), DICTIONARY_READ_CAP_BYTES));
        try (HardwoodContextImpl context = HardwoodContextImpl.create(1)) {
            ByteBuffer region = inputFile.readRange(chunkStart, readSize);
            Dictionary dict = DictionaryParser.parse(region, schema.getColumn(columnIndex), cmd, context);
            if (dict != null) {
                dictionaryCache.put(key, dict);
            }
            return dict;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public InputFile inputFile() {
        return inputFile;
    }

    public ParquetFileReader reader() {
        return reader;
    }

    public int previewPageSize() {
        return previewPageSize;
    }

    public void setPreviewPageSize(int pageSize) {
        if (pageSize <= 0) {
            throw new IllegalArgumentException("preview page size must be positive: " + pageSize);
        }
        this.previewPageSize = pageSize;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    private record ChunkKey(int rowGroupIndex, int columnIndex) {
    }

    private Facts computeFacts() {
        long compressed = 0;
        long uncompressed = 0;
        for (RowGroup rg : metadata.rowGroups()) {
            for (ColumnChunk cc : rg.columns()) {
                ColumnMetaData cmd = cc.metaData();
                compressed += cmd.totalCompressedSize();
                uncompressed += cmd.totalUncompressedSize();
            }
        }
        double ratio = compressed == 0 ? 0.0 : (double) uncompressed / compressed;
        Map<String, String> kv = metadata.keyValueMetadata();
        List<Map.Entry<String, String>> kvList = kv == null ? List.of() : new ArrayList<>(kv.entrySet());
        return new Facts(
                metadata.version(),
                metadata.createdBy(),
                metadata.numRows(),
                metadata.rowGroups().size(),
                schema.getColumnCount(),
                compressed,
                uncompressed,
                ratio,
                List.copyOf(kvList));
    }

    /// Pre-aggregated file-level facts. Everything here is cheap to display; derived
    /// from the metadata at model construction time so screens don't recompute.
    public record Facts(
            int formatVersion,
            String createdBy,
            long totalRows,
            int rowGroupCount,
            int columnCount,
            long compressedBytes,
            long uncompressedBytes,
            double compressionRatio,
            List<Map.Entry<String, String>> keyValueMetadata) {
    }
}
