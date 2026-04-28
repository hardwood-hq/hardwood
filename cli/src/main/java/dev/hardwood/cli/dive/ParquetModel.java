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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import dev.hardwood.InputFile;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.reader.ColumnIndexBuffers;
import dev.hardwood.internal.reader.Dictionary;
import dev.hardwood.internal.reader.DictionaryParser;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.internal.reader.RangeBackedInputFile;
import dev.hardwood.internal.reader.RowGroupIndexBuffers;
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
import dev.hardwood.reader.RowReader;
import dev.hardwood.s3.S3InputFile;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

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
    /// Cached set of all group node paths in the schema; computed once on
    /// first call to [#allGroupPaths]. Schema is immutable per session, so
    /// the result is safe to memoise.
    private Set<String> allGroupPathsCache;
    private int dictionaryReadCapBytes = DEFAULT_DICTIONARY_READ_CAP_BYTES;
    private static final int DICTIONARY_CACHE_CAPACITY = 4;
    private static final int PAGE_HEADER_CACHE_CAPACITY = 8;
    /// Default maximum chunk-size for a Dictionary load. Larger chunks need the
    /// user to opt in via the Dictionary-screen confirm prompt (or via
    /// `--max-dict-bytes` to raise this default). 16 MiB covers typical
    /// numeric and short-string dictionaries without accidentally loading
    /// hundreds of MB of BYTE_ARRAY values.
    private static final int DEFAULT_DICTIONARY_READ_CAP_BYTES = 16 * 1024 * 1024;

    private final Map<ChunkKey, ColumnIndex> columnIndexCache = new HashMap<>();
    private final Map<ChunkKey, OffsetIndex> offsetIndexCache = new HashMap<>();
    /// Bounded LRU: page headers are decoded once per chunk-visit and a wide
    /// table can have hundreds of chunks. Capping prevents the cache from
    /// growing unboundedly across a long dive session.
    private final java.util.LinkedHashMap<ChunkKey, List<PageHeader>> pageHeaderCache =
            new java.util.LinkedHashMap<>(PAGE_HEADER_CACHE_CAPACITY, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<ChunkKey, List<PageHeader>> eldest) {
                    return size() > PAGE_HEADER_CACHE_CAPACITY;
                }
            };
    // Per-row-group index-region prefetch. Populated lazily the first time any
    // chunk in that RG is asked for its index; subsequent column-index /
    // offset-index calls for chunks in the same RG hit this cache instead of
    // issuing their own readRange (one HTTP round-trip per RG on S3 instead of
    // one per chunk). See `_designs/COALESCED_OFFSET_INDEX_READS.md`.
    private final Map<Integer, RowGroupIndexBuffers> indexBuffersCache = new HashMap<>();
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

    /// Set of every group node's dotted path in the schema. Cached on first
    /// access — the schema is fixed for the session, but the SchemaScreen
    /// keybar consults this on every frame so a one-shot memoise saves a
    /// recursive walk per render.
    public Set<String> allGroupPaths() {
        if (allGroupPathsCache == null) {
            Set<String> out = new HashSet<>();
            SchemaNode.GroupNode root = schema.getRootNode();
            for (SchemaNode child : root.children()) {
                collectGroupPaths(child, "", out);
            }
            allGroupPathsCache = Set.copyOf(out);
        }
        return allGroupPathsCache;
    }

    private static void collectGroupPaths(SchemaNode node, String parentPath, Set<String> out) {
        if (node instanceof SchemaNode.GroupNode g) {
            String path = parentPath.isEmpty() ? g.name() : parentPath + "." + g.name();
            out.add(path);
            for (SchemaNode child : g.children()) {
                collectGroupPaths(child, path, out);
            }
        }
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
        ColumnIndexBuffers buffers = indexBuffersFor(rowGroupIndex).forColumn(columnIndex);
        ColumnIndex result = null;
        if (buffers != null && buffers.columnIndex() != null) {
            try {
                result = ColumnIndexReader.read(new ThriftCompactReader(buffers.columnIndex()));
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
        ColumnIndexBuffers buffers = indexBuffersFor(rowGroupIndex).forColumn(columnIndex);
        OffsetIndex result = null;
        if (buffers != null && buffers.offsetIndex() != null) {
            try {
                result = OffsetIndexReader.read(new ThriftCompactReader(buffers.offsetIndex()));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        offsetIndexCache.put(key, result);
        return result;
    }

    /// Lazy per-RG fetch of the contiguous offset+column index region. One
    /// `readRange` per row group instead of N per chunk — the index entries
    /// are stored contiguously in the Parquet footer, so there's no advantage
    /// to fetching them one at a time, and on remote storage a single
    /// round-trip is much cheaper than N small ones.
    private RowGroupIndexBuffers indexBuffersFor(int rowGroupIndex) {
        return indexBuffersCache.computeIfAbsent(rowGroupIndex, rg -> {
            try {
                return RowGroupIndexBuffers.fetch(inputFile, rowGroup(rg));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
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

    /// Loads the dictionary for a column chunk when the chunk is within the
    /// `dictionaryReadCapBytes` size limit. Returns `null` if the chunk is not
    /// dictionary-encoded **or** if the chunk size would exceed the cap; in
    /// the latter case, call [#dictionaryForced] to bypass and load anyway.
    public Dictionary dictionary(int rowGroupIndex, int columnIndex) {
        if (dictionaryChunkBytes(rowGroupIndex, columnIndex) > dictionaryReadCapBytes) {
            return null;
        }
        return loadDictionary(rowGroupIndex, columnIndex);
    }

    /// Bypasses the size cap. Used by the Dictionary screen's confirm-load
    /// path after the user explicitly opts into reading an oversize chunk.
    public Dictionary dictionaryForced(int rowGroupIndex, int columnIndex) {
        return loadDictionary(rowGroupIndex, columnIndex);
    }

    /// The total compressed byte size of a column chunk — used by the
    /// Dictionary screen to decide whether to show a confirm-load prompt
    /// before calling [#dictionaryForced].
    public long dictionaryChunkBytes(int rowGroupIndex, int columnIndex) {
        return chunk(rowGroupIndex, columnIndex).metaData().totalCompressedSize();
    }

    public int dictionaryReadCapBytes() {
        return dictionaryReadCapBytes;
    }

    public void setDictionaryReadCapBytes(int capBytes) {
        if (capBytes <= 0) {
            throw new IllegalArgumentException("dictionary read cap must be positive: " + capBytes);
        }
        this.dictionaryReadCapBytes = capBytes;
    }

    private Dictionary loadDictionary(int rowGroupIndex, int columnIndex) {
        ChunkKey key = new ChunkKey(rowGroupIndex, columnIndex);
        Dictionary cached = dictionaryCache.get(key);
        if (cached != null) {
            return cached;
        }
        ColumnChunk cc = chunk(rowGroupIndex, columnIndex);
        ColumnMetaData cmd = cc.metaData();
        Long dictOffset = cmd.dictionaryPageOffset();
        long chunkStart = dictOffset != null && dictOffset > 0 ? dictOffset : cmd.dataPageOffset();
        int readSize = Math.toIntExact(cmd.totalCompressedSize());
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

    /// Network usage of the underlying [InputFile] for this session.
    /// Returns `null` for local files; for [S3InputFile] (possibly
    /// behind a [RangeBackedInputFile] decorator) returns a live
    /// snapshot of request count and bytes fetched since `open()`.
    public NetStats netStats() {
        InputFile inner = inputFile instanceof RangeBackedInputFile rbf
                ? rbf.delegate()
                : inputFile;
        if (inner instanceof S3InputFile s3) {
            return new NetStats(s3.networkRequestCount(), s3.networkBytesFetched());
        }
        return null;
    }

    public record NetStats(long requestCount, long bytesFetched) {
    }

    public ParquetFileReader reader() {
        return reader;
    }

    /// Reads a page of rows starting at `firstRow`. The `consumer` is
    /// called once per row with a [RowReader] already positioned on that
    /// row.
    ///
    /// Each call builds a *fresh* cursor bounded by `head(pageSize)` and
    /// closes it before returning. The bound matters: without it, the
    /// per-column workers in the v2 pipeline race many row groups ahead
    /// of what the consumer asks for (#370), so the bytes they queue
    /// eventually get fetched — surfacing as "phantom" S3 fetches long
    /// after the user moved on. `head(pageSize)` caps the workers to
    /// exactly the rows we need; the eager close releases the
    /// iterator's metadata cache and any retained chunk-handle bytes
    /// before the next call (the dive viewport window absorbs all
    /// within-buffer navigation, so cursor reuse across calls would buy
    /// nothing).
    public void readPreviewPage(long firstRow, int pageSize, java.util.function.Consumer<RowReader> consumer)
            throws IOException {
        try (RowReader cursor = reader.buildRowReader()
                .firstRow(firstRow)
                .head(pageSize)
                .build()) {
            int read = 0;
            while (read < pageSize && cursor.hasNext()) {
                cursor.next();
                consumer.accept(cursor);
                read++;
            }
        }
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
