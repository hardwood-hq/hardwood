/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import dev.hardwood.InputFile;
import dev.hardwood.internal.metadata.DataPageHeader;
import dev.hardwood.internal.metadata.DataPageHeaderV2;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.predicate.PageDropPredicates;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.jfr.RowGroupScannedEvent;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.schema.ColumnSchema;

/// [FetchPlan] for columns without an OffsetIndex.
///
/// Pages are discovered lazily by scanning headers from fixed-size
/// [ChunkHandle]s. The column chunk is split into uniform chunks.
/// A single `ChunkHandle` serves both header scanning and page data
/// resolution.
///
/// Each time a new chunk is entered, the next chunk's `ChunkHandle` is
/// created and chained for one-ahead pre-fetch. When `ensureFetched()`
/// completes on chunk N, it triggers an async fetch of chunk N+1. When
/// the retriever later advances to N+1 (already fetched), a new handle
/// for N+2 is created and chained.
///
/// Chunk size controls the `readRange()` granularity:
///
/// - Without `maxRows`: `min(chunkLength, 128 MB)` — full column chunk
///   in one fetch for most columns.
/// - With `maxRows`: sized from the column's average compressed
///   bytes-per-value (`totalCompressedSize / numValues`) multiplied by
///   `maxRows` and a safety factor, floored at one page size and
///   capped at the default ceiling.
public final class SequentialFetchPlan implements FetchPlan {

    /// Minimum chunk size when `maxRows` is active (1 MB).
    /// Sized to roughly one Parquet data page so header scanning does not
    /// require sub-page round-trips.
    private static final int MAX_ROWS_CHUNK_FLOOR = 1024 * 1024;

    /// Safety factor applied to the `maxRows * avgBytesPerValue` estimate
    /// to absorb per-value size skew within the column chunk.
    private static final int MAX_ROWS_CHUNK_SAFETY_FACTOR = 2;

    /// Chunk size when reading without a row limit (128 MB). Also used as
    /// the ceiling for the `maxRows` dynamic estimate.
    /// Overridable via the `hardwood.internal.sequentialChunkSize` system property (bytes).
    private static final int DEFAULT_CHUNK_SIZE =
            Integer.getInteger("hardwood.internal.sequentialChunkSize", 128 * 1024 * 1024);

    /// Initial peek size used to read a page header. Grown on demand when the
    /// page header is larger (e.g. because the writer emitted inline
    /// `DataPageHeader.statistics` with long `min_value`/`max_value` binaries).
    private static final int INITIAL_PAGE_HEADER_PEEK_SIZE = 1024;

    /// Upper bound for page header peek growth. Parquet does not formally cap
    /// the page header size, but 1 MiB is well beyond what any sensible writer
    /// produces and protects against runaway reads on a corrupt file.
    private static final int MAX_PAGE_HEADER_PEEK_SIZE = 1024 * 1024;

    private final InputFile inputFile;
    private final long columnChunkOffset;
    private final int columnChunkLength;
    private final int chunkSize;
    private final ColumnSchema columnSchema;
    private final ColumnChunk columnChunk;
    private final HardwoodContextImpl context;
    private final long maxRows;
    private final int rowGroupIndex;
    private final String fileName;
    private final List<ResolvedPredicate> dropLeaves;

    private SequentialFetchPlan(InputFile inputFile, long columnChunkOffset, int columnChunkLength,
                                 int chunkSize, ColumnSchema columnSchema,
                                 ColumnChunk columnChunk, HardwoodContextImpl context,
                                 long maxRows, int rowGroupIndex, String fileName,
                                 List<ResolvedPredicate> dropLeaves) {
        this.inputFile = inputFile;
        this.columnChunkOffset = columnChunkOffset;
        this.columnChunkLength = columnChunkLength;
        this.chunkSize = chunkSize;
        this.columnSchema = columnSchema;
        this.columnChunk = columnChunk;
        this.context = context;
        this.maxRows = maxRows;
        this.rowGroupIndex = rowGroupIndex;
        this.fileName = fileName;
        this.dropLeaves = dropLeaves;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public Iterator<PageInfo> pages() {
        return new SequentialPageIterator();
    }

    /// Builds a [SequentialFetchPlan] with no predicate-driven page skipping.
    public static SequentialFetchPlan build(InputFile inputFile, ColumnSchema columnSchema,
                                      ColumnChunk columnChunk, HardwoodContextImpl context,
                                      int rowGroupIndex, String fileName, long maxRows) {
        return build(inputFile, columnSchema, columnChunk, context, rowGroupIndex, fileName,
                maxRows, List.of());
    }

    /// Builds a [SequentialFetchPlan] that may drop data pages whose inline
    /// [Statistics] prove they cannot match any of the given AND-necessary leaf
    /// predicates. Dropped pages are replaced with [PageInfo#nullPlaceholder]
    /// entries carrying the same `numValues`, so row alignment across sibling
    /// columns is preserved and the record-level filter drops the rows via SQL
    /// three-valued logic.
    ///
    /// Page skipping is only applied when the column is optional
    /// (`maxDefinitionLevel > 0`) — required columns cannot produce nulls, so
    /// every page is decoded normally.
    public static SequentialFetchPlan build(InputFile inputFile, ColumnSchema columnSchema,
                                      ColumnChunk columnChunk, HardwoodContextImpl context,
                                      int rowGroupIndex, String fileName, long maxRows,
                                      List<ResolvedPredicate> dropLeaves) {
        long columnChunkOffset = columnChunk.chunkStartOffset();
        int columnChunkLength = Math.toIntExact(columnChunk.metaData().totalCompressedSize());
        int chunkSize = Math.min(columnChunkLength,
                computeChunkSize(columnChunk.metaData(), maxRows));

        return new SequentialFetchPlan(inputFile, columnChunkOffset, columnChunkLength, chunkSize,
                columnSchema, columnChunk, context, maxRows, rowGroupIndex, fileName,
                dropLeaves == null ? List.of() : dropLeaves);
    }

    /// Computes the per-fetch chunk size.
    ///
    /// Without `maxRows`, returns the default ceiling so most column chunks
    /// are fetched in a single request. With `maxRows`, estimates the bytes
    /// required from the column's average compressed bytes-per-value:
    ///
    /// ```
    /// chunkSize = maxRows * (totalCompressedSize / numValues) * safetyFactor
    /// ```
    ///
    /// The estimate is floored at [MAX_ROWS_CHUNK_FLOOR] to avoid sub-page
    /// round-trips during header scanning and capped at [DEFAULT_CHUNK_SIZE].
    private static int computeChunkSize(ColumnMetaData metaData, long maxRows) {
        if (maxRows <= 0) {
            return DEFAULT_CHUNK_SIZE;
        }
        long numValues = metaData.numValues();
        if (numValues <= 0) {
            return MAX_ROWS_CHUNK_FLOOR;
        }
        long totalCompressedSize = metaData.totalCompressedSize();
        // Cap the effective row count so that effectiveRows * totalCompressedSize
        // cannot overflow (both factors bounded by the column chunk's own counts).
        long effectiveRows = Math.min(maxRows, numValues);
        long estimate = Math.ceilDiv(
                effectiveRows * totalCompressedSize * MAX_ROWS_CHUNK_SAFETY_FACTOR, numValues);
        // Cap the floor at the ceiling so an override of `DEFAULT_CHUNK_SIZE`
        // below the floor does not produce an invalid clamp range.
        long floor = Math.min(MAX_ROWS_CHUNK_FLOOR, DEFAULT_CHUNK_SIZE);
        long bounded = Math.clamp(estimate, floor, DEFAULT_CHUNK_SIZE);
        return Math.toIntExact(bounded);
    }

    /// Lazily discovers pages by scanning headers from [ChunkHandle]s.
    ///
    /// A single `ChunkHandle` serves both header scanning and page data
    /// resolution. As scanning advances past the current chunk, a new
    /// handle is created and chained for one-ahead pre-fetch.
    private class SequentialPageIterator implements Iterator<PageInfo> {
        private final ColumnMetaData metaData = columnChunk.metaData();
        private Dictionary dictionary;
        private boolean initialized;
        private boolean exhausted;
        private int position; // relative to column chunk start
        private long valuesRead;
        private int pageCount;
        private Boolean hasNext;

        // Current chunk handle state
        private ChunkHandle currentHandle;
        private int handleStart; // relative position where current handle starts
        private int handleEnd;   // relative position where current handle ends (exclusive)

        @Override
        public boolean hasNext() {
            if (hasNext != null) {
                return hasNext;
            }
            if (exhausted) {
                hasNext = false;
                return false;
            }
            try {
                hasNext = checkHasNext();
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to scan pages for column '"
                        + columnSchema.name() + "'", e);
            }
            return hasNext;
        }

        @Override
        public PageInfo next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            hasNext = null;
            try {
                return scanNextPage();
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to scan page for column '"
                        + columnSchema.name() + "'", e);
            }
        }

        private boolean checkHasNext() throws IOException {
            if (maxRows > 0 && valuesRead >= maxRows) {
                exhausted = true;
                emitEvent();
                return false;
            }
            if (!initialized) {
                initialize();
            }
            boolean hasMore = valuesRead < metaData.numValues() && position < columnChunkLength;
            if (!hasMore) {
                exhausted = true;
                emitEvent();
                if (valuesRead != metaData.numValues()) {
                    throw new IOException("Value count mismatch for column '" + columnSchema.name()
                            + "': metadata declares " + metaData.numValues()
                            + " values but pages contain " + valuesRead);
                }
            }
            return hasMore;
        }

        /// Reads a page header at the given relative position, growing the
        /// peek buffer on EOF. Needed because `DataPageHeader.statistics` may
        /// carry long `min_value`/`max_value` binaries that push the header
        /// past the initial peek size.
        private ParsedHeader readPageHeader(int relPos) throws IOException {
            int remaining = columnChunkLength - relPos;
            int peekSize = Math.min(INITIAL_PAGE_HEADER_PEEK_SIZE, remaining);
            while (true) {
                ByteBuffer headerBuf = readBytes(relPos, peekSize);
                ThriftCompactReader headerReader = new ThriftCompactReader(headerBuf);
                try {
                    PageHeader header = PageHeaderReader.read(headerReader);
                    return new ParsedHeader(header, headerReader.getBytesRead());
                }
                catch (EOFException eof) {
                    if (peekSize >= remaining) {
                        throw new IOException("Page header for column '"
                                + columnSchema.name() + "' exceeds the full column chunk remainder ("
                                + remaining + " bytes) — the file is likely corrupt", eof);
                    }
                    if (peekSize >= MAX_PAGE_HEADER_PEEK_SIZE) {
                        throw new IOException("Page header for column '"
                                + columnSchema.name() + "' exceeds maximum peek size ("
                                + MAX_PAGE_HEADER_PEEK_SIZE + " bytes)", eof);
                    }
                    peekSize = Math.min(remaining, Math.min(peekSize * 2, MAX_PAGE_HEADER_PEEK_SIZE));
                }
            }
        }

        /// Reads bytes at the given relative position, advancing through
        /// chunks as needed. If the range fits in the current chunk,
        /// returns a zero-copy slice. If it spans multiple chunks,
        /// assembles from each.
        private ByteBuffer readBytes(int relPos, int length) {
            if (currentHandle == null || relPos < handleStart || relPos >= handleEnd) {
                advanceChunk(relPos);
            }

            if (relPos + length <= handleEnd) {
                return currentHandle.slice(columnChunkOffset + relPos, length);
            }

            return assembleFromChunks(relPos, length);
        }

        /// Advances to the next chunk. If the current handle has a
        /// pre-fetched next handle that covers `relPos`, it is reused.
        /// Otherwise a new handle is created at `relPos`. The next
        /// chunk is always chained for one-ahead pre-fetch.
        private void advanceChunk(int relPos) {
            ChunkHandle prefetched = currentHandle != null ? currentHandle.nextChunk() : null;

            if (prefetched != null && relPos >= handleEnd
                    && relPos < handleEnd + prefetched.length()) {
                currentHandle = prefetched;
                handleStart = handleEnd;
            }
            else {
                int remaining = columnChunkLength - relPos;
                int handleLength = Math.min(remaining, chunkSize);
                currentHandle = new ChunkHandle(inputFile, columnChunkOffset + relPos, handleLength,
                        chunkPurpose(relPos));
                handleStart = relPos;
            }
            handleEnd = handleStart + currentHandle.length();

            // Chain the next chunk for one-ahead pre-fetch
            int nextStart = handleEnd;
            if (nextStart < columnChunkLength) {
                int nextRemaining = columnChunkLength - nextStart;
                int nextLength = Math.min(nextRemaining, chunkSize);
                currentHandle.setNextChunk(
                        new ChunkHandle(inputFile, columnChunkOffset + nextStart, nextLength,
                                chunkPurpose(nextStart)));
            }
        }

        private String chunkPurpose(int relPos) {
            return "rg=" + rowGroupIndex + " col='" + columnSchema.name()
                    + "' seqChunk@" + relPos;
        }

        /// Scans past the dictionary page (if present) on first access.
        private void initialize() throws IOException {
            initialized = true;
            if (position >= columnChunkLength) {
                return;
            }
            ParsedHeader parsed = readPageHeader(position);
            PageHeader header = parsed.header();
            int headerSize = parsed.headerSize();

            if (header.type() == PageHeader.PageType.DICTIONARY_PAGE) {
                int compressedSize = header.compressedPageSize();
                int numValues = header.dictionaryPageHeader().numValues();
                if (numValues < 0) {
                    throw new IOException("Invalid dictionary page for column '"
                            + columnSchema.name() + "': negative numValues (" + numValues + ")");
                }
                int dictTotalSize = headerSize + compressedSize;
                ByteBuffer dictRegion = readBytes(position, dictTotalSize);
                ByteBuffer compressedData = dictRegion.slice(headerSize, compressedSize);
                if (header.crc() != null) {
                    CrcValidator.assertCorrectCrc(header.crc(), compressedData, columnSchema.name());
                }
                dictionary = DictionaryParser.parse(dictRegion, columnSchema, metaData, context);
                position += dictTotalSize;
            }
        }

        private PageInfo scanNextPage() throws IOException {
            while (valuesRead < metaData.numValues() && position < columnChunkLength) {
                ParsedHeader parsed = readPageHeader(position);
                PageHeader header = parsed.header();
                int headerSize = parsed.headerSize();

                int compressedSize = header.compressedPageSize();
                int totalPageSize = headerSize + compressedSize;

                if (header.type() == PageHeader.PageType.DICTIONARY_PAGE) {
                    position += totalPageSize;
                    continue;
                }

                if (header.type() == PageHeader.PageType.DATA_PAGE
                        || header.type() == PageHeader.PageType.DATA_PAGE_V2) {
                    int numValues = (int) getValueCount(header);
                    PageInfo pageInfo;
                    if (canDropByInlineStats(header)) {
                        pageInfo = PageInfo.nullPlaceholder(numValues, columnSchema, metaData);
                    }
                    else {
                        ByteBuffer pageData = readBytes(position, totalPageSize);
                        pageInfo = new PageInfo(pageData, columnSchema, metaData, dictionary);
                    }
                    valuesRead += numValues;
                    position += totalPageSize;
                    pageCount++;
                    return pageInfo;
                }

                position += totalPageSize;
            }

            exhausted = true;
            return null;
        }

        /// Reads bytes that span multiple chunks by advancing through
        /// handles and concatenating. Uses a direct buffer so the result is
        /// usable from FFM-based decompressors (e.g. libdeflate), which
        /// require native MemorySegments.
        private ByteBuffer assembleFromChunks(int relPos, int length) {
            ByteBuffer combined = ByteBuffer.allocateDirect(length);
            int remaining = length;
            while (remaining > 0) {
                if (relPos < handleStart || relPos >= handleEnd) {
                    advanceChunk(relPos);
                }
                long absPos = columnChunkOffset + relPos;
                int available = handleEnd - relPos;
                int toRead = Math.min(available, remaining);
                combined.put(currentHandle.slice(absPos, toRead));
                relPos += toRead;
                remaining -= toRead;
            }
            combined.flip();
            return combined;
        }

        private long getValueCount(PageHeader header) {
            return switch (header.type()) {
                case DATA_PAGE -> header.dataPageHeader().numValues();
                case DATA_PAGE_V2 -> header.dataPageHeaderV2().numValues();
                case DICTIONARY_PAGE, INDEX_PAGE -> 0;
            };
        }

        /// Checks whether the given data page's inline [Statistics] (if any) prove
        /// that no row can match the per-column AND-necessary leaf predicates, in
        /// which case the caller emits a [PageInfo#nullPlaceholder] instead of the
        /// real page. Gated on `maxDefinitionLevel > 0` — required columns cannot
        /// represent nulls and must decode normally.
        private boolean canDropByInlineStats(PageHeader header) {
            if (dropLeaves.isEmpty() || columnSchema.maxDefinitionLevel() == 0) {
                return false;
            }
            Statistics inline = switch (header.type()) {
                case DATA_PAGE -> {
                    DataPageHeader dp = header.dataPageHeader();
                    yield dp == null ? null : dp.statistics();
                }
                case DATA_PAGE_V2 -> {
                    DataPageHeaderV2 dp = header.dataPageHeaderV2();
                    yield dp == null ? null : dp.statistics();
                }
                default -> null;
            };
            return PageDropPredicates.canDropPage(dropLeaves, inline);
        }

        private void emitEvent() {
            RowGroupScannedEvent event = new RowGroupScannedEvent();
            event.file = fileName;
            event.rowGroupIndex = rowGroupIndex;
            event.column = columnSchema.name();
            event.pageCount = pageCount;
            event.scanStrategy = RowGroupScannedEvent.STRATEGY_SEQUENTIAL;
            event.commit();
        }

        private record ParsedHeader(PageHeader header, int headerSize) {
        }
    }
}
