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
import java.util.NoSuchElementException;

import dev.hardwood.InputFile;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.jfr.RowGroupScannedEvent;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.schema.ColumnSchema;

/// [FetchPlan] for columns without an OffsetIndex.
///
/// Pages are discovered lazily by scanning headers from fixed-size
/// [ChunkHandle]s. The column chunk is split into uniform chunks
/// (default 128 MB, or 4 MB with `maxRows`). A single `ChunkHandle`
/// serves both header scanning and page data resolution.
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
/// - With `maxRows`: `min(chunkLength, 4 MB)` — limits over-fetch for
///   partial reads.
public final class SequentialFetchPlan implements FetchPlan {

    /// Chunk size when maxRows is active (4 MB).
    private static final int MAX_ROWS_CHUNK_SIZE = 4 * 1024 * 1024;

    /// Chunk size when reading without a row limit (128 MB).
    /// Overridable via the `hardwood.internal.sequentialChunkSize` system property (bytes).
    private static final int DEFAULT_CHUNK_SIZE =
            Integer.getInteger("hardwood.internal.sequentialChunkSize", 128 * 1024 * 1024);

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

    private SequentialFetchPlan(InputFile inputFile, long columnChunkOffset, int columnChunkLength,
                                 int chunkSize, ColumnSchema columnSchema,
                                 ColumnChunk columnChunk, HardwoodContextImpl context,
                                 long maxRows, int rowGroupIndex, String fileName) {
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
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public Iterator<PageInfo> pages() {
        return new SequentialPageIterator();
    }

    /// Builds a [SequentialFetchPlan].
    public static SequentialFetchPlan build(InputFile inputFile, ColumnSchema columnSchema,
                                      ColumnChunk columnChunk, HardwoodContextImpl context,
                                      int rowGroupIndex, String fileName, long maxRows) {
        long columnChunkOffset = columnChunk.chunkStartOffset();
        int columnChunkLength = Math.toIntExact(columnChunk.metaData().totalCompressedSize());
        int maxChunkSize = (maxRows > 0) ? MAX_ROWS_CHUNK_SIZE : DEFAULT_CHUNK_SIZE;
        int chunkSize = Math.min(columnChunkLength, maxChunkSize);

        return new SequentialFetchPlan(inputFile, columnChunkOffset, columnChunkLength, chunkSize,
                columnSchema, columnChunk, context, maxRows, rowGroupIndex, fileName);
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
                currentHandle = new ChunkHandle(inputFile, columnChunkOffset + relPos, handleLength);
                handleStart = relPos;
            }
            handleEnd = handleStart + currentHandle.length();

            // Chain the next chunk for one-ahead pre-fetch
            int nextStart = handleEnd;
            if (nextStart < columnChunkLength) {
                int nextRemaining = columnChunkLength - nextStart;
                int nextLength = Math.min(nextRemaining, chunkSize);
                currentHandle.setNextChunk(
                        new ChunkHandle(inputFile, columnChunkOffset + nextStart, nextLength));
            }
        }

        /// Scans past the dictionary page (if present) on first access.
        private void initialize() throws IOException {
            initialized = true;
            if (position >= columnChunkLength) {
                return;
            }
            int peekSize = Math.min(256, columnChunkLength - position);
            ByteBuffer headerBuf = readBytes(position, peekSize);
            ThriftCompactReader headerReader = new ThriftCompactReader(headerBuf);
            PageHeader header = PageHeaderReader.read(headerReader);
            int headerSize = headerReader.getBytesRead();

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
                int peekSize = Math.min(256, columnChunkLength - position);
                ByteBuffer headerBuf = readBytes(position, peekSize);
                ThriftCompactReader headerReader = new ThriftCompactReader(headerBuf);
                PageHeader header = PageHeaderReader.read(headerReader);
                int headerSize = headerReader.getBytesRead();

                int compressedSize = header.compressedPageSize();
                int totalPageSize = headerSize + compressedSize;

                if (header.type() == PageHeader.PageType.DICTIONARY_PAGE) {
                    position += totalPageSize;
                    continue;
                }

                if (header.type() == PageHeader.PageType.DATA_PAGE
                        || header.type() == PageHeader.PageType.DATA_PAGE_V2) {
                    ByteBuffer pageData = readBytes(position, totalPageSize);
                    PageInfo pageInfo = new PageInfo(pageData, columnSchema, metaData, dictionary);
                    valuesRead += getValueCount(header);
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
        /// handles and concatenating.
        private ByteBuffer assembleFromChunks(int relPos, int length) {
            ByteBuffer combined = ByteBuffer.allocate(length);
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

        private void emitEvent() {
            RowGroupScannedEvent event = new RowGroupScannedEvent();
            event.file = fileName;
            event.rowGroupIndex = rowGroupIndex;
            event.column = columnSchema.name();
            event.pageCount = pageCount;
            event.scanStrategy = RowGroupScannedEvent.STRATEGY_SEQUENTIAL;
            event.commit();
        }
    }
}
