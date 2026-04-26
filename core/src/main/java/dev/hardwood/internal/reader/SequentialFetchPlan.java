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
import java.nio.ByteOrder;
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
public final class SequentialFetchPlan implements FetchPlan, RowGroupIterator.CoalescableFirstChunk {

    /// Minimum chunk size when `maxRows` is active (1 MB).
    /// Sized to roughly one Parquet data page so header scanning does not
    /// require sub-page round-trips.
    private static final int MAX_ROWS_CHUNK_FLOOR = 1024 * 1024;

    /// Safety factor applied to the `maxRows * avgBytesPerValue` estimate
    /// to absorb per-value size skew within the column chunk.
    private static final int MAX_ROWS_CHUNK_SAFETY_FACTOR = 2;

    /// Upper bound on `columnChunkLength` for fetching the entire column
    /// in one go even under `maxRows` truncation (4 MB). Below this
    /// threshold the over-fetch is small (≤ 4× the `head(N)` floor) and
    /// the column becomes coalesce-safe — all of its bytes can join a
    /// cross-column [SharedRegion]. Above it, the per-column truncation
    /// stays in effect: a 50 MB column under `head(30)` should not pull
    /// down 50 MB. See #382.
    private static final int FETCH_WHOLE_COLUMN_THRESHOLD = 4 * MAX_ROWS_CHUNK_FLOOR;

    /// Chunk size when reading without a row limit (128 MB). Also used as
    /// the ceiling for the `maxRows` dynamic estimate.
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
    private final List<ResolvedPredicate> dropLeaves;
    /// Row ranges this column should keep. [RowRanges#ALL] means no per-page
    /// row mask is applied; a non-trivial range is paired with [#rowGroupRowCount]
    /// so the iterator can compute `pageLastRow` for the final page. 
    private final RowRanges matchingRows;
    /// Total rows in the enclosing row group. Used together with
    /// [#matchingRows] to compute the final page's `pageLastRow` when masks
    /// are active. Unused when [#matchingRows] is [RowRanges#ALL].
    private final long rowGroupRowCount;
    /// Optional pre-created first [ChunkHandle], typically a region-backed
    /// view from cross-column coalescing (#374). When set, the iterator's
    /// first `advanceChunk(0)` call uses this handle instead of creating
    /// a fresh standalone one. Subsequent advances (with `chunkSize` <
    /// columnChunkLength) still create per-column handles lazily.
    private ChunkHandle firstChunkHandle;
    private final ColumnDecryptor columnDecryptor;

    private SequentialFetchPlan(InputFile inputFile, long columnChunkOffset, int columnChunkLength,
                                 int chunkSize, ColumnSchema columnSchema,
                                 ColumnChunk columnChunk, HardwoodContextImpl context,
                                 long maxRows, int rowGroupIndex, String fileName,
                                 List<ResolvedPredicate> dropLeaves,
                                 RowRanges matchingRows, long rowGroupRowCount, ColumnDecryptor columnDecryptor) {
        if (matchingRows == null) {
            throw new IllegalArgumentException("matchingRows must not be null; use RowRanges.ALL");
        }
        if (!matchingRows.isAll() && rowGroupRowCount <= 0) {
            throw new IllegalArgumentException(
                    "rowGroupRowCount must be positive when matchingRows is non-trivial, got "
                            + rowGroupRowCount);
        }
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
        this.matchingRows = matchingRows;
        this.rowGroupRowCount = rowGroupRowCount;
        this.columnDecryptor = columnDecryptor;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public Iterator<PageInfo> pages() {
        return new SequentialPageIterator();
    }

    /// Returns the byte offset of this plan's first ChunkHandle (the
    /// chunk it would create on the first `advanceChunk(0)`). Used by
    /// cross-column coalescing in [RowGroupIterator] to decide which
    /// plans' first reads to merge into a shared region.
    @Override
    public long firstChunkOffset() {
        return columnChunkOffset;
    }

    /// Returns the byte length of this plan's first ChunkHandle.
    @Override
    public int firstChunkLength() {
        return chunkSize;
    }

    /// Coalesce-safe iff the first chunk covers the whole column chunk.
    /// Smaller chunk sizes are produced by `head(N)` truncation; in that
    /// case bridging to a neighbour column would pull in bytes the
    /// per-column iterator would otherwise have read separately,
    /// producing a double-fetch.
    @Override
    public boolean isCoalesceSafe() {
        return chunkSize == columnChunkLength;
    }

    /// Replaces the iterator's first ChunkHandle with a region-backed
    /// view, so the first read slices the shared buffer rather than
    /// issuing a per-column `readRange`.
    @Override
    public void attachSharedRegion(SharedRegion region, int rowGroupIndex) {
        String purpose = "rg=" + rowGroupIndex + " col='" + columnSchema.name()
                + "' seqChunk@0 (region-backed)";
        this.firstChunkHandle = new ChunkHandle(region,
                columnChunkOffset, chunkSize, purpose);
    }

    /// Builds a [SequentialFetchPlan] with no predicate-driven page skipping
    /// and no per-page row mask. Equivalent to passing [RowRanges#ALL].
    public static SequentialFetchPlan build(InputFile inputFile, ColumnSchema columnSchema,
                                      ColumnChunk columnChunk, HardwoodContextImpl context,
                                      int rowGroupIndex, String fileName, long maxRows, ColumnDecryptor columnDecryptor) {
        return build(inputFile, columnSchema, columnChunk, context, rowGroupIndex, fileName,
                maxRows, List.of(), RowRanges.ALL, 0L, columnDecryptor);
    }

    /// Builds a [SequentialFetchPlan] that may drop data pages whose inline
    /// [Statistics] prove they cannot match any of the given AND-necessary leaf
    /// predicates, with no per-page row mask.
    public static SequentialFetchPlan build(InputFile inputFile, ColumnSchema columnSchema,
                                      ColumnChunk columnChunk, HardwoodContextImpl context,
                                      int rowGroupIndex, String fileName, long maxRows,
                                      List<ResolvedPredicate> dropLeaves, ColumnDecryptor columnDecryptor) {
        return build(inputFile, columnSchema, columnChunk, context, rowGroupIndex, fileName,
                maxRows, dropLeaves, RowRanges.ALL, 0L, columnDecryptor);
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
    ///
    /// `matchingRows` carries the row-group-wide row ranges this column should
    /// keep. [RowRanges#ALL] disables per-page masking (today's behaviour); a
    /// non-trivial range pairs with `rowGroupRowCount` so the iterator can
    /// compute the final page's `pageLastRow`.
    public static SequentialFetchPlan build(InputFile inputFile, ColumnSchema columnSchema,
                                      ColumnChunk columnChunk, HardwoodContextImpl context,
                                      int rowGroupIndex, String fileName, long maxRows,
                                      List<ResolvedPredicate> dropLeaves,
                                      RowRanges matchingRows, long rowGroupRowCount,
                                      ColumnDecryptor columnDecryptor) {
        long columnChunkOffset = columnChunk.chunkStartOffset();
        int columnChunkLength = Math.toIntExact(columnChunk.metaData().totalCompressedSize());
        int chunkSize = Math.min(columnChunkLength,
                computeChunkSize(columnChunkLength, columnChunk.metaData(), maxRows));

        return new SequentialFetchPlan(inputFile, columnChunkOffset, columnChunkLength, chunkSize,
                columnSchema, columnChunk, context, maxRows, rowGroupIndex, fileName,
                dropLeaves == null ? List.of() : dropLeaves,
                matchingRows == null ? RowRanges.ALL : matchingRows, rowGroupRowCount, columnDecryptor);
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
    /// Columns whose entire chunk is below [FETCH_WHOLE_COLUMN_THRESHOLD]
    /// short-circuit to the full chunk length so they remain coalesce-safe
    /// (#382).
    static int computeChunkSize(int columnChunkLength, ColumnMetaData metaData, long maxRows) {
        if (maxRows <= 0) {
            return DEFAULT_CHUNK_SIZE;
        }
        if (columnChunkLength <= FETCH_WHOLE_COLUMN_THRESHOLD) {
            return columnChunkLength;
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
        /// Top-level records consumed so far. Equals [#valuesRead] for flat
        /// columns; for nested columns it counts `rep_level == 0` occurrences
        /// across the pages seen. Tracked only when [#matchingRows] is
        /// non-trivial — when masks are inactive the cursor stays at zero and
        /// is never consulted.
        private long recordsRead;
        private int pageCount;
        /// Look-ahead cache for the next page to emit. Populated lazily by
        /// [#hasNext()] and consumed by [#next()]. Look-ahead is required
        /// because per-page row masks may drop pages while counters still
        /// indicate "more values remain"; without the cache, `hasNext()`
        /// could return `true` and `next()` then have nothing to emit.
        private PageInfo nextPage;
        private boolean nextPageComputed;

        // Current chunk handle state
        private ChunkHandle currentHandle;
        private int handleStart; // relative position where current handle starts
        private int handleEnd;   // relative position where current handle ends (exclusive)

        @Override
        public boolean hasNext() {
            if (nextPageComputed) {
                return nextPage != null;
            }
            if (exhausted) {
                return false;
            }
            try {
                nextPage = findNextEmittablePage();
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to scan pages for column '"
                        + columnSchema.name() + "'", e);
            }
            nextPageComputed = true;
            if (nextPage == null) {
                exhausted = true;
                emitEvent();
            }
            return nextPage != null;
        }

        @Override
        public PageInfo next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            PageInfo result = nextPage;
            nextPage = null;
            nextPageComputed = false;
            return result;
        }

        /// Reads a page header at the given relative position, growing the
        /// peek buffer on EOF. Needed because `DataPageHeader.statistics` may
        /// carry long `min_value`/`max_value` binaries that push the header
        /// past the initial peek size.
        private ParsedHeader readPageHeader(int relPos) throws IOException {
            int remaining = columnChunkLength - relPos;
            int peekSize = Math.min(PageFormatProbe.INITIAL_PEEK_SIZE, remaining);
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
                    if (peekSize >= PageFormatProbe.MAX_PEEK_SIZE) {
                        throw new IOException("Page header for column '"
                                + columnSchema.name() + "' exceeds maximum peek size ("
                                + PageFormatProbe.MAX_PEEK_SIZE + " bytes)", eof);
                    }
                    peekSize = Math.min(remaining,
                            Math.min(peekSize * 2, PageFormatProbe.MAX_PEEK_SIZE));
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

            if (currentHandle == null && relPos == 0 && firstChunkHandle != null) {
                // First advance, externally-supplied handle (region-backed via
                // cross-column coalescing in RowGroupIterator).
                currentHandle = firstChunkHandle;
                handleStart = 0;
            }
            else if (prefetched != null && relPos >= handleEnd
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

            if(columnDecryptor != null) {
                // use metadata to check if there is a dictionary page
                if (metaData.dictionaryPageOffset() != null && metaData.dictionaryPageOffset() > 0) {
                    // readPageHeader won't help, we need to first decrypt to plaintext and then parse thrift
                    ByteBuffer rawBuf = readBytes(position, columnChunkLength - position);
                    ByteBuffer plaintextHeader = columnDecryptor.decryptDictPageHeader(rawBuf);
                    ByteBuffer plaintextData = columnDecryptor.decryptDictPageData(rawBuf);
                    dictionary = DictionaryParser.parse(plaintextHeader, plaintextData, columnSchema, metaData, context);
                    position += rawBuf.position();
                }
            }
            else {
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
        }

        /// Scans forward through page headers until an emittable data page is
        /// found, or the column chunk is exhausted. Pages are dropped when
        /// either:
        ///
        /// - the iterator-wide `maxRows` budget is hit (returns `null`); or
        /// - per-page row masking determines no row of the page falls inside
        ///   [#matchingRows] — the page body is skipped without being read or
        ///   decompressed, saving the codec invocation and value-decode work
        ///   for rows that would have been thrown away anyway.
        ///
        /// On normal exhaustion, validates that `valuesRead` matches the
        /// column chunk's declared `numValues`. The check stays load-bearing
        /// even with masked drops because `valuesRead` accumulates each
        /// page's `header.num_values` regardless of whether the page was
        /// kept, dropped, or replaced with a null-placeholder.
        private PageInfo findNextEmittablePage() throws IOException {
            if (!initialized) {
                initialize();
            }
            // Each loop iteration: read one page header, derive its row mask,
            // then either skip the page (mask null), emit it as a placeholder
            // (inline-stats drop), or emit the body slice.
            while (position < columnChunkLength && valuesRead < metaData.numValues()) {
                if (columnDecryptor != null) {
                    // For encrypted files we cannot call readPageHeader() because bytes at current
                    // position are part of encrypted blob, with a 4-byte length prefix at start, not a
                    // Thrift struct, and hence parsing would either fail or generate  garbage.
                    // So we compute the total size of both encrypted blobs (header blob,
                    // data blob) by reading their length prefixes directly — these 4-byte prefixes
                    // are always plaintext per the Parquet encryption spec.
                    // We don't decrypt here — PageDecoder handles decryption when it decodes
                    // the page. We just package the raw encrypted bytes into PageInfo.
                    ByteBuffer rawBuf = readBytes(position, columnChunkLength - position);

                    // Header blob: 4-byte length prefix + 12-byte nonce + ciphertext (length bytes, includes GCM tag)
                    rawBuf.order(ByteOrder.LITTLE_ENDIAN);
                    int headerNoncePlusCiphertext = rawBuf.getInt(0);  // from length prefix
                    int headerBlobSize = 4 + headerNoncePlusCiphertext; // total bytes on disk

                    // Data blob: starts immediately after header blob
                    int dataBlobLength = rawBuf.getInt(headerBlobSize);
                    int dataBlobTotalSize = 4 + dataBlobLength;
                    int totalEncryptedSize = headerBlobSize + dataBlobTotalSize;
                    ByteBuffer pageData = rawBuf.slice(0, totalEncryptedSize);
                    PageInfo pageInfo = new PageInfo(pageData, columnSchema, metaData, dictionary, PageRowMask.ALL, columnDecryptor);

                    // We skip valuesRead tracking for encrypted files because:
                    // 1. Exhaustion is detected via position >= columnChunkLength
                    // 2. maxRows limiting is handled at RowGroupIterator level via perRgMaxRows
                    // Per-page numValues would require decrypting the header here, which we
                    // intentionally avoid — PageDecoder owns all decryption.
                    position += totalEncryptedSize;
                    pageCount++;
                    return pageInfo;
                }

                if (maxRows > 0 && valuesRead >= maxRows) {
                    return null;
                }
                // Once `recordsRead` has crossed the last matching row, every
                // remaining page produces a null mask. Exit before parsing
                // any more page headers so trailing-region scans don't pay
                // for work we know will be discarded.
                if (!matchingRows.isAll() && recordsRead >= matchingRows.endRow()) {
                    return null;
                }
                ParsedHeader parsed = readPageHeader(position);
                PageHeader header = parsed.header();
                int headerSize = parsed.headerSize();
                int totalPageSize = headerSize + header.compressedPageSize();

                if (header.type() != PageHeader.PageType.DATA_PAGE
                        && header.type() != PageHeader.PageType.DATA_PAGE_V2) {
                    // DICTIONARY_PAGE or INDEX_PAGE — skip without emitting.
                    position += totalPageSize;
                    continue;
                }

                int numValues = (int) getValueCount(header);

                // When masks are inactive we don't need a record count for
                // the page and `recordsInPageComputed` stays false. When
                // active, flat columns return `numValues` directly; nested
                // columns walk the rep-level RLE prefix of a v2 page (a v1
                // page on a nested column would require decompression —
                // the row-group-wide gate forbids it).
                PageRowMask mask;
                int recordsInPage;
                boolean recordsInPageComputed;
                if (matchingRows.isAll()) {
                    mask = PageRowMask.ALL;
                    recordsInPage = 0;
                    recordsInPageComputed = false;
                }
                else {
                    recordsInPage = computeRecordsInPage(header, headerSize, numValues);
                    recordsInPageComputed = true;
                    long pageFirstRow = recordsRead;
                    long pageLastRow = pageFirstRow + recordsInPage;
                    mask = matchingRows.maskForPage(pageFirstRow, pageLastRow);
                }

                if (mask == null) {
                    // Drop the page entirely — the body bytes are never read,
                    // never decompressed. Counters still advance so the
                    // end-of-iteration guards remain honest.
                    valuesRead += numValues;
                    recordsRead += recordsInPage;
                    position += totalPageSize;
                    pageCount++;
                    continue;
                }

                PageInfo pageInfo;
                if (canDropByInlineStats(header)) {
                    // Inline-stats drop coexists with the mask: the placeholder
                    // covers exactly the rows the mask would have kept, so
                    // sibling columns see consistent record counts.
                    int placeholderRecords;
                    if (mask.isAll()) {
                        // No mask trimming — use the page's record count when
                        // we know it (nested columns under active masking) or
                        // its value count (flat columns or inactive masking,
                        // where the two coincide).
                        placeholderRecords = recordsInPageComputed ? recordsInPage : numValues;
                    }
                    else {
                        placeholderRecords = mask.totalRecords();
                    }
                    pageInfo = PageInfo.nullPlaceholder(placeholderRecords, columnSchema, metaData);
                }
                else {
                    ByteBuffer pageData = readBytes(position, totalPageSize);
                    pageInfo = new PageInfo(pageData, columnSchema, metaData, dictionary, mask, columnDecryptor);
                }
                valuesRead += numValues;
                recordsRead += recordsInPage;
                position += totalPageSize;
                pageCount++;
                return pageInfo;
            }

            // In case of encrypted columns, valuesRead and recordsRead are not incremented
            // because read page headers cant be read without decrypting them first.
            // PageDecoder contains all decryption. Exhaustion is checked by
            // position >= columnChunkLength, and integrity is guaranteed by GCM
            // authentication at decode time. Hence we skip these checks for encrypted flows.
            if (columnDecryptor == null && valuesRead != metaData.numValues()) {
                throw new IOException("Value count mismatch for column '" + columnSchema.name()
                        + "': metadata declares " + metaData.numValues()
                        + " values but pages contain " + valuesRead);
            }
            if (columnDecryptor == null && !matchingRows.isAll() && recordsRead != rowGroupRowCount) {
                throw new IOException("Record count mismatch for column '" + columnSchema.name()
                        + "': row group declares " + rowGroupRowCount
                        + " rows but pages contain " + recordsRead + " records.");
            }
            if (columnDecryptor == null && valuesRead != metaData.numValues()) {
                throw new IOException("Value count mismatch for column '" + columnSchema.name()
                        + "': metadata declares " + metaData.numValues()
                        + " values but pages contain " + valuesRead);
            }
            if (columnDecryptor == null && !matchingRows.isAll() && recordsRead != rowGroupRowCount) {
                throw new IOException("Record count mismatch for column '" + columnSchema.name()
                        + "': row group declares " + rowGroupRowCount
                        + " rows but pages contain " + recordsRead + " records.");
            }
            return null;
        }

        /// Computes the number of top-level records in a data page when masks
        /// are active. Flat columns return `numValues` directly. Nested
        /// columns walk the v2 page's uncompressed rep-level RLE prefix; a
        /// v1 nested page would require decompression to count records and
        /// is rejected here — the row-group-wide gate is responsible for
        /// promoting `matchingRows` to ALL before we ever reach this branch.
        private int computeRecordsInPage(PageHeader header, int headerSize, int numValues)
                throws IOException {
            if (columnSchema.maxRepetitionLevel() == 0) {
                return numValues;
            }
            if (header.type() != PageHeader.PageType.DATA_PAGE_V2) {
                throw new IllegalStateException("Per-page row masking on a nested column requires "
                        + "DATA_PAGE_V2 pages; column '" + columnSchema.name()
                        + "' has a v1 data page. The row-group-wide mask gate should have "
                        + "promoted matchingRows to RowRanges.ALL for this row group.");
            }
            DataPageHeaderV2 v2Header = header.dataPageHeaderV2();
            int repLevelLength = v2Header.repetitionLevelsByteLength();
            if (repLevelLength == 0) {
                // No repetition levels means every value is a top-level record.
                return numValues;
            }
            ByteBuffer repLevels = readBytes(position + headerSize, repLevelLength);
            return PageRecordCounter.countTopLevelRecords(repLevels, 0, repLevelLength,
                    numValues, columnSchema.maxRepetitionLevel());
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
