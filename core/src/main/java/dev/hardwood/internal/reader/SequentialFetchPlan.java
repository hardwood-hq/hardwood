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
import java.util.Objects;

import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.metadata.DataPageHeader;
import dev.hardwood.internal.metadata.DataPageHeaderV2;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.predicate.BoundsReadability;
import dev.hardwood.internal.predicate.LogContext;
import dev.hardwood.internal.predicate.PageDropPredicates;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.internal.thrift.ThriftTruncatedException;
import dev.hardwood.jfr.RowGroupScannedEvent;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.PageType;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.ParquetReadException;
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
/// the fetcher later advances to N+1 (already fetched), a new handle
/// for N+2 is created and chained.
///
/// Chunk size controls the `readRange()` granularity:
///
/// - Without `maxRows`: `min(chunkLength, 128 MB)` — full column chunk
///   in one fetch for most columns.
/// - With `maxRows`: the full column chunk when it is at most 4 MiB;
///   otherwise sized from the column's average compressed
///   bytes-per-value (`totalCompressedSize / numValues`) multiplied by
///   `maxRows` and a safety factor, floored at 1 MiB and capped at the
///   default ceiling.
///
/// The first chunk's handle belongs to the plan rather than to a walk, so
/// [#prefetch] fetches the bytes a [#pages] walk then reads.
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

    /// A record count not known yet: the page's repetition levels are still to be read.
    private static final int UNKNOWN = -1;

    static final String CHUNK_SIZE_PROPERTY = "hardwood.internal.sequentialChunkSize";

    /// Chunk size when reading without a row limit. Also used as the ceiling
    /// for the `maxRows` dynamic estimate.
    private static final int DEFAULT_CHUNK_SIZE = 128 * 1024 * 1024;

    /// The chunk size in bytes, [#DEFAULT_CHUNK_SIZE] unless overridden through
    /// [#CHUNK_SIZE_PROPERTY].
    ///
    /// Read on each call rather than cached in a static final, so that the override
    /// holds whenever it is set. Caching it would tie the value to whichever code
    /// path happened to initialise this class first, which is not something a caller
    /// setting the property can see or control. [#computeChunkSize] runs once per
    /// column chunk of a plan, so the lookup is nowhere near a hot path.
    ///
    /// An unparseable value is rejected rather than quietly replaced by the default:
    /// a chunk size that does not take effect produces a plausible read with the
    /// wrong fetch behaviour, which is exactly what makes it hard to notice.
    private static int defaultChunkSize() {
        String override = System.getProperty(CHUNK_SIZE_PROPERTY);
        if (override == null) {
            return DEFAULT_CHUNK_SIZE;
        }
        try {
            return Integer.parseInt(override);
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    CHUNK_SIZE_PROPERTY + " must be a byte count, but was: " + override, e);
        }
    }

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
    /// The readability [#dropLeaves] decide inline page statistics under.
    private final BoundsReadability dropReadability;
    /// Row ranges this column should keep. [RowRanges#ALL] means no per-page
    /// row mask is applied; a non-trivial range is paired with [#rowGroupRowCount]
    /// so the iterator can compute `pageLastRow` for the final page.
    private final RowRanges matchingRows;
    /// Total rows in the enclosing row group. Used together with
    /// [#matchingRows] to compute the final page's `pageLastRow` when masks
    /// are active. Unused when [#matchingRows] is [RowRanges#ALL].
    private final long rowGroupRowCount;
    /// The dictionary pruning has already read, or `null` when the plan parses it.
    private final Dictionary preloadedDictionary;
    private final WorkingSet.Column workingSet;
    /// Where the one-ahead pre-fetch of each chunk runs.
    private final PrefetchTasks prefetchTasks;
    /// The [ChunkHandle] of the first chunk, shared by [#prefetch] and every [#pages] walk so
    /// that a prefetched chunk is the one the walk reads. Region-backed when cross-column
    /// coalescing (#374) attached a [SharedRegion], created standalone on first use otherwise.
    /// Later chunks (with `chunkSize` < `columnChunkLength`) are created per walk.
    private ChunkHandle firstChunkHandle;
    /// Page headers the most recent [#pages] walk scanned, final once that walk runs to
    /// exhaustion and zero for one abandoned before it. Counts every header the iterator read,
    /// including those of pages a row mask or the inline statistics then kept it from emitting,
    /// which is what separates a walk that stopped scanning early from one that read on and
    /// discarded what it found.
    private int scannedPages;

    private SequentialFetchPlan(InputFile inputFile, long columnChunkOffset, int columnChunkLength,
                                 int chunkSize, ColumnSchema columnSchema,
                                 ColumnChunk columnChunk, HardwoodContextImpl context,
                                 long maxRows, int rowGroupIndex, String fileName,
                                 List<ResolvedPredicate> dropLeaves, BoundsReadability dropReadability,
                                 RowRanges matchingRows, long rowGroupRowCount,
                                 Dictionary preloadedDictionary, PrefetchTasks prefetchTasks,
                                 WorkingSet.Column workingSet) {
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
        this.dropReadability = dropReadability;
        this.matchingRows = matchingRows;
        this.rowGroupRowCount = rowGroupRowCount;
        this.preloadedDictionary = preloadedDictionary;
        this.prefetchTasks = prefetchTasks;
        this.workingSet = workingSet;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public PageWalk walk() {
        return new SequentialPageWalk();
    }

    /// Fetches the first chunk on the calling thread, which is the speculative task that
    /// planned this row group, into the handle a [#walk] reads; waiting for that task covers
    /// this read. A failure leaves the handle to the demand path, which reads it again and
    /// reports the failure to a caller that is waiting for it.
    @Override
    public void prefetch() {
        firstChunk().prefetch();
    }

    /// Returns the handle of the first chunk, creating a standalone one if none is attached.
    /// Synchronized because [#prefetch] runs on a speculative task while a column's fetcher
    /// may already be walking the plan.
    private synchronized ChunkHandle firstChunk() {
        if (firstChunkHandle == null) {
            firstChunkHandle = new ChunkHandle(inputFile, columnChunkOffset, chunkSize, chunkPurpose(0),
                    prefetchTasks);
        }
        return firstChunkHandle;
    }

    private String chunkPurpose(int relPos) {
        return "rg=" + rowGroupIndex + " col='" + columnSchema.name()
                + "' seqChunk@" + relPos;
    }

    /// Returns how many page headers the most recent [#pages] walk scanned. See [#scannedPages].
    public int scannedPages() {
        return scannedPages;
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

    @Override
    public long readStart() {
        return columnChunkOffset;
    }

    @Override
    public long readEnd() {
        return columnChunkOffset + columnChunkLength;
    }

    /// Replaces the iterator's first ChunkHandle with a region-backed
    /// view, so the first read slices the shared buffer rather than
    /// issuing a per-column `readRange`.
    @Override
    public synchronized void attachSharedRegion(SharedRegion region, int rowGroupIndex) {
        String purpose = "rg=" + rowGroupIndex + " col='" + columnSchema.name()
                + "' seqChunk@0 (region-backed)";
        this.firstChunkHandle = new ChunkHandle(region,
                columnChunkOffset, chunkSize, purpose);
    }

    /// Builds a [SequentialFetchPlan] with no predicate-driven page skipping
    /// and no per-page row mask. Equivalent to passing [RowRanges#ALL].
    public static SequentialFetchPlan build(InputFile inputFile, ColumnSchema columnSchema,
                                      ColumnChunk columnChunk, HardwoodContextImpl context,
                                      int rowGroupIndex, String fileName, long maxRows) {
        return build(inputFile, columnSchema, columnChunk, context, rowGroupIndex, fileName,
                maxRows, List.of(), RowRanges.ALL, 0L);
    }

    /// Builds a [SequentialFetchPlan] that may drop data pages whose inline
    /// [Statistics] prove they cannot match any of the given AND-necessary leaf
    /// predicates, with no per-page row mask.
    public static SequentialFetchPlan build(InputFile inputFile, ColumnSchema columnSchema,
                                      ColumnChunk columnChunk, HardwoodContextImpl context,
                                      int rowGroupIndex, String fileName, long maxRows,
                                      List<ResolvedPredicate> dropLeaves) {
        return build(inputFile, columnSchema, columnChunk, context, rowGroupIndex, fileName,
                maxRows, dropLeaves, RowRanges.ALL, 0L);
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
                                      RowRanges matchingRows, long rowGroupRowCount) {
        return build(inputFile, columnSchema, columnChunk, context, rowGroupIndex, fileName,
                maxRows, dropLeaves, BoundsReadability.ALL, matchingRows, rowGroupRowCount, null, 0,
                new PrefetchTasks(context.ioExecutor()), WorkingSet.standalone(columnSchema));
    }

    /// @param dropReadability the readability `dropLeaves` decide inline page statistics under
    ///        (see [PageDropPredicates#canDropPage])
    /// @param preloadedDictionary the column's dictionary if pruning has read it, which the plan
    ///        decodes with instead of fetching and parsing the dictionary page again; `null`
    ///        otherwise
    /// @param preloadedDictionaryEnd where the preloaded dictionary's page ends, which is where
    ///        the plan starts reading; ignored without a preloaded dictionary
    /// @param prefetchTasks where the one-ahead pre-fetch of each chunk runs; the overloads
    ///        without it give the plan tasks of its own that nothing waits for
    /// @param workingSet where the plan charges the dictionary it parses; the overloads without
    ///        it give the plan a working set of its own
    public static SequentialFetchPlan build(InputFile inputFile, ColumnSchema columnSchema,
                                      ColumnChunk columnChunk, HardwoodContextImpl context,
                                      int rowGroupIndex, String fileName, long maxRows,
                                      List<ResolvedPredicate> dropLeaves, BoundsReadability dropReadability,
                                      RowRanges matchingRows, long rowGroupRowCount,
                                      Dictionary preloadedDictionary, long preloadedDictionaryEnd,
                                      PrefetchTasks prefetchTasks, WorkingSet.Column workingSet) {
        long columnChunkOffset = columnChunk.chunkStartOffset();
        long totalCompressedSize = columnChunk.metaData().totalCompressedSize();
        if (totalCompressedSize > Integer.MAX_VALUE) {
            // The file is correct; this reader will not fetch a region it cannot
            // address with an int, and a second attempt changes nothing.
            throw new UnsupportedOperationException(ExceptionContext.readPrefix(fileName, rowGroupIndex,
                    columnSchema.fieldPath().toString())
                    + "Column chunk too large (" + totalCompressedSize
                    + " bytes); the reader supports column chunks of up to " + Integer.MAX_VALUE + " bytes");
        }
        int columnChunkLength = Math.toIntExact(totalCompressedSize);
        if (preloadedDictionary != null) {
            // The data pages start where the dictionary page ends.
            Objects.checkFromToIndex(columnChunkOffset, preloadedDictionaryEnd, columnChunkOffset + columnChunkLength);
            columnChunkLength -= Math.toIntExact(preloadedDictionaryEnd - columnChunkOffset);
            columnChunkOffset = preloadedDictionaryEnd;
        }
        int chunkSize = Math.min(columnChunkLength,
                computeChunkSize(columnChunkLength, columnChunk.metaData(), maxRows));

        return new SequentialFetchPlan(inputFile, columnChunkOffset, columnChunkLength, chunkSize,
                columnSchema, columnChunk, context, maxRows, rowGroupIndex, fileName,
                dropLeaves == null ? List.of() : dropLeaves, dropReadability,
                matchingRows == null ? RowRanges.ALL : matchingRows, rowGroupRowCount,
                preloadedDictionary, prefetchTasks, workingSet);
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
    /// round-trips during header scanning and capped at [#defaultChunkSize].
    /// Columns whose entire chunk is below [FETCH_WHOLE_COLUMN_THRESHOLD]
    /// short-circuit to the full chunk length so they remain coalesce-safe
    /// (#382).
    static int computeChunkSize(int columnChunkLength, ColumnMetaData metaData, long maxRows) {
        int defaultChunkSize = defaultChunkSize();
        if (maxRows <= 0) {
            return defaultChunkSize;
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
        // Cap the floor at the ceiling so an override of the default chunk size
        // below the floor does not produce an invalid clamp range.
        long floor = Math.min(MAX_ROWS_CHUNK_FLOOR, defaultChunkSize);
        long bounded = Math.clamp(estimate, floor, defaultChunkSize);
        return Math.toIntExact(bounded);
    }

    /// Discovers pages by scanning headers from [ChunkHandle]s, as a state machine over the
    /// bytes present.
    ///
    /// A single `ChunkHandle` serves both header scanning and page data
    /// resolution. As scanning advances past the current chunk, a new
    /// handle is created and chained for one-ahead pre-fetch.
    ///
    /// The page under way is held in fields: its parsed header, the peek it was parsed from, its
    /// record count once its repetition levels are read, and its inline-statistics decision. A
    /// chunk the page needs that is not present stops [#advance] with the page's state in place, and
    /// the next call resumes it, so no header is parsed twice and no byte is read twice. The
    /// counters of the walk ([#position], [#valuesRead], [#recordsRead], [#pageCount]) move only
    /// when a page is done with.
    private class SequentialPageWalk implements PageWalk {

        private final ColumnMetaData metaData = columnChunk.metaData();
        /// Where this chunk is, for a page whose statistics turn out to be unusable to name.
        private final LogContext logContext =
                new LogContext(fileName, rowGroupIndex).withColumn(columnSchema.fieldPath());
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
        /// Which page this walk is on, for a failure to name. Set before the bytes of a
        /// page are touched rather than after they parse, because a header that will not
        /// parse is exactly the case with no [PageInfo] to carry it.
        private int currentPage = ExceptionContext.UNKNOWN_PAGE;
        /// What the last [#advance] stopped on: bytes, or a task of the read ending.
        private Need pending;

        // The page under way, reset by clearPage() once the walk is done with it
        /// The header of the page at [#position], `null` until it is parsed.
        private PageHeader header;
        private int headerSize;
        /// The bytes the header is parsed from, `0` until the page's first attempt sets it.
        private int peekSize;
        /// The page's top-level records, [#UNKNOWN] until its repetition levels are read.
        private int recordsInPage = UNKNOWN;
        /// Whether the page's inline statistics are evaluated, and what they decided.
        private boolean dropDecided;
        private boolean dropByStats;

        // Current chunk handle state
        private ChunkHandle currentHandle;
        private int handleStart; // relative position where current handle starts
        private int handleEnd;   // relative position where current handle ends (exclusive)
        /// The handles the walk has moved past that hold bytes at or after the current page
        /// header, oldest first. A header peek or a repetition-level read can run several
        /// handles past the page's start before the page itself is read from there, and a page
        /// spanning several handles is copied once all are present, so every handle from the
        /// page's start onward stays readable from memory rather than being fetched again. A
        /// handle ending at or before a page header is released when that header is read, since
        /// no later read starts before it.
        private final List<ChunkHandle> keptHandles = new ArrayList<>();
        /// Where the page header being read starts: no read starts before it, so a handle
        /// ending at or before it is not kept.
        private int pageStart;

        @Override
        public int currentPage() {
            return currentPage;
        }

        @Override
        public Need pending() {
            return pending;
        }

        @Override
        public PageInfo advance() throws IOException {
            pending = null;
            if (exhausted) {
                return null;
            }
            if (!initialized && !initialize()) {
                return null;
            }
            return nextEmittablePage();
        }

        /// Ends the walk.
        private PageInfo end() {
            exhausted = true;
            scannedPages = pageCount;
            emitEvent();
            return null;
        }

        /// Forgets the page under way, once the walk has moved past it.
        private void clearPage() {
            header = null;
            headerSize = 0;
            peekSize = 0;
            recordsInPage = UNKNOWN;
            dropDecided = false;
            dropByStats = false;
        }

        /// Parses the header of the page at [#position] into [#header], growing the peek on a
        /// truncated parse, since `DataPageHeader.statistics` may carry long
        /// `min_value`/`max_value` binaries that push the header past the initial peek size.
        ///
        /// @return whether the header is parsed; `false` when its bytes are pending
        private boolean parseHeader() throws IOException {
            int remaining = columnChunkLength - position;
            if (peekSize == 0) {
                peekSize = Math.min(PageFormatProbe.INITIAL_PEEK_SIZE, remaining);
                // No later read starts before this header, so a handle ending at or before it is
                // never read again.
                pageStart = position;
                int relPos = position;
                keptHandles.removeIf(handle -> relativeEnd(handle) <= relPos);
            }
            while (true) {
                ByteBuffer headerBuf = readBytes(position, peekSize, true);
                if (headerBuf == null) {
                    return false;
                }
                ThriftCompactReader headerReader = new ThriftCompactReader(headerBuf);
                try {
                    header = PageHeaderReader.read(headerReader);
                    headerSize = headerReader.getBytesRead();
                    return true;
                }
                catch (ThriftTruncatedException truncated) {
                    if (peekSize >= remaining) {
                        throw new ParquetReadException("Page header for column '"
                                + columnSchema.name() + "' exceeds the full column chunk remainder ("
                                + remaining + " bytes) — the file is likely corrupt", truncated);
                    }
                    if (peekSize >= PageFormatProbe.MAX_PEEK_SIZE) {
                        throw new ParquetReadException("Page header for column '"
                                + columnSchema.name() + "' exceeds maximum peek size ("
                                + PageFormatProbe.MAX_PEEK_SIZE + " bytes)", truncated);
                    }
                    peekSize = Math.min(remaining,
                            Math.min(peekSize * 2, PageFormatProbe.MAX_PEEK_SIZE));
                }
            }
        }

        /// Reads bytes at the given relative position, advancing through chunks as needed: a
        /// zero-copy slice when the range fits in one chunk, assembled from each chunk when it
        /// spans several. Every handle the read leaves that holds bytes of the current page is
        /// kept when `keepLeftHandles` is set, since the page is read from its start afterwards;
        /// a whole page's read is its last, so it releases them.
        ///
        /// @return the bytes, or `null` when a chunk holding them is not present, which is then
        ///         [#pending]
        private ByteBuffer readBytes(int relPos, int length, boolean keepLeftHandles) throws IOException {
            ChunkHandle kept = keptHandleAt(relPos);
            if (kept != null) {
                if (relPos + length <= relativeEnd(kept)) {
                    return slice(kept, relPos, length);
                }
                return assembleFromChunks(relPos, length, keepLeftHandles);
            }
            if (currentHandle == null || relPos < handleStart || relPos >= handleEnd) {
                advanceChunk(relPos, keepLeftHandles);
            }
            if (relPos + length <= handleEnd) {
                return slice(currentHandle, relPos, length);
            }
            return assembleFromChunks(relPos, length, keepLeftHandles);
        }

        /// A slice of `handle`'s bytes, or `null` when they are not present.
        private ByteBuffer slice(ChunkHandle handle, int relPos, int length) throws IOException {
            ByteBuffer bytes = handle.poll();
            if (bytes == null) {
                pending = handle;
                return null;
            }
            return handle.slice(bytes, columnChunkOffset + relPos, length);
        }

        /// Returns the kept handle holding `relPos`, or `null` when none does.
        private ChunkHandle keptHandleAt(int relPos) {
            for (ChunkHandle handle : keptHandles) {
                if (relPos >= relativeStart(handle) && relPos < relativeEnd(handle)) {
                    return handle;
                }
            }
            return null;
        }

        private int relativeStart(ChunkHandle handle) {
            return Math.toIntExact(handle.fileOffset() - columnChunkOffset);
        }

        private int relativeEnd(ChunkHandle handle) {
            return relativeStart(handle) + handle.length();
        }

        /// Advances to the next chunk. If the current handle has a
        /// pre-fetched next handle that covers `relPos`, it is reused.
        /// Otherwise a new handle is created at `relPos`. The next
        /// chunk is always chained for one-ahead pre-fetch. The handle left
        /// goes into [#keptHandles] when `keepLeftHandle` is set and it holds
        /// bytes after [#pageStart], and is released otherwise.
        private void advanceChunk(int relPos, boolean keepLeftHandle) {
            ChunkHandle previous = currentHandle;
            ChunkHandle prefetched = previous != null ? previous.nextChunk() : null;
            if (previous != null) {
                // The plan holds on to the first handle; unlinked, it does not keep every
                // later chunk of the column reachable through the chain.
                previous.setNextChunk(null);
                if (keepLeftHandle && relativeEnd(previous) > pageStart) {
                    keptHandles.add(previous);
                }
            }

            if (currentHandle == null && relPos == 0) {
                // First advance: the plan's shared first handle, which a prefetch may
                // already have fetched, or which slices a cross-column region.
                currentHandle = firstChunk();
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
                        chunkPurpose(relPos), prefetchTasks);
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
                                chunkPurpose(nextStart), prefetchTasks));
            }
        }

        /// Scans past the dictionary page (if present) on first access.
        ///
        /// @return whether the walk is past it; `false` when bytes it needs are pending
        private boolean initialize() throws IOException {
            if (preloadedDictionary != null) {
                // The plan starts at the first data page: pruning has read the dictionary page.
                workingSet.dictionary(preloadedDictionary);
                dictionary = preloadedDictionary;
                initialized = true;
                return true;
            }
            if (position >= columnChunkLength) {
                initialized = true;
                return true;
            }
            // The chunk's first page, whichever kind it turns out to be: that is in the
            // header about to be read, so a header that will not parse can only be named by
            // position — and the dictionary page, when there is one, is at this position.
            currentPage = pageCount;
            if (header == null && !parseHeader()) {
                return false;
            }
            if (header.type() == PageType.DICTIONARY_PAGE) {
                currentPage = ExceptionContext.DICTIONARY_PAGE;
                int compressedSize = header.compressedPageSize();
                int dictTotalSize = headerSize + compressedSize;
                // Admitted before its bytes are read: a refused page read now would be read again
                // when it is admitted
                PageHeader dictionaryHeader = header;
                WorkingSet.DictionaryParse parse = workingSet.parseDictionary(header.uncompressedPageSize(), () -> {
                    ByteBuffer dictPage = readBytes(position, dictTotalSize, false);
                    if (dictPage == null) {
                        return null;
                    }
                    initialized = true;
                    // The header is already parsed, so hand it over rather than a region the
                    // parser would have to open and read the same header out of again.
                    return DictionaryParser.parsePage(dictionaryHeader, dictPage.slice(headerSize, compressedSize),
                            columnSchema, metaData, context, workingSet);
                });
                if (parse.pending() != null) {
                    pending = parse.pending();
                    return false;
                }
                if (!initialized) {
                    // The page's bytes are not present, which `readBytes` made pending
                    return false;
                }
                dictionary = parse.dictionary();
                position += dictTotalSize;
                clearPage();
                currentPage = ExceptionContext.UNKNOWN_PAGE;
                return true;
            }
            // A data page: the walk goes on from its parsed header.
            initialized = true;
            return true;
        }

        /// Scans forward through page headers until an emittable data page is
        /// found, or the column chunk is exhausted. Pages are dropped when
        /// either:
        ///
        /// - a flat column's pages cover the row group's `maxRows` budget
        ///   (ends the walk); or
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
        ///
        /// @return the page, or `null` when the walk ends or bytes the page needs are pending
        private PageInfo nextEmittablePage() throws IOException {
            // Each loop iteration: read one page header, derive its row mask,
            // then either skip the page (mask null), emit it as a placeholder
            // (inline-stats drop), or emit the body slice.
            while (position < columnChunkLength && valuesRead < metaData.numValues()) {
                if (reachedMaxRows()) {
                    return end();
                }
                // Once `recordsRead` has crossed the last matching row, every
                // remaining page produces a null mask. Exit before parsing
                // any more page headers so trailing-region scans don't pay
                // for work we know will be discarded.
                if (!matchingRows.isAll() && recordsRead >= matchingRows.endRow()) {
                    return end();
                }
                currentPage = pageCount;
                if (header == null && !parseHeader()) {
                    return null;
                }
                int totalPageSize = headerSize + header.compressedPageSize();

                if (header.type() != PageType.DATA_PAGE
                        && header.type() != PageType.DATA_PAGE_V2) {
                    // DICTIONARY_PAGE or INDEX_PAGE — skip without emitting.
                    position += totalPageSize;
                    clearPage();
                    continue;
                }

                int numValues = (int) getValueCount(header);

                // When masks are inactive we don't need a record count for
                // the page and the records stay uncounted. When active, flat
                // columns return `numValues` directly; nested columns walk the
                // rep-level RLE prefix of a v2 page (a v1 page on a nested
                // column would require decompression — the row-group-wide gate
                // forbids it).
                PageRowMask mask;
                int pageRecords;
                if (matchingRows.isAll()) {
                    mask = PageRowMask.ALL;
                    pageRecords = 0;
                }
                else {
                    if (recordsInPage == UNKNOWN) {
                        int counted = computeRecordsInPage(numValues);
                        if (counted == UNKNOWN) {
                            return null;
                        }
                        recordsInPage = counted;
                    }
                    pageRecords = recordsInPage;
                    long pageFirstRow = recordsRead;
                    long pageLastRow = pageFirstRow + pageRecords;
                    mask = matchingRows.maskForPage(pageFirstRow, pageLastRow);
                }

                if (mask == null) {
                    // Drop the page entirely — the body bytes are never read,
                    // never decompressed. Counters still advance so the
                    // end-of-iteration guards remain honest.
                    donePage(numValues, pageRecords, totalPageSize);
                    continue;
                }

                if (!dropDecided) {
                    dropByStats = canDropByInlineStats(header);
                    dropDecided = true;
                }
                PageInfo pageInfo;
                if (dropByStats) {
                    // Inline-stats drop coexists with the mask: the placeholder
                    // covers exactly the rows the mask would have kept, so
                    // sibling columns see consistent record counts.
                    int placeholderRecords;
                    if (mask.isAll()) {
                        // No mask trimming — use the page's record count when
                        // we know it (nested columns under active masking) or
                        // its value count (flat columns or inactive masking,
                        // where the two coincide).
                        placeholderRecords = recordsInPage != UNKNOWN ? recordsInPage : numValues;
                    }
                    else {
                        placeholderRecords = mask.totalRecords();
                    }
                    pageInfo = PageInfo.nullPlaceholder(placeholderRecords, columnSchema, metaData,
                            columnSchema.maxRepetitionLevel() == 0 ? placeholderRecords : PageInfo.UNKNOWN_RECORDS);
                }
                else {
                    ByteBuffer pageData = readBytes(position, totalPageSize, false);
                    if (pageData == null) {
                        return null;
                    }
                    pageInfo = PageInfo.dataPage(pageData, header, columnSchema, metaData, dictionary, mask,
                            keptRecords(mask, numValues));
                }
                donePage(numValues, pageRecords, totalPageSize);
                return pageInfo;
            }

            if (valuesRead != metaData.numValues()) {
                throw new ParquetReadException("Value count mismatch: metadata declares "
                        + metaData.numValues() + " values but pages contain " + valuesRead);
            }
            if (!matchingRows.isAll() && recordsRead != rowGroupRowCount) {
                throw new ParquetReadException("Record count mismatch: row group declares "
                        + rowGroupRowCount + " rows but pages contain " + recordsRead
                        + " records.");
            }
            return end();
        }

        /// Moves the walk past the page under way.
        private void donePage(int numValues, int pageRecords, int totalPageSize) {
            valuesRead += numValues;
            recordsRead += pageRecords;
            position += totalPageSize;
            pageCount++;
            clearPage();
        }

        /// The records a data page of `numValues` values contributes after `mask`, for
        /// [PageInfo#records()]: a column without repetition has one record per value, a column
        /// with repetition counts its records from its repetition levels once decoded.
        private int keptRecords(PageRowMask mask, int numValues) {
            if (columnSchema.maxRepetitionLevel() > 0) {
                return PageInfo.UNKNOWN_RECORDS;
            }
            return mask.isAll() ? numValues : mask.totalRecords();
        }

        /// Whether the pages yielded so far cover `maxRows` records, which this
        /// plan decides for a flat column only, whose values are its records. A
        /// nested column has more values than records, and counting them needs the
        /// repetition levels, which a v1 page holds inside the compressed body; its
        /// pages are yielded until the column's placement has placed
        /// `maxRows` records and ends the read.
        private boolean reachedMaxRows() {
            return maxRows > 0 && columnSchema.maxRepetitionLevel() == 0 && valuesRead >= maxRows;
        }

        /// Computes the number of top-level records in the page under way when masks
        /// are active. Flat columns return `numValues` directly. Nested
        /// columns walk the v2 page's uncompressed rep-level RLE prefix; a
        /// v1 nested page would require decompression to count records and
        /// is rejected here — the row-group-wide gate is responsible for
        /// promoting `matchingRows` to ALL before we ever reach this branch.
        ///
        /// @return the records, or [#UNKNOWN] when the levels are pending
        private int computeRecordsInPage(int numValues) throws IOException {
            if (columnSchema.maxRepetitionLevel() == 0) {
                return numValues;
            }
            if (header.type() != PageType.DATA_PAGE_V2) {
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
            ByteBuffer repLevels = readBytes(position + headerSize, repLevelLength, true);
            if (repLevels == null) {
                return UNKNOWN;
            }
            return PageRecordCounter.countTopLevelRecords(repLevels, 0, repLevelLength,
                    numValues, columnSchema.maxRepetitionLevel());
        }

        /// Reads bytes that span multiple chunks by advancing through handles and concatenating,
        /// once every handle holding a byte of the range is present: a range copied in part
        /// would leave the handles it passed released, and its next attempt would read them
        /// again. Uses a direct buffer so the result is usable from FFM-based decompressors
        /// (e.g. libdeflate), which require native MemorySegments.
        ///
        /// @return the bytes, or `null` when a chunk holding them is not present, which is then
        ///         [#pending]
        private ByteBuffer assembleFromChunks(int relPos, int length, boolean keepLeftHandles) throws IOException {
            int end = relPos + length;
            for (int pos = relPos; pos < end; ) {
                ChunkHandle handle = handleAt(pos, true);
                if (handle.poll() == null) {
                    pending = handle;
                    return null;
                }
                pos = relativeEnd(handle);
            }
            ByteBuffer combined = ByteBuffer.allocateDirect(length);
            for (int pos = relPos; pos < end; ) {
                ChunkHandle handle = handleAt(pos, false);
                int toRead = Math.min(relativeEnd(handle), end) - pos;
                combined.put(handle.slice(handle.poll(), columnChunkOffset + pos, toRead));
                pos += toRead;
            }
            combined.flip();
            if (!keepLeftHandles) {
                // The range is the page's last read: no later read of it starts before its end.
                keptHandles.removeIf(handle -> relativeEnd(handle) <= end);
            }
            return combined;
        }

        /// The handle holding `relPos`: a kept one, or the current one, which the walk advances
        /// to `relPos` when `advance` is set, keeping the one it leaves.
        private ChunkHandle handleAt(int relPos, boolean advance) {
            ChunkHandle kept = keptHandleAt(relPos);
            if (kept != null) {
                return kept;
            }
            if (relPos < handleStart || relPos >= handleEnd) {
                if (!advance) {
                    throw new IllegalStateException("No chunk holds position " + relPos + " of column '"
                            + columnSchema.name() + "'");
                }
                advanceChunk(relPos, true);
            }
            return currentHandle;
        }

        private long getValueCount(PageHeader header) {
            return switch (header.type()) {
                case DATA_PAGE -> header.dataPageHeader().numValues();
                case DATA_PAGE_V2 -> header.dataPageHeaderV2().numValues();
                case DICTIONARY_PAGE, INDEX_PAGE, UNKNOWN -> 0;
            };
        }

        /// Checks whether the given data page's inline [Statistics] (if any) prove
        /// that no row can match the per-column AND-necessary leaf predicates, in
        /// which case the caller emits a [PageInfo#nullPlaceholder] instead of the
        /// real page. Gated on `maxDefinitionLevel > 0` — required columns cannot
        /// represent nulls and must decode normally.
        ///
        /// A null count is held against the page's rows. A v2 header counts them. A v1 header
        /// counts values, which are the rows of a column with no repeated node above it, since
        /// such a column writes one entry per row; below a repeated node the page carries no row
        /// count and is decided from its bounds alone.
        private boolean canDropByInlineStats(PageHeader header) {
            if (dropLeaves.isEmpty() || columnSchema.maxDefinitionLevel() == 0) {
                return false;
            }
            Statistics inline;
            long rowCount;
            switch (header.type()) {
                case DATA_PAGE -> {
                    DataPageHeader dp = header.dataPageHeader();
                    inline = dp == null ? null : dp.statistics();
                    rowCount = dp == null || columnSchema.maxRepetitionLevel() > 0
                            ? PageDropPredicates.UNKNOWN_ROW_COUNT
                            : dp.numValues();
                }
                case DATA_PAGE_V2 -> {
                    DataPageHeaderV2 dp = header.dataPageHeaderV2();
                    inline = dp == null ? null : dp.statistics();
                    rowCount = dp == null ? PageDropPredicates.UNKNOWN_ROW_COUNT : dp.numRows();
                }
                default -> {
                    inline = null;
                    rowCount = PageDropPredicates.UNKNOWN_ROW_COUNT;
                }
            }
            return PageDropPredicates.canDropPage(dropLeaves, inline, rowCount, dropReadability,
                    logContext.withPageIndex(currentPage));
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
