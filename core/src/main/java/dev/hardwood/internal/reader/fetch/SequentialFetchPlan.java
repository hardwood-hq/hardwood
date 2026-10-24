/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.fetch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.metadata.DataPageHeader;
import dev.hardwood.internal.metadata.DataPageHeaderV2;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.predicate.BoundsReadability;
import dev.hardwood.internal.predicate.LogContext;
import dev.hardwood.internal.predicate.PageDropPredicates;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.batch.Dictionary;
import dev.hardwood.internal.reader.decode.DataPage;
import dev.hardwood.internal.reader.decode.PageFormatProbe;
import dev.hardwood.internal.reader.decode.PageFormatProbe.HeaderAt;
import dev.hardwood.internal.reader.decode.PageInfo;
import dev.hardwood.internal.reader.decode.PageRecordCounter;
import dev.hardwood.internal.reader.memory.WorkingSet;
import dev.hardwood.internal.reader.select.PageRowMask;
import dev.hardwood.internal.reader.select.RowRanges;
import dev.hardwood.internal.reader.step.Need;
import dev.hardwood.internal.reader.step.Parsed;
import dev.hardwood.jfr.RowGroupScannedEvent;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.PageType;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

/// [FetchPlan] for columns without an OffsetIndex.
///
/// Pages are discovered lazily by scanning headers in the bytes of the row group's [FetchSequence]
/// section requests that hold the column chunk: the whole chunk, cut into requests of at most S,
/// or, under a row limit, a first part sized by [#computeChunkSize]. A page or a header peek that
/// straddles the cut between two section requests is copied from both into one buffer. Under a row
/// limit, a walk that reads past the first part inserts the rest of the chunk as a demanded section
/// request ([SectionCursor.Rest]), so a read the limit covers fetches nothing more.
///
/// The first part's size, under a row limit: the full column chunk when it is at most 4 MiB;
/// otherwise sized from the column's average compressed bytes-per-value
/// (`totalCompressedSize / numValues`) multiplied by `maxRows` and a safety factor, floored at
/// 1 MiB and capped at S.
public final class SequentialFetchPlan implements FetchPlan {

    /// Minimum chunk size when `maxRows` is active (1 MB).
    /// Sized to roughly one Parquet data page so header scanning does not
    /// require sub-page round-trips.
    private static final int MAX_ROWS_CHUNK_FLOOR = 1024 * 1024;

    /// Safety factor applied to the `maxRows * avgBytesPerValue` estimate
    /// to absorb per-value size skew within the column chunk.
    private static final int MAX_ROWS_CHUNK_SAFETY_FACTOR = 2;

    /// Upper bound on `columnChunkLength` for fetching the entire column
    /// in one go even under `maxRows` truncation (4 MB). Below this
    /// threshold the over-fetch is small (≤ 4× the `head(N)` floor). Above it,
    /// the per-column truncation stays in effect: a 50 MB column under `head(30)`
    /// should not pull down 50 MB. See #382.
    private static final int FETCH_WHOLE_COLUMN_THRESHOLD = 4 * MAX_ROWS_CHUNK_FLOOR;

    /// A record count not known yet: the page's repetition levels are still to be read.
    private static final int UNKNOWN = -1;

    private final long columnChunkOffset;
    private final int columnChunkLength;
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
    /// The section requests overlapping the chunk, in offset order.
    private final List<? extends Section> sections;
    /// Inserts the rest of the chunk a row limit left out, or `null` when the section requests hold
    /// it all.
    private final SectionCursor.Rest rest;
    /// Page headers the most recent walk scanned, final once that walk runs to exhaustion and zero
    /// for one abandoned before it. Counts every header the walk read, including those of pages a
    /// row mask or the inline statistics then kept it from emitting, which is what separates a walk
    /// that stopped scanning early from one that read on and discarded what it found.
    private int scannedPages;

    /// A plan over `sections`, which hold the column chunk from `columnChunkOffset` on, or
    /// its first part with `rest` inserting what follows.
    public SequentialFetchPlan(long columnChunkOffset, int columnChunkLength, ColumnSchema columnSchema,
                        ColumnChunk columnChunk, HardwoodContextImpl context,
                        long maxRows, int rowGroupIndex, String fileName,
                        List<ResolvedPredicate> dropLeaves, BoundsReadability dropReadability,
                        RowRanges matchingRows, long rowGroupRowCount,
                        Dictionary preloadedDictionary, WorkingSet.Column workingSet,
                        List<? extends Section> sections, SectionCursor.Rest rest) {
        if (matchingRows == null) {
            throw new IllegalArgumentException("matchingRows must not be null; use RowRanges.ALL");
        }
        if (!matchingRows.isAll() && rowGroupRowCount <= 0) {
            throw new IllegalArgumentException(
                    "rowGroupRowCount must be positive when matchingRows is non-trivial, got "
                            + rowGroupRowCount);
        }
        this.columnChunkOffset = columnChunkOffset;
        this.columnChunkLength = columnChunkLength;
        this.columnSchema = columnSchema;
        this.columnChunk = columnChunk;
        this.context = context;
        this.maxRows = maxRows;
        this.rowGroupIndex = rowGroupIndex;
        this.fileName = fileName;
        this.dropLeaves = dropLeaves == null ? List.of() : dropLeaves;
        this.dropReadability = dropReadability;
        this.matchingRows = matchingRows;
        this.rowGroupRowCount = rowGroupRowCount;
        this.preloadedDictionary = preloadedDictionary;
        this.workingSet = workingSet;
        this.sections = sections;
        this.rest = rest;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public PageWalk walk() {
        return new SequentialPageWalk();
    }

    /// Returns how many page headers the most recent walk scanned. See [#scannedPages].
    public int scannedPages() {
        return scannedPages;
    }

    /// Builds a plan for one column chunk outside a read, with no predicate-driven page skipping
    /// and no per-page row mask; its section requests start on demand only
    /// ([FetchSequence#demandOnly]). On a file whose bytes are in memory, its walk never stops for
    /// bytes.
    public static SequentialFetchPlan build(InputFile inputFile, ColumnSchema columnSchema,
                                      ColumnChunk columnChunk, HardwoodContextImpl context,
                                      int rowGroupIndex, String fileName, long maxRows) {
        return build(inputFile, columnSchema, columnChunk, context, rowGroupIndex, fileName,
                maxRows, List.of(), RowRanges.ALL, 0L);
    }

    /// As [#build(InputFile, ColumnSchema, ColumnChunk, HardwoodContextImpl, int, String, long)],
    /// dropping data pages whose inline [Statistics] prove they cannot match any of the given
    /// AND-necessary leaf predicates, and keeping only the records within `matchingRows`. Dropped
    /// pages are replaced with [PageInfo#nullPlaceholder] pages carrying the same
    /// `numValues`, so row alignment across sibling columns is preserved and the record-level
    /// filter drops the rows via SQL three-valued logic. Page skipping is only applied when the
    /// column is optional (`maxDefinitionLevel > 0`).
    public static SequentialFetchPlan build(InputFile inputFile, ColumnSchema columnSchema,
                                      ColumnChunk columnChunk, HardwoodContextImpl context,
                                      int rowGroupIndex, String fileName, long maxRows,
                                      List<ResolvedPredicate> dropLeaves,
                                      RowRanges matchingRows, long rowGroupRowCount) {
        int maxSpan = CoalescingPolicy.maxSpanBytes();
        long start = columnChunk.chunkStartOffset();
        int length = chunkLength(columnChunk, rowGroupIndex, fileName, columnSchema);
        int firstPart = Math.min(length, computeChunkSize(length, columnChunk.metaData(), maxRows, maxSpan));
        String reason = "rg=" + rowGroupIndex + " col=" + columnSchema.name();
        FetchSequence sequence = FetchSequence.demandOnly(context);
        List<Span> spans = new ArrayList<>();
        for (long piece = start; piece < start + firstPart; piece += maxSpan) {
            spans.add(new Span(piece, Math.toIntExact(Math.min(maxSpan, start + firstPart - piece)),
                    reason, 1));
        }
        List<SectionRequest> sections = sequence.insert(0, 0, inputFile, spans);
        SectionCursor.Rest rest = firstPart == length ? null
                : SectionCursor.rest(sequence, 0, inputFile, start + length, reason, maxSpan);
        return new SequentialFetchPlan(start, length, columnSchema, columnChunk, context, maxRows,
                rowGroupIndex, fileName, dropLeaves, BoundsReadability.ALL, matchingRows, rowGroupRowCount,
                null, WorkingSet.standalone(columnSchema), sections, rest);
    }

    /// The length of `columnChunk`.
    ///
    /// @throws UnsupportedOperationException if the chunk is longer than this reader can address
    public static int chunkLength(ColumnChunk columnChunk, int rowGroupIndex, String fileName, ColumnSchema columnSchema) {
        long totalCompressedSize = columnChunk.metaData().totalCompressedSize();
        if (totalCompressedSize > Integer.MAX_VALUE) {
            // The file is correct; this reader will not fetch a region it cannot
            // address with an int, and a second attempt changes nothing.
            throw new UnsupportedOperationException(ExceptionContext.readPrefix(fileName, rowGroupIndex,
                    columnSchema.fieldPath().toString())
                    + "Column chunk too large (" + totalCompressedSize
                    + " bytes); the reader supports column chunks of up to " + Integer.MAX_VALUE + " bytes");
        }
        return Math.toIntExact(totalCompressedSize);
    }

    /// Computes the size of the first part of a column chunk a read under `maxRows` fetches.
    ///
    /// Without `maxRows`, returns `maxSpan` (S), so the whole chunk is needed. With `maxRows`,
    /// estimates the bytes required from the column's average compressed bytes-per-value:
    ///
    /// ```
    /// chunkSize = maxRows * (totalCompressedSize / numValues) * safetyFactor
    /// ```
    ///
    /// The estimate is floored at [MAX_ROWS_CHUNK_FLOOR] to avoid sub-page
    /// round-trips during header scanning and capped at `maxSpan`.
    /// Columns whose entire chunk is below [FETCH_WHOLE_COLUMN_THRESHOLD]
    /// short-circuit to the full chunk length (#382).
    public static int computeChunkSize(int columnChunkLength, ColumnMetaData metaData, long maxRows, int maxSpan) {
        if (maxRows <= 0) {
            return Math.max(columnChunkLength, maxSpan);
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
        // Cap the floor at the ceiling so a span limit below the floor does not produce an
        // invalid clamp range.
        long floor = Math.min(MAX_ROWS_CHUNK_FLOOR, maxSpan);
        long bounded = Math.clamp(estimate, floor, maxSpan);
        return Math.toIntExact(bounded);
    }

    /// Discovers pages by scanning headers in the section requests that hold the chunk, as a state
    /// machine over the bytes present.
    ///
    /// The page under way is held in fields: its parsed header, the peek it was parsed from, its
    /// record count once its repetition levels are read, and its inline-statistics decision. An
    /// section request the page needs that is not present stops [#advance] with the page's state in
    /// place, and the next call resumes it, so no header is parsed twice and no byte is read twice.
    /// The counters of the walk ([#position], [#valuesRead], [#recordsRead], [#pageCount]) move
    /// only when a page is done with. No read starts before the header of the page under way, so
    /// the walk passes the section requests that end at or before it.
    private class SequentialPageWalk implements PageWalk {

        private final ColumnMetaData metaData = columnChunk.metaData();
        /// Where this chunk is, for a page whose statistics turn out to be unusable to name.
        private final LogContext logContext =
                new LogContext(fileName, rowGroupIndex).withColumn(columnSchema.fieldPath());
        private final SectionCursor cursor = new SectionCursor(sections, rest);
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
        /// The peek the header is read from, `0` until the page's first attempt sets it.
        private int peekSize;
        /// The page's top-level records, [#UNKNOWN] until its repetition levels are read.
        private int recordsInPage = UNKNOWN;
        /// Whether the page's inline statistics are evaluated, and what they decided.
        private boolean dropDecided;
        private boolean dropByStats;

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
            cursor.releaseAll();
            PageWalk.scanned(fileName, rowGroupIndex, columnSchema, pageCount, RowGroupScannedEvent.STRATEGY_SEQUENTIAL);
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
            if (peekSize == 0) {
                peekSize = PageFormatProbe.INITIAL_PEEK_SIZE;
                // No later read starts before this header
                cursor.releaseBefore(columnChunkOffset + position);
            }
            Parsed<HeaderAt> read = PageFormatProbe.readHeader(cursor, columnChunkOffset + position,
                    columnChunkLength - position, peekSize, "");
            if (read.needsBytes()) {
                // Resumed from the peek reached, so no peek is read twice
                peekSize = read.needLength();
                pending = cursor.pending();
                return false;
            }
            header = read.value().header();
            headerSize = read.value().length();
            return true;
        }

        /// The `length` bytes at chunk-relative `relPos`: a slice of the section request holding
        /// them, or a copy when they straddle a cut.
        ///
        /// @return the bytes, or `null` when a section request holding them is not present, which
        ///         is then [#pending]
        private ByteBuffer readBytes(int relPos, int length) throws IOException {
            ByteBuffer bytes = cursor.read(columnChunkOffset + relPos, length);
            if (bytes == null) {
                pending = cursor.pending();
            }
            return bytes;
        }

    /// Scans past the dictionary page (if present) on first access.
        ///
        /// @return whether the walk is past it; `false` when bytes it needs are pending
        private boolean initialize() throws IOException {
            if (preloadedDictionary != null) {
                // The plan starts at the first data page: pruning has read the dictionary page.
                dictionary = DictionaryStep.preloaded(preloadedDictionary, workingSet);
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
                int dictTotalSize = headerSize + header.compressedPageSize();
                ByteBuffer dictPage = readBytes(position, dictTotalSize);
                if (dictPage == null) {
                    return false;
                }
                WorkingSet.DictionaryParse parse = DictionaryStep.parse(new DataPage(header, headerSize, dictPage),
                        columnSchema, metaData, context, workingSet);
                if (parse.pending() != null) {
                    pending = parse.pending();
                    return false;
                }
                dictionary = parse.dictionary();
                position += dictTotalSize;
                clearPage();
                currentPage = ExceptionContext.UNKNOWN_PAGE;
                initialized = true;
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
                            PageInfo.keptRecords(columnSchema, PageRowMask.ALL, placeholderRecords));
                }
                else {
                    ByteBuffer pageData = readBytes(position, totalPageSize);
                    if (pageData == null) {
                        return null;
                    }
                    pageInfo = new PageInfo(new DataPage(header, headerSize, pageData), columnSchema, metaData,
                            dictionary, mask, PageInfo.keptRecords(columnSchema, mask, numValues));
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
            ByteBuffer repLevels = readBytes(position + headerSize, repLevelLength);
            if (repLevels == null) {
                return UNKNOWN;
            }
            return PageRecordCounter.countTopLevelRecords(repLevels, 0, repLevelLength,
                    numValues, columnSchema.maxRepetitionLevel());
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

    }
}
