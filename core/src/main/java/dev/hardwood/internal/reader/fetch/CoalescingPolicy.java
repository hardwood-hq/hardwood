/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.fetch;

import java.util.ArrayList;
import java.util.List;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.reader.memory.InternalProperty;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.schema.ColumnSchema;

/// Decides whether two byte ranges a read needs are fetched as one request: when the gap between
/// them is at most [#GAP_BYTES] (G) and the merged span at most [#maxSpanBytes()] (S). Every merge
/// on the read path applies this rule: a row group's needed data ranges across its columns
/// ([#coalesce]), and the slices of a page-index window ([CoalescedRequests]).
///
/// A merge only bridges the gap between two needed ranges; it never extends a request before the
/// first needed byte or past the last one.
public final class CoalescingPolicy {

    /// Largest gap bridged. Roughly the serial break-even of a remote request — the bytes that
    /// take as long to transfer as a round trip, e.g. 30 ms at 50 MiB/s — so fetching the gap
    /// costs no more than issuing a second request.
    static final int GAP_BYTES = 1024 * 1024;

    public static final String MAX_SPAN_BYTES_PROPERTY = "hardwood.internal.maxCoalescedBytes";

    /// Largest request without the override, so that each `readRange()` stays bounded, enabling
    /// read-ahead to overlap decode and a read that stops early to leave the rest unfetched.
    private static final int DEFAULT_MAX_SPAN_BYTES = 128 * 1024 * 1024;

    /// The least first part of a chunk a read under a row limit fetches, so that scanning its page
    /// headers takes no round trip per page.
    private static final int MAX_ROWS_CHUNK_FLOOR = 1024 * 1024;

    /// How many times the bytes a row limit's rows take on average the first part covers.
    private static final int MAX_ROWS_CHUNK_SAFETY_FACTOR = 2;

    /// The chunk length up to which a read under a row limit fetches the whole chunk at once.
    private static final int FETCH_WHOLE_COLUMN_THRESHOLD = 4 * MAX_ROWS_CHUNK_FLOOR;

    private CoalescingPolicy() {
    }

    /// The largest request, S: [#DEFAULT_MAX_SPAN_BYTES] unless overridden through
    /// [#MAX_SPAN_BYTES_PROPERTY]. Read on each call rather than cached, so that the override holds
    /// whenever it is set; a read takes it once, when its engine is built. A value that is not a
    /// positive byte count is rejected rather than replaced by the default: a span limit that does
    /// not take effect produces a plausible read with the wrong requests.
    public static int maxSpanBytes() {
        return InternalProperty.intValue(MAX_SPAN_BYTES_PROPERTY, DEFAULT_MAX_SPAN_BYTES, 1, "a positive byte count");
    }

    /// Whether the range `[nextStart, nextEnd)` joins a request spanning `[start, end)` under the
    /// span limit `maxSpan`.
    ///
    /// @param start first byte of the request so far
    /// @param end end of the request so far, exclusive
    /// @param nextStart first byte of the range to add, at or after `end`
    /// @param nextEnd end of the range to add, exclusive
    public static boolean merges(long start, long end, long nextStart, long nextEnd, long maxSpan) {
        return nextStart - end <= GAP_BYTES && nextEnd - start <= maxSpan;
    }

    /// Bytes `[start, end)` of the file one decoded column needs, before they are coalesced into
    /// requests.
    public record Need(int column, long start, long end) {
    }

    /// The bytes of a decoded column's chunk, `[start, end)`.
    public record Extent(int column, String name, long start, long end) {
    }

    /// Coalesces `needs`, in offset order, into a row group's requests, those [CoalescedRequests]
    /// plans for them: a request bridges gaps of at most G and is at most S long unless one need
    /// alone is longer. Its reason names every column whose need it holds.
    ///
    /// No request crosses `tailOffset`, where the tail the file's footer entry read begins
    /// ([TailedInputFile]): a request inside the tail is sliced from it, so a need that straddles
    /// the tail's start is cut there, and the part before it is all that is requested.
    public static List<FileRange> coalesce(List<Need> needs, String[] names, int rowGroupIndex, int maxSpan,
                                           long tailOffset) {
        needs = cutAt(needs, tailOffset);
        CoalescedRequests.Builder builder = CoalescedRequests.builder();
        for (Need need : needs) {
            builder.add(need.start(), Math.toIntExact(need.end() - need.start()));
        }
        CoalescedRequests requests = builder.build(tailOffset, maxSpan);
        List<FileRange> ranges = new ArrayList<>(requests.requestCount());
        int nextNeed = 0;
        boolean[] served = new boolean[names.length];
        List<Integer> servedOrder = new ArrayList<>();
        for (int r = 0; r < requests.requestCount(); r++) {
            long start = requests.requestStart(r);
            long end = requests.requestEnd(r);
            for (; nextNeed < needs.size() && needs.get(nextNeed).start() < end; nextNeed++) {
                int column = needs.get(nextNeed).column();
                if (!served[column]) {
                    served[column] = true;
                    servedOrder.add(column);
                }
            }
            StringBuilder reason = new StringBuilder("rg=").append(rowGroupIndex);
            for (int column : servedOrder) {
                reason.append(" col=").append(names[column]);
                served[column] = false;
            }
            servedOrder.clear();
            ranges.add(new FileRange(start, Math.toIntExact(end - start), reason.toString()));
        }
        return ranges;
    }

    /// `needs`, in offset order, with the one that straddles `offset` cut in two there.
    private static List<Need> cutAt(List<Need> needs, long offset) {
        for (int i = 0; i < needs.size(); i++) {
            Need need = needs.get(i);
            if (need.start() < offset && need.end() > offset) {
                List<Need> cut = new ArrayList<>(needs);
                cut.set(i, new Need(need.column(), need.start(), offset));
                cut.add(i + 1, new Need(need.column(), offset, need.end()));
                return cut;
            }
        }
        return needs;
    }

    /// The length of `columnChunk`.
    ///
    /// @throws UnsupportedOperationException if the chunk is longer than this reader can address
    public static int chunkLength(ColumnChunk columnChunk, int rowGroupIndex, String fileName,
                                  ColumnSchema columnSchema) {
        long totalCompressedSize = columnChunk.metaData().totalCompressedSize();
        if (totalCompressedSize > Integer.MAX_VALUE) {
            // the file is correct; this reader does not fetch a region it cannot address with an int
            throw new UnsupportedOperationException(ExceptionContext.readPrefix(fileName, rowGroupIndex,
                    columnSchema.fieldPath().toString())
                    + "Column chunk too large (" + totalCompressedSize
                    + " bytes); the reader supports column chunks of up to " + Integer.MAX_VALUE + " bytes");
        }
        return Math.toIntExact(totalCompressedSize);
    }

    /// The size of the first part of a column chunk without an OffsetIndex a read under `maxRows`
    /// fetches, the rest following on demand.
    ///
    /// Without `maxRows`, returns at least `maxSpan` (S), so the whole chunk is needed. With
    /// `maxRows`, estimates the bytes required from the column's average compressed bytes per
    /// value:
    ///
    /// ```
    /// chunkSize = maxRows * (totalCompressedSize / numValues) * safetyFactor
    /// ```
    ///
    /// The estimate is floored at [#MAX_ROWS_CHUNK_FLOOR] to avoid sub-page round trips while the
    /// headers are scanned, and capped at `maxSpan`. A chunk of at most
    /// [#FETCH_WHOLE_COLUMN_THRESHOLD] bytes is fetched whole.
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
        // both factors are bounded by the chunk's own counts, so the product cannot overflow
        long effectiveRows = Math.min(maxRows, numValues);
        long estimate = Math.ceilDiv(effectiveRows * totalCompressedSize * MAX_ROWS_CHUNK_SAFETY_FACTOR, numValues);
        // a span limit below the floor lowers the floor, so the clamp range stays valid
        long floor = Math.min(MAX_ROWS_CHUNK_FLOOR, maxSpan);
        return Math.toIntExact(Math.clamp(estimate, floor, maxSpan));
    }
}
