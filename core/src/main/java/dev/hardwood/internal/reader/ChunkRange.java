/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;

/// A contiguous byte range covering one or more column chunks within a single
/// row group.
///
/// Produced by coalescing nearby column chunk regions so that a single
/// `readRange()` call can fetch multiple chunks at once. Each entry
/// records the original column index and the chunk's position within the
/// range for slicing after the read.
///
/// @param offset  absolute file offset of the first byte in this range
/// @param length  total number of bytes in this range
/// @param entries the column chunks covered by this range, in file order
public record ChunkRange(long offset, int length, List<ChunkEntry> entries) {

    /// Maximum gap (in bytes) between two regions that will be merged into a
    /// single `readRange()` call. Regions separated by more than this
    /// gap are fetched in separate calls.
    ///
    /// 1 MB is a reasonable default: it limits over-fetching while ensuring
    /// that typical adjacent column chunks collapse into a single request.
    /// For local files the gap tolerance is harmless —
    /// `MappedInputFile.readRange()` is a zero-copy slice regardless.
    public static final int MAX_GAP_BYTES = 1024 * 1024;

    /// A single column chunk within a [ChunkRange].
    ///
    /// @param columnIndex original column index within the row group
    /// @param chunkOffset absolute file offset of this chunk
    /// @param chunkLength compressed size of this chunk in bytes
    record ChunkEntry(int columnIndex, long chunkOffset, int chunkLength) {
    }

    /// Collects projected column chunks from a row group and coalesces them
    /// into merged byte ranges.
    ///
    /// @param columns        all column chunks in the row group
    /// @param projectedColumns original column indices to include
    /// @param maxGapBytes    maximum gap (in bytes) to bridge when merging
    /// @return coalesced ranges, each covering one or more column chunks
    public static List<ChunkRange> coalesce(List<ColumnChunk> columns, int[] projectedColumns, int maxGapBytes) {
        List<ChunkEntry> entries = new ArrayList<>(projectedColumns.length);
        for (int colIdx : projectedColumns) {
            ColumnMetaData meta = columns.get(colIdx).metaData();
            Long dictOffset = meta.dictionaryPageOffset();
            long chunkStart = (dictOffset != null && dictOffset > 0)
                    ? dictOffset : meta.dataPageOffset();
            int chunkLen = Math.toIntExact(meta.totalCompressedSize());
            entries.add(new ChunkEntry(colIdx, chunkStart, chunkLen));
        }
        entries.sort(Comparator.comparingLong(ChunkEntry::chunkOffset));

        return coalesce(entries, maxGapBytes);
    }

    /// Coalesces pre-sorted chunk entries into merged byte ranges.
    /// Two consecutive entries are merged when the gap between the end of one
    /// and the start of the next is at most `maxGapBytes`.
    static List<ChunkRange> coalesce(List<ChunkEntry> entries, int maxGapBytes) {
        List<ChunkRange> ranges = new ArrayList<>();

        long rangeStart = entries.get(0).chunkOffset();
        long rangeEnd = rangeStart + entries.get(0).chunkLength();
        List<ChunkEntry> rangeEntries = new ArrayList<>();
        rangeEntries.add(entries.get(0));

        for (int i = 1; i < entries.size(); i++) {
            ChunkEntry entry = entries.get(i);
            long gap = entry.chunkOffset() - rangeEnd;

            if (gap <= maxGapBytes) {
                rangeEnd = Math.max(rangeEnd, entry.chunkOffset() + entry.chunkLength());
                rangeEntries.add(entry);
            }
            else {
                ranges.add(new ChunkRange(rangeStart,
                        Math.toIntExact(rangeEnd - rangeStart), List.copyOf(rangeEntries)));
                rangeStart = entry.chunkOffset();
                rangeEnd = rangeStart + entry.chunkLength();
                rangeEntries.clear();
                rangeEntries.add(entry);
            }
        }

        ranges.add(new ChunkRange(rangeStart,
                Math.toIntExact(rangeEnd - rangeStart), List.copyOf(rangeEntries)));
        return ranges;
    }
}
