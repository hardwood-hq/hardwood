/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import dev.hardwood.InputFile;
import dev.hardwood.internal.thrift.OffsetIndexReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// The bytes a read has to fetch to decode its result, and a check that a read fetched no more.
///
/// A read needs, for every column it decodes and every row group holding a matching row, the data
/// pages whose rows intersect the matching rows, plus the column's dictionary page. A column chunk
/// without an OffsetIndex cannot be narrowed to pages, so all of it is needed. This is computed from
/// the footer and the OffsetIndex alone, independently of the reader under test.
///
/// A read is within budget when every data byte it fetches
///
/// - lies inside a needed range, or in a gap of at most
///   [CoalescingPolicy#GAP_BYTES] between two needed ranges: coalescing bridges such
///   gaps, but never extends a fetch before the first needed byte or past the last one; and
/// - is fetched once.
///
/// Data bytes are those inside a column chunk. Reads issued for row-group pruning, which read a
/// chunk's dictionary page or bloom filter to decide whether to read the chunk at all, carry the
/// `pruning` fetch reason and are left out: their cost is pinned by the pruning tests.
final class IoBudget {

    private static final String PRUNING_REASON = " pruning";

    /// File bytes `[offset, end)`.
    record Range(long offset, long end) {

        boolean contains(long from, long to) {
            return offset <= from && to <= end;
        }

        boolean overlaps(long from, long to) {
            return from < end && offset < to;
        }
    }

    private final List<Range> chunks;
    private final List<Range> allowed;

    private IoBudget(List<Range> chunks, List<Range> allowed) {
        this.chunks = chunks;
        this.allowed = allowed;
    }

    /// The budget of a read of `columns` whose result is the file-wide rows `[fromRow, toRow)`.
    static IoBudget of(Path file, List<String> columns, long fromRow, long toRow) throws IOException {
        InputFile inputFile = InputFile.of(file);
        inputFile.open();
        try (inputFile; ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            FileSchema schema = reader.getFileSchema();
            List<Range> chunks = new ArrayList<>();
            List<Range> needed = new ArrayList<>();
            long rowGroupFirstRow = 0;
            for (RowGroup rowGroup : reader.getFileMetaData().rowGroups()) {
                long from = Math.max(fromRow - rowGroupFirstRow, 0);
                long to = Math.min(toRow - rowGroupFirstRow, rowGroup.numRows());
                for (ColumnChunk chunk : rowGroup.columns()) {
                    long start = chunk.chunkStartOffset();
                    chunks.add(new Range(start, start + chunk.metaData().totalCompressedSize()));
                }
                if (from < to) {
                    for (String column : columns) {
                        ColumnChunk chunk = rowGroup.columns().get(schema.getColumn(column).columnIndex());
                        needed.addAll(neededRanges(inputFile, chunk, rowGroup.numRows(), from, to));
                    }
                }
                rowGroupFirstRow += rowGroup.numRows();
            }
            return new IoBudget(chunks, bridge(needed));
        }
    }

    /// Asserts that the data reads `file` recorded are within this budget.
    void assertWithin(CountingInputFile file) {
        List<CountingInputFile.Read> dataReads = file.reads().stream()
                .filter(read -> !read.reason().endsWith(PRUNING_REASON))
                .filter(read -> chunks.stream().anyMatch(chunk -> chunk.overlaps(read.offset(), read.end())))
                .sorted(Comparator.comparingLong(CountingInputFile.Read::offset))
                .toList();

        assertThat(dataReads)
                .filteredOn(read -> allowed.stream().noneMatch(range -> range.contains(read.offset(), read.end())))
                .as("data reads outside the needed ranges %s bridged by gaps of at most %d bytes",
                        allowed, CoalescingPolicy.GAP_BYTES)
                .isEmpty();

        for (int i = 1; i < dataReads.size(); i++) {
            CountingInputFile.Read previous = dataReads.get(i - 1);
            assertThat(dataReads.get(i).offset())
                    .as("data bytes fetched twice: %s and %s", previous, dataReads.get(i))
                    .isGreaterThanOrEqualTo(previous.end());
        }
    }

    private static List<Range> neededRanges(InputFile inputFile, ColumnChunk chunk, long numRows,
            long from, long to) throws IOException {
        long chunkStart = chunk.chunkStartOffset();
        if (chunk.offsetIndexOffset() == null) {
            return List.of(new Range(chunkStart, chunkStart + chunk.metaData().totalCompressedSize()));
        }
        List<PageLocation> pages = OffsetIndexReader.read(new ThriftCompactReader(
                inputFile.readRange(chunk.offsetIndexOffset(), chunk.offsetIndexLength()))).pageLocations();
        List<Range> needed = new ArrayList<>();
        for (int i = 0; i < pages.size(); i++) {
            PageLocation page = pages.get(i);
            long pageEndRow = i + 1 < pages.size() ? pages.get(i + 1).firstRowIndex() : numRows;
            if (page.firstRowIndex() < to && from < pageEndRow) {
                needed.add(new Range(page.offset(), page.offset() + page.compressedPageSize()));
            }
        }
        long firstPageOffset = pages.get(0).offset();
        if (!needed.isEmpty() && chunkStart < firstPageOffset) {
            needed.add(new Range(chunkStart, firstPageOffset));
        }
        return needed;
    }

    /// Merges ranges no further apart than the coalescing gap into one.
    private static List<Range> bridge(List<Range> ranges) {
        List<Range> sorted = ranges.stream().sorted(Comparator.comparingLong(Range::offset)).toList();
        List<Range> merged = new ArrayList<>();
        for (Range range : sorted) {
            Range last = merged.isEmpty() ? null : merged.get(merged.size() - 1);
            if (last != null && range.offset() - last.end() <= CoalescingPolicy.GAP_BYTES) {
                merged.set(merged.size() - 1, new Range(last.offset(), Math.max(last.end(), range.end())));
            }
            else {
                merged.add(range);
            }
        }
        return merged;
    }
}
