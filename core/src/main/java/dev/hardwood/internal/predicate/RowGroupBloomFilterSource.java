/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.io.IOException;
import java.nio.ByteBuffer;

import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.ExceptionContext.ReadContext.Region;
import dev.hardwood.internal.ReadScope;
import dev.hardwood.internal.bloomfilter.BloomFilter;
import dev.hardwood.internal.bloomfilter.BloomFilterHeader;
import dev.hardwood.internal.thrift.BloomFilterHeaderReader;
import dev.hardwood.internal.thrift.BloomFilterReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.RowGroup;

/// [BloomFilterSource] backed by one `(InputFile, RowGroup)` pair.
///
/// Each column's filter is read lazily — only when [#forColumn] is first called for it — and the
/// result (including absence) is cached for the lifetime of this source, so an `IN` list probing
/// the same column reads its filter once. The cache is a pair of arrays indexed by column position;
/// a row group is evaluated single-threaded (a sequential `stream().filter(...)`), so it needs no
/// synchronization.
public final class RowGroupBloomFilterSource implements BloomFilterSource {

    private static final System.Logger LOG = System.getLogger(RowGroupBloomFilterSource.class.getName());

    /// Bytes read to parse the header when `bloom_filter_length` is absent. The header is a tiny
    /// Thrift struct (an i32 plus three single-variant unions), comfortably under this bound.
    private static final int HEADER_PROBE_BYTES = 64;

    private final InputFile inputFile;
    private final RowGroup rowGroup;
    /// The row group's own index in the file, for the failures that can name no byte.
    private final int rowGroupIndex;
    /// Per-column filter cache indexed by column position; `null` entries mean either "not read yet"
    /// or "read, no filter" — `read[i]` disambiguates so absence is cached, not re-fetched.
    private final BloomFilter[] filters;
    private final boolean[] read;
    /// File length, resolved at most once and only on the legacy (length-absent) path. `-1` means
    /// "not yet fetched"; a Parquet file is never zero-length, so the sentinel is unambiguous.
    private long fileLength = -1;

    public RowGroupBloomFilterSource(InputFile inputFile, RowGroup rowGroup, int rowGroupIndex) {
        this.inputFile = inputFile;
        this.rowGroup = rowGroup;
        this.rowGroupIndex = rowGroupIndex;
        int columnCount = rowGroup.columns().size();
        this.filters = new BloomFilter[columnCount];
        this.read = new boolean[columnCount];
    }

    @Override
    public BloomFilter forColumn(int columnIndex) throws IOException {
        // Mirror RowGroupFilterEvaluator.getStatistics: an index past this row group's column count
        // (a narrower/corrupt footer reached via a predicate resolved against the reference schema)
        // yields "no filter" — conservatively keeping the row group — rather than throwing.
        if (columnIndex < 0 || columnIndex >= filters.length) {
            return null;
        }
        if (!read[columnIndex]) {
            filters[columnIndex] = readFilter(columnIndex);
            read[columnIndex] = true;
        }
        return filters[columnIndex];
    }

    private BloomFilter readFilter(int columnIndex) throws IOException {
        ColumnChunk columnChunk = rowGroup.columns().get(columnIndex);
        ColumnMetaData metaData = columnChunk.metaData();
        Long offset = metaData.bloomFilterOffset();
        if (offset == null) {
            return null;
        }
        // The offset addresses the file named by file_path, not this one. Pruning on whatever
        // sits there would drop row groups that match.
        requireSameFile(columnChunk, columnIndex);

        try (ReadScope.Scope scope = ReadScope.file(inputFile.name())
                .column(rowGroupIndex, columnPath(columnIndex))
                .region(Region.BLOOM_FILTER, offset)) {
            return readFilterAt(metaData, offset);
        }
    }

    /// Reads the filter the footer placed at `offset`, inside the scope that
    /// names where that is.
    private BloomFilter readFilterAt(ColumnMetaData metaData, long offset) throws IOException {
        if (offset <= 0) {
            // The offset is present but points at or before the file's magic header, so it cannot
            // name a real filter. Treat it as corruption but stay conservative — decline to prune
            // rather than throw, keeping the row group (statistics still apply) — and warn so the
            // malformed footer is visible instead of silently reducing pruning.
            ReadScope.Place where = ReadScope.current();
            LOG.log(System.Logger.Level.WARNING, () -> where.describe()
                    + "Ignoring invalid bloom_filter_offset " + offset
                    + "; keeping the row group (statistics still apply)");
            return null;
        }
        try {
            return readFilter(offset, metaData.bloomFilterLength());
        }
        catch (IOException e) {
            throw new IOException(ReadScope.here() + "Failed to read the bloom filter", e);
        }
    }

    private FieldPath columnPath(int columnIndex) {
        return rowGroup.columns().get(columnIndex).metaData().pathInSchema();
    }

    /// Reads the filter at `offset`. When `length` is known the whole region is read in one call;
    /// otherwise the header is probed first to derive the total length.
    private BloomFilter readFilter(long offset, Integer length) throws IOException {
        if (length != null) {
            ByteBuffer buffer = inputFile.readRange(offset, length);
            return BloomFilterReader.read(new ThriftCompactReader(buffer));
        }
        // Legacy writers omit bloom_filter_length. Over-read a fixed window — large enough to
        // hold the header (a fixed-shape struct: an i32 plus three single-variant unions, ~19
        // bytes at most), clamped so it never runs past EOF — and parse just the header to learn
        // the region's total length.
        int probe = Math.toIntExact(Math.min(fileLength() - offset, HEADER_PROBE_BYTES));
        ByteBuffer window = inputFile.readRange(offset, probe);
        ThriftCompactReader windowReader = new ThriftCompactReader(window);
        BloomFilterHeader header = BloomFilterHeaderReader.read(windowReader);
        int totalLength = Math.addExact(windowReader.getBytesRead(), header.numBytes());
        if (totalLength <= probe) {
            // The probe window already covers the whole filter; slice the bitset straight from
            // where the header parse ended, reusing the parsed header instead of decoding it again.
            return BloomFilterReader.readBitset(header, windowReader);
        }
        // The bitset extends past the probe window: re-fetch the exact region and parse it in full.
        ByteBuffer buffer = inputFile.readRange(offset, totalLength);
        return BloomFilterReader.read(new ThriftCompactReader(buffer));
    }

    /// File length, fetched at most once. Only the legacy length-absent path needs it, so it is
    /// resolved lazily rather than in the constructor.
    private long fileLength() throws IOException {
        if (fileLength < 0) {
            fileLength = inputFile.length();
        }
        return fileLength;
    }

    /// Fails unless this chunk stores its data in the file being read.
    ///
    /// The split-file layout is legal Parquet that this reader does not read, so the failure
    /// leaves as the [UnsupportedOperationException] it arrived as, carrying the column it
    /// was raised for.
    private void requireSameFile(ColumnChunk columnChunk, int columnIndex) {
        try {
            columnChunk.requireSameFile();
        }
        catch (UnsupportedOperationException e) {
            // Named, not positioned: the file is correct, so no byte of it is
            // worth putting in front of anyone.
            throw new UnsupportedOperationException(ExceptionContext.filePrefix(inputFile.name())
                    + "Cannot read column " + columnIndex + ": " + e.getMessage(), e);
        }
    }
}
