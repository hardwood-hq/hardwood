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
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.bloomfilter.BloomFilter;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.RowGroup;

/// [BloomFilterSource] backed by one `(InputFile, RowGroup)` pair.
///
/// Each column's filter is read lazily — only when [#forColumn] is first called for it — and the
/// result (including absence) is cached for the lifetime of this source, so an `IN` list probing
/// the same column reads its filter once. The cache is a pair of arrays indexed by column position;
/// a row group is evaluated single-threaded (a sequential `stream().filter(...)`), so it needs no
/// synchronization.
///
/// When a [BloomFilterPrefetch] was supplied by the pruning pass, a filter it already fetched is
/// served from those bytes with no `readRange`; anything unprefetched — including candidates the
/// prefetcher skipped or whose fetch failed — goes through the lazy path below, exactly as it
/// would without prefetching.
public final class RowGroupBloomFilterSource implements BloomFilterSource {

    private static final System.Logger LOG = System.getLogger(RowGroupBloomFilterSource.class.getName());

    private final InputFile inputFile;
    private final RowGroup rowGroup;
    /// Prefetched filter bytes shared across this file's sources, or `null` when prefetching is
    /// disabled. Lookup keys are `bloom_filter_offset` values, unique per filter.
    private final BloomFilterPrefetch prefetch;
    /// Per-column filter cache indexed by column position; `null` entries mean either "not read yet"
    /// or "read, no filter" — `read[i]` disambiguates so absence is cached, not re-fetched.
    private final BloomFilter[] filters;
    private final boolean[] read;
    /// File length, resolved at most once and only on the legacy (length-absent) path. `-1` means
    /// "not yet fetched"; a Parquet file is never zero-length, so the sentinel is unambiguous.
    private long fileLength = -1;

    public RowGroupBloomFilterSource(InputFile inputFile, RowGroup rowGroup) {
        this(inputFile, rowGroup, null);
    }

    /// Use when the pruning pass prefetched the row group's filters; pass `null` for the
    /// plain lazy behavior.
    public RowGroupBloomFilterSource(InputFile inputFile, RowGroup rowGroup, BloomFilterPrefetch prefetch) {
        this.inputFile = inputFile;
        this.rowGroup = rowGroup;
        this.prefetch = prefetch;
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
        requireSameFile(columnChunk);
        if (offset <= 0) {
            // The offset is present but points at or before the file's magic header, so it cannot
            // name a real filter. Treat it as corruption but stay conservative — decline to prune
            // rather than throw, keeping the row group (statistics still apply) — and warn so the
            // malformed footer is visible instead of silently reducing pruning.
            LOG.log(System.Logger.Level.WARNING, () -> columnPrefix(metaData)
                    + "Ignoring invalid bloom_filter_offset " + offset
                    + "; keeping the row group (statistics still apply)");
            return null;
        }
        try {
            Integer length = metaData.bloomFilterLength();
            if (prefetch != null) {
                BloomFilterPrefetch.PrefetchedBloom prefetched = prefetch.lookup(offset);
                if (prefetched != null) {
                    return BloomFilterProbe.parseComplete(prefetched.data());
                }
                if (length == null) {
                    BloomFilterPrefetch.PrefetchedProbe probe = prefetch.lookupProbe(offset);
                    if (probe != null) {
                        return readFilter(offset, probe.filterLength());
                    }
                }
            }
            return readFilter(offset, length);
        }
        catch (UnsupportedOperationException e) {
            // The header names an algorithm, hash or compression this version of Hardwood does
            // not implement — a correct file it cannot evaluate, not a corrupt one. The filter
            // is unusable but the row group is still readable, so decline to prune rather than
            // fail the whole read, and warn so the reduced pruning is visible.
            //
            // Reading a filter raises this for no other reason, so the catch is as narrow as the
            // condition. Anything unsupported added under here later would be swallowed by it:
            // raise that from the frame that knows what to do with it instead.
            LOG.log(System.Logger.Level.WARNING, () -> columnPrefix(metaData)
                    + "Cannot evaluate the bloom filter: " + e.getMessage()
                    + "; keeping the row group (statistics still apply)");
            return null;
        }
    }

    /// The `[file: column 'X'] ` prefix for a message about one column of this row group.
    ///
    /// A `RowGroup` does not carry its own ordinal, so the row group is the one part of the
    /// position this class cannot name.
    private String columnPrefix(ColumnMetaData metaData) {
        return ExceptionContext.readPrefix(inputFile.name(), ExceptionContext.UNKNOWN_ROW_GROUP,
                metaData.pathInSchema().toString());
    }

    /// Reads the filter at `offset`. When `length` is known the whole region is read in one call;
    /// otherwise the header is probed first to derive the total length.
    private BloomFilter readFilter(long offset, Integer length) throws IOException {
        try (FetchReason.Scope ignored = FetchReason.set("bloom filter " + offset)) {
            if (length != null) {
                ByteBuffer buffer = inputFile.readRange(offset, length);
                return BloomFilterProbe.parseComplete(buffer);
            }
            // Legacy writers omit bloom_filter_length. Over-read a fixed window — large enough to
            // hold the header (a fixed-shape struct: an i32 plus three single-variant unions, ~19
            // bytes at most), clamped so it never runs past EOF — and parse just the header to learn
            // the region's total length.
            int probe = BloomFilterProbe.probeLength(fileLength(), offset);
            BloomFilterProbe.Result parsed = BloomFilterProbe.parseWindow(inputFile.readRange(offset, probe));
            return switch (parsed) {
                // The probe window already covers the whole filter: use it directly, no re-fetch.
                case BloomFilterProbe.Result.Complete complete -> complete.filter();
                // The bitset extends past the probe window: re-fetch the exact region and parse it in full.
                case BloomFilterProbe.Result.Oversized oversized ->
                    BloomFilterProbe.parseComplete(inputFile.readRange(offset, oversized.totalLength()));
            };
        }
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
    /// Checked, because reading a filter is a read like any other and every frame above this
    /// one says so; the cause is the [IOException] the metadata contract advertises for the
    /// split-file layout.
    private void requireSameFile(ColumnChunk columnChunk) {
        try {
            columnChunk.requireSameFile();
        }
        catch (UnsupportedOperationException e) {
            throw new UnsupportedOperationException(
                    columnPrefix(columnChunk.metaData()) + e.getMessage(), e);
        }
    }
}
