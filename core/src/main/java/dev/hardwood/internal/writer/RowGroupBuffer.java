/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import dev.hardwood.OutputFile;
import dev.hardwood.internal.compression.Compressor;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;

/// Buffers the column chunks of a single row group. A record range's shredded levels are
/// appended across all columns; at flush the column chunks are written contiguously in
/// schema order, each recording the file offset at which its first page lands.
public final class RowGroupBuffer {

    /// Records appended between two readings of what the row group holds.
    ///
    /// It bounds how far a row group can overshoot its byte target: at most one slice, and a
    /// slice is drawn from the batch the caller has already materialized, a [ColumnSource]
    /// holding the caller's arrays by reference rather than copying them. Small enough that the
    /// overshoot is a fraction of the target for any record a caller can hold; large enough that
    /// a column's slice is one bulk copy rather than a call per value.
    private static final int SLICE_RECORDS = 4096;

    private final FileSchema schema;
    private final ColumnChunkBuffer[] columns;
    private final long targetBytes;
    /// The configured row target, held down to what a chunk's buffers can index. One bound rather
    /// than two: a caller's row target above the structural ceiling is the ceiling.
    private final int targetRows;
    private int rowCount;
    private long peakRetainedBytes;

    /// Writes out and resets the buffered row group, which [RowGroupBuffer#append] calls to cut
    /// it.
    @FunctionalInterface
    public interface Flush {

        /// Writes the buffered row group out and [resets][RowGroupBuffer#reset()] the buffer.
        void flush() throws IOException;
    }

    /// @param schema the file schema
    /// @param pageTargetBytes encoded bytes after which a data page is cut at flush
    /// @param rowGroupBufferTargetBytes retained bytes at which the row group is cut
    /// @param rowGroupTargetRows records at which the row group is cut
    /// @param encodings each leaf column's resolved encoding policy, in schema order
    /// @param statisticsTruncationLength the maximum `BYTE_ARRAY` `min` / `max` bound length
    /// @param compressor compresses each page body before it is buffered
    /// @param codec the codec `compressor` applies, recorded in each chunk's metadata
    public RowGroupBuffer(FileSchema schema, int pageTargetBytes, long rowGroupBufferTargetBytes,
                          long rowGroupTargetRows, ColumnEncoding[] encodings,
                          int statisticsTruncationLength, Compressor compressor, CompressionCodec codec) {
        this(schema, pageTargetBytes, rowGroupBufferTargetBytes, rowGroupTargetRows, encodings,
                statisticsTruncationLength, compressor, codec, MAX_STORE_CAPACITY);
    }

    /// A buffer whose column chunks are capped at `storeCapacity` rather than at
    /// [#MAX_STORE_CAPACITY], and whose row groups at the row ceiling that capacity sets. The
    /// real cap is crossed by a column chunk of two billion entries or two gigabytes of values,
    /// out of a test's reach, so the tests of the cut it forces lower it instead.
    RowGroupBuffer(FileSchema schema, int pageTargetBytes, long rowGroupBufferTargetBytes,
                   long rowGroupTargetRows, ColumnEncoding[] encodings, int statisticsTruncationLength,
                   Compressor compressor, CompressionCodec codec, int storeCapacity) {
        this.schema = schema;
        this.columns = new ColumnChunkBuffer[schema.getColumnCount()];
        this.targetBytes = rowGroupBufferTargetBytes;
        this.targetRows = (int) Math.min(rowGroupTargetRows, maxRows(storeCapacity));
        // An equal share each, so the whole schema's buffers start at one row group's worth
        // however many columns it has.
        long budgetBytesPerColumn = rowGroupBufferTargetBytes / columns.length;
        for (int c = 0; c < columns.length; c++) {
            columns[c] = new ColumnChunkBuffer(schema.getColumn(c), pageTargetBytes, budgetBytesPerColumn,
                    encodings[c], statisticsTruncationLength, compressor, codec, storeCapacity);
        }
    }

    /// Appends every record `shredder` is bound to, cutting the row group through `flush` as
    /// often as it fills. A record goes into one row group whole.
    ///
    /// A slice at a time. What a range actually costs is only known once it has been appended —
    /// a value interned against a live dictionary retains an index where it repeats and an index
    /// plus the value where it does not, which needs the hash — so a slice is sized by what it
    /// *could* cost, and the row group is then cut on what it turned out to hold. Sizing it by
    /// the bound is what keeps a batch whose records widen part way through from carrying a row
    /// group far past its target, which no measurement of the records already appended could
    /// anticipate.
    ///
    /// @param shredder bound to the current batch
    /// @param sources the current batch's value sources, one per column
    /// @param flush writes out and resets the row group each time it is cut
    /// @throws IllegalArgumentException if a record is more than an empty row group's stores can
    ///         take
    public void append(RecordShredder shredder, ColumnSource[] sources, Flush flush) throws IOException {
        int rows = shredder.recordCount();
        int pos = 0;
        // Carried across iterations: what the row group holds after a slice is also the room the
        // next slice is sized against, so it is read once per slice rather than once for each.
        long retained = retainedBytes();
        while (pos < rows) {
            int slice = sliceThatFits(shredder, sources, pos,
                    Math.min(Math.min(rows - pos, SLICE_RECORDS), targetRows - rowCount),
                    targetBytes - retained);
            if (slice == 0) {
                // The next record would take a column chunk's store past what it can hold, a
                // structural cap that closes the group whatever the byte target says.
                retained = cut(flush);
                continue;
            }
            appendRecords(shredder, sources, pos, slice);
            pos += slice;
            retained = retainedBytes();
            peakRetainedBytes = Math.max(peakRetainedBytes, retained);
            // Either target closes the group, whichever is reached first.
            if (retained >= targetBytes || rowCount >= targetRows) {
                retained = cut(flush);
            }
        }
    }

    /// Cuts the row group through `flush` and returns what the buffer retains after it.
    private long cut(Flush flush) throws IOException {
        flush.flush();
        if (rowCount != 0) {
            throw new IllegalStateException("A row group flush left " + rowCount + " records buffered");
        }
        return retainedBytes();
    }

    /// Shreds the same record range into every column and advances the row count.
    private void appendRecords(RecordShredder shredder, ColumnSource[] sources, int from, int count) {
        for (int c = 0; c < columns.length; c++) {
            columns[c].append(shredder, sources[c], c, from, count);
        }
        rowCount += count;
    }

    /// The largest slice of `[from, from + count)`, halving down from `count`, that this row group
    /// can take without passing `room` — or one record, which goes in whatever it costs, a record
    /// not being divisible across row groups. `0` where not even one more record can go in without
    /// overflowing one of a column chunk's stores, which closes the row group whatever `room`
    /// says.
    ///
    /// The bound is what a slice can cost rather than what it will, so this is a floor on how much
    /// could be appended rather than the most that would fit. That is all it has to be against
    /// `room`: the row group is cut on what it turns out to hold, so an over-cautious slice costs
    /// an extra reading of a number every buffer already tracks, never a row group cut short.
    ///
    /// Halving rather than searching for the largest fit keeps this to a dozen evaluations of the
    /// bound in the worst case, and to one wherever a whole slice fits — which is every slice of a
    /// row group but its last few.
    ///
    /// @throws IllegalArgumentException if the row group is empty and its stores still cannot
    ///         take the record at `from`, which no row group can then hold
    private int sliceThatFits(RecordShredder shredder, ColumnSource[] sources, int from, int count,
                              long room) {
        for (int slice = count; slice > 0; slice >>= 1) {
            long bound = maxRetainedBytesFor(shredder, sources, from, slice);
            if (bound != ColumnChunkBuffer.EXCEEDS_STORES && (bound <= room || slice == 1)) {
                return slice;
            }
        }
        if (rowCount == 0) {
            throw recordTooLarge(shredder, sources, from);
        }
        return 0;
    }

    /// The most appending `[from, from + count)` can add across every column, or
    /// [ColumnChunkBuffer#EXCEEDS_STORES] where some column's stores cannot take it.
    private long maxRetainedBytesFor(RecordShredder shredder, ColumnSource[] sources, int from,
                                     int count) {
        long bytes = 0;
        for (int c = 0; c < columns.length; c++) {
            long column = maxRetainedBytesFor(shredder, sources, c, from, count);
            if (column == ColumnChunkBuffer.EXCEEDS_STORES) {
                return ColumnChunkBuffer.EXCEEDS_STORES;
            }
            bytes += column;
        }
        return bytes;
    }

    private long maxRetainedBytesFor(RecordShredder shredder, ColumnSource[] sources, int column,
                                     int from, int count) {
        long leaves = shredder.leafRange(column, from, count);
        return columns[column].maxRetainedBytesFor(sources[column], count,
                (int) (leaves >>> Integer.SIZE), (int) leaves, shredder.phantomLayers(column));
    }

    /// Reports the record at `from` as one that no row group can hold, naming the first column
    /// whose stores cannot take it.
    private IllegalArgumentException recordTooLarge(RecordShredder shredder, ColumnSource[] sources,
                                                    int from) {
        for (int c = 0; c < columns.length; c++) {
            if (maxRetainedBytesFor(shredder, sources, c, from, 1) == ColumnChunkBuffer.EXCEEDS_STORES) {
                long leaves = shredder.leafRange(c, from, 1);
                return columns[c].recordTooLarge(schema.getColumn(c), sources[c],
                        (int) (leaves >>> Integer.SIZE), (int) leaves, shredder.phantomLayers(c));
            }
        }
        throw new IllegalStateException("No column's stores reject record " + from);
    }

    /// The number of records buffered so far.
    public int rowCount() {
        return rowCount;
    }

    /// The most any of a column chunk's `int`-indexed stores may hold: entries in a level store,
    /// values or offsets in a value store, indices in the index store, bytes in a binary column's
    /// packed content, and bytes in a dictionary page body. Below the JVM's array-length ceiling.
    ///
    /// A structural cap on a row group, as [#MAX_ROWS] is: [#append] cuts the row group before
    /// the next record could take any chunk's store past it, whatever the byte target says, so a
    /// target larger than a chunk can hold yields more row groups rather than a failed write.
    public static final int MAX_STORE_CAPACITY = Integer.MAX_VALUE - 8;

    /// Records a row group may hold, whatever the configured targets say; see [#maxRows(int)].
    public static final int MAX_ROWS = maxRows(MAX_STORE_CAPACITY);

    /// Records a row group may hold under a store capacity of `storeCapacity`.
    ///
    /// A chunk holds its entries one below the capacity, a value store keeping one offset past
    /// its last value, and every record costs every column at least one entry. A flat column
    /// costs exactly one, so this is where its entry cap falls: the row ceiling is reached with
    /// it, and the entry cap cuts a row group before the row ceiling only where a column repeats.
    private static int maxRows(int storeCapacity) {
        return storeCapacity - 1;
    }

    /// The most this buffer has held for one row group so far, across every row group it has
    /// buffered.
    public long peakRetainedBytes() {
        return peakRetainedBytes;
    }

    /// The bytes this row group retains, summed across its column chunks. [#append] cuts the row
    /// group once this reaches the configured target.
    ///
    /// The sum is over columns rather than over values, so it costs the same whether the group
    /// holds a thousand records or a million, and it is read once per appended slice.
    public long retainedBytes() {
        long bytes = 0;
        for (ColumnChunkBuffer column : columns) {
            bytes += column.retainedBytes();
        }
        return bytes;
    }

    /// Whether no rows have been buffered.
    public boolean isEmpty() {
        return rowCount == 0;
    }

    /// Starts the next row group, keeping the buffers this one grew. A row group's worth of
    /// retained values is the writer's largest allocation, so rebuilding these per group would
    /// make the write path's garbage scale with the number of row groups rather than with one.
    public void reset() {
        for (ColumnChunkBuffer column : columns) {
            column.reset();
        }
        rowCount = 0;
    }

    /// Writes the buffered column chunks to `out` in schema order and returns the row
    /// group's metadata.
    public RowGroup flushTo(OutputFile out) throws IOException {
        List<ColumnChunk> chunks = new ArrayList<>(columns.length);
        long totalByteSize = 0;
        for (int c = 0; c < columns.length; c++) {
            long chunkStartOffset = out.position();
            ColumnMetaData meta = columns[c].flushTo(out, schema.getColumn(c), chunkStartOffset);
            chunks.add(new ColumnChunk(meta, null, null, null, null, ""));
            totalByteSize += meta.totalUncompressedSize();
        }
        return new RowGroup(chunks, totalByteSize, rowCount);
    }
}
