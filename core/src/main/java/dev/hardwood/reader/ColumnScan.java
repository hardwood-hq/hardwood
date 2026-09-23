/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.Closeable;
import java.io.IOException;

import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.BatchExchange;
import dev.hardwood.internal.reader.BinaryBatchValues;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.internal.reader.LeafCompaction;
import dev.hardwood.internal.reader.NestedBatch;
import dev.hardwood.internal.reader.NestedColumnWorker;
import dev.hardwood.internal.reader.RecordFilterTally;
import dev.hardwood.internal.reader.RowGroupIterator;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.schema.FileSchema;

/// One column-reader read: the [ColumnCursor]s of every decoded column, advanced
/// together over one [RowGroupIterator].
///
/// The cursors are in decoded order: the payload columns the [ColumnReader] views
/// expose come first, then any predicate columns outside the projection. For a
/// filtered read, each [#advance()] asks the [SelectionEngine] for the matching
/// records of the aligned batches and compacts every payload cursor's batch down
/// to them.
///
/// A monotonically increasing [#generation()] lets the views share one advance:
/// a view that has already consumed the current generation advances the scan, a
/// view that has not adopts the step a sibling advanced to.
final class ColumnScan implements Closeable {

    private final ColumnCursor[] cursors;
    private final int payloadCount;
    /// `null` for an unfiltered read.
    private final SelectionEngine engine;
    private final RowGroupIterator rowGroupIterator;
    /// Per-file record-filter counts for JFR. Every batch a filtered read produces
    /// has passed through the selection, so the counts are complete for the read.
    private final RecordFilterTally tally = new RecordFilterTally();

    private long generation;
    private boolean hasBatch;
    private int recordCount;
    private boolean closed;

    private ColumnScan(ColumnCursor[] cursors, int payloadCount, SelectionEngine engine,
                       RowGroupIterator rowGroupIterator) {
        this.cursors = cursors;
        this.payloadCount = payloadCount;
        this.engine = engine;
        this.rowGroupIterator = rowGroupIterator;
    }

    /// A scan over every column of `projected`. With a `filter`, the columns past
    /// [ProjectedSchema#exposedColumnCount] carry the predicate and are decoded to
    /// evaluate it; the payload columns are compacted to the matching records.
    static ColumnScan open(HardwoodContextImpl context,
                           boolean fixedListFastPathEnabled,
                           RowGroupIterator rowGroupIterator,
                           FileSchema schema,
                           ProjectedSchema projected,
                           ResolvedPredicate filter,
                           int batchSize) {
        NestedColumnWorker.IndexMode indexMode = filter == null
                ? NestedColumnWorker.IndexMode.REAL_VIEW
                : NestedColumnWorker.IndexMode.REAL_VIEW_KEEP_LEVELS;
        int columnCount = projected.getProjectedColumnCount();
        ColumnCursor[] cursors = new ColumnCursor[columnCount];
        for (int i = 0; i < columnCount; i++) {
            cursors[i] = ColumnCursor.create(schema.getColumn(projected.toOriginalIndex(i)), schema,
                    rowGroupIterator, context, fixedListFastPathEnabled, i, batchSize, indexMode);
        }
        SelectionEngine engine = filter == null
                ? null
                : SelectionEngine.create(schema, projected, filter, cursors, batchSize);
        return new ColumnScan(cursors, projected.exposedColumnCount(), engine, rowGroupIterator);
    }

    /// A scan for a read in which pruning dropped every row group: it has no
    /// cursors, starts no worker, and its first [#advance()] returns `false`.
    static ColumnScan empty(RowGroupIterator rowGroupIterator) {
        return new ColumnScan(new ColumnCursor[0], 0, null, rowGroupIterator);
    }

    /// The cursor of payload column `index`.
    ColumnCursor cursor(int index) {
        return cursors[index];
    }

    /// Fails once the scan is closed. A view checks this before taking up a step as well as
    /// before advancing, so no reader of a closed group yields another batch.
    ///
    /// @throws IllegalStateException if the scan is closed
    void requireOpen() {
        if (closed) {
            throw new IllegalStateException(
                    "The column read is closed: closing any reader of a group closes every reader of it");
        }
    }

    long generation() {
        return generation;
    }

    boolean hasBatch() {
        return hasBatch;
    }

    int recordCount() {
        return recordCount;
    }

    /// Advances every cursor once, checks that they agree, and, for a filtered
    /// read, compacts the payload cursors to the matching records. Increments the
    /// generation when a batch is produced.
    ///
    /// @return `false` once the input is exhausted
    /// @throws IllegalStateException if the scan is closed, or the cursors did not advance
    ///         in lockstep
    boolean advance() throws IOException {
        requireOpen();
        hasBatch = false;
        recordCount = 0;
        if (cursors.length == 0) {
            return false;
        }
        if (!cursors[0].advance()) {
            // Drain the remaining cursors so the shared iterator finalizes cleanly.
            for (int i = 1; i < cursors.length; i++) {
                cursors[i].advance();
            }
            return false;
        }
        int decodedCount = cursors[0].recordCount();
        for (int i = 1; i < cursors.length; i++) {
            checkLockstep(cursors[i], decodedCount);
        }
        recordCount = engine == null ? decodedCount : select(decodedCount);
        hasBatch = true;
        generation++;
        return true;
    }

    private void checkLockstep(ColumnCursor cursor, int expectedCount) throws IOException {
        if (!cursor.advance()) {
            throw new IllegalStateException(
                    "ColumnReader '" + cursor.column().name()
                            + "' exhausted before peer column '"
                            + cursors[0].column().name()
                            + "' — readers from the same projection must advance in lockstep");
        }
        int count = cursor.recordCount();
        if (count != expectedCount) {
            throw new IllegalStateException(
                    "ColumnReader batch sizes diverged: column '"
                            + cursors[0].column().name() + "' has " + expectedCount
                            + " records, column '" + cursor.column().name()
                            + "' has " + count);
        }
    }

    /// Computes the selection from the decoded (pre-compaction) batches, then
    /// compacts the payload cursors. Order matters: a payload column that is also
    /// a predicate column must be read before it is compacted. `kept` is the
    /// engine's reusable buffer; every payload cursor consumes it here, before the
    /// next advance overwrites it.
    ///
    /// @return the number of matching records
    private int select(int decodedCount) {
        int matchCount = engine.computeSelection(decodedCount);
        int[] kept = engine.selection();
        if (matchCount >= 0) {
            for (int i = 0; i < payloadCount; i++) {
                compact(cursors[i], kept, matchCount);
            }
        }
        int selected = matchCount < 0 ? decodedCount : matchCount;
        tally.switchFile(cursors[0].fileName());
        tally.recordBatch(decodedCount, selected);
        return selected;
    }

    /// Compacts `cursor`'s current batch down to the `count` records whose
    /// ascending indices occupy `kept[0..count)`.
    private static void compact(ColumnCursor cursor, int[] kept, int count) {
        if (cursor.isNested()) {
            cursor.replaceNestedBatch(compactNestedBatch(cursor.nestedBatch(), kept, count));
        }
        else {
            compactFlatBatchInPlace(cursor.flatBatch(), kept, count);
        }
    }

    /// Compacts a flat batch to the kept records. The fixed-width value array is
    /// gathered **in place** — `kept` is strictly ascending with `kept[j] >= j`,
    /// so `values[j] = values[kept[j]]` never overwrites a slot still to be
    /// read. The batch is detached (consumer-owned, never recycled) and not yet
    /// observed by a view, so mutating it is safe and avoids a fresh per-batch
    /// `values[]` allocation on the common path. Variable-length
    /// (`BinaryBatchValues`) leaves can't gather in place and get a compacted
    /// copy; validity is rebuilt only when a kept record is null.
    private static void compactFlatBatchInPlace(BatchExchange.Batch batch, int[] kept, int count) {
        Object values = batch.values;
        if (values instanceof BinaryBatchValues binary) {
            batch.values = LeafCompaction.compactBinary(binary, kept, count);
        }
        else {
            compactPrimitiveInPlace(values, kept, count);
        }
        batch.validity = compactValidity(batch.validity, kept, count);
        batch.recordCount = count;
    }

    private static void compactPrimitiveInPlace(Object values, int[] kept, int count) {
        switch (values) {
            case int[] a -> { for (int j = 0; j < count; j++) a[j] = a[kept[j]]; }
            case long[] a -> { for (int j = 0; j < count; j++) a[j] = a[kept[j]]; }
            case float[] a -> { for (int j = 0; j < count; j++) a[j] = a[kept[j]]; }
            case double[] a -> { for (int j = 0; j < count; j++) a[j] = a[kept[j]]; }
            case boolean[] a -> { for (int j = 0; j < count; j++) a[j] = a[kept[j]]; }
            default -> throw new IllegalStateException("Unexpected leaf array type: " + values.getClass());
        }
    }

    /// Gathers the present/null bits at the kept record positions into a fresh
    /// bitmap. Mirrors the set-bit-=-present polarity of
    /// [BatchExchange.Batch#validity]; returns `null` when no kept record is
    /// null (the sparse "all present" representation).
    private static long[] compactValidity(long[] src, int[] kept, int count) {
        if (src == null) {
            return null;
        }
        long[] out = null;
        for (int j = 0; j < count; j++) {
            int idx = kept[j];
            boolean present = (src[idx >>> 6] & (1L << idx)) != 0L;
            if (present) {
                if (out != null) {
                    out[j >>> 6] |= 1L << j;
                }
            }
            else if (out == null) {
                // First null encountered: materialise the bitmap and mark every
                // earlier kept record present.
                out = new long[(count + 63) >>> 6];
                for (int b = 0; b < j; b++) {
                    out[b >>> 6] |= 1L << b;
                }
            }
        }
        return out;
    }

    /// Compacts a nested batch to the selected top-level records by slicing the
    /// raw `(definitionLevels, repetitionLevels, values)` triplet per record —
    /// each record is the contiguous level run `[recordOffsets[r], end)`. The
    /// compacted batch carries no real-items view; [ColumnReader] rebuilds it
    /// lazily from the sliced raw arrays, so no layer bookkeeping is rebuilt here.
    private static NestedBatch compactNestedBatch(NestedBatch src, int[] kept, int count) {
        int[] recordOffsets = src.recordOffsets;
        int srcRecordCount = src.recordCount;
        int srcValueCount = src.valueCount;

        // Total kept leaf slots, to size the gather index up front.
        int total = 0;
        for (int j = 0; j < count; j++) {
            int r = kept[j];
            int start = recordOffsets[r];
            int end = (r + 1 < srcRecordCount) ? recordOffsets[r + 1] : srcValueCount;
            total += end - start;
        }

        int[] keptLeaves = new int[total];
        int[] newRecordOffsets = new int[count];
        int pos = 0;
        for (int j = 0; j < count; j++) {
            int r = kept[j];
            newRecordOffsets[j] = pos;
            int start = recordOffsets[r];
            int end = (r + 1 < srcRecordCount) ? recordOffsets[r + 1] : srcValueCount;
            for (int k = start; k < end; k++) {
                keptLeaves[pos++] = k;
            }
        }

        NestedBatch out = new NestedBatch();
        out.fileName = src.fileName;
        out.fixedListK = src.fixedListK;
        out.recordCount = count;
        out.valueCount = total;
        out.values = LeafCompaction.compact(src.values, keptLeaves);
        // A fixed-width batch carries no level arrays; selection keeps whole
        // k-element records, so it stays fixed-width and the real view is rebuilt
        // arithmetically from the new record count.
        out.definitionLevels = src.definitionLevels != null ? gather(src.definitionLevels, keptLeaves) : null;
        out.repetitionLevels = src.repetitionLevels != null ? gather(src.repetitionLevels, keptLeaves) : null;
        out.recordOffsets = newRecordOffsets;
        return out;
    }

    private static int[] gather(int[] src, int[] indices) {
        int[] out = new int[indices.length];
        for (int i = 0; i < indices.length; i++) {
            out[i] = src[indices[i]];
        }
        return out;
    }

    /// Stops every cursor's worker, then releases the iterator, even when a
    /// cursor's teardown fails. Idempotent.
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        tally.close();
        // Released even when a cursor's teardown fails: this scan will not run its
        // close body again, so skipping the release would leave the work list
        // reachable for the parent reader's whole lifetime.
        try (rowGroupIterator) {
            for (ColumnCursor cursor : cursors) {
                cursor.close();
            }
        }
    }
}
