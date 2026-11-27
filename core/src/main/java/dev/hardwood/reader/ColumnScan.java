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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.predicate.ColumnBatchMatcher;
import dev.hardwood.internal.predicate.CompiledBatchFilter;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.batch.BinaryBatchValues;
import dev.hardwood.internal.reader.batch.FlatBatch;
import dev.hardwood.internal.reader.batch.LeafCompaction;
import dev.hardwood.internal.reader.batch.NestedBatch;
import dev.hardwood.internal.reader.batch.NestedIndexMode;
import dev.hardwood.internal.reader.batch.NestedLevelComputer;
import dev.hardwood.internal.reader.engine.PreparedRead;
import dev.hardwood.internal.reader.engine.ReadEngine;
import dev.hardwood.internal.reader.row.CloseClaim;
import dev.hardwood.internal.reader.row.RecordFilterTally;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.internal.schema.ReadProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// One column-reader read: the [ColumnCursor]s of every decoded column, advanced
/// together over one [ReadEngine].
///
/// The cursors are in decoded order: the payload columns the [ColumnReader] views
/// expose come first, then any predicate columns outside the projection. For a
/// filtered read, each [#advance()] asks the [SelectionEngine] for the matching
/// records of the aligned batches and compacts every payload cursor's batch down
/// to them. Where the filter compiles to batch matchers, each predicate column runs
/// its matcher on the job that completes its batch, as the flat row reader's
/// columns do, and the engine only merges their bitmaps.
///
/// A scan also trims the records it returns: it drops the rows before its physical
/// skip that the read publishes ([ReadEngine#leadingRows()]), then, after the
/// filter, the first `offset` records and every record past `limit`, so that a
/// column read takes `skip` and `head` as a row read does.
///
/// One party advances a scan: its [ColumnReaders] group, or the single
/// [ColumnReader] that is its only view. The views take up each step after it.
final class ColumnScan implements Closeable {

    private final ColumnCursor[] cursors;
    private final int payloadCount;
    /// `null` for an unfiltered read.
    private final SelectionEngine engine;
    /// The read the cursors' columns run on, whose close stops them.
    private final ReadEngine read;
    /// The leading decoded records still to drop, `-1` until the first batch, when the read
    /// knows how many rows before its skip it published.
    private long leadingLeft = -1;
    /// The records after the filter still to drop, and still to return.
    private long offsetLeft;
    private long limitLeft;
    /// The indices of a trimmed step where every decoded record is selected, reused across steps.
    private int[] run = new int[0];
    /// Per-file record-filter counts for JFR. Every batch the scan takes of a filtered read
    /// passes through the selection, so the counts cover those batches: all of the read's, but
    /// where a limit ends the scan before the read's end.
    private final RecordFilterTally tally = new RecordFilterTally();

    private boolean hasBatch;
    private int recordCount;
    /// Claimed by the thread that closes the scan; a closer on another thread, such as the
    /// parent's, waits until that close has completed.
    private final CloseClaim closeClaim = new CloseClaim();

    private ColumnScan(ColumnCursor[] cursors, int payloadCount, SelectionEngine engine,
                       ReadEngine read, long offset, long limit) {
        this.cursors = cursors;
        this.payloadCount = payloadCount;
        this.engine = engine;
        this.read = read;
        this.offsetLeft = offset;
        this.limitLeft = limit;
    }

    /// The engine's predicate view rejects what the reader cannot assemble (a shredded
    /// Variant `typed_value` of a type the shredding spec does not allow). The read's
    /// columns have started by then and no scan will own them, so a failure closes the read
    /// before it propagates, naming the file whose schema it was found in.
    private static SelectionEngine createSelectionEngine(FileSchema schema, ProjectedSchema decoded,
                                                         ResolvedPredicate filter, CompiledBatchFilter compiled,
                                                         ColumnCursor[] cursors, int batchSize,
                                                         PreparedRead read, ReadEngine engine) {
        try {
            return SelectionEngine.create(schema, decoded, filter, compiled, cursors, batchSize);
        }
        catch (RuntimeException e) {
            RuntimeException placed = ExceptionContext.addFileContext(read.referenceFileName(), e);
            engine.closeAfter(placed);
            throw placed;
        }
    }

    /// A scan over every decoded column of `projection`, driven by the engine `read` starts. With a `filter`, the
    /// filter-only columns past the payload carry the predicate and are decoded to evaluate it; the
    /// payload columns are compacted to the matching records. `layers` holds the layers of every
    /// column of `schema`, indexed by column index.
    ///
    /// @param offset the records after the filter the scan drops before the first it returns
    /// @param limit the records after the filter the scan returns at most, `Long.MAX_VALUE` for no
    ///        limit
    /// @throws IOException if the first file cannot be read as its row groups are listed
    static ColumnScan open(PreparedRead read, boolean fixedListFastPathEnabled, FileSchema schema,
                                 NestedLevelComputer.Layers[] layers, ReadProjection projection,
                                 ResolvedPredicate filter, int batchSize, long budgetRows, long offset, long limit)
            throws IOException {
        // a scan that compacts its batches keeps the raw levels a nested batch is compacted on: one
        // with a filter; one whose unfiltered read publishes rows before its skip, as many as the
        // prepared read counts; and one with an offset or a limit
        boolean compacts = filter != null || read.leadingRows() > 0 || offset > 0 || limit != Long.MAX_VALUE;
        NestedIndexMode indexMode = compacts
                ? NestedIndexMode.REAL_VIEW_KEEP_LEVELS
                : NestedIndexMode.REAL_VIEW;
        ProjectedSchema decoded = projection.decoded();
        List<NestedLevelComputer.Layers> nestedLayers = new ArrayList<>();
        for (int i = 0; i < decoded.getProjectedColumnCount(); i++) {
            ColumnSchema column = schema.getColumn(decoded.toOriginalIndex(i));
            NestedLevelComputer.Layers columnLayers = layers[column.columnIndex()];
            nestedLayers.add(ColumnCursor.isNested(columnLayers, column) ? columnLayers : null);
        }
        CompiledBatchFilter compiled = filter == null ? null : SelectionEngine.compile(schema, decoded, filter);
        ReadEngine engine = compiled == null
                ? read.start(decoded, nestedLayers, indexMode, fixedListFastPathEnabled, batchSize, budgetRows)
                : read.start(decoded, nestedLayers, indexMode, fixedListFastPathEnabled, batchSize, budgetRows,
                        exchanges(schema, decoded, nestedLayers, compiled, batchSize), compiled.columnMatchers());
        List<BatchExchange<?>> exchanges = engine.exchanges();
        ColumnCursor[] cursors = new ColumnCursor[exchanges.size()];
        for (int i = 0; i < cursors.length; i++) {
            cursors[i] = ColumnCursor.of(schema.getColumn(decoded.toOriginalIndex(i)), exchanges.get(i),
                    nestedLayers.get(i) != null);
        }
        SelectionEngine selection = filter == null
                ? null
                : createSelectionEngine(schema, decoded, filter, compiled, cursors, batchSize, read, engine);
        return new ColumnScan(cursors, projection.payloadColumnCount(), selection, engine, offset, limit);
    }

    /// The detaching exchange of each decoded column, in decoded order, for a read whose filter
    /// compiled to batch matchers: a column with a matcher gets batches carrying a `matches`
    /// array, which its matcher fills as each batch's cell completes.
    ///
    /// @throws IllegalStateException if a matcher is compiled for a column decoded through
    ///         levels, whose batches carry no `matches` array
    private static List<BatchExchange<?>> exchanges(FileSchema schema, ProjectedSchema decoded,
                                                    List<NestedLevelComputer.Layers> nestedLayers,
                                                    CompiledBatchFilter compiled, int batchSize) {
        ColumnBatchMatcher[] matchers = compiled.columnMatchers();
        List<BatchExchange<?>> exchanges = new ArrayList<>(nestedLayers.size());
        for (int c = 0; c < nestedLayers.size(); c++) {
            String name = schema.getColumn(decoded.toOriginalIndex(c)).name();
            boolean matched = c < matchers.length && matchers[c] != null;
            if (nestedLayers.get(c) != null) {
                if (matched) {
                    throw new IllegalStateException("Column '" + name
                            + "' is decoded through levels and cannot carry a batch matcher");
                }
                exchanges.add(BatchExchange.detachingNested(name, batchSize));
            }
            else {
                exchanges.add(BatchExchange.detachingFlat(name, batchSize, matched));
            }
        }
        return exchanges;
    }

    /// The cursor of payload column `index`.
    ColumnCursor cursor(int index) {
        return cursors[index];
    }

    /// @throws IllegalStateException if the scan is closed
    private void requireOpen() {
        if (closeClaim.isClaimed()) {
            throw new IllegalStateException("The column read is closed");
        }
    }

    boolean hasBatch() {
        return hasBatch;
    }

    int recordCount() {
        return recordCount;
    }

    /// Advances every cursor to its next batch, checks that they agree, and compacts the payload
    /// cursors to the records the step returns: those the filter matches, after the scan drops
    /// the rows before its skip and those its offset and limit leave out ([#trim]). A filter-only
    /// cursor is advanced only when statistics did not prove the step's batch. A step that returns
    /// no record is passed over and the cursors advance again, so a batch the scan holds is never
    /// empty.
    ///
    /// @return `false` once the input is exhausted
    /// @throws IllegalStateException if the scan is closed, or the cursors did not advance
    ///         in lockstep
    boolean advance() throws IOException {
        requireOpen();
        hasBatch = false;
        recordCount = 0;
        while (recordCount == 0) {
            if (limitLeft == 0) {
                // the limit is reached: the read's later batches are not taken, and its close stops it
                return false;
            }
            if (cursors.length == 0 || !cursors[0].advance()) {
                drainAfterEnd();
                return false;
            }
            recordCount = step();
        }
        hasBatch = true;
        return true;
    }

    /// Takes the next decoded batch of every cursor, which the first cursor has advanced to, and
    /// returns its record count after the filter.
    private int step() throws IOException {
        int decodedCount = cursors[0].recordCount();
        for (int i = 1; i < payloadCount; i++) {
            checkLockstep(cursors[i], decodedCount);
        }
        // A filter-only column is not read in a row group statistics proved, so its cursor
        // advances only for the steps the selection evaluates. In a proven step it keeps an
        // earlier step's batch, which nothing reads: the selection answers the step from the
        // first cursor, a payload column's.
        if (!cursors[0].filterAlwaysMatches()) {
            for (int i = payloadCount; i < cursors.length; i++) {
                checkLockstep(cursors[i], decodedCount);
            }
        }
        // the selection is computed from the decoded batches before any payload cursor is
        // compacted: a payload column that is also a predicate column must be read first
        int selected = engine == null ? -1 : engine.computeSelection(decodedCount);
        return trim(decodedCount, selected);
    }

    /// Drains the remaining cursors so the shared read finalizes cleanly, checking
    /// that none of them still produces a batch.
    private void drainAfterEnd() throws IOException {
        for (int i = 1; i < cursors.length; i++) {
            if (cursors[i].advance()) {
                throw new IllegalStateException(
                        "ColumnReader '" + cursors[i].column().name()
                                + "' produced a batch after peer column '"
                                + cursors[0].column().name()
                                + "' was exhausted — readers from the same projection must advance in lockstep");
            }
        }
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

    /// Trims the step's selection to the records the scan returns, counting the filtered records
    /// from the skip on for JFR, and compacts the payload cursors to them: drops the decoded
    /// records the read published before its skip, then the
    /// first selected records the offset still covers, then every selected record past the
    /// limit. Each drops a run from the start or the end of the selection, so what remains is one
    /// run of it.
    ///
    /// @param selected the matching records, whose indices occupy the start of the engine's
    ///        selection, or `-1` when every decoded record is selected
    /// @return the number of records the step returns
    private int trim(int decodedCount, int selected) {
        if (leadingLeft < 0) {
            leadingLeft = read.leadingRows();
        }
        int[] kept = selected < 0 ? null : engine.selection();
        int count = selected < 0 ? decodedCount : selected;
        int leading = (int) Math.min(leadingLeft, decodedCount);
        leadingLeft -= leading;
        // the first selected record at or past the skip
        int from = kept == null ? leading : firstAtOrAfter(kept, count, leading);
        if (engine != null) {
            // the records before the skip lie outside the read, so the tally leaves them out
            tally.switchFile(cursors[0].fileName());
            tally.recordBatch(decodedCount - leading, count - from);
        }
        int skipped = (int) Math.min(offsetLeft, count - from);
        offsetLeft -= skipped;
        from += skipped;
        int to = from + (int) Math.min(count - from, limitLeft);
        limitLeft -= to - from;
        if (to == from) {
            // nothing of the step is returned: the scan takes the next one
            return 0;
        }
        if (from == 0 && to == decodedCount) {
            // every decoded record is returned, in order
            return decodedCount;
        }
        int[] run = runOf(kept, from, to);
        for (int i = 0; i < payloadCount; i++) {
            compact(cursors[i], run, to - from);
        }
        return to - from;
    }

    /// The position of the first of `kept[0..count)`, which ascend, that is at least `index`.
    private static int firstAtOrAfter(int[] kept, int count, int index) {
        int position = Arrays.binarySearch(kept, 0, count, index);
        return position >= 0 ? position : -position - 1;
    }

    /// The indices of the selection's run `[from, to)` at the start of an array: the engine's
    /// selection, `kept`, shifted down in place; or, where every decoded record is selected,
    /// `from..to-1` in the scan's run buffer. Every payload cursor consumes it before the next
    /// step overwrites it.
    private int[] runOf(int[] kept, int from, int to) {
        if (kept == null) {
            if (run.length < to - from) {
                run = new int[to - from];
            }
            for (int i = 0; i < to - from; i++) {
                run[i] = from + i;
            }
            return run;
        }
        if (from > 0) {
            System.arraycopy(kept, from, kept, 0, to - from);
        }
        return kept;
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
    private static void compactFlatBatchInPlace(FlatBatch batch, int[] kept, int count) {
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
    /// [FlatBatch#validity]; returns `null` when no kept record is
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

    /// Closes the read, which stops every cursor's column, even when closing the tally fails.
    /// Idempotent: a close on another thread while one runs, such as the parent's, waits until
    /// that close has completed.
    @Override
    public void close() throws IOException {
        if (!closeClaim.claim()) {
            return;
        }
        // Released even when the tally's close fails: this scan will not run its close body
        // again, so skipping the release would leave the work list reachable for the parent
        // reader's whole lifetime.
        try (read) {
            tally.close();
        }
        finally {
            closeClaim.completed();
        }
    }
}
