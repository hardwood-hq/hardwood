/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.Arrays;
import java.util.concurrent.Executor;

import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.LayerKind;
import dev.hardwood.schema.ColumnSchema;

/// Per-column pipeline that decodes pages in parallel and assembles nested batches.
///
/// Extends [ColumnWorker] with nested-specific assembly: tracking record boundaries
/// via repetition levels, growing value arrays, and copying definition/repetition
/// levels into [NestedBatch] holders.
public class NestedColumnWorker extends ColumnWorker<NestedBatch> {

    private final int maxRepetitionLevel;
    private final NestedLevelComputer.Layers layers;

    // --- Nested assembly state (drain thread only) ---
    private Object nestedValues;
    private int[] nestedDefLevels;
    private int[] nestedRepLevels;
    private int[] nestedRecordOffsets;
    private int nestedValueCount;

    /// Whether we have seen the first value in the nested stream.
    private boolean nestedFirstValueSeen;

    /// Elements per row of the batch currently being assembled when it is on the
    /// fixed-size-list fast path, or `0` for a regular batch. A batch stays
    /// homogeneous: [#assemblePage] flushes the open batch before switching
    /// between the fast and regular paths (or between different `k`).
    private int batchFixedK;

    /// Creates a new nested column worker.
    ///
    /// @param pageSource yields [PageInfo] objects for this column
    /// @param exchange the output exchange for assembled batches
    /// @param column the column schema
    /// @param batchCapacity rows per batch
    /// @param decompressorFactory for creating page decompressors
    /// @param decodeExecutor executor for decode tasks
    /// @param maxRows maximum rows to assemble (0 = unlimited)
    /// @param layers per-layer descriptor (kinds + def thresholds) for the
    ///        column's schema chain, or `null` for non-repeated columns. The
    ///        descriptor drives the layer-indexed `multiLevelOffsets` shape.
    /// Convenience constructor with the fixed-size-list fast path enabled.
    public NestedColumnWorker(PageSource pageSource, BatchExchange<NestedBatch> exchange,
                              ColumnSchema column, int batchCapacity,
                              DecompressorFactory decompressorFactory,
                              Executor decodeExecutor, long maxRows,
                              NestedLevelComputer.Layers layers) {
        this(pageSource, exchange, column, batchCapacity, decompressorFactory,
             decodeExecutor, maxRows, layers, true);
    }

    public NestedColumnWorker(PageSource pageSource, BatchExchange<NestedBatch> exchange,
                              ColumnSchema column, int batchCapacity,
                              DecompressorFactory decompressorFactory,
                              Executor decodeExecutor, long maxRows,
                              NestedLevelComputer.Layers layers,
                              boolean fixedListFastPathEnabled) {
        super(pageSource, exchange, column, batchCapacity, decompressorFactory,
              decodeExecutor, maxRows);
        this.maxRepetitionLevel = column.maxRepetitionLevel();
        this.layers = layers;
        this.fixedListFastPathEnabled = fixedListFastPathEnabled;
    }

    @Override
    void initDrainState() {
        int initialCapacity = batchCapacity * 2;
        nestedValues = BatchExchange.allocateArray(column, initialCapacity);
        nestedDefLevels = new int[initialCapacity];
        nestedRepLevels = new int[initialCapacity];
        nestedRecordOffsets = new int[batchCapacity];
    }

    @Override
    void assemblePage(Page page, PageRowMask mask) {
        int k = page.fixedListK();
        boolean pageClean = k > 0;
        // A published batch must stay homogeneous — wholly clean (levels omitted)
        // or wholly regular — but batches must also cut at the same row
        // boundaries across all columns, which is only true at batch-capacity and
        // file boundaries. We therefore never flush at a page boundary: when the
        // open clean batch meets a regular page or a different k, we convert it to
        // the regular representation in place (synthesizing its levels) and keep
        // filling the same batch. A clean page arriving into an already-regular
        // batch is assembled with synthesized levels too.
        if (batchFixedK > 0 && (!pageClean || k != batchFixedK)) {
            materializeCleanBatchLevels();
        }
        if (pageClean && (rowsInCurrentBatch == 0 || batchFixedK == k)) {
            assembleCleanPage(page, mask, k);
        }
        else if (pageClean) {
            assembleCleanPageAsRegular(page, mask, k);
        }
        else {
            assembleRegularPage(page, mask);
        }
    }

    /// Converts the open clean fast-path batch to the regular representation by
    /// synthesizing the levels it omitted: every element is present (definition
    /// level `maxDefinitionLevel`) and each record of `k` elements starts a new
    /// list (repetition level `0` at the record start, `1` for the following
    /// `k - 1` elements). The clean path lays records out as contiguous `k`-value
    /// runs from index `0`, so `v % k == 0` marks a record start. Called when a
    /// batch that began clean must absorb a regular or differently-shaped page
    /// rather than being flushed early (which would misalign sibling columns).
    private void materializeCleanBatchLevels() {
        int k = batchFixedK;
        for (int v = 0; v < nestedValueCount; v++) {
            nestedDefLevels[v] = maxDefinitionLevel;
            nestedRepLevels[v] = (v % k == 0) ? 0 : 1;
        }
        batchFixedK = 0;
    }

    /// Assembles a clean fixed-`k` page into an already-regular batch: values are
    /// bulk-copied per record as on the fast path, but the synthesized levels are
    /// written so the batch stays wholly regular. Mirrors [#assembleCleanPage]'s
    /// masking and batch/row splitting.
    private void assembleCleanPageAsRegular(Page page, PageRowMask mask, int k) {
        nestedFirstValueSeen = true;
        int pageRecords = page.size() / k;
        boolean maskAll = mask.isAll();
        int intervalCount = maskAll ? 0 : mask.intervalCount();
        int intervalCursor = 0;

        for (int r = 0; r < pageRecords; r++) {
            if (!maskAll) {
                while (intervalCursor < intervalCount && r >= mask.end(intervalCursor)) {
                    intervalCursor++;
                }
                if (intervalCursor >= intervalCount || r < mask.start(intervalCursor)) {
                    continue;
                }
            }

            if (rowsInCurrentBatch >= batchCapacity) {
                publishCurrentBatch();
                if (done) {
                    return;
                }
            }
            if (maxRows > 0 && totalRowsAssembled >= maxRows) {
                if (rowsInCurrentBatch > 0) {
                    publishCurrentBatch();
                }
                finishDrain();
                return;
            }

            ensureNestedCapacity(nestedValueCount + k);
            if (nestedRecordOffsets.length <= rowsInCurrentBatch) {
                nestedRecordOffsets = Arrays.copyOf(nestedRecordOffsets,
                        nestedRecordOffsets.length * 2);
            }
            nestedRecordOffsets[rowsInCurrentBatch] = nestedValueCount;
            int destStart = nestedValueCount;
            copyValueRun(page, r * k, destStart, k);
            for (int j = 0; j < k; j++) {
                nestedDefLevels[destStart + j] = maxDefinitionLevel;
                nestedRepLevels[destStart + j] = (j == 0) ? 0 : 1;
            }
            nestedValueCount += k;
            rowsInCurrentBatch++;
            totalRowsAssembled++;
        }
    }

    /// Fast-path assembly for a clean fixed-size-list page: every record is a
    /// present list of exactly `k` elements, so record boundaries are implicit
    /// and the level arrays are skipped. Values are bulk-copied per record and
    /// record offsets set arithmetically; masking and batch/row splitting mirror
    /// [#assembleRegularPage] at record granularity.
    private void assembleCleanPage(Page page, PageRowMask mask, int k) {
        nestedFirstValueSeen = true;
        int pageRecords = page.size() / k;
        boolean maskAll = mask.isAll();
        int intervalCount = maskAll ? 0 : mask.intervalCount();
        int intervalCursor = 0;

        for (int r = 0; r < pageRecords; r++) {
            if (!maskAll) {
                while (intervalCursor < intervalCount && r >= mask.end(intervalCursor)) {
                    intervalCursor++;
                }
                if (intervalCursor >= intervalCount || r < mask.start(intervalCursor)) {
                    continue;
                }
            }

            if (rowsInCurrentBatch >= batchCapacity) {
                publishCurrentBatch();
                if (done) {
                    return;
                }
            }
            if (maxRows > 0 && totalRowsAssembled >= maxRows) {
                if (rowsInCurrentBatch > 0) {
                    publishCurrentBatch();
                }
                finishDrain();
                return;
            }

            ensureNestedCapacity(nestedValueCount + k);
            copyValueRun(page, r * k, nestedValueCount, k);
            nestedRecordOffsets[rowsInCurrentBatch] = nestedValueCount;
            nestedValueCount += k;
            rowsInCurrentBatch++;
            totalRowsAssembled++;
            batchFixedK = k;
        }
    }

    private void assembleRegularPage(Page page, PageRowMask mask) {
        int pageSize = page.size();
        int[] pageDefLevels = page.definitionLevels();
        int[] pageRepLevels = page.repetitionLevels();

        // Validate: the very first repetition level in the column must be 0
        if (!nestedFirstValueSeen && pageSize > 0) {
            nestedFirstValueSeen = true;
            if (pageRepLevels != null && pageRepLevels[0] != 0) {
                throw new IllegalStateException(
                        "Invalid column chunk for '" + column.name()
                        + "': first repetition level must be 0 but was " + pageRepLevels[0]);
            }
        }

        boolean maskAll = mask.isAll();
        int intervalCount = maskAll ? 0 : mask.intervalCount();
        int intervalCursor = 0;
        int recordIndex = -1;

        for (int i = 0; i < pageSize; i++) {
            int repLevel = pageRepLevels != null ? pageRepLevels[i] : 0;
            boolean atRecordStart = repLevel == 0;

            if (atRecordStart) {
                recordIndex++;
                if (!maskAll) {
                    while (intervalCursor < intervalCount
                            && recordIndex >= mask.end(intervalCursor)) {
                        intervalCursor++;
                    }
                }
            }

            if (!maskAll && (intervalCursor >= intervalCount
                    || recordIndex < mask.start(intervalCursor))) {
                continue;
            }

            // A record start (repLevel = 0) closes the previous top-level record and
            // opens a new one — except at the first kept value of the stream, where
            // we also start record 0. Masked-out records are already filtered by the
            // `continue` above, so they never reach this branch.
            if (atRecordStart && (nestedValueCount > 0 || rowsInCurrentBatch > 0)) {
                // Previous record is complete — check if batch is full
                if (rowsInCurrentBatch >= batchCapacity) {
                    publishCurrentBatch();
                    if (done) {
                        return;
                    }
                }

                // Check maxRows after publishing
                if (maxRows > 0 && totalRowsAssembled >= maxRows) {
                    if (rowsInCurrentBatch > 0) {
                        publishCurrentBatch();
                    }
                    finishDrain();
                    return;
                }

                // Start new record
                if (nestedRecordOffsets.length <= rowsInCurrentBatch) {
                    nestedRecordOffsets = Arrays.copyOf(nestedRecordOffsets,
                            nestedRecordOffsets.length * 2);
                }
                nestedRecordOffsets[rowsInCurrentBatch] = nestedValueCount;
                rowsInCurrentBatch++;
                totalRowsAssembled++;
            }
            else if (nestedValueCount == 0 && rowsInCurrentBatch == 0) {
                // First kept value of the stream — start record 0. May not be the
                // first value of the first page if the mask skipped earlier records.
                nestedRecordOffsets[0] = 0;
                rowsInCurrentBatch = 1;
                totalRowsAssembled++;
            }

            // Ensure capacity for the new value
            ensureNestedCapacity(nestedValueCount + 1);

            // Copy value and levels
            nestedDefLevels[nestedValueCount] = pageDefLevels != null
                    ? pageDefLevels[i] : maxDefinitionLevel;
            nestedRepLevels[nestedValueCount] = repLevel;
            copyOneValue(page, i, nestedValues, nestedValueCount);
            nestedValueCount++;
        }
    }

    @Override
    void publishCurrentBatch() {
        if (done) {
            return;
        }
        currentBatch.recordCount = rowsInCurrentBatch;
        currentBatch.valueCount = nestedValueCount;
        currentBatch.fixedListK = batchFixedK;
        if (batchFixedK > 0) {
            // Clean fixed-k batch: boundaries are implicit, levels are omitted.
            currentBatch.definitionLevels = null;
            currentBatch.repetitionLevels = null;
        }
        else {
            currentBatch.definitionLevels = Arrays.copyOf(nestedDefLevels, nestedValueCount);
            currentBatch.repetitionLevels = Arrays.copyOf(nestedRepLevels, nestedValueCount);
        }
        currentBatch.recordOffsets = Arrays.copyOf(nestedRecordOffsets, rowsInCurrentBatch);
        currentBatch.values = trimValues(nestedValues, nestedValueCount);
        currentBatch.fileName = currentBatchFileName;

        // Compute index structures before publishing so the consumer thread
        // doesn't need to do expensive index computation.
        computeIndex(currentBatch);

        long t0 = System.nanoTime();
        try {
            if (!exchange.publish(currentBatch)) {
                done = true; // stopped during publish
                return;
            }
            currentBatch = exchange.takeBatch();
            if (currentBatch == null) {
                done = true; // stopped during take
                return;
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            done = true;
            return;
        }
        publishBlockNanos += System.nanoTime() - t0;
        batchesPublished++;

        rowsInCurrentBatch = 0;
        nestedValueCount = 0;
        batchFixedK = 0;
        // The accumulator is reused for the next batch; clear its dictionary
        // slot so dictIndex state is rebuilt from scratch (the dictIndices array
        // is retained and overwritten in place).
        if (nestedValues instanceof BinaryBatchValues bbv) {
            bbv.dictionary = null;
        }
    }

    /// Computes index structures (element validity and layer-indexed
    /// multi-level offsets) on the batch before it is published. Set bits
    /// indicate **present** values; `null` validity references mean every
    /// item at that scope is present in this batch.
    ///
    /// `multiLevelOffsets` is indexed by **layer** (one slot per `STRUCT` /
    /// `REPEATED` layer). `multiLevelOffsets[k]` is `null` for `STRUCT`
    /// layers and sentinel-suffixed for `REPEATED` layers.
    private void computeIndex(NestedBatch batch) {
        if (batch.fixedListK > 0) {
            // All elements present, boundaries at multiples of k: no validity and
            // arithmetic layer offsets, skipping the O(valueCount) level scans.
            batch.elementValidity = null;
            batch.multiLevelOffsets = fixedListLayerOffsets(batch.fixedListK, batch.recordCount);
            return;
        }

        int[] defLevels = batch.definitionLevels;
        int valueCount = batch.valueCount;

        batch.elementValidity = NestedLevelComputer.computeElementValidity(
                defLevels, valueCount, maxDefinitionLevel);

        if (layers != null && layers.count() > 0 && valueCount > 0) {
            batch.multiLevelOffsets = NestedLevelComputer.computeLayerOffsets(
                    batch.repetitionLevels, valueCount, batch.recordCount, layers);
        }
        else {
            batch.multiLevelOffsets = null;
        }
    }

    /// Layer-indexed offsets for a clean fixed-k batch: the single `REPEATED`
    /// layer of the `LIST` shape gets the arithmetic sentinel-suffixed offsets
    /// `[0, k, 2k, ..., recordCount * k]`; other layers (none for this shape)
    /// stay `null`. Mirrors [NestedLevelComputer#computeLayerOffsets] for the
    /// clean case.
    private int[][] fixedListLayerOffsets(int k, int recordCount) {
        if (layers == null || layers.count() == 0) {
            return null;
        }
        int layerCount = layers.count();
        int[][] result = new int[layerCount][];
        for (int i = 0; i < layerCount; i++) {
            if (layers.kinds()[i] == LayerKind.REPEATED) {
                result[i] = NestedLevelComputer.fixedListOffsets(k, recordCount);
            }
        }
        return result;
    }

    // ==================== Nested Helpers ====================

    private static final byte[] EMPTY_BYTES = new byte[0];

    /// Copies a single value from a page into the batch values array.
    /// For byte-array physical types, the value is appended into the
    /// shared bytes buffer of [BinaryBatchValues] and its offsets are
    /// updated; null entries collapse to a zero-length span (or, for
    /// `FIXED_LEN_BYTE_ARRAY`, leave the pre-filled trivial offsets
    /// unchanged).
    private void copyOneValue(Page page, int srcIndex, Object destValues, int destIndex) {
        switch (page) {
            case Page.IntPage p -> ((int[]) destValues)[destIndex] = p.values()[srcIndex];
            case Page.LongPage p -> ((long[]) destValues)[destIndex] = p.values()[srcIndex];
            case Page.FloatPage p -> ((float[]) destValues)[destIndex] = p.values()[srcIndex];
            case Page.DoublePage p -> ((double[]) destValues)[destIndex] = p.values()[srcIndex];
            case Page.BooleanPage p -> ((boolean[]) destValues)[destIndex] = p.values()[srcIndex];
            case Page.ByteArrayPage p -> {
                byte[] val = p.values()[srcIndex];
                BinaryBatchValues bbv = (BinaryBatchValues) destValues;
                if (val != null) {
                    bbv.appendAt(destIndex, val, 0, val.length);
                }
                else if (physicalType != PhysicalType.FIXED_LEN_BYTE_ARRAY) {
                    // Variable-length null: zero-length span at this index.
                    bbv.appendAt(destIndex, EMPTY_BYTES, 0, 0);
                }
                // FIXED_LEN null: trivial offsets stay; bytes content is undefined.
                // Record the per-value dictionary index so stringAt can intern; a
                // no-op for non-string columns, and plain/null values fall back
                // to the packed-byte path (see BinaryBatchValues#recordDictIndex).
                bbv.recordDictIndex(p.dictIndices(), p.dictionary(), srcIndex, destIndex);
            }
        }
    }

    /// Ensures the value and level accumulators hold at least `needed` slots.
    /// The three arrays are grown together so `nestedDefLevels.length` remains
    /// the shared capacity marker for both the regular and fast paths, even
    /// though the fast path leaves the level arrays unwritten.
    private void ensureNestedCapacity(int needed) {
        if (needed <= nestedDefLevels.length) {
            return;
        }
        int newCapacity = nestedDefLevels.length * 2;
        if (newCapacity < needed) {
            newCapacity = needed;
        }
        nestedValues = growValues(nestedValues, newCapacity);
        nestedDefLevels = Arrays.copyOf(nestedDefLevels, newCapacity);
        nestedRepLevels = Arrays.copyOf(nestedRepLevels, newCapacity);
    }

    /// Copies `count` consecutive values from `page` starting at `srcStart` into
    /// the value accumulator at `destStart`. Primitive pages bulk-copy; byte
    /// arrays fall back to the per-element append.
    private void copyValueRun(Page page, int srcStart, int destStart, int count) {
        switch (page) {
            case Page.IntPage p -> System.arraycopy(p.values(), srcStart, (int[]) nestedValues, destStart, count);
            case Page.LongPage p -> System.arraycopy(p.values(), srcStart, (long[]) nestedValues, destStart, count);
            case Page.FloatPage p -> System.arraycopy(p.values(), srcStart, (float[]) nestedValues, destStart, count);
            case Page.DoublePage p -> System.arraycopy(p.values(), srcStart, (double[]) nestedValues, destStart, count);
            case Page.BooleanPage p -> System.arraycopy(p.values(), srcStart, (boolean[]) nestedValues, destStart, count);
            // The fast path is gated to primitive numeric element types
            // (PageDecoder#isFixedListElementSupported), so a byte-array page
            // never reaches clean assembly.
            case Page.ByteArrayPage p -> throw new IllegalStateException(
                    "byte-array element unexpected on the fixed-size-list fast path");
        }
    }

    /// Grows a typed values array to the new capacity. For
    /// [BinaryBatchValues], the offsets array is grown to `newCapacity + 1`
    /// (re-filling the trivial `i * width` mapping for
    /// `FIXED_LEN_BYTE_ARRAY`); the bytes buffer for variable-length types
    /// grows lazily on overflow inside [BinaryBatchValues#appendAt], but for
    /// fixed-width it grows here to keep `width * newCapacity`.
    private Object growValues(Object values, int newCapacity) {
        return switch (values) {
            case int[] a -> Arrays.copyOf(a, newCapacity);
            case long[] a -> Arrays.copyOf(a, newCapacity);
            case float[] a -> Arrays.copyOf(a, newCapacity);
            case double[] a -> Arrays.copyOf(a, newCapacity);
            case boolean[] a -> Arrays.copyOf(a, newCapacity);
            case BinaryBatchValues bbv -> growBinaryBatchValues(bbv, newCapacity);
            default -> throw new IllegalStateException("Unexpected values array type: " + values.getClass());
        };
    }

    private BinaryBatchValues growBinaryBatchValues(BinaryBatchValues bbv, int newCapacity) {
        int oldCapacity = bbv.offsets.length - 1;
        if (newCapacity <= oldCapacity) {
            return bbv;
        }
        int[] newOffsets = Arrays.copyOf(bbv.offsets, newCapacity + 1);
        if (physicalType == PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            int width = column.typeLength();
            for (int i = oldCapacity + 1; i <= newCapacity; i++) {
                newOffsets[i] = Math.multiplyExact(i, width);
            }
            byte[] newBytes = new byte[Math.multiplyExact(width, newCapacity)];
            System.arraycopy(bbv.bytes, 0, newBytes, 0, bbv.bytes.length);
            bbv.bytes = newBytes;
        }
        // Keep dictIndices sized to the value capacity once the accumulator has
        // switched onto the dictionary representation.
        if (bbv.dictIndices != null) {
            bbv.dictIndices = Arrays.copyOf(bbv.dictIndices, newCapacity);
        }
        bbv.offsets = newOffsets;
        return bbv;
    }

    /// Snapshots the assembled values for `[0, size)` into an independent array
    /// the published batch owns outright.
    ///
    /// The drain reuses its accumulators ([#nestedValues] and friends) for the
    /// next batch — [#publishCurrentBatch] only resets the counts, not the
    /// backing storage — so the published batch must not alias them. For
    /// [BinaryBatchValues] that means copying the meaningful **bytes** prefix as
    /// well as the offsets: sharing the bytes buffer would let the next batch's
    /// `appendAt` (which restarts at byte offset 0) overwrite the still-unread
    /// rows of the batch just published.
    private Object trimValues(Object values, int size) {
        return switch (values) {
            case int[] a -> Arrays.copyOf(a, size);
            case long[] a -> Arrays.copyOf(a, size);
            case float[] a -> Arrays.copyOf(a, size);
            case double[] a -> Arrays.copyOf(a, size);
            case boolean[] a -> Arrays.copyOf(a, size);
            case BinaryBatchValues bbv -> {
                int byteLength = bbv.offsets[size];
                BinaryBatchValues trimmed = new BinaryBatchValues(
                        Arrays.copyOf(bbv.bytes, byteLength),
                        Arrays.copyOf(bbv.offsets, size + 1));
                if (bbv.dictionary != null) {
                    trimmed.dictionary = bbv.dictionary;
                    trimmed.dictIndices = Arrays.copyOf(bbv.dictIndices, size);
                }
                yield trimmed;
            }
            default -> throw new IllegalStateException("Unexpected values array type: " + values.getClass());
        };
    }
}
