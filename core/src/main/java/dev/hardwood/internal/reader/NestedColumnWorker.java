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
    public NestedColumnWorker(PageSource pageSource, BatchExchange<NestedBatch> exchange,
                              ColumnSchema column, int batchCapacity,
                              DecompressorFactory decompressorFactory,
                              Executor decodeExecutor, long maxRows,
                              NestedLevelComputer.Layers layers) {
        super(pageSource, exchange, column, batchCapacity, decompressorFactory,
              decodeExecutor, maxRows);
        this.maxRepetitionLevel = column.maxRepetitionLevel();
        this.layers = layers;
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
            if (nestedValueCount >= nestedDefLevels.length) {
                int newCapacity = nestedDefLevels.length * 2;
                nestedValues = growValues(nestedValues, newCapacity);
                nestedDefLevels = Arrays.copyOf(nestedDefLevels, newCapacity);
                nestedRepLevels = Arrays.copyOf(nestedRepLevels, newCapacity);
            }

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
        currentBatch.definitionLevels = Arrays.copyOf(nestedDefLevels, nestedValueCount);
        currentBatch.repetitionLevels = Arrays.copyOf(nestedRepLevels, nestedValueCount);
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
            }
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
        bbv.offsets = newOffsets;
        return bbv;
    }

    /// Trims a typed values array to the exact size. For
    /// [BinaryBatchValues] this trims the offsets array to `size + 1` and
    /// publishes the (possibly capacity-sized) bytes buffer as-is — only
    /// the prefix `[0, offsets[size])` is meaningful per the
    /// capacity-sized contract.
    private Object trimValues(Object values, int size) {
        return switch (values) {
            case int[] a -> a.length == size ? a : Arrays.copyOf(a, size);
            case long[] a -> a.length == size ? a : Arrays.copyOf(a, size);
            case float[] a -> a.length == size ? a : Arrays.copyOf(a, size);
            case double[] a -> a.length == size ? a : Arrays.copyOf(a, size);
            case boolean[] a -> a.length == size ? a : Arrays.copyOf(a, size);
            case BinaryBatchValues bbv -> {
                if (bbv.offsets.length == size + 1) {
                    yield bbv;
                }
                int[] trimmed = Arrays.copyOf(bbv.offsets, size + 1);
                yield new BinaryBatchValues(bbv.bytes, trimmed);
            }
            default -> throw new IllegalStateException("Unexpected values array type: " + values.getClass());
        };
    }
}
