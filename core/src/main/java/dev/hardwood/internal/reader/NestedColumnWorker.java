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
import dev.hardwood.schema.ColumnSchema;

/// Per-column pipeline that decodes pages in parallel and assembles nested batches.
///
/// Extends [ColumnWorker] with nested-specific assembly: tracking record boundaries
/// via repetition levels, growing value arrays, and copying definition/repetition
/// levels into [NestedBatch] holders.
public class NestedColumnWorker extends ColumnWorker<NestedBatch> {

    private final int maxRepetitionLevel;
    private final int[] levelNullThresholds;

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
    /// @param levelNullThresholds per-level definition level thresholds (null if maxRepLevel == 0)
    public NestedColumnWorker(PageSource pageSource, BatchExchange<NestedBatch> exchange,
                              ColumnSchema column, int batchCapacity,
                              DecompressorFactory decompressorFactory,
                              Executor decodeExecutor, long maxRows,
                              int[] levelNullThresholds) {
        super(pageSource, exchange, column, batchCapacity, decompressorFactory,
              decodeExecutor, maxRows);
        this.maxRepetitionLevel = column.maxRepetitionLevel();
        this.levelNullThresholds = levelNullThresholds;
    }

    @Override
    void initDrainState() {
        int initialCapacity = batchCapacity * 2;
        nestedValues = BatchExchange.allocateArray(physicalType, initialCapacity);
        nestedDefLevels = new int[initialCapacity];
        nestedRepLevels = new int[initialCapacity];
        nestedRecordOffsets = new int[batchCapacity];
    }

    @Override
    void assemblePage(Page page) {
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

        for (int i = 0; i < pageSize; i++) {
            int repLevel = pageRepLevels != null ? pageRepLevels[i] : 0;

            // repLevel == 0 starts a new top-level record (except at the very first value
            // of the very first page, where we also start record 0)
            if (repLevel == 0 && (nestedValueCount > 0 || rowsInCurrentBatch > 0)) {
                // Previous record is complete — check if batch is full
                if (rowsInCurrentBatch >= batchCapacity) {
                    publishCurrentBatch();
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
                // Very first value of the stream — start record 0
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
                return; // stopped during publish
            }
            currentBatch = exchange.takeBatch();
            if (currentBatch == null) {
                return; // stopped during take
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        publishBlockNanos += System.nanoTime() - t0;
        batchesPublished++;

        rowsInCurrentBatch = 0;
        nestedValueCount = 0;
    }

    /// Computes index structures (element nulls, multi-level offsets, level nulls)
    /// on the batch before it is published.
    private void computeIndex(NestedBatch batch) {
        int[] defLevels = batch.definitionLevels;
        int valueCount = batch.valueCount;

        batch.elementNulls = NestedLevelComputer.computeElementNulls(
                defLevels, valueCount, maxDefinitionLevel);

        if (maxRepetitionLevel > 0 && batch.repetitionLevels != null && valueCount > 0) {
            batch.multiLevelOffsets = NestedLevelComputer.computeMultiLevelOffsets(
                    batch.repetitionLevels, valueCount, batch.recordCount, maxRepetitionLevel);
            batch.levelNulls = NestedLevelComputer.computeLevelNulls(
                    defLevels, batch.repetitionLevels, valueCount,
                    maxRepetitionLevel, levelNullThresholds);
        }
        else {
            batch.multiLevelOffsets = null;
            batch.levelNulls = null;
        }
    }

    // ==================== Nested Helpers ====================

    /// Copies a single value from a page into the batch values array.
    private void copyOneValue(Page page, int srcIndex, Object destValues, int destIndex) {
        switch (page) {
            case Page.IntPage p -> ((int[]) destValues)[destIndex] = p.values()[srcIndex];
            case Page.LongPage p -> ((long[]) destValues)[destIndex] = p.values()[srcIndex];
            case Page.FloatPage p -> ((float[]) destValues)[destIndex] = p.values()[srcIndex];
            case Page.DoublePage p -> ((double[]) destValues)[destIndex] = p.values()[srcIndex];
            case Page.BooleanPage p -> ((boolean[]) destValues)[destIndex] = p.values()[srcIndex];
            case Page.ByteArrayPage p -> ((byte[][]) destValues)[destIndex] = p.values()[srcIndex];
        }
    }

    /// Grows a typed values array to the new capacity.
    private Object growValues(Object values, int newCapacity) {
        return switch (values) {
            case int[] a -> Arrays.copyOf(a, newCapacity);
            case long[] a -> Arrays.copyOf(a, newCapacity);
            case float[] a -> Arrays.copyOf(a, newCapacity);
            case double[] a -> Arrays.copyOf(a, newCapacity);
            case boolean[] a -> Arrays.copyOf(a, newCapacity);
            case byte[][] a -> Arrays.copyOf(a, newCapacity);
            default -> throw new IllegalStateException("Unexpected values array type: " + values.getClass());
        };
    }

    /// Trims a typed values array to the exact size.
    private Object trimValues(Object values, int size) {
        return switch (values) {
            case int[] a -> a.length == size ? a : Arrays.copyOf(a, size);
            case long[] a -> a.length == size ? a : Arrays.copyOf(a, size);
            case float[] a -> a.length == size ? a : Arrays.copyOf(a, size);
            case double[] a -> a.length == size ? a : Arrays.copyOf(a, size);
            case boolean[] a -> a.length == size ? a : Arrays.copyOf(a, size);
            case byte[][] a -> a.length == size ? a : Arrays.copyOf(a, size);
            default -> throw new IllegalStateException("Unexpected values array type: " + values.getClass());
        };
    }
}
