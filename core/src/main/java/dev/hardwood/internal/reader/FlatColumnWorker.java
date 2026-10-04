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
import dev.hardwood.internal.predicate.ColumnBatchMatcher;
import dev.hardwood.schema.ColumnSchema;

/// Per-column pipeline that decodes pages in parallel and assembles flat batches.
///
/// Extends [ColumnWorker] with flat-specific assembly: arraycopy of typed values
/// and null tracking via a packed `long[]` validity bitmap (set-bit-= -present).
public class FlatColumnWorker extends ColumnWorker<BatchExchange.Batch> {

    private long[] currentValidity;
    private final ColumnBatchMatcher columnFilter;
    /// Tracks whether any absent (null) leaf has been seen in the current
    /// batch; cleared by [#publishCurrentBatch]. When still false at publish
    /// time, [BatchExchange.Batch#validity] is set to `null` to signal
    /// "all leaves present in this batch."
    private boolean currentBatchHasAbsents;

    /// Creates a new flat column worker.
    ///
    /// @param pageSource yields [PageInfo] objects for this column
    /// @param exchange the output exchange for assembled batches
    /// @param column the column schema
    /// @param batchCapacity rows per batch
    /// @param decompressorFactory for creating page decompressors
    /// @param decodeExecutor executor for decode tasks
    /// @param maxRows maximum rows to assemble (0 = unlimited)
    /// @param columnFilter optional drain-side per-column filter that runs against every
    ///                    published batch, writing matches into the batch's `matches`
    ///                    array. `null` leaves the worker on the existing path — no
    ///                    filter evaluation.
    public FlatColumnWorker(PageSource pageSource, BatchExchange<BatchExchange.Batch> exchange,
                            ColumnSchema column, int batchCapacity,
                            DecompressorFactory decompressorFactory,
                            Executor decodeExecutor, long maxRows,
                            ColumnBatchMatcher columnFilter) {
        super(pageSource, exchange, column, batchCapacity, decompressorFactory,
              decodeExecutor, maxRows);
        this.columnFilter = columnFilter;
    }

    @Override
    void initDrainState() {
        currentValidity = maxDefinitionLevel > 0 ? new long[(batchCapacity + 63) >>> 6] : null;
        currentBatchHasAbsents = false;
        currentCapacity = capacityOfTakenBatch();
    }

    /// Rows the current batch's value array holds; `0` until a batch taken without values
    /// gets them on its first rows ([#allocateValues]).
    private int currentCapacity;

    private int capacityOfTakenBatch() {
        return currentBatch != null && currentBatch.values != null ? batchCapacity : 0;
    }

    /// Gives a batch the column readers' exchange hands out without values its value array,
    /// sized to the rows it can still receive: at most the batch capacity, and, where batches
    /// end with their row group, no more than the row group has left. A file of small row
    /// groups then allocates per batch what the batch can hold.
    private void allocateValues() {
        currentCapacity = (int) Math.max(1, Math.min(batchCapacity, rowGroupRowsLeft));
        currentBatch.values = BatchExchange.allocateArray(column, currentCapacity);
    }

    /// Writes the mask a matcher would produce when every record matches: all-ones
    /// for `[0, recordCount)` bits, tail bits of the last active word zeroed, words
    /// beyond the active range untouched.
    private static void fillAllOnes(long[] words, int recordCount) {
        int fullWords = recordCount >>> 6;
        int tail = recordCount & 63;
        Arrays.fill(words, 0, fullWords, -1L);
        if (tail != 0) {
            words[fullWords] = -1L >>> (64 - tail);
        }
    }

    @Override
    void assemblePage(Page page, PageRowMask mask) {
        if (mask.isAll()) {
            copyPageRange(page, 0, page.size());
            return;
        }
        int intervalCount = mask.intervalCount();
        for (int i = 0; i < intervalCount; i++) {
            if (done) {
                return;
            }
            copyPageRange(page, mask.start(i), mask.end(i));
        }
    }

    /// Copies values at page-relative offsets `[rangeStart, rangeEnd)` into
    /// the current batch, publishing and rolling over as the batch fills and
    /// stopping early when `maxRows` is reached.
    private void copyPageRange(Page page, int rangeStart, int rangeEnd) {
        int pagePosition = rangeStart;

        while (pagePosition < rangeEnd) {
            if (currentBatch.values == null) {
                allocateValues();
            }
            int spaceInBatch = currentCapacity - rowsInCurrentBatch;
            int toCopy = Math.min(spaceInBatch, rangeEnd - pagePosition);

            // Respect the active row cap: limit the copy to the remaining budget
            if (activeMaxRows > 0) {
                long remaining = activeMaxRows - totalRowsAssembled;
                if (remaining <= 0) {
                    finishDrain();
                    return;
                }
                toCopy = (int) Math.min(toCopy, remaining);
            }

            copyPageData(page, pagePosition, rowsInCurrentBatch, toCopy);

            rowsInCurrentBatch += toCopy;
            totalRowsAssembled += toCopy;
            rowGroupRowsLeft -= toCopy;
            pagePosition += toCopy;

            if (rowsInCurrentBatch >= currentCapacity) {
                publishCurrentBatch();
                if (done) {
                    return;
                }
            }

            // Check if we've hit the limit after publishing
            if (activeMaxRows > 0 && totalRowsAssembled >= activeMaxRows) {
                if (rowsInCurrentBatch > 0) {
                    publishCurrentBatch();
                }
                finishDrain();
                return;
            }
        }
    }

    @Override
    void publishCurrentBatch() {
        if (done) {
            return;
        }
        currentBatch.recordCount = rowsInCurrentBatch;
        currentBatch.validity = (currentValidity != null && currentBatchHasAbsents)
                ? Arrays.copyOf(currentValidity, (rowsInCurrentBatch + 63) >>> 6)
                : null;
        currentBatch.fileName = currentBatchFileName;
        currentBatch.filterAlwaysMatches = currentBatchFilterAlwaysMatches;

        if (columnFilter != null) {
            if (currentBatchFilterAlwaysMatches) {
                // Statistics proved every record of this batch's row group matches the
                // predicate: emit the all-ones mask directly instead of evaluating records.
                fillAllOnes(currentBatch.matches, rowsInCurrentBatch);
            }
            else {
                // Drain-side filter: evaluate while the just-filled value array is hot in
                // this drain core's L1. Writes into currentBatch.matches in place.
                columnFilter.test(currentBatch, currentBatch.matches);
            }
        }

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
        currentCapacity = capacityOfTakenBatch();
        if (currentValidity != null) {
            Arrays.fill(currentValidity, 0L);
        }
        currentBatchHasAbsents = false;
        // The freshly-taken batch may be recycled; reset its byte buffer and
        // dictionary state so the next batch rebuilds them from scratch (the
        // arrays are retained and overwritten in place).
        if (currentBatch.values instanceof BinaryBatchValues bbv) {
            bbv.reset();
        }
    }

    private void copyPageData(Page page, int srcPos, int destPos, int length) {
        Object values = currentBatch.values;
        switch (page) {
            case Page.IntPage p -> {
                System.arraycopy(p.values(), srcPos, (int[]) values, destPos, length);
                markNulls(p.definitionLevels(), srcPos, destPos, length);
            }
            case Page.LongPage p -> {
                System.arraycopy(p.values(), srcPos, (long[]) values, destPos, length);
                markNulls(p.definitionLevels(), srcPos, destPos, length);
            }
            case Page.FloatPage p -> {
                System.arraycopy(p.values(), srcPos, (float[]) values, destPos, length);
                markNulls(p.definitionLevels(), srcPos, destPos, length);
            }
            case Page.DoublePage p -> {
                System.arraycopy(p.values(), srcPos, (double[]) values, destPos, length);
                markNulls(p.definitionLevels(), srcPos, destPos, length);
            }
            case Page.BooleanPage p -> {
                System.arraycopy(p.values(), srcPos, (boolean[]) values, destPos, length);
                markNulls(p.definitionLevels(), srcPos, destPos, length);
            }
            case Page.ByteArrayPage p -> {
                BinaryBatchValues bbv = (BinaryBatchValues) values;
                bbv.appendRange(p.values(), srcPos, destPos, length);
                // Plain values carry no entry index; in a batch that already holds a
                // dictionary they fall back to the packed-byte path
                // (see BinaryBatchValues#recordDictIndices).
                bbv.recordDictIndices(null, null, srcPos, destPos, length);
                markNulls(p.definitionLevels(), srcPos, destPos, length);
            }
            case Page.DictionaryByteArrayPage p -> {
                BinaryBatchValues bbv = (BinaryBatchValues) values;
                bbv.viewDictionaryRange(p, srcPos, destPos, length);
                // Record per-value dictionary indices, for string interning and the
                // column reader's dictionary ids; a null records -1
                // (see BinaryBatchValues#recordDictIndices).
                bbv.recordDictIndices(p.dictIndices(), p.dictionary(), srcPos, destPos, length);
                markNulls(p.definitionLevels(), srcPos, destPos, length);
            }
        }
    }

    /// Records a validity bit for each value just copied. Set bit means the
    /// leaf at that position is **present** (`def == maxDefinitionLevel`);
    /// absent positions leave their bit clear.
    ///
    /// On the no-absents fast path the bitmap is left untouched — a `null`
    /// validity at publish-time is the sparse representation of "every leaf
    /// in this batch is present", so setting bits we'd then drop is wasted
    /// work. The first absent encountered switches the bitmap on by
    /// backfilling the bits for all previously-seen present values
    /// (`[0, destPos + i)`); subsequent values then maintain the bitmap
    /// normally. When `defLevels` is `null` the page has no def-level stream,
    /// which implies every leaf in the page is present — only touch the
    /// bitmap if it was already switched on by an earlier absent in the
    /// batch.
    private void markNulls(int[] defLevels, int srcPos, int destPos, int length) {
        if (currentValidity == null) {
            return;
        }
        if (defLevels == null) {
            if (currentBatchHasAbsents) {
                BitmapWords.setRange(currentValidity, destPos, destPos + length);
            }
            return;
        }
        for (int i = 0; i < length; i++) {
            if (defLevels[srcPos + i] < maxDefinitionLevel) {
                if (!currentBatchHasAbsents) {
                    currentBatchHasAbsents = true;
                    BitmapWords.setRange(currentValidity, 0, destPos + i);
                }
            }
            else if (currentBatchHasAbsents) {
                int bit = destPos + i;
                currentValidity[bit >>> 6] |= 1L << bit;
            }
        }
    }
}
