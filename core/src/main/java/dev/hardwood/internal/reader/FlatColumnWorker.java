/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.BitSet;
import java.util.concurrent.Executor;

import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.internal.predicate.BatchMatcher;
import dev.hardwood.schema.ColumnSchema;

/// Per-column pipeline that decodes pages in parallel and assembles flat batches.
///
/// Extends [ColumnWorker] with flat-specific assembly: arraycopy of typed values
/// and null tracking via [BitSet].
public class FlatColumnWorker extends ColumnWorker<BatchExchange.Batch> {

    private BitSet currentNulls;
    private BatchMatcher columnFilter;

    /// Creates a new flat column worker.
    ///
    /// @param pageSource yields [PageInfo] objects for this column
    /// @param exchange the output exchange for assembled batches
    /// @param column the column schema
    /// @param batchCapacity rows per batch
    /// @param decompressorFactory for creating page decompressors
    /// @param decodeExecutor executor for decode tasks
    /// @param maxRows maximum rows to assemble (0 = unlimited)
    public FlatColumnWorker(PageSource pageSource, BatchExchange<BatchExchange.Batch> exchange,
                            ColumnSchema column, int batchCapacity,
                            DecompressorFactory decompressorFactory,
                            Executor decodeExecutor, long maxRows) {
        super(pageSource, exchange, column, batchCapacity, decompressorFactory,
              decodeExecutor, maxRows);
    }

    /// Installs a drain-side per-column filter that runs against every published batch,
    /// writing matches into the batch's `matches` array. Must be set before [#start()]
    /// and only by [FlatRowReader] when the drain-side path is enabled. `null` (the
    /// default) leaves the worker on the existing path — no filter evaluation.
    public void setColumnFilter(BatchMatcher columnFilter) {
        this.columnFilter = columnFilter;
    }

    @Override
    void initDrainState() {
        currentNulls = maxDefinitionLevel > 0 ? new BitSet(batchCapacity) : null;
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
            int spaceInBatch = batchCapacity - rowsInCurrentBatch;
            int toCopy = Math.min(spaceInBatch, rangeEnd - pagePosition);

            // Respect maxRows: limit the copy to remaining budget
            if (maxRows > 0) {
                long remaining = maxRows - totalRowsAssembled;
                if (remaining <= 0) {
                    finishDrain();
                    return;
                }
                toCopy = (int) Math.min(toCopy, remaining);
            }

            copyPageData(page, pagePosition, rowsInCurrentBatch, toCopy);

            rowsInCurrentBatch += toCopy;
            totalRowsAssembled += toCopy;
            pagePosition += toCopy;

            if (rowsInCurrentBatch >= batchCapacity) {
                publishCurrentBatch();
                if (done) {
                    return;
                }
            }

            // Check if we've hit the limit after publishing
            if (maxRows > 0 && totalRowsAssembled >= maxRows) {
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
        currentBatch.nulls = (currentNulls != null && !currentNulls.isEmpty())
                ? (BitSet) currentNulls.clone()
                : null;
        currentBatch.fileName = currentBatchFileName;

        if (columnFilter != null) {
            // Drain-side filter: evaluate while the just-filled value array is hot in
            // this drain core's L1. Writes into currentBatch.matches in place.
            columnFilter.test(currentBatch, currentBatch.matches);
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
        if (currentNulls != null) {
            currentNulls.clear();
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
                System.arraycopy(p.values(), srcPos, (byte[][]) values, destPos, length);
                markNulls(p.definitionLevels(), srcPos, destPos, length);
            }
        }
    }

    private void markNulls(int[] defLevels, int srcPos, int destPos, int length) {
        if (currentNulls != null && defLevels != null) {
            for (int i = 0; i < length; i++) {
                if (defLevels[srcPos + i] < maxDefinitionLevel) {
                    currentNulls.set(destPos + i);
                }
            }
        }
    }
}
