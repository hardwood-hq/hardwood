/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.IOException;
import java.io.InterruptedIOException;

import dev.hardwood.internal.reader.BatchExchange;
import dev.hardwood.internal.reader.ColumnWorker;
import dev.hardwood.internal.reader.FlatColumnWorker;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.internal.reader.NestedBatch;
import dev.hardwood.internal.reader.NestedColumnWorker;
import dev.hardwood.internal.reader.NestedLevelComputer;
import dev.hardwood.internal.reader.PageSource;
import dev.hardwood.internal.reader.RowGroupIterator;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// The decode pipeline of one column in a [ColumnScan]: the column's worker, the
/// [BatchExchange] it publishes into, and the batch most recently taken from it.
///
/// A cursor carries no accessor or cache state; the [ColumnReader] views read its
/// current batch. Exactly one of [#flatBatch()] and [#nestedBatch()] is in use,
/// fixed by [#isNested()].
final class ColumnCursor {

    private final ColumnSchema column;
    private final boolean nested;
    private final BatchExchange<BatchExchange.Batch> flatExchange;
    private final BatchExchange<NestedBatch> nestedExchange;
    private final ColumnWorker<?> worker;

    private BatchExchange.Batch flatBatch;
    private NestedBatch nestedBatch;
    private boolean exhausted;
    private boolean closed;

    private ColumnCursor(ColumnSchema column, BatchExchange<BatchExchange.Batch> flatExchange,
                         BatchExchange<NestedBatch> nestedExchange, ColumnWorker<?> worker) {
        this.column = column;
        this.nested = nestedExchange != null;
        this.flatExchange = flatExchange;
        this.nestedExchange = nestedExchange;
        this.worker = worker;
    }

    /// Whether a column with `layers` decodes through the nested pipeline: any
    /// column whose schema chain contributes a `STRUCT` or `REPEATED` layer, or
    /// that repeats, needs its definition levels to derive per-layer validity.
    /// Only the no-layers case takes the [FlatColumnWorker] fast path.
    static boolean isNested(NestedLevelComputer.Layers layers, ColumnSchema column) {
        return layers.count() > 0 || column.maxRepetitionLevel() > 0;
    }

    /// Starts the worker for `column`, which sits at `projectedColumnIndex` in the
    /// projection `rowGroupIterator` was initialised with. Batches are published
    /// detached, so every batch the cursor hands out is fresh and never reused.
    static ColumnCursor create(ColumnSchema column, FileSchema schema,
                               RowGroupIterator rowGroupIterator,
                               HardwoodContextImpl context,
                               boolean fixedListFastPathEnabled,
                               int projectedColumnIndex,
                               int batchSize,
                               NestedColumnWorker.IndexMode indexMode) {
        NestedLevelComputer.Layers layers = NestedLevelComputer.computeLayers(
                schema.getRootNode(), column.columnIndex());
        PageSource pageSource = new PageSource(rowGroupIterator, projectedColumnIndex);

        if (isNested(layers, column)) {
            BatchExchange<NestedBatch> exchange = BatchExchange.detaching(
                    column.name(), () -> {
                        NestedBatch b = new NestedBatch();
                        b.values = BatchExchange.allocateArray(column, batchSize);
                        return b;
                    });
            NestedColumnWorker worker = new NestedColumnWorker(
                    pageSource, exchange, column, batchSize,
                    context.decompressorFactory(), context.executor(), 0,
                    layers, indexMode, fixedListFastPathEnabled);
            worker.start();
            return new ColumnCursor(column, null, exchange, worker);
        }
        BatchExchange<BatchExchange.Batch> exchange = BatchExchange.detaching(
                column.name(), () -> {
                    BatchExchange.Batch b = new BatchExchange.Batch();
                    b.values = BatchExchange.allocateArray(column, batchSize);
                    return b;
                });
        FlatColumnWorker worker = new FlatColumnWorker(
                pageSource, exchange, column, batchSize,
                context.decompressorFactory(), context.executor(), 0, null);
        worker.start();
        return new ColumnCursor(column, exchange, null, worker);
    }

    /// Takes the next batch from the exchange.
    ///
    /// @return `false` at the end of the stream, and on every call after it
    /// @throws IOException if the pipeline failed to read or decode the column, or
    ///         [InterruptedIOException] if the thread was interrupted while waiting
    boolean advance() throws IOException {
        if (exhausted) {
            return false;
        }
        if (nested) {
            nestedBatch = poll(nestedExchange);
            if (nestedBatch == null || nestedBatch.recordCount == 0) {
                nestedExchange.checkError();
                return end();
            }
            return true;
        }
        flatBatch = poll(flatExchange);
        if (flatBatch == null || flatBatch.recordCount == 0) {
            flatExchange.checkError();
            return end();
        }
        return true;
    }

    private boolean end() {
        exhausted = true;
        flatBatch = null;
        nestedBatch = null;
        return false;
    }

    private <B> B poll(BatchExchange<B> exchange) throws IOException {
        try {
            return exchange.poll();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            InterruptedIOException interrupted = new InterruptedIOException(
                    "Interrupted while waiting for the next batch of column '" + column.name() + "'");
            interrupted.initCause(e);
            throw interrupted;
        }
    }

    ColumnSchema column() {
        return column;
    }

    boolean isNested() {
        return nested;
    }

    /// The current flat batch; `null` for a nested column or when no batch is current.
    BatchExchange.Batch flatBatch() {
        return flatBatch;
    }

    /// The current nested batch; `null` for a flat column or when no batch is current.
    NestedBatch nestedBatch() {
        return nestedBatch;
    }

    /// Replaces the current nested batch with a compacted copy of it.
    void replaceNestedBatch(NestedBatch batch) {
        this.nestedBatch = batch;
    }

    int recordCount() {
        return nested ? nestedBatch.recordCount : flatBatch.recordCount;
    }

    String fileName() {
        return nested ? nestedBatch.fileName : flatBatch.fileName;
    }

    /// Stops the worker. Idempotent.
    void close() {
        if (closed) {
            return;
        }
        closed = true;
        worker.close();
    }
}
