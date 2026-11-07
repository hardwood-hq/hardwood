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

import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.batch.NestedBatch;
import dev.hardwood.internal.reader.batch.NestedLevelComputer;
import dev.hardwood.internal.reader.pipeline.DecodedColumn;
import dev.hardwood.internal.reader.pipeline.FlatColumn;
import dev.hardwood.internal.reader.pipeline.NestedColumn;
import dev.hardwood.schema.ColumnSchema;

/// One column in a [ColumnScan]: the [BatchExchange] its column of the read publishes into, and the
/// batch most recently taken from it. The read's close stops the column.
///
/// A cursor carries no accessor or cache state; the [ColumnReader] views read its
/// current batch. Exactly one of [#flatBatch()] and [#nestedBatch()] is in use,
/// fixed by [#isNested()].
final class ColumnCursor {

    private final ColumnSchema column;
    private final boolean nested;
    private final BatchExchange<BatchExchange.Batch> flatExchange;
    private final BatchExchange<NestedBatch> nestedExchange;

    private BatchExchange.Batch flatBatch;
    private NestedBatch nestedBatch;
    private boolean exhausted;

    private ColumnCursor(ColumnSchema column, BatchExchange<BatchExchange.Batch> flatExchange,
                         BatchExchange<NestedBatch> nestedExchange) {
        this.column = column;
        this.nested = nestedExchange != null;
        this.flatExchange = flatExchange;
        this.nestedExchange = nestedExchange;
    }

    /// Whether a column with `layers` decodes through the nested pipeline: any
    /// column whose schema chain contributes a `STRUCT` or `REPEATED` layer, or
    /// that repeats, needs its definition levels to derive per-layer validity.
    /// Only the no-layers case is read as a flat column ([FlatColumn]).
    static boolean isNested(NestedLevelComputer.Layers layers, ColumnSchema column) {
        return layers.count() > 0 || column.maxRepetitionLevel() > 0;
    }

    /// The cursor of a column the read drives, over its exchange.
    static ColumnCursor of(ColumnSchema column, DecodedColumn side) {
        return switch (side) {
            case FlatColumn flat -> new ColumnCursor(column, flat.exchange(), null);
            case NestedColumn nested -> new ColumnCursor(column, null, nested.exchange());
        };
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

    /// Whether statistics proved every record of the current (pre-selection) batch
    /// matches the filter.
    boolean filterAlwaysMatches() {
        return nested ? nestedBatch.filterAlwaysMatches : flatBatch.filterAlwaysMatches;
    }

    String fileName() {
        return nested ? nestedBatch.fileName : flatBatch.fileName;
    }
}
