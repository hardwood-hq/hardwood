/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.pipeline.BatchCell;

/// Keeps a read's batches in plan order and publishes them: it records where each column's
/// placement opens its next cell, publishes each column's completed batches into its exchange in
/// row order, holds the ranges a batch's pages were decoded from until every column has published
/// it, and ends each column with the read's failure ([ReadFailure]) once settled, at the first batch
/// whose rows reach the failure's row. Used under the read's lock.
final class BatchPublisher {

    private final RangeFetcher fetcher;
    private final List<BatchExchange<?>> exchanges;
    private final int batchRows;
    /// the rows a decode may start past the first row some column has not published
    private final long capRows;
    /// every batch added, in row order; `null` once every column has published it
    private final List<Batch> batches = new ArrayList<>();
    /// per column, the next batch it publishes, and the next its placement opens a cell in
    private final int[] published;
    private final int[] opened;
    /// the columns that may publish since they last tried
    private final BitSet publishable = new BitSet();
    /// the first batch some column has not published
    private int head;
    private final ReadFailure readFailure;

    BatchPublisher(RangeFetcher fetcher, ReadFailure readFailure, List<BatchExchange<?>> exchanges, int batchRows,
               long capRows) {
        this.fetcher = fetcher;
        this.readFailure = readFailure;
        this.exchanges = exchanges;
        this.batchRows = batchRows;
        this.capRows = capRows;
        this.published = new int[exchanges.size()];
        this.opened = new int[exchanges.size()];
    }

    void add(Batch batch) {
        batches.add(batch);
    }

    /// The last batch added, which a row group the plan continues it in extends.
    Batch last() {
        return batches.getLast();
    }

    /// The column's placement opened `cell` at its next batch it is read in.
    void open(int column, BatchCell<?> cell) {
        Batch b = batches.get(opened[column]++);
        while (b == null || b.skips(column)) {
            b = batches.get(opened[column]++);
        }
        if (b.row() != cell.firstRow()) {
            throw new IllegalStateException("Column " + column + " opens a batch at row " + cell.firstRow()
                    + ", the plan at " + b.row());
        }
        b.open(column, cell);
    }

    /// The column is not read in the last batch added: a filter-only column where statistics
    /// proved every row matches, of which the consumers take no batch.
    void skipLast(int column) {
        Batch b = batches.getLast();
        b.completed(column);
        publishable.set(column);
    }

    /// The batch holding the column's `cell`: one of the last it opened.
    Batch of(int column, BatchCell<?> cell) {
        Batch b = find(column, cell);
        if (b == null) {
            throw new IllegalStateException("Column " + column + " holds no open batch with cell at row "
                    + cell.firstRow());
        }
        return b;
    }

    /// The batch holding the column's `cell` holds `ranges` until every column has published it;
    /// they are returned at once where it has been.
    void hold(int column, BatchCell<?> cell, List<RangeFetch> ranges) {
        Batch b = find(column, cell);
        if (b != null) {
            b.hold(ranges);
        }
        else {
            fetcher.readersDone(ranges);
        }
    }

    /// The batch holding the column's `cell`, searched back from the last the column opened;
    /// `null` where every column has published it.
    private Batch find(int column, BatchCell<?> cell) {
        for (int i = opened[column] - 1; i >= head; i--) {
            Batch b = batches.get(i);
            if (b != null && b.cell(column) == cell) {
                return b;
            }
        }
        return null;
    }

    /// The column's `cell` is complete: its batch can be published.
    void completed(int column, BatchCell<?> cell) {
        of(column, cell).completed(column);
        publishable.set(column);
    }

    /// The consumer of `column` made room in its exchange, or every column may publish.
    void publishable(int column) {
        publishable.set(column);
    }

    void allPublishable() {
        publishable.set(0, published.length);
    }

    /// Publishes each column's completed batches in order as far as its exchange takes them,
    /// then its failure once it reaches it, or its end once `allBatched` and every batch is
    /// published. `batchedTo` is the key below which every batch has been added.
    void publish(long batchedTo, boolean allBatched) {
        for (int c = publishable.nextSetBit(0); c >= 0; c = publishable.nextSetBit(c + 1)) {
            publishable.clear(c);
            if (!exchanges.get(c).isFinished()) {
                publish(c, batchedTo, allBatched);
            }
        }
    }

    private void publish(int c, long batchedTo, boolean allBatched) {
        BatchExchange<Object> exchange = cast(exchanges.get(c));
        for (int i = published[c]; ; i = ++published[c]) {
            if (i == batches.size() ? batchedTo >= readFailure.row() : batches.get(i).endKey() > readFailure.row()) {
                if (!readFailure.settled()) {
                    return;
                }
                exchange.signalError(readFailure.cause());
                return;
            }
            if (i == batches.size()) {
                if (allBatched) {
                    exchange.finish();
                }
                return;
            }
            Batch b = batches.get(i);
            if (b.skips(c)) {
                passed(c, i);
                continue;
            }
            if (!b.complete(c) || !exchange.tryPublish(b.cell(c).batch())) {
                return;
            }
            b.cell(c).release();
            passed(c, i);
        }
    }

    /// `column` is done with batch `i`; the last column returns the ranges it holds and drops it.
    private void passed(int column, int i) {
        Batch b = batches.get(i);
        if (b.passed(column)) {
            fetcher.readersDone(b.held());
            batches.set(i, null);
            while (head < batches.size() && batches.get(head) == null) {
                head++;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static BatchExchange<Object> cast(BatchExchange<?> exchange) {
        return (BatchExchange<Object>) exchange;
    }

    /// Where the consumer stands: its need, the work [JobScheduler] guarantees progress to, ends
    /// with the next batch of the column furthest ahead, which where none is added starts at
    /// `batchedTo`; where that batch is the last the plan added, the units not yet batched may
    /// go on with it, so the need reaches the next of them. Decodes reach the cap past the first
    /// row some column has not published. Nothing past the failure starts.
    JobScheduler.Position position(long batchedTo) {
        int furthest = furthest();
        long need = batchedTo + batchRows;
        if (furthest >= 0 && furthest < batches.size()) {
            Batch next = batches.get(furthest);
            need = Math.max(next.endKey(), next.key() + batchRows);
            if (furthest == batches.size() - 1) {
                need = Math.max(need, batchedTo + 1);
            }
        }
        return new JobScheduler.Position(need, firstUnpublished(batchedTo) + capRows, readFailure.row());
    }

    /// The next batch of the column furthest ahead, `-1` where every column has ended.
    private int furthest() {
        int furthest = -1;
        for (int c = 0; c < published.length; c++) {
            if (!exchanges.get(c).isFinished()) {
                furthest = Math.max(furthest, published[c]);
            }
        }
        return furthest;
    }

    /// The key of the first row some column has not published.
    long firstUnpublished(long batchedTo) {
        return head < batches.size() ? batches.get(head).key() : batchedTo;
    }

    /// Ends every exchange not ended, as the read closes: a consumer waiting for a batch finds the
    /// end.
    void stop() {
        for (BatchExchange<?> exchange : exchanges) {
            if (!exchange.isFinished()) {
                exchange.finish();
            }
        }
    }
}
