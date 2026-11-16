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
import dev.hardwood.internal.reader.plan.BatchPlan;

/// Keeps a read's batches in plan order and publishes them: it records where each column's
/// placement opens its next cell, publishes each column's completed batches into its exchange in
/// row order, holds the ranges a batch's pages were decoded from until every column has published
/// it, and ends each column with the read's failure ([ReadFailure]) once settled, at the first batch
/// whose rows reach the failure's row. Used under the read's lock.
///
/// A batch is made as the read reaches it, not as its unit is batched: by a placement opening a
/// cell in it, by a column that reads its unit publishing up to it, or by the consumer's position.
/// Each unit's batches are placed by arithmetic on its segments, so none needs the batches before
/// it made, and the batches a read holds follow the rows its pages deliver, at most one batch
/// ahead of them, rather than the rows a footer declares, which no page has confirmed yet. A
/// column that skips a unit passes its batches without making them.
final class BatchPublisher {

    /// A batched unit, and the batches made of those it starts.
    private static final class Span {
        private final BatchPlan.RowGroupBatches batches;
        private final long base;
        private final BitSet skipped;
        /// the index of the first batch it starts
        private final long firstIndex;
        /// the segment its first batch starts at: `1` where its first segment continues the batch
        /// the unit before left open, which that segment extends
        private final int firstSegment;
        /// the batches it starts made so far, in order from its first; `null` once every column
        /// has published it
        private final List<Batch> made = new ArrayList<>();
        /// the batches of `made` every column has published, from its first
        private int dropped;
        /// whether its first segment has extended the batch it continues
        private boolean continued;

        Span(BatchPlan.RowGroupBatches batches, long base, BitSet skipped, long firstIndex) {
            this.batches = batches;
            this.base = base;
            this.skipped = skipped;
            this.firstIndex = firstIndex;
            this.firstSegment = batches.continuedRows() != 0 ? 1 : 0;
        }

        /// The number of batches it starts.
        int count() {
            return batches.segmentCount() - firstSegment;
        }

        /// The index past its last batch.
        long end() {
            return firstIndex + count();
        }

        boolean continues() {
            return firstSegment == 1;
        }

        boolean madeAll() {
            return made.size() == count();
        }

        /// The key its segment `segment` starts at.
        long key(int segment) {
            return base + batches.rowsBefore(segment);
        }

        /// Makes its next batch, of `columns` columns.
        Batch makeNext(int columns, BitSet publishable) {
            int segment = firstSegment + made.size();
            long key = key(segment);
            Batch b = new Batch(key, batches.readRowsBefore() + key - base, columns);
            take(b, segment, publishable);
            made.add(b);
            return b;
        }

        /// Takes its segment `segment` into `b`: extends it to the segment's end and marks the
        /// columns that skip it.
        void take(Batch b, int segment, BitSet publishable) {
            b.extendTo(key(segment) + batches.segmentRows(segment));
            for (int c = skipped.nextSetBit(0); c >= 0; c = skipped.nextSetBit(c + 1)) {
                b.completed(c);
                publishable.set(c);
            }
        }

        /// Whether every batch it starts is made and published by every column, and it has
        /// extended the batch it continues.
        boolean done() {
            return madeAll() && dropped == made.size() && (!continues() || continued);
        }
    }

    private final RangeFetcher fetcher;
    private final List<BatchExchange<?>> exchanges;
    private final int batchRows;
    /// the rows a decode may start past the first row some column has not published
    private final long capRows;
    private final int columns;
    /// the batched units, in unit order, from the first not done at index `firstSpan`
    private final List<Span> spans = new ArrayList<>();
    private int firstSpan;
    /// the index past the last batch of every unit batched
    private long planned;
    /// per column, the next batch it publishes, and the next its placement opens a cell in
    private final long[] published;
    private final long[] opened;
    /// the columns that may publish since they last tried
    private final BitSet publishable = new BitSet();
    /// the first batch some column has not published
    private long head;
    private final ReadFailure readFailure;

    BatchPublisher(RangeFetcher fetcher, ReadFailure readFailure, List<BatchExchange<?>> exchanges, int batchRows,
               long capRows) {
        this.fetcher = fetcher;
        this.readFailure = readFailure;
        this.exchanges = exchanges;
        this.batchRows = batchRows;
        this.capRows = capRows;
        this.columns = exchanges.size();
        this.published = new long[columns];
        this.opened = new long[columns];
    }

    /// A unit is batched: its batches are `batches`, its first row at key `base`, and the
    /// columns in `skipped` are not read in it. A first segment that continues the batch the
    /// unit before left open extends it as soon as that batch is made.
    void add(BatchPlan.RowGroupBatches batches, long base, BitSet skipped) {
        if (batches.segmentCount() == 0) {
            return;
        }
        Span span = new Span(batches, base, skipped, planned);
        if (span.continues()) {
            Span owner = continuedSpan(spans.size());
            if (owner.madeAll()) {
                continueBatch(owner, span);
            }
        }
        spans.add(span);
        planned = span.end();
    }

    /// The span whose last batch the span at position `p` continues: the last before it that
    /// starts a batch.
    private Span continuedSpan(int p) {
        for (int q = p - 1; q >= firstSpan; q--) {
            if (spans.get(q).count() > 0) {
                return spans.get(q);
            }
        }
        throw new IllegalStateException("A row group continues a batch that is not open");
    }

    /// Extends `owner`'s last batch by `span`'s first segment.
    private void continueBatch(Span owner, Span span) {
        Batch last = owner.made.getLast();
        if (last == null) {
            throw new IllegalStateException("A row group continues a batch every column has published");
        }
        span.take(last, 0, publishable);
        span.continued = true;
    }

    /// The position in `spans` of the span holding batch `i`, which a unit batched holds.
    private int spanOf(long i) {
        // the last span that starts at or before i; spans that start no batch share their index
        // with the span after them
        int low = firstSpan;
        int high = spans.size() - 1;
        while (low < high) {
            int mid = (low + high + 1) >>> 1;
            if (spans.get(mid).firstIndex <= i) {
                low = mid;
            }
            else {
                high = mid - 1;
            }
        }
        return low;
    }

    /// The batch at index `i`, making the batches of its unit up to it; `null` where every
    /// column has published it.
    private Batch batch(long i) {
        int p = spanOf(i);
        Span span = spans.get(p);
        int k = Math.toIntExact(i - span.firstIndex);
        while (span.made.size() <= k) {
            span.makeNext(columns, publishable);
            if (span.madeAll()) {
                continueLast(p);
            }
        }
        return span.made.get(k);
    }

    /// The span at position `p` has made its last batch: the units after it that continue that
    /// batch extend it, in order.
    private void continueLast(int p) {
        Span owner = spans.get(p);
        for (int q = p + 1; q < spans.size() && spans.get(q).continues(); q++) {
            continueBatch(owner, spans.get(q));
            if (spans.get(q).count() > 0) {
                return;
            }
        }
    }

    /// Whether batch `i` is made.
    private boolean made(long i) {
        Span span = spans.get(spanOf(i));
        return i - span.firstIndex < span.made.size();
    }

    /// Whether `column` is not read in the unit of batch `i`, while some column is.
    private boolean skipsUnitOf(long i, int column) {
        BitSet skipped = spans.get(spanOf(i)).skipped;
        return skipped.get(column) && skipped.cardinality() < columns;
    }

    /// The column's placement opened `cell` at its next batch it is read in. The units it skips
    /// are passed over without making their batches, as are the batches every column has
    /// published, which a column that skipped them may not have passed in its placement.
    void open(int column, BatchCell<?> cell) {
        long i = Math.max(opened[column], head);
        while (i < planned) {
            Span span = spans.get(spanOf(i));
            if (!span.skipped.get(column)) {
                break;
            }
            i = span.end();
        }
        Batch b = i < planned ? batch(i) : null;
        if (b == null) {
            throw new IllegalStateException("Column " + column + " opens a batch at row " + cell.firstRow()
                    + " past the batches planned");
        }
        if (b.row() != cell.firstRow()) {
            throw new IllegalStateException("Column " + column + " opens a batch at row " + cell.firstRow()
                    + ", the plan at " + b.row());
        }
        opened[column] = i + 1;
        cell.batchIndex(i);
        b.open(column, cell);
    }

    /// The batch holding the column's `cell`.
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

    /// The batch holding the column's `cell`, at the index the cell was opened at; `null` where
    /// the column has published it.
    private Batch find(int column, BatchCell<?> cell) {
        long i = cell.batchIndex();
        if (i < head || !made(i)) {
            return null;
        }
        Span span = spans.get(spanOf(i));
        Batch b = span.made.get(Math.toIntExact(i - span.firstIndex));
        return b != null && b.cell(column) == cell ? b : null;
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
        for (long i = published[c]; ; i = ++published[c]) {
            if (i < planned && !made(i) && skipsUnitOf(i, c) && !readFailure.settled()) {
                // the columns that read the unit make its batches
                return;
            }
            if (i == planned ? batchedTo >= readFailure.row() : batch(i).endKey() > readFailure.row()) {
                if (!readFailure.settled()) {
                    return;
                }
                exchange.signalError(readFailure.cause());
                return;
            }
            if (i == planned) {
                if (allBatched) {
                    exchange.finish();
                }
                return;
            }
            Batch b = batch(i);
            if (b.skips(c)) {
                passed(c, i, b);
                continue;
            }
            if (!b.complete(c) || !exchange.tryPublish(b.cell(c).columnBatch())) {
                return;
            }
            b.cell(c).release();
            passed(c, i, b);
        }
    }

    /// `column` is done with batch `i`, `b`; the last column returns the ranges it holds and
    /// drops it.
    private void passed(int column, long i, Batch b) {
        if (!b.passed(column)) {
            return;
        }
        fetcher.readersDone(b.held());
        Span span = spans.get(spanOf(i));
        span.made.set(Math.toIntExact(i - span.firstIndex), null);
        advanceHead();
    }

    /// Moves the head past the batches every column has published, and drops the units done.
    private void advanceHead() {
        while (head < planned && made(head)) {
            Span span = spans.get(spanOf(head));
            if (span.made.get(Math.toIntExact(head - span.firstIndex)) != null) {
                break;
            }
            span.dropped++;
            head++;
        }
        while (firstSpan < spans.size() - 1 && spans.get(firstSpan).done() && spans.get(firstSpan).end() <= head) {
            firstSpan++;
        }
        // the units done leave the front once they are half the list
        if (firstSpan > spans.size() / 2) {
            spans.subList(0, firstSpan).clear();
            firstSpan = 0;
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
        long furthest = furthest();
        long need = batchedTo + batchRows;
        if (furthest >= 0 && furthest < planned) {
            // a batch not made is reached by no column yet: it holds at most a batch's rows
            need = keyOf(furthest) + batchRows;
            if (made(furthest)) {
                Batch next = spans.get(spanOf(furthest)).made.get(indexInSpan(furthest));
                if (next != null) {
                    need = Math.max(next.endKey(), need);
                }
            }
            if (furthest == planned - 1) {
                need = Math.max(need, batchedTo + 1);
            }
        }
        return new JobScheduler.Position(need, firstUnpublished(batchedTo) + capRows, readFailure.row());
    }

    /// The next batch of the column furthest ahead, `-1` where every column has ended.
    private long furthest() {
        long furthest = -1;
        for (int c = 0; c < published.length; c++) {
            if (!exchanges.get(c).isFinished()) {
                furthest = Math.max(furthest, published[c]);
            }
        }
        return furthest;
    }

    /// The key of the first row some column has not published.
    long firstUnpublished(long batchedTo) {
        return head == planned ? batchedTo : keyOf(head);
    }

    /// The key batch `i` starts at, made or not.
    private long keyOf(long i) {
        Span span = spans.get(spanOf(i));
        return span.key(span.firstSegment + indexInSpan(i));
    }

    /// The position of batch `i` among the batches its unit starts.
    private int indexInSpan(long i) {
        return Math.toIntExact(i - spans.get(spanOf(i)).firstIndex);
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
