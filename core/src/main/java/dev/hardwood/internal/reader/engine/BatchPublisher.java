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
import java.util.function.LongUnaryOperator;

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
/// cell in it, or by a column that reads its unit publishing up to it; the consumer's position
/// reads the key of a batch not yet made from its unit's segments. Each unit's batches are placed
/// by arithmetic on its segments, so none needs the batches before it made, and the batches a
/// read holds follow the rows its pages deliver, at most one batch ahead of them, rather than the
/// rows a footer declares, which no page has confirmed yet. A column that skips a unit passes its
/// batches without making them until the read's failure has settled; it then makes them up to the
/// first one reaching the failure's row, which the failure bounds by the rows delivered.
final class BatchPublisher {

    /// A batched unit, and the batches made of those it starts.
    private static final class Span {
        private final BatchPlan.RowGroupBatches batches;
        private final long base;
        private final BitSet skipped;
        /// the rows the units batched before it keep, from the read's first
        private final long keptBefore;
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

        Span(BatchPlan.RowGroupBatches batches, long base, BitSet skipped, long firstIndex, long keptBefore) {
            this.keptBefore = keptBefore;
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
        Batch makeNext(int columns, ColumnQueue publishable) {
            int segment = firstSegment + made.size();
            long key = key(segment);
            Batch b = new Batch(key, batches.readRowsBefore() + key - base, columns);
            take(b, segment, publishable);
            made.add(b);
            return b;
        }

        /// Takes its segment `segment` into `b`: extends it to the segment's end and marks the
        /// columns that skip it.
        void take(Batch b, int segment, ColumnQueue publishable) {
            b.extendTo(key(segment) + batches.segmentRows(segment));
            for (int c = skipped.nextSetBit(0); c >= 0; c = skipped.nextSetBit(c + 1)) {
                b.completed(c);
                publishable.add(c);
            }
        }

        /// Whether every batch it starts is made and published by every column, and it has
        /// extended the batch it continues.
        boolean done() {
            return madeAll() && dropped == made.size() && (!continues() || continued);
        }
    }

    /// Columns, each held at most once, in the order they were added.
    private static final class ColumnQueue {
        private final int[] columns;
        private final boolean[] held;
        private int first;
        private int size;

        ColumnQueue(int columns) {
            this.columns = new int[columns];
            this.held = new boolean[columns];
        }

        void add(int column) {
            if (!held[column]) {
                held[column] = true;
                columns[(first + size++) % columns.length] = column;
            }
        }

        /// Takes the first column, `-1` where none is held.
        int poll() {
            if (size == 0) {
                return -1;
            }
            int column = columns[first];
            first = (first + 1) % columns.length;
            size--;
            held[column] = false;
            return column;
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
    /// the rows every unit batched keeps, from the read's first
    private long keptBatched;
    /// per column, the next batch it publishes, and the next its placement opens a cell in
    private final long[] published;
    private final long[] opened;
    /// the columns that may publish since they last tried
    private final ColumnQueue publishable;
    /// the first batch some column has not published
    private long head;
    /// the consumer's next batch ([#recountConsumerNext()]), and the open columns whose next batch it is
    private long consumerNext;
    private int atConsumerNext;
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
        this.publishable = new ColumnQueue(columns);
        recountConsumerNext();
    }

    /// A unit is batched: its batches are `batches`, its first row at key `base`, and the
    /// columns in `skipped` are not read in it. A first segment that continues the batch the
    /// unit before left open extends it as soon as that batch is made.
    void add(BatchPlan.RowGroupBatches batches, long base, BitSet skipped) {
        if (batches.segmentCount() == 0) {
            return;
        }
        Span span = new Span(batches, base, skipped, planned, keptBatched);
        keptBatched += batches.plannedRows();
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

    /// Batch `i`, at or past the head, where it is made; `null` where it is not or every column
    /// has published it.
    private Batch madeBatch(long i) {
        Span span = spans.get(spanOf(i));
        int k = Math.toIntExact(i - span.firstIndex);
        return k < span.made.size() ? span.made.get(k) : null;
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
        Batch b = i < head ? null : madeBatch(i);
        return b != null && b.cell(column) == cell ? b : null;
    }

    /// The column's `cell` is complete: its batch can be published.
    void completed(int column, BatchCell<?> cell) {
        of(column, cell).completed(column);
        publishable.add(column);
    }

    /// The consumer of `column` made room in its exchange, or every column may publish.
    void publishable(int column) {
        publishable.add(column);
    }

    void allPublishable() {
        for (int c = 0; c < columns; c++) {
            publishable.add(c);
        }
    }

    /// Publishes each column's completed batches in order as far as its exchange takes them,
    /// then its failure once it reaches it, or its end once `allBatched` and every batch is
    /// published. `batchedTo` is the key below which every batch has been added. Visits only the
    /// columns that may publish, those that become so as others publish included.
    void publish(long batchedTo, boolean allBatched) {
        for (int c = publishable.poll(); c >= 0; c = publishable.poll()) {
            if (!exchanges.get(c).isFinished()) {
                publish(c, batchedTo, allBatched);
            }
        }
    }

    private void publish(int c, long batchedTo, boolean allBatched) {
        BatchExchange<Object> exchange = cast(exchanges.get(c));
        for (long i = published[c]; ; i = advance(c)) {
            if (i < planned && !made(i) && skipsUnitOf(i, c) && !readFailure.settled()) {
                // the columns that read the unit make its batches
                return;
            }
            if (i == planned ? batchedTo >= readFailure.row() : batch(i).endKey() > readFailure.row()) {
                if (!readFailure.settled()) {
                    return;
                }
                exchange.signalError(readFailure.cause());
                left(i);
                return;
            }
            if (i == planned) {
                if (allBatched) {
                    exchange.finish();
                    left(i);
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

    /// Moves `c` to its next batch.
    private long advance(int c) {
        long i = ++published[c];
        left(i - 1);
        return i;
    }

    /// An open column whose next batch was `i` has moved past it or ended.
    private void left(long i) {
        if (i == consumerNext && --atConsumerNext == 0) {
            recountConsumerNext();
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

    /// Where the consumer stands: its need, the work [JobScheduler] guarantees progress to, reaches
    /// the first row past its next batch, which where none is added starts at `batchedTo`: a
    /// column with repetition closes a batch only as it places the record after it, so the decode
    /// of a page starting where the batch ends completes it. Where that batch is the last the plan
    /// added, the units not yet batched may go on with it, so the need reaches the next of them.
    /// Decodes reach the cap's kept rows past the first row some column has not published. Nothing past the
    /// failure starts.
    JobScheduler.Position position(long batchedTo, LongUnaryOperator pastBatched) {
        long next = consumerNext;
        long need = batchedTo + batchRows + 1;
        if (next >= 0 && next < planned) {
            // a batch not made is reached by no column yet: it holds at most a batch's rows
            need = keyOf(next) + batchRows + 1;
            Batch b = madeBatch(next);
            if (b != null) {
                need = Math.max(b.endKey() + 1, need);
            }
            if (next == planned - 1) {
                need = Math.max(need, batchedTo + 1);
            }
        }
        return new JobScheduler.Position(need, reach(pastBatched), readFailure.row());
    }

    /// The key the cap's kept rows past the first row some column has not published end at. A
    /// unit's base lies past every row of the units before it, so the rows a unit's filter drops
    /// lie between the key of its last kept row and the next unit's base, and take none of the
    /// cap. Each batched unit records the rows the units batched before it keep, so the cap's end
    /// among them is found by a search over those counts, not a walk; past the batched units,
    /// `pastBatched` gives the key the rows left end at.
    private long reach(LongUnaryOperator pastBatched) {
        if (head == planned) {
            return pastBatched.applyAsLong(capRows);
        }
        Span first = spans.get(spanOf(head));
        long end = first.keptBefore + keyOf(head) - first.base + capRows;
        if (end >= keptBatched) {
            return pastBatched.applyAsLong(end - keptBatched);
        }
        Span span = spans.get(spanKeeping(end));
        return span.base + end - span.keptBefore;
    }

    /// The position in `spans` of the span holding kept row `kept`, counted from the read's first:
    /// the last whose units before it keep no more.
    private int spanKeeping(long kept) {
        int low = firstSpan;
        int high = spans.size() - 1;
        while (low < high) {
            int mid = (low + high + 1) >>> 1;
            if (spans.get(mid).keptBefore <= kept) {
                low = mid;
            }
            else {
                high = mid - 1;
            }
        }
        return low;
    }

    /// Counts the consumer's next batch: the next of the column furthest behind, since a consumer takes
    /// a batch of every column it reads in a step before the next step; `-1` where every column
    /// has ended. A column that does not read a unit passes its batches as they are made, ahead
    /// of the consumer, so the column furthest ahead may be a batch of the plan no decode has
    /// reached. Kept as the columns publish: a column's next batch only rises and an ended column
    /// does not reopen, so the columns are looked at again only once the last one at it leaves it.
    /// An abandoned read is not kept, as it asks for its position no more.
    private void recountConsumerNext() {
        long next = -1;
        int at = 0;
        for (int c = 0; c < published.length; c++) {
            if (exchanges.get(c).isFinished()) {
                continue;
            }
            if (next < 0 || published[c] < next) {
                next = published[c];
                at = 1;
            }
            else if (published[c] == next) {
                at++;
            }
        }
        consumerNext = next;
        atConsumerNext = at;
    }

    /// The key of the first row some column has not published.
    long firstUnpublished(long batchedTo) {
        return head == planned ? batchedTo : keyOf(head);
    }

    /// The key batch `i` starts at, made or not.
    private long keyOf(long i) {
        Span span = spans.get(spanOf(i));
        return span.key(span.firstSegment + Math.toIntExact(i - span.firstIndex));
    }

    /// Ends every exchange not ended, as the read closes: a consumer waiting for a batch finds the
    /// end.
    void stop() {
        for (BatchExchange<?> exchange : exchanges) {
            if (!exchange.isFinished()) {
                exchange.finish();
            }
        }
        recountConsumerNext();
    }
}
