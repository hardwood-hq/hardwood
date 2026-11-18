/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.batch.FlatBatch;
import dev.hardwood.internal.reader.memory.ReadAheadBuffer;
import dev.hardwood.internal.reader.pipeline.BatchCell;
import dev.hardwood.internal.reader.plan.BatchPlan;

import static org.assertj.core.api.Assertions.assertThat;

class BatchPublisherTest {

    private static final int BATCH_ROWS = 100;

    private final BatchPublisher publisher = new BatchPublisher(
            new RangeFetcher(new JobScheduler(new ReadAheadBuffer(1000), () -> {
            }, 1, 1000)), new ReadFailure(),
            List.of(BatchExchange.detachingFlat("c", BATCH_ROWS)), BATCH_ROWS, 50);

    /// The last batch the plan has added may go on into the units not yet batched, such as
    /// those Bloom filters drop as they are planned, which lie past the slowest column's
    /// look-ahead: the planning of the next is within need, and the look-ahead reaches to it.
    @Test
    void needReachesTheNextUnitWhileTheLastBatchMayGoOn() {
        publisher.add(new BatchPlan(BATCH_ROWS, false, false, 0).plan(0, 0, "a", false, 20), 1000, new BitSet());

        JobScheduler.Position at = publisher.position(5000);

        assertThat(at.need()).isGreaterThan(5000);
        assertThat(at.reach()).isEqualTo(at.need());
    }

    /// The look-ahead reaches the cap's rows past the first row some column has not published.
    @Test
    void lookAheadReachesTheCapPastTheFirstUnpublishedRow() {
        BatchPublisher wide = new BatchPublisher(new RangeFetcher(new JobScheduler(new ReadAheadBuffer(1000), () -> {
        }, 1, 1000)), new ReadFailure(), List.of(BatchExchange.detachingFlat("c", BATCH_ROWS)), BATCH_ROWS, 500);
        wide.add(new BatchPlan(BATCH_ROWS, false, false, 0).plan(0, 0, "a", false, 20), 1000, new BitSet());

        JobScheduler.Position at = wide.position(1020);

        assertThat(at.need()).isEqualTo(1101);
        assertThat(at.reach()).isEqualTo(1500);
    }

    /// A batch the plan has closed, by adding the next, ends the need one row past it: a column
    /// with repetition closes the batch only as it places the record after it.
    @Test
    void needReachesOneRowPastAClosedBatch() {
        BatchPlan plan = new BatchPlan(BATCH_ROWS, true, false, 0);
        publisher.add(plan.plan(0, 0, "a", false, 20), 1000, new BitSet());
        publisher.add(plan.plan(1, 0, "a", false, 20), 1020, new BitSet());

        assertThat(publisher.position(5000).need()).isEqualTo(1101);
    }

    /// A row group whose first rows continue the batch the one before left open, added before any
    /// batch is made: its own batches start after the continued one, at the plan's rows.
    @Test
    void aContinuingRowGroupStartsItsBatchesAfterTheOneItContinues() throws Exception {
        BatchExchange<FlatBatch> exchange = BatchExchange.detachingFlat("c", BATCH_ROWS);
        BatchPublisher one = publisher(List.of(exchange));
        BatchPlan plan = new BatchPlan(BATCH_ROWS, false, false, 0);
        // 20 rows open the first batch; 80 of the next row group's 150 close it, 70 start the next
        one.add(plan.plan(0, 0, "a", false, 20), 0, new BitSet());
        one.add(plan.plan(1, 0, "a", false, 150), 20, new BitSet());

        TestCell first = cell(exchange, 0);
        TestCell second = cell(exchange, 100);
        one.open(0, first);
        one.open(0, second);
        one.completed(0, first);
        one.completed(0, second);
        one.publish(170, true);

        assertThat(first.batchIndex()).isZero();
        assertThat(second.batchIndex()).isEqualTo(1);
        assertThat(drain(exchange)).containsExactly(first.batch, second.batch);
    }

    /// A filter-only column that skips a row group opens its next cell past the row group's
    /// batches without making them, and waits in publishing until the column reading the row group
    /// has made and published them; then both finish.
    @Test
    void aColumnSkippingARowGroupWaitsForTheColumnsReadingIt() throws Exception {
        BatchExchange<FlatBatch> payload = BatchExchange.detachingFlat("p", BATCH_ROWS);
        BatchExchange<FlatBatch> filterOnly = BatchExchange.detachingFlat("f", BATCH_ROWS);
        BatchPublisher two = publisher(List.of(payload, filterOnly));
        BatchPlan plan = new BatchPlan(BATCH_ROWS, true, false, 0);
        BitSet skipped = new BitSet();
        skipped.set(1);
        // three batches the filter-only column skips, then two it reads
        two.add(plan.plan(0, 0, "a", true, 3 * BATCH_ROWS), 0, skipped);
        two.add(plan.plan(1, 0, "a", false, 2 * BATCH_ROWS), 3 * BATCH_ROWS, new BitSet());

        List<TestCell> filterCells = List.of(cell(filterOnly, 3 * BATCH_ROWS), cell(filterOnly, 4 * BATCH_ROWS));
        for (TestCell c : filterCells) {
            two.open(1, c);
            two.completed(1, c);
        }
        assertThat(filterCells.get(0).batchIndex()).isEqualTo(3);
        two.publish(5 * BATCH_ROWS, true);
        assertThat(filterOnly.isFinished()).isFalse();

        List<TestCell> payloadCells = new ArrayList<>();
        for (int b = 0; b < 5; b++) {
            TestCell c = cell(payload, (long) b * BATCH_ROWS);
            two.open(0, c);
            two.completed(0, c);
            payloadCells.add(c);
        }
        two.publish(5 * BATCH_ROWS, true);

        assertThat(drain(payload)).containsExactlyElementsOf(payloadCells.stream().map(c -> c.batch).toList());
        assertThat(drain(filterOnly)).containsExactly(filterCells.get(0).batch, filterCells.get(1).batch);
        assertThat(two.firstUnpublished(5 * BATCH_ROWS)).isEqualTo(5 * BATCH_ROWS);
    }

    /// A filter-only column that skips a unit passes its batches as the payload column's
    /// placement makes them, reaching the end of a plan still growing while the payload column has
    /// published two. The consumer waits for the payload column's third batch, so the need ends
    /// there, not at the end of the plan (#1814).
    @Test
    void needFollowsTheColumnFurthestBehind() throws Exception {
        BatchExchange<FlatBatch> payload = BatchExchange.detachingFlat("p", BATCH_ROWS);
        BatchExchange<FlatBatch> filterOnly = BatchExchange.detachingFlat("f", BATCH_ROWS);
        BatchPublisher two = publisher(List.of(payload, filterOnly));
        BitSet skipped = new BitSet();
        skipped.set(1);
        two.add(new BatchPlan(BATCH_ROWS, false, false, 0).plan(0, 0, "a", false, 5 * BATCH_ROWS), 0, skipped);

        List<TestCell> payloadCells = new ArrayList<>();
        for (int b = 0; b < 5; b++) {
            TestCell c = cell(payload, (long) b * BATCH_ROWS);
            two.open(0, c);
            payloadCells.add(c);
        }
        two.completed(0, payloadCells.get(0));
        two.completed(0, payloadCells.get(1));
        two.publish(5 * BATCH_ROWS, false);

        assertThat(filterOnly.isFinished()).isFalse();
        assertThat(two.position(5 * BATCH_ROWS).need()).isEqualTo(3 * BATCH_ROWS + 1);
    }

    private static BatchPublisher publisher(List<BatchExchange<?>> exchanges) {
        return new BatchPublisher(new RangeFetcher(new JobScheduler(new ReadAheadBuffer(1000), () -> {
        }, 1, 1000)), new ReadFailure(), exchanges, BATCH_ROWS, 50);
    }

    private static TestCell cell(BatchExchange<FlatBatch> exchange, long firstRow) {
        return new TestCell(firstRow, exchange.newBatch());
    }

    /// The batches the exchange holds, in publish order, up to its end.
    private static List<FlatBatch> drain(BatchExchange<FlatBatch> exchange) throws IOException, InterruptedException {
        List<FlatBatch> batches = new ArrayList<>();
        for (FlatBatch b = exchange.poll(); b != null; b = exchange.poll()) {
            batches.add(b);
        }
        return batches;
    }

    /// A cell holding a batch of its exchange, complete as soon as it is marked so.
    private static final class TestCell implements BatchCell<FlatBatch> {
        private final long firstRow;
        private final FlatBatch batch;
        private long batchIndex = -1;
        private boolean completed;

        TestCell(long firstRow, FlatBatch batch) {
            this.firstRow = firstRow;
            this.batch = batch;
        }

        @Override
        public long firstRow() {
            return firstRow;
        }

        @Override
        public long batchIndex() {
            return batchIndex;
        }

        @Override
        public void batchIndex(long index) {
            batchIndex = index;
        }

        @Override
        public boolean isCompleted() {
            return completed;
        }

        @Override
        public void markCompleted() {
            completed = true;
        }

        @Override
        public FlatBatch columnBatch() {
            return batch;
        }

        @Override
        public void release() {
        }
    }
}
