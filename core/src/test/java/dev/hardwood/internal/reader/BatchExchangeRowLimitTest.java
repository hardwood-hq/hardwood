/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// How far a detaching [BatchExchange] lets its drain run ahead: two full batches' worth of rows,
/// however the batches are cut, and once that is reached, not again until the consumer has taken
/// the queue down to one full batch's worth.
///
/// Each batch here is an `Integer` holding its own row count.
class BatchExchangeRowLimitTest {

    private static final int CAPACITY = 4;

    /// How long a publish expected to wait is given to show that it does not return. A wait that
    /// has not ended by then is taken as waiting; a wrong pass would need the exchange to release
    /// the drain only after this long, which it has no reason to.
    private static final long STILL_WAITING_MILLIS = 100;

    @Test
    void twoFullBatchesFillTheQueue() throws Exception {
        BatchExchange<Integer> exchange = exchange();
        assertThat(exchange.publish(CAPACITY)).isTrue();
        assertThat(exchange.publish(CAPACITY)).isTrue();

        CompletableFuture<Boolean> third = publishAsync(exchange, CAPACITY);
        assertStillWaiting(third);

        assertThat(exchange.poll()).isEqualTo(CAPACITY);
        assertThat(third.get(5, TimeUnit.SECONDS)).as("room for one full batch again").isTrue();
    }

    @Test
    void shortBatchesQueueUpToTwoFullBatchesOfRows() throws Exception {
        BatchExchange<Integer> exchange = exchange();
        for (int i = 0; i < 2 * CAPACITY; i++) {
            assertThat(exchange.publish(1)).as("one-row batch %d", i).isTrue();
        }

        assertStillWaiting(publishAsync(exchange, 1));
    }

    @Test
    void aBatchThatWouldTakeTheQueuePastTheLimitWaits() throws Exception {
        BatchExchange<Integer> exchange = exchange();
        exchange.publish(CAPACITY);
        exchange.publish(CAPACITY - 1);

        CompletableFuture<Boolean> waiting = publishAsync(exchange, 2);
        assertStillWaiting(waiting);

        assertThat(exchange.poll()).isEqualTo(CAPACITY);
        assertThat(waiting.get(5, TimeUnit.SECONDS)).as("down to one full batch's worth").isTrue();
    }

    @Test
    void aDrainThatFoundTheQueueFullWaitsForItToHoldOneFullBatch() throws Exception {
        BatchExchange<Integer> exchange = exchange();
        for (int i = 0; i < 2 * CAPACITY; i++) {
            exchange.publish(1);
        }
        CompletableFuture<Boolean> waiting = publishAsync(exchange, 1);
        assertStillWaiting(waiting);

        for (int i = 0; i < CAPACITY - 1; i++) {
            exchange.poll();
        }
        assertStillWaiting(waiting);

        exchange.poll();
        assertThat(waiting.get(5, TimeUnit.SECONDS)).as("down to one full batch's worth").isTrue();
    }

    @Test
    void finishReleasesAWaitingDrain() throws Exception {
        BatchExchange<Integer> exchange = exchange();
        exchange.publish(CAPACITY);
        exchange.publish(CAPACITY);
        CompletableFuture<Boolean> waiting = publishAsync(exchange, CAPACITY);
        assertStillWaiting(waiting);

        exchange.finish();

        assertThat(waiting.get(5, TimeUnit.SECONDS)).as("the batch is not published").isFalse();
    }

    @Test
    void aFlatBatchCountsTheRowsItsArrayHoldsNotItsRecords() throws Exception {
        BatchExchange<BatchExchange.Batch> exchange = BatchExchange.detachingFlat("c", CAPACITY);
        exchange.publish(flatBatch(1, CAPACITY));
        exchange.publish(flatBatch(1, CAPACITY));

        assertStillWaiting(publishAsync(exchange, flatBatch(1, CAPACITY)));
    }

    @Test
    void aNestedBatchCountsItsRecords() throws Exception {
        BatchExchange<NestedBatch> exchange = BatchExchange.detachingNested("c", CAPACITY);
        NestedBatch first = new NestedBatch();
        first.recordCount = CAPACITY;
        NestedBatch second = new NestedBatch();
        second.recordCount = CAPACITY - 1;
        exchange.publish(first);
        exchange.publish(second);

        NestedBatch third = new NestedBatch();
        third.recordCount = 2;
        assertStillWaiting(publishAsync(exchange, third));
    }

    private static BatchExchange.Batch flatBatch(int records, int capacity) {
        BatchExchange.Batch batch = new BatchExchange.Batch();
        batch.recordCount = records;
        batch.capacity = capacity;
        return batch;
    }

    private static BatchExchange<Integer> exchange() {
        return BatchExchange.detaching("c", () -> 0, Integer::intValue, CAPACITY);
    }

    /// Starts a publish on its own thread and returns once that thread has either finished or
    /// parked, so that what the test does next happens after the publish has looked at the queue.
    private static <B> CompletableFuture<Boolean> publishAsync(BatchExchange<B> exchange, B batch)
            throws InterruptedException {
        CompletableFuture<Boolean> published = new CompletableFuture<>();
        Thread drain = Thread.ofPlatform().daemon().start(() -> {
            try {
                published.complete(exchange.publish(batch));
            }
            catch (Throwable t) {
                published.completeExceptionally(t);
            }
        });
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!published.isDone() && drain.getState() != Thread.State.TIMED_WAITING
                && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        return published;
    }

    private static void assertStillWaiting(CompletableFuture<Boolean> publish) throws InterruptedException {
        Thread.sleep(STILL_WAITING_MILLIS);
        assertThat(publish).as("the publish is still waiting for room").isNotDone();
    }
}
