/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.batch;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// How far a detaching [BatchExchange] lets its column publish ahead of the consumer: two full
/// batches' worth of rows, however the batches are cut. A publish past that is refused until the
/// consumer takes a batch; an empty queue takes any batch.
///
/// Each batch here is an `Integer` holding its own row count.
class BatchExchangeRowLimitTest {

    private static final int CAPACITY = 4;

    @Test
    void twoFullBatchesFillTheQueue() throws Exception {
        BatchExchange<Integer> exchange = exchange();
        assertThat(exchange.tryPublish(CAPACITY)).isTrue();
        assertThat(exchange.tryPublish(CAPACITY)).isTrue();
        assertThat(exchange.tryPublish(CAPACITY)).as("past two full batches").isFalse();

        assertThat(exchange.poll()).isEqualTo(CAPACITY);
        assertThat(exchange.tryPublish(CAPACITY)).as("room for one full batch again").isTrue();
    }

    @Test
    void shortBatchesQueueUpToTwoFullBatchesOfRows() throws Exception {
        BatchExchange<Integer> exchange = exchange();
        for (int i = 0; i < 2 * CAPACITY; i++) {
            assertThat(exchange.tryPublish(1)).as("one-row batch %d", i).isTrue();
        }

        assertThat(exchange.tryPublish(1)).as("past two full batches' rows").isFalse();
    }

    @Test
    void aBatchThatWouldTakeTheQueuePastTheLimitIsRefused() throws Exception {
        BatchExchange<Integer> exchange = exchange();
        assertThat(exchange.tryPublish(CAPACITY)).isTrue();
        assertThat(exchange.tryPublish(CAPACITY - 1)).isTrue();

        assertThat(exchange.tryPublish(2)).isFalse();
        assertThat(exchange.poll()).isEqualTo(CAPACITY);
        assertThat(exchange.tryPublish(2)).isTrue();
    }

    @Test
    void anEmptyQueueTakesAnyBatch() {
        BatchExchange<Integer> exchange = exchange();
        assertThat(exchange.tryPublish(3 * CAPACITY)).isTrue();
    }

    @Test
    void aFlatBatchCountsTheRowsItsArrayHoldsNotItsRecords() {
        BatchExchange<BatchExchange.Batch> exchange = BatchExchange.detachingFlat("c", CAPACITY);
        assertThat(exchange.tryPublish(flatBatch(1, CAPACITY))).isTrue();
        assertThat(exchange.tryPublish(flatBatch(1, CAPACITY))).isTrue();

        assertThat(exchange.tryPublish(flatBatch(1, CAPACITY))).isFalse();
    }

    @Test
    void aNestedBatchCountsItsRecords() {
        BatchExchange<NestedBatch> exchange = BatchExchange.detachingNested("c", CAPACITY);
        assertThat(exchange.tryPublish(nestedBatch(CAPACITY))).isTrue();
        assertThat(exchange.tryPublish(nestedBatch(CAPACITY - 1))).isTrue();

        assertThat(exchange.tryPublish(nestedBatch(2))).isFalse();
    }

    private static BatchExchange.Batch flatBatch(int records, int capacity) {
        BatchExchange.Batch batch = new BatchExchange.Batch();
        batch.recordCount = records;
        batch.capacity = capacity;
        return batch;
    }

    private static NestedBatch nestedBatch(int records) {
        NestedBatch batch = new NestedBatch();
        batch.recordCount = records;
        return batch;
    }

    private static BatchExchange<Integer> exchange() {
        return BatchExchange.detaching("c", () -> 0, Integer::intValue, CAPACITY);
    }
}
