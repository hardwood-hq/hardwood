/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.batch;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import dev.hardwood.jfr.AbstractJfrRecorderTest;
import jdk.jfr.consumer.RecordedEvent;

import static org.assertj.core.api.Assertions.assertThat;

/// Asserts that a consumer stalling on the assembly pipeline emits
/// `dev.hardwood.BatchWait`, and that a consumer served from a full ready queue
/// emits nothing.
///
/// Driven at the [BatchExchange] level rather than through a file read: whether a
/// real read outruns its pipeline depends on decode and I/O timing, so publishing
/// the batch once the consumer waits for it, or before it asks, is the only way to
/// pin both the presence and the absence of the event.
class BatchWaitEventTest extends AbstractJfrRecorderTest {

    private static final String BATCH_WAIT_EVENT = "dev.hardwood.BatchWait";

    @Test
    void stalledConsumerEmitsBatchWaitEventForItsColumn() throws Exception {
        BatchExchange<String> exchange = BatchExchange.detaching("price", () -> "batch", batch -> 1, 1);
        AtomicReference<String> polled = new AtomicReference<>();
        Thread consumer = Thread.ofPlatform().start(() -> {
            try {
                polled.set(exchange.poll());
            }
            catch (InterruptedException | IOException e) {
                throw new IllegalStateException(e);
            }
        });

        // Published only once the consumer waits in the empty ready queue; nothing else takes the
        // queue's lock before the publish, so a waiting consumer is one inside poll()'s wait
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (consumer.getState() != Thread.State.WAITING) {
            assertThat(System.nanoTime() - deadline).as("the consumer waits for a batch").isNegative();
            Thread.sleep(1);
        }
        exchange.tryPublish("batch-1");
        consumer.join();
        assertThat(polled.get()).isEqualTo("batch-1");
        awaitEvents();

        RecordedEvent event = events(BATCH_WAIT_EVENT).findFirst().orElseThrow();
        assertThat(event.getString("column")).isEqualTo("price");
        assertThat(event.getDuration())
                .as("the event's duration is the stall the consumer paid")
                .isGreaterThan(Duration.ZERO);
        assertThat(events(BATCH_WAIT_EVENT).count())
                .as("one stall, one event")
                .isEqualTo(1);
    }

    @Test
    void consumerServedWithoutWaitingEmitsNoEvent() throws Exception {
        BatchExchange<String> exchange = BatchExchange.detaching("price", () -> "batch", batch -> 1, 1);
        exchange.tryPublish("batch-1");

        assertThat(exchange.poll()).isEqualTo("batch-1");
        awaitEvents();

        assertThat(events(BATCH_WAIT_EVENT).count())
                .as("the non-blocking poll hit, so the consumer never stalled")
                .isZero();
    }

    @Test
    void drainedFinishedExchangeEmitsNoEvent() throws Exception {
        BatchExchange<String> exchange = BatchExchange.detaching("price", () -> "batch", batch -> 1, 1);
        exchange.finish();

        assertThat(exchange.poll()).isNull();
        awaitEvents();

        assertThat(events(BATCH_WAIT_EVENT).count())
                .as("end of stream is not a stall — every column would report one otherwise")
                .isZero();
    }
}
