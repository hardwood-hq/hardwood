/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// What [BatchExchange#finish()] may and may not do to the batches already in flight.
///
/// `finish()` hands the consumer an end-of-stream sentinel through the ready queue, so that a
/// poll already waiting learns of the end from the queue rather than from the next expiry of its
/// timed poll. That puts a non-batch into the same queue the batches travel through, and these
/// are the three ways that could go wrong: the sentinel overtaking a batch, the sentinel being
/// recycled as if it were one, and the queue reading as ended only once.
///
/// The latency the sentinel exists for is asserted separately, in
/// [dev.hardwood.ReaderEofLatencyTest].
class BatchExchangeTest {

    @Test
    void deliversEveryQueuedBatchBeforeTheEnd() throws Exception {
        BatchExchange<Object> exchange = BatchExchange.detaching("c", Object::new);
        Object first = new Object();
        Object second = new Object();

        exchange.publish(first);
        exchange.publish(second);
        exchange.finish();

        assertThat(exchange.poll()).as("first batch").isSameAs(first);
        assertThat(exchange.poll()).as("second batch").isSameAs(second);
        assertThat(exchange.poll()).as("end of stream").isNull();
    }

    /// The case above fills the queue to capacity, so `finish()` cannot offer its sentinel at
    /// all and the consumer reaches the end through the `finished` flag. This is the other side:
    /// room to spare, the sentinel queued behind the batch, and the batch still delivered first.
    @Test
    void queuesTheSentinelBehindABatchRatherThanAheadOfIt() throws Exception {
        BatchExchange<Object> exchange = BatchExchange.detaching("c", Object::new);
        Object only = new Object();

        exchange.publish(only);
        exchange.finish();

        assertThat(exchange.poll()).as("the batch").isSameAs(only);
        assertThat(exchange.poll()).as("end of stream").isNull();
    }

    /// A consumer that keeps asking past the end keeps being told the same thing. The sentinel
    /// is consumed by the poll that reports it, so every later poll has to reach the same
    /// conclusion from the `finished` flag instead.
    @Test
    void staysEndedOnceItHasEnded() throws Exception {
        BatchExchange<Object> exchange = BatchExchange.detaching("c", Object::new);
        exchange.finish();

        assertThat(exchange.poll()).as("first poll past the end").isNull();
        assertThat(exchange.poll()).as("second poll past the end").isNull();
        assertThat(exchange.poll()).as("third poll past the end").isNull();
    }

    /// In recycling mode the consumer's leftovers go back to the free pool for the drain to
    /// refill. The sentinel is not one of them: a pool holding it would hand the drain something
    /// to write a batch of values into that is not a batch.
    @Test
    void doesNotRecycleTheSentinelIntoTheFreePool() throws Exception {
        List<Object> holders = new ArrayList<>();
        BatchExchange<Object> exchange = BatchExchange.recycling("c", () -> {
            Object holder = new Object();
            holders.add(holder);
            return holder;
        });

        Object taken = exchange.takeBatch();
        exchange.publish(taken);
        exchange.finish();
        exchange.drainReady();

        List<Object> pooled = new ArrayList<>();
        Object holder;
        while ((holder = exchange.takeBatch()) != null) {
            pooled.add(holder);
        }

        assertThat(pooled)
                .as("everything the free pool holds was made by the batch factory")
                .isSubsetOf(holders);
        assertThat(pooled)
                .as("the published batch came back and nothing was lost with it")
                .containsExactlyInAnyOrderElementsOf(holders);
    }

    /// An error ends the stream too, and has to reach the consumer rather than sit behind a
    /// poll that has already returned. [BatchExchange#signalError] goes through `finish()` for
    /// that reason, which puts the sentinel in the queue ahead of the throw.
    @Test
    void raisesAnErrorThroughTheEndOfStreamPath() {
        BatchExchange<Object> exchange = BatchExchange.detaching("c", Object::new);
        exchange.signalError(new IllegalStateException("decode failed"));

        assertThat(exchange.isFinished()).as("an error ends the stream").isTrue();
        assertThatThrownBy(exchange::poll)
                .as("the error reaches the consumer rather than a silent null")
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("decode failed");
    }

    /// Two failures can reach one exchange, for instance when decode tasks for two pages of a
    /// column both fail. The first one ended the stream, so it is the one the consumer sees.
    @Test
    void reportsTheFirstOfSeveralErrors() {
        BatchExchange<Object> exchange = BatchExchange.detaching("c", Object::new);
        IllegalStateException first = new IllegalStateException("page 3 is corrupt");
        OutOfMemoryError second = new OutOfMemoryError("Java heap space");
        exchange.signalError(first);
        exchange.signalError(second);

        assertThatThrownBy(exchange::poll)
                .isSameAs(first)
                .hasMessage("page 3 is corrupt");
        assertThatThrownBy(exchange::checkError)
                .isSameAs(first)
                .hasMessage("page 3 is corrupt");
        assertThat(first.getSuppressed()).containsExactly(second);
    }

    /// The same throwable signalled twice is raised once, and is not suppressed onto itself.
    @Test
    void reportsARepeatedErrorOnce() {
        BatchExchange<Object> exchange = BatchExchange.detaching("c", Object::new);
        IllegalStateException error = new IllegalStateException("page 3 is corrupt");
        exchange.signalError(error);
        exchange.signalError(error);

        assertThatThrownBy(exchange::checkError)
                .isSameAs(error)
                .hasMessage("page 3 is corrupt");
        assertThat(error.getSuppressed()).isEmpty();
    }
}
