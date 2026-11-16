/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.util.BitSet;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.memory.ReadAheadBuffer;
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

        assertThat(at.need()).isEqualTo(1100);
        assertThat(at.reach()).isEqualTo(1500);
    }

    /// A batch the plan has closed, by adding the next, ends the need where it ends.
    @Test
    void needEndsWithAClosedBatch() {
        BatchPlan plan = new BatchPlan(BATCH_ROWS, true, false, 0);
        publisher.add(plan.plan(0, 0, "a", false, 20), 1000, new BitSet());
        publisher.add(plan.plan(1, 0, "a", false, 20), 1020, new BitSet());

        assertThat(publisher.position(5000).need()).isEqualTo(1100);
    }
}
