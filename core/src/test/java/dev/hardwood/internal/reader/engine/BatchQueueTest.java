/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.memory.ReadAheadBuffer;

import static org.assertj.core.api.Assertions.assertThat;

class BatchQueueTest {

    private static final int BATCH_ROWS = 100;

    private final BatchQueue queue = new BatchQueue(
            new JobScheduler(new ReadAheadBuffer(1000), () -> {
            }, 1, 1000),
            List.of(BatchExchange.detachingFlat("c", BATCH_ROWS)), BATCH_ROWS, 50);

    /// The last batch the plan has added may go on into the units not yet installed, such as
    /// those Bloom filters drop as they are planned, which lie past the slowest column's
    /// look-ahead: the planning of the next is within need, and the look-ahead reaches to it.
    @Test
    void needReachesTheNextUnitWhileTheLastBatchMayGoOn() {
        Batch open = new Batch(1000, 0, 1);
        open.endKey = 1020;
        queue.add(open);

        JobScheduler.Position at = queue.position(5000);

        assertThat(at.need()).isGreaterThan(5000);
        assertThat(at.reach()).isEqualTo(at.need());
    }

    /// The look-ahead reaches the cap's rows past the first row some column has not published.
    @Test
    void lookAheadReachesTheCapPastTheFirstUnpublishedRow() {
        BatchQueue wide = new BatchQueue(new JobScheduler(new ReadAheadBuffer(1000), () -> {
        }, 1, 1000), List.of(BatchExchange.detachingFlat("c", BATCH_ROWS)), BATCH_ROWS, 500);
        Batch open = new Batch(1000, 0, 1);
        open.endKey = 1020;
        wide.add(open);

        JobScheduler.Position at = wide.position(1020);

        assertThat(at.need()).isEqualTo(1100);
        assertThat(at.reach()).isEqualTo(1500);
    }

    /// A batch the plan has closed, by adding the next, ends the need where it ends.
    @Test
    void needEndsWithAClosedBatch() {
        Batch closed = new Batch(1000, 0, 1);
        closed.endKey = 1020;
        queue.add(closed);
        queue.add(new Batch(1020, 20, 1));

        assertThat(queue.position(5000).need()).isEqualTo(1100);
    }
}
