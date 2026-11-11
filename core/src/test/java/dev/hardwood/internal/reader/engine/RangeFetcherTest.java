/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.nio.ByteBuffer;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.fetch.FileRange;
import dev.hardwood.internal.reader.memory.ReadAheadBuffer;

import static org.assertj.core.api.Assertions.assertThat;

class RangeFetcherTest {

    private static final long NONE = Long.MAX_VALUE;
    private static final JobScheduler.Position AT_START = new JobScheduler.Position(0, NONE, NONE);

    private final JobScheduler scheduler = new JobScheduler(new ReadAheadBuffer(1000), () -> {
    }, 4, 1000);
    private final RangeFetcher fetcher = new RangeFetcher(scheduler);
    private final InputFile file = InputFile.of(ByteBuffer.allocate(1000));

    /// A started fetch's charge goes back to the scheduler with the range's last reader, not before.
    @Test
    void theLastReaderReturnsAStartedFetchsCharge() {
        RangeFetch range = range(2);
        fetcher.fetch(file, range, 10, t -> {
        });
        assertThat(scheduler.start(AT_START)).hasSize(1);

        fetcher.readersDone(List.of(range));
        assertThat(scheduler.takeReleased()).isZero();
        fetcher.readersDone(List.of(range));
        assertThat(scheduler.takeReleased()).isEqualTo(100);
    }

    /// A fetch nothing reads any more does not start.
    @Test
    void theLastReaderTakesOutAFetchNotStarted() {
        RangeFetch range = range(1);
        fetcher.fetch(file, range, 10, t -> {
        });

        fetcher.readersDone(List.of(range));

        assertThat(scheduler.start(AT_START)).isEmpty();
    }

    private static RangeFetch range(int readers) {
        RangeFetch range = new RangeFetch(new FileRange(0, 100, "test"));
        for (int i = 0; i < readers; i++) {
            range.charge.reader();
        }
        return range;
    }
}
