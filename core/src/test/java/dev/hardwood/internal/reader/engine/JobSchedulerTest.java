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
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.reader.memory.ReadAheadBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class JobSchedulerTest {

    private static final long NONE = Long.MAX_VALUE;
    private static final Runnable NOTHING = () -> {
    };

    private final List<String> applied = new ArrayList<>();
    private final AtomicInteger wakes = new AtomicInteger();

    private JobScheduler.Job<String> job(String name, long key, long cost, JobScheduler.Kind kind, Charge charge) {
        return job(name + " ", key, cost, kind, charge, buffers -> name);
    }

    /// A job that, as it ends, records its value, or `prefix` and its failure.
    private <T> JobScheduler.Job<T> job(String prefix, long key, long cost, JobScheduler.Kind kind, Charge charge,
                                        JobScheduler.Work<T> work) {
        return new JobScheduler.Job<>(key, cost, kind, charge, work, value -> applied.add(String.valueOf(value)),
                t -> applied.add(prefix + "failed: " + t.getMessage()));
    }

    private static List<Long> keys(List<JobScheduler.Job<?>> jobs) {
        return jobs.stream().map(j -> j.key()).toList();
    }

    private JobScheduler scheduler(long budget, int cpuSlots, long fetchAhead) {
        return new JobScheduler(new ReadAheadBuffer(budget), wakes::incrementAndGet, cpuSlots, fetchAhead);
    }

    @Test
    void startsLowestKeyFirstAcrossKinds() {
        JobScheduler s = scheduler(1000, 4, 1000);
        s.add(job("d", 30, 0, JobScheduler.Kind.DECODE, null));
        s.add(job("f", 10, 0, JobScheduler.Kind.FETCH, null));
        s.add(job("c", 20, 0, JobScheduler.Kind.CPU, null));

        assertThat(keys(s.start(new JobScheduler.Position(0, NONE, NONE)))).containsExactly(10L, 20L, 30L);
        assertThat(s.inFlight()).isEqualTo(3);
    }

    @Test
    void lookAheadHoldsBackDecodesOnly() {
        JobScheduler s = scheduler(1000, 4, 1000);
        s.add(job("d1", 10, 0, JobScheduler.Kind.DECODE, null));
        s.add(job("d2", 50, 0, JobScheduler.Kind.DECODE, null));
        s.add(job("f", 60, 0, JobScheduler.Kind.FETCH, null));

        assertThat(keys(s.start(new JobScheduler.Position(0, 50, NONE)))).containsExactly(10L, 60L);
    }

    @Test
    void decodeBelowNeedStartsPastTheLookAhead() {
        JobScheduler s = scheduler(1000, 4, 1000);
        s.add(job("d1", 60, 0, JobScheduler.Kind.DECODE, null));
        s.add(job("d2", 90, 0, JobScheduler.Kind.DECODE, null));
        s.add(job("d3", 120, 0, JobScheduler.Kind.DECODE, null));

        // the consumer's next batch reaches past the look-ahead: its decodes start regardless
        assertThat(keys(s.start(new JobScheduler.Position(100, 50, NONE)))).containsExactly(60L, 90L);
    }

    @Test
    void cpuJobsWaitForASlot() {
        JobScheduler s = scheduler(1000, 1, 1000);
        JobScheduler.Job<?> first = job("c1", 10, 0, JobScheduler.Kind.CPU, null);
        s.add(first);
        s.add(job("c2", 20, 0, JobScheduler.Kind.CPU, null));
        s.add(job("f", 30, 0, JobScheduler.Kind.FETCH, null));
        JobScheduler.Position at = new JobScheduler.Position(0, NONE, NONE);

        assertThat(keys(s.start(at))).containsExactly(10L, 30L);
        s.run(first, null).run();
        assertThat(keys(s.start(at))).containsExactly(20L);
        assertThat(applied).containsExactly("c1");
    }

    @Test
    void fetchStartsWhileLessThanFIsAhead() {
        JobScheduler s = scheduler(1000, 4, 100);
        Charge first = new Charge();
        first.reader();
        s.add(job("f1", 10, 150, JobScheduler.Kind.FETCH, first));
        s.add(job("f2", 20, 10, JobScheduler.Kind.FETCH, new Charge()));
        s.add(job("d", 30, 10, JobScheduler.Kind.DECODE, new Charge()));
        JobScheduler.Position at = new JobScheduler.Position(0, NONE, NONE);

        // a request larger than F still runs one ahead; F then holds back fetches, not decodes
        assertThat(keys(s.start(at))).containsExactly(10L, 30L);
        assertThat(first.readerDone()).isTrue();
        s.release(first);
        assertThat(keys(s.start(at))).containsExactly(20L);
    }

    @Test
    void refusalByRHoldsBackChargingKindsAndWakesOnRelease() {
        ReadAheadBuffer r = new ReadAheadBuffer(100);
        JobScheduler s = new JobScheduler(r, wakes::incrementAndGet, 4, 1000);
        Charge first = new Charge();
        first.reader();
        s.add(job("f1", 10, 80, JobScheduler.Kind.FETCH, first));
        s.add(job("f2", 20, 80, JobScheduler.Kind.FETCH, new Charge()));
        s.add(job("d", 30, 10, JobScheduler.Kind.DECODE, new Charge()));
        s.add(job("c", 40, 0, JobScheduler.Kind.CPU, null));
        JobScheduler.Position at = new JobScheduler.Position(0, NONE, NONE);

        assertThat(keys(s.start(at))).containsExactly(10L, 40L);
        assertThat(s.start(at)).isEmpty();

        assertThat(first.readerDone()).isTrue();
        s.release(first);
        r.release(s.takeReleased());
        assertThat(wakes).hasValue(1);
        assertThat(keys(s.start(at))).containsExactly(20L, 30L);
        assertThat(r.held()).isEqualTo(90);
    }

    @Test
    void needStartsOneRefusedJobAtATimeUncharged() {
        ReadAheadBuffer r = new ReadAheadBuffer(10);
        JobScheduler s = new JobScheduler(r, wakes::incrementAndGet, 4, 0);
        JobScheduler.Job<?> first = job("f1", 10, 50, JobScheduler.Kind.FETCH, new Charge());
        s.add(first);
        s.add(job("f2", 20, 50, JobScheduler.Kind.FETCH, new Charge()));
        s.add(job("f3", 99, 50, JobScheduler.Kind.FETCH, new Charge()));
        JobScheduler.Position at = new JobScheduler.Position(50, NONE, NONE);

        assertThat(keys(s.start(at))).containsExactly(10L);
        assertThat(r.held()).isZero();
        assertThat(s.start(at)).isEmpty();

        s.run(first, null).run();
        assertThat(keys(s.start(at))).containsExactly(20L);
    }

    /// The progress guarantee whatever the bounds: jobs of every kind at random rows and costs,
    /// some larger than R, under R, F and CPU slots smaller than what is ready and a look-ahead
    /// that may lie behind the consumer's next batch. The consumer's next batch starts at the
    /// lowest row a job not yet ended serves, and a job's bytes are held until the consumer has
    /// passed its row. While a job is left, one is in flight, and every job ends.
    @Test
    void everyJobEndsWhateverTheBounds() {
        for (int seed = 0; seed < 500; seed++) {
            readToTheEnd(seed);
        }
    }

    private static void readToTheEnd(int seed) {
        Random random = new Random(seed);
        ReadAheadBuffer r = new ReadAheadBuffer(1 + random.nextInt(200));
        JobScheduler s = new JobScheduler(r, NOTHING, 1 + random.nextInt(3), random.nextInt(150));
        JobScheduler.Kind[] kinds = JobScheduler.Kind.values();
        List<JobScheduler.Job<?>> left = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            JobScheduler.Kind kind = kinds[random.nextInt(kinds.length)];
            boolean fetch = kind == JobScheduler.Kind.FETCH || kind == JobScheduler.Kind.FOOTER;
            long cost = fetch ? random.nextInt(300) : 0;
            JobScheduler.Job<?> job = new JobScheduler.Job<>(random.nextInt(1000), cost, kind,
                    cost > 0 ? new Charge() : null, buffers -> null, value -> {
                    }, t -> {
                    });
            s.add(job);
            left.add(job);
        }
        List<JobScheduler.Job<?>> running = new ArrayList<>();
        List<JobScheduler.Job<?>> holding = new ArrayList<>();
        while (!left.isEmpty()) {
            long consumer = left.stream().mapToLong(j -> j.key()).min().getAsLong();
            for (JobScheduler.Job<?> held : List.copyOf(holding)) {
                if (held.key() < consumer) {
                    s.release(held.charge());
                    holding.remove(held);
                }
            }
            r.release(s.takeReleased());
            long need = consumer + 1 + random.nextInt(100);
            long reach = consumer + random.nextInt(200) - 100;
            running.addAll(s.start(new JobScheduler.Position(need, reach, NONE)));
            if (running.isEmpty()) {
                fail("seed " + seed + ": nothing in flight, the job at row " + consumer + " waits");
            }
            JobScheduler.Job<?> ended = running.remove(random.nextInt(running.size()));
            s.run(ended, null).run();
            left.remove(ended);
            if (ended.charge() != null) {
                holding.add(ended);
            }
        }
    }

    @Test
    void nothingPastTheEndStarts() {
        JobScheduler s = scheduler(1000, 4, 1000);
        s.add(job("a", 10, 0, JobScheduler.Kind.CPU, null));
        s.add(job("b", 20, 0, JobScheduler.Kind.CPU, null));
        s.add(job("c", 21, 0, JobScheduler.Kind.CPU, null));

        assertThat(keys(s.start(new JobScheduler.Position(0, NONE, 20)))).containsExactly(10L, 20L);
    }

    @Test
    void fetchIsRetriedOnce() {
        JobScheduler s = scheduler(1000, 4, 1000);
        AtomicInteger runs = new AtomicInteger();
        JobScheduler.Job<String> flaky = job("", 10, 0, JobScheduler.Kind.FETCH, null, buffers -> {
            if (runs.incrementAndGet() == 1) {
                throw new IOException("transient");
            }
            return "read";
        });
        s.add(flaky);
        s.start(new JobScheduler.Position(0, NONE, NONE));

        s.run(flaky, null).run();

        assertThat(runs).hasValue(2);
        assertThat(applied).containsExactly("read");
        assertThat(s.inFlight()).isZero();
    }

    @Test
    void failedJobAppliesItsFailure() {
        JobScheduler s = scheduler(1000, 4, 1000);
        JobScheduler.Job<String> failing = job("", 10, 0, JobScheduler.Kind.CPU, null, buffers -> {
            throw new IllegalStateException("boom");
        });
        s.add(failing);
        s.start(new JobScheduler.Position(0, NONE, NONE));

        s.run(failing, null).run();

        assertThat(applied).containsExactly("failed: boom");
        assertThat(s.inFlight()).isZero();
    }

    @Test
    void theSchedulerReturnsAChargeItsLastReaderLetsGo() {
        ReadAheadBuffer r = new ReadAheadBuffer(1000);
        JobScheduler s = new JobScheduler(r, NOTHING, 4, 1000);
        Charge range = new Charge();
        range.reader();
        range.reader();
        s.add(job("f", 10, 100, JobScheduler.Kind.FETCH, range));
        s.start(new JobScheduler.Position(0, NONE, NONE));

        assertThat(range.readerDone()).isFalse();
        assertThat(range.readerDone()).isTrue();
        assertThat(s.takeReleased()).isZero();
        s.release(range);
        assertThat(s.takeReleased()).isEqualTo(100);
    }
}
