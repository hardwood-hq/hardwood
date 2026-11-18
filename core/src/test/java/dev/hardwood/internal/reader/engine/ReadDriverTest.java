/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.reader.step.ReadQueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Drives a [ReadDriver] on the test's thread alone: a context of zero threads, whose CPU jobs wait
/// on the read's queue, and fetches queued for the test to run.
class ReadDriverTest {

    private static final JobScheduler.Position ANYWHERE = new JobScheduler.Position(0, Long.MAX_VALUE,
            Long.MAX_VALUE);

    private final List<String> applied = new ArrayList<>();
    private final ArrayDeque<Runnable> fetches = new ArrayDeque<>();
    private final HardwoodContextImpl context = HardwoodContextImpl.withIoExecutor(0, fetches::add);
    private final ReadQueue queue = new ReadQueue(context);
    private final ReadDriver driver = new ReadDriver(context, queue, Long.MAX_VALUE, () -> ANYWHERE,
            t -> applied.add("broken: " + t), t -> applied.add("abandoned: " + t));

    @AfterEach
    void closeContext() {
        context.close();
    }

    private JobScheduler.Job<String> job(String name, long key, long cost, JobScheduler.Kind kind, Charge charge,
                                         Runnable then) {
        return new JobScheduler.Job<>(key, cost, kind, charge, buffers -> name, value -> {
            applied.add(value);
            then.run();
        }, t -> applied.add(name + " failed: " + t.getMessage()));
    }

    @Test
    void appliesAResultAndStartsWhatItMadePossible() {
        JobScheduler s = driver.scheduler();
        JobScheduler.Job<?> decode = job("decode", 10, 0, JobScheduler.Kind.DECODE, null, () -> {
        });
        JobScheduler.Job<?> fetch = job("fetch", 10, 0, JobScheduler.Kind.FETCH, null, () -> s.add(decode));

        driver.update(() -> s.add(fetch));
        assertThat(fetches).hasSize(1);
        assertThat(queue.runOne()).isFalse();

        fetches.poll().run();
        assertThat(applied).containsExactly("fetch");
        assertThat(queue.runOne()).isTrue();
        assertThat(applied).containsExactly("fetch", "decode");

        driver.awaitIdle();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aPollThatKeepsThrowingLeavesTheLockFree() throws InterruptedException {
        ReadDriver throwing = new ReadDriver(context, queue, Long.MAX_VALUE, () -> {
            throw new IllegalStateException("poll");
        }, t -> applied.add("broken: " + t.getMessage()), t -> applied.add("abandoned: " + t.getMessage()));

        assertThatThrownBy(() -> throwing.update(() -> applied.add("result")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("poll");

        assertThat(applied).containsExactly("result", "broken: poll");
        // a close on another thread takes the lock, which the throw let go of
        Thread closer = Thread.ofPlatform().start(throwing::close);
        closer.join();
    }

    /// A job's end whose application throws, as an `OutOfMemoryError` does where reporting it
    /// allocates with the heap exhausted, abandons the read on the job's thread: the throw stays
    /// there, and a close does not wait for the job whose end was never applied.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aJobWhoseEndThrowsAbandonsTheRead() {
        boolean[] pollThrows = { false };
        ReadDriver throwing = new ReadDriver(context, queue, Long.MAX_VALUE, () -> {
            if (pollThrows[0]) {
                throw new OutOfMemoryError("poll");
            }
            return ANYWHERE;
        }, t -> applied.add("broken: " + t.getMessage()), t -> applied.add("abandoned: " + t.getMessage()));
        JobScheduler s = throwing.scheduler();
        throwing.update(() -> s.add(job("fetch", 10, 0, JobScheduler.Kind.FETCH, null, () -> {
        })));
        pollThrows[0] = true;

        fetches.poll().run();

        assertThat(applied).containsExactly("fetch", "broken: poll", "abandoned: poll");
        throwing.close();
    }

    /// A job's end whose application dispatches a further job, and the dispatch throws: the read
    /// is abandoned, and its close returns, though the further job counts as started and never
    /// ends.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aDispatchThatThrowsAbandonsTheReadAndItsCloseReturns() {
        ArrayDeque<Runnable> dispatched = new ArrayDeque<>();
        int[] executed = { 0 };
        try (HardwoodContextImpl failing = HardwoodContextImpl.withIoExecutor(0, run -> {
            if (++executed[0] > 1) {
                throw new OutOfMemoryError("dispatch");
            }
            dispatched.add(run);
        })) {
            ReadDriver read = new ReadDriver(failing, new ReadQueue(failing), Long.MAX_VALUE, () -> ANYWHERE,
                    t -> applied.add("broken: " + t.getMessage()), t -> applied.add("abandoned: " + t.getMessage()));
            JobScheduler s = read.scheduler();
            JobScheduler.Job<?> next = job("next", 20, 0, JobScheduler.Kind.FETCH, null, () -> {
            });
            read.update(() -> s.add(job("fetch", 10, 0, JobScheduler.Kind.FETCH, null, () -> s.add(next))));

            dispatched.poll().run();

            assertThat(applied).containsExactly("fetch", "abandoned: dispatch");
            read.close();
        }
    }

    /// A read woken for room by another read's release, whose bookkeeping then throws on that
    /// read's thread, is abandoned there, and the read whose release woke it goes on: one read's
    /// failure never reaches another.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aReadWokenByAnotherReadsReleaseFailsAloneWhereItThrows() {
        boolean[] pollThrows = { false };
        ReadDriver waiting = new ReadDriver(context, queue, Long.MAX_VALUE, () -> {
            if (pollThrows[0]) {
                throw new OutOfMemoryError("poll");
            }
            return ANYWHERE;
        }, t -> applied.add("waiting broken: " + t.getMessage()),
                t -> applied.add("waiting abandoned: " + t.getMessage()));
        // a fetch ahead larger than R waits for R's next release
        JobScheduler.Job<?> large = job("large", 10, Long.MAX_VALUE / 2, JobScheduler.Kind.FETCH, new Charge(), () -> {
        });
        waiting.update(() -> waiting.scheduler().add(large));
        pollThrows[0] = true;
        JobScheduler s = driver.scheduler();
        JobScheduler.Job<?> charged = job("charged", 10, 1, JobScheduler.Kind.CPU, new Charge(), () -> {
        });
        driver.update(() -> s.add(charged));

        // its end releases R, which wakes the waiting read on this read's job
        assertThat(queue.runOne()).isTrue();

        assertThat(applied).containsExactly("charged", "waiting broken: poll", "waiting abandoned: poll");
        driver.awaitIdle();
        waiting.close();
    }

    /// A result applied on a thread that runs none of the read's jobs, the consumer's taking a
    /// batch, whose application throws: the read is abandoned rather than the throw leaving it with
    /// counts that never balance, and its close returns.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aResultAppliedOnTheConsumersThreadThatThrowsAbandonsTheRead() {
        boolean[] pollThrows = { false };
        ReadDriver read = new ReadDriver(context, queue, Long.MAX_VALUE, () -> {
            if (pollThrows[0]) {
                throw new OutOfMemoryError("poll");
            }
            return ANYWHERE;
        }, t -> applied.add("broken: " + t.getMessage()), t -> applied.add("abandoned: " + t.getMessage()));
        JobScheduler s = read.scheduler();
        // a fetch dispatched and never run, so the read's counts do not balance
        read.update(() -> s.add(job("fetch", 10, 0, JobScheduler.Kind.FETCH, null, () -> {
        })));
        pollThrows[0] = true;

        read.updateOrAbandon(() -> applied.add("room"));

        assertThat(applied).containsExactly("room", "broken: poll", "abandoned: poll");
        read.close();
    }

    /// A result that throws fails the read with what it threw, classified as every failure of a
    /// read is: a file's check met while a result is applied reaches the reader as a
    /// `ParquetReadException`, not raw.
    @Test
    void aResultThatThrowsFailsTheReadWithTheClassifiedFailure() {
        driver.update(() -> {
            throw new ArithmeticException("integer overflow");
        });

        assertThat(applied).containsExactly(
                "broken: dev.hardwood.reader.ParquetReadException: integer overflow");
    }

    @Test
    void holderAppliesAResultQueuedWhileItHoldsTheLock() {
        driver.locked(() -> {
            driver.update(() -> applied.add("queued"));
            applied.add("held");
        });

        assertThat(applied).containsExactly("held", "queued");
    }

    @Test
    void closeRunsWhatIsInFlightAndReturnsEveryCharge() {
        JobScheduler s = driver.scheduler();
        Charge never = new Charge();
        never.reader();
        driver.update(() -> s.add(job("decode", 10, 100, JobScheduler.Kind.DECODE, never, () -> {
        })));
        assertThat(context.readAheadBuffer().held()).isEqualTo(100);

        driver.close();

        assertThat(applied).containsExactly("decode");
        assertThat(context.readAheadBuffer().held()).isZero();
    }

    @Test
    void closeStartsNothingFurther() {
        JobScheduler s = driver.scheduler();
        driver.close();

        driver.update(() -> s.add(job("decode", 10, 0, JobScheduler.Kind.DECODE, null, () -> {
        })));

        assertThat(queue.runOne()).isFalse();
        assertThat(applied).isEmpty();
    }

    @Test
    void jobAClosedContextRefusesEndsWithItsFailure() {
        JobScheduler s = driver.scheduler();
        context.close();

        driver.update(() -> s.add(job("decode", 10, 0, JobScheduler.Kind.DECODE, null, () -> {
        })));

        assertThat(applied).containsExactly(
                "decode failed: The HardwoodContext was closed while a reader using it was reading");
        driver.awaitIdle();
    }
}
