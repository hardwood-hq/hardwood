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

        driver.submit(() -> s.add(fetch));
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

        assertThatThrownBy(() -> throwing.submit(() -> applied.add("result")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("poll");

        assertThat(applied).containsExactly("result", "broken: poll");
        // a close on another thread takes the lock, which the throw let go of
        Thread closer = Thread.ofPlatform().start(throwing::close);
        closer.join();
    }

    /// A job's end applied by a holder whose poll then keeps throwing still counts the job as
    /// ended: the throw reaches the holder's caller, and a close that follows returns.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void aJobEndedBeforeAPollThatKeepsThrowingStillCountsAsEnded() {
        boolean[] pollThrows = { false };
        ReadDriver throwing = new ReadDriver(context, queue, Long.MAX_VALUE, () -> {
            if (pollThrows[0]) {
                throw new IllegalStateException("poll");
            }
            return ANYWHERE;
        }, t -> applied.add("broken: " + t.getMessage()), t -> applied.add("abandoned: " + t.getMessage()));
        JobScheduler s = throwing.scheduler();
        throwing.submit(() -> s.add(job("fetch", 10, 0, JobScheduler.Kind.FETCH, null, () -> {
        })));

        // the fetch ends while this thread holds the lock, so its end is applied as the lock is let go
        assertThatThrownBy(() -> throwing.runLocked(() -> {
            fetches.poll().run();
            pollThrows[0] = true;
        })).isInstanceOf(IllegalStateException.class).hasMessage("poll");

        assertThat(applied).containsExactly("fetch", "broken: poll");
        throwing.close();
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
        throwing.submit(() -> s.add(job("fetch", 10, 0, JobScheduler.Kind.FETCH, null, () -> {
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
            read.submit(() -> s.add(job("fetch", 10, 0, JobScheduler.Kind.FETCH, null, () -> s.add(next))));

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
        waiting.submit(() -> waiting.scheduler().add(large));
        pollThrows[0] = true;
        JobScheduler s = driver.scheduler();
        JobScheduler.Job<?> charged = job("charged", 10, 1, JobScheduler.Kind.CPU, new Charge(), () -> {
        });
        driver.submit(() -> s.add(charged));

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
        read.submit(() -> s.add(job("fetch", 10, 0, JobScheduler.Kind.FETCH, null, () -> {
        })));
        pollThrows[0] = true;

        read.submitOrAbandon(() -> applied.add("room"));

        assertThat(applied).containsExactly("room", "broken: poll", "abandoned: poll");
        read.close();
    }

    /// A read abandoned while its close waits for a job that never ends: the close returns.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void aReadAbandonedWhileItsCloseWaitsLetsTheCloseReturn() throws Exception {
        ReadDriver read = new ReadDriver(context, queue, Long.MAX_VALUE, () -> ANYWHERE, t -> {
            throw new OutOfMemoryError("report");
        }, t -> applied.add("abandoned: " + t.getMessage()));
        JobScheduler s = read.scheduler();
        // a fetch dispatched and never run, so the close waits for it
        read.submit(() -> s.add(job("fetch", 10, 0, JobScheduler.Kind.FETCH, null, () -> {
        })));
        Thread closer = Thread.ofPlatform().start(read::close);
        while (closer.getState() != Thread.State.TIMED_WAITING && closer.getState() != Thread.State.WAITING) {
            Thread.sleep(1);
        }

        read.submitOrAbandon(() -> {
            throw new IllegalStateException("result");
        });
        closer.join();

        assertThat(applied).containsExactly("abandoned: report");
    }

    /// A result that throws fails the read with what it threw, classified as every failure of a
    /// read is: a file's check met while a result is applied reaches the reader as a
    /// `ParquetReadException`, not raw.
    @Test
    void aResultThatThrowsFailsTheReadWithTheClassifiedFailure() {
        driver.submit(() -> {
            throw new ArithmeticException("integer overflow");
        });

        assertThat(applied).containsExactly(
                "broken: dev.hardwood.reader.ParquetReadException: integer overflow");
    }

    @Test
    void holderAppliesAResultQueuedWhileItHoldsTheLock() {
        driver.runLocked(() -> {
            driver.submit(() -> applied.add("queued"));
            applied.add("held");
        });

        assertThat(applied).containsExactly("held", "queued");
    }

    @Test
    void closeRunsWhatIsInFlightAndReturnsEveryCharge() {
        JobScheduler s = driver.scheduler();
        Charge never = new Charge();
        never.reader();
        driver.submit(() -> s.add(job("decode", 10, 100, JobScheduler.Kind.DECODE, never, () -> {
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

        driver.submit(() -> s.add(job("decode", 10, 0, JobScheduler.Kind.DECODE, null, () -> {
        })));

        assertThat(queue.runOne()).isFalse();
        assertThat(applied).isEmpty();
    }

    @Test
    void jobAClosedContextRefusesEndsWithItsFailure() {
        JobScheduler s = driver.scheduler();
        context.close();

        driver.submit(() -> s.add(job("decode", 10, 0, JobScheduler.Kind.DECODE, null, () -> {
        })));

        assertThat(applied).containsExactly(
                "decode failed: The HardwoodContext was closed while a reader using it was reading");
        driver.awaitIdle();
    }

    // ==================== Progress check ====================

    /// The consumer's position, which [#checked] reports.
    private JobScheduler.Position position = new JobScheduler.Position(10, 10, Long.MAX_VALUE);
    /// whether the consumer waits on the column the check is asked for
    private boolean consumerWaits = true;
    private boolean waitsOnOtherRead;

    /// A driver whose reconcile reports [#position], and which records what it was abandoned with.
    private ReadDriver checked() {
        return new ReadDriver(context, queue, Long.MAX_VALUE, () -> position,
                t -> applied.add("broken: " + t.getMessage()), t -> applied.add("abandoned: " + t.getMessage()));
    }

    private void checkProgress(ReadDriver d) {
        d.checkProgress("c", () -> consumerWaits, () -> waitsOnOtherRead);
    }

    private void addDecodeAt(ReadDriver d, long key) {
        JobScheduler s = d.scheduler();
        d.submit(() -> s.add(job("decode", key, 0, JobScheduler.Kind.DECODE, null, () -> {
        })));
    }

    @Test
    void aReadWhoseOnlyJobLiesPastTheLookAheadFailsNamingIt() {
        ReadDriver d = checked();
        addDecodeAt(d, 10);

        checkProgress(d);

        assertThat(applied).containsExactly("abandoned: The read of column 'c' stalled: no job is in flight, and the lowest "
                + "pending job, a decode job at row 10, lies past the look-ahead; the consumer needs the rows below 10, "
                + "and decodes reach row 10");
        assertThat(queue.runOne()).as("no job starts past the stall").isFalse();
    }

    @Test
    void aReadWithNoJobLeftFails() {
        ReadDriver d = checked();

        checkProgress(d);

        assertThat(applied).containsExactly("abandoned: The read of column 'c' stalled: no job is in flight or pending; "
                + "the consumer needs the rows below 10, and decodes reach row 10");
    }

    @Test
    void aReadWhoseOnlyJobLiesPastItsEndFailsNamingIt() {
        position = new JobScheduler.Position(10, 10, 5);
        ReadDriver d = checked();
        JobScheduler s = d.scheduler();
        d.submit(() -> s.add(job("plan", 7, 0, JobScheduler.Kind.PLAN, null, () -> {
        })));

        checkProgress(d);

        assertThat(applied).containsExactly("abandoned: The read of column 'c' stalled: no job is in flight, and the lowest "
                + "pending job, a plan job at row 7, lies past the read's end at row 5; the consumer needs the rows "
                + "below 10, and decodes reach row 10");
    }

    @Test
    void aReadWithAJobInFlightIsNotChecked() {
        ReadDriver d = checked();
        JobScheduler s = d.scheduler();
        d.submit(() -> s.add(job("fetch", 20, 0, JobScheduler.Kind.FETCH, null, () -> {
        })));
        assertThat(fetches).hasSize(1);

        checkProgress(d);

        assertThat(applied).isEmpty();
        fetches.poll().run();
        assertThat(applied).containsExactly("fetch");
    }

    @Test
    void aConsumerThatNoLongerWaitsIsNotChecked() {
        ReadDriver d = checked();
        consumerWaits = false;

        checkProgress(d);

        assertThat(applied).isEmpty();
    }

    /// A read whose charge did not fit into R waits for another read to release room, which wakes it.
    @Test
    void aReadWaitingForRoomInTheReadAheadBufferIsNotStalled() {
        ReadDriver d = checked();
        JobScheduler s = d.scheduler();
        d.submit(() -> s.add(job("fetch", 20, Long.MAX_VALUE / 2, JobScheduler.Kind.FETCH, new Charge(), () -> {
        })));
        assertThat(fetches).as("refused by R").isEmpty();

        checkProgress(d);

        assertThat(applied).isEmpty();
    }

    @Test
    void aReadWaitingForAnotherReadsFooterLoadIsNotStalled() {
        ReadDriver d = checked();
        waitsOnOtherRead = true;

        checkProgress(d);

        assertThat(applied).isEmpty();
    }

    /// A job the position lets start that no event tried is a lost wake-up, which fails the read
    /// rather than being started by the check.
    @Test
    void aJobNoEventStartedFailsTheRead() {
        ReadDriver d = checked();
        addDecodeAt(d, 10);
        // the look-ahead moves on without an event that polls the scheduler
        position = new JobScheduler.Position(10, 20, Long.MAX_VALUE);

        checkProgress(d);

        assertThat(applied).containsExactly("abandoned: The read of column 'c' missed a wake-up: no job was in flight, "
                + "and a decode job at row 10 could start");
        assertThat(queue.runOne()).as("the job is not dispatched").isFalse();
    }

    /// The job the check started is never dispatched, and the close of the abandoned read returns
    /// what it was charged.
    @Test
    void theCloseOfAReadThatMissedAWakeUpReturnsEveryCharge() {
        ReadDriver d = checked();
        JobScheduler s = d.scheduler();
        d.submit(() -> s.add(job("decode", 10, 100, JobScheduler.Kind.DECODE, new Charge(), () -> {
        })));
        position = new JobScheduler.Position(0, 20, Long.MAX_VALUE);

        checkProgress(d);
        assertThat(context.readAheadBuffer().held()).isEqualTo(100);
        d.close();

        assertThat(applied).containsExactly("abandoned: The read of column 'c' missed a wake-up: no job was in flight, "
                + "and a decode job at row 10 could start");
        assertThat(context.readAheadBuffer().held()).isZero();
    }

    /// A reconcile that throws during the check, as one does where the heap is exhausted, leaves the
    /// read half changed: the read is abandoned with the throw, and its close does not wait for it.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aCheckThatThrowsAbandonsTheReadWithTheThrow() {
        ReadDriver d = new ReadDriver(context, queue, Long.MAX_VALUE, () -> {
            throw new OutOfMemoryError("reconcile");
        }, t -> applied.add("broken: " + t.getMessage()), t -> applied.add("abandoned: " + t.getMessage()));

        checkProgress(d);
        d.close();

        assertThat(applied).containsExactly("abandoned: reconcile");
    }

    @Test
    void aBatchOnlyTheCheckPublishesFailsTheRead() {
        ReadDriver d = new ReadDriver(context, queue, Long.MAX_VALUE, () -> {
            consumerWaits = false;
            return position;
        }, t -> applied.add("broken: " + t.getMessage()), t -> applied.add("abandoned: " + t.getMessage()));
        consumerWaits = true;

        checkProgress(d);

        assertThat(applied).containsExactly("abandoned: The read of column 'c' missed a wake-up: no job was in flight, "
                + "and its next batch was publishable");
    }
}
