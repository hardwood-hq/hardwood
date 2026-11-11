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
            t -> applied.add("broken: " + t));

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
        }, t -> applied.add("broken: " + t.getMessage()));

        assertThatThrownBy(() -> throwing.update(() -> applied.add("result")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("poll");

        assertThat(applied).containsExactly("result", "broken: poll");
        // a close on another thread takes the lock, which the throw let go of
        Thread closer = Thread.ofPlatform().start(throwing::close);
        closer.join();
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
