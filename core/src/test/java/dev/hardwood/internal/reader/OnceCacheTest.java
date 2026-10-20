/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// [OnceCache] computes a value once per key, as `ConcurrentHashMap.computeIfAbsent` does, with no
/// lock held while it computes.
class OnceCacheTest {

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aCallerArrivingDuringTheComputationWaitsForItsValue() throws Exception {
        OnceCache<String> cache = new OnceCache<>();
        CountDownLatch computing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger computations = new AtomicInteger();
        Thread first = Thread.ofVirtual().start(() -> cache.computeIfAbsent(1, key -> {
            computations.incrementAndGet();
            computing.countDown();
            await(release);
            return "value";
        }));
        computing.await();
        AtomicReference<String> waited = new AtomicReference<>();
        Thread second = Thread.ofVirtual().start(() -> waited.set(cache.computeIfAbsent(1, key -> {
            computations.incrementAndGet();
            return "other";
        })));
        while (second.getState() != Thread.State.WAITING) {
            Thread.onSpinWait();
        }
        assertThat(cache.get(1)).isNull();

        release.countDown();
        first.join();
        second.join();

        assertThat(waited.get()).isEqualTo("value");
        assertThat(computations).hasValue(1);
        assertThat(cache.get(1)).isEqualTo("value");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aCallerThatWaitedForAFailedComputationComputesInTurn() throws Exception {
        OnceCache<String> cache = new OnceCache<>();
        CountDownLatch computing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        IllegalStateException failure = new IllegalStateException("planning failed");
        AtomicReference<Throwable> firstOutcome = new AtomicReference<>();
        Thread first = Thread.ofVirtual().start(() -> {
            try {
                cache.computeIfAbsent(1, key -> {
                    computing.countDown();
                    await(release);
                    throw failure;
                });
            }
            catch (IllegalStateException e) {
                firstOutcome.set(e);
            }
        });
        computing.await();
        AtomicReference<String> waited = new AtomicReference<>();
        Thread second = Thread.ofVirtual().start(() -> waited.set(cache.computeIfAbsent(1, key -> "retried")));
        while (second.getState() != Thread.State.WAITING) {
            Thread.onSpinWait();
        }

        release.countDown();
        first.join();
        second.join();

        assertThat(firstOutcome.get()).isSameAs(failure);
        assertThat(waited.get()).isEqualTo("retried");
        assertThat(cache.get(1)).isEqualTo("retried");
    }

    @Test
    void aNullValueIsNotCached() {
        OnceCache<String> cache = new OnceCache<>();

        assertThat(cache.computeIfAbsent(1, key -> null)).isNull();

        assertThat(cache.isEmpty()).isTrue();
        assertThat(cache.computeIfAbsent(1, key -> "value")).isEqualTo("value");
    }

    @Test
    void anEntryRemovedDuringItsComputationIsNotCached() {
        OnceCache<String> cache = new OnceCache<>();

        String value = cache.computeIfAbsent(1, key -> {
            cache.remove(key);
            return "value";
        });

        assertThat(value).isEqualTo("value");
        assertThat(cache.get(1)).isNull();
        assertThat(cache.isEmpty()).isTrue();
    }

    @Test
    void theThreadThatComputedAValueGetsItAgain() {
        OnceCache<String> cache = new OnceCache<>();
        AtomicInteger computations = new AtomicInteger();

        cache.computeIfAbsent(1, key -> "value" + computations.incrementAndGet());

        assertThat(cache.computeIfAbsent(1, key -> "value" + computations.incrementAndGet())).isEqualTo("value1");
        assertThat(computations).hasValue(1);
    }

    @Test
    void aComputationAskingForItsOwnKeyFails() {
        OnceCache<String> cache = new OnceCache<>();

        assertThatThrownBy(() -> cache.computeIfAbsent(1, key -> cache.computeIfAbsent(1, again -> "value")))
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessage("Recursive computation of entry 1");
        assertThat(cache.isEmpty()).isTrue();
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        }
        catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }
}
