/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LazyTest {

    @Test
    void computesTheValueOnFirstGetOnly() {
        AtomicInteger calls = new AtomicInteger();
        Lazy<String> lazy = Lazy.of(() -> "value" + calls.incrementAndGet());

        assertThat(calls).hasValue(0);
        assertThat(lazy.get()).isEqualTo("value1");
        assertThat(lazy.get()).isEqualTo("value1");
        assertThat(calls).hasValue(1);
    }

    @Test
    void concurrentFirstCallsComputeTheValueOnce() throws InterruptedException {
        AtomicInteger calls = new AtomicInteger();
        Lazy<Object> lazy = Lazy.of(() -> {
            calls.incrementAndGet();
            return new Object();
        });
        CountDownLatch start = new CountDownLatch(1);
        List<Object> seen = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            threads.add(Thread.ofPlatform().start(() -> {
                try {
                    start.await();
                }
                catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
                Object value = lazy.get();
                synchronized (seen) {
                    seen.add(value);
                }
            }));
        }
        start.countDown();
        for (Thread thread : threads) {
            thread.join();
        }

        assertThat(calls).hasValue(1);
        assertThat(seen).hasSize(8).containsOnly(lazy.get());
    }

    @Test
    void refusesANullValue() {
        Lazy<String> lazy = Lazy.of(() -> null);

        assertThatThrownBy(lazy::get)
                .isInstanceOf(NullPointerException.class)
                .hasMessage("supplier returned null");
    }
}
