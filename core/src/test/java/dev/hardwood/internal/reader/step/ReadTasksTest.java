/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.step;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

class ReadTasksTest {

    /// A wait whose condition comes to hold without a signal, as where a read is abandoned with
    /// the heap exhausted and waking its waiting threads fails to allocate, ends all the same.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aWaitEndsWhenItsConditionHoldsWithoutASignal() throws InterruptedException {
        ReadTasks tasks = new ReadTasks();
        tasks.started();
        AtomicBoolean abandoned = new AtomicBoolean();
        Thread closer = Thread.ofPlatform().start(() -> tasks.await(() -> tasks.idle() || abandoned.get(), null));
        while (!(LockSupport.getBlocker(closer) instanceof ReadTasks)) {
            Thread.onSpinWait();
        }

        abandoned.set(true);

        closer.join();
        assertThat(tasks.idle()).isFalse();
    }
}
