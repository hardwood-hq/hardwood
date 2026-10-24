/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3.internal;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/// Runs the checks of an [IdleBoundSubscriber] after a delay.
@FunctionalInterface
public interface IdleTimer {

    /// Runs each check on the thread of the JDK's shared [CompletableFuture#delayedExecutor]
    /// scheduler once the delay has passed. A check is a flag test, and at most a cancel.
    IdleTimer DELAYED = (check, delay) ->
            CompletableFuture.delayedExecutor(delay.toNanos(), TimeUnit.NANOSECONDS, Runnable::run).execute(check);

    /// Runs `check` once, after `delay`.
    void schedule(Runnable check, Duration delay);
}
