/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/// The close of a read, which a reader and its parent can run on two threads at once. One thread
/// claims the close and runs it; every other closer waits until that close has completed, so no
/// close returns while the read is still being torn down.
public final class CloseClaim {

    private final AtomicBoolean claimed = new AtomicBoolean();
    private final CountDownLatch completed = new CountDownLatch(1);

    /// Claims the close for the calling thread. A thread that does not get the claim waits until
    /// the claimed close has completed, carrying on through interrupts and restoring them.
    ///
    /// @return whether the calling thread runs the close, and must call [#completed()] once it has
    public boolean claim() {
        if (claimed.compareAndSet(false, true)) {
            return true;
        }
        boolean interrupted = false;
        while (true) {
            try {
                completed.await();
                break;
            }
            catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        return false;
    }

    /// Whether a thread has claimed the close.
    public boolean isClaimed() {
        return claimed.get();
    }

    /// Ends the claimed close and releases every closer waiting for it.
    public void completed() {
        completed.countDown();
    }
}
