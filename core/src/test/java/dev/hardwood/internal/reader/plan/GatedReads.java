/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import dev.hardwood.InputFile;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.ObservedPool;

/// Files whose bytes are in memory but which a read treats as remote, reads held at a gate by
/// their [FetchReason], and the wait until a read can make no progress before its gate opens.
final class GatedReads {

    private GatedReads() {
    }

    /// Waits until the read can make no progress before `gate` releases what it holds: every I/O
    /// task has finished or is held at the gate, and every thread of the pool waits for its next
    /// task, with no I/O task finishing meanwhile, whose end could have queued work.
    static void awaitSettled(QuiescentExecutor io, ReadGate gate, ObservedPool pool)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (true) {
            long finished = io.finishedCount();
            if (io.running() == gate.inFlight()) {
                pool.awaitIdle("A thread of the pool is busy while the read waits at the gate");
                if (io.running() == gate.inFlight() && io.finishedCount() == finished) {
                    return;
                }
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("The read did not settle: " + io.running() + " I/O tasks running, "
                        + gate.inFlight() + " of them held");
            }
            Thread.sleep(1);
        }
    }

    /// Holds back every read whose [FetchReason] it holds once armed until released, and counts how
    /// many are in flight.
    static final class ReadGate {

        private final Predicate<String> holds;
        private volatile CountDownLatch released = new CountDownLatch(1);
        private final AtomicInteger inFlight = new AtomicInteger();
        private final AtomicInteger maxInFlight = new AtomicInteger();
        private volatile boolean armed;

        ReadGate(Predicate<String> holds) {
            this.holds = holds;
        }

        /// Holds the reads from now on, until the next [#release].
        void arm() {
            released = new CountDownLatch(1);
            armed = true;
        }

        void release() {
            released.countDown();
        }

        int maxInFlight() {
            return maxInFlight.get();
        }

        int inFlight() {
            return inFlight.get();
        }

        /// Waits at most five seconds for `count` held reads to be in flight at once.
        boolean awaitInFlight(int count) throws InterruptedException {
            for (int i = 0; i < 5_000; i++) {
                if (inFlight.get() >= count) {
                    return true;
                }
                Thread.sleep(1);
            }
            return false;
        }

        void pass() throws IOException {
            if (!armed || !holds.test(FetchReason.current())) {
                return;
            }
            CountDownLatch latch = released;
            maxInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
            try {
                if (!latch.await(30, TimeUnit.SECONDS)) {
                    throw new IOException("Read gate never released");
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
            finally {
                inFlight.decrementAndGet();
            }
        }
    }

    /// A file of bytes in memory that the reader treats as remote, so that it reads ahead.
    static class RemoteLike implements InputFile {

        private final InputFile delegate;

        RemoteLike(InputFile delegate) {
            this.delegate = delegate;
        }

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            return delegate.readRange(offset, length);
        }

        @Override
        public long length() throws IOException {
            return delegate.length();
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    /// A remote-like file whose reads pass through a [ReadGate].
    static final class GatedFile extends RemoteLike {

        private final ReadGate gate;

        GatedFile(InputFile delegate, ReadGate gate) {
            super(delegate);
            this.gate = gate;
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            gate.pass();
            return super.readRange(offset, length);
        }
    }
}
