/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.zip.CRC32;

import dev.hardwood.internal.compression.Compressor;

/// The page bodies one flush hands from a chunk's thread to whichever flush thread is idle, so a
/// chunk's compression spreads across threads once there are fewer chunks left than threads.
///
/// A chunk's thread cuts and builds its pages in order and writes them in order; only compressing
/// a body and taking its CRC, which depend on that body alone, move. A chunk hands a body off only
/// while more flush threads wait for work than there are bodies queued ([#tryReserve()]), so a
/// flush whose threads all have chunks to encode compresses every page where it is built, and the
/// bodies in flight are bounded by the flush's threads: each is queued, which an idle thread
/// accounts for, or being compressed by one.
///
/// A body is claimed exactly once, by an idle worker or by its own chunk's thread, which takes back
/// a body still unclaimed when the chunk ends, so a chunk never waits for a compression to start,
/// only for one another thread has claimed. An idle worker
/// stays until the flush's last chunk is done, so a flush under a codec holds its workers' pool
/// threads for its whole length.
final class PageCompression {

    private final ConcurrentLinkedQueue<Page> queue = new ConcurrentLinkedQueue<>();
    /// Bodies handed off and not yet claimed.
    private final AtomicInteger queued = new AtomicInteger();
    /// Flush threads waiting for work.
    private final AtomicInteger idle = new AtomicInteger();
    /// Idle workers parked until a body is offered or the flush is over.
    private final ConcurrentLinkedQueue<Thread> parked = new ConcurrentLinkedQueue<>();

    /// Reserves a place for the next body a chunk hands off, where an idle worker has nothing
    /// queued for it; `false` where the chunk should compress the body itself. Reserved
    /// atomically, so chunks racing for one idle worker queue one body, not one each.
    boolean tryReserve() {
        for (int q = queued.get(); idle.get() > q; q = queued.get()) {
            if (queued.compareAndSet(q, q + 1)) {
                return true;
            }
        }
        return false;
    }

    /// Hands `length` bytes of `body`, which the caller no longer touches, to an idle worker, in
    /// the place [#tryReserve()] reserved.
    ///
    /// @param levels the bytes stored uncompressed ahead of the body, which its CRC covers too
    /// @param rawIfNotSmaller whether the body is stored as it is where compressing it does not
    ///        shrink it
    Page offer(Compressor compressor, byte[] levels, byte[] body, int length, boolean rawIfNotSmaller) {
        Page page = new Page(compressor, levels, body, 0, length, rawIfNotSmaller);
        queue.add(page);
        Thread worker = parked.poll();
        if (worker != null) {
            LockSupport.unpark(worker);
        }
        return page;
    }

    /// Wakes every parked worker, for the flush has nothing left to hand off.
    void wakeAll() {
        for (Thread worker = parked.poll(); worker != null; worker = parked.poll()) {
            LockSupport.unpark(worker);
        }
    }

    /// Compresses `length` bytes of `body` from `offset` on the calling thread, as a page that is
    /// never queued.
    ///
    /// @param levels the bytes stored uncompressed ahead of the body, which its CRC covers too
    /// @param rawIfNotSmaller whether the body is stored as it is where compressing it does not
    ///        shrink it
    Page compressHere(Compressor compressor, byte[] levels, byte[] body, int offset, int length,
                      boolean rawIfNotSmaller) {
        Page page = new Page(compressor, levels, body, offset, length, rawIfNotSmaller);
        page.claimed.set(true);
        page.run();
        return page;
    }

    /// Compresses queued bodies on the calling thread, counted as idle while it waits for one,
    /// until `done` holds, parked between bodies; whoever makes `done` hold calls [#wakeAll()].
    /// A worker stays while any chunk is still being encoded, however long a
    /// body takes to come: a chunk compresses its page itself when no worker is idle at that
    /// moment, so the next body can be a page's compression away, which no fixed wait bounds
    /// across codecs.
    void help(BooleanSupplier done) {
        idle.incrementAndGet();
        try {
            while (!done.getAsBoolean()) {
                Page page = queue.poll();
                if (page == null) {
                    Thread self = Thread.currentThread();
                    parked.add(self);
                    // Checked after registering, so a body offered or a flush ended before it is
                    // seen here, and one after it wakes this thread.
                    if (queue.isEmpty() && !done.getAsBoolean()) {
                        LockSupport.park(this);
                    }
                    parked.remove(self);
                }
                else if (page.claim()) {
                    idle.decrementAndGet();
                    try {
                        page.run();
                    }
                    finally {
                        idle.incrementAndGet();
                    }
                }
            }
        }
        finally {
            idle.decrementAndGet();
        }
    }

    /// One page body handed off, and once compressed, what is written for it: the levels a
    /// `DATA_PAGE_V2` stores uncompressed (empty for a `DATA_PAGE`, whose levels are part of the
    /// body), then the body, compressed or, for a page that may store it raw and where compressing
    /// does not shrink it, as it is.
    final class Page {

        private final Compressor compressor;
        private final AtomicBoolean claimed = new AtomicBoolean();
        private final byte[] levels;
        private byte[] body;
        private final int offset;
        private final int length;
        private final boolean rawIfNotSmaller;
        /// Set once the body is compressed or has failed; the fields below are read after it.
        private volatile boolean done;
        private volatile Thread waiter;
        private byte[] stored;
        private boolean compressed;
        private int crc;
        private Throwable failure;

        private Page(Compressor compressor, byte[] levels, byte[] body, int offset, int length,
                     boolean rawIfNotSmaller) {
            this.compressor = compressor;
            this.levels = levels;
            this.body = body;
            this.offset = offset;
            this.length = length;
            this.rawIfNotSmaller = rawIfNotSmaller;
        }

        /// Takes the body for the calling thread to compress; `false` where another thread has.
        boolean claim() {
            if (claimed.compareAndSet(false, true)) {
                queued.decrementAndGet();
                return true;
            }
            return false;
        }

        /// Compresses the claimed body and takes the CRC of what is stored, the levels ahead of
        /// it included, recording a failure for the chunk to raise.
        void run() {
            try {
                byte[] out = compressor.compress(body, offset, length);
                compressed = !rawIfNotSmaller || out.length < length;
                // Copied: the body may be the chunk's buffer, which its next page overwrites.
                stored = compressed ? out : Arrays.copyOfRange(body, offset, offset + length);
                CRC32 checksum = new CRC32();
                checksum.update(levels, 0, levels.length);
                checksum.update(stored, 0, stored.length);
                crc = (int) checksum.getValue();
            }
            catch (Throwable t) {
                failure = t;
            }
            finally {
                body = null;
                done = true;
                Thread w = waiter;
                if (w != null) {
                    LockSupport.unpark(w);
                }
            }
        }

        boolean isDone() {
            return done;
        }

        /// Waits for the thread that claimed the body to finish it.
        void await() {
            waiter = Thread.currentThread();
            while (!done) {
                LockSupport.park(this);
            }
        }

        /// The levels stored uncompressed ahead of [#stored()].
        byte[] levels() {
            return levels;
        }

        byte[] stored() {
            return stored;
        }

        /// Whether [#stored()] is the body compressed, rather than the body as it is.
        boolean compressed() {
            return compressed;
        }

        int crc() {
            return crc;
        }

        /// The failure compressing the body raised, or `null`.
        Throwable failure() {
            return failure;
        }
    }
}
