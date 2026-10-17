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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.LockSupport;

/// Appends one slice of records to every column chunk of a row group, on worker threads and the
/// caller's alike, and returns once every column has taken it.
///
/// Within a slice the columns are independent: each reads its own source and the shredder bound
/// to the batch, and writes only its own chunk. Columns are claimed exactly once, in schema
/// order, from a shared counter, and the caller claims too, so a slice never waits for a worker
/// task to start, only for a column a worker has claimed to finish. A pool busy with other work
/// slows the append down to the caller's own pace without stalling it.
///
/// A slice is a few microseconds of work per column, about what it takes to wake a parked pool
/// thread, so a worker does not end with its slice: it waits for the next one, and the caller
/// waits for the columns workers hold, each spinning for as long as waking a parked thread has
/// been taking ([WakeCost]) and then parked. Spinning for the cost of a wake-up keeps a wait within
/// twice the cheaper of spinning throughout and parking at once, whatever wake-ups cost where the
/// writer runs: little on bare metal, much more on a virtual machine. The wait is bounded, so a worker never holds a pool thread for longer: one that sees no
/// slice in that time leaves the pool thread, and the caller starts workers again at the next
/// slice. Since no worker waits indefinitely, a caller whose next batch comes from a reader on the
/// same pool cannot deadlock against its own workers. A worker that has served slices for a
/// millisecond goes to the back of the pool's queue, so tasks other users of a shared pool queued
/// meanwhile run before it. [#retire()] sends waiting workers away at once, before a flush needs
/// the pool.
///
/// On a failure, claiming stops, every column already claimed is waited for (also where starting
/// a worker fails, since workers from the previous slice may be appending), so no thread still
/// appends to the row group when the exception reaches the writer, and the first failure in
/// schema order is rethrown as it was raised. Columns are claimed in schema order, so every
/// column before a failing one has been claimed and runs to the end: the failure reported is the
/// one a serial append would raise.
public final class ParallelAppend {

    /// How long a worker waits for the next slice before leaving its pool thread, spinning then
    /// parked. Bounds how long a pool thread is held for a writer that stopped writing.
    private static final long LINGER_NANOS = 1_000_000;

    /// How long a worker serves slices before making way for other tasks queued on the pool.
    private static final long TENURE_NANOS = 1_000_000;

    /// How long waking a parked thread takes, from the `unpark` to the woken thread running, as
    /// the writers in this JVM have observed it: what a wait spins for before it parks. A property
    /// of the machine rather than of one writer, so it is shared. Starts at nothing, so the first
    /// waits park and measure.
    ///
    /// The estimate is the median of the last few samples, so a wake-up that met a pause, such as
    /// a collection or a descheduled virtual CPU, does not set it. A spin takes at most half of a
    /// wait, so where wake-ups are slow a wait still parks, and samples, and the estimate follows
    /// the machine down as well as up.
    static final class WakeCost {

        private static final int SAMPLES = 8;

        private static final AtomicLongArray samples = new AtomicLongArray(SAMPLES);
        private static final AtomicInteger nextSample = new AtomicInteger();
        private static volatile long nanos;

        private WakeCost() {
        }

        /// How long a wait that may last up to `wait` spins before parking.
        static long spin(long wait) {
            return Math.min(nanos, wait / 2);
        }

        /// Takes the time from `unparkedAt`, when the `unpark` that woke the calling thread was
        /// issued, to now as a sample; `0` where the waker has not stamped it yet, which says
        /// nothing.
        static void sample(long unparkedAt) {
            if (unparkedAt == 0) {
                return;
            }
            long sample = System.nanoTime() - unparkedAt;
            if (sample <= 0) {
                return;
            }
            samples.set(Math.floorMod(nextSample.getAndIncrement(), SAMPLES), sample);
            long[] sorted = new long[SAMPLES];
            for (int i = 0; i < SAMPLES; i++) {
                sorted[i] = samples.get(i);
            }
            Arrays.sort(sorted);
            nanos = sorted[SAMPLES / 2];
        }
    }

    /// Appends one column's share of the current slice.
    @FunctionalInterface
    public interface ColumnAppend {

        void append(int column);
    }

    private final int columns;
    private final ExecutorService executor;
    private final int workers;
    /// Workers started and not yet left, including those still queued on the pool.
    private final AtomicInteger running = new AtomicInteger();
    /// The slice workers claim from; `null` once retired.
    private volatile Slice slice;
    /// Workers parked waiting for the next slice, which publishing one wakes.
    private final ConcurrentLinkedQueue<Thread> parked = new ConcurrentLinkedQueue<>();

    /// @param columns the column chunks a slice is appended to
    /// @param executor runs the worker tasks
    /// @param workers the most workers to keep beside the caller
    public ParallelAppend(int columns, ExecutorService executor, int workers) {
        this.columns = columns;
        this.executor = executor;
        this.workers = workers;
    }

    /// Runs `task` for every column, on the workers and the calling thread, and returns once
    /// every column has run.
    ///
    /// @throws RuntimeException or [Error] the first failure in schema order, as raised
    public void append(ColumnAppend task) {
        Slice current = new Slice(task, Thread.currentThread());
        publish(current);
        Throwable startFailure = null;
        try {
            startWorkers();
        }
        catch (Throwable t) {
            // Workers waiting from the previous slice may already be appending this one, so the
            // caller takes the rest and waits for them before the failure leaves.
            startFailure = t;
        }
        current.claim();
        current.await();
        try {
            current.rethrowFirstFailure();
        }
        catch (Throwable t) {
            if (startFailure != null) {
                t.addSuppressed(startFailure);
            }
            throw t;
        }
        if (startFailure != null) {
            rethrow(startFailure);
        }
    }

    /// Starts workers until [#workers] are running.
    private void startWorkers() {
        for (int started = running.get(); started < workers; started = running.get()) {
            if (running.compareAndSet(started, started + 1)) {
                startWorker();
            }
        }
    }

    /// Sends waiting workers away: each releases its pool thread at once.
    public void retire() {
        publish(null);
    }

    /// Hands workers `next`, waking those parked for it.
    private void publish(Slice next) {
        if (next != null) {
            next.publishedAt = System.nanoTime();
        }
        slice = next;
        for (Thread worker = parked.poll(); worker != null; worker = parked.poll()) {
            LockSupport.unpark(worker);
        }
    }

    private void startWorker() {
        try {
            executor.execute(this::work);
        }
        catch (Throwable t) {
            running.decrementAndGet();
            throw t;
        }
    }

    /// A worker task: claims columns from each slice it sees, until it is retired or no slice
    /// follows the last one within [#LINGER_NANOS]. One that has served [#TENURE_NANOS] goes to
    /// the back of the pool's queue at the end of a slice, still counted as running, so that tasks
    /// queued by other users of the pool run before it.
    private void work() {
        boolean requeued = false;
        try {
            long start = System.nanoTime();
            Slice taken = null;
            for (Slice next = slice; next != null; next = awaitNext(taken)) {
                next.claim();
                taken = next;
                if (System.nanoTime() - start >= TENURE_NANOS) {
                    requeued = requeue();
                    return;
                }
            }
        }
        finally {
            if (!requeued) {
                running.decrementAndGet();
            }
        }
    }

    /// Queues this worker again; `false` where the pool takes no more tasks.
    private boolean requeue() {
        try {
            executor.execute(this::work);
            return true;
        }
        catch (RejectedExecutionException e) {
            return false;
        }
    }

    /// The slice after `taken`, or `null` where none comes within [#LINGER_NANOS] or the workers
    /// are retired: spinning for the [WakeCost], then parked until a slice is published or the
    /// wait runs out.
    ///
    /// A worker leaving just as a slice is published is still counted by the caller starting
    /// workers for it, so that slice may run with one worker fewer; the next one starts it again.
    private Slice awaitNext(Slice taken) {
        long start = System.nanoTime();
        long spin = WakeCost.spin(LINGER_NANOS);
        Slice next = slice;
        while (next == taken && System.nanoTime() - start < spin) {
            Thread.onSpinWait();
            next = slice;
        }
        if (next != taken) {
            return next;
        }
        Thread self = Thread.currentThread();
        parked.add(self);
        try {
            boolean parked = false;
            // Read after registering, so a slice published before it is seen here and one
            // published after it wakes this thread.
            for (next = slice; next == taken; next = slice) {
                long left = LINGER_NANOS - (System.nanoTime() - start);
                if (left <= 0) {
                    return null;
                }
                LockSupport.parkNanos(this, left);
                parked = true;
            }
            if (parked && next != null) {
                WakeCost.sample(next.publishedAt);
            }
            return next;
        }
        finally {
            parked.remove(self);
        }
    }

    private static void rethrow(Throwable failure) {
        if (failure instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        throw new IllegalStateException("A column append failed", failure);
    }

    /// One slice: the column counter, the count of columns finished, and each column's failure.
    private final class Slice {

        private final ColumnAppend task;
        private final Thread caller;
        /// The next column to claim; any value from `columns` up means none is left.
        private final AtomicInteger next = new AtomicInteger();
        /// The columns claimed before claiming stopped: `columns` until a failure stops it.
        private final AtomicInteger claimed;
        private final AtomicInteger finished = new AtomicInteger();
        /// When the last claimed column finished, so the caller it woke can tell what waking took.
        private volatile long finishedAt;
        /// When the slice was published, so a worker it woke can tell what waking took.
        private volatile long publishedAt;
        /// Written before the column counts as finished, read once every claimed one has.
        private final Throwable[] failures;

        Slice(ColumnAppend task, Thread caller) {
            this.task = task;
            this.caller = caller;
            this.claimed = new AtomicInteger(columns);
            this.failures = new Throwable[columns];
        }

        /// Claims and appends columns until none is left or claiming has stopped.
        void claim() {
            for (int c = next.getAndIncrement(); c < columns; c = next.getAndIncrement()) {
                try {
                    task.append(c);
                }
                catch (Throwable t) {
                    failures[c] = t;
                    // A claim either precedes the `getAndSet` and returns an index below what it
                    // returns, or follows it and returns one past the last column, so the
                    // columns below it are exactly those claimed.
                    claimed.accumulateAndGet(next.getAndSet(columns), Math::min);
                }
                finally {
                    // A failing thread lowers `claimed` before counting itself finished, so
                    // whichever thread finishes last reads the final count.
                    if (finished.incrementAndGet() >= claimed.get() && Thread.currentThread() != caller) {
                        finishedAt = System.nanoTime();
                        LockSupport.unpark(caller);
                    }
                }
            }
        }

        /// Waits on the calling thread until every claimed column has finished: spinning for the
        /// [WakeCost], then parking until the last worker to finish wakes it.
        void await() {
            long start = System.nanoTime();
            long spin = WakeCost.spin(LINGER_NANOS);
            while (finished.get() < claimed.get()) {
                if (System.nanoTime() - start < spin) {
                    Thread.onSpinWait();
                    continue;
                }
                boolean parked = false;
                while (finished.get() < claimed.get()) {
                    LockSupport.park(this);
                    parked = true;
                }
                if (parked) {
                    // `finishedAt` is stamped before the `unpark`, so the wake-up is measured
                    // from the moment it was issued.
                    WakeCost.sample(finishedAt);
                }
            }
        }

        void rethrowFirstFailure() {
            for (Throwable failure : failures) {
                if (failure != null) {
                    rethrow(failure);
                }
            }
        }
    }
}
