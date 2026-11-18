/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine.pico;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

/// A read engine stripped to its scheduling, for teaching: the jobs, priority, bounds, need and
/// lifetimes of `_designs/READ_PIPELINE.md` "Scheduling", with parks standing in for Parquet. See
/// `README.md` beside this file.
///
/// Run with `java -cp <classes> dev.hardwood.internal.reader.engine.pico.PicoEngine`, add
/// `-Dtrace=true` for one line per job start.
public final class PicoEngine {

    /// The shape of the read and the bounds. Every byte count is in bytes, every time in nanoseconds.
    record Settings(long rows, int pageRows, long pageBytes, int pagesPerRange, int batchRows, long latencyNanos,
            long bytesPerSecond, long decodeNanos, long fetchAhead, int lookAhead, int cpuSlots) {
        int pagesPerBatch() { return batchRows / pageRows; } // simplification: a page lies in exactly one batch
    }

    enum Kind { FETCH, DECODE }

    /// A job: the first row it serves, the bytes it holds, its work outside the lock, and the
    /// callback that applies its end under the lock. Simplification: no work produces a value or fails.
    record Job(Kind kind, long key, long cost, Runnable work, Runnable onValue) {}

    /// Something a job waits for that ends once, here a range's bytes; it wakes its waiters as it ends.
    static final class Output {
        private List<Runnable> waiters = new ArrayList<>(); // null once ended

        void whenEnded(Runnable waiter) {
            if (waiters == null) {
                waiter.run();
            }
            else {
                waiters.add(waiter);
            }
        }

        void arrived() {
            List<Runnable> woken = waiters;
            waiters = null;
            woken.forEach(Runnable::run);
        }
    }

    private static final boolean TRACE = Boolean.getBoolean("trace");

    private final Settings s;
    private final ExecutorService fetchers = Executors.newVirtualThreadPerTaskExecutor();
    private final ExecutorService cpu; // the fixed pool behind the CPU slots
    // One priority: ready jobs lowest key first, across both kinds (see pump()).
    private final PriorityQueue<Job> readyFetches = new PriorityQueue<>(Comparator.comparingLong(Job::key));
    private final PriorityQueue<Job> readyDecodes = new PriorityQueue<>(Comparator.comparingLong(Job::key));
    private final int pages;
    private final Output[] rangeBytes;
    private final int[] pagesLeftInRange;
    private final long[] chargedToF; // per range, what its fetch counts against F: 0 where need started it
    private final int[] pagesLeftInBatch;
    private final long start = System.nanoTime();
    private int consumer; // the consumer's next batch
    private int cpuInFlight;
    private int neededInFlight;
    private long fetchedAhead;
    private long held;
    private long peakHeld;

    PicoEngine(Settings s) {
        this.s = s;
        this.cpu = Executors.newFixedThreadPool(s.cpuSlots());
        pages = Math.toIntExact(s.rows() / s.pageRows());
        int ranges = Math.ceilDiv(pages, s.pagesPerRange());
        rangeBytes = new Output[ranges];
        pagesLeftInRange = new int[ranges];
        chargedToF = new long[ranges];
        pagesLeftInBatch = new int[Math.ceilDiv(pages, s.pagesPerBatch())];
        // Simplification: planning is left out; every range and page is known at the start.
        for (int r = 0; r < ranges; r++) {
            rangeBytes[r] = new Output();
            readyFetches.add(fetchJob(r));
        }
        for (int p = 0; p < pages; p++) {
            int range = p / s.pagesPerRange();
            long row = (long) p * s.pageRows();
            pagesLeftInRange[range]++;
            pagesLeftInBatch[p / s.pagesPerBatch()]++;
            // A page waits for its range's bytes, and is ready to decode once they arrive.
            rangeBytes[range].whenEnded(() -> readyDecodes.add(decodeJob(range, row)));
        }
    }

    private long rangeSize(int range) {
        return Math.min(s.pagesPerRange(), pages - range * s.pagesPerRange()) * s.pageBytes();
    }

    private Job fetchJob(int range) {
        long key = (long) range * s.pagesPerRange() * s.pageRows();
        long bytes = rangeSize(range);
        return new Job(Kind.FETCH, key, bytes,
                () -> LockSupport.parkNanos(s.latencyNanos() + bytes * 1_000_000_000L / s.bytesPerSecond()), // stands in for I/O
                rangeBytes[range]::arrived);
    }

    private Job decodeJob(int range, long row) {
        int batch = Math.toIntExact(row / s.batchRows());
        return new Job(Kind.DECODE, row, 0, () -> LockSupport.parkNanos(s.decodeNanos()), () -> { // stands in for CPU work
            // Lifetimes: a range's bytes are held until its last reader, its last page, is decoded.
            if (--pagesLeftInRange[range] == 0) {
                held -= rangeSize(range);
                fetchedAhead -= chargedToF[range];
            }
            pagesLeftInBatch[batch]--;
        });
    }

    /// Starts what the bounds and need let start, lowest key first across kinds. Called under the lock.
    private void pump() {
        long need = (long) (consumer + 1) * s.batchRows(); // jobs keyed below it serve the consumer's next batch
        // Simplification: one column, taken as soon as it is published, so the look-ahead counts from the consumer.
        long reach = Math.max(need, (long) (consumer + s.lookAhead()) * s.batchRows());
        boolean fetchesRefused = false;
        boolean decodesRefused = false;
        while (true) {
            Job f = fetchesRefused ? null : readyFetches.peek();
            Job d = decodesRefused || cpuInFlight == s.cpuSlots() ? null : readyDecodes.peek();
            Job job = f == null ? d : d == null || f.key() <= d.key() ? f : d;
            if (job == null) {
                return;
            }
            boolean needed = job.key() < need;
            // Need: while no job below need is in flight, the lowest one starts, refused by no bound.
            boolean forced = needed && neededInFlight == 0;
            // The bounds; a refusal holds back only the later jobs of its own kind.
            if (job.kind() == Kind.DECODE && job.key() >= reach) {
                decodesRefused = true;
            }
            else if (job.kind() == Kind.FETCH && !forced && fetchedAhead >= s.fetchAhead()) {
                fetchesRefused = true;
            }
            else {
                start(job, needed, forced);
            }
        }
    }

    private void start(Job job, boolean needed, boolean forced) {
        if (needed) {
            neededInFlight++;
        }
        if (job.kind() == Kind.FETCH) {
            readyFetches.poll();
            held += job.cost();
            peakHeld = Math.max(peakHeld, held);
            if (!forced) { // the fetch need forces counts against no F
                fetchedAhead += job.cost();
                chargedToF[Math.toIntExact(job.key() / s.pageRows() / s.pagesPerRange())] = job.cost();
            }
        }
        else {
            readyDecodes.poll();
            cpuInFlight++;
        }
        if (TRACE) {
            System.out.printf("%8.1f ms  %-6s key %8d%s%n", (System.nanoTime() - start) / 1e6, job.kind(), job.key(),
                    forced ? "  forced by need" : "");
        }
        (job.kind() == Kind.FETCH ? fetchers : cpu).execute(() -> {
            job.work().run(); // outside the lock
            synchronized (this) { // simplification: no hand-off; each thread waits for the one lock
                if (needed) {
                    neededInFlight--;
                }
                if (job.kind() == Kind.DECODE) {
                    cpuInFlight--;
                }
                job.onValue().run();
                pump();
                notifyAll();
            }
        });
    }

    /// The consumer: takes every batch in order. Returns the wall time.
    synchronized long run() throws InterruptedException {
        pump();
        while (consumer < pagesLeftInBatch.length) {
            while (pagesLeftInBatch[consumer] > 0) {
                wait();
            }
            consumer++;
            pump();
        }
        fetchers.close();
        cpu.close();
        return System.nanoTime() - start;
    }

    public static void main(String[] args) throws InterruptedException {
        long ms = 1_000_000L;
        long range = 4 * 100_000L;
        System.out.println("latency  F(ranges)  look-ahead  cpu  wall ms  peak held KB");
        long[][] rows = {
                // latency ms, F in ranges, look-ahead batches, CPU slots
                { 0, 1, 8, 4 }, { 20, 1, 8, 4 }, { 20, 16, 8, 4 },
                { 0, 16, 1, 4 }, { 0, 16, 8, 4 }, { 0, 16, 8, 1 } };
        for (long[] r : rows) {
            Settings s = new Settings(200_000, 1_000, 100_000, 4, 10_000, r[0] * ms, 100_000_000L,
                    2 * ms, r[1] * range, Math.toIntExact(r[2]), Math.toIntExact(r[3]));
            PicoEngine engine = new PicoEngine(s);
            long wall = engine.run();
            System.out.printf("%5d ms  %9d  %10d  %3d  %7d  %12d%n", r[0], r[1], r[2], r[3], wall / ms, engine.peakHeld / 1000);
        }
    }
}
