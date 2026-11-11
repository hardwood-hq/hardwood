/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.reader.memory.ReadAheadBuffer;

/// Decides which ready job of a read starts, and knows nothing of Parquet. Runs under the
/// read's lock ([ReadDriver]), except [#run].
///
/// A job is ranked by its key, the first row of the read it serves, and starts lowest key first
/// within three bounds: R, the context's [ReadAheadBuffer], on the bytes it chose to hold; decodes no further
/// than the look-ahead past the consumer; and fetches while less than F is fetched ahead. CPU
/// jobs wait for a free slot. The first job a bound refuses holds back the later jobs of the kinds
/// that bound governs, and only those. Planning takes a free slot before any other CPU job,
/// whatever its key: it leads to a fetch, whose latency the decodes ahead of it would otherwise
/// wait out first.
///
/// **Invariant: while no job keyed below need (the consumer's next batch) is in flight, the
/// lowest ready job keyed below it starts, refused by no bound: a fetch at once, a CPU job at the
/// next free slot a plan job does not take.** It is
/// the progress guarantee and no bound of its own: the look-ahead never reaches below need
/// ([Position]), and that one job holds what it reads uncharged to R and counts against no F, so
/// the next batch advances one job at a time where the bounds hold everything else back.
final class JobScheduler {

    enum Kind {
        /// I/O that holds what it fetches: counts against F
        FETCH,
        /// a footer's read: a fetch whose key, the end of the rows listed, is no row the data reached
        FOOTER,
        /// CPU work that leads to a fetch: a footer's parse, a row group's probe and plan; a probe
        /// held back by R where it is charged the dictionary parses it runs
        PLAN,
        /// CPU work that holds values for the consumer: held back by the look-ahead, and by R where it
        /// is charged the buffers it holds while it runs
        DECODE,
        /// other CPU work: planning, scans, assembly
        CPU
    }

    /// A job's work outside the read's lock, producing its value.
    interface Work<T> {
        /// @param buffers the decode buffers of the thread running a CPU job, `null` for a fetch
        T run(DecodeBuffers buffers) throws Exception;
    }

    /// A job: `cost` bytes, taken by `charge` as it starts, then `work` outside the lock; as it
    /// ends, under the lock, `onValue` takes the value its work produced, or `onFailure` its failure.
    static final class Job<T> {
        /// lowered while the job waits, where the read learns it serves an earlier row ([#lower])
        private long key;
        private final long cost;
        private final Kind kind;
        /// what holds the bytes once started; `null` for a job that holds none
        private final Charge charge;
        private final Work<T> work;
        private final Consumer<T> onValue;
        private final Consumer<Throwable> onFailure;
        private long seq;
        /// keyed below need as it started
        private boolean needed;

        Job(long key, long cost, Kind kind, Charge charge, Work<T> work, Consumer<T> onValue,
            Consumer<Throwable> onFailure) {
            this.key = key;
            this.cost = cost;
            this.kind = kind;
            this.charge = charge;
            this.work = work;
            this.onValue = onValue;
            this.onFailure = onFailure;
        }

        /// The first row of the read it serves.
        long key() {
            return key;
        }

        /// The bytes it holds once started.
        long cost() {
            return cost;
        }

        Kind kind() {
            return kind;
        }

        /// What holds its bytes once started, `null` for a job that holds none.
        Charge charge() {
            return charge;
        }

        Work<T> work() {
            return work;
        }

        /// Takes the value its work produced, under the lock.
        Consumer<T> onValue() {
            return onValue;
        }

        /// Takes its failure, under the lock.
        Consumer<Throwable> onFailure() {
            return onFailure;
        }

        /// The order it was added in, which breaks ties between equal keys.
        long seq() {
            return seq;
        }

        /// Whether it was keyed below need as it started.
        boolean needed() {
            return needed;
        }

        /// It is added as the `seq`-th job.
        void added(long seq) {
            this.seq = seq;
        }

        /// It serves row `key`, before it starts.
        void keyAt(long key) {
            this.key = key;
        }

        /// It started keyed below need.
        void startedNeeded() {
            needed = true;
        }

        boolean cpu() {
            return kind == Kind.PLAN || kind == Kind.DECODE || kind == Kind.CPU;
        }
    }

    /// Where the consumer stands: below `need`, the lowest job starts whatever R and F while no
    /// other job below it is in flight; decodes start only below `reach`, which is never below
    /// `need`; nothing keyed past `end` starts, so the jobs at a failure's row still run and the
    /// lowest column's failure there wins.
    record Position(long need, long reach, long end) {
        Position {
            reach = Math.max(reach, need);
        }
    }

    private static final Comparator<Job<?>> ORDER = (a, b) -> a.key() != b.key() ? Long.compare(a.key(), b.key())
            : Long.compare(a.seq(), b.seq());
    private static final Kind[] KINDS = Kind.values();
    /// the kinds a refusal holds back: a bound's own kind, and for R the fetches and the decodes,
    /// or for a refused plan the plans
    private static final int FETCHES = 1 << Kind.FETCH.ordinal() | 1 << Kind.FOOTER.ordinal();
    private static final int DECODES = 1 << Kind.DECODE.ordinal();
    private static final int PLANS = 1 << Kind.PLAN.ordinal();

    private final ReadAheadBuffer budget;
    private final AtomicBoolean waitingForRoom = new AtomicBoolean();
    /// registered with R once a charge did not fit; runs `wake` on its next release
    private final Runnable roomReleased;
    private final int cpuSlots;
    private final long fetchAheadLimit;

    /// ready jobs by kind, each lowest key first
    private final List<TreeSet<Job<?>>> ready = Stream.of(KINDS).map(k -> new TreeSet<>(ORDER)).toList();
    private long seq;
    private int cpuInFlight;
    /// the jobs started and not ended, lowest key first
    private final TreeSet<Job<?>> running = new TreeSet<>(ORDER);
    /// started jobs keyed below need, not yet ended
    private int neededInFlight;
    private long fetchedAhead;
    /// the highest key of a fetch started that holds bytes: how far the read's data has reached
    private long fetchedTo;
    private long held;
    /// bytes returned under the lock, for the driver to release to R once it has let go of it
    private long released;

    /// @param wake run, on any thread and possibly under the lock, once R has room again after a
    ///        charge did not fit
    JobScheduler(ReadAheadBuffer budget, Runnable wake, int cpuSlots, long fetchAheadLimit) {
        this.budget = budget;
        this.roomReleased = () -> {
            waitingForRoom.set(false);
            wake.run();
        };
        this.cpuSlots = cpuSlots;
        this.fetchAheadLimit = fetchAheadLimit;
    }

    void add(Job<?> job) {
        job.added(seq++);
        ready.get(job.kind().ordinal()).add(job);
    }

    /// Keys `job` at `key` where that is lower and the job has not started: work the read keyed by
    /// an estimate turns out to serve an earlier row, which need must reach.
    void lower(Job<?> job, long key) {
        TreeSet<Job<?>> set = ready.get(job.kind().ordinal());
        if (key < job.key() && set.remove(job)) {
            job.keyAt(key);
            set.add(job);
        }
    }

    /// Takes `job` out before it starts; returns whether it was waiting.
    boolean remove(Job<?> job) {
        return ready.get(job.kind().ordinal()).remove(job);
    }

    /// Starts what the position and the bounds let start, lowest key first.
    List<Job<?>> start(Position at) {
        List<Job<?>> out = new ArrayList<>();
        int refused = 0;
        while (true) {
            Job<?> job = null;
            for (Kind k : KINDS) {
                TreeSet<Job<?>> set = ready.get(k.ordinal());
                boolean open = (refused & 1 << k.ordinal()) == 0 && (k == Kind.FETCH || k == Kind.FOOTER || cpuInFlight < cpuSlots);
                if (open && !set.isEmpty() && set.first().key() <= at.end() && (job == null || before(set.first(), job))) {
                    job = set.first();
                }
            }
            if (job == null) {
                return out;
            }
            refused |= tryStart(job, at, out);
        }
    }

    private static boolean before(Job<?> a, Job<?> b) {
        return (a.kind() == Kind.PLAN) != (b.kind() == Kind.PLAN) ? a.kind() == Kind.PLAN : ORDER.compare(a, b) < 0;
    }

    /// Starts `job`, or returns the kinds the bound that refused it holds back.
    private int tryStart(Job<?> job, Position at, List<Job<?>> out) {
        boolean needed = job.key() < at.need();
        if (job.kind() == Kind.DECODE && job.key() >= at.reach()) {
            return DECODES;
        }
        boolean forced = needed && neededInFlight == 0;
        boolean ahead = !job.cpu() && !forced;
        // a fetch ahead starts while less than F is fetched ahead, so a request larger than F
        // still runs one ahead
        if (ahead && fetchedAhead >= fetchAheadLimit) {
            return FETCHES;
        }
        // the job need forces holds what it reads uncharged: R holds what the read chose to start
        long charged = forced ? 0 : job.cost();
        if (!charge(charged)) {
            // a plan starts first whatever its key, so a refused one holds back only the plans after
            // it: work keyed below it, need's among it, is still tried
            return job.kind() == Kind.PLAN ? PLANS : FETCHES | DECODES;
        }
        if (needed) {
            job.startedNeeded();
            neededInFlight++;
        }
        if (job.charge() != null) {
            job.charge().take(charged, ahead);
        }
        if (ahead) {
            fetchedAhead += job.cost();
        }
        if (job.kind() == Kind.FETCH && job.cost() > 0) {
            fetchedTo = Math.max(fetchedTo, job.key());
        }
        remove(job);
        if (job.cpu()) {
            cpuInFlight++;
        }
        running.add(job);
        out.add(job);
        return 0;
    }

    /// Runs a started job's work outside the lock; returns what to apply under it once the job has
    /// ended: its `onValue` takes the value its work produced, or its `onFailure` the failure. A CPU
    /// job's charge, the buffers its work holds, is returned as it ends; a fetch's stays with what it
    /// fetched. A fetch that fails with an exception is run once more: the one place a transient
    /// fault is retried.
    <T> Runnable run(Job<T> job, DecodeBuffers buffers) {
        T value;
        try {
            value = job.cpu() ? job.work().run(buffers) : retried(job.work());
        }
        catch (Throwable t) {
            return refused(job, t);
        }
        return () -> {
            ended(job);
            if (job.cpu() && job.charge() != null) {
                release(job.charge());
            }
            job.onValue().accept(value);
        };
    }

    /// The action applied under the lock for a started job that failed with `t`, or never ran: it returns
    /// its charge, and its `onFailure` takes the failure.
    <T> Runnable refused(Job<T> job, Throwable t) {
        return () -> {
            ended(job);
            if (job.charge() != null) {
                release(job.charge());
            }
            job.onFailure().accept(t);
        };
    }

    private static <T> T retried(Work<T> work) throws Exception {
        try {
            return work.run(null);
        }
        catch (Exception e) {
            return work.run(null);
        }
    }

    /// Takes `bytes` from R where they fit; else waits for its next release.
    private boolean charge(long bytes) {
        if (bytes > 0) {
            long seen = budget.releases();
            if (!budget.tryCharge(bytes)) {
                if (waitingForRoom.compareAndSet(false, true)) {
                    budget.whenReleased(seen, roomReleased);
                }
                return false;
            }
            held += bytes;
        }
        return true;
    }

    /// Returns the bytes `charge` holds to the bounds.
    void release(Charge charge) {
        long bytes = charge.takeBack();
        if (bytes > 0) {
            held -= bytes;
            released += bytes;
            if (charge.ahead()) {
                fetchedAhead -= bytes;
            }
        }
    }

    private void ended(Job<?> job) {
        running.remove(job);
        if (job.needed()) {
            neededInFlight--;
        }
        if (job.cpu()) {
            cpuInFlight--;
        }
    }

    /// The highest key of a fetch started that holds bytes, `0` before the first.
    long fetchedTo() {
        return fetchedTo;
    }

    int inFlight() {
        return running.size();
    }

    /// Whether a job keyed at or below `key` is ready or running: work that may still fail at a
    /// row no later than it.
    boolean pendingThrough(long key) {
        if (!running.isEmpty() && running.first().key() <= key) {
            return true;
        }
        for (TreeSet<Job<?>> set : ready) {
            if (!set.isEmpty() && set.first().key() <= key) {
                return true;
            }
        }
        return false;
    }

    /// The bytes returned since the last call, for the driver to release to R once it has let go
    /// of the lock: R runs other reads' waiters on the releasing thread.
    long takeReleased() {
        long r = released;
        released = 0;
        return r;
    }

    /// Returns every charge still held, once nothing is in flight, and stops waiting for R.
    void releaseAll() {
        budget.cancel(roomReleased);
        released += held;
        held = 0;
        fetchedAhead = 0;
    }
}
