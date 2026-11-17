/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.IntToLongFunction;
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
        /// CPU work that leads to a fetch: a footer's parse, a row group's pruning and plan;
        /// pruning held back by R where it is charged the dictionary parses it runs
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
    /// A decode may also keep `keptCost` bytes past its end, the batches it filled ahead of the
    /// consumer: taken by `kept` as it starts, and returned by whoever holds them then
    /// ([#release(Charge)]), or as it fails.
    static final class Job<T> {
        /// lowered while the job waits, where the read learns it serves an earlier row ([#lower])
        private long key;
        private final long cost;
        private final Kind kind;
        /// what holds the bytes once started; `null` for a job that holds none
        private final Charge charge;
        private final long keptCost;
        /// what holds the bytes kept past its end; `null` for a job that keeps none
        private final Charge kept;
        /// the column whose page it decodes, for the read's reserve; `-1` for none
        private final int column;
        private final Work<T> work;
        private final Consumer<T> onValue;
        private final Consumer<Throwable> onFailure;
        private long seq;
        /// keyed below need as it started
        private boolean needed;

        Job(long key, long cost, Kind kind, Charge charge, Work<T> work, Consumer<T> onValue,
            Consumer<Throwable> onFailure) {
            this(key, cost, kind, charge, 0, null, -1, work, onValue, onFailure);
        }

        Job(long key, long cost, Kind kind, Charge charge, long keptCost, Charge kept, int column, Work<T> work,
            Consumer<T> onValue, Consumer<Throwable> onFailure) {
            this.key = key;
            this.cost = cost;
            this.kind = kind;
            this.charge = charge;
            this.keptCost = keptCost;
            this.kept = kept;
            this.column = column;
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

        /// What holds the bytes it keeps past its end, `null` for a job that keeps none.
        Charge kept() {
            return kept;
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

    /// A data page need could force later whose decode is not added yet: at its key, what its
    /// decode would allocate at most ([#reserve]), or, for a page whose bound is not known yet (a
    /// binary page whose header has not arrived), a mark that no decode past it starts ahead.
    static final class Pending {
        private final long key;
        private final long seq;
        private long bytes;
        private boolean unbounded;

        private Pending(long key, long seq) {
            this.key = key;
            this.seq = seq;
        }
    }

    private static final Comparator<Pending> PENDING_ORDER = (a, b) -> a.key != b.key ? Long.compare(a.key, b.key)
            : Long.compare(a.seq, b.seq);

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
    /// per column, its ready decodes that keep bytes past their end, lowest key first ([#reserve])
    private final List<TreeSet<Job<?>>> waitingKept = new ArrayList<>();
    /// per column, its data pages whose decode is not added yet, lowest key first ([#reserve])
    private final List<TreeSet<Pending>> pending = new ArrayList<>();
    /// per flat column, the key past its last page found, `-1` for a column not registered ([#known])
    private long[] knownEnd = new long[0];
    private long pendingSeq;
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
    /// of `held`, the buffers of this read's running tasks, which their ends release
    private long heldTransient;
    private long releasedTransient;
    /// per column, the bytes charged for its free holders
    private IntToLongFunction freeHeldBytes = column -> 0;
    /// set once [#releaseAll] has returned every charge
    private boolean releasedAll;
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
        if (keeps(job)) {
            waitingKept(job.column).add(job);
        }
    }

    /// Registers a data page of `column` at `key` whose decode is not added yet; its bound is set
    /// with [#bound].
    Pending pend(int column, long key) {
        Pending p = new Pending(key, pendingSeq++);
        while (pending.size() <= column) {
            pending.add(new TreeSet<>(PENDING_ORDER));
        }
        pending.get(column).add(p);
        return p;
    }

    /// Sets what the decode of the page `p` stands for would allocate at most, `bytes`, or that
    /// it is not known yet (`unbounded`).
    static void bound(Pending p, long bytes, boolean unbounded) {
        p.bytes = bytes;
        p.unbounded = unbounded;
    }

    /// The page `p` stands for has its decode added, or will not decode.
    void unpend(int column, Pending p) {
        if (column < pending.size()) {
            pending.get(column).remove(p);
        }
    }

    /// Records that `column`'s pages are known up to `end`, the key past the last one found; a
    /// column registered with `0` knows none yet.
    void known(int column, long end) {
        if (column >= knownEnd.length) {
            int from = knownEnd.length;
            knownEnd = Arrays.copyOf(knownEnd, column + 1);
            Arrays.fill(knownEnd, from, knownEnd.length, -1);
        }
        knownEnd[column] = Math.max(knownEnd[column], end);
    }

    /// Whether some page need could force before `job`'s is not bounded yet, which leaves no room
    /// to reserve for it: a registered column whose pages are not known as far as `job`'s key,
    /// or a page below it whose bound is not known (a binary page whose header has not arrived).
    private boolean pastUnknown(Job<?> job) {
        for (int column = 0; column < knownEnd.length; column++) {
            long end = knownEnd[column];
            if (end >= 0 && end <= job.key()) {
                return true;
            }
        }
        for (TreeSet<Pending> pages : pending) {
            for (Pending p : pages) {
                if (p.key >= job.key()) {
                    break;
                }
                if (p.unbounded) {
                    return true;
                }
            }
        }
        return false;
    }

    /// Whether `job` is a decode that keeps bytes past its end, which the read's reserve counts.
    private static boolean keeps(Job<?> job) {
        return job.kind() == Kind.DECODE && job.kept() != null && job.column >= 0;
    }

    /// The waiting decodes of `column` that keep bytes, lowest key first.
    private TreeSet<Job<?>> waitingKept(int column) {
        while (waitingKept.size() <= column) {
            waitingKept.add(new TreeSet<>(ORDER));
        }
        return waitingKept.get(column);
    }

    /// Keys `job` at `key` where that is lower and the job has not started: work the read keyed by
    /// an estimate turns out to serve an earlier row, which need must reach.
    void lower(Job<?> job, long key) {
        TreeSet<Job<?>> set = ready.get(job.kind().ordinal());
        if (key < job.key() && set.remove(job)) {
            if (keeps(job)) {
                waitingKept(job.column).remove(job);
            }
            job.keyAt(key);
            set.add(job);
            if (keeps(job)) {
                waitingKept(job.column).add(job);
            }
        }
    }

    /// Takes `job` out before it starts; returns whether it was waiting.
    boolean remove(Job<?> job) {
        boolean removed = ready.get(job.kind().ordinal()).remove(job);
        if (removed && keeps(job)) {
            waitingKept(job.column).remove(job);
        }
        return removed;
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
        // a page decoded ahead of need takes room only where room is reserved for every page need
        // could force before it, which takes every column's pages below it being known
        if (!needed && keeps(job) && pastUnknown(job)) {
            return DECODES;
        }
        // the job need forces holds what it reads uncharged: R holds what the read chose to start
        long charged = forced ? 0 : job.cost();
        long kept = job.kept() == null ? 0 : job.keptCost;
        // the batches of a page the consumer's next batch needs are taken from the reserve, which
        // kept room for them, whether its decode is forced or starts beside the forced one
        boolean keptNeeded = needed && kept > 0;
        if (keptNeeded) {
            // the cells it fills with its column's free holders, charged already, allocate nothing;
            // its true-up settles what it did allocate
            kept = Math.max(0, kept - freeHeldBytes.applyAsLong(job.column));
        }
        if (!forced && !charge(charged + (keptNeeded ? 0 : kept), !keptNeeded && kept > 0 ? reserve(job) : 0)) {
            // a plan starts first whatever its key, so a refused one holds back only the plans after
            // it: work keyed below it, need's among it, is still tried
            return job.kind() == Kind.PLAN ? PLANS : FETCHES | DECODES;
        }
        if (keptNeeded && kept > 0 && budget.held() + kept > budget.limit()) {
            if (budget.heldBeyondTasks() + kept <= budget.limit()) {
                // running tasks' buffers take the room, and their ends release it whatever the
                // consumer does: the job waits for them
                if (!forced && charged > 0) {
                    budget.release(charged);
                    held -= charged;
                }
                waitForRoom();
                return DECODES;
            }
        }
        if (keptNeeded) {
            // what does not fit the ledger beside what only consumers' progress releases fails the
            // job, which allocates nothing
            long total = budget.held() + kept;
            if (kept > 0 && total > budget.limit()) {
                job.kept().refused(kept, total, budget.limit());
                kept = 0;
            }
            else {
                budget.charge(kept);
                held += kept;
            }
        }
        if (needed) {
            job.startedNeeded();
            neededInFlight++;
        }
        if (job.charge() != null) {
            job.charge().take(charged, ahead);
            if (job.cpu() && charged > 0) {
                job.charge().transientBuffers();
                budget.markTransient(charged);
                heldTransient += charged;
            }
        }
        if (job.kept() != null) {
            job.kept().take(kept, false);
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
            if (job.kept() != null) {
                release(job.kept());
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

    /// Sets the bytes charged for each column's free holders, which a decode takes before it
    /// allocates ([#reserve]).
    void freeHeldBytes(IntToLongFunction freeHeldBytes) {
        this.freeHeldBytes = freeHeldBytes;
    }

    /// What need could force next beside `job`: per column, what its first page not decoded,
    /// `job`'s aside, would allocate beyond the column's free holders: a decode added and not
    /// started, or a page whose decode is not added yet ([#pend]), whichever comes first. Decodes
    /// start in key order, so where they stop, the next decode need forces in a column is that
    /// one; a column's free holders only grow until that decode takes them.
    private long reserve(Job<?> job) {
        long reserve = 0;
        for (int column = 0; column < Math.max(waitingKept.size(), pending.size()); column++) {
            Job<?> first = null;
            if (column < waitingKept.size()) {
                TreeSet<Job<?>> waiting = waitingKept.get(column);
                first = waiting.isEmpty() ? null : waiting.first();
                if (first == job) {
                    first = waiting.higher(job);
                }
            }
            Pending page = column < pending.size() && !pending.get(column).isEmpty() ? pending.get(column).first() : null;
            long bytes;
            if (page != null && (first == null || page.key < first.key())) {
                bytes = page.bytes;
            }
            else {
                bytes = first == null ? 0 : first.keptCost;
            }
            reserve += Math.max(0, bytes - freeHeldBytes.applyAsLong(column));
        }
        return reserve;
    }

    /// Waits for R's next release, which runs `wake` once.
    private void waitForRoom() {
        long seen = budget.releases();
        if (waitingForRoom.compareAndSet(false, true)) {
            budget.whenReleased(seen, roomReleased);
        }
    }

    /// Takes `bytes` from R where they fit beside `reserve`; else waits for its next release.
    private boolean charge(long bytes, long reserve) {
        if (bytes > 0) {
            long seen = budget.releases();
            if (!budget.tryCharge(bytes, reserve)) {
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
            if (charge.isTransient()) {
                heldTransient -= bytes;
                releasedTransient += bytes;
            }
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

    /// Of the bytes [#takeReleased] returned last, those counted as running tasks' buffers.
    long takeReleasedTransient() {
        long r = releasedTransient;
        releasedTransient = 0;
        return r;
    }

    /// Charges `bytes` a decode allocated beyond what it was admitted with, whether or not they fit:
    /// they are allocated already.
    void chargeHeld(long bytes) {
        if (!releasedAll) {
            budget.charge(bytes);
            held += bytes;
        }
    }

    /// Returns `bytes` a batch the consumer took was charged; nothing once [#releaseAll] has
    /// returned every charge.
    void releaseTaken(long bytes) {
        if (!releasedAll) {
            held -= bytes;
            released += bytes;
        }
    }

    /// Returns every charge still held, once nothing is in flight, and stops waiting for R.
    void releaseAll() {
        releasedAll = true;
        budget.cancel(roomReleased);
        released += held;
        held = 0;
        releasedTransient += heldTransient;
        heldTransient = 0;
        fetchedAhead = 0;
    }
}
