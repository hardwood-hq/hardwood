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
import dev.hardwood.internal.reader.batch.AllocationTally;
import dev.hardwood.internal.reader.memory.ReadAheadBuffer;
import dev.hardwood.reader.ParquetReadException;

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
    /// A decode of a flat page also fills batch holders: it starts with a claim of its frontier
    /// page's bound, held by `claim`, and its work counts into `tally` what the holders' arrays
    /// gained or lost; as it ends, the claim is settled to the tally ([#settle]), and the holders
    /// hold those bytes until they leave the read.
    static final class Job<T> {
        /// lowered while the job waits, where the read learns it serves an earlier row ([#lower])
        private long key;
        private final long cost;
        private final Kind kind;
        /// what holds the bytes once started; `null` for a job that holds none
        private final Charge charge;
        /// the page it decodes in its column's frontier; `null` for a job that fills no holder
        private final FrontierPage page;
        /// what holds its claim while it runs; `null` for a job that fills no holder
        private final Charge claim;
        private final AllocationTally tally;
        /// what its work draws past its claim; `null` for a job that fills no holder
        private final Draw draw;
        /// of its column's free holders, the bytes its claim counts on reusing ([#earmark])
        private long earmarked;
        private final Work<T> work;
        private final Consumer<T> onValue;
        private final Consumer<Throwable> onFailure;
        private long seq;
        /// keyed below need as it started
        private boolean needed;

        Job(long key, long cost, Kind kind, Charge charge, Work<T> work, Consumer<T> onValue,
            Consumer<Throwable> onFailure) {
            this(key, cost, kind, charge, null, null, null, null, work, onValue, onFailure);
        }

        Job(long key, long cost, Kind kind, Charge charge, FrontierPage page, Charge claim, AllocationTally tally,
            Draw draw, Work<T> work, Consumer<T> onValue, Consumer<Throwable> onFailure) {
            this.key = key;
            this.cost = cost;
            this.kind = kind;
            this.charge = charge;
            this.page = page;
            this.claim = claim;
            this.tally = tally;
            this.draw = draw;
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

    /// What a decode draws past its claim for the values it reconstructs beyond its page's bytes,
    /// which no header bounds: drawn on its thread as the decode learns them, before it allocates
    /// them. A decode need forced draws what fits the ledger, and fails the read where they do not;
    /// a decode ahead of need draws what fits beside the reserves it started beside, and otherwise
    /// gives up ([NoRoomException]), so that its page starts again with what it learned covered.
    static final class Draw {
        /// the reconstructed bytes its claim covers, learned by an earlier start of its page
        private final long covered;
        private ReadAheadBuffer budget;
        private boolean forced;
        private long reserve;
        private long drawn;

        Draw(long covered) {
            this.covered = covered;
        }

        /// Its decode starts: need forced it, or it started beside `reserve`.
        private void start(ReadAheadBuffer budget, boolean forced, long reserve) {
            this.budget = budget;
            this.forced = forced;
            this.reserve = reserve;
        }

        /// Draws `bytes` of reconstructed values, less what its claim covers.
        ///
        /// @throws dev.hardwood.reader.ParquetReadException for a decode need forced, if they do not fit
        /// @throws NoRoomException for a decode ahead of need, if they do not fit
        void draw(long bytes) {
            long beyond = bytes - covered;
            if (beyond <= 0) {
                return;
            }
            if (!budget.tryCharge(beyond, forced ? 0 : reserve)) {
                if (forced) {
                    throw new ParquetReadException("Data page needs " + beyond + " more bytes for the values it "
                            + "reconstructs, which takes the context's heap quota to " + (budget.held() + beyond)
                            + " bytes, more than its limit of " + budget.limit() + " ("
                            + ReadAheadBuffer.READ_AHEAD_BYTES_PROPERTY + ")");
                }
                throw new NoRoomException(bytes);
            }
            drawn += beyond;
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

    /// A page of a column from when the read finds it until its decode starts or it will not
    /// decode: at its key, what its decode keeps past its end at most and its task's buffers. While
    /// what it keeps is not known in full, no other decode keyed at or past it starts beside the one
    /// need forces: a page whose header has not arrived, which reserves nothing, and a page whose
    /// decode reconstructs values beyond its bytes, which reserves what its header bounds until a
    /// decode of it learns the rest. Each flat column also has one past its last page found, never
    /// bounded: the pages the read has not found yet ([#found]).
    static final class FrontierPage {
        private final int column;
        private long key;
        private final long seq;
        private long bytes;
        /// of `bytes`, those no free holder of the column brings
        private long fresh;
        /// the buffers its decode's task holds while it runs
        private long task;
        /// whether `bytes` and `task` are set
        private boolean bounded;
        /// whether they hold all it keeps
        private boolean exact;

        private FrontierPage(int column, long key, long seq) {
            this.column = column;
            this.key = key;
            this.seq = seq;
        }
    }

    private static final Comparator<FrontierPage> FRONTIER_ORDER = (a, b) -> a.key != b.key
            ? Long.compare(a.key, b.key) : Long.compare(a.seq, b.seq);

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
    /// per flat column, its frontier: the pages whose decode has not started, lowest key first;
    /// `null` for a column decoded through levels
    private final List<TreeSet<FrontierPage>> frontier = new ArrayList<>();
    /// per flat column, its frontier page past the last page found
    private final List<FrontierPage> notFound = new ArrayList<>();
    /// the frontier pages of every column without a bound, lowest key first
    private final TreeSet<FrontierPage> unbounded = new TreeSet<>(FRONTIER_ORDER);
    private long frontierSeq;
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
    /// per column, the bytes charged for its free holders
    private IntToLongFunction freeHeldBytes = column -> 0;
    /// per flat column, of its free holders' bytes, those running claims count on reusing
    private long[] earmarked = new long[0];
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
    }

    /// Adds the data page of the flat `column` at `key` to its frontier, without a bound until
    /// [#bound] gives it one.
    FrontierPage enter(int column, long key) {
        FrontierPage page = new FrontierPage(column, key, frontierSeq++);
        frontier(column).add(page);
        unbounded.add(page);
        return page;
    }

    /// Bounds what the decode of `page` keeps past its end, `bytes`, of which `fresh` no free
    /// holder of the column brings, and the buffers its task holds while it runs, `task`; `bytes`
    /// is also the claim its decode starts with. Unless `exact`, its decode reconstructs values
    /// beyond them, which only a decode of it learns.
    void bound(FrontierPage page, long bytes, long fresh, long task, boolean exact) {
        page.bytes = bytes;
        page.fresh = fresh;
        page.task = task;
        page.bounded = true;
        if (exact && !page.exact) {
            page.exact = true;
            unbounded.remove(page);
        }
    }

    /// `page` leaves its column's frontier: it will not decode.
    void leave(FrontierPage page) {
        frontier.get(page.column).remove(page);
        unbounded.remove(page);
    }

    /// The decode of `page` starts: it leaves its column's frontier, as what it keeps is charged
    /// from here on; a page not bounded in full stays unknown until the decode ends, as only then is
    /// all it keeps charged.
    private void started(FrontierPage page) {
        frontier.get(page.column).remove(page);
        if (page.exact) {
            unbounded.remove(page);
        }
    }

    /// Records that the flat `column`'s pages are found up to `end`, the key past the last one;
    /// a column registered with `0` has none found yet.
    void found(int column, long end) {
        TreeSet<FrontierPage> pages = frontier(column);
        while (notFound.size() <= column) {
            notFound.add(null);
        }
        FrontierPage past = notFound.get(column);
        if (past == null) {
            past = new FrontierPage(column, end, frontierSeq++);
            notFound.set(column, past);
        }
        else if (end > past.key) {
            pages.remove(past);
            unbounded.remove(past);
            past.key = end;
        }
        else {
            return;
        }
        pages.add(past);
        unbounded.add(past);
    }

    private TreeSet<FrontierPage> frontier(int column) {
        while (frontier.size() <= column) {
            frontier.add(null);
        }
        if (column >= earmarked.length) {
            earmarked = Arrays.copyOf(earmarked, column + 1);
        }
        TreeSet<FrontierPage> pages = frontier.get(column);
        if (pages == null) {
            pages = new TreeSet<>(FRONTIER_ORDER);
            frontier.set(column, pages);
        }
        return pages;
    }

    /// Whether a page before `job`'s in frontier order, by key and then in the order the read found
    /// them, is not bounded in full, which leaves no room to reserve for what it keeps: a page found
    /// whose header has not arrived or whose reconstructed values no decode has learned, or one not
    /// found yet. The first such page is never held back by another, so one always starts.
    private boolean pastUnknown(Job<?> job) {
        return !unbounded.isEmpty() && FRONTIER_ORDER.compare(unbounded.first(), job.page) < 0;
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

    /// Takes `job`, a fetch, out before it starts; returns whether it was waiting.
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
        // a page decoded beside the one need forces takes room only where room is reserved for every
        // page need could force before it, which takes every column's pages below it being known
        FrontierPage page = job.page;
        if (!forced && page != null && pastUnknown(job)) {
            return DECODES;
        }
        long cost = job.cost();
        // a decode's claim counts on the column's free holders no running claim counts on: they are
        // charged already, and its cells reuse them
        long claim = page == null ? 0 : page.bytes - available(page);
        if (forced) {
            if (page == null) {
                // a fetch or plan need forces holds what it reads uncharged
                cost = 0;
            }
            else if (cost + claim > 0 && heldOnLedger() + cost + claim > budget.limit()) {
                // the reserve kept room for the decode need forces; where the room is not there, what
                // the consumer's rows hold takes it, and the decode fails the read, allocating nothing
                job.claim.refused(cost + claim, heldOnLedger() + cost + claim, budget.limit());
                cost = 0;
                claim = 0;
            }
            else {
                budget.charge(cost + claim);
                held += cost + claim;
            }
        }
        long reserve = forced ? 0 : reserve(job);
        if (!forced && !charge(cost + claim, reserve)) {
            // a plan starts first whatever its key, so a refused one holds back only the plans after
            // it: work keyed below it, need's among it, is still tried
            return job.kind() == Kind.PLAN ? PLANS : FETCHES | DECODES;
        }
        if (needed) {
            job.startedNeeded();
            neededInFlight++;
        }
        if (job.charge() != null) {
            job.charge().take(cost, ahead);
        }
        if (page != null) {
            started(page);
            if (!job.claim.isRefused()) {
                job.earmarked = page.bytes - claim;
                earmarked[page.column] += job.earmarked;
            }
            job.claim.take(claim, false);
            if (job.draw != null) {
                job.draw.start(budget, forced, reserve);
            }
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
        if (job.draw != null && buffers != null) {
            buffers.set(ReadCharges.DRAW, job.draw);
        }
        try {
            value = job.cpu() ? job.work().run(buffers) : retried(job.work());
        }
        catch (Throwable t) {
            return refused(job, t);
        }
        finally {
            if (job.draw != null && buffers != null) {
                buffers.set(ReadCharges.DRAW, null);
            }
        }
        return () -> {
            ended(job);
            if (job.cpu() && job.charge() != null) {
                release(job.charge());
            }
            settle(job, false);
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
            settle(job, true);
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

    /// What need could force next beside `job`: per column, what the first page of its frontier,
    /// `job`'s aside, keeps past its decode beyond the column's free holders, and the largest of
    /// those pages' task buffers, as need forces one decode at a time. Decodes start in key order,
    /// so where they stop, the next decode need forces in a column is that one; a column's free
    /// holders only grow until that decode takes them. A frontier page without a bound reserves
    /// nothing: no decode past it starts ahead ([#pastUnknown]).
    private long reserve(Job<?> job) {
        long reserve = 0;
        long task = 0;
        for (int column = 0; column < frontier.size(); column++) {
            TreeSet<FrontierPage> pages = frontier.get(column);
            FrontierPage first = pages == null || pages.isEmpty() ? null : pages.first();
            if (first != null && first == job.page) {
                first = pages.higher(first);
            }
            if (first != null && first.bounded) {
                reserve += first.bytes - available(first);
                task = Math.max(task, first.task);
            }
        }
        return reserve + task;
    }

    /// Of what the decode of `page` adds to its column's batch holders, the bytes the column's
    /// free holders cover that no running claim counts on.
    private long available(FrontierPage page) {
        long free = Math.max(0, freeHeldBytes.applyAsLong(page.column) - earmarked[page.column]);
        return Math.min(free, page.bytes - page.fresh);
    }

    /// What R holds, less what this read has returned under the lock and hands back once it lets go
    /// of it.
    private long heldOnLedger() {
        return budget.held() - released;
    }

    /// Takes `bytes` from R where they fit beside `reserve`, counting what this read returns once
    /// it lets go of the lock as returned; else waits for R's next release.
    private boolean charge(long bytes, long reserve) {
        if (bytes > 0) {
            long seen = budget.releases();
            if (!budget.tryCharge(bytes, reserve - released)) {
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

    /// The bytes the read holds on R.
    long held() {
        return held;
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

    /// Settles the claim of `job`, as it ends, with what it drew past it: a claim with a tally to
    /// what its work counted, returning what the holders did not take and charging what they took
    /// beyond it, whether or not that fits, as it is allocated already; a claim without one, a
    /// dictionary's or a page's the column keeps, is held on, for its decode's `onValue` to hand on,
    /// or returned where the job failed.
    private void settle(Job<?> job, boolean failed) {
        if (job.claim == null) {
            return;
        }
        unbounded.remove(job.page);
        earmarked[job.page.column] -= job.earmarked;
        // what it drew R charged as it drew it
        long drawn = job.draw == null ? 0 : job.draw.drawn;
        held += drawn;
        if (job.tally == null) {
            job.claim.grow(drawn);
            if (failed) {
                release(job.claim);
            }
            return;
        }
        settleHeld(job.tally.bytes() - job.claim.takeBack() - drawn);
    }

    /// Charges `bytes` the read's batch holders gained, or returns them, negative, where they lost
    /// them, whether or not they fit: they are allocated already.
    void settleHeld(long bytes) {
        if (releasedAll || bytes == 0) {
            return;
        }
        if (bytes > 0) {
            budget.charge(bytes);
            held += bytes;
        }
        else {
            held += bytes;
            released -= bytes;
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
        fetchedAhead = 0;
    }
}
