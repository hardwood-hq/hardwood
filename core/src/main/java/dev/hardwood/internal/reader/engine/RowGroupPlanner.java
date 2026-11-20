/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import dev.hardwood.InputFile;
import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.io.HeldBytes;
import dev.hardwood.internal.reader.fetch.FileRange;
import dev.hardwood.internal.reader.footer.FileColumnOrdinals;
import dev.hardwood.internal.reader.plan.BatchPlan;
import dev.hardwood.internal.reader.plan.IndexWindow;
import dev.hardwood.internal.reader.plan.RowGroupPruning;
import dev.hardwood.internal.reader.plan.UnitListing;
import dev.hardwood.internal.reader.plan.UnitPlan;
import dev.hardwood.internal.reader.select.RowRanges;
import dev.hardwood.internal.reader.step.ByteSource;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import jdk.jfr.Event;

/// Plans a read's row groups, moving each through its states ([Unit]). Lists each file's row
/// groups that statistics and the read's skip and limit keep, as [Unit]s; prunes each unit by its
/// Bloom filters and dictionaries; reads its window's page index (sliced from the file's tail where
/// it holds it), from which [PagePlanner] plans its pages and ranges into a [UnitPlan]; and batches
/// the units in unit order, so batches stay in row order whichever plan finishes first. A unit
/// whose planning fails fails the read at its first row, between two units. Used under the read's
/// lock, except the jobs' work.
final class RowGroupPlanner {

    /// The fetch of an [IndexWindow] in the read: its first unit, at whose key its requests are
    /// fetched, and its requests: its Bloom filters, read first, and its page index, read once a
    /// unit of it needs it; each held by every unit of it.
    static final class WindowFetch {
        private final UnitListing first;
        private final List<RangeFetch> bloomFilters;
        private final List<RangeFetch> index;
        private boolean indexRead;

        WindowFetch(UnitListing first, List<RangeFetch> bloomFilters, List<RangeFetch> index) {
            this.first = first;
            this.bloomFilters = bloomFilters;
            this.index = index;
        }

        /// Its first unit.
        UnitListing first() {
            return first;
        }

        /// Its Bloom-filter requests.
        List<RangeFetch> bloomFilters() {
            return bloomFilters;
        }

        /// Its page-index requests.
        List<RangeFetch> index() {
            return index;
        }

        /// A unit of it waits for its page index: returns whether that starts its read, which
        /// happens once.
        boolean readIndex() {
            boolean first = !indexRead;
            indexRead = true;
            return first;
        }
    }

    private final JobScheduler scheduler;
    private final BatchPublisher publisher;
    private final ReadFailure readFailure;
    private final RangeFetcher fetcher;
    private final PageTracker tracker;
    private final HeaderScanner scanner;
    private final ColumnContext columns;
    private final BatchPlan batchPlan;
    private final ColumnSchema[] schemas;
    private final List<Unit> units = new ArrayList<>();
    /// the rows a unit not yet planned is taken to keep, for the look-ahead
    private final KeptRowsEstimate keptEstimate;
    private int nextBatched;
    private long endKey;
    private boolean listed;
    /// what statistics reported of each listed file, committed once the consumer reaches the file,
    /// and the files and units whose reports are committed: a unit's are once the consumer reaches
    /// it and it is planned, so a read that stops before a file or unit reports nothing of it
    private final List<FileReport> fileReports = new ArrayList<>();
    private int reportedFiles;
    private int reportedUnits;
    /// the unit holding the consumer's first unpublished row, as far as [#fileAt] has moved it
    private int consumerUnit;
    /// the units let go of: the consumer has passed them, and they are batched and reported
    private int passedUnits;

    private final RowGroupPruning pruning;
    /// plans a unit's pages and ranges once its page index has arrived
    private final PagePlanner pages;
    /// the rows the read's row limit leaves to list, and the leading rows its physical skip still
    /// drops. Under a filter the budget counts
    /// the rows of row groups statistics prove to match, every row of which is returned, and the
    /// first one they leave undecided lifts it, as the batch plan's cap
    private long budget;
    private final boolean filteredRead;
    private long skip;

    RowGroupPlanner(JobScheduler scheduler, RangeFetcher fetcher, BatchPublisher publisher, ReadFailure readFailure,
                    PageTracker tracker, HeaderScanner scanner, ColumnContext columns, BatchPlan batchPlan, ColumnSchema[] schemas, boolean[] filterOnly, RowGroupPruning pruning,
                    long budget, boolean filteredRead, long skip, long maskedRows) {
        this.budget = budget;
        this.keptEstimate = new KeptRowsEstimate(batchPlan.capacity());
        this.filteredRead = filteredRead;
        this.skip = skip;
        this.pages = new PagePlanner(schemas, filterOnly, pruning, maskedRows);
        this.pruning = pruning;
        this.scheduler = scheduler;
        this.publisher = publisher;
        this.readFailure = readFailure;
        this.fetcher = fetcher;
        this.tracker = tracker;
        this.scanner = scanner;
        this.columns = columns;
        this.batchPlan = batchPlan;
        this.schemas = schemas;
    }

    /// Lists `filtered`, the row groups of a file, `file`, that its statistics keep, as units
    /// behind those listed, each column's chunk at its ordinal in the file, and starts reading
    /// their Bloom filters and page indexes, a window of units at a time ([IndexWindow]). What
    /// reads the file's length runs before anything is listed, so a file that fails lists nothing.
    ///
    /// @throws IOException if the file's length cannot be read
    void addFile(int fileIndex, InputFile file, FileSchema fileSchema, RowGroupPruning.FilteredRowGroups filtered,
                 FileColumnOrdinals ordinals) throws IOException {
        int from = units.size();
        List<UnitListing> listings = new ArrayList<>();
        List<IndexWindow.Member> members = new ArrayList<>();
        long budget = this.budget;
        long skip = this.skip;
        long key = endKey;
        for (RowGroupPruning.FilteredRowGroup kept : filtered.rowGroups()) {
            RowGroup rg = kept.rowGroup();
            if (budget <= 0) {
                break;
            }
            if (filteredRead && !kept.alwaysMatches()) {
                budget = Long.MAX_VALUE;
            }
            if (skip >= rg.numRows()) {
                skip -= rg.numRows();
                continue;
            }
            long leading = skip;
            skip = 0;
            ColumnChunk[] chunks = new ColumnChunk[schemas.length];
            for (int c = 0; c < schemas.length; c++) {
                chunks[c] = rg.columns().get(ordinals.fileOrdinal(schemas[c].columnIndex()));
            }
            // the leading rows of a physical skip are read and the reader drops them
            UnitListing u = new UnitListing(file, fileIndex, fileSchema, ordinals, rg, kept.fileRowGroupIndex(),
                    from + listings.size(), key, kept.alwaysMatches(), kept.leafDecisions(), kept.bloomFilterColumns(),
                    chunks, leading + Math.min(rg.numRows() - leading, budget));
            budget -= u.limit() - leading;
            key += rg.numRows();
            listings.add(u);
            members.add(member(u, kept));
        }
        List<IndexWindow> windows = members.isEmpty() ? List.of()
                : IndexWindow.plan(IndexWindow.budgetBytes(), file, members);
        // dictionary pruning's pages, requested with the window's Bloom filters whatever those
        // decide, each on its own and held by its unit alone (FETCH_PLANNING.md, Pruning reads)
        List<List<FileRange>> dictionaries = new ArrayList<>(listings.size());
        for (UnitListing u : listings) {
            dictionaries.add(u.storedInThisFile() ? RowGroupPruning.dictionaryRanges(u) : List.of());
        }
        if (filtered.event() != null) {
            fileReports.add(new FileReport(filtered.event(), endKey));
        }
        this.budget = budget;
        this.skip = skip;
        endKey = key;
        startWindows(file, from, listings, members, windows, dictionaries);
    }

    /// What `u`'s plan reads of its window: the OffsetIndex of each decoded column but a
    /// filter-only one in a unit statistics prove to match in full, the ColumnIndex of each column
    /// page filtering reads, and its Bloom filters; nothing where a chunk lies in another file,
    /// which fails the unit before any of them is read.
    private IndexWindow.Member member(UnitListing u, RowGroupPruning.FilteredRowGroup kept) {
        RowGroup rg = kept.rowGroup();
        if (!u.storedInThisFile()) {
            return new IndexWindow.Member(rg, kept.fileRowGroupIndex(), new BitSet(), new BitSet(), new BitSet());
        }
        BitSet columnIndexes = RowGroupPruning.columnIndexColumns(u, pruning.pageFiltering(u));
        BitSet offsetIndexes = (BitSet) columnIndexes.clone();
        for (int c = 0; c < schemas.length; c++) {
            if (!pages.filterOnly(c) || !kept.alwaysMatches()) {
                offsetIndexes.set(u.columnOrdinals().fileOrdinal(schemas[c].columnIndex()));
            }
        }
        for (int o = offsetIndexes.nextSetBit(0); o >= 0; o = offsetIndexes.nextSetBit(o + 1)) {
            if (rg.columns().get(o).offsetIndexOffset() == null) {
                offsetIndexes.clear(o);
            }
        }
        return new IndexWindow.Member(rg, kept.fileRowGroupIndex(), offsetIndexes, columnIndexes, u.bloomFilterColumns());
    }

    /// Adds the units of `listings`, the first at `from`, a window at a time: fetches the
    /// dictionary pages each one's pruning reads and the window's Bloom filters, and starts each.
    private void startWindows(InputFile file, int from, List<UnitListing> listings, List<IndexWindow.Member> members,
                              List<IndexWindow> windows, List<List<FileRange>> dictionaries) {
        int first = from;
        for (IndexWindow window : windows) {
            int count = window.memberCount();
            WindowFetch w = new WindowFetch(listings.get(first - from), ranges(file, window.bloomFilterRanges(), count),
                    ranges(file, window.indexRanges(), count));
            for (int i = first; i < first + count; i++) {
                UnitListing u = listings.get(i - from);
                IndexWindow.Member m = members.get(i - from);
                List<RangeFetch> pruningPages = ranges(file, dictionaries.get(i - from), 1);
                request(u, pruningPages);
                Unit.Listed l = new Unit.Listed(u, m.offsetIndexColumns(), m.columnIndexColumns(), w, pruningPages);
                // one its Bloom filters and dictionaries cannot drop waits for its page index at once
                units.add(RowGroupPruning.droppableOnArrival(u) ? l : new Unit.AwaitingIndex(l, null, List.of()));
            }
            request(w.first(), w.bloomFilters());
            for (int i = first; i < first + count; i++) {
                start(units.get(i));
            }
            first += count;
        }
    }

    /// Fetches of `requests` of `file` in file order, each held by `readers`, sliced where the file
    /// holds them.
    private static List<RangeFetch> ranges(InputFile file, List<FileRange> requests, int readers) {
        List<RangeFetch> ranges = new ArrayList<>(requests.size());
        for (FileRange range : requests.stream().sorted(Comparator.comparingLong(FileRange::start)).toList()) {
            RangeFetch r = RangeFetch.of(file, range);
            for (int i = 0; i < readers; i++) {
                r.addReader();
            }
            ranges.add(r);
        }
        return ranges;
    }

    /// Fetches those of `ranges` not held, at `u`'s key, then runs `then` once they have arrived; a
    /// range that cannot be read fails the read at `u`.
    private void fetch(UnitListing u, List<RangeFetch> ranges, Runnable then) {
        request(u, ranges);
        whenArrived(u, ranges, then);
    }

    /// Fetches those of `ranges` not held, at `u`'s key.
    private void request(UnitListing u, List<RangeFetch> ranges) {
        for (RangeFetch r : ranges) {
            if (!r.output().hasValue()) {
                fetcher.fetch(u.file(), r, u.base());
            }
        }
    }

    /// Runs `then` once `ranges` have arrived: waits for the first it lacks and, woken, looks again.
    private void whenArrived(UnitListing u, List<RangeFetch> ranges, Runnable then) {
        RangeFetch missing = RangeFetch.firstMissing(ranges);
        if (missing == null) {
            then.run();
        }
        else if (missing.output().failure() != null) {
            readFailure.failAt(u.base(), missing.output().failure() instanceof IOException e ? new IOException(
                    ExceptionContext.filePrefix(u.file().name()) + "Failed to fetch metadata for row group "
                            + u.rowGroup(), e) : missing.output().failure());
        }
        else {
            missing.output().whenEnded(() -> whenArrived(u, ranges, then));
        }
    }

    /// The pruning of `l`, whose Bloom filters and dictionaries have arrived: its read steps read
    /// the window's Bloom filters, then the dictionary bytes fetched for it, and record what they
    /// lack.
    private static Unit.BloomAndDictionaryPruning pruning(Unit.Listed l) {
        List<RangeFetch> fetched = new ArrayList<>(l.dictionaries());
        List<FileRange> missing = new ArrayList<>();
        String reason = "rg=" + l.listing().rowGroup() + " pruning";
        List<RangeFetch> bloomFilters = l.window().bloomFilters();
        ByteSource bytes = (offset, length) -> {
            ByteBuffer found = RangeFetch.read(bloomFilters, offset, length);
            if (found == null) {
                found = RangeFetch.read(fetched, offset, length);
            }
            if (found == null) {
                missing.add(new FileRange(offset, length, reason));
            }
            return found;
        };
        return new Unit.BloomAndDictionaryPruning(l, new RowGroupPruning.Inputs(), fetched, missing, bytes);
    }

    /// Starts a unit just listed: a listed one is pruned once its Bloom filters and dictionaries
    /// have arrived; one they cannot drop lets go of them and waits for its page index.
    private void start(Unit u) {
        if (u instanceof Unit.Listed l) {
            List<RangeFetch> reads = new ArrayList<>(l.window().bloomFilters());
            reads.addAll(l.dictionaries());
            whenArrived(l.listing(), reads, () -> prune(pruning(l)));
            return;
        }
        Unit.AwaitingIndex a = (Unit.AwaitingIndex) u;
        fetcher.readersDone(a.listed().window().bloomFilters());
        fetcher.readersDone(a.listed().dictionaries());
        awaitIndex(a);
    }

    /// Reads `p`'s Bloom filters and the dictionary pages its evaluation may consult, on a job: what
    /// the window did not bring, a filter or page read header first, is fetched and it reads again,
    /// until it holds them all. Then evaluates them once: in the same job where no dictionary parse
    /// needs a charge, which is every parse within [DecodeBuffers#RETAINED_BYTES], and otherwise on
    /// a job charged for them ([#evaluate]).
    private void prune(Unit.BloomAndDictionaryPruning p) {
        set(p);
        UnitListing u = p.listing();
        p.missing().clear();
        scheduler.add(new JobScheduler.Job<>(u.base(), 0, planKind(u), null, buffers -> {
            // its chunks lie in the file before a dictionary of them is read
            pages.extents(u);
            pruning.read(u, p.bytes(), p.inputs());
            boolean uncharged = p.missing().isEmpty() && RowGroupPruning.parseBytes(u, p.inputs()) == 0;
            return Optional.ofNullable(uncharged ? pruning.evaluate(u, p.inputs()) : null);
        }, arrival -> {
            if (arrival.isPresent()) {
                pruned(p, arrival.get());
            }
            else if (p.missing().isEmpty()) {
                evaluate(p);
            }
            else {
                List<RangeFetch> more = ranges(u.file(), p.missing(), 1);
                p.fetched().addAll(more);
                p.fetched().sort(Comparator.comparingLong(r -> r.range().start()));
                fetch(u, more, () -> prune(p));
            }
        }, t -> readFailure.failAt(u.base(), t)));
    }

    /// Evaluates `p`'s Bloom filters and dictionaries once, where a dictionary parse it may run
    /// exceeds [DecodeBuffers#RETAINED_BYTES], on a job charged those parses, from the headers
    /// [#prune] parsed: R holds them while it runs, unless need forces it, so that the dictionary
    /// pruning of several row groups parses such dictionaries at once only within R.
    private void evaluate(Unit.BloomAndDictionaryPruning p) {
        UnitListing u = p.listing();
        long cost = RowGroupPruning.parseBytes(u, p.inputs());
        scheduler.add(new JobScheduler.Job<>(u.base(), cost, planKind(u), cost > 0 ? new Charge() : null,
                buffers -> pruning.evaluate(u, p.inputs()), arrival -> pruned(p, arrival),
                t -> readFailure.failAt(u.base(), t)));
    }

    /// `p`'s Bloom filters and dictionaries decided: a unit they dropped is planned with no row,
    /// one they kept waits for its page index.
    private void pruned(Unit.BloomAndDictionaryPruning p, RowGroupPruning.Arrival arrival) {
        WindowFetch w = p.listed().window();
        fetcher.readersDone(w.bloomFilters());
        fetcher.readersDone(p.fetched());
        List<Event> events = arrival.dropEvent() == null ? List.of() : List.of(arrival.dropEvent());
        if (arrival.dropped()) {
            set(new Unit.Planned(p.listing(), UnitPlan.empty(schemas.length, RowRanges.NONE), events));
            batchPlanned(w.index());
            return;
        }
        awaitIndex(new Unit.AwaitingIndex(p.listed(), p.inputs().decoded(), events));
    }

    /// `a` waits for its window's page index, which the first unit of the window to wait for it
    /// fetches, and is planned once it has arrived.
    private void awaitIndex(Unit.AwaitingIndex a) {
        set(a);
        WindowFetch w = a.listed().window();
        if (w.readIndex()) {
            fetch(w.first(), w.index(), () -> plan(a));
        }
        else {
            whenArrived(a.listing(), w.index(), () -> plan(a));
        }
    }

    /// Replaces the unit's state with `u`.
    private void set(Unit u) {
        units.set(u.listing().index(), u);
        if (u instanceof Unit.Planned p) {
            long rows = p.listing().metaData().numRows();
            keptEstimate.planned(p.plan().matching().rowCount(rows), rows);
        }
    }

    /// Whether the read's row limit leaves rows to list.
    boolean budgetLeft() {
        return budget > 0;
    }

    /// Every file is listed, or the listing failed: no unit follows those listed.
    void listed() {
        listed = true;
        if (allBatched()) {
            publisher.allPublishable();
        }
    }

    /// The key below which every batch has been added.
    long batchedKey() {
        return nextBatched < units.size() ? units.get(nextBatched).listing().base() : endKey;
    }

    /// The key `rows` kept rows past the batched units end at, the units not batched counted in
    /// unit order: a planned unit by the rows its filter keeps, one not yet planned by the rows it
    /// is taken to keep ([KeptRowsEstimate]), and past the units listed every row.
    long keyPastBatched(long rows) {
        return keptEstimate.keyPast(nextBatched, units.size(), i -> units.get(i).listing().base(),
                i -> units.get(i).listing().metaData().numRows(), this::plannedKept, endKey, rows);
    }

    /// The rows unit `i` keeps once planned, `-1` before.
    private long plannedKept(int i) {
        return units.get(i) instanceof Unit.Planned p
                ? p.plan().matching().rowCount(p.listing().metaData().numRows())
                : -1;
    }

    /// The file of the unit holding `key`, `-1` where none is listed; `consumer` where `key` is
    /// the consumer's first unpublished row, which only moves on, else a key at or past it.
    int fileAt(long key, boolean consumer) {
        int i = consumerUnit;
        while (i < units.size()
                && units.get(i).listing().base() + units.get(i).listing().metaData().numRows() <= key) {
            i++;
        }
        if (consumer) {
            consumerUnit = i;
        }
        return i < units.size() ? units.get(i).listing().fileIndex() : -1;
    }

    boolean allBatched() {
        return listed && nextBatched == units.size();
    }

    long endKey() {
        return endKey;
    }

    /// Plans `a` on a job, its page index arrived.
    private void plan(Unit.AwaitingIndex a) {
        UnitListing u = a.listing();
        List<RangeFetch> index = a.listed().window().index();
        // the page filtering events, filled by the job
        List<Event> events = new ArrayList<>();
        scheduler.add(new JobScheduler.Job<>(u.base(), 0, planKind(u), null, buffers -> {
            try {
                return pages.plan(a, index, events);
            }
            catch (Exception e) {
                // the row group's, at no column and no page
                throw columns.withContext(e, u.file().name(), u.rowGroup());
            }
        }, plan -> {
            List<Event> all = new ArrayList<>(a.events());
            all.addAll(events);
            set(new Unit.Planned(u, plan, List.copyOf(all)));
            batchPlanned(index);
        }, t -> {
            fetcher.readersDone(index);
            readFailure.failAt(u.base(), t);
        }));
    }

    /// The kind of `u`'s pruning and plan jobs: planning ahead leads to fetches, whose latency it
    /// hides; the plan of a unit whose file holds every byte leads to none, and is held to the
    /// look-ahead like the decodes it leads to, so that the read does not hold every unit's pages
    /// at once.
    private static JobScheduler.Kind planKind(UnitListing u) {
        return HeldBytes.inMemory(u.file()) ? JobScheduler.Kind.DECODE : JobScheduler.Kind.PLAN;
    }

    /// Commits what pruning reports of the files and units the consumer has reached, its first
    /// unpublished row at `key`, and lets go of the units the consumer has passed.
    void report(long key) {
        for (; reportedFiles < fileReports.size() && fileReports.get(reportedFiles).base() <= key; reportedFiles++) {
            fileReports.get(reportedFiles).event().commit();
        }
        for (; reportedUnits < units.size() && events(units.get(reportedUnits)) != null
                && units.get(reportedUnits).listing().base() <= key; reportedUnits++) {
            events(units.get(reportedUnits)).forEach(Event::commit);
        }
        // a unit is read by the cursors from the consumer's, the next to report and the next to
        // batch on: one before all three is let go of, its plan with it
        for (int last = Math.min(consumerUnit, Math.min(reportedUnits, nextBatched)); passedUnits < last; passedUnits++) {
            units.set(passedUnits, null);
        }
    }

    /// What `u`'s pruning reports, `null` before it is planned.
    private static List<Event> events(Unit u) {
        return switch (u) {
            case Unit.Planned p -> p.events();
            case Unit.Batched i -> i.events();
            default -> null;
        };
    }

    /// A file's statistics report, and the key of its first row.
    private record FileReport(Event event, long base) {
    }

    /// Charges the dictionary `u`'s dictionary pruning decoded for `column` as the column takes it,
    /// against the batches `u` places; a charge over the read's limit fails the read at the unit.
    private void chargePreloaded(Unit.Batched u, int column) {
        try {
            columns.chargeDictionary(column, u, () -> {
                columns.workingSet(column).dictionary(u.plan().preloaded()[column]);
                return null;
            });
        }
        catch (RuntimeException e) {
            readFailure.failAt(u.listing().base(), column, columns.withContext(column, e, u.listing().file().name(),
                    u.listing().rowGroup(), ExceptionContext.UNKNOWN_PAGE));
        }
    }

    /// A unit is planned, its planning done with `index`: batches the units planned in unit
    /// order, up to the read's failure.
    private void batchPlanned(List<RangeFetch> index) {
        fetcher.readersDone(index);
        while (nextBatched < units.size() && units.get(nextBatched) instanceof Unit.Planned p
                && p.listing().base() < readFailure.row()) {
            nextBatched++;
            takeBatches(p);
        }
        if (allBatched()) {
            publisher.allPublishable();
        }
    }

    /// Takes the unit's batches from the batch plan and hands them to [BatchPublisher], which
    /// makes each as the read reaches it, its first rows continuing the batch the unit before left
    /// open where the plan says so; then hands its pages and ranges to [PageTracker].
    private void takeBatches(Unit.Planned planned) {
        UnitListing u = planned.listing();
        BatchPlan.RowGroupBatches batches;
        try {
            batches = batchPlan.plan(u.index(), u.fileIndex(), u.file().name(), u.alwaysMatches(),
                    planned.plan().matching().rowCount(u.metaData().numRows()));
        }
        catch (ParquetReadException e) {
            // a row count the plan cannot take: the read fails at the unit's first row, and no unit
            // after it is batched
            readFailure.failAt(u.base(), columns.withContext(e, u.file().name(), u.rowGroup()));
            return;
        }
        // a unit of which the batches read no row (dropped, or past the read's row limit) reads no page
        Unit.Batched p = new Unit.Batched(u, batches.segmentCount() == 0
                ? UnitPlan.empty(schemas.length, planned.plan().matching()) : planned.plan(), batches, planned.events());
        set(p);
        if (batches.endsRead()) {
            // the row cap ends the read in it: no further file is listed
            budget = 0;
        }
        BitSet skipped = new BitSet(schemas.length);
        for (int c = 0; c < schemas.length; c++) {
            if (pages.skips(u, c)) {
                skipped.set(c);
            }
        }
        publisher.add(batches, u.base(), skipped);
        for (int c = 0; c < schemas.length; c++) {
            if (p.plan().preloaded()[c] != null) {
                chargePreloaded(p, c);
            }
        }
        PageTracker.ScanInput scans = tracker.track(p, skipped);
        for (int c = 0; c < schemas.length; c++) {
            if (p.plan().pages()[c] == null) {
                scanner.scan(p, c, scans.placements()[c], scans.ranges());
            }
        }
        tracker.handedToScans(scans);
    }
}
