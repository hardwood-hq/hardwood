/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;

import dev.hardwood.InputFile;
import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.io.HeldBytes;
import dev.hardwood.internal.reader.decode.DataPage;
import dev.hardwood.internal.reader.fetch.FileRange;
import dev.hardwood.internal.reader.footer.FileColumnOrdinals;
import dev.hardwood.internal.reader.plan.BatchPlan;
import dev.hardwood.internal.reader.plan.IndexWindow;
import dev.hardwood.internal.reader.plan.RowGroupPruning;
import dev.hardwood.internal.reader.plan.UnitListing;
import dev.hardwood.internal.reader.plan.UnitPlan;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import jdk.jfr.Event;

/// Plans a read's row groups, moving each through its states ([Unit.State]). Lists each file's row
/// groups that statistics and the read's skip and limit keep, as [Unit]s; probes each unit's Bloom
/// filters and dictionaries; reads its window's page index (sliced from the file's tail where it
/// holds it), from which [PagePlanner] plans its pages and ranges into a [UnitPlan]; and batches the units in unit
/// order, so batches stay in row order whichever plan finishes first. A unit whose planning fails fails the read at its first row, between two units. Used
/// under the read's lock, except the jobs' work.
final class RowGroupPlanner {

    /// The fetch of an [IndexWindow] in the read: its units, the `count` from the one at `first`,
    /// and its requests: its Bloom filters, read first, and its page index, read once a unit of it
    /// survives its Bloom filters and dictionaries; each held by every unit of it.
    static final class WindowFetch {
        private final int first;
        private final int count;
        private final List<RangeFetch> bloomFilters;
        private final List<RangeFetch> index;
        private boolean indexRead;

        WindowFetch(int first, int count, List<RangeFetch> bloomFilters, List<RangeFetch> index) {
            this.first = first;
            this.count = count;
            this.bloomFilters = bloomFilters;
            this.index = index;
        }

        /// The index of its first unit.
        int first() {
            return first;
        }

        /// The index past its last unit.
        int end() {
            return first + count;
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

    /// Lists the row groups of a file, `file` with `rowGroups`, that its statistics keep, as
    /// [#addFile(int, InputFile, FileSchema, RowGroupPruning.FilteredRowGroups, FileColumnOrdinals)].
    ///
    /// @throws UncheckedIOException if the file's length cannot be read
    void addFile(int fileIndex, InputFile file, FileSchema fileSchema, List<RowGroup> rowGroups,
                 FileColumnOrdinals ordinals) {
        try {
            addFile(fileIndex, file, fileSchema, pruning.filterRowGroups(rowGroups, file, ordinals), ordinals);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /// Lists `filtered`, the row groups of a file, `file`, that its statistics keep, as units
    /// behind those listed, each column's chunk at its ordinal in the file, and reads their bloom
    /// filters and page indexes, a window of units at a time ([IndexWindow]).
    ///
    /// @throws UncheckedIOException if the file's length cannot be read
    void addFile(int fileIndex, InputFile file, FileSchema fileSchema, RowGroupPruning.FilteredRowGroups filtered,
                 FileColumnOrdinals ordinals) {
        try {
            int from = units.size();
            List<UnitListing> listings = new ArrayList<>();
            List<IndexWindow.Member> members = new ArrayList<>();
            if (filtered.event() != null) {
                fileReports.add(new FileReport(filtered.event(), endKey));
            }
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
                        from + listings.size(),
                        endKey, kept.alwaysMatches(), kept.leafDecisions(), kept.bloomFilterColumns(), chunks,
                        leading + Math.min(rg.numRows() - leading, budget));
                BitSet columnIndexes = RowGroupPruning.columnIndexColumns(u, pruning.pageFiltering(u));
                BitSet offsetIndexes = (BitSet) columnIndexes.clone();
                for (int c = 0; c < schemas.length; c++) {
                    if (!pages.filterOnly(c) || !kept.alwaysMatches()) {
                        offsetIndexes.set(ordinals.fileOrdinal(schemas[c].columnIndex()));
                    }
                }
                for (int o = offsetIndexes.nextSetBit(0); o >= 0; o = offsetIndexes.nextSetBit(o + 1)) {
                    if (rg.columns().get(o).offsetIndexOffset() == null) {
                        offsetIndexes.clear(o);
                    }
                }
                budget -= u.limit() - leading;
                listings.add(u);
                endKey += rg.numRows();
                // a row group with a chunk in another file fails before any of its indexes is read
                members.add(u.storedInThisFile() ? new IndexWindow.Member(rg, kept.fileRowGroupIndex(),
                        offsetIndexes, columnIndexes, u.bloomFilterColumns())
                        : new IndexWindow.Member(rg, kept.fileRowGroupIndex(), new BitSet(), new BitSet(), new BitSet()));
            }
            if (members.isEmpty()) {
                return;
            }
            int first = from;
            for (IndexWindow window : IndexWindow.plan(IndexWindow.budgetBytes(), file, members)) {
                int count = window.memberCount();
                WindowFetch w = new WindowFetch(first, count, ranges(file, window.bloomFilterRanges(), count),
                        ranges(file, window.indexRanges(), count));
                for (int i = first; i < first + count; i++) {
                    // a row group with a chunk in another file reads no index: it fails first
                    IndexWindow.Member m = members.get(i - from);
                    units.add(new Unit.Listed(listings.get(i - from), m.offsetIndexColumns(), m.columnIndexColumns(), w));
                }
                first += count;
                fetch(units.get(w.first()).listing(), w.bloomFilters(), () -> {
                    for (int i = w.first(); i < w.end(); i++) {
                        bloomFiltersArrived((Unit.Listed) units.get(i));
                    }
                });
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
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
        for (RangeFetch r : ranges) {
            if (!r.output().hasValue()) {
                fetcher.fetch(u.file(), r, u.base());
            }
        }
        whenArrived(u, ranges, then);
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

    /// `l`'s Bloom filters have arrived: it is probed where they or its dictionaries may drop it,
    /// and waits for its page index otherwise.
    private void bloomFiltersArrived(Unit.Listed l) {
        if (RowGroupPruning.droppableOnArrival(l.listing())) {
            probe(l.probing());
            return;
        }
        fetcher.readersDone(l.window().bloomFilters());
        awaitIndex(l.nothingToProbe());
    }

    /// Probes `p`'s Bloom filters and dictionaries on a job: the dictionary bytes a pass lacks are
    /// fetched and it is probed again, until it is decided. A pass is charged the buffers of the
    /// dictionary parses it may run ([#parseBytes]).
    private void probe(Unit.Probing p) {
        set(p);
        UnitListing u = p.listing();
        p.missing().clear();
        long cost = parseBytes(p);
        scheduler.add(new JobScheduler.Job<>(u.base(), cost, planKind(u), cost > 0 ? new Charge() : null, buffers -> {
            // its chunks lie in the file before a dictionary of them is read
            pages.extents(u);
            return pruning.probeOnArrival(u, p.bytes(), p.bytes(), p.probe());
        }, arrival -> {
            if (arrival.dropped() || p.missing().isEmpty()) {
                probed(p, arrival);
                return;
            }
            List<RangeFetch> more = ranges(u.file(), p.missing(), 1);
            p.fetching(more);
            fetch(u, more, () -> probe(p));
        }, t -> readFailure.failAt(u.base(), t)));
    }

    /// The buffers of the dictionary parses `p`'s next pass may run, from the headers of the
    /// dictionary pages it holds and has not parsed ([DataPage#taskBytes]), each where it exceeds
    /// [DecodeBuffers#RETAINED_BYTES]: R holds them while the pass runs, unless need forces it, so
    /// that the probes of several row groups parse such dictionaries at once only within R. A
    /// smaller parse is not charged, as a context keeps that much per thread anyway.
    private long parseBytes(Unit.Probing p) {
        long bytes = 0;
        for (RowGroupPruning.HeldDictionary d : RowGroupPruning.dictionariesToParse(p.listing(), p.held(), p.probe())) {
            long task = DataPage.taskBytes(d.header(), d.schema());
            bytes += task > DecodeBuffers.RETAINED_BYTES ? task : 0;
        }
        return bytes;
    }

    /// `p`'s Bloom filters and dictionaries decided: a unit they dropped is planned with no row,
    /// one they kept waits for its page index.
    private void probed(Unit.Probing p, RowGroupPruning.Arrival arrival) {
        WindowFetch w = p.listed().window();
        fetcher.readersDone(w.bloomFilters());
        fetcher.readersDone(p.probed());
        if (arrival.dropped()) {
            set(p.dropped(arrival.dropEvent(), schemas.length));
            batchPlanned(w.index());
            return;
        }
        awaitIndex(p.kept(arrival.dropEvent()));
    }

    /// `a` waits for its window's page index, which the first unit of the window to wait for it
    /// fetches, and is planned once it has arrived.
    private void awaitIndex(Unit.AwaitingIndex a) {
        set(a);
        WindowFetch w = a.listed().window();
        if (w.readIndex()) {
            fetch(units.get(w.first()).listing(), w.index(), () -> plan(a));
        }
        else {
            whenArrived(a.listing(), w.index(), () -> plan(a));
        }
    }

    /// Replaces the unit's state with `u`.
    private void set(Unit u) {
        units.set(u.listing().index(), u);
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
            set(a.planned(plan, events));
            batchPlanned(index);
        }, t -> {
            fetcher.readersDone(index);
            readFailure.failAt(u.base(), t);
        }));
    }

    /// The kind of `u`'s probe and plan: planning ahead leads to fetches, whose latency it hides;
    /// the plan of a unit whose file holds every byte leads to none, and is held to the look-ahead
    /// like the decodes it leads to, so that the read does not hold every unit's pages at once.
    private static JobScheduler.Kind planKind(UnitListing u) {
        return HeldBytes.inMemory(u.file()) ? JobScheduler.Kind.DECODE : JobScheduler.Kind.PLAN;
    }

    /// Commits what pruning reports of the files and units the consumer has reached, its first
    /// unpublished row at `key`.
    void report(long key) {
        for (; reportedFiles < fileReports.size() && fileReports.get(reportedFiles).base() <= key; reportedFiles++) {
            fileReports.get(reportedFiles).event().commit();
        }
        for (; reportedUnits < units.size() && events(units.get(reportedUnits)) != null
                && units.get(reportedUnits).listing().base() <= key; reportedUnits++) {
            events(units.get(reportedUnits)).forEach(Event::commit);
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

    /// Charges the dictionary `u`'s probe decoded for `column` as the column takes it, against the
    /// batches `u` places; a charge over the read's limit fails the read at the unit.
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

    /// Takes the unit's batches from the batch plan, its first rows continuing the batch the unit
    /// before left open where the plan says so, and hands its pages and ranges to [PageTracker].
    private void takeBatches(Unit.Planned planned) {
        UnitListing u = planned.listing();
        BatchPlan.RowGroupBatches batches = batchPlan.plan(u.index(), u.fileIndex(), u.file().name(),
                u.alwaysMatches(), planned.plan().matching().rowCount(u.metaData().numRows()));
        Unit.Batched p = planned.batched(batches);
        set(p);
        if (batches.endsRead()) {
            // the row cap ends the read in it: no further file is listed
            budget = 0;
        }
        long first = u.base();
        for (int i = 0; i < batches.segmentCount(); i++) {
            if (i > 0 || batches.continuedRows() == 0) {
                publisher.add(new Batch(first, batches.readRowsBefore() + first - u.base(), schemas.length));
            }
            first += batches.segmentRows(i);
            publisher.last().extendTo(first);
            for (int c = 0; c < schemas.length; c++) {
                if (pages.skips(u, c)) {
                    publisher.skipLast(c);
                }
            }
        }
        for (int c = 0; c < schemas.length; c++) {
            if (p.plan().preloaded()[c] != null) {
                chargePreloaded(p, c);
            }
        }
        PageTracker.ScanInput scans = tracker.track(p);
        for (int c = 0; c < schemas.length; c++) {
            if (p.plan().pages()[c] == null) {
                scanner.scan(p, c, scans.placements()[c], scans.ranges());
            }
        }
        tracker.handedToScans(scans);
    }
}
