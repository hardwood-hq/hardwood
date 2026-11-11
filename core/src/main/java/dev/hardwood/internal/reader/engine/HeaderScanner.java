/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.predicate.BoundsReadability;
import dev.hardwood.internal.predicate.LogContext;
import dev.hardwood.internal.predicate.PageDropPredicates;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.decode.PageFormatProbe;
import dev.hardwood.internal.reader.fetch.CoalescingPolicy;
import dev.hardwood.internal.reader.fetch.FileRange;
import dev.hardwood.internal.reader.footer.FileColumnOrdinals;
import dev.hardwood.internal.reader.plan.NeededPage;
import dev.hardwood.internal.reader.select.PageRowMask;
import dev.hardwood.internal.reader.step.Parsed;
import dev.hardwood.jfr.RowGroupScannedEvent;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.metadata.PageType;
import dev.hardwood.reader.ParquetReadException;

/// Scans each column chunk without an OffsetIndex header by header as its bytes arrive, on a job,
/// the chain Parquet forces within such a chunk, and hands the pages it finds to their column's
/// placement in page order. A scan holds each range of its chunk until it has passed it, and its
/// dictionary until it ends; its failure is placed at the first row it has not passed. A page its
/// inline statistics prove holds no matching row is handed on as nulls, read from no bytes.
///
/// Under a row limit a chunk's ranges are its first part only: a scan that reaches their end
/// fetches the rest a piece at a time, and a scan stops once its column needs no further row (a
/// flat column's pages cover the limit or pass the last row a page filter keeps, or its placement
/// has placed its last row). Used under the read's lock, except the scan's work.
final class HeaderScanner {

    /// The scan of one chunk: where it stands, in bytes, values and pages.
    private static final class Scan {
        private final Unit.Batched unit;
        private final int column;
        private final PagePlacer.ChunkPlacement placement;
        private final List<RangeFetch> ranges;
        private final long end;
        private final long values;
        private long position;
        private long valuesRead;
        private int pages;
        private ChunkDictionary dictionary;
        /// the AND-necessary leaves whose inline statistics drop a page, and their readability
        private List<ResolvedPredicate> leaves = List.of();
        private BoundsReadability readability = BoundsReadability.ALL;
        /// the first range the scan still holds, and how far the bytes its last job saw had arrived
        /// without a gap: a job walks as far as they reach, so the scan runs again only once more
        /// has arrived
        private int held;
        private long scannedTo = -1;
        private boolean running;
        private boolean ended;

        Scan(Unit.Batched unit, int column, PagePlacer.ChunkPlacement placement, List<RangeFetch> ranges,
             long start, long end, ChunkDictionary dictionary) {
            this.unit = unit;
            this.column = column;
            this.placement = placement;
            this.ranges = ranges;
            this.end = end;
            this.position = start;
            this.values = unit.listing().chunks()[column].metaData().numValues();
            this.dictionary = dictionary;
        }

        Unit.Batched unit() {
            return unit;
        }

        int column() {
            return column;
        }

        PagePlacer.ChunkPlacement placement() {
            return placement;
        }

        /// Every range of the chunk the scan has read or reads, in offset order.
        List<RangeFetch> ranges() {
            return ranges;
        }

        /// The ranges the scan still holds, from the first it has not passed.
        List<RangeFetch> held() {
            return ranges.subList(held, ranges.size());
        }

        /// The chunk's end.
        long end() {
            return end;
        }

        /// The values the chunk's metadata declares.
        long values() {
            return values;
        }

        /// The offset of the first header the scan has not read.
        long position() {
            return position;
        }

        /// The values of the data pages the scan has passed.
        long valuesRead() {
            return valuesRead;
        }

        /// The data pages the scan has passed.
        int pages() {
            return pages;
        }

        /// The chunk's dictionary, `null` until the scan finds it where the probe did not decode it.
        ChunkDictionary dictionary() {
            return dictionary;
        }

        List<ResolvedPredicate> leaves() {
            return leaves;
        }

        BoundsReadability readability() {
            return readability;
        }

        /// A page's inline statistics drop it where `leaves`, read by `readability`, prove it
        /// holds no matching row.
        void dropsBy(List<ResolvedPredicate> leaves, BoundsReadability readability) {
            this.leaves = leaves;
            this.readability = readability;
        }

        /// The scan found the chunk's dictionary.
        void found(ChunkDictionary dictionary) {
            this.dictionary = dictionary;
        }

        /// The scan passed a data page of `values` values.
        void passedDataPage(int values) {
            pages++;
            valuesRead += values;
        }

        /// How far the bytes its last job saw had arrived without a gap.
        long scannedTo() {
            return scannedTo;
        }

        /// Its next job walks the bytes that arrived without a gap up to `arrivedTo`.
        void start(long arrivedTo) {
            scannedTo = arrivedTo;
            running = true;
        }

        /// Whether a job of it is in flight.
        boolean running() {
            return running;
        }

        /// Its job ended.
        void idle() {
            running = false;
        }

        /// It read the headers up to `position`; returns the ranges it held and has now passed.
        List<RangeFetch> advanceTo(long position) {
            this.position = position;
            int from = held;
            while (held < ranges.size() && ranges.get(held).range().end() <= position) {
                held++;
            }
            return List.copyOf(ranges.subList(from, held));
        }

        /// It reads `range` too, the next piece of the chunk.
        void add(RangeFetch range) {
            ranges.add(range);
        }

        /// Whether it is done with its chunk.
        boolean ended() {
            return ended;
        }

        /// It is done with its chunk; returns the ranges it still held, which it holds no longer.
        List<RangeFetch> stop() {
            ended = true;
            List<RangeFetch> released = List.copyOf(held());
            held = ranges.size();
            return released;
        }
    }

    /// A page header found at `offset`, `length` bytes with its body.
    private record Found(PageHeader header, long offset, int length) {
    }

    /// How far a scan's job got.
    private record Step(List<Found> found, long position) {
    }

    private final JobScheduler scheduler;
    private final RangeFetcher fetcher;
    private final PageTracker tracker;
    private final PagePlacer placer;
    private final BatchPublisher publisher;
    /// per reference column, the leaves a page's inline statistics are decided by; empty without
    /// a filter or where metadata filtering is off
    private final Map<Integer, List<ResolvedPredicate>> dropLeaves;
    private final int maxSpan = CoalescingPolicy.maxSpanBytes();

    HeaderScanner(JobScheduler scheduler, RangeFetcher fetcher, PageTracker tracker, PagePlacer placer, BatchPublisher publisher,
                  Map<Integer, List<ResolvedPredicate>> dropLeaves) {
        this.scheduler = scheduler;
        this.fetcher = fetcher;
        this.tracker = tracker;
        this.placer = placer;
        this.publisher = publisher;
        this.dropLeaves = dropLeaves;
    }

    /// Scans `column`'s chunk of `u`, whose bytes lie in those of `ranges` it overlaps, into
    /// `chunk`.
    void scan(Unit.Batched u, int column, PagePlacer.ChunkPlacement placement, List<RangeFetch> ranges) {
        ColumnChunk meta = u.listing().chunks()[column];
        long start = meta.chunkStartOffset();
        long end = start + CoalescingPolicy.chunkLength(meta, u.listing().rowGroup(), u.listing().file().name(), placer.schema(column));
        ChunkDictionary dictionary = null;
        if (u.plan().preloaded()[column] != null) {
            // decoded by the unit's probe: the scan starts at the first data page
            dictionary = ChunkDictionary.preloaded(u.plan().preloaded()[column]);
            start = u.plan().dictionaryEnd()[column];
        }
        List<RangeFetch> overlapping = new ArrayList<>();
        for (RangeFetch r : ranges) {
            if (r.range().start() < end && r.range().end() > start) {
                overlapping.add(r);
            }
        }
        Scan s = new Scan(u, column, placement, overlapping, start, end, dictionary);
        // inline page statistics are bounds like any other: a column whose bounds this file records
        // in an order this reader cannot read drops no page; the leaves are in the reference file's
        // ordinals, so the column's order travels with them
        FileColumnOrdinals ordinals = u.listing().columnOrdinals();
        int referenceOrdinal = placer.schema(column).columnIndex();
        int fileOrdinal = ordinals.fileOrdinal(referenceOrdinal);
        if (ordinals.boundsReadability().readable(fileOrdinal)) {
            s.dropsBy(dropLeaves.getOrDefault(referenceOrdinal, List.of()),
                    BoundsReadability.ofOneColumn(ordinals.boundsReadability().int96TimestampOrder(fileOrdinal)));
        }
        for (RangeFetch r : overlapping) {
            r.addReader();
        }
        next(s);
    }

    /// Scans on where the bytes at the scan's position have arrived; where no byte past those it
    /// walked has arrived, waits for the first range it lacks and, woken, looks again.
    private void next(Scan s) {
        if (s.running() || s.ended()) {
            return;
        }
        if (s.position() >= s.end() || s.valuesRead() >= s.values()) {
            end(s);
            return;
        }
        if (needsNoFurtherRow(s)) {
            finish(s);
            return;
        }
        List<RangeFetch> arrived = new ArrayList<>();
        for (RangeFetch r : s.held()) {
            if (r.failure() != null) {
                failed(s, r.failure());
                return;
            }
            if (r.bytes() == null) {
                break;
            }
            arrived.add(r);
        }
        long arrivedTo = arrived.isEmpty() ? -1 : arrived.getLast().range().end();
        if (arrivedTo == s.scannedTo()) {
            // nothing new to walk: it waits for the first range it lacks, or fetches the rest
            if (arrived.size() < s.held().size()) {
                s.held().get(arrived.size()).output().whenEnded(() -> next(s));
            }
            else {
                fetchRest(s);
            }
            return;
        }
        long position = s.position();
        long values = s.values() - s.valuesRead();
        long end = s.end();
        // a page is handed on only where the ranges held reach its end
        long held = s.ranges().getLast().range().end();
        s.start(arrivedTo);
        scheduler.add(new JobScheduler.Job<>(key(s), 0, JobScheduler.Kind.CPU, null,
                buffers -> walk(arrived, position, end, held, values), step -> scanned(s, step), t -> failed(s, t)));
    }

    /// Walks the headers from `position` as far as `arrived` reaches, up to `end` or `values`
    /// values, stopping before a page that ends past `held`, where the ranges held end; on a job.
    private static Step walk(List<RangeFetch> arrived, long position, long end, long held, long values)
            throws Exception {
        List<Found> found = new ArrayList<>();
        while (position < end && values > 0) {
            Parsed<PageFormatProbe.HeaderAt> read = PageFormatProbe.readHeader(
                    (offset, length) -> RangeFetch.read(arrived, offset, length), position, end - position,
                    PageFormatProbe.INITIAL_PEEK_SIZE, "");
            if (read.needsBytes()) {
                return new Step(found, position);
            }
            PageHeader header = read.value().header();
            int length = read.value().length() + header.compressedPageSize();
            if (position + length > held) {
                return new Step(found, position);
            }
            found.add(new Found(header, position, length));
            position += length;
            values -= switch (header.type()) {
                case DATA_PAGE -> header.dataPageHeader().numValues();
                case DATA_PAGE_V2 -> header.dataPageHeaderV2().numValues();
                default -> 0;
            };
        }
        return new Step(found, position);
    }

    /// Hands the pages a step found to the chunk, lets go of the ranges it passed, and scans on.
    private void scanned(Scan s, Step step) {
        s.idle();
        for (Found f : step.found()) {
            PageType type = f.header().type();
            if (type == PageType.DICTIONARY_PAGE && s.pages() == 0 && s.dictionary() == null) {
                ChunkDictionary dictionary = new ChunkDictionary();
                s.found(dictionary);
                TrackedPage dictionaryPage = new TrackedPage(s.unit(), s.column(), key(s),
                        NeededPage.dictionary(f.offset(), f.length()), within(s, f), dictionary, false);
                fetcher.keyAt(dictionaryPage.ranges(), dictionaryPage.key());
                tracker.addDictionaryPage(dictionaryPage);
            }
            else if (type == PageType.DATA_PAGE || type == PageType.DATA_PAGE_V2) {
                int values = type == PageType.DATA_PAGE ? f.header().dataPageHeader().numValues()
                        : f.header().dataPageHeaderV2().numValues();
                // a page past the last row the column needs is passed, not placed, and its values
                // still count toward the chunk's
                if (!needsNoFurtherRow(s)) {
                    placeScanned(s, f, values);
                }
                s.passedDataPage(values);
            }
        }
        fetcher.readersDone(s.advanceTo(step.position()));
        next(s);
        tracker.placed(s.column());
    }

    /// Hands on a data page the scan found, masked by the page filter, unless the filter keeps
    /// none of its rows.
    private void placeScanned(Scan s, Found f, int values) {
        // a flat page's rows are its values: a page filter masks them, and drops a page it keeps
        // no row of
        long row = row(s);
        PageRowMask mask = placer.nested(s.column()) ? PageRowMask.ALL
                : s.unit().plan().matching().maskForPage(row, row + values);
        if (mask == null) {
            return;
        }
        boolean nulls = PageDropPredicates.canDropPage(s.leaves(), f.header(), placer.schema(s.column()),
                s.readability(), new LogContext(s.unit().listing().file().name(), s.unit().listing().rowGroup(),
                        placer.schema(s.column()).fieldPath(), s.pages()));
        // a page of nulls reads no bytes and no dictionary, and stands for the rows its mask
        // keeps, so its siblings agree on the rows
        NeededPage page = new NeededPage(new PageLocation(f.offset(), f.length(), row),
                nulls ? PageRowMask.ALL : mask, s.pages(), values);
        TrackedPage tracked = nulls
                ? new TrackedPage(s.unit(), s.column(), key(s), page, List.of(), null, true)
                : new TrackedPage(s.unit(), s.column(), key(s), page, within(s, f), s.dictionary(), true);
        if (nulls) {
            tracked.standsForNulls(mask.isAll() ? values : mask.totalRecords());
        }
        // the ranges were keyed by where they lie in the chunk; the page they hold serves its own
        // row, which may be lower
        fetcher.keyAt(tracked.ranges(), tracked.key());
        tracker.add(s.placement(), tracked);
    }

    /// Whether the scan's column needs no further row of the chunk: a flat column's pages cover
    /// the row limit's share of the unit or pass the last row its page filter keeps, or the
    /// column's placement has placed its last row.
    private boolean needsNoFurtherRow(Scan s) {
        if (placer.placementEnded(s.column())) {
            return true;
        }
        if (placer.nested(s.column())) {
            return false;
        }
        return row(s) >= s.unit().listing().limit()
                || !s.unit().plan().matching().isAll() && row(s) >= s.unit().plan().matching().endRow();
    }

    /// The first row of the chunk the scan has not passed: a flat column's rows are its values,
    /// while a column decoded through levels stays at its unit's first, whose rows a header does
    /// not give.
    private long row(Scan s) {
        return placer.nested(s.column()) ? 0 : s.valuesRead();
    }

    /// The key of the first row the scan has not passed.
    private long key(Scan s) {
        return s.unit().key(row(s));
    }

    /// The scan has passed every byte it holds and its chunk goes on: the next piece of it, at
    /// most S, is fetched at the scan's row, held by the scan alone.
    private void fetchRest(Scan s) {
        long from = s.ranges().isEmpty() ? s.position() : s.ranges().getLast().range().end();
        if (from >= s.end()) {
            return;
        }
        int length = Math.toIntExact(Math.min(maxSpan, s.end() - from));
        RangeFetch r = RangeFetch.of(s.unit().listing().file(), new FileRange(from, length,
                "rg=" + s.unit().listing().rowGroup() + " col=" + placer.schema(s.column()).name()));
        r.addReader();
        s.add(r);
        if (!r.hasArrived()) {
            fetcher.fetch(s.unit().listing().file(), r, key(s));
        }
        r.output().whenEnded(() -> next(s));
    }

    private static List<RangeFetch> within(Scan s, Found f) {
        List<RangeFetch> found = new ArrayList<>(2);
        for (RangeFetch r : s.ranges()) {
            if (r.range().start() < f.offset() + f.length() && r.range().end() > f.offset()) {
                found.add(r);
            }
        }
        return found;
    }

    /// The chunk has no page left: its value count must be the metadata's.
    private void end(Scan s) {
        if (s.valuesRead() != s.values()) {
            failed(s, new ParquetReadException("Value count mismatch: metadata declares " + s.values()
                    + " values but pages contain " + s.valuesRead()));
            return;
        }
        finish(s);
    }

    /// The scan is done with its chunk, at its end or where its column needs no further row.
    private void finish(Scan s) {
        PageTracker.reportPagesLocated(s.unit().listing().file().name(), s.unit().listing().rowGroup(), placer.schema(s.column()), s.pages(),
                RowGroupScannedEvent.STRATEGY_SEQUENTIAL);
        release(s);
        tracker.placed(s.column());
    }

    /// A scan failed: the failure is placed at the first row it has not passed, and at the page it
    /// was on.
    private void failed(Scan s, Throwable t) {
        s.idle();
        Throwable placed = t instanceof Exception e
                ? placer.placed(s.column(), e, s.unit().listing().file().name(), s.unit().listing().rowGroup(), s.pages()) : t;
        publisher.failAt(key(s), s.column(), placed);
        s.placement().failed(placed);
        release(s);
        tracker.placed(s.column());
    }

    /// Lets go of what the scan holds, and ends its chunk.
    private void release(Scan s) {
        s.placement().allAdded();
        fetcher.readersDone(s.stop());
    }
}
