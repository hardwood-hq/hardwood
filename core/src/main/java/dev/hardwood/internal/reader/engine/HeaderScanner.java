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

import dev.hardwood.internal.io.HeldBytes;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.predicate.BoundsReadability;
import dev.hardwood.internal.predicate.LogContext;
import dev.hardwood.internal.predicate.PageDropPredicates;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.decode.PageFormatProbe;
import dev.hardwood.internal.reader.fetch.CoalescingPolicy;
import dev.hardwood.internal.reader.fetch.Span;
import dev.hardwood.internal.reader.footer.FileColumnOrdinals;
import dev.hardwood.internal.reader.plan.NeededPage;
import dev.hardwood.internal.reader.select.PageRowMask;
import dev.hardwood.internal.reader.step.Parsed;
import dev.hardwood.jfr.RowGroupScannedEvent;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.metadata.PageType;
import dev.hardwood.reader.ParquetReadException;

/// The chain Parquet forces within a column chunk without an OffsetIndex: a read scans it
/// header by header as its bytes arrive, on a job, and hands the pages it finds to their column's
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
        final Unit.Installed unit;
        final int column;
        final PagePlacer.Chunk chunk;
        final List<PageFetcher.Range> ranges;
        final long end;
        final long values;
        long position;
        long valuesRead;
        int pages;
        PageFetcher.Range dictionary;
        /// the AND-necessary leaves whose inline statistics drop a page, and their readability
        List<ResolvedPredicate> leaves = List.of();
        BoundsReadability readability = BoundsReadability.ALL;
        /// the first range the scan still holds, and how far the bytes its last job saw had arrived
        /// without a gap: a job walks as far as they reach, so the scan runs again only once more
        /// has arrived
        int held;
        long scannedTo = -1;
        boolean running;
        boolean ended;

        Scan(Unit.Installed unit, int column, PagePlacer.Chunk chunk, List<PageFetcher.Range> ranges,
             long start, long end) {
            this.unit = unit;
            this.column = column;
            this.chunk = chunk;
            this.ranges = ranges;
            this.end = end;
            this.position = start;
            this.values = unit.listing().chunks()[column].metaData().numValues();
        }

    }

    /// A page header found at `offset`, `length` bytes with its body.
    private record Found(PageHeader header, long offset, int length) {
    }

    /// How far a scan's job got.
    private record Step(List<Found> found, long position) {
    }

    private final JobScheduler scheduler;
    private final PageFetcher fetcher;
    private final PagePlacer placer;
    private final BatchQueue queue;
    /// per reference column, the leaves a page's inline statistics are decided by; empty without
    /// a filter or where metadata filtering is off
    private final Map<Integer, List<ResolvedPredicate>> dropLeaves;
    private final int maxSpan = CoalescingPolicy.maxSpanBytes();

    HeaderScanner(JobScheduler scheduler, PageFetcher fetcher, PagePlacer placer, BatchQueue queue,
                  Map<Integer, List<ResolvedPredicate>> dropLeaves) {
        this.scheduler = scheduler;
        this.fetcher = fetcher;
        this.placer = placer;
        this.queue = queue;
        this.dropLeaves = dropLeaves;
    }

    /// Scans `column`'s chunk of `u`, whose bytes lie in those of `ranges` it overlaps, into
    /// `chunk`.
    void scan(Unit.Installed u, int column, PagePlacer.Chunk chunk, List<PageFetcher.Range> ranges) {
        ColumnChunk meta = u.listing().chunks()[column];
        long start = meta.chunkStartOffset();
        long end = start + CoalescingPolicy.chunkLength(meta, u.listing().rowGroup(), u.listing().file().name(), placer.schema(column));
        PageFetcher.Range dictionary = null;
        if (u.plan().preloaded()[column] != null) {
            // decoded by the unit's probe: the scan starts at the first data page
            dictionary = new PageFetcher.Range(null);
            dictionary.values = u.plan().preloaded()[column];
            dictionary.arrived = true;
            dictionary.charge.reader();
            start = u.plan().dictionaryEnd()[column];
        }
        List<PageFetcher.Range> overlapping = new ArrayList<>();
        for (PageFetcher.Range r : ranges) {
            if (r.span.start() < end && r.span.start() + r.span.length() > start) {
                overlapping.add(r);
            }
        }
        Scan s = new Scan(u, column, chunk, overlapping, start, end);
        s.dictionary = dictionary;
        // inline page statistics are bounds like any other: a column whose bounds this file records
        // in an order this reader cannot read drops no page; the leaves are in the reference file's
        // ordinals, so the column's order travels with them
        FileColumnOrdinals ordinals = u.listing().columnOrdinals();
        int referenceOrdinal = placer.schema(column).columnIndex();
        int fileOrdinal = ordinals.fileOrdinal(referenceOrdinal);
        if (ordinals.boundsReadability().readable(fileOrdinal)) {
            s.leaves = dropLeaves.getOrDefault(referenceOrdinal, List.of());
            s.readability = BoundsReadability.ofOneColumn(ordinals.boundsReadability().int96TimestampOrder(fileOrdinal));
        }
        for (PageFetcher.Range r : overlapping) {
            r.charge.reader();
            r.listeners.add(() -> next(s));
        }
        next(s);
    }

    /// Scans on where the bytes at the scan's position have arrived.
    private void next(Scan s) {
        if (s.running || s.ended) {
            return;
        }
        if (s.position >= s.end || s.valuesRead >= s.values) {
            end(s);
            return;
        }
        if (needsNoFurtherRow(s)) {
            finish(s);
            return;
        }
        List<PageFetcher.Range> arrived = new ArrayList<>();
        for (int i = s.held; i < s.ranges.size(); i++) {
            PageFetcher.Range r = s.ranges.get(i);
            if (r.failure != null) {
                failed(s, r.failure);
                return;
            }
            if (r.bytes == null) {
                break;
            }
            arrived.add(r);
        }
        long arrivedTo = arrived.isEmpty() ? -1 : arrived.getLast().span.start() + arrived.getLast().span.length();
        if (arrivedTo == s.scannedTo) {
            if (arrived.size() == s.ranges.size() - s.held) {
                fetchRest(s);
            }
            return;
        }
        s.scannedTo = arrivedTo;
        long position = s.position;
        long values = s.values - s.valuesRead;
        long end = s.end;
        // a page is handed on only where the ranges held reach its end
        PageFetcher.Range lastHeld = s.ranges.getLast();
        long held = lastHeld.span.start() + lastHeld.span.length();
        s.running = true;
        scheduler.add(new JobScheduler.Job(key(s), 0, JobScheduler.Kind.CPU, null, buffers -> {
            Step step = walk(arrived, position, end, held, values);
            return () -> scanned(s, step);
        }, t -> () -> failed(s, t)));
    }

    /// Walks the headers from `position` as far as `arrived` reaches, up to `end` or `values`
    /// values, stopping before a page that ends past `held`, where the ranges held end; on a job.
    private static Step walk(List<PageFetcher.Range> arrived, long position, long end, long held, long values)
            throws Exception {
        List<Found> found = new ArrayList<>();
        while (position < end && values > 0) {
            Parsed<PageFormatProbe.HeaderAt> read = PageFormatProbe.readHeader(
                    (offset, length) -> PageFetcher.read(arrived, offset, length), position, end - position,
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
        s.running = false;
        List<PageFetcher.PageWork> pages = new ArrayList<>();
        for (Found f : step.found()) {
            PageType type = f.header().type();
            if (type == PageType.DICTIONARY_PAGE && s.pages == 0 && s.dictionary == null) {
                s.dictionary = new PageFetcher.Range(null);
                s.dictionary.charge.reader();
                PageFetcher.PageWork dictionaryPage = new PageFetcher.PageWork(s.unit, s.column, key(s),
                        NeededPage.dictionary(f.offset(), f.length()), within(s, f), s.dictionary, false);
                fetcher.keyAt(dictionaryPage.ranges, dictionaryPage.key);
                pages.add(fetcher.hold(dictionaryPage));
            }
            else if (type == PageType.DATA_PAGE || type == PageType.DATA_PAGE_V2) {
                int values = type == PageType.DATA_PAGE ? f.header().dataPageHeader().numValues()
                        : f.header().dataPageHeaderV2().numValues();
                // a page past the last row the column needs is passed, not placed, and its values
                // still count toward the chunk's
                if (!needsNoFurtherRow(s)) {
                    placeScanned(s, f, values, pages);
                }
                s.pages++;
                s.valuesRead += values;
            }
        }
        s.position = step.position();
        while (s.held < s.ranges.size()
                && s.ranges.get(s.held).span.start() + s.ranges.get(s.held).span.length() <= s.position) {
            fetcher.readersDone(List.of(s.ranges.get(s.held++)));
        }
        fetcher.decodeReady(pages);
        next(s);
        fetcher.placed(s.column);
    }

    /// Hands on a data page the scan found, masked by the page filter, unless the filter keeps
    /// none of its rows.
    private void placeScanned(Scan s, Found f, int values, List<PageFetcher.PageWork> pages) {
        // a flat page's rows are its values: a page filter masks them, and drops a page it keeps
        // no row of
        long row = row(s);
        PageRowMask mask = placer.nested(s.column) ? PageRowMask.ALL
                : s.unit.plan().matching().maskForPage(row, row + values);
        if (mask == null) {
            return;
        }
        boolean nulls = PageDropPredicates.canDropPage(s.leaves, f.header(), placer.schema(s.column),
                s.readability, new LogContext(s.unit.listing().file().name(), s.unit.listing().rowGroup(),
                        placer.schema(s.column).fieldPath(), s.pages));
        // a page of nulls reads no bytes and no dictionary, and stands for the rows its mask
        // keeps, so its siblings agree on the rows
        NeededPage page = new NeededPage(new PageLocation(f.offset(), f.length(), row),
                nulls ? PageRowMask.ALL : mask, s.pages, values);
        PageFetcher.PageWork pw = nulls
                ? new PageFetcher.PageWork(s.unit, s.column, key(s), page, List.of(), null, true)
                : new PageFetcher.PageWork(s.unit, s.column, key(s), page, within(s, f), s.dictionary, true);
        if (nulls) {
            pw.placeholder = mask.isAll() ? values : mask.totalRecords();
        }
        // the ranges were keyed by where they lie in the chunk; the page they hold serves its own
        // row, which may be lower
        fetcher.keyAt(pw.ranges, pw.key);
        pages.add(fetcher.add(s.chunk, pw));
    }

    /// Whether the scan's column needs no further row of the chunk: a flat column's pages cover
    /// the row limit's share of the unit or pass the last row its page filter keeps, or the
    /// column's placement has placed its last row.
    private boolean needsNoFurtherRow(Scan s) {
        if (placer.placementEnded(s.column)) {
            return true;
        }
        if (placer.nested(s.column)) {
            return false;
        }
        return row(s) >= s.unit.listing().limit()
                || !s.unit.plan().matching().isAll() && row(s) >= s.unit.plan().matching().endRow();
    }

    /// The first row of the chunk the scan has not passed: a flat column's rows are its values,
    /// while a column decoded through levels stays at its unit's first, whose rows a header does
    /// not give.
    private long row(Scan s) {
        return placer.nested(s.column) ? 0 : s.valuesRead;
    }

    /// The key of the first row the scan has not passed.
    private long key(Scan s) {
        return s.unit.key(row(s));
    }

    /// The scan has passed every byte it holds and its chunk goes on: the next piece of it, at
    /// most S, is fetched at the scan's row, held by the scan alone.
    private void fetchRest(Scan s) {
        long from = s.ranges.isEmpty() ? s.position
                : s.ranges.getLast().span.start() + s.ranges.getLast().span.length();
        if (from >= s.end) {
            return;
        }
        int length = Math.toIntExact(Math.min(maxSpan, s.end - from));
        PageFetcher.Range r = new PageFetcher.Range(new Span(from, length,
                "rg=" + s.unit.listing().rowGroup() + " col=" + placer.schema(s.column).name(), 1));
        r.charge.reader();
        r.listeners.add(() -> next(s));
        s.ranges.add(r);
        r.bytes = HeldBytes.slice(s.unit.listing().file(), from, length);
        if (r.bytes != null) {
            r.arrived = true;
            next(s);
            return;
        }
        fetcher.addFetch(s.unit.listing().file(), r, key(s));
    }

    private static List<PageFetcher.Range> within(Scan s, Found f) {
        List<PageFetcher.Range> found = new ArrayList<>(2);
        for (PageFetcher.Range r : s.ranges) {
            if (r.span.start() < f.offset() + f.length() && r.span.start() + r.span.length() > f.offset()) {
                found.add(r);
            }
        }
        return found;
    }

    /// The chunk has no page left: its value count must be the metadata's.
    private void end(Scan s) {
        if (s.valuesRead != s.values) {
            failed(s, new ParquetReadException("Value count mismatch: metadata declares " + s.values
                    + " values but pages contain " + s.valuesRead));
            return;
        }
        finish(s);
    }

    /// The scan is done with its chunk, at its end or where its column needs no further row.
    private void finish(Scan s) {
        PageFetcher.scanned(s.unit.listing().file().name(), s.unit.listing().rowGroup(), placer.schema(s.column), s.pages,
                RowGroupScannedEvent.STRATEGY_SEQUENTIAL);
        release(s);
        fetcher.placed(s.column);
    }

    /// A scan failed: the failure is placed at the first row it has not passed, and at the page it
    /// was on.
    private void failed(Scan s, Throwable t) {
        s.running = false;
        Throwable placed = t instanceof Exception e
                ? placer.placed(s.column, e, s.unit.listing().file().name(), s.unit.listing().rowGroup(), s.pages) : t;
        queue.failAt(key(s), s.column, placed);
        s.chunk.failure = placed;
        release(s);
        fetcher.placed(s.column);
    }

    /// Lets go of what the scan holds, and ends its chunk.
    private void release(Scan s) {
        s.ended = true;
        s.chunk.ended = true;
        fetcher.readersDone(s.ranges.subList(s.held, s.ranges.size()));
        s.held = s.ranges.size();
        if (s.dictionary != null) {
            fetcher.readersDone(List.of(s.dictionary));
        }
    }
}
