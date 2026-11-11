/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.reader.batch.Dictionary;
import dev.hardwood.internal.reader.decode.DataPage;
import dev.hardwood.internal.reader.decode.DictionaryParser;
import dev.hardwood.internal.reader.fetch.FileRange;
import dev.hardwood.internal.reader.pipeline.Cell;
import dev.hardwood.internal.reader.pipeline.NestedPageTask;
import dev.hardwood.internal.reader.plan.NeededPage;
import dev.hardwood.jfr.RowGroupScannedEvent;
import dev.hardwood.metadata.PageType;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

/// Tracks each page of a batched unit until it can decode, and starts its decode then: once
/// every range holding its bytes has arrived, a data page of a chunk with a dictionary once the
/// dictionary's values have, and a flat page once it is placed. Takes a unit's ranges and planned
/// pages, and takes the pages a scan finds. A chunk's dictionary page decodes into its
/// [ChunkDictionary], which the chunk's data pages wait for as for their ranges, and a flat page
/// waits for its placement too. A page waits for the first of these it lacks and, woken, looks
/// again. A range or dictionary that fails fails the read at each page waiting for it, and the
/// lowest row wins. Used under the read's lock, except the jobs' work.
final class PageTracker {

    private final JobScheduler scheduler;
    private final RangeFetcher fetcher;
    private final BatchPublisher publisher;
    private final PagePlacer placer;
    private final HardwoodContextImpl context;

    PageTracker(JobScheduler scheduler, RangeFetcher fetcher, BatchPublisher publisher, PagePlacer placer, HardwoodContextImpl context) {
        this.scheduler = scheduler;
        this.fetcher = fetcher;
        this.publisher = publisher;
        this.placer = placer;
        this.context = context;
    }

    /// What the scans of a batched unit's chunks without an OffsetIndex read: its ranges and
    /// each column's chunk of the placement.
    record ScanInput(List<RangeFetch> ranges, PagePlacer.ChunkPlacement[] placements) {
    }

    /// Starts tracking the pages of `u`, once it is batched: places its planned pages,
    /// fetches its ranges and decodes each page whose bytes are there; returns the ranges and the
    /// placement's chunks, for the scans of its chunks without an OffsetIndex. A range a scan reads
    /// is keyed at the row as far into the chunk as it lies. Tracking holds each range as one
    /// more reader, so that one its pages are done with stays until the scans hold it too;
    /// [#handedToScans] lets go of it.
    ScanInput track(Unit.Batched u) {
        List<RangeFetch> ranges = new ArrayList<>();
        PagePlacer.ChunkPlacement[] placements = new PagePlacer.ChunkPlacement[u.plan().pages().length];
        for (FileRange range : u.plan().ranges()) {
            RangeFetch r = RangeFetch.of(u.listing().file(), range);
            r.addReader();
            ranges.add(r);
        }
        // the first row each range serves among the pages planned in it
        long[] keys = new long[ranges.size()];
        Arrays.fill(keys, Long.MAX_VALUE);
        for (int c = 0; c < u.plan().pages().length; c++) {
            List<NeededPage> needed = u.plan().pages()[c];
            PagePlacer.ChunkPlacement placement = placer.placement(c, u);
            placements[c] = placement;
            if (needed != null) {
                placement.allAdded();
            }
            if (needed == null || needed.isEmpty()) {
                continue;
            }
            long firstKey = u.key(needed.getFirst().location().firstRowIndex());
            ChunkDictionary dictionary = null;
            if (u.plan().preloaded()[c] != null) {
                // decoded by the unit's probe
                dictionary = ChunkDictionary.preloaded(u.plan().preloaded()[c]);
            }
            else if (u.plan().dictionaryStart()[c] > 0) {
                dictionary = new ChunkDictionary();
                int length = Math.toIntExact(u.plan().firstDataPage()[c] - u.plan().dictionaryStart()[c]);
                addDictionaryPage(new TrackedPage(u, c, firstKey, NeededPage.dictionary(u.plan().dictionaryStart()[c], length),
                        overlapping(ranges, keys, firstKey, u.plan().dictionaryStart()[c], length), dictionary, false));
            }
            reportPagesLocated(u.listing().file().name(), u.listing().rowGroup(), placer.schema(c), needed.size(),
                    RowGroupScannedEvent.STRATEGY_OFFSET_INDEX);
            for (NeededPage page : needed) {
                int length = page.location().compressedPageSize();
                long key = u.key(page.location().firstRowIndex());
                add(placement, new TrackedPage(u, c, key, page,
                        overlapping(ranges, keys, key, page.location().offset(), length), dictionary, true));
            }
        }
        for (int i = 0; i < ranges.size(); i++) {
            if (!ranges.get(i).output().hasValue()) {
                fetcher.fetch(u.listing().file(), ranges.get(i), key(u, ranges.get(i), keys[i]));
            }
        }
        for (int c = 0; c < u.plan().pages().length; c++) {
            placed(c);
        }
        return new ScanInput(ranges, placements);
    }

    /// The scans of `input`'s chunks hold the ranges they read: tracking lets go of each, and
    /// one that nothing reads is not fetched.
    void handedToScans(ScanInput input) {
        fetcher.readersDone(input.ranges());
    }

    /// The row `r` serves first: its first page's, `pageKey`, or for a chunk a scan reads, the row
    /// as far into the chunk as the range lies.
    private static long key(Unit.Batched u, RangeFetch r, long pageKey) {
        long key = pageKey;
        long rows = u.listing().metaData().numRows();
        for (int c = 0; c < u.plan().pages().length; c++) {
            long start = u.listing().chunks()[c].chunkStartOffset();
            long length = u.listing().chunks()[c].metaData().totalCompressedSize();
            long into = Math.max(0, r.range().start() - start);
            if (u.plan().pages()[c] == null && r.range().start() < start + length && r.range().end() > start) {
                key = Math.min(key, u.key((long) (rows * (double) into / length)));
            }
        }
        return key;
    }

    /// Adds `tracked`, a dictionary page, which decodes once its bytes are there.
    void addDictionaryPage(TrackedPage tracked) {
        hold(tracked);
        decodeWhenReady(tracked);
    }

    /// Adds `tracked`, a data page of `chunk`, to its column's placement. A page decoded through
    /// levels decodes before it is placed, a flat page once placed too ([#placed]).
    void add(PagePlacer.ChunkPlacement placement, TrackedPage tracked) {
        hold(tracked);
        placer.add(tracked.column(), placement, tracked);
        decodeWhenReady(tracked);
    }

    /// Places the pages `column` can place, ending each flat page's placement, which its decode waits
    /// on; a page that fills no cell is done and never decodes.
    void placed(int column) {
        for (TrackedPage tracked : placer.place(column)) {
            if (placer.nested(column)) {
                continue;
            }
            if (tracked.task().segments().isEmpty()) {
                done(tracked);
            }
            else {
                tracked.placement().arrived(null);
            }
        }
    }

    /// Those of `ranges` that hold part of `[offset, offset + length)`, a page serving row `key`:
    /// lowers each one's entry in `keys` to it.
    private static List<RangeFetch> overlapping(List<RangeFetch> ranges, long[] keys, long key, long offset, int length) {
        List<RangeFetch> found = new ArrayList<>(2);
        for (int i = 0; i < ranges.size(); i++) {
            RangeFetch r = ranges.get(i);
            if (r.range().start() < offset + length && r.range().end() > offset) {
                found.add(r);
                keys[i] = Math.min(keys[i], key);
            }
        }
        return found;
    }

    /// `tracked` will read its ranges until done.
    private void hold(TrackedPage tracked) {
        for (RangeFetch r : tracked.ranges()) {
            r.addReader();
        }
    }

    /// Decodes `tracked` once its bytes, a data page's dictionary and a flat page's placement are
    /// there: it waits for the first it lacks and, woken, looks again. A range or dictionary that
    /// failed fails the read at the page, which then never decodes.
    private void decodeWhenReady(TrackedPage tracked) {
        RangeFetch missing = RangeFetch.firstMissing(tracked.ranges());
        if (missing != null) {
            awaitOrFail(tracked, missing.output());
        }
        else if (!dictionaryThere(tracked)) {
            awaitOrFail(tracked, tracked.dictionary().output());
        }
        else if (tracked.data() && !placer.nested(tracked.column()) && !tracked.placement().ended()) {
            awaitOrFail(tracked, tracked.placement());
        }
        else {
            addDecode(tracked);
        }
    }

    /// `tracked` lacks `output`: it fails at the page where that has failed, and waits for it
    /// otherwise.
    private void awaitOrFail(TrackedPage tracked, Output<?> output) {
        if (output.failure() != null) {
            publisher.failAt(tracked.key(), tracked.column(), output.failure());
        }
        else {
            output.whenEnded(() -> decodeWhenReady(tracked));
        }
    }

    /// Whether `tracked` reads no dictionary, or its dictionary's values have arrived.
    private static boolean dictionaryThere(TrackedPage tracked) {
        return !tracked.data() || tracked.dictionary() == null || tracked.dictionary().output().hasValue();
    }

    private void addDecode(TrackedPage tracked) {
        if (!tracked.data()) {
            addDictionaryDecode(tracked);
            return;
        }
        if (placer.nested(tracked.column())) {
            addMeasure(tracked);
            return;
        }
        // its values are the read's working set's, which the decode charges, not R's
        scheduler.add(new JobScheduler.Job<>(tracked.key(), 0, JobScheduler.Kind.DECODE, null, buffers -> {
            Dictionary dictionary = tracked.dictionary() == null ? null : tracked.dictionary().output().value();
            return placer.decode(tracked.column(), tracked.task(),
                    tracked.placeholder() > 0 ? null : tracked.bytes(), dictionary, buffers);
        }, completed -> {
            // its ranges are held by the batch of its last row until every column has published it,
            // as R holds the bytes fetched ahead of the consumer
            publisher.hold(tracked.column(), tracked.task().segments().getLast().cell(), tracked.ranges());
            for (Cell cell : completed) {
                publisher.completed(tracked.column(), cell);
            }
        }, t -> failed(tracked, t)));
    }

    /// Decodes and measures a page decoded through levels, which can then be placed.
    private void addMeasure(TrackedPage tracked) {
        scheduler.add(new JobScheduler.Job<>(tracked.key(), 0, JobScheduler.Kind.DECODE, null, buffers -> {
            NestedPageTask task = placer.nestedTask(tracked,
                    tracked.placeholder() > 0 ? null : DataPage.parse(tracked.bytes()),
                    tracked.dictionary() == null ? null : tracked.dictionary().output().value());
            try {
                task.decodeAndMeasure(buffers);
            }
            catch (RuntimeException e) {
                throw placer.placed(tracked.column(), e, task);
            }
            return task;
        }, task -> {
            tracked.ready(task);
            done(tracked);
            placed(tracked.column());
        }, t -> failed(tracked, t)));
    }

    /// `tracked`'s decode failed with `t`: the read fails at the page, which is done.
    private void failed(TrackedPage tracked, Throwable t) {
        publisher.failAt(tracked.key(), tracked.column(), t);
        done(tracked);
    }

    /// Parses a chunk's dictionary page, ending its dictionary's output, which the chunk's data
    /// pages wait on.
    private void addDictionaryDecode(TrackedPage tracked) {
        Output<Dictionary> output = tracked.dictionary().output();
        scheduler.add(new JobScheduler.Job<>(tracked.key(), 0, JobScheduler.Kind.DECODE, null, buffers -> {
            try {
                return parseDictionary(tracked, buffers);
            }
            catch (Exception e) {
                throw placer.placed(tracked.column(), e, tracked.unit().listing().file().name(),
                        tracked.unit().listing().rowGroup(), ExceptionContext.DICTIONARY_PAGE);
            }
        }, values -> {
            done(tracked);
            output.arrived(values);
        }, t -> {
            failed(tracked, t);
            output.failed(t);
        }));
    }

    private Dictionary parseDictionary(TrackedPage tracked, DecodeBuffers buffers) {
        ByteBuffer region = tracked.bytes();
        DataPage page = DataPage.parse(region);
        int pageLength = page.headerSize() + page.header().compressedPageSize();
        if (page.header().type() != PageType.DICTIONARY_PAGE) {
            return null;
        }
        if (pageLength > region.remaining()) {
            throw new ParquetReadException("Malformed Parquet metadata: the dictionary page header declares "
                    + pageLength + " bytes but only " + region.remaining() + " bytes lie before the first data page");
        }
        return placer.chargeDictionary(tracked.column(), tracked.unit(), () -> DictionaryParser.parsePage(page.header(), page.body(),
                placer.schema(tracked.column()), tracked.unit().listing().chunks()[tracked.column()].metaData(), context,
                placer.workingSet(tracked.column()), buffers));
    }

    /// A page is done with its ranges; the last reader of each returns it.
    private void done(TrackedPage tracked) {
        fetcher.readersDone(tracked.ranges());
    }

    /// Reports that the pages of `column` in row group `rowGroupIndex` of `file` are located:
    /// `pageCount` pages, by `strategy`.
    static void reportPagesLocated(String file, int rowGroupIndex, ColumnSchema column, int pageCount, String strategy) {
        RowGroupScannedEvent event = new RowGroupScannedEvent();
        event.file = file;
        event.rowGroupIndex = rowGroupIndex;
        event.column = column.name();
        event.pageCount = pageCount;
        event.scanStrategy = strategy;
        event.commit();
    }
}
