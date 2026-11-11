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
import java.util.List;

import dev.hardwood.InputFile;
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
/// [ChunkDictionary], which the chunk's data pages wait for as for their ranges. A range that cannot
/// be read fails the read at its first page's row. Used under the read's lock, except the jobs'
/// work.
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
        List<TrackedPage> pages = new ArrayList<>();
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
                pages.add(hold(new TrackedPage(u, c, firstKey, NeededPage.dictionary(u.plan().dictionaryStart()[c], length),
                        overlapping(ranges, u.plan().dictionaryStart()[c], length), dictionary, false)));
            }
            reportPagesLocated(u.listing().file().name(), u.listing().rowGroup(), placer.schema(c), needed.size(),
                    RowGroupScannedEvent.STRATEGY_OFFSET_INDEX);
            for (NeededPage page : needed) {
                int length = page.location().compressedPageSize();
                TrackedPage pw = new TrackedPage(u, c, u.key(page.location().firstRowIndex()), page,
                        overlapping(ranges, page.location().offset(), length), dictionary, true);
                pages.add(add(placement, pw));
            }
        }
        for (RangeFetch r : ranges) {
            if (!r.hasArrived()) {
                addFetch(u.listing().file(), r, key(u, r));
            }
        }
        decodeReady(pages);
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

    /// The row `r` serves first: its first page's, or for a chunk a scan reads, the row as far into
    /// the chunk as the range lies.
    private static long key(Unit.Batched u, RangeFetch r) {
        long key = Long.MAX_VALUE;
        for (TrackedPage pw : r.waiting()) {
            key = Math.min(key, pw.key());
        }
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

    /// Decodes those of `pages` whose bytes and dictionary are there and that wait for no
    /// placement.
    void decodeReady(List<TrackedPage> pages) {
        for (TrackedPage pw : pages) {
            if (pw.waitsForNothing()) {
                addDecode(pw);
            }
        }
    }

    /// Adds `pw`, a data page of `chunk`, to its column's placement: a flat page decodes once
    /// placed, so it waits for its placement as for a range.
    TrackedPage add(PagePlacer.ChunkPlacement placement, TrackedPage pw) {
        hold(pw);
        placer.add(pw.column(), placement, pw);
        if (!placer.nested(pw.column())) {
            pw.waitForOneMore();
        }
        return pw;
    }

    /// Places what `column` can place; a flat page placed may then decode, one that fills no cell
    /// is done.
    void placed(int column) {
        for (TrackedPage pw : placer.place(column)) {
            if (placer.nested(column)) {
                continue;
            }
            if (pw.task().segments().isEmpty()) {
                done(pw);
            }
            else if (pw.oneArrived()) {
                addDecode(pw);
            }
        }
    }

    private static List<RangeFetch> overlapping(List<RangeFetch> ranges, long offset, int length) {
        List<RangeFetch> found = new ArrayList<>(2);
        for (RangeFetch r : ranges) {
            if (r.range().start() < offset + length && r.range().end() > offset) {
                found.add(r);
            }
        }
        return found;
    }

    /// `pw` will read its ranges, and a data page its dictionary: each waits for it until done.
    TrackedPage hold(TrackedPage pw) {
        for (RangeFetch r : pw.ranges()) {
            r.addReader();
            if (!r.hasArrived()) {
                r.await(pw);
                pw.waitForOneMore();
            }
        }
        ChunkDictionary dictionary = pw.dictionary();
        if (pw.data() && dictionary != null) {
            dictionary.addReader();
            if (!dictionary.hasArrived()) {
                dictionary.await(pw);
                pw.waitForOneMore();
            }
        }
        return pw;
    }

    /// Fetches `r`, whose bytes pages wait for, keyed at `key`: a failure fails each of them.
    void addFetch(InputFile file, RangeFetch r, long key) {
        fetcher.fetch(file, r, key, t -> {
            if (t == null) {
                arrived(r);
                return;
            }
            r.failed(t);
            for (TrackedPage pw : r.waiting()) {
                publisher.failAt(pw.key(), pw.column(), t);
            }
            r.notifyListeners();
        });
    }

    /// `r`'s bytes have arrived: the pages waiting for it may decode.
    private void arrived(RangeFetch r) {
        release(r.waiting());
        r.notifyListeners();
    }

    /// The values of `d` have arrived: the data pages waiting for them may decode.
    private void arrived(ChunkDictionary d, Dictionary values) {
        d.arrived(values);
        release(d.waiting());
    }

    /// `waiting` have one thing less to wait for; those waiting for nothing more decode.
    private void release(List<TrackedPage> waiting) {
        for (TrackedPage pw : waiting) {
            if (pw.oneArrived()) {
                addDecode(pw);
            }
        }
    }

    private void addDecode(TrackedPage pw) {
        if (!pw.data()) {
            addDictionaryDecode(pw);
            return;
        }
        if (placer.nested(pw.column())) {
            addMeasure(pw);
            return;
        }
        // its values are the read's working set's, which the decode charges, not R's
        scheduler.add(new JobScheduler.Job(pw.key(), 0, JobScheduler.Kind.DECODE, null, buffers -> {
            Dictionary dictionary = pw.dictionary() == null ? null : pw.dictionary().values();
            List<Cell> completed = placer.decode(pw.column(), pw.task(),
                    pw.placeholder() > 0 ? null : pw.bytes(), dictionary, buffers);
            return () -> {
                // its ranges are held by the batch of its last row until every column has published
                // it, as R holds what is fetched ahead of the consumer
                publisher.hold(pw.column(), pw.task().segments().getLast().cell(), pw.ranges());
                for (Cell cell : completed) {
                    publisher.completed(pw.column(), cell);
                }
                if (pw.dictionary() != null) {
                    pw.dictionary().readerDone();
                }
            };
        }, t -> () -> {
            publisher.failAt(pw.key(), pw.column(), t);
            done(pw);
        }));
    }

    /// Decodes and measures a page decoded through levels, which can then be placed.
    private void addMeasure(TrackedPage pw) {
        scheduler.add(new JobScheduler.Job(pw.key(), 0, JobScheduler.Kind.DECODE, null, buffers -> {
            NestedPageTask task = placer.nestedTask(pw,
                    pw.placeholder() > 0 ? null : DataPage.parse(pw.bytes()),
                    pw.dictionary() == null ? null : pw.dictionary().values());
            try {
                task.decodeAndMeasure(buffers);
            }
            catch (RuntimeException e) {
                throw placer.placed(pw.column(), e, task);
            }
            return () -> {
                pw.ready(task);
                done(pw);
                placed(pw.column());
            };
        }, t -> () -> {
            publisher.failAt(pw.key(), pw.column(), t);
            done(pw);
        }));
    }

    /// Parses a chunk's dictionary page; its values arrive as a range's bytes do.
    private void addDictionaryDecode(TrackedPage pw) {
        scheduler.add(new JobScheduler.Job(pw.key(), 0, JobScheduler.Kind.DECODE, null,
                buffers -> {
                    Dictionary values = parseDictionary(pw, buffers);
                    return () -> {
                        done(pw);
                        arrived(pw.dictionary(), values);
                    };
                }, t -> () -> {
                    publisher.failAt(pw.key(), pw.column(), t instanceof Exception e ? placer.placed(pw.column(), e, pw.unit().listing().file().name(),
                            pw.unit().listing().rowGroup(), ExceptionContext.DICTIONARY_PAGE) : t);
                    done(pw);
                }));
    }

    private Dictionary parseDictionary(TrackedPage pw, DecodeBuffers buffers) {
        ByteBuffer region = pw.bytes();
        DataPage page = DataPage.parse(region);
        int pageLength = page.headerSize() + page.header().compressedPageSize();
        if (page.header().type() != PageType.DICTIONARY_PAGE) {
            return null;
        }
        if (pageLength > region.remaining()) {
            throw new ParquetReadException("Malformed Parquet metadata: the dictionary page header declares "
                    + pageLength + " bytes but only " + region.remaining() + " bytes lie before the first data page");
        }
        return placer.chargeDictionary(pw.column(), pw.unit(), () -> DictionaryParser.parsePage(page.header(), page.body(),
                placer.schema(pw.column()), pw.unit().listing().chunks()[pw.column()].metaData(), context, placer.workingSet(pw.column()),
                buffers));
    }

    /// A page is done with its ranges and its dictionary; the last reader of each returns it.
    private void done(TrackedPage pw) {
        fetcher.readersDone(pw.ranges());
        if (pw.data() && pw.dictionary() != null) {
            pw.dictionary().readerDone();
        }
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
