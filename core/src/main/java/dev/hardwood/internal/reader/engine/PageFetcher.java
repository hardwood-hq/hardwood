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
import java.util.List;
import java.util.function.Consumer;

import dev.hardwood.InputFile;
import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.io.HeldBytes;
import dev.hardwood.internal.io.ReadRanges;
import dev.hardwood.internal.reader.batch.Dictionary;
import dev.hardwood.internal.reader.decode.DataPage;
import dev.hardwood.internal.reader.decode.DictionaryParser;
import dev.hardwood.internal.reader.decode.PageDecoder;
import dev.hardwood.internal.reader.fetch.Span;
import dev.hardwood.internal.reader.pipeline.Cell;
import dev.hardwood.internal.reader.pipeline.NestedPageTask;
import dev.hardwood.internal.reader.pipeline.PageTask;
import dev.hardwood.internal.reader.plan.NeededPage;
import dev.hardwood.jfr.RowGroupScannedEvent;
import dev.hardwood.metadata.PageType;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

/// A read's ranges and pages once their unit is installed: it fetches the unit's ranges and
/// decodes each page once every range holding its bytes has arrived, in a chunk with a
/// dictionary once the dictionary has. A chunk's dictionary is a range too, whose values a decode
/// job produces and its data pages read. A range that cannot be read fails the read at its first
/// page's row. Used under the read's lock, except the jobs' work.
final class PageFetcher {

    /// A range of the file, or a chunk's dictionary: its bytes or values once arrived, its charge,
    /// and the pages that wait for it.
    static final class Range {
        final Span span;
        final Charge charge = new Charge();
        final List<PageWork> waiting = new ArrayList<>();
        ByteBuffer bytes;
        Dictionary values;
        boolean arrived;
        Throwable failure;
        /// its fetch, once added, so that a scan finding a page in it can key it at the page's row
        JobScheduler.Job fetch;
        /// what waits for the range beyond its pages, run once it has arrived or failed: the scan
        /// of its chunk
        final List<Runnable> listeners = new ArrayList<>(1);

        Range(Span span) {
            this.span = span;
        }

        /// A reader is done; the last returns the charge and lets go of the bytes or values, and
        /// takes out its fetch where that has not started: nothing would read what it fetches.
        void readerDone(JobScheduler scheduler) {
            if (charge.read(scheduler)) {
                bytes = null;
                values = null;
                if (fetch != null) {
                    scheduler.remove(fetch);
                }
            }
        }
    }

    /// A page to decode, `page`, with the ranges its bytes lie in and its chunk's dictionary,
    /// `null` for a chunk without one; [PagePlacer] gives it its `task` and decides when it is
    /// `ready` to be placed. A dictionary page (`data` false) produces its dictionary's values.
    static final class PageWork {
        final Unit.Installed unit;
        final int column;
        final long key;
        final NeededPage page;
        final List<Range> ranges;
        final Range dictionary;
        final boolean data;
        /// the rows a page its inline statistics drop stands for, as nulls without bytes; `0` for a
        /// page read from its bytes
        int placeholder;
        PageDecoder decoder;
        PageTask task;
        boolean ready;
        int missing;

        PageWork(Unit.Installed unit, int column, long key, NeededPage page, List<Range> ranges, Range dictionary,
                 boolean data) {
            this.unit = unit;
            this.column = column;
            this.key = key;
            this.page = page;
            this.ranges = ranges;
            this.dictionary = dictionary;
            this.data = data;
        }

        /// The page's bytes, from its ranges: `null` where one has not arrived.
        ByteBuffer bytes() {
            return read(ranges, page.location().offset(), page.location().compressedPageSize());
        }
    }

    private final JobScheduler scheduler;
    private final BatchQueue queue;
    private final PagePlacer placer;
    private final HardwoodContextImpl context;

    PageFetcher(JobScheduler scheduler, BatchQueue queue, PagePlacer placer, HardwoodContextImpl context) {
        this.scheduler = scheduler;
        this.queue = queue;
        this.placer = placer;
        this.context = context;
    }

    /// What the scans of an installed unit's chunks without an OffsetIndex read: its ranges and
    /// each column's chunk of the placement.
    record ScanInput(List<Range> ranges, PagePlacer.Chunk[] placement) {
    }

    /// Places the unit's planned pages, fetches its ranges and decodes each page whose bytes are
    /// there; returns the ranges and the placement's chunks, for the scans of its chunks without an
    /// OffsetIndex. A range a
    /// scan reads is keyed at the row as far into the chunk as it lies. The install holds each range
    /// as one more reader, so that one its pages are done with stays until the scans hold it too;
    /// [#installed] lets go of it.
    ScanInput install(Unit.Installed u) {
        List<Range> ranges = new ArrayList<>();
        PagePlacer.Chunk[] placement = new PagePlacer.Chunk[u.plan().pages().length];
        for (Span span : u.plan().spans()) {
            Range r = new Range(span);
            r.charge.reader();
            r.bytes = HeldBytes.slice(u.listing().file(), span.start(), span.length());
            r.arrived = r.bytes != null;
            ranges.add(r);
        }
        List<PageWork> pages = new ArrayList<>();
        for (int c = 0; c < u.plan().pages().length; c++) {
            List<NeededPage> needed = u.plan().pages()[c];
            PagePlacer.Chunk chunk = placer.chunk(c, u);
            placement[c] = chunk;
            chunk.ended = needed != null;
            if (needed == null || needed.isEmpty()) {
                continue;
            }
            long firstKey = u.key(needed.getFirst().location().firstRowIndex());
            Range dictionary = null;
            if (u.plan().preloaded()[c] != null) {
                // decoded by the unit's probe
                dictionary = new Range(null);
                dictionary.values = u.plan().preloaded()[c];
                dictionary.arrived = true;
            }
            else if (u.plan().dictionaryStart()[c] > 0) {
                dictionary = new Range(null);
                int length = Math.toIntExact(u.plan().firstDataPage()[c] - u.plan().dictionaryStart()[c]);
                pages.add(hold(new PageWork(u, c, firstKey, NeededPage.dictionary(u.plan().dictionaryStart()[c], length),
                        overlapping(ranges, u.plan().dictionaryStart()[c], length), dictionary, false)));
            }
            scanned(u.listing().file().name(), u.listing().rowGroup(), placer.schema(c), needed.size(),
                    RowGroupScannedEvent.STRATEGY_OFFSET_INDEX);
            for (NeededPage page : needed) {
                int length = page.location().compressedPageSize();
                PageWork pw = new PageWork(u, c, u.key(page.location().firstRowIndex()), page,
                        overlapping(ranges, page.location().offset(), length), dictionary, true);
                pages.add(add(chunk, pw));
            }
        }
        for (Range r : ranges) {
            if (!r.arrived) {
                addFetch(u.listing().file(), r, key(u, r));
            }
        }
        decodeReady(pages);
        for (int c = 0; c < u.plan().pages().length; c++) {
            placed(c);
        }
        return new ScanInput(ranges, placement);
    }

    /// The scans of `input`'s chunks hold the ranges they read: the install lets go of each, and
    /// one that nothing reads is not fetched.
    void installed(ScanInput input) {
        readersDone(input.ranges());
    }

    /// The row `r` serves first: its first page's, or for a chunk a scan reads, the row as far into
    /// the chunk as the range lies.
    private static long key(Unit.Installed u, Range r) {
        long key = Long.MAX_VALUE;
        for (PageWork pw : r.waiting) {
            key = Math.min(key, pw.key);
        }
        long rows = u.listing().metaData().numRows();
        for (int c = 0; c < u.plan().pages().length; c++) {
            long start = u.listing().chunks()[c].chunkStartOffset();
            long length = u.listing().chunks()[c].metaData().totalCompressedSize();
            long into = Math.max(0, r.span.start() - start);
            if (u.plan().pages()[c] == null && r.span.start() < start + length && r.span.start() + r.span.length() > start) {
                key = Math.min(key, u.key((long) (rows * (double) into / length)));
            }
        }
        return key;
    }

    /// Decodes those of `pages` whose bytes and dictionary are there and that wait for no
    /// placement.
    void decodeReady(List<PageWork> pages) {
        for (PageWork pw : pages) {
            if (pw.missing == 0) {
                addDecode(pw);
            }
        }
    }

    /// Adds `pw`, a data page of `chunk`, to its column's placement: a flat page decodes once
    /// placed, so it waits for its placement as for a range.
    PageWork add(PagePlacer.Chunk chunk, PageWork pw) {
        hold(pw);
        placer.add(pw.column, chunk, pw);
        if (!placer.nested(pw.column)) {
            pw.missing++;
        }
        return pw;
    }

    /// Places what `column` can place; a flat page placed may then decode, one that fills no cell
    /// is done.
    void placed(int column) {
        for (PageWork pw : placer.place(column)) {
            if (placer.nested(column)) {
                continue;
            }
            if (pw.task.segments().isEmpty()) {
                done(pw);
            }
            else if (--pw.missing == 0) {
                addDecode(pw);
            }
        }
    }

    private static List<Range> overlapping(List<Range> ranges, long offset, int length) {
        List<Range> found = new ArrayList<>(2);
        for (Range r : ranges) {
            if (r.span.start() < offset + length && r.span.start() + r.span.length() > offset) {
                found.add(r);
            }
        }
        return found;
    }

    /// `pw` will read its ranges, and a data page its dictionary: each waits for it until done.
    PageWork hold(PageWork pw) {
        for (Range r : pw.ranges) {
            r.charge.reader();
            if (!r.arrived) {
                r.waiting.add(pw);
                pw.missing++;
            }
        }
        if (pw.data && pw.dictionary != null) {
            pw.dictionary.charge.reader();
            if (!pw.dictionary.arrived) {
                pw.dictionary.waiting.add(pw);
                pw.missing++;
            }
        }
        return pw;
    }

    void addFetch(InputFile file, Range r, long key) {
        fetch(file, r, key, t -> {
            if (t == null) {
                arrived(r, null);
                return;
            }
            r.failure = t;
            for (PageWork pw : r.waiting) {
                queue.failAt(pw.key, pw.column, t);
            }
            r.listeners.forEach(Runnable::run);
        });
    }

    /// Fetches `r` as a job keyed at `key`; `then` runs under the lock once it has arrived, with
    /// `null`, or with its failure, its charge returned.
    void fetch(InputFile file, Range r, long key, Consumer<Throwable> then) {
        r.fetch = new JobScheduler.Job(key, r.span.length(), JobScheduler.Kind.FETCH, r.charge, buffers -> {
            ByteBuffer bytes;
            try (FetchReason.Scope ignored = FetchReason.set(r.span.reason())) {
                bytes = ReadRanges.read(file, r.span.start(), r.span.length());
            }
            return () -> {
                r.bytes = bytes;
                then.accept(null);
            };
        }, t -> () -> {
            r.charge.returnTo(scheduler);
            then.accept(t instanceof Exception e ? new IOException(ExceptionContext.filePrefix(file.name())
                    + "Failed to fetch data at offset " + r.span.start() + " (length " + r.span.length() + ")", e)
                    : t);
        });
        scheduler.add(r.fetch);
    }

    /// Keys the fetches of `ranges` not started yet at `key` where that is lower: a scan found a
    /// page serving row `key` in them, whose bytes the consumer's need must reach.
    void keyAt(List<Range> ranges, long key) {
        for (Range r : ranges) {
            if (r.fetch != null) {
                scheduler.lower(r.fetch, key);
            }
        }
    }

    /// `r`'s bytes, or a dictionary's `values`, have arrived: the pages waiting for it may decode.
    private void arrived(Range r, Dictionary values) {
        r.values = values;
        r.arrived = true;
        for (PageWork pw : r.waiting) {
            if (--pw.missing == 0) {
                addDecode(pw);
            }
        }
        r.listeners.forEach(Runnable::run);
    }

    private void addDecode(PageWork pw) {
        if (!pw.data) {
            addDictionaryDecode(pw);
            return;
        }
        if (placer.nested(pw.column)) {
            addMeasure(pw);
            return;
        }
        // its values are the read's working set's, which the decode charges, not R's
        scheduler.add(new JobScheduler.Job(pw.key, 0, JobScheduler.Kind.DECODE, null, buffers -> {
            Dictionary dictionary = pw.dictionary == null ? null : pw.dictionary.values;
            List<Cell> completed = placer.decode(pw.column, pw.task,
                    pw.placeholder > 0 ? null : pw.bytes(), dictionary, buffers);
            return () -> {
                // its ranges are held by the batch of its last row until every column has published
                // it, as R holds what is fetched ahead of the consumer
                queue.hold(pw.column, pw.task.segments().getLast().cell(), pw.ranges);
                for (Cell cell : completed) {
                    queue.completed(pw.column, cell);
                }
                if (pw.dictionary != null) {
                    readersDone(List.of(pw.dictionary));
                }
            };
        }, t -> () -> {
            queue.failAt(pw.key, pw.column, t);
            done(pw);
        }));
    }

    /// Decodes and measures a page decoded through levels, which can then be placed.
    private void addMeasure(PageWork pw) {
        scheduler.add(new JobScheduler.Job(pw.key, 0, JobScheduler.Kind.DECODE, null, buffers -> {
            NestedPageTask task = placer.nestedTask(pw,
                    pw.placeholder > 0 ? null : DataPage.parse(pw.bytes()),
                    pw.dictionary == null ? null : pw.dictionary.values);
            try {
                task.decodeAndMeasure(buffers);
            }
            catch (RuntimeException e) {
                throw placer.placed(pw.column, e, task);
            }
            return () -> {
                pw.task = task;
                pw.ready = true;
                done(pw);
                placed(pw.column);
            };
        }, t -> () -> {
            queue.failAt(pw.key, pw.column, t);
            done(pw);
        }));
    }

    /// Parses a chunk's dictionary page; its values arrive as a range's bytes do.
    private void addDictionaryDecode(PageWork pw) {
        scheduler.add(new JobScheduler.Job(pw.key, 0, JobScheduler.Kind.DECODE, null,
                buffers -> {
                    Dictionary values = parseDictionary(pw, buffers);
                    return () -> {
                        done(pw);
                        arrived(pw.dictionary, values);
                    };
                }, t -> () -> {
                    queue.failAt(pw.key, pw.column, t instanceof Exception e ? placer.placed(pw.column, e, pw.unit.listing().file().name(),
                            pw.unit.listing().rowGroup(), ExceptionContext.DICTIONARY_PAGE) : t);
                    done(pw);
                }));
    }

    private Dictionary parseDictionary(PageWork pw, DecodeBuffers buffers) {
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
        return placer.inWindow(pw.column, pw.unit, () -> DictionaryParser.parsePage(page.header(), page.body(),
                placer.schema(pw.column), pw.unit.listing().chunks()[pw.column].metaData(), context, placer.workingSet(pw.column),
                buffers));
    }

    /// A page is done with its ranges and its dictionary; the last reader of each returns it.
    private void done(PageWork pw) {
        readersDone(pw.ranges);
        if (pw.data && pw.dictionary != null) {
            readersDone(List.of(pw.dictionary));
        }
    }

    /// A reader of each of `ranges` is done; the last of a range's lets go of its bytes or values.
    void readersDone(List<Range> ranges) {
        for (Range r : ranges) {
            r.readerDone(scheduler);
        }
    }

    /// `[offset, offset + length)` from those of `ranges` that hold part of it: a slice where one
    /// holds it, a copy where it straddles two; `null` where a range holding part of it has not
    /// arrived, or `ranges` do not reach its end.
    static ByteBuffer read(List<Range> ranges, long offset, int length) {
        long end = offset + length;
        if (ranges.isEmpty() || ranges.getLast().span.start() + ranges.getLast().span.length() < end) {
            return null;
        }
        ByteBuffer copy = null;
        for (Range r : ranges) {
            long start = r.span.start();
            if (start >= end || start + r.span.length() <= offset) {
                continue;
            }
            if (r.bytes == null) {
                return null;
            }
            if (start <= offset && start + r.span.length() >= end) {
                return r.bytes.slice(Math.toIntExact(offset - start), length);
            }
            if (copy == null) {
                copy = ByteBuffer.allocate(length);
            }
            long s = Math.max(offset, start);
            long e = Math.min(end, start + r.span.length());
            copy.put(Math.toIntExact(s - offset), r.bytes, Math.toIntExact(s - start), Math.toIntExact(e - s));
        }
        return copy;
    }

    /// Reports a scan of `column` in row group `rowGroupIndex` of `file` that has located its last
    /// page: `pageCount` pages, located by `strategy`.
    static void scanned(String file, int rowGroupIndex, ColumnSchema column, int pageCount, String strategy) {
        RowGroupScannedEvent event = new RowGroupScannedEvent();
        event.file = file;
        event.rowGroupIndex = rowGroupIndex;
        event.column = column.name();
        event.pageCount = pageCount;
        event.scanStrategy = strategy;
        event.commit();
    }
}
