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
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.reader.batch.Dictionary;
import dev.hardwood.internal.reader.decode.DataPage;
import dev.hardwood.internal.reader.decode.DictionaryParser;
import dev.hardwood.internal.reader.decode.PageFormatProbe;
import dev.hardwood.internal.reader.fetch.FileRange;
import dev.hardwood.internal.reader.memory.WorkingSet;
import dev.hardwood.internal.reader.pipeline.Cell;
import dev.hardwood.internal.reader.pipeline.NestedPageTask;
import dev.hardwood.internal.reader.plan.NeededPage;
import dev.hardwood.internal.reader.step.Parsed;
import dev.hardwood.jfr.RowGroupScannedEvent;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PageLocation;
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
    private final ReadFailure readFailure;
    private final CellWriter cells;
    private final ColumnContext columns;
    private final HardwoodContextImpl context;
    /// the rows of a unit whose planned pages are tracked at once: a batch byte budget's
    private final long sliceRows;

    PageTracker(JobScheduler scheduler, RangeFetcher fetcher, BatchPublisher publisher, ReadFailure readFailure,
                CellWriter cells, ColumnContext columns, HardwoodContextImpl context, long sliceRows) {
        this.scheduler = scheduler;
        this.fetcher = fetcher;
        this.publisher = publisher;
        this.readFailure = readFailure;
        this.cells = cells;
        this.columns = columns;
        this.context = context;
        this.sliceRows = Math.max(1, sliceRows);
    }

    /// What the scans of a batched unit's chunks without an OffsetIndex read: its ranges and
    /// each column's chunk of the placement.
    record ScanInput(List<RangeFetch> ranges, CellWriter.ChunkPlacement[] placements) {
    }

    /// Starts tracking the pages of `u`, once it is batched: fetches its ranges, decodes its
    /// dictionary pages, and tracks its planned pages a slice of rows at a time as the look-ahead
    /// reaches them ([#trackSlice]); returns the ranges and the placement's chunks, for the scans
    /// of its chunks without an OffsetIndex. A range is keyed at the first row its planned pages
    /// serve, or, read by a scan, at the row as far into the chunk as it lies. Each planned page
    /// in a range is one of its readers from here, before the page is tracked; tracking holds each
    /// range as one more reader, so that one its pages are done with stays until the scans hold
    /// it too ([#handedToScans]).
    ScanInput track(Unit.Batched u) {
        List<RangeFetch> ranges = new ArrayList<>();
        int n = u.plan().pages().length;
        CellWriter.ChunkPlacement[] placements = new CellWriter.ChunkPlacement[n];
        for (FileRange range : u.plan().ranges()) {
            RangeFetch r = RangeFetch.of(u.listing().file(), range);
            r.addReader();
            ranges.add(r);
        }
        // the first row each range serves among the pages planned in it
        long[] keys = new long[ranges.size()];
        Arrays.fill(keys, Long.MAX_VALUE);
        // the column of that row's page, the lowest where two serve it
        int[] keyColumns = new int[ranges.size()];
        Slices slices = new Slices(u, ranges, placements, new ChunkDictionary[n], new int[n], new int[n]);
        long first = Long.MAX_VALUE;
        for (int c = 0; c < n; c++) {
            List<NeededPage> needed = u.plan().pages()[c];
            CellWriter.ChunkPlacement placement = cells.placement(c, u);
            placements[c] = placement;
            if (needed == null) {
                continue;
            }
            if (needed.isEmpty()) {
                placement.allAdded();
                continue;
            }
            long firstKey = u.key(needed.getFirst().location().firstRowIndex());
            first = Math.min(first, firstKey);
            if (u.plan().preloaded()[c] != null) {
                // decoded by the unit's dictionary pruning
                slices.dictionaries()[c] = ChunkDictionary.preloaded(u.plan().preloaded()[c]);
            }
            else if (u.plan().dictionaryStart()[c] > 0) {
                int length = Math.toIntExact(u.plan().firstDataPage()[c] - u.plan().dictionaryStart()[c]);
                Exception tooLarge = tooLarge(u, c, length, ExceptionContext.DICTIONARY_PAGE, "Dictionary page");
                if (tooLarge != null) {
                    // its data pages cannot decode without it, so none of the chunk is read
                    readFailure.failAt(firstKey, c, tooLarge);
                    slices.ends()[c] = 0;
                    continue;
                }
                ChunkDictionary dictionary = new ChunkDictionary();
                slices.dictionaries()[c] = dictionary;
                lowerKeys(ranges, keys, keyColumns, c, firstKey, u.plan().dictionaryStart()[c], length);
                addDictionaryPage(new TrackedPage(u, c, firstKey, NeededPage.dictionary(u.plan().dictionaryStart()[c], length),
                        overlapping(ranges, u.plan().dictionaryStart()[c], length), dictionary, false));
            }
            reportPagesLocated(u.listing().file().name(), u.listing().rowGroup(), columns.schema(c), needed.size(),
                    RowGroupScannedEvent.STRATEGY_OFFSET_INDEX);
            slices.ends()[c] = needed.size();
            for (int i = 0; i < needed.size(); i++) {
                NeededPage page = needed.get(i);
                long key = u.key(page.location().firstRowIndex());
                // refused before it lowers its ranges' keys, so that a range only it or the
                // chunk's later pages read is keyed past the failure and never fetched; neither
                // is tracked
                Exception tooLarge = tooLarge(u, c, page.location().compressedPageSize(), page.pageIndex(), "Data page");
                if (tooLarge != null) {
                    readFailure.failAt(key, c, tooLarge);
                    slices.ends()[c] = i;
                    break;
                }
                lowerKeys(ranges, keys, keyColumns, c, key, page.location().offset(),
                        page.location().compressedPageSize());
            }
        }
        for (int i = 0; i < ranges.size(); i++) {
            if (!ranges.get(i).output().hasValue()) {
                fetcher.fetch(u.listing().file(), ranges.get(i), key(u, ranges.get(i), keys[i]));
            }
        }
        if (first != Long.MAX_VALUE) {
            for (int i = 0; i < ranges.size(); i++) {
                if (keys[i] != Long.MAX_VALUE) {
                    failOnFailure(ranges.get(i), keys[i], keyColumns[i]);
                }
            }
            addSlice(slices, first);
        }
        for (int c = 0; c < n; c++) {
            place(c);
        }
        return new ScanInput(ranges, placements);
    }

    /// A range that fails fails the read at the first row its planned pages serve, in that page's
    /// column, as that page would once tracked: the read meets the failure however far ahead of the
    /// slices tracked it lies.
    private void failOnFailure(RangeFetch r, long key, int column) {
        r.output().whenEnded(() -> {
            if (r.output().failure() != null) {
                readFailure.failAt(key, column, r.output().failure());
            }
        });
    }

    /// A unit's planned pages, tracked a slice at a time: per column, its dictionary, the next
    /// page not tracked, and the page tracking ends before, short of the column's pages where one
    /// is refused for its size ([#tooLarge]).
    private record Slices(Unit.Batched unit, List<RangeFetch> ranges, CellWriter.ChunkPlacement[] placements,
                          ChunkDictionary[] dictionaries, int[] next, int[] ends) {
    }

    /// Tracks the slice of `slices` starting at row `key` once the look-ahead reaches it: a job
    /// held back as a decode is, whose work is nothing.
    private void addSlice(Slices slices, long key) {
        scheduler.add(new JobScheduler.Job<Void>(key, 0, JobScheduler.Kind.DECODE, null, buffers -> null,
                done -> trackSlice(slices, key), t -> readFailure.failAt(key, t)));
    }

    /// Tracks each column's planned pages starting below `key` plus a slice's rows, places them,
    /// and adds the slice starting at the first page left; a column with none left has every page
    /// added.
    private void trackSlice(Slices slices, long key) {
        Unit.Batched u = slices.unit();
        long end = key + sliceRows;
        long next = Long.MAX_VALUE;
        for (int c = 0; c < slices.next().length; c++) {
            List<NeededPage> needed = u.plan().pages()[c];
            if (needed == null || slices.next()[c] >= slices.ends()[c]) {
                continue;
            }
            int i = slices.next()[c];
            int last = slices.ends()[c];
            for (; i < last && u.key(needed.get(i).location().firstRowIndex()) < end; i++) {
                NeededPage page = needed.get(i);
                long pageKey = u.key(page.location().firstRowIndex());
                add(slices.placements()[c], new TrackedPage(u, c, pageKey, page,
                        overlapping(slices.ranges(), page.location().offset(), page.location().compressedPageSize()),
                        slices.dictionaries()[c], true));
            }
            slices.next()[c] = i;
            if (i == needed.size()) {
                slices.placements()[c].allAdded();
            }
            else if (i < last) {
                next = Math.min(next, u.key(needed.get(i).location().firstRowIndex()));
            }
            place(c);
        }
        if (next != Long.MAX_VALUE) {
            addSlice(slices, next);
        }
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
        decodeWhenReady(tracked);
    }

    /// Adds `tracked`, a data page of `chunk`, to its column's placement. A page decoded through
    /// levels decodes before it is placed, a flat page once placed too ([#place]).
    void add(CellWriter.ChunkPlacement placement, TrackedPage tracked) {
        cells.add(tracked.column(), placement, tracked);
        decodeWhenReady(tracked);
    }

    /// Whether `tracked`, a page a header scan found, is refused for storing more bytes than the
    /// read's working-set limit ([#tooLarge]), checked before the scan keys its ranges to it and
    /// adds it: the read fails at the page, which is never added and holds no range, and a refused
    /// dictionary page fails its chunk's dictionary, which the chunk's data pages wait on.
    boolean refused(TrackedPage tracked) {
        Exception tooLarge = tooLarge(tracked.unit(), tracked.column(), tracked.page().location().compressedPageSize(),
                tracked.data() ? tracked.page().pageIndex() : ExceptionContext.DICTIONARY_PAGE,
                tracked.data() ? "Data page" : "Dictionary page");
        if (tooLarge == null) {
            return false;
        }
        readFailure.failAt(tracked.key(), tracked.column(), tooLarge);
        if (!tracked.data()) {
            tracked.dictionary().output().failed(tooLarge);
        }
        return true;
    }

    /// The failure of a page of `column` in `u` storing `stored` bytes, where they alone exceed the
    /// read's working-set limit ([WorkingSet.Column#requireStorable]); `null` where they do not. A
    /// page of a planned chunk is checked before its ranges are keyed to it, and one a scan finds
    /// before the scan keys them ([#refused]).
    private Exception tooLarge(Unit.Batched u, int column, long stored, int pageIndex, String what) {
        try {
            columns.workingSet(column).requireStorable(stored, what);
            return null;
        }
        catch (ParquetReadException e) {
            return columns.withContext(column, e, u.listing().file().name(), u.listing().rowGroup(), pageIndex);
        }
    }

    /// Places the pages `column` can place, ending each flat page's placement, which its decode waits
    /// on; a page that fills no cell is done and never decodes.
    void place(int column) {
        for (TrackedPage tracked : cells.place(column)) {
            if (columns.nested(column)) {
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

    /// Those of `ranges` that hold part of `[offset, offset + length)`.
    private static List<RangeFetch> overlapping(List<RangeFetch> ranges, long offset, int length) {
        List<RangeFetch> found = new ArrayList<>(2);
        for (RangeFetch r : ranges) {
            if (r.range().start() < offset + length && r.range().end() > offset) {
                found.add(r);
            }
        }
        return found;
    }

    /// Each of `ranges` that holds part of `[offset, offset + length)`, a page of `column` serving
    /// row `key`, has the page as one more reader until it is done; its entry in `keys` is lowered
    /// to `key`, recording the column in `keyColumns`. Columns come in ascending order, so of two
    /// pages serving one row the lower column's stays.
    private static void lowerKeys(List<RangeFetch> ranges, long[] keys, int[] keyColumns, int column, long key,
                                  long offset, int length) {
        for (int i = 0; i < ranges.size(); i++) {
            RangeFetch r = ranges.get(i);
            if (r.range().start() < offset + length && r.range().end() > offset) {
                r.addReader();
                if (key < keys[i]) {
                    keys[i] = key;
                    keyColumns[i] = column;
                }
            }
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
        else if (tracked.data() && !columns.nested(tracked.column()) && !tracked.placement().ended()) {
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
            readFailure.failAt(tracked.key(), tracked.column(), output.failure());
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
        if (columns.nested(tracked.column())) {
            addMeasure(tracked);
            return;
        }
        // its values are the read's working set's, which the decode charges; R holds its task's
        // buffers beside the decode need forces
        scheduler.add(decodeJob(tracked, buffers -> {
            Dictionary dictionary = tracked.dictionary() == null ? null : tracked.dictionary().output().value();
            return cells.decode(tracked.column(), tracked.task(),
                    tracked::dataPage, dictionary, buffers);
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
        scheduler.add(decodeJob(tracked, buffers -> {
            NestedPageTask task = cells.nestedTask(tracked,
                    tracked.placeholder() > 0 ? null : tracked.dataPage(),
                    tracked.dictionary() == null ? null : tracked.dictionary().output().value());
            try {
                task.decodeAndMeasure(buffers);
            }
            catch (RuntimeException e) {
                throw columns.withContext(tracked.column(), e, task);
            }
            return task;
        }, task -> {
            tracked.ready(task);
            done(tracked);
            place(tracked.column());
        }, t -> failed(tracked, t)));
    }

    /// The job decoding `tracked`, charged its task's buffers where they exceed
    /// [DecodeBuffers#RETAINED_BYTES] ([#taskBytes]): R holds them while it runs, unless need forces
    /// it, so that a read decodes such pages beside the one its working set counts only within R.
    /// A smaller task is not charged, as a context keeps that much per thread anyway.
    private <T> JobScheduler.Job<T> decodeJob(TrackedPage tracked, JobScheduler.Work<T> work, Consumer<T> onValue,
                                              Consumer<Throwable> onFailure) {
        long bytes = taskBytes(tracked);
        long cost = bytes > DecodeBuffers.RETAINED_BYTES ? bytes : 0;
        return new JobScheduler.Job<>(tracked.key(), cost, JobScheduler.Kind.DECODE, cost > 0 ? new Charge() : null,
                work, onValue, onFailure);
    }

    /// The buffers the task decoding `tracked` holds, from its header ([DataPage#taskBytes]), or a
    /// null placeholder's levels. A header its scan did not hand on is read here from a peek at the
    /// page's start, so that only its bytes are parsed, and copied where the page straddles two
    /// ranges, under the read's lock; the page keeps it for its decode ([TrackedPage#dataPage]).
    /// `0` for a page whose header does not parse: its decode fails on it.
    private long taskBytes(TrackedPage tracked) {
        ColumnSchema schema = columns.schema(tracked.column());
        if (tracked.placeholder() > 0) {
            return WorkingSet.Column.pageBuffers(schema, tracked.placeholder(), 0, Encoding.PLAIN);
        }
        if (tracked.header() != null) {
            return DataPage.taskBytes(tracked.header().header(), schema);
        }
        PageLocation location = tracked.page().location();
        try {
            Parsed<PageFormatProbe.HeaderAt> read = PageFormatProbe.readHeader(
                    (offset, length) -> RangeFetch.read(tracked.ranges(), offset, length), location.offset(),
                    location.compressedPageSize(), PageFormatProbe.Extent.PAGE, PageFormatProbe.INITIAL_PEEK_SIZE, "");
            if (read.needsBytes()) {
                return 0;
            }
            tracked.headerRead(read.value());
            return DataPage.taskBytes(read.value().header(), schema);
        }
        catch (IOException | RuntimeException e) {
            return 0;
        }
    }

    /// `tracked`'s decode failed with `t`: the read fails at the page, which is done.
    private void failed(TrackedPage tracked, Throwable t) {
        readFailure.failAt(tracked.key(), tracked.column(), t);
        done(tracked);
    }

    /// Parses a chunk's dictionary page, ending its dictionary's output, which the chunk's data
    /// pages wait on.
    private void addDictionaryDecode(TrackedPage tracked) {
        Output<Dictionary> output = tracked.dictionary().output();
        scheduler.add(decodeJob(tracked, buffers -> {
            try {
                return parseDictionary(tracked, buffers);
            }
            catch (Exception e) {
                throw columns.withContext(tracked.column(), e, tracked.unit().listing().file().name(),
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
        DataPage page = tracked.dataPage();
        ByteBuffer region = page.bytes();
        int pageLength = page.headerSize() + page.header().compressedPageSize();
        if (page.header().type() != PageType.DICTIONARY_PAGE) {
            return null;
        }
        if (pageLength > region.remaining()) {
            throw new ParquetReadException("Malformed Parquet metadata: the dictionary page header declares "
                    + pageLength + " bytes but only " + region.remaining() + " bytes lie before the first data page");
        }
        return columns.chargeDictionary(tracked.column(), tracked.unit(), () -> DictionaryParser.parsePage(page.header(), page.body(),
                columns.schema(tracked.column()), tracked.unit().listing().chunks()[tracked.column()].metaData(), context,
                columns.workingSet(tracked.column()), buffers));
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
