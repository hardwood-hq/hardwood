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
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.reader.batch.AllocationTally;
import dev.hardwood.internal.reader.batch.Dictionary;
import dev.hardwood.internal.reader.decode.DataPage;
import dev.hardwood.internal.reader.decode.DictionaryParser;
import dev.hardwood.internal.reader.decode.PageFormatProbe;
import dev.hardwood.internal.reader.decode.PageInfo;
import dev.hardwood.internal.reader.fetch.FileRange;
import dev.hardwood.internal.reader.memory.ReadAheadBuffer;
import dev.hardwood.internal.reader.memory.WorkingSet;
import dev.hardwood.internal.reader.pipeline.Cell;
import dev.hardwood.internal.reader.pipeline.NestedPageTask;
import dev.hardwood.internal.reader.plan.NeededPage;
import dev.hardwood.internal.reader.select.PageRowMask;
import dev.hardwood.internal.reader.step.Parsed;
import dev.hardwood.jfr.RowGroupScannedEvent;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.metadata.PageType;
import dev.hardwood.metadata.RepetitionType;
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
    private final int batchCapacity;

    /// @param batchCapacity the most rows a cell holds
    PageTracker(JobScheduler scheduler, RangeFetcher fetcher, BatchPublisher publisher, ReadFailure readFailure,
                CellWriter cells, ColumnContext columns, HardwoodContextImpl context, int batchCapacity) {
        this.batchCapacity = batchCapacity;
        this.scheduler = scheduler;
        // every flat column knows none of its pages until its scan finds them
        for (int column = 0; column < columns.count(); column++) {
            if (!columns.nested(column)) {
                scheduler.found(column, 0);
            }
        }
        this.fetcher = fetcher;
        this.publisher = publisher;
        this.readFailure = readFailure;
        this.cells = cells;
        this.columns = columns;
        this.context = context;
    }

    /// What the scans of a batched unit's chunks without an OffsetIndex read: its ranges and
    /// each column's chunk of the placement.
    record ScanInput(List<RangeFetch> ranges, CellWriter.ChunkPlacement[] placements) {
    }

    /// Starts tracking the pages of `u`, once it is batched: places its planned pages,
    /// fetches its ranges and decodes each page whose bytes are there; returns the ranges and the
    /// placement's chunks, for the scans of its chunks without an OffsetIndex. A range a scan reads
    /// is keyed at the row as far into the chunk as it lies. Tracking holds each range as one
    /// more reader, so that one its pages are done with stays until the scans hold it too;
    /// [#handedToScans] lets go of it.
    ScanInput track(Unit.Batched u) {
        List<RangeFetch> ranges = new ArrayList<>();
        CellWriter.ChunkPlacement[] placements = new CellWriter.ChunkPlacement[u.plan().pages().length];
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
            CellWriter.ChunkPlacement placement = cells.placement(c, u);
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
                // decoded by the unit's dictionary pruning
                dictionary = ChunkDictionary.preloaded(u.plan().preloaded()[c]);
            }
            else if (u.plan().dictionaryStart()[c] > 0) {
                dictionary = new ChunkDictionary();
                int length = Math.toIntExact(u.plan().firstDataPage()[c] - u.plan().dictionaryStart()[c]);
                addDictionaryPage(new TrackedPage(u, c, firstKey, NeededPage.dictionary(u.plan().dictionaryStart()[c], length),
                        overlapping(ranges, keys, firstKey, u.plan().dictionaryStart()[c], length), dictionary, false));
            }
            reportPagesLocated(u.listing().file().name(), u.listing().rowGroup(), columns.schema(c), needed.size(),
                    RowGroupScannedEvent.STRATEGY_OFFSET_INDEX);
            for (NeededPage page : needed) {
                int length = page.location().compressedPageSize();
                long key = u.key(page.location().firstRowIndex());
                add(placement, new TrackedPage(u, c, key, page,
                        overlapping(ranges, keys, key, page.location().offset(), length), dictionary, true));
            }
            if (dictionary != null) {
                dictionary.seal();
            }
        }
        for (int i = 0; i < ranges.size(); i++) {
            if (!ranges.get(i).output().hasValue()) {
                fetcher.fetch(u.listing().file(), ranges.get(i), key(u, ranges.get(i), keys[i]));
            }
        }
        for (int c = 0; c < u.plan().pages().length; c++) {
            place(c);
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
        tracked.frontier(scheduler.enter(tracked.column(), tracked.key()));
        decodeWhenReady(tracked);
    }

    /// Adds `tracked`, a data page of `chunk`, to its column's placement. A page decoded through
    /// levels decodes before it is placed, a flat page once placed too ([#place]).
    void add(CellWriter.ChunkPlacement placement, TrackedPage tracked) {
        hold(tracked);
        if (tracked.dictionary() != null) {
            tracked.dictionary().pageAdded();
        }
        cells.add(tracked.column(), placement, tracked);
        tracked.frontier(scheduler.enter(tracked.column(), tracked.key()));
        if (!columns.nested(tracked.column())) {
            ColumnSchema schema = columns.schema(tracked.column());
            int kept = tracked.placeholder() > 0
                    ? PageInfo.keptRecords(schema, PageRowMask.ALL, tracked.placeholder())
                    : PageInfo.keptRecords(schema, tracked.page().mask(), tracked.page().rows());
            scheduler.found(tracked.column(), tracked.key() + kept);
        }
        decodeWhenReady(tracked);
    }

    /// Bounds what `tracked`'s decode adds to its column's batch holders, for the read's reserve
    /// and its decode's claim: the arrays of every cell it fills, which its decode may be the one
    /// to allocate, for a nullable column each cell's validity and the one the holder held before,
    /// and for plain binary values the chunks their bytes take, at most twice the page's
    /// uncompressed size. A page not placed yet counts the cells its rows can reach. A binary page
    /// without its header has no bound yet.
    private void bound(TrackedPage tracked) {
        ColumnSchema schema = columns.schema(tracked.column());
        PageHeader header = header(tracked);
        if (!tracked.data() || columns.nested(tracked.column())) {
            boundKept(tracked, schema, header);
            return;
        }
        boolean numeric = WorkingSet.Column.numeric(schema);
        if (!numeric && header == null && tracked.placeholder() == 0) {
            return;
        }
        int segments = tracked.task().segments().size();
        long reached = segments > 0 ? segments
                : ((long) tracked.page().rows() + batchCapacity - 1) / batchCapacity + 1;
        long validity = schema.maxDefinitionLevel() > 0 ? reached * ((batchCapacity + 63L) / 64) * Long.BYTES : 0;
        // what no free holder brings: each cell's validity, installed anew, and the chunks of plain
        // bytes, of which a free holder brings at most full ones, which a fill of few bytes does not
        // take
        long fresh = validity + tracked.reconstructed();
        if (!numeric && header != null && !isDictionary(DataPage.encoding(header))) {
            fresh += 2L * header.uncompressedPageSize();
        }
        // a reused holder keeps its last validity until the cell's replaces it as the cell completes
        long bytes = reached * holderBytes(tracked.column(), schema) + validity + fresh;
        scheduler.bound(tracked.frontier(), bytes, fresh, chargedTaskBytes(tracked, header), exact(tracked, header));
    }

    /// Whether the bound of `tracked`, from its `header`, holds all its decode keeps: not for a
    /// `DELTA_BYTE_ARRAY` page, whose values a decode reconstructs beyond its bytes, until a
    /// decode of it learned them.
    private static boolean exact(TrackedPage tracked, PageHeader header) {
        return header == null || header.type() == PageType.DICTIONARY_PAGE || tracked.reconstructed() > 0
                || DataPage.encoding(header) != Encoding.DELTA_BYTE_ARRAY;
    }

    /// Bounds what the decode of `tracked`, a dictionary page or a page decoded through levels,
    /// keeps past its end, from its `header`: a dictionary's entries, which its parse holds as
    /// buffers while it runs too; a page's batches, levels and offsets with its buffers, which the
    /// column keeps until its cells fill. A page without its header has no bound yet.
    private void boundKept(TrackedPage tracked, ColumnSchema schema, PageHeader header) {
        long kept;
        long task = 0;
        if (!tracked.data()) {
            if (header == null || header.type() != PageType.DICTIONARY_PAGE) {
                return;
            }
            kept = WorkingSet.Column.dictionaryEntries(schema, header.dictionaryPageHeader().numValues(),
                    header.uncompressedPageSize());
            task = chargedTaskBytes(tracked, header);
        }
        else if (tracked.placeholder() > 0) {
            kept = WorkingSet.Column.keptPage(schema, tracked.placeholder(), 0, Encoding.PLAIN);
        }
        else if (header == null || header.type() == PageType.DICTIONARY_PAGE) {
            return;
        }
        else {
            kept = WorkingSet.Column.keptPage(schema, DataPage.numValues(header), header.uncompressedPageSize(),
                    DataPage.encoding(header)) + tracked.reconstructed();
        }
        scheduler.bound(tracked.frontier(), kept, kept, task, exact(tracked, header));
    }

    /// The bytes the arrays of one cell's batch holder of `column` take at most beside its
    /// validity: its values at the read's batch capacity, a binary column's views, entry ids and
    /// the window its completion may build among them; for a column below an optional group its
    /// definition levels; and for a filtered column its matches.
    private long holderBytes(int column, ColumnSchema schema) {
        long bytes = WorkingSet.Column.batchBytes(schema, batchCapacity, 0);
        if (!WorkingSet.Column.numeric(schema)) {
            bytes += 3L * batchCapacity * Integer.BYTES;
        }
        if (schema.maxDefinitionLevel() > (schema.repetitionType() == RepetitionType.OPTIONAL ? 1 : 0)) {
            bytes += (long) batchCapacity * Integer.BYTES;
        }
        if (cells.filtered(column)) {
            bytes += (batchCapacity + 63L) / 64 * Long.BYTES;
        }
        return bytes;
    }

    private static boolean isDictionary(Encoding encoding) {
        return encoding == Encoding.PLAIN_DICTIONARY || encoding == Encoding.RLE_DICTIONARY;
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
        if (tracked.frontier() != null) {
            bound(tracked);
        }
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
        // its task's buffers are the read's working set's, which the decode charges; R holds them
        // beside the decode need forces, and the claim of what it adds to the column's batch holders
        bound(tracked);
        Charge claim = new Charge();
        AllocationTally tally = new AllocationTally();
        scheduler.add(decodeJob(tracked, claim, tally, buffers -> {
            if (claim.isRefused()) {
                throw columns.withContext(tracked.column(), refusal("Data page", claim), tracked.task());
            }
            Dictionary dictionary = tracked.dictionary() == null ? null : tracked.dictionary().output().value();
            return cells.decode(tracked.column(), tracked.task(),
                    tracked::dataPage, dictionary, buffers, tally);
        }, completed -> {
            // its ranges are held by the batch of its last row until every column has published it,
            // as R holds the bytes fetched ahead of the consumer
            publisher.hold(tracked.column(), tracked.task().segments().getLast().cell(), tracked.ranges());
            tracked.decodeEnded();
            for (Cell cell : completed) {
                publisher.completed(tracked.column(), cell);
            }
        }, t -> failed(tracked, t)));
    }

    /// Decodes and measures a page decoded through levels, which can then be placed.
    private void addMeasure(TrackedPage tracked) {
        bound(tracked);
        Charge claim = new Charge();
        scheduler.add(decodeJob(tracked, claim, null, buffers -> {
            NestedPageTask task = cells.nestedTask(tracked,
                    tracked.placeholder() > 0 ? null : tracked.dataPage(),
                    tracked.dictionary() == null ? null : tracked.dictionary().output().value());
            if (claim.isRefused()) {
                throw columns.withContext(tracked.column(), refusal("Data page", claim), task);
            }
            try {
                task.decodeAndMeasure(buffers);
            }
            catch (NoRoomException e) {
                throw e;
            }
            catch (RuntimeException e) {
                throw columns.withContext(tracked.column(), e, task);
            }
            return task;
        }, task -> {
            // the column keeps the page until its cells fill, and its batches hold its values until
            // the consumer takes them
            publisher.ride(tracked.column(), tracked.key() + task.info().records(), claim.takeBack());
            tracked.ready(task);
            done(tracked);
            place(tracked.column());
        }, t -> failed(tracked, t)));
    }

    /// The job decoding `tracked`, charged its task's buffers where they exceed
    /// [DecodeBuffers#RETAINED_BYTES] ([#taskBytes]): R holds them while it runs, unless need forces
    /// it, so that a read decodes such pages beside the one its working set counts only within R.
    /// A smaller task is not charged, as a context keeps that much per thread anyway. With `claim`,
    /// a flat page's decode, it also claims the bound of its frontier page, settled to what `tally`
    /// counts as it ends ([JobScheduler.Job]).
    private <T> JobScheduler.Job<T> decodeJob(TrackedPage tracked, Charge claim, AllocationTally tally,
                                              JobScheduler.Work<T> work, Consumer<T> onValue,
                                              Consumer<Throwable> onFailure) {
        // a page decoded through levels keeps its buffers past its task, which its claim holds
        long cost = tracked.data() && columns.nested(tracked.column()) ? 0 : chargedTaskBytes(tracked, header(tracked));
        return new JobScheduler.Job<>(tracked.key(), cost, JobScheduler.Kind.DECODE, cost > 0 ? new Charge() : null,
                tracked.frontier(), claim, tally, tracked.data() ? new JobScheduler.Draw(tracked.reconstructed()) : null,
                work, onValue, onFailure);
    }

    /// The buffers the task decoding `tracked` holds where they exceed
    /// [DecodeBuffers#RETAINED_BYTES], which the read charges; `0` for a smaller task, as a
    /// context keeps that much per thread anyway.
    private long chargedTaskBytes(TrackedPage tracked, PageHeader header) {
        long bytes = taskBytes(tracked, header);
        return bytes > DecodeBuffers.RETAINED_BYTES ? bytes : 0;
    }

    /// The buffers the task decoding `tracked` holds, from its `header` ([DataPage#taskBytes]), or
    /// a null placeholder's levels; `0` for a page whose header does not parse: its decode fails on
    /// it.
    private long taskBytes(TrackedPage tracked, PageHeader header) {
        ColumnSchema schema = columns.schema(tracked.column());
        if (tracked.placeholder() > 0) {
            return WorkingSet.Column.pageBuffers(schema, tracked.placeholder(), 0, Encoding.PLAIN);
        }
        return header == null ? 0 : DataPage.taskBytes(header, schema);
    }

    /// The header of `tracked`, `null` for a null placeholder or a header that does not parse. A
    /// header its scan did not hand on is read here from a peek at the page's start, so that only
    /// its bytes are parsed, and copied where the page straddles two ranges, under the read's lock;
    /// the page keeps it for its decode ([TrackedPage#dataPage]).
    private PageHeader header(TrackedPage tracked) {
        if (tracked.placeholder() > 0) {
            return null;
        }
        if (tracked.header() != null) {
            return tracked.header().header();
        }
        PageLocation location = tracked.page().location();
        try {
            Parsed<PageFormatProbe.HeaderAt> read = PageFormatProbe.readHeader(
                    (offset, length) -> RangeFetch.read(tracked.ranges(), offset, length), location.offset(),
                    location.compressedPageSize(), PageFormatProbe.Extent.PAGE, PageFormatProbe.INITIAL_PEEK_SIZE, "");
            if (read.needsBytes()) {
                return null;
            }
            tracked.headerRead(read.value());
            return read.value().header();
        }
        catch (IOException | RuntimeException e) {
            return null;
        }
    }

    /// `tracked`'s decode failed with `t`: the read fails at the page, which is done; or, where the
    /// decode gave up for lack of room for the values it reconstructs, the page returns to its
    /// column's frontier with them counted, and decodes again.
    private void failed(TrackedPage tracked, Throwable t) {
        if (t instanceof NoRoomException noRoom) {
            tracked.reconstructed(noRoom.bytes());
            tracked.frontier(scheduler.enter(tracked.column(), tracked.key()));
            addDecode(tracked);
            return;
        }
        readFailure.failAt(tracked.key(), tracked.column(), t);
        done(tracked);
    }

    /// Parses a chunk's dictionary page, ending its dictionary's output, which the chunk's data
    /// pages wait on.
    private void addDictionaryDecode(TrackedPage tracked) {
        Output<Dictionary> output = tracked.dictionary().output();
        bound(tracked);
        Charge claim = new Charge();
        scheduler.add(decodeJob(tracked, claim, null, buffers -> {
            try {
                if (claim.isRefused()) {
                    throw refusal("Dictionary page", claim);
                }
                return parseDictionary(tracked, buffers);
            }
            catch (Exception e) {
                throw columns.withContext(tracked.column(), e, tracked.unit().listing().file().name(),
                        tracked.unit().listing().rowGroup(), ExceptionContext.DICTIONARY_PAGE);
            }
        }, values -> {
            holdDictionary(tracked.column(), tracked.unit(), tracked.dictionary(), claim.takeBack());
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
        return DictionaryParser.parsePage(page.header(), page.body(), columns.schema(tracked.column()),
                tracked.unit().listing().chunks()[tracked.column()].metaData(), context, columns.charges(), buffers);
    }

    /// The failure of a decode need forced whose `claim` did not fit the context's heap quota:
    /// `what` needs the bytes the claim names.
    static ParquetReadException refusal(String what, Charge claim) {
        return new ParquetReadException(what + " needs " + claim.refusedNeeded() + " bytes to decode, which takes "
                + "the context's heap quota to " + claim.refusedTotal() + " bytes, more than its limit of "
                + claim.refusedLimit() + " (" + ReadAheadBuffer.READ_AHEAD_BYTES_PROPERTY + ")");
    }

    /// Holds `bytes` charged for the entries of `dictionary`, `u`'s chunk of `column`'s: a numeric
    /// column's until every data page of the chunk has decoded, after which its batches hold the
    /// values they took from it; a binary column's while a batch holds rows of the chunk, as its
    /// batches refer to its entries.
    void holdDictionary(int column, Unit.Batched u, ChunkDictionary dictionary, long bytes) {
        if (WorkingSet.Column.numeric(columns.schema(column))) {
            dictionary.holdUntilDecoded(bytes, scheduler::releaseTaken);
        }
        else {
            publisher.ride(column, chunkEnd(u), bytes);
        }
    }

    /// The key past the last row of `u`'s row group the read takes.
    static long chunkEnd(Unit.Batched u) {
        return u.key(u.listing().metaData().numRows());
    }

    /// A page is done with its ranges; the last reader of each returns it.
    private void done(TrackedPage tracked) {
        tracked.decodeEnded();
        if (tracked.frontier() != null) {
            scheduler.leave(tracked.frontier());
        }
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
