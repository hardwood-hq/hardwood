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
import dev.hardwood.internal.reader.batch.Dictionary;
import dev.hardwood.internal.reader.batch.FlatBatch;
import dev.hardwood.internal.reader.decode.DataPage;
import dev.hardwood.internal.reader.decode.DictionaryParser;
import dev.hardwood.internal.reader.decode.PageFormatProbe;
import dev.hardwood.internal.reader.decode.PageInfo;
import dev.hardwood.internal.reader.fetch.FileRange;
import dev.hardwood.internal.reader.memory.ReadAheadBuffer;
import dev.hardwood.internal.reader.memory.WorkingSet;
import dev.hardwood.internal.reader.pipeline.Cell;
import dev.hardwood.internal.reader.pipeline.NestedPageTask;
import dev.hardwood.internal.reader.pipeline.Segment;
import dev.hardwood.internal.reader.plan.NeededPage;
import dev.hardwood.internal.reader.select.PageRowMask;
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
    private final int batchCapacity;

    /// @param batchCapacity the most rows a cell holds
    PageTracker(JobScheduler scheduler, RangeFetcher fetcher, BatchPublisher publisher, ReadFailure readFailure,
                CellWriter cells, ColumnContext columns, HardwoodContextImpl context, int batchCapacity) {
        this.batchCapacity = batchCapacity;
        this.scheduler = scheduler;
        // every flat column knows none of its pages until its scan finds them
        for (int column = 0; column < columns.count(); column++) {
            if (!columns.nested(column)) {
                scheduler.known(column, 0);
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
        decodeWhenReady(tracked);
    }

    /// Adds `tracked`, a data page of `chunk`, to its column's placement. A page decoded through
    /// levels decodes before it is placed, a flat page once placed too ([#place]).
    void add(CellWriter.ChunkPlacement placement, TrackedPage tracked) {
        hold(tracked);
        cells.add(tracked.column(), placement, tracked);
        if (tracked.data() && !columns.nested(tracked.column())) {
            tracked.pending(scheduler.pend(tracked.column(), tracked.key()));
            ColumnSchema schema = columns.schema(tracked.column());
            int kept = tracked.placeholder() > 0
                    ? PageInfo.keptRecords(schema, PageRowMask.ALL, tracked.placeholder())
                    : PageInfo.keptRecords(schema, tracked.page().mask(), tracked.page().rows());
            scheduler.known(tracked.column(), tracked.key() + kept);
        }
        decodeWhenReady(tracked);
    }

    /// Bounds what `tracked`'s decode would allocate, for the read's reserve: from its header and
    /// cells where it has them; a numeric page from its rows otherwise, and a cell it may open past
    /// them; a binary page without its header has no bound yet.
    private void bound(TrackedPage tracked) {
        ColumnSchema schema = columns.schema(tracked.column());
        PageHeader header = header(tracked);
        boolean numeric = WorkingSet.Column.numeric(schema);
        if (!numeric && header == null && tracked.placeholder() == 0) {
            JobScheduler.bound(tracked.pending(), 0, true);
        }
        else if (!tracked.task().segments().isEmpty()) {
            JobScheduler.bound(tracked.pending(), keptBytes(tracked, header), false);
        }
        else {
            long values = (long) tracked.page().rows() + batchCapacity;
            long bytes = values * WorkingSet.Column.batchBytes(schema, 1, 0) + validityBytes(schema, values)
                    + (schema.maxDefinitionLevel() > 1 ? values * Integer.BYTES : 0)
                    + (numeric || header == null ? 0 : header.uncompressedPageSize());
            JobScheduler.bound(tracked.pending(), bytes, false);
        }
    }

    /// `tracked` leaves the read's reserve: its decode is added, or it will not decode.
    private void unpend(TrackedPage tracked) {
        if (tracked.pending() != null) {
            scheduler.unpend(tracked.column(), tracked.pending());
            tracked.pending(null);
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
        if (tracked.pending() != null) {
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
        // its values are the read's working set's, which the decode charges; R holds its task's
        // buffers beside the decode need forces, and the batches it fills until they are published
        unpend(tracked);
        Charge kept = new Charge();
        scheduler.add(decodeJob(tracked, kept, buffers -> {
            if (kept.isRefused()) {
                throw columns.withContext(tracked.column(), new ParquetReadException("Data page needs "
                        + kept.refusedNeeded() + " bytes for its batches, which takes the context's heap quota to "
                        + kept.refusedTotal() + " bytes, more than its limit of " + kept.refusedLimit() + " ("
                        + ReadAheadBuffer.READ_AHEAD_BYTES_PROPERTY + ")"), tracked.task());
            }
            Dictionary dictionary = tracked.dictionary() == null ? null : tracked.dictionary().output().value();
            return cells.decode(tracked.column(), tracked.task(),
                    tracked::dataPage, dictionary, buffers);
        }, completed -> {
            // its ranges are held by the batch of its last row until every column has published it,
            // as R holds the bytes fetched ahead of the consumer
            publisher.hold(tracked.column(), tracked.task().segments().getLast().cell(), tracked.ranges());
            holdKept(tracked, kept);
            for (Cell cell : completed) {
                publisher.completed(tracked.column(), cell);
            }
        }, t -> failed(tracked, t)));
    }

    /// Decodes and measures a page decoded through levels, which can then be placed.
    private void addMeasure(TrackedPage tracked) {
        scheduler.add(decodeJob(tracked, null, buffers -> {
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
    /// A smaller task is not charged, as a context keeps that much per thread anyway. With `kept`,
    /// the job also charges the batches its page fills ([#batchBytes]), which `kept` holds past its
    /// end, until they are published: unless need forces it, the read decodes ahead of the consumer
    /// only within R.
    private <T> JobScheduler.Job<T> decodeJob(TrackedPage tracked, Charge kept, JobScheduler.Work<T> work,
                                              Consumer<T> onValue, Consumer<Throwable> onFailure) {
        PageHeader header = header(tracked);
        long bytes = taskBytes(tracked, header);
        long cost = bytes > DecodeBuffers.RETAINED_BYTES ? bytes : 0;
        long keptCost = kept == null ? 0 : keptBytes(tracked, header);
        return new JobScheduler.Job<>(tracked.key(), cost, JobScheduler.Kind.DECODE, cost > 0 ? new Charge() : null,
                keptCost, kept, tracked.column(), work, onValue, onFailure);
    }

    /// The bytes `tracked`'s decode leaves in the batches it fills: the arrays of each cell it
    /// opens, which its decode allocates at the cell's capacity, and, for a binary column, the
    /// page's bytes, which its values copy.
    private long keptBytes(TrackedPage tracked, PageHeader header) {
        ColumnSchema schema = columns.schema(tracked.column());
        long bytes = 0;
        boolean dictionary = header != null && isDictionary(DataPage.encoding(header));
        for (Segment segment : tracked.task().segments()) {
            if (segment.cellOffset() == 0) {
                bytes += cellBytes(schema, segment.cell().capacity(), dictionary);
            }
        }
        if (!WorkingSet.Column.numeric(schema) && header != null && !dictionary) {
            bytes += header.uncompressedPageSize();
        }
        return bytes;
    }

    private static boolean isDictionary(Encoding encoding) {
        return encoding == Encoding.PLAIN_DICTIONARY || encoding == Encoding.RLE_DICTIONARY;
    }

    /// The bytes a cell of `capacity` records allocates for `schema`'s column: its values, as
    /// dictionary ids where its page is dictionary-encoded, for a nullable column its validity bits,
    /// and for one below an optional struct its definition levels.
    private static long cellBytes(ColumnSchema schema, int capacity, boolean dictionary) {
        long bytes = dictionary
                ? (long) capacity * Integer.BYTES
                : WorkingSet.Column.batchBytes(schema, capacity, 0);
        bytes += validityBytes(schema, capacity);
        if (schema.maxDefinitionLevel() > 1) {
            bytes += (long) capacity * Integer.BYTES;
        }
        return bytes;
    }

    /// The bytes of a nullable column's validity bits for a cell of `capacity` records.
    private static long validityBytes(ColumnSchema schema, long capacity) {
        return schema.maxDefinitionLevel() > 0 ? (capacity + 63L) / 64 * Long.BYTES : 0;
    }

    /// Hands what `tracked`'s decode kept to the batches that hold it, which return it once the
    /// consumer takes them ([BatchExchange#setTakenListener]): each opened cell's arrays to the
    /// cell's batch, the rest to the batch of the page's last row. A batch made later, as the read
    /// reaches it, carries its charge from its decode on.
    private void holdKept(TrackedPage tracked, Charge kept) {
        ColumnSchema schema = columns.schema(tracked.column());
        long admitted = kept.takeBack();
        long grown = 0;
        for (Segment segment : tracked.task().segments()) {
            FlatBatch batch = segment.cell().columnBatch();
            if (batch != null) {
                // a recycled holder keeps the charge of its arrays from the fill that grew them; the
                // validity its cell completes it with counts from its capacity
                long held = batch.arrayBytes() + validityBytes(schema, segment.cell().capacity());
                grown += held - batch.heldBytes;
                batch.heldBytes = held;
            }
        }
        if (grown < admitted) {
            scheduler.releaseTaken(admitted - grown);
        }
        else if (grown > admitted) {
            scheduler.chargeHeld(grown - admitted);
        }
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

    /// `tracked`'s decode failed with `t`: the read fails at the page, which is done.
    private void failed(TrackedPage tracked, Throwable t) {
        readFailure.failAt(tracked.key(), tracked.column(), t);
        done(tracked);
    }

    /// Parses a chunk's dictionary page, ending its dictionary's output, which the chunk's data
    /// pages wait on.
    private void addDictionaryDecode(TrackedPage tracked) {
        Output<Dictionary> output = tracked.dictionary().output();
        scheduler.add(decodeJob(tracked, null, buffers -> {
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
        unpend(tracked);
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
