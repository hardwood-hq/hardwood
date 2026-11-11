/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.internal.predicate.ColumnBatchMatcher;
import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.batch.BinaryBatchValues;
import dev.hardwood.internal.reader.batch.BitmapWords;
import dev.hardwood.internal.reader.batch.Dictionary;
import dev.hardwood.internal.reader.batch.NestedBatch;
import dev.hardwood.internal.reader.batch.NestedIndexMode;
import dev.hardwood.internal.reader.decode.BinaryPageValues;
import dev.hardwood.internal.reader.decode.DataPage;
import dev.hardwood.internal.reader.decode.FlatPageValues;
import dev.hardwood.internal.reader.decode.PageDecoder;
import dev.hardwood.internal.reader.decode.PageInfo;
import dev.hardwood.internal.reader.memory.WorkingSet;
import dev.hardwood.internal.reader.pipeline.BatchCell;
import dev.hardwood.internal.reader.pipeline.BinaryFill;
import dev.hardwood.internal.reader.pipeline.Cell;
import dev.hardwood.internal.reader.pipeline.ColumnPlacer;
import dev.hardwood.internal.reader.pipeline.FlatFill;
import dev.hardwood.internal.reader.pipeline.NestedCell;
import dev.hardwood.internal.reader.pipeline.NestedPageTask;
import dev.hardwood.internal.reader.pipeline.PageTask;
import dev.hardwood.internal.reader.pipeline.Segment;
import dev.hardwood.internal.reader.pipeline.SegmentFillException;
import dev.hardwood.internal.reader.select.PageRowMask;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.reader.ParquetReadException;

/// Writes each column's pages into the cells of a read's batch plan: places them, decodes and fills
/// them, and completes each cell into its exchange's batch. Pages are placed through Hardwood's
/// [ColumnPlacer], in page order: a column's chunks unit after unit, a chunk's pages as they are known
/// (planned, once their unit is batched, or found by a scan), each once it can be. A flat page can be placed at
/// once; its job then decodes it and fills its segments, and its cell completes at its last fill
/// or its seal, whichever comes last. A page decoded through levels can be placed once its job has
/// measured it; a cell it is placed into is filled on a job once sealed.
///
/// At the read's end every column's last cell is sealed. At a failure between two units, every
/// column's open cell is sealed, or abandoned where the column is short of the plan; at a failure
/// inside a unit the open cell is left, as the batch holding the failure's row is never published.
/// Used under the read's lock, except [#decode] and [#nestedTask].
final class CellWriter {

    /// Scratch a column decoded through levels keeps for its next pages.
    private static final int SCRATCH_POOL = 4;

    /// The placement of one column chunk: its pages in page order as they are known, how many are
    /// placed, whether the chunk has ended, and a scan's failure past its pages.
    static final class ChunkPlacement {
        private final Unit.Batched unit;
        private final List<TrackedPage> pages = new ArrayList<>();
        private PageDecoder decoder;
        private int placed;
        private boolean ended;
        /// the scan's failure, past its pages: the column places nothing after them
        private Throwable failure;

        ChunkPlacement(Unit.Batched unit) {
            this.unit = unit;
        }

        Unit.Batched unit() {
            return unit;
        }

        /// The chunk's decoder, `null` until its first data page is added.
        PageDecoder decoder() {
            return decoder;
        }

        void decoder(PageDecoder decoder) {
            this.decoder = decoder;
        }

        /// `page` follows its pages.
        void add(TrackedPage page) {
            pages.add(page);
        }

        /// Takes its next page, where that is ready to be placed; `null` otherwise.
        TrackedPage nextReady() {
            if (placed < pages.size() && pages.get(placed).ready()) {
                return pages.get(placed++);
            }
            return null;
        }

        /// Every page of the chunk is added.
        void allAdded() {
            ended = true;
        }

        /// Whether the chunk has ended and every page of it is placed.
        boolean done() {
            return ended && placed == pages.size();
        }

        /// The chunk's scan failed with `failure` past its pages; the chunk ends there.
        void failed(Throwable failure) {
            this.failure = failure;
            ended = true;
        }

        /// The scan's failure past its pages, `null` where none.
        Throwable failure() {
            return failure;
        }
    }

    private final JobScheduler scheduler;
    private final BatchPublisher publisher;
    private final ColumnContext columns;
    private final List<BatchExchange<?>> exchanges;
    private final NestedIndexMode indexMode;
    private final boolean fixedListFastPathEnabled;
    private final DecompressorFactory decompressors;
    private final List<ColumnPlacer<? extends BatchCell<?>>> placers = new ArrayList<>();
    private final List<ArrayDeque<ChunkPlacement>> placements = new ArrayList<>();
    private final List<ArrayBlockingQueue<PageDecoder.LevelScratch>> scratch = new ArrayList<>();
    private final BinaryBatchValues.ViewDemand[] views;
    /// per flat column, its filter fragment, which writes each batch's matches as it completes;
    /// `null` for none
    private final ColumnBatchMatcher[] filters;
    /// per column, the last page it placed, which a shortfall at the read's end is placed with
    private final PageTask[] last;

    CellWriter(JobScheduler scheduler, BatchPublisher publisher, ColumnContext columns, List<BatchExchange<?>> exchanges,
               NestedIndexMode indexMode, boolean fixedListFastPathEnabled, int batchCapacity,
               DecompressorFactory decompressors, ColumnBatchMatcher[] filters) {
        this.scheduler = scheduler;
        this.filters = filters;
        this.publisher = publisher;
        this.columns = columns;
        this.exchanges = exchanges;
        this.indexMode = indexMode;
        this.fixedListFastPathEnabled = fixedListFastPathEnabled;
        this.decompressors = decompressors;
        int n = columns.count();
        this.views = new BinaryBatchValues.ViewDemand[n];
        this.last = new PageTask[n];
        for (int c = 0; c < n; c++) {
            placers.add(columns.nested(c) ? new ColumnPlacer<>(new NestedSink(c), batchCapacity)
                    : new ColumnPlacer<>(new Sink(c), batchCapacity));
            placements.add(new ArrayDeque<>());
            scratch.add(new ArrayBlockingQueue<>(SCRATCH_POOL));
            views[c] = new BinaryBatchValues.ViewDemand();
        }
    }

    /// The column's chunk of `u`, behind the chunks of the units before it.
    ChunkPlacement placement(int column, Unit.Batched u) {
        ChunkPlacement placement = new ChunkPlacement(u);
        placements.get(column).addLast(placement);
        return placement;
    }

    /// Adds `tracked`, a data page of `column`'s `chunk`, behind the chunk's pages; a flat page can be
    /// placed at once.
    void add(int column, ChunkPlacement placement, TrackedPage tracked) {
        ColumnMetaData meta = placement.unit().listing().chunks()[column].metaData();
        if (placement.decoder() == null) {
            placement.decoder(new PageDecoder(meta, columns.schema(column), decompressors,
                    !columns.nested(column) || fixedListFastPathEnabled, columns.workingSet(column)));
        }
        tracked.decoder(placement.decoder());
        if (!columns.nested(column)) {
            Unit.Batched u = placement.unit();
            tracked.ready(new PageTask(info(column, tracked, null, null), u.batches().fileName(), u.listing().rowGroup(),
                    tracked.page().pageIndex(), u.batches(), tracked.decoder(),
                    u.batches().readRowsBefore() + tracked.page().location().firstRowIndex()));
        }
        placement.add(tracked);
    }

    private PageInfo info(int column, TrackedPage tracked, DataPage page, Dictionary dictionary) {
        if (tracked.placeholder() > 0) {
            return PageInfo.nullPlaceholder(tracked.placeholder(), columns.schema(column), tracked.unit().listing().chunks()[column].metaData(),
                    PageInfo.keptRecords(columns.schema(column), PageRowMask.ALL, tracked.placeholder()));
        }
        return new PageInfo(page, columns.schema(column), tracked.unit().listing().chunks()[column].metaData(), dictionary,
                tracked.page().mask(), PageInfo.keptRecords(columns.schema(column), tracked.page().mask(), tracked.page().rows()));
    }

    /// Whether `column`'s placement has ended: it has placed its last row, or met a failure.
    boolean placementEnded(int column) {
        return placers.get(column).stopped();
    }

    /// Whether every column's placement has ended, of a read of at least one: the read takes no
    /// further page.
    boolean allPlacementEnded() {
        if (placers.isEmpty()) {
            return false;
        }
        for (ColumnPlacer<?> p : placers) {
            if (!p.stopped()) {
                return false;
            }
        }
        return true;
    }

    /// The task of a page decoded through levels whose bytes are `page`, on a job.
    NestedPageTask nestedTask(TrackedPage tracked, DataPage page, Dictionary dictionary) {
        PageDecoder.LevelScratch levels = scratch.get(tracked.column()).poll();
        Unit.Batched u = tracked.unit();
        return new NestedPageTask(info(tracked.column(), tracked, page, dictionary), u.batches().fileName(), u.listing().rowGroup(),
                tracked.page().pageIndex(), u.batches(), tracked.decoder(),
                u.batches().readRowsBefore() + tracked.page().location().firstRowIndex(),
                levels != null ? levels : new PageDecoder.LevelScratch());
    }

    /// Places the column's pages that can be, in order, moving on to its next chunk once one has
    /// ended and is placed; returns the pages it took.
    List<TrackedPage> place(int column) {
        List<TrackedPage> placed = new ArrayList<>();
        ArrayDeque<ChunkPlacement> q = placements.get(column);
        while (!q.isEmpty()) {
            ChunkPlacement placement = q.peekFirst();
            for (TrackedPage tracked = placement.nextReady(); tracked != null; tracked = placement.nextReady()) {
                if (!placers.get(column).stopped()) {
                    place(column, tracked);
                }
                placed.add(tracked);
            }
            if (!placement.done()) {
                break;
            }
            if (placement.failure() != null && !placers.get(column).stopped()) {
                stop(column, placement.failure());
            }
            q.pollFirst();
        }
        return placed;
    }

    private void place(int column, TrackedPage tracked) {
        PageTask task = tracked.task();
        try {
            if (task instanceof NestedPageTask n) {
                placers.get(column).place(n, n.measure(), columns.schema(column).maxRepetitionLevel() > 0);
            }
            else {
                placers.get(column).place(task);
            }
            last[column] = task;
        }
        catch (RuntimeException e) {
            // at the open cell, which is abandoned: the segments placed before it still fill theirs
            BatchCell<?> open = placers.get(column).abandonOpenCell();
            publisher.failAt(open != null ? open.firstRow() : Math.max(tracked.key(), placers.get(column).nextRow()), column,
                    columns.withContext(column, e, task));
        }
        if (task instanceof NestedPageTask n && n.allSegmentsFilled()) {
            n.releaseScratch(scratch.get(column));
        }
    }

    /// Ends every column's placement at the read's end, once each has placed every page it was
    /// given, in column order; a column short of the plan fails at its open cell, or past
    /// `endKey`, the end of the read, where it has none.
    void end(long endKey) {
        for (int c = 0; c < columns.count(); c++) {
            if (!placers.get(c).stopped() && !placements.get(c).isEmpty()) {
                return;
            }
        }
        for (int c = 0; c < columns.count(); c++) {
            if (!placers.get(c).stopped()) {
                try {
                    placers.get(c).end();
                }
                catch (RuntimeException e) {
                    BatchCell<?> abandoned = placers.get(c).abandonOpenCell();
                    publisher.failAt(abandoned != null ? abandoned.firstRow() : endKey, c,
                            last[c] == null ? e : columns.withContext(c, e, last[c]));
                }
            }
        }
    }

    /// Seals the open cell of every column that has placed every record up to `failure`'s row,
    /// `failedAt`, and none past it: for a failure between two units, the batch open across them
    /// is published. A column short of the plan, where every unit before the failure is batched
    /// (`batchedToIt`), abandons its open cell, which moves the failure to it.

    void sealAt(Throwable failure, long failedAt, boolean batchedToIt) {
        for (int c = 0; c < columns.count(); c++) {
            long next = placers.get(c).nextRow();
            // a column that placed all it was given up to the failure and stops short of it holds
            // fewer rows than the plan
            if (!placers.get(c).stopped() && (next == failedAt || next < failedAt && batchedToIt && placements.get(c).isEmpty())) {
                stop(c, failure);
            }
        }
    }

    /// Stops `column`'s placement at `failure`: its open cell is sealed where the plan lets it
    /// close there, else abandoned, which moves the failure to it.
    private void stop(int column, Throwable failure) {
        BatchCell<?> abandoned = placers.get(column).sealOpenCell(failure, columns.schema(column).fieldPath().toString());
        if (abandoned != null) {
            publisher.failAt(abandoned.firstRow(), column, failure);
        }
    }

    /// Decodes a placed flat page whose bytes are `page` and fills its segments, on a job; returns
    /// the cells whose last fill it was, completed.
    List<Cell> decode(int column, PageTask task, ByteBuffer page, Dictionary dictionary, DecodeBuffers buffers)
            throws Exception {
        PageDecoder.LevelScratch levels = FlatFill.scratch(buffers);
        try {
            boolean binary = BinaryBatchValues.holds(columns.schema(column).type());
            PageInfo info = task.info();
            // a page its inline statistics drop decodes as nulls, without bytes
            PageDecoder.InPlaceNestedPage nulls = info.isNullPlaceholder()
                    ? task.decoder().nullsInPlace(info.placeholderNumValues(), levels, false) : null;
            DataPage dataPage = nulls == null ? DataPage.parse(page) : null;
            FlatPageValues fixed = binary ? null : nulls != null ? nulls.flat()
                    : task.decoder().decodeFlat(dataPage, dictionary, levels, buffers);
            BinaryPageValues bytes = !binary ? null : nulls != null ? nulls.binary()
                    : task.decoder().decodeFlatBinary(dataPage, dictionary, levels, buffers);
            checkRows(task.info(), binary ? bytes.size() : fixed.size());
            int maxDefinitionLevel = columns.schema(column).maxDefinitionLevel();
            List<Cell> completed = new ArrayList<>(1);
            for (Segment segment : task.segments()) {
                Cell cell = segment.cell();
                BatchExchange.Batch batch = cell.allocate(exchange(column), columns.schema(column));
                if (binary) {
                    BinaryFill.fill(segment, bytes, (BinaryBatchValues) batch.values, maxDefinitionLevel);
                }
                else {
                    FlatFill.fill(segment, fixed, batch.values, maxDefinitionLevel);
                }
                FlatFill.copyDefinitionLevels(segment, binary ? bytes.definitionLevels() : fixed.definitionLevels(),
                        batch.definitionLevels, maxDefinitionLevel);
                if (cell.pending().decrementAndGet() == 0) {
                    complete(column, cell);
                    completed.add(cell);
                }
            }
            return completed;
        }
        catch (RuntimeException e) {
            throw columns.withContext(column, e, task);
        }
        finally {
            levels.trim();
        }
    }

    /// Checks that a decoded page holds the rows its placement took from its metadata: every row
    /// of an unmasked page, and at least the rows up to its mask's last for a masked one.
    ///
    /// @throws ParquetReadException if it does not
    private static void checkRows(PageInfo info, int values) {
        PageRowMask mask = info.mask();
        if (mask.isAll() ? values != info.records() : values < mask.end(mask.intervalCount() - 1)) {
            throw new ParquetReadException("Page holds " + values + " values, but its metadata gives it "
                    + (mask.isAll() ? info.records() : "at least " + mask.end(mask.intervalCount() - 1)) + " rows");
        }
    }

    /// Writes the batch's matches for the column's filter fragment `filter`: all ones when
    /// statistics proved every record matches, else the matcher's verdict, while the values are in
    /// this job's cache. A matcher keeps scratch between calls, and cells complete on several jobs
    /// at once, so its calls are serialized.
    private static void testFilter(ColumnBatchMatcher filter, BatchExchange.Batch batch, int records) {
        if (batch.filterAlwaysMatches) {
            BitmapWords.fillAllOnes(batch.matches, records);
            return;
        }
        synchronized (filter) {
            filter.test(batch, batch.matches);
        }
    }

    @SuppressWarnings("unchecked")
    private BatchExchange<BatchExchange.Batch> exchange(int column) {
        return (BatchExchange<BatchExchange.Batch>) exchanges.get(column);
    }

    /// Completes a flat cell's batch once its last fill and its seal are both done.
    private void complete(int column, Cell cell) {
        BatchExchange.Batch batch = cell.allocate(exchange(column), columns.schema(column));
        int records = cell.records();
        batch.recordCount = records;
        FlatFill.backfillValidity(cell);
        long[] validity = cell.validity();
        int words = (records + 63) >>> 6;
        batch.validity = validity == null || validity.length == words ? validity : Arrays.copyOf(validity, words);
        batch.fileName = cell.fileName();
        batch.filterAlwaysMatches = cell.alwaysMatches();
        ColumnBatchMatcher filter = filters != null && column < filters.length ? filters[column] : null;
        if (batch.values instanceof BinaryBatchValues values) {
            BinaryFill.finish(cell, values, views[column]);
            if (views[column].requested || filter != null && filter.readsEveryValueView()) {
                values.ensureViews();
            }
        }
        if (filter != null) {
            testFilter(filter, batch, records);
        }
    }

    /// Fills a sealed cell of a column decoded through levels, on a job keyed at its batch.
    @SuppressWarnings("unchecked")
    private void fill(int column, NestedCell cell) {
        long key = publisher.of(column, cell).key();
        scheduler.add(new JobScheduler.Job<Void>(key, 0, JobScheduler.Kind.CPU, null, buffers -> {
            cell.fill((BatchExchange<NestedBatch>) exchanges.get(column), columns.schema(column), columns.layers(column),
                    indexMode, views[column], buffers, scratch.get(column));
            return null;
        }, filled -> publisher.completed(column, cell), t -> {
            PageTask page = t instanceof SegmentFillException s ? s.page() : cell.lastPage();
            Throwable failure = t instanceof SegmentFillException s ? s.failure() : t;
            publisher.failAt(key, column, page != null && failure instanceof Exception e ? columns.withContext(column, e, page) : failure);
        }));
    }

    /// What the placer's decisions do to one flat column; under the read's lock.
    private final class Sink implements ColumnPlacer.Sink<Cell> {

        private final int column;

        Sink(int column) {
            this.column = column;
        }

        @Override
        public Cell open(String fileName, boolean alwaysMatches, int plannedRecords, int capacity, long firstRow) {
            Cell cell = new Cell(fileName, alwaysMatches, plannedRecords, capacity, firstRow);
            publisher.open(column, cell);
            return cell;
        }

        @Override
        public void add(Cell cell, PageTask page, int recordStart, int recordEnd, int valueStart, int valueEnd) {
            page.segments().add(cell.add(page, recordStart, recordEnd));
        }

        @Override
        public void sealed(Cell cell) {
            if (cell.pending().decrementAndGet() == 0) {
                complete(column, cell);
                publisher.completed(column, cell);
            }
        }

    }

    /// What the placer's decisions do to one column decoded through levels; under the read's lock.
    private final class NestedSink implements ColumnPlacer.Sink<NestedCell> {

        private final int column;

        NestedSink(int column) {
            this.column = column;
        }

        @Override
        public NestedCell open(String fileName, boolean alwaysMatches, int plannedRecords, int capacity,
                               long firstRow) {
            NestedCell cell = new NestedCell(fileName, alwaysMatches, firstRow);
            publisher.open(column, cell);
            return cell;
        }

        @Override
        public void add(NestedCell cell, PageTask page, int recordStart, int recordEnd, int valueStart,
                        int valueEnd) {
            WorkingSet.Column workingSet = columns.workingSet(column);
            cell.add((NestedPageTask) page, recordStart, recordEnd, valueStart, valueEnd, keptPage(workingSet, page.info()));
            // before the cell's fill allocates its batch: the column keeps every page it has values of
            workingSet.openBatch(cell.keptBytes(), cell.pages());
        }

        /// What the column keeps of the page `info` describes until the cells it is placed into fill.
        private static long keptPage(WorkingSet.Column workingSet, PageInfo info) {
            if (info.isNullPlaceholder()) {
                return workingSet.keptPage(info.placeholderNumValues(), 0, Encoding.PLAIN);
            }
            DataPage page = info.dataPage();
            return workingSet.keptPage(page.numValues(), page.header().uncompressedPageSize(), page.encoding());
        }

        @Override
        public void sealed(NestedCell cell) {
            fill(column, cell);
        }

    }
}
