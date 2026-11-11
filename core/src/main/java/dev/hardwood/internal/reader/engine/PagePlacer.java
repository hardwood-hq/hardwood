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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.Supplier;

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.internal.predicate.ColumnBatchMatcher;
import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.batch.BinaryBatchValues;
import dev.hardwood.internal.reader.batch.BitmapWords;
import dev.hardwood.internal.reader.batch.Dictionary;
import dev.hardwood.internal.reader.batch.NestedBatch;
import dev.hardwood.internal.reader.batch.NestedIndexMode;
import dev.hardwood.internal.reader.batch.NestedLevelComputer;
import dev.hardwood.internal.reader.decode.BinaryPageValues;
import dev.hardwood.internal.reader.decode.DataPage;
import dev.hardwood.internal.reader.decode.FlatPageValues;
import dev.hardwood.internal.reader.decode.PageDecoder;
import dev.hardwood.internal.reader.decode.PageInfo;
import dev.hardwood.internal.reader.memory.WorkingSet;
import dev.hardwood.internal.reader.pipeline.BatchCell;
import dev.hardwood.internal.reader.pipeline.BinaryFill;
import dev.hardwood.internal.reader.pipeline.Cell;
import dev.hardwood.internal.reader.pipeline.FlatFill;
import dev.hardwood.internal.reader.pipeline.NestedCell;
import dev.hardwood.internal.reader.pipeline.NestedPageTask;
import dev.hardwood.internal.reader.pipeline.PageTask;
import dev.hardwood.internal.reader.pipeline.Placer;
import dev.hardwood.internal.reader.pipeline.Segment;
import dev.hardwood.internal.reader.pipeline.SegmentFillException;
import dev.hardwood.internal.reader.plan.BatchPlan;
import dev.hardwood.internal.reader.select.PageRowMask;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

/// Places each column's pages into the cells of a read's batch plan, through Hardwood's
/// [Placer], in page order: a column's chunks unit after unit, a chunk's pages as they are known
/// (planned at install, or found by a scan), each once it can be. A flat page can be placed at
/// once; its job then decodes it and fills its segments, and its cell completes at its last fill
/// or its seal, whichever comes last. A page decoded through levels can be placed once its job has
/// measured it; a cell it is placed into is filled on a job once sealed.
///
/// At the read's end every column's last cell is sealed. At a failure between two units, every
/// column's open cell is sealed, or abandoned where the column is short of the plan; at a failure
/// inside a unit the open cell is left, as the batch holding the failure's row is never published.
/// Used under the read's lock, except [#decode] and [#nestedTask].
final class PagePlacer {

    /// Scratch a column decoded through levels keeps for its next pages.
    private static final int SCRATCH_POOL = 4;

    /// A column's pages of one unit, in page order, and how many are placed.
    static final class Chunk {
        final Unit.Installed unit;
        final List<PageFetcher.PageWork> pages = new ArrayList<>();
        PageDecoder decoder;
        int placed;
        boolean ended;
        /// the scan's failure, past its pages: the column places nothing after them
        Throwable failure;

        Chunk(Unit.Installed unit) {
            this.unit = unit;
        }
    }

    private final JobScheduler scheduler;
    private final BatchQueue queue;
    private final ColumnSchema[] schemas;
    private final List<BatchExchange<?>> exchanges;
    /// per column, its layers where it is decoded through levels, `null` for a flat one
    private final List<NestedLevelComputer.Layers> layers;
    private final NestedIndexMode indexMode;
    private final boolean fixedListFastPathEnabled;
    private final DecompressorFactory decompressors;
    private final boolean footersSupplied;
    /// the read's working set, which every decode and dictionary charges before it allocates
    private final WorkingSet workingSet;
    private final List<Placer<? extends BatchCell<?>>> placers = new ArrayList<>();
    private final List<ArrayDeque<Chunk>> chunks = new ArrayList<>();
    private final List<ArrayBlockingQueue<PageDecoder.LevelScratch>> scratch = new ArrayList<>();
    private final BinaryBatchValues.ViewDemand[] views;
    /// per flat column, its filter fragment, which writes each batch's matches as it completes;
    /// `null` for none
    private final ColumnBatchMatcher[] filters;
    /// per column, the last page it placed, which a shortfall at the read's end is placed with
    private final PageTask[] last;

    PagePlacer(JobScheduler scheduler, BatchQueue queue, ColumnSchema[] schemas, List<BatchExchange<?>> exchanges,
               List<NestedLevelComputer.Layers> layers, NestedIndexMode indexMode, boolean fixedListFastPathEnabled,
               int batchCapacity, DecompressorFactory decompressors, boolean footersSupplied, WorkingSet workingSet,
               ColumnBatchMatcher[] filters) {
        this.scheduler = scheduler;
        this.filters = filters;
        this.queue = queue;
        this.schemas = schemas;
        this.exchanges = exchanges;
        this.layers = layers;
        this.indexMode = indexMode;
        this.fixedListFastPathEnabled = fixedListFastPathEnabled;
        this.decompressors = decompressors;
        this.footersSupplied = footersSupplied;
        this.workingSet = workingSet;
        this.views = new BinaryBatchValues.ViewDemand[schemas.length];
        this.last = new PageTask[schemas.length];
        for (int c = 0; c < schemas.length; c++) {
            placers.add(nested(c) ? new Placer<>(new NestedSink(c), batchCapacity)
                    : new Placer<>(new Sink(c), batchCapacity));
            chunks.add(new ArrayDeque<>());
            scratch.add(new ArrayBlockingQueue<>(SCRATCH_POOL));
            views[c] = new BinaryBatchValues.ViewDemand();
        }
    }

    /// The working set's handle for `column`.
    WorkingSet.Column workingSet(int column) {
        return workingSet.column(column, schemas[column]);
    }

    boolean nested(int column) {
        return layers.get(column) != null;
    }

    ColumnSchema schema(int column) {
        return schemas[column];
    }

    /// The column's chunk of `u`, behind the chunks of the units before it.
    Chunk chunk(int column, Unit.Installed u) {
        Chunk chunk = new Chunk(u);
        chunks.get(column).addLast(chunk);
        return chunk;
    }

    /// Runs `charge`, which charges a dictionary of `column`'s chunk of `u`, in the window of the
    /// batches `u` places: one column's dictionaries one at a time, so that each counts beside
    /// those of its own batch alone, in whichever order they decode; on any thread.
    <T> T inWindow(int column, Unit.Installed u, Supplier<T> charge) {
        BatchPlan.RowGroupBatches batches = u.batches();
        synchronized (views[column]) {
            workingSet.enter(column, batches.readRowsBefore() - batches.continuedRows(),
                    batches.readRowsBefore() + batches.plannedRows());
            return charge.get();
        }
    }

    /// Adds `pw`, a data page of `column`'s `chunk`, behind the chunk's pages; a flat page can be
    /// placed at once.
    void add(int column, Chunk chunk, PageFetcher.PageWork pw) {
        ColumnMetaData meta = chunk.unit.listing().chunks()[column].metaData();
        if (chunk.decoder == null) {
            chunk.decoder = new PageDecoder(meta, schemas[column], decompressors,
                    !nested(column) || fixedListFastPathEnabled, workingSet(column));
        }
        pw.decoder = chunk.decoder;
        if (!nested(column)) {
            pw.task = new PageTask(info(column, pw, null, null), chunk.unit.batches().fileName(), chunk.unit.listing().rowGroup(),
                    pw.page.pageIndex(), chunk.unit.batches(), pw.decoder,
                    chunk.unit.batches().readRowsBefore() + pw.page.location().firstRowIndex());
            pw.ready = true;
        }
        chunk.pages.add(pw);
    }

    private PageInfo info(int column, PageFetcher.PageWork pw, DataPage page, Dictionary dictionary) {
        if (pw.placeholder > 0) {
            return PageInfo.nullPlaceholder(pw.placeholder, schemas[column], pw.unit.listing().chunks()[column].metaData(),
                    PageInfo.keptRecords(schemas[column], PageRowMask.ALL, pw.placeholder));
        }
        return new PageInfo(page, schemas[column], pw.unit.listing().chunks()[column].metaData(), dictionary,
                pw.page.mask(), PageInfo.keptRecords(schemas[column], pw.page.mask(), pw.page.rows()));
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
        for (Placer<?> p : placers) {
            if (!p.stopped()) {
                return false;
            }
        }
        return true;
    }

    /// The task of a page decoded through levels whose bytes are `page`, on a job.
    NestedPageTask nestedTask(PageFetcher.PageWork pw, DataPage page, Dictionary dictionary) {
        PageDecoder.LevelScratch levels = scratch.get(pw.column).poll();
        Unit.Installed u = pw.unit;
        return new NestedPageTask(info(pw.column, pw, page, dictionary), u.batches().fileName(), u.listing().rowGroup(),
                pw.page.pageIndex(), u.batches(), pw.decoder, u.batches().readRowsBefore() + pw.page.location().firstRowIndex(),
                levels != null ? levels : new PageDecoder.LevelScratch());
    }

    /// Places the column's pages that can be, in order, moving on to its next chunk once one has
    /// ended and is placed; returns the pages it took.
    List<PageFetcher.PageWork> place(int column) {
        List<PageFetcher.PageWork> placed = new ArrayList<>();
        ArrayDeque<Chunk> q = chunks.get(column);
        while (!q.isEmpty()) {
            Chunk chunk = q.peekFirst();
            while (chunk.placed < chunk.pages.size() && chunk.pages.get(chunk.placed).ready) {
                PageFetcher.PageWork pw = chunk.pages.get(chunk.placed++);
                if (!placers.get(column).stopped()) {
                    place(column, pw);
                }
                placed.add(pw);
            }
            if (!chunk.ended || chunk.placed < chunk.pages.size()) {
                break;
            }
            if (chunk.failure != null && !placers.get(column).stopped()) {
                stop(column, chunk.failure);
            }
            q.pollFirst();
        }
        return placed;
    }

    private void place(int column, PageFetcher.PageWork pw) {
        PageTask task = pw.task;
        try {
            if (task instanceof NestedPageTask n) {
                placers.get(column).place(n, n.measure(), schemas[column].maxRepetitionLevel() > 0);
            }
            else {
                placers.get(column).place(task);
            }
            last[column] = task;
        }
        catch (RuntimeException e) {
            // at the open cell, which is abandoned: the segments placed before it still fill theirs
            BatchCell<?> open = placers.get(column).abandonOpenCell();
            queue.failAt(open != null ? open.firstRow() : Math.max(pw.key, placers.get(column).nextRow()), column,
                    placed(column, e, task));
        }
        if (task instanceof NestedPageTask n && n.allSegmentsFilled()) {
            n.releaseScratch(scratch.get(column));
        }
    }

    /// Ends every column's placement at the read's end, once each has placed every page it was
    /// given, in column order; a column short of the plan fails at its open cell, or past
    /// `endKey`, the end of the read, where it has none.
    void end(long endKey) {
        for (int c = 0; c < schemas.length; c++) {
            if (!placers.get(c).stopped() && !chunks.get(c).isEmpty()) {
                return;
            }
        }
        for (int c = 0; c < schemas.length; c++) {
            if (!placers.get(c).stopped()) {
                try {
                    placers.get(c).end();
                }
                catch (RuntimeException e) {
                    BatchCell<?> abandoned = placers.get(c).abandonOpenCell();
                    queue.failAt(abandoned != null ? abandoned.firstRow() : endKey, c,
                            last[c] == null ? e : placed(c, e, last[c]));
                }
            }
        }
    }

    /// Seals the open cell of every column that has placed every record up to `failure`'s row,
    /// `failedAt`, and none past it: for a failure between two units, the batch open across them
    /// is published. A column short of the plan, where every unit before the failure is installed
    /// (`installedToIt`), abandons its open cell, which moves the failure to it.

    void sealAt(Throwable failure, long failedAt, boolean installedToIt) {
        for (int c = 0; c < schemas.length; c++) {
            long next = placers.get(c).nextRow();
            // a column that placed all it was given up to the failure and stops short of it holds
            // fewer rows than the plan
            if (!placers.get(c).stopped() && (next == failedAt || next < failedAt && installedToIt && chunks.get(c).isEmpty())) {
                stop(c, failure);
            }
        }
    }

    /// Stops `column`'s placement at `failure`: its open cell is sealed where the plan lets it
    /// close there, else abandoned, which moves the failure to it.
    private void stop(int column, Throwable failure) {
        BatchCell<?> abandoned = placers.get(column).sealOpenCell(failure, schemas[column].fieldPath().toString());
        if (abandoned != null) {
            queue.failAt(abandoned.firstRow(), column, failure);
        }
    }

    /// Decodes a placed flat page whose bytes are `page` and fills its segments, on a job; returns
    /// the cells whose last fill it was, completed.
    List<Cell> decode(int column, PageTask task, ByteBuffer page, Dictionary dictionary, DecodeBuffers buffers)
            throws Exception {
        PageDecoder.LevelScratch levels = FlatFill.scratch(buffers);
        try {
            boolean binary = BinaryBatchValues.holds(schemas[column].type());
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
            int maxDefinitionLevel = schemas[column].maxDefinitionLevel();
            List<Cell> completed = new ArrayList<>(1);
            for (Segment segment : task.segments()) {
                Cell cell = segment.cell();
                BatchExchange.Batch batch = cell.allocate(exchange(column), schemas[column]);
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
            throw placed(column, e, task);
        }
        finally {
            levels.trim();
        }
    }

    /// `e`, placed at the column and the file, row group and page of `page`.
    Exception placed(int column, Exception e, PageTask page) {
        return placed(column, e, page.fileName(), page.rowGroup(), page.pageIndex());
    }

    /// `e` met reading `column`, typed as a read failure and placed with the file, row group and
    /// page it came from. Where a [dev.hardwood.MetadataSource] supplied the footers, a read failure
    /// also says so ([ExceptionContext#addSuppliedFooterHint]): the checks a supplied footer passes
    /// on open do not catch every file rewritten since the footer was read.
    Exception placed(int column, Exception e, String fileName, int rowGroup, int page) {
        if (HardwoodContextImpl.isContextClosed(e)) {
            // the context's, not the file's, whichever thread of the read met the refusal first
            return e;
        }
        Exception typed = e instanceof RuntimeException runtime ? ExceptionContext.asReadFailure(runtime) : e;
        if (fileName == null || fileName.isEmpty()) {
            return typed;
        }
        String columnPath = schemas[column].fieldPath().toString();
        if (typed instanceof RuntimeException re) {
            RuntimeException placed = ExceptionContext.addReadContext(fileName, rowGroup, columnPath, page, re);
            return footersSupplied ? ExceptionContext.addSuppliedFooterHint(placed) : placed;
        }
        if (typed instanceof IOException ioe) {
            // stays checked: the read carries a failure across threads as a Throwable
            return ExceptionContext.addReadContext(fileName, rowGroup, columnPath, page, ioe);
        }
        return typed;
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
        BatchExchange.Batch batch = cell.allocate(exchange(column), schemas[column]);
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
        long key = queue.of(column, cell).key;
        scheduler.add(new JobScheduler.Job(key, 0, JobScheduler.Kind.CPU, null, buffers -> {
            cell.fill((BatchExchange<NestedBatch>) exchanges.get(column), schemas[column], layers.get(column),
                    indexMode, views[column], buffers, scratch.get(column));
            return () -> queue.completed(column, cell);
        }, t -> () -> {
            PageTask page = t instanceof SegmentFillException s ? s.page() : cell.lastPage();
            Throwable failure = t instanceof SegmentFillException s ? s.failure() : t;
            queue.failAt(key, column, page != null && failure instanceof Exception e ? placed(column, e, page) : failure);
        }));
    }

    /// What the placer's decisions do to one flat column; under the read's lock.
    private final class Sink implements Placer.Sink<Cell> {

        private final int column;

        Sink(int column) {
            this.column = column;
        }

        @Override
        public Cell open(String fileName, boolean alwaysMatches, int plannedRecords, int capacity, long firstRow) {
            Cell cell = new Cell(fileName, alwaysMatches, plannedRecords, capacity, firstRow);
            queue.open(column, cell);
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
                queue.completed(column, cell);
            }
        }

    }

    /// What the placer's decisions do to one column decoded through levels; under the read's lock.
    private final class NestedSink implements Placer.Sink<NestedCell> {

        private final int column;

        NestedSink(int column) {
            this.column = column;
        }

        @Override
        public NestedCell open(String fileName, boolean alwaysMatches, int plannedRecords, int capacity,
                               long firstRow) {
            NestedCell cell = new NestedCell(fileName, alwaysMatches, firstRow);
            queue.open(column, cell);
            return cell;
        }

        @Override
        public void add(NestedCell cell, PageTask page, int recordStart, int recordEnd, int valueStart,
                        int valueEnd) {
            cell.add((NestedPageTask) page, recordStart, recordEnd, valueStart, valueEnd);
        }

        @Override
        public void sealed(NestedCell cell) {
            fill(column, cell);
        }

    }
}
