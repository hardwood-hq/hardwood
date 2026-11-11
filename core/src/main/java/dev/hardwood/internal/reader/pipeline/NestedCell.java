/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.batch.BinaryBatchValues;
import dev.hardwood.internal.reader.batch.NestedBatch;
import dev.hardwood.internal.reader.batch.NestedIndexMode;
import dev.hardwood.internal.reader.batch.NestedLevelComputer;
import dev.hardwood.internal.reader.decode.PageDecoder;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

/// The work of one column decoded through levels for one planned batch: the [NestedSegment]s of the
/// measured pages placed into it, in page order.
///
/// A cell goes through these steps, each once:
///
/// 1. **Opened** by the [ColumnPlacer] at its batch's first record.
/// 2. **Segments placed**, in page order, each from a page already measured.
/// 3. **Sealed** when the batch holds every record it will hold: for a column with repetition
///    once the next record start, the end of the row group or the end of the read shows its last
///    record is complete.
/// 4. **Filled**, once it is sealed, by a thread that takes it off the column's sealed cells,
///    which allocates its arrays at their final size from its segments' measures, copies every
///    segment from its page ([NestedFill]) and drops the segments.
/// 5. **Completed** by that thread, and published in plan order.
///
/// The segments and totals are guarded by the column lock until the cell is sealed; the thread
/// that takes it off the sealed cells then owns it until it is completed.
public final class NestedCell implements BatchCell<NestedBatch> {

    final String fileName;
    final boolean alwaysMatches;
    private final long firstRow;

    private final List<NestedSegment> segments = new ArrayList<>(2);
    private int records;
    private int values;
    /// What the column keeps of the pages the cell has values of, and how many they are.
    private long keptBytes;
    private int pages;
    /// The page of the last segment placed, which a failure while filling the cell is placed with.
    private PageTask lastPage;

    private NestedBatch batch;
    /// Guarded by the column lock: whether the cell is completed and waits to be published.
    private boolean completed;

    public NestedCell(String fileName, boolean alwaysMatches, long firstRow) {
        this.fileName = fileName;
        this.alwaysMatches = alwaysMatches;
        this.firstRow = firstRow;
    }

    /// Places the page's records `[recordStart, recordEnd)`, holding its values `[valueStart,
    /// valueEnd)`, after every segment placed so far; the page counts the segment until it is
    /// filled.
    ///
    /// @param keptBytes what the column keeps of the page until the cell fills, counted once per page
    /// @throws ParquetReadException if the cell's values would exceed an array's reach
    public void add(NestedPageTask page, int recordStart, int recordEnd, int valueStart, int valueEnd,
                    long keptBytes) {
        int cellValues;
        try {
            cellValues = Math.addExact(values, valueEnd - valueStart);
        }
        catch (ArithmeticException e) {
            throw new ParquetReadException("A batch holds more than " + Integer.MAX_VALUE
                    + " values of the column, more than an array can hold", e);
        }
        if (page != lastPage) {
            this.keptBytes += keptBytes;
            pages++;
        }
        page.unfilledSegments.incrementAndGet();
        segments.add(new NestedSegment(page, recordStart, recordEnd, valueStart, valueEnd, records, values));
        records += recordEnd - recordStart;
        values = cellValues;
        lastPage = page;
    }

    /// What the column keeps of the pages the cell has values of, the sum of their `keptBytes`.
    public long keptBytes() {
        return keptBytes;
    }

    /// The pages the cell has values of.
    public int pages() {
        return pages;
    }

    List<NestedSegment> segments() {
        return segments;
    }

    int records() {
        return records;
    }

    int values() {
        return values;
    }

    public PageTask lastPage() {
        return lastPage;
    }

    /// Drops the segments, and with them the pages, once the cell is filled.
    void dropSegments() {
        segments.clear();
        lastPage = null;
    }

    /// Sets the batch the cell's fill produced.
    /// Fills the sealed cell ([NestedFill]) and drops its segments, returning the scratch of each
    /// page whose cells are then all filled to `pool`.
    public void fill(BatchExchange<NestedBatch> exchange, ColumnSchema column, NestedLevelComputer.Layers layers,
                     NestedIndexMode indexMode, BinaryBatchValues.ViewDemand viewDemand, DecodeBuffers buffers,
                     ArrayBlockingQueue<PageDecoder.LevelScratch> pool) {
        filled(NestedFill.fill(this, exchange, column, layers, indexMode, viewDemand, buffers));
        for (NestedSegment segment : segments()) {
            if (segment.page.unfilledSegments.decrementAndGet() == 0) {
                segment.page.releaseScratch(pool);
            }
        }
        // The cell holds what it was filled from no longer: a cell waiting for room in the
        // exchange would otherwise keep its pages' arrays alive
        dropSegments();
    }

    void filled(NestedBatch batch) {
        this.batch = batch;
    }

    @Override
    public long firstRow() {
        return firstRow;
    }

    @Override
    public boolean isCompleted() {
        return completed;
    }

    @Override
    public void markCompleted() {
        completed = true;
    }

    @Override
    public NestedBatch batch() {
        return batch;
    }

    @Override
    public void release() {
        segments.clear();
        lastPage = null;
        batch = null;
    }
}
