/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import dev.hardwood.schema.ColumnSchema;

/// The work of one column for one planned batch: the batch it fills and the [Segment]s placed
/// into it in page order.
///
/// A cell goes through these steps, each once:
///
/// 1. **Opened** by the [Placer] at its batch's first record.
/// 2. **Segments placed**, in page order, by the column's fetcher. Each segment's offset is the
///    cell's record count before it.
/// 3. **Sealed** when the batch holds every record the plan puts in it.
/// 4. **Allocated** by the first page task that reaches it ([#allocate]): its arrays are created
///    at the size the plan gives, once.
/// 5. **Filled**: each segment by its page's task, concurrently.
/// 6. **Completed** by the task that fills its last segment, or by the seal when that comes last,
///    and published in plan order.
///
/// [#pending] decides which completes the cell: it starts at one, the seal's token, gains one per
/// segment placed and loses one per segment filled and one at the seal, so it reaches zero
/// exactly once, after the last fill and the seal both happened. Fills write disjoint ranges of
/// the cell's values without a lock; the validity words at the edges of a segment's bits are
/// merged atomically ([FlatFill]). A binary column's segments copy their plain bytes into chunks
/// of their own ([BinaryFill]), so no fill waits for another.
final class Cell implements ScheduledCell<BatchExchange.Batch> {

    /// [#plannedRecords] of a cell whose batch the plan leaves open past the row group it opens in.
    static final int UNKNOWN_RECORDS = -1;

    private static final VarHandle VALIDITY;

    static {
        try {
            VALIDITY = MethodHandles.lookup().findVarHandle(Cell.class, "validity", long[].class);
        }
        catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /// The file the batch is attributed to, from the batch plan.
    final String fileName;
    /// Whether statistics proved every row of the batch matches the filter, from the batch plan.
    final boolean alwaysMatches;
    /// The records the plan puts in the batch when the plan closes it in the row group it opens
    /// in, [#UNKNOWN_RECORDS] otherwise.
    final int plannedRecords;
    /// The records the batch's arrays take: [#plannedRecords], or the batch capacity when the plan
    /// leaves the batch open.
    final int capacity;
    /// The read's row of the batch's first record.
    final long firstRow;

    /// The batch, created by [#allocate].
    private BatchExchange.Batch batch;
    /// Set once [#batch] holds its arrays.
    private volatile boolean allocated;
    private final ReentrantLock allocation = new ReentrantLock();

    /// Guarded by the column lock: the segments placed, in page order, and their records.
    private final List<Segment> segments = new ArrayList<>();
    private int records;
    /// The page of the last segment placed, which a failure while completing the cell is placed
    /// with.
    private PageTask lastPage;
    /// Guarded by the column lock: whether the cell is completed and waits to be published.
    private boolean completed;

    /// Which task completes the cell; see the class description.
    final AtomicInteger pending;

    /// The cell's validity bits, once a segment met a null. Installed once, by compare-and-set.
    @SuppressWarnings("unused")
    private volatile long[] validity;

    Cell(String fileName, boolean alwaysMatches, int plannedRecords, int capacity, long firstRow) {
        this.fileName = fileName;
        this.alwaysMatches = alwaysMatches;
        this.plannedRecords = plannedRecords;
        this.capacity = capacity;
        this.firstRow = firstRow;
        this.pending = new AtomicInteger(1);
    }

    /// Places the page's kept records `[recordStart, recordEnd)` into this cell after every
    /// segment placed so far.
    ///
    /// @throws IllegalStateException if the batch cannot take them
    Segment add(PageTask page, int recordStart, int recordEnd) {
        int count = recordEnd - recordStart;
        if (count > capacity - records) {
            throw new IllegalStateException("Batch plan mismatch: a batch of " + capacity
                    + " rows cannot take " + count + " more after " + records);
        }
        Segment segment = new Segment(this, page, recordStart, recordEnd, records);
        records += count;
        segments.add(segment);
        lastPage = page;
        pending.incrementAndGet();
        return segment;
    }

    /// Takes the cell's batch from `exchange` and gives it a value array of at least [#capacity]
    /// values, unless a task already did: a returned holder's array when it is large enough, a new
    /// one otherwise. Runs on the first task that reaches the cell; another that reaches it
    /// meanwhile parks on the cell's lock until the array exists, rather than spinning while it is
    /// zeroed, and the lock orders the array's creation before its writes into it.
    BatchExchange.Batch allocate(BatchExchange<BatchExchange.Batch> exchange, ColumnSchema column) {
        if (!allocated) {
            allocation.lock();
            try {
                if (!allocated) {
                    BatchExchange.Batch taken = exchange.newBatch();
                    if (taken.values == null || taken.capacity < capacity) {
                        taken.values = BatchExchange.allocateArray(column, capacity);
                        taken.capacity = capacity;
                    }
                    if (taken.values instanceof BinaryBatchValues binary) {
                        binary.prepareForFill();
                    }
                    batch = taken;
                    allocated = true;
                }
            }
            finally {
                allocation.unlock();
            }
        }
        return batch;
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

    /// The batch, once [#allocate] ran.
    @Override
    public BatchExchange.Batch batch() {
        return batch;
    }

    @Override
    public void release() {
        segments.clear();
        lastPage = null;
        batch = null;
        validity = null;
    }

    /// The segments placed, in page order.
    List<Segment> segments() {
        return segments;
    }

    int records() {
        return records;
    }

    PageTask lastPage() {
        return lastPage;
    }

    /// The cell's validity bits, `null` while no segment met a null.
    long[] validity() {
        return (long[]) VALIDITY.getVolatile(this);
    }

    /// The cell's validity bits, installing cleared words for [#capacity] records if no segment
    /// has yet. Two segments meeting their first null at once install one array between them.
    long[] installValidity() {
        long[] current = validity();
        if (current != null) {
            return current;
        }
        long[] created = new long[(capacity + 63) >>> 6];
        long[] witness = (long[]) VALIDITY.compareAndExchange(this, (long[]) null, created);
        return witness != null ? witness : created;
    }
}
