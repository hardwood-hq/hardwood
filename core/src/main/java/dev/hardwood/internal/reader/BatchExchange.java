/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import dev.hardwood.jfr.BatchWaitEvent;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.ColumnSchema;

/// Exchange buffer between a [ScheduledColumn]'s page tasks and the consumer. The column publishes
/// without waiting ([#tryPublish]); a batch that finds no room waits in the column until the
/// consumer's next take ([#setRoomListener]).
///
/// Two modes are available:
///
/// - **Recycling** (`BatchExchange.recycling()`): batch holders cycle between producer and
///   consumer via `freeQueue` and `readyQueue`; a cell takes a free holder when there is one and a
///   new one from the factory otherwise ([#newBatch]). Used by [FlatRowReader] and
///   [NestedRowReader] where the consumer returns batches after reading.
///
/// - **Detaching** (`BatchExchange.detaching()`): allocates a fresh batch each time
///   the producer needs one via the `batchFactory`. The consumer keeps ownership of each
///   batch (the arrays are not recycled). Back-pressure comes from a limit on the rows
///   the ready queue holds. Used by [ColumnReader] where the caller retains the arrays
///   between batches.
///
/// The detaching limit counts rows rather than batches because a batch can be cut short, at a
/// row group or a file, and a read over small row groups publishes many short batches: counted in
/// rows, the queue holds what two full batches hold however the batches are cut. A batch counts
/// the rows its arrays were allocated for, not the rows it holds, so that the limit bounds memory.
///
/// @param <B> the batch type (e.g. [Batch] for flat, [NestedBatch] for nested)
public class BatchExchange<B> {

    static final int READY_QUEUE_CAPACITY = 2;

    /// Per-value byte budget for variable-length `BYTE_ARRAY` / `INT96`
    /// buffers allocated by [#allocateArray]. The bytes buffer is sized to
    /// `BINARY_BYTES_PER_VALUE_HINT * capacity` when the first value is appended
    /// and grown on overflow; a batch whose values are all views into a
    /// dictionary never allocates it at that size.
    public static final int BINARY_BYTES_PER_VALUE_HINT = 32;

    /// A mutable batch holder for flat columns, reused in recycling mode. The column's pipeline
    /// writes into it, the consumer reads from it.
    ///
    /// `validity` carries set-bit-= -present semantics: a set bit at position
    /// `i` means the leaf value at that position is present. Packed into
    /// `long[]` of length `(recordCount + 63) >>> 6` (each word covers 64
    /// consecutive rows, low bit = lowest row). `null` is the sparse
    /// representation of "every leaf in this batch is present."
    ///
    /// `values` is a typed primitive array (`int[]`, `long[]`, …) for
    /// fixed-width physical types and a [BinaryBatchValues] for byte-array
    /// types.
    public static final class Batch {
        public Object values;
        public long[] validity;
        public int recordCount;
        /// The rows [#values] was allocated for, at least [#recordCount]. A batch ended early,
        /// at a file or by a row cap, can hold far fewer rows than its arrays have room for, and
        /// a detaching exchange limits its ready queue by this so that the limit bounds memory.
        public int capacity;
        public String fileName;
        /// Per-batch matches mask, populated by [dev.hardwood.internal.predicate.ColumnBatchMatcher]
        /// on the task that completes the batch when drain-side filtering is enabled. `null` means "no
        /// fragment for this column" — [BatchMatchMerger] never reads those columns, since
        /// it visits exactly the ones its plan references. When non-null, sized to
        /// `(batchCapacity + 63) >>> 6` and overwritten on every drain-side test call.
        public long[] matches;

        /// Whether statistics proved every record of this batch matches the filter
        /// predicate, making [#matches] all-ones without per-record evaluation.
        /// Only set on the drain-side filter path; `false` is always safe.
        public boolean filterAlwaysMatches;
    }

    /// Handed to the consumer by [#finish()] so that a poll already waiting on the queue learns
    /// of the end of the stream from the queue itself, rather than from the next expiry of its
    /// timed poll. See [#finish()] for why offering it can be left to succeed or not.
    private static final Object END_OF_STREAM = new Object();

    private final BlockingQueue<Object> readyQueue;
    private final ArrayBlockingQueue<B> freeQueue;
    private final Supplier<B> batchFactory;
    private final String columnName;

    /// Detaching mode only: the rows a batch's arrays were allocated for, and the rows the ready
    /// queue may hold. `null` and zero in recycling mode, where the bounded ready queue is the
    /// limit.
    private final ToIntFunction<B> batchRows;
    private final long readyRowLimit;
    private final AtomicLong readyRows = new AtomicLong();

    private final AtomicReference<Throwable> error = new AtomicReference<>();
    private volatile boolean finished;
    /// Run on the consumer's thread after it takes a batch, for a column whose [#tryPublish]
    /// found no room; `null` for none.
    private volatile Runnable roomListener;

    private BatchExchange(String columnName, BlockingQueue<Object> readyQueue,
                          ArrayBlockingQueue<B> freeQueue, Supplier<B> batchFactory,
                          ToIntFunction<B> batchRows, int batchCapacity) {
        this.columnName = columnName;
        this.readyQueue = readyQueue;
        this.freeQueue = freeQueue;
        this.batchFactory = batchFactory;
        this.batchRows = batchRows;
        this.readyRowLimit = (long) READY_QUEUE_CAPACITY * batchCapacity;
    }

    /// Creates a recycling exchange that pre-allocates batch holders.
    ///
    /// Three batches are pre-allocated (`READY_QUEUE_CAPACITY + 1`): one filling
    /// plus up to two queued. The consumer must return consumed batches to `freeQueue()`.
    /// A scheduled column fills holders past those three while it decodes ahead
    /// ([#newBatch]); the free queue keeps at most three of the holders returned.
    ///
    /// @param columnName column name for error messages
    /// @param batchFactory creates the initial batch holders, and those a scheduled column
    ///        needs beyond them
    /// @param <B> the batch type
    /// @return a recycling [BatchExchange]
    public static <B> BatchExchange<B> recycling(String columnName, Supplier<B> batchFactory) {
        ArrayBlockingQueue<Object> readyQueue = new ArrayBlockingQueue<>(READY_QUEUE_CAPACITY);
        ArrayBlockingQueue<B> freeQueue = new ArrayBlockingQueue<>(READY_QUEUE_CAPACITY + 1);

        for (int i = 0; i < READY_QUEUE_CAPACITY + 1; i++) {
            freeQueue.add(batchFactory.get());
        }

        return new BatchExchange<>(columnName, readyQueue, freeQueue, batchFactory, null, 0);
    }

    /// Creates a detaching exchange that allocates a fresh batch each time.
    ///
    /// Every batch comes from `batchFactory.get()`. The consumer owns each batch after reading —
    /// arrays are not recycled. Back-pressure comes from the rows the ready queue holds: a
    /// publish finds no room when its batch would take the queue past `READY_QUEUE_CAPACITY`
    /// full batches' worth.
    ///
    /// @param columnName column name for error messages
    /// @param batchFactory creates a fresh batch on each call
    /// @param batchRows the rows a batch's arrays were allocated for, at most
    ///        `batchCapacity`; a batch counts as at least one row
    /// @param batchCapacity the most rows a batch holds
    /// @param <B> the batch type
    /// @return a detaching [BatchExchange]
    public static <B> BatchExchange<B> detaching(String columnName, Supplier<B> batchFactory,
                                                 ToIntFunction<B> batchRows, int batchCapacity) {
        if (batchCapacity <= 0) {
            throw new IllegalArgumentException("Batch capacity must be positive: " + batchCapacity);
        }
        return new BatchExchange<>(columnName, new LinkedBlockingQueue<>(), null, batchFactory,
                batchRows, batchCapacity);
    }

    /// Creates a detaching exchange for flat batches. A flat batch counts the rows its value
    /// array was allocated for ([Batch#capacity]), since a batch ended early keeps full-size
    /// arrays.
    ///
    /// @param columnName column name for error messages
    /// @param batchCapacity the most rows a batch holds
    /// @return a detaching [BatchExchange] of [Batch]es
    public static BatchExchange<Batch> detachingFlat(String columnName, int batchCapacity) {
        return detaching(columnName, Batch::new, batch -> batch.capacity, batchCapacity);
    }

    /// Creates a detaching exchange for nested batches. A nested batch counts its records, since
    /// its arrays are allocated at the size its records hold.
    ///
    /// @param columnName column name for error messages
    /// @param batchCapacity the most records a batch holds
    /// @return a detaching [BatchExchange] of [NestedBatch]es
    public static BatchExchange<NestedBatch> detachingNested(String columnName, int batchCapacity) {
        return detaching(columnName, NestedBatch::new, batch -> batch.recordCount, batchCapacity);
    }

    /// Returns the column name for error messages.
    public String name() {
        return columnName;
    }

    // ==================== Column Side ====================

    /// Queues the batch without waiting, if the ready queue has room: in detaching mode, if it
    /// keeps the queue within `readyRowLimit` rows, an empty queue taking any batch; in recycling
    /// mode, if the bounded queue has a free slot. For one publisher at a time: the consumer only
    /// ever takes batches out, so the room this checks is still there when it queues.
    ///
    /// @return whether the batch was queued; if not, the [#setRoomListener] listener runs once
    ///         the consumer has taken a batch
    boolean tryPublish(B batch) {
        if (batchRows == null) {
            return readyQueue.offer(batch);
        }
        long rows = rowsOf(batch);
        long queued = readyRows.get();
        if (queued > 0 && queued + rows > readyRowLimit) {
            return false;
        }
        return queue(batch, rows);
    }

    /// A batch for the column to fill and publish ([#tryPublish]): a returned
    /// holder in recycling mode when one is free, a new one otherwise. Its arrays are the
    /// returned holder's, or none.
    B newBatch() {
        B free = freeQueue != null ? freeQueue.poll() : null;
        return free != null ? free : batchFactory.get();
    }

    /// Sets what runs on the consumer's thread each time it takes a batch: for a column whose
    /// [#tryPublish] found no room, and which publishes there instead of waiting.
    void setRoomListener(Runnable listener) {
        this.roomListener = listener;
    }

    private boolean queue(B batch, long rows) {
        readyRows.addAndGet(rows);
        readyQueue.offer(batch);
        return true;
    }

    /// Accounts for a batch the consumer has taken, and runs the room listener. The sentinel and
    /// an empty poll are passed through.
    private Object taken(Object batch) {
        if (batch == null || batch == END_OF_STREAM) {
            return batch;
        }
        if (batchRows != null) {
            @SuppressWarnings("unchecked")
            B taken = (B) batch;
            readyRows.addAndGet(-rowsOf(taken));
        }
        Runnable listener = roomListener;
        if (listener != null) {
            listener.run();
        }
        return batch;
    }

    /// A batch of no rows counts as one, so that the row limit also bounds the batches queued.
    private long rowsOf(B batch) {
        return Math.max(1, batchRows.applyAsInt(batch));
    }

    /// Marks the stream ended, and hands a waiting consumer the sentinel that says so.
    ///
    /// The flag alone is not enough. A consumer inside [#poll()]'s timed loop is waiting on the
    /// queue, not on the flag, and reads the flag only when its 10 ms poll expires.
    ///
    /// The offer is deliberately non-blocking and its result deliberately ignored. A consumer is
    /// only ever left waiting when it found the queue empty, and in that state the offer has room
    /// and succeeds. It fails only when the queue is full, which only the recycling mode's bounded
    /// queue can be, and a consumer with batches still to take is not waiting for anything: by the
    /// time it has taken them, `finished` is set and [#poll()] returns without waiting at all. So the sentinel removes a delay where there is
    /// one, and where it cannot be delivered there was none to remove.
    ///
    /// Nothing rests on it arriving. The timed poll remains the liveness guarantee.
    public void finish() {
        finished = true;
        readyQueue.offer(END_OF_STREAM);
    }

    public boolean isFinished() {
        return finished;
    }

    /// Ends the stream with a failure. Goes through [#finish()] so that a waiting consumer is
    /// released as promptly as a clean end releases it, and raises the error on its way out
    /// rather than after the next poll expires.
    ///
    /// The first error signalled is the one raised, since it is the failure that ended the stream.
    /// Each later one is attached to it as a suppressed exception rather than dropped, so a
    /// failure that follows the first, such as an `OutOfMemoryError` in another decode task,
    /// still reaches the caller.
    public void signalError(Throwable t) {
        if (!error.compareAndSet(null, t)) {
            Throwable first = error.get();
            if (first != t) {
                first.addSuppressed(t);
            }
        }
        finish();
    }

    // ==================== Consumer Side ====================

    /// Polls for the next batch. Returns the batch if available, or `null` if
    /// the pipeline has finished and the queue is drained.
    ///
    /// This method handles the ordering between `readyQueue` and `finished`
    /// correctly: a non-blocking poll is attempted first, and if the queue is
    /// empty the method falls through to a timed poll loop. The `finished` flag
    /// is only checked **after** a timed poll returns null, guaranteeing that any
    /// batch published before `finish()` is visible.
    ///
    /// The end of the stream arrives twice over, and the ordering above is why that is safe. It
    /// arrives as [#END_OF_STREAM] through the queue, which is what releases a poll already
    /// waiting, and it arrives as the `finished` flag, which is what a poll entering later reads
    /// before it waits at all. Both are behind every batch: the sentinel by queue order, the flag
    /// because it is only read once a poll has come back empty.
    @SuppressWarnings("unchecked")
    public B poll() throws InterruptedException, IOException {
        Object batch = taken(readyQueue.poll());
        if (batch == END_OF_STREAM) {
            checkError();
            return null;
        }
        if (batch != null) {
            return (B) batch;
        }
        if (finished) {
            return (B) drop(taken(readyQueue.poll()));
        }
        // Past this point the consumer is stalled on the pipeline: the ready
        // queue is empty and the column has not finished. The event's duration
        // is the stall, so it spans every exit from the timed poll loop.
        BatchWaitEvent event = new BatchWaitEvent();
        event.begin();
        if (DebugTiming.enabled()) {
            DebugTiming.log("poll-wait-begin col=" + columnName);
        }
        try {
            while ((batch = taken(readyQueue.poll(10, TimeUnit.MILLISECONDS))) == null) {
                if (finished) {
                    return (B) drop(taken(readyQueue.poll()));
                }
                checkError();
            }
            if (batch == END_OF_STREAM) {
                checkError();
                return null;
            }
            return (B) batch;
        }
        finally {
            event.column = columnName;
            event.commit();
            if (DebugTiming.enabled()) {
                DebugTiming.log("poll-wait col=" + columnName + " result=" + (batch == END_OF_STREAM ? "END" : batch == null ? "finished" : "batch"));
            }
        }
    }

    /// The sentinel read as nothing, so that a queue holding only it reads as an empty one.
    private static Object drop(Object element) {
        return element == END_OF_STREAM ? null : element;
    }

    /// Returns a consumed batch to the free pool so a later cell can reuse it.
    /// Only valid in recycling mode — detaching-mode consumers keep ownership of their batches.
    public void recycle(B batch) {
        if (freeQueue == null) {
            throw new IllegalStateException(
                    "recycle() is not valid in detaching mode for column '" + columnName + "'");
        }
        freeQueue.offer(batch);
    }

    /// Drains any batches left in the ready queue back to the free pool.
    /// Called from the consumer's `close()` after the column has stopped.
    /// No-op in detaching mode.
    public void drainReady() {
        if (freeQueue == null) {
            return;
        }
        Object leftover;
        while ((leftover = readyQueue.poll()) != null) {
            if (leftover != END_OF_STREAM) {
                @SuppressWarnings("unchecked")
                B batch = (B) leftover;
                freeQueue.offer(batch);
            }
        }
    }

    /// Checks if the pipeline encountered an error and throws if so.
    /// Call this after detecting a finished/null batch to surface pipeline errors.
    ///
    /// An [IOException], a `RuntimeException` and an [Error] are all rethrown as they stand.
    /// [ScheduledColumn] has already said what a failure means by the time it reaches here — a
    /// decode failure is a `ParquetReadException`, a fetch failure the `IOException` it was —
    /// and an `Error` is neither the file's fault nor something a caller retries. Relabelling
    /// any of them would undo that one frame below the reader, and an `OutOfMemoryError`
    /// reported as a `RuntimeException` is catchable by handlers that must never see it.
    public void checkError() throws IOException {
        Throwable t = error.get();
        if (t != null) {
            if (t instanceof IOException io) {
                throw io;
            }
            if (t instanceof RuntimeException re) {
                throw re;
            }
            if (t instanceof Error e) {
                throw e;
            }
            throw new RuntimeException("Error in pipeline for column '" + columnName + "'", t);
        }
    }

    // ==================== Utilities ====================

    /// Allocates the per-batch values buffer for a column.
    ///
    /// For fixed-width physical types this returns the typed primitive
    /// array (`int[]`, `long[]`, …). For byte-array-shaped types it returns
    /// an empty [BinaryBatchValues] with views for `capacity` values. Its
    /// bytes buffer is allocated on first use, sized for appended values by
    /// `BINARY_BYTES_PER_VALUE_HINT` per value (`BYTE_ARRAY`, `INT96`) or by
    /// the column's width (`FIXED_LEN_BYTE_ARRAY`). The width is present and
    /// positive — [RowGroupIterator#initialize] validates it for every column
    /// a read touches before the first batch is allocated for it.
    public static Object allocateArray(ColumnSchema column, int capacity) {
        PhysicalType type = column.type();
        return switch (type) {
            case INT32 -> new int[capacity];
            case INT64 -> new long[capacity];
            case FLOAT -> new float[capacity];
            case DOUBLE -> new double[capacity];
            case BOOLEAN -> new boolean[capacity];
            case BYTE_ARRAY, INT96 -> new BinaryBatchValues(capacity, BINARY_BYTES_PER_VALUE_HINT);
            case FIXED_LEN_BYTE_ARRAY -> new BinaryBatchValues(capacity, column.typeLength());
        };
    }
}
