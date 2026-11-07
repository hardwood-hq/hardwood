/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.batch;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import dev.hardwood.internal.reader.step.ReadQueue;
import dev.hardwood.jfr.BatchWaitEvent;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.ColumnSchema;

/// Exchange buffer between a [dev.hardwood.internal.reader.pipeline.DecodedColumn]'s decode jobs
/// and the consumer. The column publishes without waiting ([#tryPublish]); a batch that finds no
/// room waits in the column until the consumer's next take ([#setRoomListener]).
///
/// Two modes are available:
///
/// - **Recycling** (`BatchExchange.recycling()`): batch holders cycle between producer and consumer
///   via `freeQueue` and `readyQueue`; a cell takes a free holder when there is one and a new one
///   from the factory otherwise ([#newBatch]). Used by
///   [dev.hardwood.internal.reader.row.FlatRowReader] and
///   [dev.hardwood.internal.reader.row.NestedRowReader] where the consumer returns batches after
///   reading. A flat cell refills a returned holder's arrays; a nested cell gives the holder new
///   arrays for every batch ([dev.hardwood.internal.reader.pipeline.NestedFill]), so the values a
///   row reader's flyweights refer to are never overwritten.
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
        /// Per-batch matches mask, populated by
        /// [dev.hardwood.internal.predicate.ColumnBatchMatcher] on the task that completes the
        /// batch when batch-matcher filtering is enabled. `null` means "no fragment for this
        /// column" — [dev.hardwood.internal.reader.row.BatchMatchMerger] never reads those columns,
        /// since it visits exactly the ones its plan references. When non-null, sized to
        /// `(batchCapacity + 63) >>> 6` and overwritten on every batch-matcher test call.
        public long[] matches;

        /// Whether statistics proved every record of this batch matches the filter
        /// predicate, making [#matches] all-ones without per-record evaluation.
        /// Only set on the batch-matcher filter path; `false` is always safe.
        public boolean filterAlwaysMatches;
    }

    /// Handed to the consumer by [#finish()] so that a poll already waiting on the queue learns
    /// of the end of the stream from the queue itself. See [#finish()] for why offering it can be
    /// left to succeed or not.
    private static final Object END_OF_STREAM = new Object();

    private final BlockingQueue<Object> readyQueue;
    private final BlockingQueue<B> freeQueue;
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
    /// The read's queue on a context without a pool, which [#poll()] runs; `null` where the
    /// context's pool runs the read.
    private ReadQueue queue;

    private BatchExchange(String columnName, BlockingQueue<Object> readyQueue,
                          BlockingQueue<B> freeQueue, Supplier<B> batchFactory,
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
    /// A decoded column fills holders past those three while it decodes ahead
    /// ([#newBatch]); the free queue keeps at most three of the holders returned, for holders
    /// whose arrays are allocated per batch anyway.
    ///
    /// @param columnName column name for error messages
    /// @param batchFactory creates the initial batch holders, and those a decoded column
    ///        needs beyond them
    /// @param <B> the batch type
    /// @return a recycling [BatchExchange]
    public static <B> BatchExchange<B> recycling(String columnName, Supplier<B> batchFactory) {
        return recycling(columnName, batchFactory, new ArrayBlockingQueue<>(READY_QUEUE_CAPACITY + 1));
    }

    /// Creates a recycling exchange whose free queue keeps every holder returned, the ones a
    /// decoded column filled past the three pre-allocated while it decoded ahead included, so
    /// that a read reuses the arrays of every holder its read-ahead filled rather than allocating
    /// new ones. The holders kept are at most those the read-ahead filled at once.
    ///
    /// @param columnName column name for error messages
    /// @param batchFactory creates the initial batch holders, and those a decoded column
    ///        needs beyond them
    /// @param <B> the batch type
    /// @return a recycling [BatchExchange]
    public static <B> BatchExchange<B> recyclingEveryHolder(String columnName, Supplier<B> batchFactory) {
        return recycling(columnName, batchFactory, new LinkedBlockingQueue<>());
    }

    private static <B> BatchExchange<B> recycling(String columnName, Supplier<B> batchFactory,
                                                  BlockingQueue<B> freeQueue) {
        ArrayBlockingQueue<Object> readyQueue = new ArrayBlockingQueue<>(READY_QUEUE_CAPACITY);

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

    // ==================== Column Side ====================

    /// Queues the batch without waiting, if the ready queue has room: in detaching mode, if it
    /// keeps the queue within `readyRowLimit` rows, an empty queue taking any batch; in recycling
    /// mode, if the bounded queue has a free slot. For one publisher at a time: the consumer only
    /// ever takes batches out, so the room this checks is still there when it queues.
    ///
    /// @return whether the batch was queued; if not, the [#setRoomListener] listener runs once
    ///         the consumer has taken a batch
    public boolean tryPublish(B batch) {
        boolean published;
        if (batchRows == null) {
            published = readyQueue.offer(batch);
        }
        else {
            long rows = rowsOf(batch);
            long queued = readyRows.get();
            if (queued > 0 && queued + rows > readyRowLimit) {
                return false;
            }
            published = queue(batch, rows);
        }
        if (published) {
            signalQueue();
        }
        return published;
    }

    /// A batch for the column to fill and publish ([#tryPublish]): a returned
    /// holder in recycling mode when one is free, a new one otherwise. Its arrays are the
    /// returned holder's, or none.
    public B newBatch() {
        B free = freeQueue != null ? freeQueue.poll() : null;
        return free != null ? free : batchFactory.get();
    }

    /// Sets what runs on the consumer's thread each time it takes a batch: for a column whose
    /// [#tryPublish] found no room, and which publishes there instead of waiting.
    public void setRoomListener(Runnable listener) {
        this.roomListener = listener;
    }

    /// Makes [#poll()] run the read's queue whenever it finds no batch, rather than wait for one,
    /// and the column's publishes signal the queue. Called before the read starts.
    public void setQueue(ReadQueue queue) {
        this.queue = queue;
    }

    private boolean queue(B batch, long rows) {
        readyRows.addAndGet(rows);
        readyQueue.offer(batch);
        return true;
    }

    /// Wakes the threads parked on the read's queue, after the ready queue changed.
    private void signalQueue() {
        if (queue != null) {
            queue.tasks().signal();
        }
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
    /// The flag alone is not enough. A consumer inside [#poll()]'s wait is waiting on the queue,
    /// not on the flag, and reads the flag only when the queue hands it something.
    ///
    /// The offer is deliberately non-blocking and its result deliberately ignored. A consumer is
    /// only ever left waiting when it found the queue empty, and in that state the offer has room
    /// and succeeds. It fails only when the queue is full, which only the recycling mode's bounded
    /// queue can be, and a consumer with batches still to take is not waiting for anything: by the
    /// time it has taken them, `finished` is set and [#poll()] returns without waiting at all.
    ///
    /// That makes the sentinel or a batch the element that releases every waiting consumer: the
    /// flag is set before the offer, a consumer waits only after reading it unset, so a consumer
    /// waiting when the offer fails is waiting on a full queue, and takes a batch.
    public void finish() {
        finished = true;
        readyQueue.offer(END_OF_STREAM);
        signalQueue();
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
    /// empty the method falls through to a waiting take. The `finished` flag
    /// is only checked **after** a poll returns null, guaranteeing that any
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
        if (queue != null) {
            return drive();
        }
        // Past this point the consumer is stalled on the pipeline: the ready
        // queue is empty and the column has not finished. The event's duration
        // is the stall, so it spans every exit from the wait.
        BatchWaitEvent event = new BatchWaitEvent();
        event.begin();
        try {
            batch = taken(readyQueue.take());
            if (batch == END_OF_STREAM) {
                checkError();
                return null;
            }
            return (B) batch;
        }
        finally {
            event.column = columnName;
            event.commit();
        }
    }

    /// Runs the read's queue on the consumer's thread until a batch is published or the stream
    /// ends, parking while nothing is queued; [BatchWaitEvent] spans the parks, which wait for
    /// reads in flight. An interrupt ends the wait before the next item runs, as it ends a pooled
    /// read's wait: an item may read a file, whose channel an interrupted thread would close.
    ///
    /// @throws IllegalStateException if no job of the read is in flight and no batch is ready:
    ///         nothing could publish one any more
    @SuppressWarnings("unchecked")
    private B drive() throws InterruptedException, IOException {
        while (true) {
            long seen = queue.tasks().signals();
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
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            if (queue.runOne()) {
                continue;
            }
            if (queue.tasks().idle()) {
                // Looked at again: work that ended before the queue was idle published before
                if (readyQueue.isEmpty() && !finished) {
                    throw new IllegalStateException("The read of column '" + columnName
                            + "' stalled: no batch is ready, and no task is queued or running and no read in flight");
                }
                continue;
            }
            BatchWaitEvent event = new BatchWaitEvent();
            event.begin();
            queue.tasks().park(seen);
            event.column = columnName;
            event.commit();
            if (Thread.interrupted()) {
                throw new InterruptedException();
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
    ///
    /// The close can run on another thread than a consumer still inside [#poll()], as a parent
    /// reader's close does. The queue ends holding the sentinel once the stream has finished, so
    /// that such a consumer is released: draining it, or the batches that took the room its offer
    /// in [#finish()] needed, would otherwise leave the consumer waiting on an empty queue.
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
        if (finished) {
            readyQueue.offer(END_OF_STREAM);
        }
    }

    /// Checks if the pipeline encountered an error and throws if so.
    /// Call this after detecting a finished/null batch to surface pipeline errors.
    ///
    /// An [IOException], a `RuntimeException` and an [Error] are all rethrown as they stand.
    /// [dev.hardwood.internal.reader.pipeline.DecodedColumn] has already said what a failure
    /// means by the time it reaches here — a decode failure is a `ParquetReadException`, a fetch
    /// failure the `IOException` it was — and an `Error` is neither the file's fault nor something
    /// a caller retries. Relabelling any of them would undo that one frame below the reader, and an
    /// `OutOfMemoryError` reported as a `RuntimeException` is catchable by handlers that must never
    /// see it.
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
    /// an empty [BinaryBatchValues] with views for `capacity` values, whose
    /// fill allocates the chunks its plain values' bytes take.
    public static Object allocateArray(ColumnSchema column, int capacity) {
        PhysicalType type = column.type();
        return switch (type) {
            case INT32 -> new int[capacity];
            case INT64 -> new long[capacity];
            case FLOAT -> new float[capacity];
            case DOUBLE -> new double[capacity];
            case BOOLEAN -> new boolean[capacity];
            case BYTE_ARRAY, INT96, FIXED_LEN_BYTE_ARRAY -> new BinaryBatchValues(capacity);
        };
    }
}
