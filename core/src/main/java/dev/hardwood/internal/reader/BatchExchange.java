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
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import dev.hardwood.jfr.BatchWaitEvent;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.ColumnSchema;

/// Exchange buffer between a [ColumnWorker] drain thread and the consumer.
///
/// Two modes are available:
///
/// - **Recycling** (`BatchExchange.recycling()`): pre-allocates batch holders that cycle
///   between drain and consumer via `freeQueue` and `readyQueue`. No per-batch allocation.
///   Used by [FlatRowReader] and [NestedRowReader] where the consumer returns batches
///   after reading.
///
/// - **Detaching** (`BatchExchange.detaching()`): allocates a fresh batch each time
///   the drain needs one via the `batchFactory`. The consumer keeps ownership of each
///   batch (the arrays are not recycled). Back-pressure comes from the bounded `readyQueue`.
///   Used by [ColumnReader] where the caller retains the arrays between batches.
///
/// @param <B> the batch type (e.g. [Batch] for flat, [NestedBatch] for nested)
public class BatchExchange<B> {

    static final int READY_QUEUE_CAPACITY = 2;

    /// Per-value byte budget for variable-length `BYTE_ARRAY` / `INT96`
    /// buffers allocated by [#allocateArray]. The bytes buffer is pre-sized
    /// to `BINARY_BYTES_PER_VALUE_HINT * capacity` and grown on overflow by
    /// the worker append path.
    public static final int BINARY_BYTES_PER_VALUE_HINT = 32;

    /// A mutable batch holder for flat columns. Pre-allocated and reused — no per-batch allocation.
    /// The drain writes into it, the consumer reads from it.
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
        public String fileName;
        /// Per-batch matches mask, populated by [dev.hardwood.internal.predicate.ColumnBatchMatcher]
        /// on the drain thread when drain-side filtering is enabled. `null` means "no
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

    private final ArrayBlockingQueue<Object> readyQueue;
    private final ArrayBlockingQueue<B> freeQueue;
    private final Supplier<B> batchFactory;
    private final String columnName;

    private volatile Throwable error;
    private volatile boolean finished;

    private BatchExchange(String columnName, ArrayBlockingQueue<Object> readyQueue,
                          ArrayBlockingQueue<B> freeQueue, Supplier<B> batchFactory) {
        this.columnName = columnName;
        this.readyQueue = readyQueue;
        this.freeQueue = freeQueue;
        this.batchFactory = batchFactory;
    }

    /// Creates a recycling exchange that pre-allocates batch holders.
    ///
    /// Three batches are pre-allocated (`READY_QUEUE_CAPACITY + 1`): one filling
    /// plus up to two queued. The consumer must return consumed batches to `freeQueue()`.
    ///
    /// @param columnName column name for error messages
    /// @param batchFactory creates the initial batch holders
    /// @param <B> the batch type
    /// @return a recycling [BatchExchange]
    public static <B> BatchExchange<B> recycling(String columnName, Supplier<B> batchFactory) {
        ArrayBlockingQueue<Object> readyQueue = new ArrayBlockingQueue<>(READY_QUEUE_CAPACITY);
        ArrayBlockingQueue<B> freeQueue = new ArrayBlockingQueue<>(READY_QUEUE_CAPACITY + 1);

        for (int i = 0; i < READY_QUEUE_CAPACITY + 1; i++) {
            freeQueue.add(batchFactory.get());
        }

        return new BatchExchange<>(columnName, readyQueue, freeQueue, null);
    }

    /// Creates a detaching exchange that allocates a fresh batch each time.
    ///
    /// The drain calls `batchFactory.get()` for every batch. The consumer owns
    /// each batch after reading — arrays are not recycled. Back-pressure comes
    /// from the bounded `readyQueue` (capacity 2).
    ///
    /// @param columnName column name for error messages
    /// @param batchFactory creates a fresh batch on each call
    /// @param <B> the batch type
    /// @return a detaching [BatchExchange]
    public static <B> BatchExchange<B> detaching(String columnName, Supplier<B> batchFactory) {
        ArrayBlockingQueue<Object> readyQueue = new ArrayBlockingQueue<>(READY_QUEUE_CAPACITY);
        return new BatchExchange<>(columnName, readyQueue, null, batchFactory);
    }

    /// Returns the column name for error messages.
    public String name() {
        return columnName;
    }

    // ==================== Worker Side ====================

    /// Takes a batch for the drain to fill.
    ///
    /// In recycling mode, polls the `freeQueue` with timeout so the drain can
    /// check the `finished` flag periodically. In detaching mode, calls
    /// `batchFactory.get()` to allocate a fresh batch.
    public B takeBatch() throws InterruptedException {
        if (freeQueue != null) {
            // Recycling mode: poll from free queue
            B batch;
            while ((batch = freeQueue.poll(10, TimeUnit.MILLISECONDS)) == null) {
                if (finished) {
                    return null;
                }
            }
            return batch;
        }
        else {
            // Detaching mode: allocate a fresh batch
            return batchFactory.get();
        }
    }

    /// Publishes a filled batch to the consumer. Uses offer with timeout
    /// so the drain can check the `finished` flag periodically.
    public boolean publish(B batch) throws InterruptedException {
        while (!readyQueue.offer(batch, 10, TimeUnit.MILLISECONDS)) {
            if (finished) {
                return false;
            }
        }
        return true;
    }

    /// Marks the stream ended, and hands a waiting consumer the sentinel that says so.
    ///
    /// The flag alone is not enough. A consumer inside [#poll()]'s timed loop is waiting on the
    /// queue, not on the flag, and reads the flag only when its 10 ms poll expires — the same
    /// shape #1027 found on the teardown side, where a drain waiting inside the exchange could
    /// not see `finished` until its own window elapsed.
    ///
    /// The offer is deliberately non-blocking and its result deliberately ignored. A consumer is
    /// only ever left waiting when it found the queue empty, and in that state the offer has room
    /// and succeeds. It fails only when the queue is full, and a consumer with batches still to
    /// take is not waiting for anything: by the time it has taken them, `finished` is set and
    /// [#poll()] returns without waiting at all. So the sentinel removes a delay where there is
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
    public void signalError(Throwable t) {
        error = t;
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
        Object batch = readyQueue.poll();
        if (batch == END_OF_STREAM) {
            checkError();
            return null;
        }
        if (batch != null) {
            return (B) batch;
        }
        if (finished) {
            return (B) drop(readyQueue.poll());
        }
        // Past this point the consumer is stalled on the pipeline: the ready
        // queue is empty and the drain has not finished. The event's duration
        // is the stall, so it spans every exit from the timed poll loop.
        BatchWaitEvent event = new BatchWaitEvent();
        event.begin();
        try {
            while ((batch = readyQueue.poll(10, TimeUnit.MILLISECONDS)) == null) {
                if (finished) {
                    return (B) drop(readyQueue.poll());
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
        }
    }

    /// The sentinel read as nothing, so that a queue holding only it reads as an empty one.
    private static Object drop(Object element) {
        return element == END_OF_STREAM ? null : element;
    }

    /// Returns a consumed batch to the free pool so it can be refilled by the drain.
    /// Only valid in recycling mode — detaching-mode consumers keep ownership of their batches.
    public void recycle(B batch) {
        if (freeQueue == null) {
            throw new IllegalStateException(
                    "recycle() is not valid in detaching mode for column '" + columnName + "'");
        }
        freeQueue.offer(batch);
    }

    /// Drains any batches left in the ready queue back to the free pool.
    /// Called from the consumer's `close()` after the drain thread has stopped.
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
    /// [ColumnWorker] has already said what a failure means by the time it reaches here — a
    /// decode failure is a `ParquetReadException`, a fetch failure the `IOException` it was —
    /// and an `Error` is neither the file's fault nor something a caller retries. Relabelling
    /// any of them would undo that one frame below the reader, and an `OutOfMemoryError`
    /// reported as a `RuntimeException` is catchable by handlers that must never see it.
    public void checkError() throws IOException {
        Throwable t = error;
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
    /// a [BinaryBatchValues] holding the concatenated byte buffer and a
    /// sentinel-suffixed offsets array of length `capacity + 1`:
    ///
    /// - `BYTE_ARRAY` / `INT96`: bytes buffer is **capacity-sized** to
    ///   `BINARY_BYTES_PER_VALUE_HINT * capacity` and grows on overflow
    ///   via [BinaryBatchValues#appendAt].
    /// - `FIXED_LEN_BYTE_ARRAY`: bytes buffer is sized exactly to
    ///   `width * capacity` (`width = column.typeLength()`); offsets are
    ///   filled trivially as `i * width`. The width is present and positive —
    ///   [RowGroupIterator#initialize] validates it for every column a read
    ///   touches before the first batch is allocated for it.
    public static Object allocateArray(ColumnSchema column, int capacity) {
        PhysicalType type = column.type();
        return switch (type) {
            case INT32 -> new int[capacity];
            case INT64 -> new long[capacity];
            case FLOAT -> new float[capacity];
            case DOUBLE -> new double[capacity];
            case BOOLEAN -> new boolean[capacity];
            case BYTE_ARRAY, INT96 -> {
                BinaryBatchValues bbv = new BinaryBatchValues(
                        new byte[Math.multiplyExact(BINARY_BYTES_PER_VALUE_HINT, capacity)],
                        new int[capacity + 1]);
                // String leaves carry per-value dictionary indices so the row
                // reader reuses one interned String per entry; the backing array
                // is allocated lazily on the first dictionary page (see
                // BinaryBatchValues#ensureDictionary).
                bbv.internStrings = isStringColumn(column);
                yield bbv;
            }
            case FIXED_LEN_BYTE_ARRAY -> {
                int width = column.typeLength();
                byte[] bytes = new byte[Math.multiplyExact(width, capacity)];
                int[] offsets = new int[capacity + 1];
                for (int i = 0; i <= capacity; i++) {
                    offsets[i] = Math.multiplyExact(i, width);
                }
                yield new BinaryBatchValues(bytes, offsets);
            }
        };
    }

    /// Allocates a per-batch values buffer and enables dictionary-index
    /// retention when a dictionary-aware filter consumes the binary values.
    /// Fixed-width primitive arrays ignore the retention flag.
    ///
    /// Unlike [BinaryBatchValues#internStrings], which the two-argument form
    /// derives from the column alone, retention is a property of the compiled
    /// predicate rather than of the column, so only allocation sites that know
    /// the batch's matcher can set it. A drain that fills its own accumulator
    /// instead of the batch this returns — as [NestedColumnWorker] does —
    /// cannot honour it at all.
    public static Object allocateArray(ColumnSchema column, int capacity, boolean retainDictionaryIndices) {
        Object values = allocateArray(column, capacity);
        if (retainDictionaryIndices && values instanceof BinaryBatchValues binaryValues) {
            binaryValues.retainDictionaryIndices = true;
        }
        return values;
    }

    /// Whether `column` is a `UTF8` / `JSON` `BYTE_ARRAY` column — the leaves
    /// whose row-reader values are materialised as `String` and so benefit from
    /// dictionary-entry interning ([BinaryBatchValues#internStrings]). Resolves
    /// through [LeafKind] so the recording gate matches the consumer gate exactly.
    private static boolean isStringColumn(ColumnSchema column) {
        return LeafKind.of(column.type(), column.logicalType()) == LeafKind.STRING;
    }
}
