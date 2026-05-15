/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.BitSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

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
    /// `validity` carries set-bit-= -present semantics (a set bit means the
    /// leaf value at that position is present); `null` is the sparse
    /// representation of "every leaf in this batch is present."
    ///
    /// `values` is a typed primitive array (`int[]`, `long[]`, …) for
    /// fixed-width physical types and a [BinaryBatchValues] for byte-array
    /// types.
    public static final class Batch {
        public Object values;
        /// Public per-batch validity bitmap. Set-bit-= -present polarity; `null` is
        /// the sparse "every leaf present" form. When non-null, references
        /// [#validityBuffer]; consumers must treat it as read-only.
        public BitSet validity;
        /// Pre-allocated scratch BitSet used by the drain to record per-row
        /// presence without per-batch allocation. Allocated once by the
        /// [BatchExchange] factory for nullable columns; `null` for REQUIRED
        /// columns. The drain calls `validityBuffer.clear()` between batches and
        /// switches it on lazily when the first absent is observed. At publish
        /// time, [#validity] is assigned to this buffer iff at least one absent
        /// was recorded; otherwise [#validity] is set to `null`.
        public BitSet validityBuffer;
        public int recordCount;
        public String fileName;
        /// Per-batch matches mask, populated by [dev.hardwood.internal.predicate.ColumnBatchMatcher]
        /// on the drain thread when drain-side filtering is enabled. `null` means "no
        /// fragment for this column" — interpreted as all-ones during intersection.
        /// When non-null, sized to `(batchCapacity + 63) >>> 6` and overwritten on every
        /// drain-side test call.
        public long[] matches;
    }

    private final ArrayBlockingQueue<B> readyQueue;
    private final ArrayBlockingQueue<B> freeQueue;
    private final Supplier<B> batchFactory;
    private final String columnName;

    private volatile Throwable error;
    private volatile boolean finished;

    private BatchExchange(String columnName, ArrayBlockingQueue<B> readyQueue,
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
        ArrayBlockingQueue<B> readyQueue = new ArrayBlockingQueue<>(READY_QUEUE_CAPACITY);
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
        ArrayBlockingQueue<B> readyQueue = new ArrayBlockingQueue<>(READY_QUEUE_CAPACITY);
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

    public void finish() {
        finished = true;
    }

    public boolean isFinished() {
        return finished;
    }

    public void signalError(Throwable t) {
        error = t;
        finished = true;
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
    public B poll() throws InterruptedException {
        B batch = readyQueue.poll();
        if (batch != null) {
            return batch;
        }
        if (finished) {
            return readyQueue.poll();
        }
        while ((batch = readyQueue.poll(10, TimeUnit.MILLISECONDS)) == null) {
            if (finished) {
                return readyQueue.poll();
            }
            checkError();
        }
        return batch;
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
        B leftover;
        while ((leftover = readyQueue.poll()) != null) {
            freeQueue.offer(leftover);
        }
    }

    /// Checks if the pipeline encountered an error and throws if so.
    /// Call this after detecting a finished/null batch to surface pipeline errors.
    public void checkError() {
        Throwable t = error;
        if (t != null) {
            if (t instanceof RuntimeException re) {
                throw re;
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
    ///   filled trivially as `i * width`.
    public static Object allocateArray(ColumnSchema column, int capacity) {
        PhysicalType type = column.type();
        return switch (type) {
            case INT32 -> new int[capacity];
            case INT64 -> new long[capacity];
            case FLOAT -> new float[capacity];
            case DOUBLE -> new double[capacity];
            case BOOLEAN -> new boolean[capacity];
            case BYTE_ARRAY, INT96 -> new BinaryBatchValues(
                    new byte[Math.multiplyExact(BINARY_BYTES_PER_VALUE_HINT, capacity)],
                    new int[capacity + 1]);
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
}
