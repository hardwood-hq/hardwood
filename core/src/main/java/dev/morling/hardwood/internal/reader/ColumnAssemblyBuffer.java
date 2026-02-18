/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import java.util.BitSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import dev.morling.hardwood.metadata.PhysicalType;
import dev.morling.hardwood.schema.ColumnSchema;

/**
 * Blocking queue of pre-assembled batches for a single column.
 * <p>
 * Enables eager batch assembly: the assembly thread assembles pages into TypedColumnData
 * as they are decoded, rather than waiting for the consumer to do this work.
 * </p>
 * <p>
 * Uses a {@link BlockingQueue} for producer-consumer coordination, which is more
 * CPU-friendly than spin-waiting when waits are longer.
 * </p>
 */
public class ColumnAssemblyBuffer {

    private static final System.Logger LOG = System.getLogger(ColumnAssemblyBuffer.class.getName());

    /**
     * Number of batches to buffer ahead.
     * We use 2 slots plus a pool of 3 arrays: while consumer uses one batch,
     * producer can have one in the queue and be filling another.
     */
    private static final int QUEUE_CAPACITY = 2;

    private final ColumnSchema column;
    private final PhysicalType physicalType;
    private final int batchCapacity;
    private final int maxDefinitionLevel;

    // Blocking queue of ready batches (produced by assembly thread, consumed by reader)
    private final BlockingQueue<TypedColumnData> readyBatches;

    // Pool of reusable arrays (producer takes, consumer returns via separate queue)
    private final BlockingQueue<Object> arrayPool;

    // Working state for the current batch being filled
    private Object currentValues;
    private BitSet currentNulls;  // Built incrementally during copyPageData
    private int rowsInCurrentBatch = 0;

    // Signals that no more data will be produced
    private volatile boolean finished = false;

    // Stores any error from the producer thread
    private volatile Throwable error = null;

    /**
     * Creates a new column assembly buffer.
     *
     * @param column the column schema
     * @param batchCapacity maximum rows per batch
     */
    public ColumnAssemblyBuffer(ColumnSchema column, int batchCapacity) {
        this.column = column;
        this.physicalType = column.type();
        this.batchCapacity = batchCapacity;
        this.maxDefinitionLevel = column.maxDefinitionLevel();

        // Blocking queue for published batches
        this.readyBatches = new ArrayBlockingQueue<>(QUEUE_CAPACITY);

        // Pool of reusable arrays: need QUEUE_CAPACITY + 1 (one being filled)
        // Plus 1 extra for the consumer's current batch = QUEUE_CAPACITY + 2
        this.arrayPool = new ArrayBlockingQueue<>(QUEUE_CAPACITY + 2);
        for (int i = 0; i < QUEUE_CAPACITY + 1; i++) {
            arrayPool.add(allocateArray(physicalType, batchCapacity));
        }

        // Take first array for current batch
        this.currentValues = arrayPool.poll();
        this.currentNulls = maxDefinitionLevel > 0 ? new BitSet(batchCapacity) : null;
    }

    private static Object allocateArray(PhysicalType type, int capacity) {
        return switch (type) {
            case INT32 -> new int[capacity];
            case INT64 -> new long[capacity];
            case FLOAT -> new float[capacity];
            case DOUBLE -> new double[capacity];
            case BOOLEAN -> new boolean[capacity];
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> new byte[capacity][];
        };
    }

    // ==================== Producer Methods (called by assembly thread) ====================

    /**
     * Appends a decoded page to the assembly buffer.
     * Called by the assembly thread after getting a decoded page.
     *
     * @param page the decoded page
     */
    public void appendPage(Page page) {
        int pageSize = page.size();
        int pagePosition = 0;

        while (pagePosition < pageSize) {
            int spaceInBatch = batchCapacity - rowsInCurrentBatch;
            int toCopy = Math.min(spaceInBatch, pageSize - pagePosition);

            // Copy page data to current working arrays
            copyPageData(page, pagePosition, rowsInCurrentBatch, toCopy);

            rowsInCurrentBatch += toCopy;
            pagePosition += toCopy;

            // Batch full? Create TypedColumnData and publish it.
            if (rowsInCurrentBatch >= batchCapacity) {
                publishCurrentBatch();
            }
        }
    }

    /**
     * Signals that no more pages will be produced.
     * Publishes any partial batch remaining.
     * This method is idempotent - subsequent calls have no effect.
     */
    public void finish() {
        if (finished) {
            return;
        }
        if (rowsInCurrentBatch > 0) {
            publishCurrentBatch();
        }
        finished = true;
    }

    /**
     * Signals that an error occurred in the producer thread.
     * The consumer will receive this error when calling awaitNextBatch().
     */
    public void signalError(Throwable t) {
        this.error = t;
        finished = true;
    }

    private void copyPageData(Page page, int srcPos, int destPos, int length) {
        switch (page) {
            case Page.IntPage p -> {
                System.arraycopy(p.values(), srcPos, (int[]) currentValues, destPos, length);
                markNulls(p.definitionLevels(), srcPos, destPos, length);
            }
            case Page.LongPage p -> {
                System.arraycopy(p.values(), srcPos, (long[]) currentValues, destPos, length);
                markNulls(p.definitionLevels(), srcPos, destPos, length);
            }
            case Page.FloatPage p -> {
                System.arraycopy(p.values(), srcPos, (float[]) currentValues, destPos, length);
                markNulls(p.definitionLevels(), srcPos, destPos, length);
            }
            case Page.DoublePage p -> {
                System.arraycopy(p.values(), srcPos, (double[]) currentValues, destPos, length);
                markNulls(p.definitionLevels(), srcPos, destPos, length);
            }
            case Page.BooleanPage p -> {
                System.arraycopy(p.values(), srcPos, (boolean[]) currentValues, destPos, length);
                markNulls(p.definitionLevels(), srcPos, destPos, length);
            }
            case Page.ByteArrayPage p -> {
                System.arraycopy(p.values(), srcPos, (byte[][]) currentValues, destPos, length);
                markNulls(p.definitionLevels(), srcPos, destPos, length);
            }
        }
    }

    /**
     * Marks null positions in currentNulls based on definition levels.
     * A value is null if its definition level is less than maxDefinitionLevel.
     */
    private void markNulls(int[] defLevels, int srcPos, int destPos, int length) {
        if (currentNulls != null && defLevels != null) {
            for (int i = 0; i < length; i++) {
                if (defLevels[srcPos + i] < maxDefinitionLevel) {
                    currentNulls.set(destPos + i);
                }
            }
        }
    }

    /**
     * Creates TypedColumnData from current working arrays and publishes it.
     * The null bitmap was already built incrementally during copyPageData().
     */
    private void publishCurrentBatch() {
        int recordCount = rowsInCurrentBatch;

        // Clone currentNulls since we reuse it for the next batch
        BitSet nulls = (currentNulls != null && !currentNulls.isEmpty())
                ? (BitSet) currentNulls.clone()
                : null;

        // Create TypedColumnData using currentValues and cloned nulls
        TypedColumnData data = createTypedColumnDataDirect(currentValues, recordCount, nulls);

        try {
            // Publish batch (blocks if queue is full)
            readyBatches.put(data);

            // Get next array from pool (blocks if pool is empty, waiting for consumer to return one)
            currentValues = arrayPool.take();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while publishing batch", e);
        }

        LOG.log(System.Logger.Level.TRACE,
                "Published batch for column ''{0}'' with {1} rows",
                column.name(), recordCount);

        // Reset for next batch
        rowsInCurrentBatch = 0;
        if (currentNulls != null) {
            currentNulls.clear();
        }
    }

    /**
     * Creates TypedColumnData using the provided array directly (no copy).
     * The array will be swapped out and reused later, so the consumer must
     * finish using it before the next batch from this slot is published.
     */
    private TypedColumnData createTypedColumnDataDirect(Object values, int recordCount, BitSet nulls) {
        return switch (physicalType) {
            case INT32 -> new FlatColumnData.IntColumn(column, (int[]) values, nulls, recordCount);
            case INT64 -> new FlatColumnData.LongColumn(column, (long[]) values, nulls, recordCount);
            case FLOAT -> new FlatColumnData.FloatColumn(column, (float[]) values, nulls, recordCount);
            case DOUBLE -> new FlatColumnData.DoubleColumn(column, (double[]) values, nulls, recordCount);
            case BOOLEAN -> new FlatColumnData.BooleanColumn(column, (boolean[]) values, nulls, recordCount);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> new FlatColumnData.ByteArrayColumn(column, (byte[][]) values, nulls, recordCount);
        };
    }

    // ==================== Consumer Methods (called by reader thread) ====================

    // Track previous batch to return its array to pool
    private TypedColumnData previousBatch = null;

    /**
     * Waits for and returns the next ready TypedColumnData.
     * Blocks until data is available or the producer signals completion.
     * <p>
     * If the producer encountered an error, this method will first return any
     * batches that were successfully queued before the error, then throw the
     * error when the queue is empty.
     *
     * @return the next TypedColumnData, or null if no more data
     * @throws RuntimeException if the producer encountered an error and the queue is empty
     */
    public TypedColumnData awaitNextBatch() {
        // Return previous batch's array to the pool for reuse
        if (previousBatch != null) {
            returnArrayToPool(previousBatch);
            previousBatch = null;
        }

        // Try to get next batch from queue first (drain before checking error)
        TypedColumnData data = readyBatches.poll();
        if (data == null) {
            // Queue is empty - check for error before returning null
            checkError();

            if (finished) {
                return null;  // No more batches
            }
            // Block waiting for batch
            try {
                while (!finished) {
                    data = readyBatches.poll(10, java.util.concurrent.TimeUnit.MILLISECONDS);
                    if (data != null) {
                        break;
                    }
                }
                if (data == null) {
                    checkError();
                    return null;  // Finished while waiting
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }

        previousBatch = data;
        return data;
    }

    /**
     * Checks if the producer encountered an error and throws it.
     */
    private void checkError() {
        Throwable t = error;
        if (t != null) {
            if (t instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException("Error in assembly thread for column '" + column.name() + "'", t);
        }
    }

    /**
     * Returns the backing array from a TypedColumnData to the pool for reuse.
     */
    private void returnArrayToPool(TypedColumnData data) {
        Object array = switch (data) {
            case FlatColumnData.IntColumn c -> c.values();
            case FlatColumnData.LongColumn c -> c.values();
            case FlatColumnData.FloatColumn c -> c.values();
            case FlatColumnData.DoubleColumn c -> c.values();
            case FlatColumnData.BooleanColumn c -> c.values();
            case FlatColumnData.ByteArrayColumn c -> c.values();
            default -> null;
        };
        if (array != null) {
            arrayPool.offer(array);
        }
    }

    /**
     * Checks if there are more batches available or pending.
     */
    public boolean hasMore() {
        return !readyBatches.isEmpty() || !finished;
    }

    /**
     * Checks if the producer has finished (either normally or with error).
     */
    public boolean isProducerFinished() {
        return finished;
    }

    /**
     * Gets the column schema.
     */
    public ColumnSchema column() {
        return column;
    }
}
