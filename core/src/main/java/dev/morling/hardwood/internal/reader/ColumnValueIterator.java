/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import dev.morling.hardwood.schema.ColumnSchema;

/**
 * Reads column values from pages and prefetches them into TypedColumnData batches.
 *
 * <p>This class uses async prefetching with adaptive depth to ensure that
 * TypedColumnData is typically ready before it's needed. The prefetch depth
 * automatically adapts based on whether batches are ready when requested:</p>
 * <ul>
 *   <li>If prefetch() finds the batch ready (hit), depth may decrease</li>
 *   <li>If prefetch() has to wait for a batch (miss), depth increases</li>
 * </ul>
 *
 * <p>For flat schemas, use {@link #prefetch(int)} which copies directly into
 * typed primitive arrays. For nested schemas, it handles repetition levels.</p>
 */
public class ColumnValueIterator {

    private static final System.Logger LOG = System.getLogger(ColumnValueIterator.class.getName());

    private static final int INITIAL_PREFETCH_DEPTH = 1;
    private static final int MAX_PREFETCH_DEPTH = 4;
    private static final int HITS_TO_DECREASE = 10;
    private static final int MISSES_TO_INCREASE = 1;

    private final PageCursor pageCursor;
    private final ColumnSchema column;
    private final Executor executor;
    private final int maxDefinitionLevel;
    private final boolean flatSchema;

    private Page currentPage;
    private int position;
    private int currentRecordStart;
    private boolean recordActive;

    // Async prefetch state
    private final Deque<CompletableFuture<TypedColumnData>> prefetchQueue = new ArrayDeque<>();
    private int prefetchBatchSize;
    private int targetPrefetchDepth = INITIAL_PREFETCH_DEPTH;
    private int hitCount = 0;
    private int missCount = 0;
    private boolean exhausted = false;

    public ColumnValueIterator(PageCursor pageCursor, ColumnSchema column, Executor executor, boolean flatSchema) {
        this.pageCursor = pageCursor;
        this.column = column;
        this.executor = executor;
        this.maxDefinitionLevel = column.maxDefinitionLevel();
        this.flatSchema = flatSchema;
    }

    // ==================== Batch Prefetch ====================

    /**
     * Prefetch values for up to {@code maxRecords} records into typed arrays.
     * Uses async prefetching to ensure the next batch is typically ready.
     *
     * @param maxRecords maximum number of records to prefetch
     * @return typed column data containing values, levels, and record boundaries
     */
    public TypedColumnData prefetch(int maxRecords) {
        if (exhausted) {
            return emptyTypedColumnData();
        }

        TypedColumnData result;

        // Check if we have a prefetched batch ready
        if (!prefetchQueue.isEmpty() && prefetchBatchSize == maxRecords) {
            CompletableFuture<TypedColumnData> future = prefetchQueue.pollFirst();

            if (future.isDone()) {
                // Hit: prefetch was ready
                hitCount++;
                missCount = 0;
                if (hitCount >= HITS_TO_DECREASE && targetPrefetchDepth > INITIAL_PREFETCH_DEPTH) {
                    targetPrefetchDepth--;
                    hitCount = 0;
                    LOG.log(System.Logger.Level.DEBUG, "Decreasing prefetch depth for column ''{0}'' to {1}",
                            column.name(), targetPrefetchDepth);
                }
            }
            else {
                // Miss: had to wait
                LOG.log(System.Logger.Level.DEBUG, "Prefetch miss for column ''{0}'', current depth={1}",
                        column.name(), targetPrefetchDepth);
                missCount++;
                hitCount = 0;
                if (missCount >= MISSES_TO_INCREASE && targetPrefetchDepth < MAX_PREFETCH_DEPTH) {
                    targetPrefetchDepth++;
                    LOG.log(System.Logger.Level.DEBUG, "Increasing prefetch depth for column ''{0}'' to {1}",
                            column.name(), targetPrefetchDepth);
                    missCount = 0;
                }
            }

            result = future.join();
        }
        else {
            // No prefetch available or wrong batch size - clear queue and compute synchronously
            LOG.log(System.Logger.Level.DEBUG, "Prefetch queue empty for column ''{0}'', computing synchronously",
                    column.name());
            prefetchQueue.clear();
            result = computeBatch(maxRecords);
        }

        if (result.recordCount() == 0) {
            exhausted = true;
        }
        else {
            // Fill prefetch queue up to target depth
            fillPrefetchQueue(maxRecords);
        }

        return result;
    }

    /**
     * Fill the prefetch queue up to targetPrefetchDepth.
     * Batches are chained sequentially since each depends on state from the previous.
     */
    private void fillPrefetchQueue(int maxRecords) {
        prefetchBatchSize = maxRecords;
        int toAdd = targetPrefetchDepth - prefetchQueue.size();
        for (int i = 0; i < toAdd && !exhausted; i++) {
            // Chain each batch after the previous one to maintain sequential computation
            CompletableFuture<TypedColumnData> previous = prefetchQueue.peekLast();
            CompletableFuture<TypedColumnData> next;
            if (previous == null) {
                // First in queue - start immediately
                next = CompletableFuture.supplyAsync(() -> computeBatch(maxRecords), executor);
            }
            else {
                // Chain after previous batch completes
                next = previous.thenApplyAsync(ignored -> computeBatch(maxRecords), executor);
            }
            prefetchQueue.addLast(next);
        }
    }

    /**
     * Compute a batch synchronously from current state.
     */
    private TypedColumnData computeBatch(int maxRecords) {
        if (flatSchema) {
            return computeFlatBatch(maxRecords);
        }
        return computeNestedBatch(maxRecords);
    }

    /**
     * Return an empty TypedColumnData based on the column's physical type.
     */
    private TypedColumnData emptyTypedColumnData() {
        if (flatSchema) {
            return switch (column.type()) {
                case INT32 -> new FlatColumnData.IntColumn(column, new int[0], null, 0);
                case INT64 -> new FlatColumnData.LongColumn(column, new long[0], null, 0);
                case FLOAT -> new FlatColumnData.FloatColumn(column, new float[0], null, 0);
                case DOUBLE -> new FlatColumnData.DoubleColumn(column, new double[0], null, 0);
                case BOOLEAN -> new FlatColumnData.BooleanColumn(column, new boolean[0], null, 0);
                case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> new FlatColumnData.ByteArrayColumn(column, new byte[0][], null, 0);
            };
        }
        int maxDefLevel = column.maxDefinitionLevel();
        return switch (column.type()) {
            case INT32 -> new NestedColumnData.IntColumn(column, new int[0], new int[0], null, null, maxDefLevel, 0);
            case INT64 -> new NestedColumnData.LongColumn(column, new long[0], new int[0], null, null, maxDefLevel, 0);
            case FLOAT -> new NestedColumnData.FloatColumn(column, new float[0], new int[0], null, null, maxDefLevel, 0);
            case DOUBLE -> new NestedColumnData.DoubleColumn(column, new double[0], new int[0], null, null, maxDefLevel, 0);
            case BOOLEAN -> new NestedColumnData.BooleanColumn(column, new boolean[0], new int[0], null, null, maxDefLevel, 0);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> new NestedColumnData.ByteArrayColumn(column, new byte[0][], new int[0], null, null, maxDefLevel, 0);
        };
    }

    // ==================== Flat Batch Computation ====================

    private void markNulls(BitSet nulls, int[] defLevels, int srcPos, int destPos, int count, int maxDefLevel) {
        if (nulls == null) {
            return;
        }
        for (int i = 0; i < count; i++) {
            if (defLevels[srcPos + i] < maxDefLevel) {
                nulls.set(destPos + i);
            }
        }
    }

    private TypedColumnData computeFlatBatch(int maxRecords) {
        int maxDefLevel = column.maxDefinitionLevel();

        if (!ensurePageLoaded()) {
            return emptyTypedColumnData();
        }

        return switch (currentPage) {
            case Page.IntPage p -> computeFlatInt(maxRecords, maxDefLevel);
            case Page.LongPage p -> computeFlatLong(maxRecords, maxDefLevel);
            case Page.FloatPage p -> computeFlatFloat(maxRecords, maxDefLevel);
            case Page.DoublePage p -> computeFlatDouble(maxRecords, maxDefLevel);
            case Page.BooleanPage p -> computeFlatBoolean(maxRecords, maxDefLevel);
            case Page.ByteArrayPage p -> computeFlatByteArray(maxRecords, maxDefLevel);
        };
    }

    private TypedColumnData computeFlatInt(int maxRecords, int maxDefLevel) {
        int[] values = new int[maxRecords];
        BitSet nulls = maxDefLevel > 0 ? new BitSet(maxRecords) : null;

        int recordCount = 0;
        while (recordCount < maxRecords && ensurePageLoaded()) {
            Page.IntPage page = (Page.IntPage) currentPage;
            int available = page.size() - position;
            int toCopy = Math.min(available, maxRecords - recordCount);

            System.arraycopy(page.values(), position, values, recordCount, toCopy);
            markNulls(nulls, page.definitionLevels(), position, recordCount, toCopy, maxDefLevel);

            position += toCopy;
            recordCount += toCopy;
        }

        if (recordCount < maxRecords) {
            values = Arrays.copyOf(values, recordCount);
        }

        return new FlatColumnData.IntColumn(column, values, nulls, recordCount);
    }

    private TypedColumnData computeFlatLong(int maxRecords, int maxDefLevel) {
        long[] values = new long[maxRecords];
        BitSet nulls = maxDefLevel > 0 ? new BitSet(maxRecords) : null;

        int recordCount = 0;
        while (recordCount < maxRecords && ensurePageLoaded()) {
            Page.LongPage page = (Page.LongPage) currentPage;
            int available = page.size() - position;
            int toCopy = Math.min(available, maxRecords - recordCount);

            System.arraycopy(page.values(), position, values, recordCount, toCopy);
            markNulls(nulls, page.definitionLevels(), position, recordCount, toCopy, maxDefLevel);

            position += toCopy;
            recordCount += toCopy;
        }

        if (recordCount < maxRecords) {
            values = Arrays.copyOf(values, recordCount);
        }

        return new FlatColumnData.LongColumn(column, values, nulls, recordCount);
    }

    private TypedColumnData computeFlatFloat(int maxRecords, int maxDefLevel) {
        float[] values = new float[maxRecords];
        BitSet nulls = maxDefLevel > 0 ? new BitSet(maxRecords) : null;

        int recordCount = 0;
        while (recordCount < maxRecords && ensurePageLoaded()) {
            Page.FloatPage page = (Page.FloatPage) currentPage;
            int available = page.size() - position;
            int toCopy = Math.min(available, maxRecords - recordCount);

            System.arraycopy(page.values(), position, values, recordCount, toCopy);
            markNulls(nulls, page.definitionLevels(), position, recordCount, toCopy, maxDefLevel);

            position += toCopy;
            recordCount += toCopy;
        }

        if (recordCount < maxRecords) {
            values = Arrays.copyOf(values, recordCount);
        }

        return new FlatColumnData.FloatColumn(column, values, nulls, recordCount);
    }

    private TypedColumnData computeFlatDouble(int maxRecords, int maxDefLevel) {
        double[] values = new double[maxRecords];
        BitSet nulls = maxDefLevel > 0 ? new BitSet(maxRecords) : null;

        int recordCount = 0;
        while (recordCount < maxRecords && ensurePageLoaded()) {
            Page.DoublePage page = (Page.DoublePage) currentPage;
            int available = page.size() - position;
            int toCopy = Math.min(available, maxRecords - recordCount);

            System.arraycopy(page.values(), position, values, recordCount, toCopy);
            markNulls(nulls, page.definitionLevels(), position, recordCount, toCopy, maxDefLevel);

            position += toCopy;
            recordCount += toCopy;
        }

        if (recordCount < maxRecords) {
            values = Arrays.copyOf(values, recordCount);
        }

        return new FlatColumnData.DoubleColumn(column, values, nulls, recordCount);
    }

    private TypedColumnData computeFlatBoolean(int maxRecords, int maxDefLevel) {
        boolean[] values = new boolean[maxRecords];
        BitSet nulls = maxDefLevel > 0 ? new BitSet(maxRecords) : null;

        int recordCount = 0;
        while (recordCount < maxRecords && ensurePageLoaded()) {
            Page.BooleanPage page = (Page.BooleanPage) currentPage;
            int available = page.size() - position;
            int toCopy = Math.min(available, maxRecords - recordCount);

            System.arraycopy(page.values(), position, values, recordCount, toCopy);
            markNulls(nulls, page.definitionLevels(), position, recordCount, toCopy, maxDefLevel);

            position += toCopy;
            recordCount += toCopy;
        }

        if (recordCount < maxRecords) {
            values = Arrays.copyOf(values, recordCount);
        }

        return new FlatColumnData.BooleanColumn(column, values, nulls, recordCount);
    }

    private TypedColumnData computeFlatByteArray(int maxRecords, int maxDefLevel) {
        byte[][] values = new byte[maxRecords][];
        BitSet nulls = maxDefLevel > 0 ? new BitSet(maxRecords) : null;

        int recordCount = 0;
        while (recordCount < maxRecords && ensurePageLoaded()) {
            Page.ByteArrayPage page = (Page.ByteArrayPage) currentPage;
            int available = page.size() - position;
            int toCopy = Math.min(available, maxRecords - recordCount);

            System.arraycopy(page.values(), position, values, recordCount, toCopy);
            markNulls(nulls, page.definitionLevels(), position, recordCount, toCopy, maxDefLevel);

            position += toCopy;
            recordCount += toCopy;
        }

        if (recordCount < maxRecords) {
            values = Arrays.copyOf(values, recordCount);
        }

        return new FlatColumnData.ByteArrayColumn(column, values, nulls, recordCount);
    }

    // ==================== Nested Batch Computation ====================

    private TypedColumnData computeNestedBatch(int maxRecords) {
        int maxDefLevel = column.maxDefinitionLevel();

        if (!ensurePageLoaded()) {
            return switch (column.type()) {
                case INT32 -> new NestedColumnData.IntColumn(column, new int[0], new int[0], new int[0], new int[0], maxDefLevel, 0);
                case INT64 -> new NestedColumnData.LongColumn(column, new long[0], new int[0], new int[0], new int[0], maxDefLevel, 0);
                case FLOAT -> new NestedColumnData.FloatColumn(column, new float[0], new int[0], new int[0], new int[0], maxDefLevel, 0);
                case DOUBLE -> new NestedColumnData.DoubleColumn(column, new double[0], new int[0], new int[0], new int[0], maxDefLevel, 0);
                case BOOLEAN -> new NestedColumnData.BooleanColumn(column, new boolean[0], new int[0], new int[0], new int[0], maxDefLevel, 0);
                case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> new NestedColumnData.ByteArrayColumn(column, new byte[0][], new int[0], new int[0], new int[0], maxDefLevel, 0);
            };
        }

        return switch (currentPage) {
            case Page.IntPage p -> computeNestedInt(maxRecords, maxDefLevel);
            case Page.LongPage p -> computeNestedLong(maxRecords, maxDefLevel);
            case Page.FloatPage p -> computeNestedFloat(maxRecords, maxDefLevel);
            case Page.DoublePage p -> computeNestedDouble(maxRecords, maxDefLevel);
            case Page.BooleanPage p -> computeNestedBoolean(maxRecords, maxDefLevel);
            case Page.ByteArrayPage p -> computeNestedByteArray(maxRecords, maxDefLevel);
        };
    }

    private TypedColumnData computeNestedInt(int maxRecords, int maxDefLevel) {
        int estimatedValues = maxRecords * 2;
        int[] values = new int[estimatedValues];
        int[] defLevels = new int[estimatedValues];
        int[] repLevels = new int[estimatedValues];
        int[] recordOffsets = new int[maxRecords];

        int valueCount = 0;
        int recordCount = 0;

        while (recordCount < maxRecords && nextRecord()) {
            recordOffsets[recordCount] = valueCount;

            while (hasValue()) {
                if (valueCount >= values.length) {
                    int newSize = values.length * 2;
                    values = Arrays.copyOf(values, newSize);
                    defLevels = Arrays.copyOf(defLevels, newSize);
                    repLevels = Arrays.copyOf(repLevels, newSize);
                }

                repLevels[valueCount] = repetitionLevel();
                defLevels[valueCount] = definitionLevel();
                values[valueCount] = ((Page.IntPage) currentPage).get(position);
                valueCount++;
                advance();
            }

            recordCount++;
        }

        if (valueCount < values.length) {
            values = Arrays.copyOf(values, valueCount);
            defLevels = Arrays.copyOf(defLevels, valueCount);
            repLevels = Arrays.copyOf(repLevels, valueCount);
        }
        if (recordCount < recordOffsets.length) {
            recordOffsets = Arrays.copyOf(recordOffsets, recordCount);
        }

        return new NestedColumnData.IntColumn(column, values, defLevels, repLevels, recordOffsets, maxDefLevel, recordCount);
    }

    private TypedColumnData computeNestedLong(int maxRecords, int maxDefLevel) {
        int estimatedValues = maxRecords * 2;
        long[] values = new long[estimatedValues];
        int[] defLevels = new int[estimatedValues];
        int[] repLevels = new int[estimatedValues];
        int[] recordOffsets = new int[maxRecords];

        int valueCount = 0;
        int recordCount = 0;

        while (recordCount < maxRecords && nextRecord()) {
            recordOffsets[recordCount] = valueCount;

            while (hasValue()) {
                if (valueCount >= values.length) {
                    int newSize = values.length * 2;
                    values = Arrays.copyOf(values, newSize);
                    defLevels = Arrays.copyOf(defLevels, newSize);
                    repLevels = Arrays.copyOf(repLevels, newSize);
                }

                repLevels[valueCount] = repetitionLevel();
                defLevels[valueCount] = definitionLevel();
                values[valueCount] = ((Page.LongPage) currentPage).get(position);
                valueCount++;
                advance();
            }

            recordCount++;
        }

        if (valueCount < values.length) {
            values = Arrays.copyOf(values, valueCount);
            defLevels = Arrays.copyOf(defLevels, valueCount);
            repLevels = Arrays.copyOf(repLevels, valueCount);
        }
        if (recordCount < recordOffsets.length) {
            recordOffsets = Arrays.copyOf(recordOffsets, recordCount);
        }

        return new NestedColumnData.LongColumn(column, values, defLevels, repLevels, recordOffsets, maxDefLevel, recordCount);
    }

    private TypedColumnData computeNestedFloat(int maxRecords, int maxDefLevel) {
        int estimatedValues = maxRecords * 2;
        float[] values = new float[estimatedValues];
        int[] defLevels = new int[estimatedValues];
        int[] repLevels = new int[estimatedValues];
        int[] recordOffsets = new int[maxRecords];

        int valueCount = 0;
        int recordCount = 0;

        while (recordCount < maxRecords && nextRecord()) {
            recordOffsets[recordCount] = valueCount;

            while (hasValue()) {
                if (valueCount >= values.length) {
                    int newSize = values.length * 2;
                    values = Arrays.copyOf(values, newSize);
                    defLevels = Arrays.copyOf(defLevels, newSize);
                    repLevels = Arrays.copyOf(repLevels, newSize);
                }

                repLevels[valueCount] = repetitionLevel();
                defLevels[valueCount] = definitionLevel();
                values[valueCount] = ((Page.FloatPage) currentPage).get(position);
                valueCount++;
                advance();
            }

            recordCount++;
        }

        if (valueCount < values.length) {
            values = Arrays.copyOf(values, valueCount);
            defLevels = Arrays.copyOf(defLevels, valueCount);
            repLevels = Arrays.copyOf(repLevels, valueCount);
        }
        if (recordCount < recordOffsets.length) {
            recordOffsets = Arrays.copyOf(recordOffsets, recordCount);
        }

        return new NestedColumnData.FloatColumn(column, values, defLevels, repLevels, recordOffsets, maxDefLevel, recordCount);
    }

    private TypedColumnData computeNestedDouble(int maxRecords, int maxDefLevel) {
        int estimatedValues = maxRecords * 2;
        double[] values = new double[estimatedValues];
        int[] defLevels = new int[estimatedValues];
        int[] repLevels = new int[estimatedValues];
        int[] recordOffsets = new int[maxRecords];

        int valueCount = 0;
        int recordCount = 0;

        while (recordCount < maxRecords && nextRecord()) {
            recordOffsets[recordCount] = valueCount;

            while (hasValue()) {
                if (valueCount >= values.length) {
                    int newSize = values.length * 2;
                    values = Arrays.copyOf(values, newSize);
                    defLevels = Arrays.copyOf(defLevels, newSize);
                    repLevels = Arrays.copyOf(repLevels, newSize);
                }

                repLevels[valueCount] = repetitionLevel();
                defLevels[valueCount] = definitionLevel();
                values[valueCount] = ((Page.DoublePage) currentPage).get(position);
                valueCount++;
                advance();
            }

            recordCount++;
        }

        if (valueCount < values.length) {
            values = Arrays.copyOf(values, valueCount);
            defLevels = Arrays.copyOf(defLevels, valueCount);
            repLevels = Arrays.copyOf(repLevels, valueCount);
        }
        if (recordCount < recordOffsets.length) {
            recordOffsets = Arrays.copyOf(recordOffsets, recordCount);
        }

        return new NestedColumnData.DoubleColumn(column, values, defLevels, repLevels, recordOffsets, maxDefLevel, recordCount);
    }

    private TypedColumnData computeNestedBoolean(int maxRecords, int maxDefLevel) {
        int estimatedValues = maxRecords * 2;
        boolean[] values = new boolean[estimatedValues];
        int[] defLevels = new int[estimatedValues];
        int[] repLevels = new int[estimatedValues];
        int[] recordOffsets = new int[maxRecords];

        int valueCount = 0;
        int recordCount = 0;

        while (recordCount < maxRecords && nextRecord()) {
            recordOffsets[recordCount] = valueCount;

            while (hasValue()) {
                if (valueCount >= values.length) {
                    int newSize = values.length * 2;
                    values = Arrays.copyOf(values, newSize);
                    defLevels = Arrays.copyOf(defLevels, newSize);
                    repLevels = Arrays.copyOf(repLevels, newSize);
                }

                repLevels[valueCount] = repetitionLevel();
                defLevels[valueCount] = definitionLevel();
                values[valueCount] = ((Page.BooleanPage) currentPage).get(position);
                valueCount++;
                advance();
            }

            recordCount++;
        }

        if (valueCount < values.length) {
            values = Arrays.copyOf(values, valueCount);
            defLevels = Arrays.copyOf(defLevels, valueCount);
            repLevels = Arrays.copyOf(repLevels, valueCount);
        }
        if (recordCount < recordOffsets.length) {
            recordOffsets = Arrays.copyOf(recordOffsets, recordCount);
        }

        return new NestedColumnData.BooleanColumn(column, values, defLevels, repLevels, recordOffsets, maxDefLevel, recordCount);
    }

    private TypedColumnData computeNestedByteArray(int maxRecords, int maxDefLevel) {
        int estimatedValues = maxRecords * 2;
        byte[][] values = new byte[estimatedValues][];
        int[] defLevels = new int[estimatedValues];
        int[] repLevels = new int[estimatedValues];
        int[] recordOffsets = new int[maxRecords];

        int valueCount = 0;
        int recordCount = 0;

        while (recordCount < maxRecords && nextRecord()) {
            recordOffsets[recordCount] = valueCount;

            while (hasValue()) {
                if (valueCount >= values.length) {
                    int newSize = values.length * 2;
                    values = Arrays.copyOf(values, newSize);
                    defLevels = Arrays.copyOf(defLevels, newSize);
                    repLevels = Arrays.copyOf(repLevels, newSize);
                }

                repLevels[valueCount] = repetitionLevel();
                defLevels[valueCount] = definitionLevel();
                values[valueCount] = ((Page.ByteArrayPage) currentPage).get(position);
                valueCount++;
                advance();
            }

            recordCount++;
        }

        if (valueCount < values.length) {
            values = Arrays.copyOf(values, valueCount);
            defLevels = Arrays.copyOf(defLevels, valueCount);
            repLevels = Arrays.copyOf(repLevels, valueCount);
        }
        if (recordCount < recordOffsets.length) {
            recordOffsets = Arrays.copyOf(recordOffsets, recordCount);
        }

        return new NestedColumnData.ByteArrayColumn(column, values, defLevels, repLevels, recordOffsets, maxDefLevel, recordCount);
    }

    // ==================== Internal Helpers ====================

    private boolean ensurePageLoaded() {
        if (currentPage != null && position < currentPage.size()) {
            return true;
        }

        if (!pageCursor.hasNext()) {
            return false;
        }

        currentPage = pageCursor.nextPage();
        position = 0;
        return currentPage != null;
    }

    private boolean nextRecord() {
        if (!ensurePageLoaded()) {
            recordActive = false;
            return false;
        }

        currentRecordStart = position;
        recordActive = true;
        return true;
    }

    private boolean hasValue() {
        if (!recordActive || !ensurePageLoaded()) {
            return false;
        }

        if (position == currentRecordStart) {
            return true;
        }

        int[] repLevels = currentPage.repetitionLevels();
        return repLevels != null && repLevels[position] > 0;
    }

    private int repetitionLevel() {
        int[] repLevels = currentPage.repetitionLevels();
        return repLevels != null ? repLevels[position] : 0;
    }

    private int definitionLevel() {
        int[] defLevels = currentPage.definitionLevels();
        return defLevels != null ? defLevels[position] : maxDefinitionLevel;
    }

    private void advance() {
        position++;

        if (currentPage != null && position >= currentPage.size()) {
            if (pageCursor.hasNext()) {
                currentPage = pageCursor.nextPage();
                position = 0;
                if (recordActive && currentPage != null) {
                    int[] repLevels = currentPage.repetitionLevels();
                    if (repLevels != null && repLevels[0] > 0) {
                        currentRecordStart = 0;
                    }
                    else {
                        currentRecordStart = -1;
                    }
                }
            }
        }
    }

    /**
     * Check if there are more records available.
     */
    public boolean hasMore() {
        return !exhausted && ensurePageLoaded();
    }
}
