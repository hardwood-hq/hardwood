/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import java.util.Arrays;
import java.util.BitSet;

import dev.morling.hardwood.schema.ColumnSchema;

/**
 * Reads column values from pages into TypedColumnData batches.
 *
 * <p>Batch computation is done synchronously when requested. Page-level prefetching
 * (handled by {@link PageCursor}) ensures decoded pages are ready, while column-level
 * parallelism (handled by the row reader) ensures all columns compute in parallel.</p>
 *
 * <p>For flat schemas, {@link #readBatch(int)} copies directly into typed primitive
 * arrays. For nested schemas, it tracks repetition/definition levels.</p>
 */
public class ColumnValueIterator {

    private final PageCursor pageCursor;
    private final ColumnSchema column;
    private final int maxDefinitionLevel;
    private final boolean flatSchema;

    private Page currentPage;
    private int position;
    private int currentRecordStart;
    private boolean recordActive;
    private boolean exhausted = false;

    // Reusable arrays for flat batch computation (avoids per-batch allocation)
    private long[] reusableLongArray;
    private double[] reusableDoubleArray;
    private int[] reusableIntArray;
    private float[] reusableFloatArray;
    private boolean[] reusableBooleanArray;
    private byte[][] reusableByteArrayArray;
    private BitSet reusableNulls;

    public ColumnValueIterator(PageCursor pageCursor, ColumnSchema column, boolean flatSchema) {
        this.pageCursor = pageCursor;
        this.column = column;
        this.maxDefinitionLevel = column.maxDefinitionLevel();
        this.flatSchema = flatSchema;
    }

    // ==================== Batch Reading ====================

    /**
     * Read values for up to {@code maxRecords} records into typed arrays.
     *
     * <p>Page-level prefetching ensures decoded pages are typically ready.
     * Column-level parallelism is handled by the row reader calling this
     * method on all columns concurrently.</p>
     *
     * <p>If a shared stop count is configured and this column approaches a file
     * transition, it will update the shared counter to signal other columns to
     * stop at the same point.</p>
     *
     * @param maxRecords maximum number of records to read
     * @return typed column data containing values, levels, and record boundaries
     */
    public TypedColumnData readBatch(int maxRecords) {
        if (exhausted) {
            return emptyTypedColumnData();
        }

        TypedColumnData result = flatSchema ? computeFlatBatch(maxRecords) : computeNestedBatch(maxRecords);

        if (result.recordCount() == 0) {
            exhausted = true;
        }

        return result;
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

    private BitSet reuseOrCreateNulls(int maxDefLevel, int maxRecords) {
        if (maxDefLevel <= 0) {
            return null;
        }
        if (reusableNulls == null) {
            reusableNulls = new BitSet(maxRecords);
        }
        else {
            reusableNulls.clear();
        }
        return reusableNulls;
    }

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
        if (reusableIntArray == null || reusableIntArray.length < maxRecords) {
            reusableIntArray = new int[maxRecords];
        }
        int[] values = reusableIntArray;
        BitSet nulls = reuseOrCreateNulls(maxDefLevel, maxRecords);

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

        return new FlatColumnData.IntColumn(column, values, nulls, recordCount);
    }

    private TypedColumnData computeFlatLong(int maxRecords, int maxDefLevel) {
        if (reusableLongArray == null || reusableLongArray.length < maxRecords) {
            reusableLongArray = new long[maxRecords];
        }
        long[] values = reusableLongArray;
        BitSet nulls = reuseOrCreateNulls(maxDefLevel, maxRecords);

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

        return new FlatColumnData.LongColumn(column, values, nulls, recordCount);
    }

    private TypedColumnData computeFlatFloat(int maxRecords, int maxDefLevel) {
        if (reusableFloatArray == null || reusableFloatArray.length < maxRecords) {
            reusableFloatArray = new float[maxRecords];
        }
        float[] values = reusableFloatArray;
        BitSet nulls = reuseOrCreateNulls(maxDefLevel, maxRecords);

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

        return new FlatColumnData.FloatColumn(column, values, nulls, recordCount);
    }

    private TypedColumnData computeFlatDouble(int maxRecords, int maxDefLevel) {
        if (reusableDoubleArray == null || reusableDoubleArray.length < maxRecords) {
            reusableDoubleArray = new double[maxRecords];
        }
        double[] values = reusableDoubleArray;
        BitSet nulls = reuseOrCreateNulls(maxDefLevel, maxRecords);

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

        return new FlatColumnData.DoubleColumn(column, values, nulls, recordCount);
    }

    private TypedColumnData computeFlatBoolean(int maxRecords, int maxDefLevel) {
        if (reusableBooleanArray == null || reusableBooleanArray.length < maxRecords) {
            reusableBooleanArray = new boolean[maxRecords];
        }
        boolean[] values = reusableBooleanArray;
        BitSet nulls = reuseOrCreateNulls(maxDefLevel, maxRecords);

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

        return new FlatColumnData.BooleanColumn(column, values, nulls, recordCount);
    }

    private TypedColumnData computeFlatByteArray(int maxRecords, int maxDefLevel) {
        if (reusableByteArrayArray == null || reusableByteArrayArray.length < maxRecords) {
            reusableByteArrayArray = new byte[maxRecords][];
        }
        byte[][] values = reusableByteArrayArray;
        BitSet nulls = reuseOrCreateNulls(maxDefLevel, maxRecords);

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
