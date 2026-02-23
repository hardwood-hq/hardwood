/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import dev.hardwood.schema.ColumnSchema;

/**
 * Typed column data for nested schemas.
 * <p>
 * Stores definition levels, repetition levels, and record offsets needed by
 * {@link NestedBatchIndex} to enable flyweight navigation over hierarchical row structures.
 * </p>
 */
public sealed interface NestedColumnData extends TypedColumnData {

    int maxDefinitionLevel();

    int[] definitionLevels();

    /** Repetition levels, null for columns with maxRepLevel == 0. */
    int[] repetitionLevels();

    /** Record offsets, null for columns with maxRepLevel == 0. */
    int[] recordOffsets();

    @Override
    default boolean isNull(int index) {
        return getDefLevel(index) < maxDefinitionLevel();
    }

    default int getDefLevel(int index) {
        int[] defLevels = definitionLevels();
        return defLevels != null ? defLevels[index] : maxDefinitionLevel();
    }

    default int getRepLevel(int index) {
        int[] repLevels = repetitionLevels();
        return repLevels != null ? repLevels[index] : 0;
    }

    default int getStartOffset(int recordIndex) {
        int[] offsets = recordOffsets();
        return offsets != null ? offsets[recordIndex] : recordIndex;
    }

    default int getValueCount(int recordIndex) {
        int[] offsets = recordOffsets();
        if (offsets == null) {
            return 1; // No repetition: one value per record
        }
        int start = offsets[recordIndex];
        int end = (recordIndex + 1 < recordCount()) ? offsets[recordIndex + 1] : valueCount();
        return end - start;
    }

    record IntColumn(ColumnSchema column, int[] values, int[] definitionLevels, int[] repetitionLevels,
                     int[] recordOffsets, int maxDefinitionLevel, int recordCount) implements NestedColumnData {
        public int get(int index) {
            return values[index];
        }

        @Override
        public Object getValue(int index) {
            return values[index];
        }

        @Override
        public int valueCount() {
            return values.length;
        }
    }

    record LongColumn(ColumnSchema column, long[] values, int[] definitionLevels, int[] repetitionLevels,
                      int[] recordOffsets, int maxDefinitionLevel, int recordCount) implements NestedColumnData {
        public long get(int index) {
            return values[index];
        }

        @Override
        public Object getValue(int index) {
            return values[index];
        }

        @Override
        public int valueCount() {
            return values.length;
        }
    }

    record FloatColumn(ColumnSchema column, float[] values, int[] definitionLevels, int[] repetitionLevels,
                       int[] recordOffsets, int maxDefinitionLevel, int recordCount) implements NestedColumnData {
        public float get(int index) {
            return values[index];
        }

        @Override
        public Object getValue(int index) {
            return values[index];
        }

        @Override
        public int valueCount() {
            return values.length;
        }
    }

    record DoubleColumn(ColumnSchema column, double[] values, int[] definitionLevels, int[] repetitionLevels,
                        int[] recordOffsets, int maxDefinitionLevel, int recordCount) implements NestedColumnData {
        public double get(int index) {
            return values[index];
        }

        @Override
        public Object getValue(int index) {
            return values[index];
        }

        @Override
        public int valueCount() {
            return values.length;
        }
    }

    record BooleanColumn(ColumnSchema column, boolean[] values, int[] definitionLevels, int[] repetitionLevels,
                         int[] recordOffsets, int maxDefinitionLevel, int recordCount) implements NestedColumnData {
        public boolean get(int index) {
            return values[index];
        }

        @Override
        public Object getValue(int index) {
            return values[index];
        }

        @Override
        public int valueCount() {
            return values.length;
        }
    }

    record ByteArrayColumn(ColumnSchema column, byte[][] values, int[] definitionLevels, int[] repetitionLevels,
                           int[] recordOffsets, int maxDefinitionLevel, int recordCount) implements NestedColumnData {
        public byte[] get(int index) {
            return values[index];
        }

        @Override
        public Object getValue(int index) {
            return values[index];
        }

        @Override
        public int valueCount() {
            return values.length;
        }
    }
}
