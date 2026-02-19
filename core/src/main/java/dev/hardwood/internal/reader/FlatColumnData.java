/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.BitSet;

import dev.hardwood.schema.ColumnSchema;

/**
 * Typed column data for flat schemas (no nested structures).
 * <p>
 * Uses array copying from pages to provide consistent batch sizes across columns.
 * A pre-computed {@link BitSet} provides fast null checks without storing definition levels.
 * </p>
 */
public sealed interface FlatColumnData extends TypedColumnData {

    /** Pre-computed null flags for fast null checks. Null if column is non-nullable. */
    BitSet nulls();

    @Override
    default boolean isNull(int index) {
        BitSet n = nulls();
        return n != null && n.get(index);
    }

    record IntColumn(ColumnSchema column, int[] values, BitSet nulls, int recordCount) implements FlatColumnData {
        public int get(int index) {
            return values[index];
        }

        @Override
        public Object getValue(int index) {
            return get(index);
        }

        @Override
        public int valueCount() {
            return recordCount;
        }
    }

    record LongColumn(ColumnSchema column, long[] values, BitSet nulls, int recordCount) implements FlatColumnData {
        public long get(int index) {
            return values[index];
        }

        @Override
        public Object getValue(int index) {
            return get(index);
        }

        @Override
        public int valueCount() {
            return recordCount;
        }
    }

    record FloatColumn(ColumnSchema column, float[] values, BitSet nulls, int recordCount) implements FlatColumnData {
        public float get(int index) {
            return values[index];
        }

        @Override
        public Object getValue(int index) {
            return get(index);
        }

        @Override
        public int valueCount() {
            return recordCount;
        }
    }

    record DoubleColumn(ColumnSchema column, double[] values, BitSet nulls, int recordCount) implements FlatColumnData {
        public double get(int index) {
            return values[index];
        }

        @Override
        public Object getValue(int index) {
            return get(index);
        }

        @Override
        public int valueCount() {
            return recordCount;
        }
    }

    record BooleanColumn(ColumnSchema column, boolean[] values, BitSet nulls, int recordCount) implements FlatColumnData {
        public boolean get(int index) {
            return values[index];
        }

        @Override
        public Object getValue(int index) {
            return get(index);
        }

        @Override
        public int valueCount() {
            return recordCount;
        }
    }

    record ByteArrayColumn(ColumnSchema column, byte[][] values, BitSet nulls, int recordCount) implements FlatColumnData {
        public byte[] get(int index) {
            return values[index];
        }

        @Override
        public Object getValue(int index) {
            return get(index);
        }

        @Override
        public int valueCount() {
            return recordCount;
        }
    }
}
