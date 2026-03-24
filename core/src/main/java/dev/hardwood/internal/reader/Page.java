/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

/// Sealed interface for typed column page data with primitive arrays.
///
/// This eliminates boxing overhead by storing values directly in typed arrays.
/// All access is via typed accessors - there is no generic getObject() method.
///
/// Implementations correspond to Parquet physical types:
///
/// - [BooleanPage] - BOOLEAN
/// - [IntPage] - INT32
/// - [LongPage] - INT64
/// - [FloatPage] - FLOAT
/// - [DoublePage] - DOUBLE
/// - [ByteArrayPage] - BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96
public sealed interface Page {

    int size();

    int maxDefinitionLevel();

    int[] definitionLevels();

    int[] repetitionLevels();

    default boolean isNull(int index) {
        int[] defLevels = definitionLevels();
        if (defLevels == null) {
            return false;
        }
        return defLevels[index] < maxDefinitionLevel();
    }

    record BooleanPage(boolean[] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel, int size)
            implements Page {
        public boolean get(int index) {
            return values[index];
        }
    }

    record IntPage(int[] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel, int size)
            implements Page {
        public int get(int index) {
            return values[index];
        }
    }

    record LongPage(long[] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel, int size)
            implements Page {
        public long get(int index) {
            return values[index];
        }
    }

    record FloatPage(float[] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel, int size)
            implements Page {
        public float get(int index) {
            return values[index];
        }
    }

    record DoublePage(double[] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel, int size)
            implements Page {
        public double get(int index) {
            return values[index];
        }
    }

    record ByteArrayPage(byte[][] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel, int size)
            implements Page {
        public byte[] get(int index) {
            return values[index];
        }
    }
}
