/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ParquetReadException;

/// The values of a decompressed flat data page, read in place by its fill: `PLAIN` bytes or the
/// dictionary ids of a fixed-width column, and the page's definition levels.
///
/// The fill writes the page's rows into its segments in page order, so the values are read with
/// one forward cursor: [#copy] moves it to the run's first row, counting the non-null values it
/// passes, then writes the run. A page whose mask drops rows passes them on the way.
///
/// The ids and the definition levels live in the decode thread's scratch, which the page holds
/// for the length of its task.
final class FlatPageValues {

    private static final VarHandle INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle FLOAT = MethodHandles.byteArrayViewVarHandle(float[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle DOUBLE = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.LITTLE_ENDIAN);

    private final PhysicalType type;
    /// The page's values, nulls included.
    private final int numValues;
    /// The page's definition levels, `null` when every value is present.
    private final int[] definitionLevels;
    private final int maxDefinitionLevel;
    /// `PLAIN` values: the decompressed page and where its values start; `null` for ids.
    private final byte[] plain;
    private final int plainOffset;
    /// Dictionary ids of the non-null values, and the dictionary's values; `null` for `PLAIN`.
    private final int[] ids;
    private final Object dictionaryValues;

    /// The page row the cursor is at, and the non-null values before it.
    private int row;
    private int nonNull;

    private FlatPageValues(PhysicalType type, int numValues, int[] definitionLevels, int maxDefinitionLevel,
                           byte[] plain, int plainOffset, int[] ids, Object dictionaryValues) {
        this.type = type;
        this.numValues = numValues;
        this.definitionLevels = definitionLevels;
        this.maxDefinitionLevel = maxDefinitionLevel;
        this.plain = plain;
        this.plainOffset = plainOffset;
        this.ids = ids;
        this.dictionaryValues = dictionaryValues;
    }

    /// `PLAIN` values of `type` from `offset` in `data`, which must hold every non-null value.
    ///
    /// @throws ParquetReadException if the value section does not end with the page's values
    static FlatPageValues plain(PhysicalType type, int[] definitionLevels, int maxDefinitionLevel, int numValues,
                                byte[] data, int offset, int limit) {
        int nonNull = countNonNull(definitionLevels, maxDefinitionLevel, numValues);
        long bytes = (long) nonNull * width(type);
        if (bytes > limit - offset) {
            throw new ParquetReadException("Unexpected EOF while reading " + type + " values");
        }
        if (bytes != limit - offset) {
            throw new ParquetReadException("PLAIN value section does not end with its " + nonNull + " " + type
                    + " values; leftover bytes: " + (limit - offset - bytes));
        }
        return new FlatPageValues(type, numValues, definitionLevels, maxDefinitionLevel, data, offset, null, null);
    }

    /// Dictionary ids already decoded into `ids`, one per non-null value, and the values of the
    /// dictionary they refer to.
    ///
    /// @throws ArrayIndexOutOfBoundsException if an id lies outside the dictionary
    static FlatPageValues dictionary(PhysicalType type, int numValues, int[] definitionLevels,
                                     int maxDefinitionLevel, int[] ids, int idCount, Dictionary dictionary) {
        Object values = switch (dictionary) {
            case Dictionary.IntDictionary d when type == PhysicalType.INT32 -> d.values();
            case Dictionary.LongDictionary d when type == PhysicalType.INT64 -> d.values();
            case Dictionary.FloatDictionary d when type == PhysicalType.FLOAT -> d.values();
            case Dictionary.DoubleDictionary d when type == PhysicalType.DOUBLE -> d.values();
            default -> throw new ParquetReadException("A " + type + " page refers to a dictionary of another type");
        };
        int size = dictionary.size();
        for (int i = 0; i < idCount; i++) {
            int id = ids[i];
            if (id < 0 || id >= size) {
                throw new ArrayIndexOutOfBoundsException("Dictionary index " + id
                        + " out of bounds for a dictionary of " + size + " entries");
            }
        }
        return new FlatPageValues(type, numValues, definitionLevels, maxDefinitionLevel, null, 0, ids, values);
    }

    /// The non-null values among a page's first `numValues`.
    static int countNonNull(int[] definitionLevels, int maxDefinitionLevel, int numValues) {
        if (definitionLevels == null) {
            return numValues;
        }
        int count = 0;
        for (int i = 0; i < numValues; i++) {
            if (definitionLevels[i] == maxDefinitionLevel) {
                count++;
            }
        }
        return count;
    }

    private static int width(PhysicalType type) {
        return switch (type) {
            case INT32, FLOAT -> 4;
            case INT64, DOUBLE -> 8;
            default -> throw new IllegalArgumentException("Not a fixed-width numeric type: " + type);
        };
    }

    /// A reader of the same values with a cursor of its own at the page's first row, so that two
    /// fills can read one page at once.
    FlatPageValues view() {
        return new FlatPageValues(type, numValues, definitionLevels, maxDefinitionLevel, plain, plainOffset, ids,
                dictionaryValues);
    }

    /// The page's values, nulls included.
    int size() {
        return numValues;
    }

    int[] definitionLevels() {
        return definitionLevels;
    }

    /// Writes page rows `[pageRow, pageRow + length)` to `out` from `destPos`. A null leaves its
    /// element as it is. Rows must be asked for in increasing order.
    void copy(int pageRow, Object out, int destPos, int length) {
        skipTo(pageRow);
        if (definitionLevels == null) {
            copyDense(out, destPos, length);
            nonNull += length;
        }
        else {
            for (int i = 0; i < length; i++) {
                if (definitionLevels[pageRow + i] == maxDefinitionLevel) {
                    copyOne(out, destPos + i, nonNull++);
                }
            }
        }
        row = pageRow + length;
    }

    private void skipTo(int pageRow) {
        if (pageRow < row) {
            throw new IllegalStateException("Page rows read out of order: row " + pageRow + " after " + row);
        }
        nonNull += definitionLevels == null
                ? pageRow - row
                : countNonNullBetween(row, pageRow);
        row = pageRow;
    }

    private int countNonNullBetween(int from, int to) {
        int count = 0;
        for (int i = from; i < to; i++) {
            if (definitionLevels[i] == maxDefinitionLevel) {
                count++;
            }
        }
        return count;
    }

    /// Writes the `length` values from the cursor's non-null position, every one present.
    private void copyDense(Object out, int destPos, int length) {
        int src = nonNull;
        if (ids != null) {
            switch (type) {
                case INT32 -> {
                    int[] dest = (int[]) out;
                    int[] dict = (int[]) dictionaryValues;
                    for (int i = 0; i < length; i++) {
                        dest[destPos + i] = dict[ids[src + i]];
                    }
                }
                case INT64 -> {
                    long[] dest = (long[]) out;
                    long[] dict = (long[]) dictionaryValues;
                    for (int i = 0; i < length; i++) {
                        dest[destPos + i] = dict[ids[src + i]];
                    }
                }
                case FLOAT -> {
                    float[] dest = (float[]) out;
                    float[] dict = (float[]) dictionaryValues;
                    for (int i = 0; i < length; i++) {
                        dest[destPos + i] = dict[ids[src + i]];
                    }
                }
                case DOUBLE -> {
                    double[] dest = (double[]) out;
                    double[] dict = (double[]) dictionaryValues;
                    for (int i = 0; i < length; i++) {
                        dest[destPos + i] = dict[ids[src + i]];
                    }
                }
                default -> throw new IllegalStateException("Not a fixed-width numeric type: " + type);
            }
            return;
        }
        switch (type) {
            case INT32 -> {
                int[] dest = (int[]) out;
                int at = plainOffset + src * 4;
                for (int i = 0; i < length; i++) {
                    dest[destPos + i] = (int) INT.get(plain, at + i * 4);
                }
            }
            case INT64 -> {
                long[] dest = (long[]) out;
                int at = plainOffset + src * 8;
                for (int i = 0; i < length; i++) {
                    dest[destPos + i] = (long) LONG.get(plain, at + i * 8);
                }
            }
            case FLOAT -> {
                float[] dest = (float[]) out;
                int at = plainOffset + src * 4;
                for (int i = 0; i < length; i++) {
                    dest[destPos + i] = (float) FLOAT.get(plain, at + i * 4);
                }
            }
            case DOUBLE -> {
                double[] dest = (double[]) out;
                int at = plainOffset + src * 8;
                for (int i = 0; i < length; i++) {
                    dest[destPos + i] = (double) DOUBLE.get(plain, at + i * 8);
                }
            }
            default -> throw new IllegalStateException("Not a fixed-width numeric type: " + type);
        }
    }

    /// Writes the non-null value `src` to `out[dest]`.
    private void copyOne(Object out, int dest, int src) {
        if (ids != null) {
            int id = ids[src];
            switch (type) {
                case INT32 -> ((int[]) out)[dest] = ((int[]) dictionaryValues)[id];
                case INT64 -> ((long[]) out)[dest] = ((long[]) dictionaryValues)[id];
                case FLOAT -> ((float[]) out)[dest] = ((float[]) dictionaryValues)[id];
                case DOUBLE -> ((double[]) out)[dest] = ((double[]) dictionaryValues)[id];
                default -> throw new IllegalStateException("Not a fixed-width numeric type: " + type);
            }
            return;
        }
        switch (type) {
            case INT32 -> ((int[]) out)[dest] = (int) INT.get(plain, plainOffset + src * 4);
            case INT64 -> ((long[]) out)[dest] = (long) LONG.get(plain, plainOffset + src * 8);
            case FLOAT -> ((float[]) out)[dest] = (float) FLOAT.get(plain, plainOffset + src * 4);
            case DOUBLE -> ((double[]) out)[dest] = (double) DOUBLE.get(plain, plainOffset + src * 8);
            default -> throw new IllegalStateException("Not a fixed-width numeric type: " + type);
        }
    }
}
