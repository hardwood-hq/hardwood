/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Binary;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnBatch;
import dev.hardwood.writer.StructBuilder;

/// One writable physical type, in the three forms the write-path interop gate needs it: how to
/// declare a column of it, how to fill a [ColumnBatch] column with its values, and how to read
/// one back out of a parquet-java [Group].
///
/// Values are a pure function of an ordinal, so a case picks its distinct count and the values
/// follow. The low ordinals are the type's boundary values — signed extremes, infinities, `NaN`,
/// signed zeros, empty and high-byte binaries — so a small-cardinality case exercises them; from
/// there the values are the ordinal itself, which keeps a high-cardinality case cheap to
/// describe.
enum TypeFixture {

    BOOLEAN(PhysicalType.BOOLEAN, null, false),
    INT32(PhysicalType.INT32, null, true),
    INT64(PhysicalType.INT64, null, true),
    FLOAT(PhysicalType.FLOAT, null, true),
    DOUBLE(PhysicalType.DOUBLE, null, true),
    BYTE_ARRAY(PhysicalType.BYTE_ARRAY, null, true),
    FIXED_LEN_BYTE_ARRAY(PhysicalType.FIXED_LEN_BYTE_ARRAY, 8, true);

    private static final int[] INT32_BOUNDARIES = {
            0, 1, -1, Integer.MAX_VALUE, Integer.MIN_VALUE, 42, -100_000, 123_456 };

    private static final long[] INT64_BOUNDARIES = {
            0L, 1L, -1L, Long.MAX_VALUE, Long.MIN_VALUE, 42L, -1_000_000_000_000L, 987_654_321L };

    private static final float[] FLOAT_BOUNDARIES = {
            0.0f, 1.0f, -1.0f, Float.MAX_VALUE, -Float.MAX_VALUE, Float.MIN_VALUE,
            Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, -0.0f, Float.NaN };

    private static final double[] DOUBLE_BOUNDARIES = {
            0.0, 1.0, -1.0, Double.MAX_VALUE, -Double.MAX_VALUE, Double.MIN_VALUE,
            Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, -0.0, Double.NaN };

    /// `BYTE_ARRAY` boundaries: an ordinary value first (so a single-entry dictionary case is the
    /// #901 shape and nothing else), then the empty value and one whose bytes are above `0x7f`,
    /// which orders differently under a signed comparison than the unsigned one the format
    /// mandates for an unannotated binary.
    private static final byte[][] BINARY_BOUNDARIES = {
            "hardwood".getBytes(StandardCharsets.UTF_8),
            new byte[0],
            "a".getBytes(StandardCharsets.UTF_8),
            { (byte) 0xff, (byte) 0xff } };

    /// `FIXED_LEN_BYTE_ARRAY` boundaries at the declared length of 8, spanning the unsigned range
    /// and both sides of the sign bit of the leading byte.
    private static final byte[][] FIXED_BOUNDARIES = {
            fixedOf((byte) 0x00), fixedOf((byte) 0xff), fixedOf((byte) 0x80), fixedOf((byte) 0x7f) };

    private final PhysicalType physicalType;
    private final Integer typeLength;
    private final boolean dictionaryCapable;

    TypeFixture(PhysicalType physicalType, Integer typeLength, boolean dictionaryCapable) {
        this.physicalType = physicalType;
        this.typeLength = typeLength;
        this.dictionaryCapable = dictionaryCapable;
    }

    /// The physical type this fixture writes, which is what decides the encodings it may carry.
    PhysicalType physicalType() {
        return physicalType;
    }

    /// Whether the writer dictionary-encodes this type at all. `BOOLEAN` never is — a two-value
    /// dictionary cannot beat the bit-packed `PLAIN` layout — so a dictionary case degenerates to
    /// a `PLAIN` one for it.
    boolean dictionaryCapable() {
        return dictionaryCapable;
    }

    /// Adds a column of this type to a schema being built.
    FileSchema.Builder declare(FileSchema.Builder builder, String column, RepetitionType repetition) {
        return typeLength == null
                ? builder.addColumn(column, physicalType, repetition)
                : builder.addColumn(column, physicalType, repetition, c -> c.typeLength(typeLength));
    }

    /// Fills a batch column with the case's values, through the mask-less setter when no row is
    /// null and the plain-mask setter otherwise.
    void set(ColumnBatch batch, String column, InteropCase testCase) {
        boolean[] nulls = testCase.nulls();
        int rows = testCase.rows();
        switch (this) {
            case BOOLEAN -> {
                boolean[] values = new boolean[rows];
                for (int r = 0; r < rows; r++) {
                    values[r] = (boolean) value(testCase.ordinal(r));
                }
                if (nulls == null) {
                    batch.booleans(column, values);
                }
                else {
                    batch.booleans(column, values, nulls);
                }
            }
            case INT32 -> {
                int[] values = new int[rows];
                for (int r = 0; r < rows; r++) {
                    values[r] = (int) value(testCase.ordinal(r));
                }
                if (nulls == null) {
                    batch.ints(column, values);
                }
                else {
                    batch.ints(column, values, nulls);
                }
            }
            case INT64 -> {
                long[] values = new long[rows];
                for (int r = 0; r < rows; r++) {
                    values[r] = (long) value(testCase.ordinal(r));
                }
                if (nulls == null) {
                    batch.longs(column, values);
                }
                else {
                    batch.longs(column, values, nulls);
                }
            }
            case FLOAT -> {
                float[] values = new float[rows];
                for (int r = 0; r < rows; r++) {
                    values[r] = (float) value(testCase.ordinal(r));
                }
                if (nulls == null) {
                    batch.floats(column, values);
                }
                else {
                    batch.floats(column, values, nulls);
                }
            }
            case DOUBLE -> {
                double[] values = new double[rows];
                for (int r = 0; r < rows; r++) {
                    values[r] = (double) value(testCase.ordinal(r));
                }
                if (nulls == null) {
                    batch.doubles(column, values);
                }
                else {
                    batch.doubles(column, values, nulls);
                }
            }
            case BYTE_ARRAY -> {
                byte[][] values = binaryValues(testCase);
                if (nulls == null) {
                    batch.bytes(column, values);
                }
                else {
                    batch.bytes(column, values, nulls);
                }
            }
            case FIXED_LEN_BYTE_ARRAY -> {
                byte[][] values = binaryValues(testCase);
                if (nulls == null) {
                    batch.fixed(column, values);
                }
                else {
                    batch.fixed(column, values, nulls);
                }
            }
        }
    }

    /// Sets this column's value for one record through the row-oriented API, so the gate covers
    /// the files that layer produces as well as the ones the columnar API does.
    void set(StructBuilder row, String column, int ordinal) {
        switch (this) {
            case BOOLEAN -> row.setBoolean(column, (boolean) value(ordinal));
            case INT32 -> row.setInt(column, (int) value(ordinal));
            case INT64 -> row.setLong(column, (long) value(ordinal));
            case FLOAT -> row.setFloat(column, (float) value(ordinal));
            case DOUBLE -> row.setDouble(column, (double) value(ordinal));
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> row.setBinary(column, (byte[]) value(ordinal));
        }
    }

    private byte[][] binaryValues(InteropCase testCase) {
        byte[][] values = new byte[testCase.rows()][];
        for (int r = 0; r < values.length; r++) {
            values[r] = (byte[]) value(testCase.ordinal(r));
        }
        return values;
    }

    /// The value at an ordinal, boxed, in the representation the batch setter and the [Group]
    /// accessor both produce, so the two are directly comparable.
    Object value(int ordinal) {
        return switch (this) {
            case BOOLEAN -> (ordinal & 1) == 0;
            case INT32 -> ordinal < INT32_BOUNDARIES.length ? INT32_BOUNDARIES[ordinal] : ordinal;
            case INT64 -> ordinal < INT64_BOUNDARIES.length ? INT64_BOUNDARIES[ordinal] : (long) ordinal;
            case FLOAT -> ordinal < FLOAT_BOUNDARIES.length ? FLOAT_BOUNDARIES[ordinal] : (float) ordinal;
            case DOUBLE -> ordinal < DOUBLE_BOUNDARIES.length ? DOUBLE_BOUNDARIES[ordinal] : (double) ordinal;
            case BYTE_ARRAY -> ordinal < BINARY_BOUNDARIES.length
                    ? BINARY_BOUNDARIES[ordinal]
                    : ("v" + ordinal).getBytes(StandardCharsets.UTF_8);
            case FIXED_LEN_BYTE_ARRAY -> ordinal < FIXED_BOUNDARIES.length
                    ? FIXED_BOUNDARIES[ordinal]
                    : ByteBuffer.allocate(Long.BYTES).putLong(ordinal).array();
        };
    }

    /// The value at an ordinal in the representation parquet-java's statistics compare in, whose
    /// natural order is the type-defined sort order the format specifies: signed for the numeric
    /// types, unsigned lexicographical for the binary ones, which [Binary#compareTo] implements.
    Comparable<?> statisticsValue(int ordinal) {
        Object value = value(ordinal);
        return value instanceof byte[] bytes ? Binary.fromConstantByteArray(bytes) : (Comparable<?>) value;
    }

    /// Reads this column's single value out of a parquet-java row.
    Object read(Group row, int field) {
        return switch (this) {
            case BOOLEAN -> row.getBoolean(field, 0);
            case INT32 -> row.getInteger(field, 0);
            case INT64 -> row.getLong(field, 0);
            case FLOAT -> row.getFloat(field, 0);
            case DOUBLE -> row.getDouble(field, 0);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> row.getBinary(field, 0).getBytes();
        };
    }

    /// The `min` the case's present values imply, or `null` if it has none. `NaN` is excluded:
    /// the type-defined order for the floating-point types leaves it out of the bounds, so a
    /// column holding one still gets bounds over the rest.
    Comparable<?> expectedMin(InteropCase testCase) {
        return normalizeZero(bound(testCase, true), true);
    }

    /// The `max` the case's present values imply, or `null` if it has none.
    ///
    /// @see #expectedMin
    Comparable<?> expectedMax(InteropCase testCase) {
        return normalizeZero(bound(testCase, false), false);
    }

    /// Applies the format's sign normalization of a floating-point zero bound, so a reader's
    /// `[min, max]` test is correct for either signed zero: a zero `min` is written as `-0.0` and
    /// a zero `max` as `+0.0`, whichever zero the data held.
    private static Comparable<?> normalizeZero(Comparable<?> bound, boolean min) {
        if (bound instanceof Float value && value == 0.0f) {
            return min ? -0.0f : 0.0f;
        }
        if (bound instanceof Double value && value == 0.0) {
            return min ? -0.0 : 0.0;
        }
        return bound;
    }

    @SuppressWarnings("unchecked")
    private Comparable<Object> bound(InteropCase testCase, boolean min) {
        Comparable<Object> best = null;
        for (int r = 0; r < testCase.rows(); r++) {
            if (testCase.isNull(r)) {
                continue;
            }
            int ordinal = testCase.ordinal(r);
            if (isNaN(value(ordinal))) {
                continue;
            }
            Comparable<Object> candidate = (Comparable<Object>) statisticsValue(ordinal);
            if (best == null || (min ? candidate.compareTo(best) < 0 : candidate.compareTo(best) > 0)) {
                best = candidate;
            }
        }
        return best;
    }

    private static boolean isNaN(Object value) {
        return value instanceof Float f && Float.isNaN(f) || value instanceof Double d && Double.isNaN(d);
    }

    private static byte[] fixedOf(byte fill) {
        byte[] value = new byte[8];
        Arrays.fill(value, fill);
        return value;
    }
}
