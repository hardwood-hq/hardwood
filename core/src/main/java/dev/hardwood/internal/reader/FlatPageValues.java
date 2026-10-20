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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import dev.hardwood.internal.encoding.AlpDecoder;
import dev.hardwood.internal.encoding.DeltaBinaryPackedDecoder;
import dev.hardwood.internal.encoding.simd.SimdOperations;
import dev.hardwood.internal.encoding.simd.VectorSupport;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ParquetReadException;

/// The values of a data page of a fixed-width numeric or boolean column, read in place by its
/// fills, and the page's definition levels. The values are the page's own bytes in any encoding
/// but the ids ones (`PLAIN`, `BYTE_STREAM_SPLIT`, `ALP`, `DELTA_BINARY_PACKED`), or ids decoded
/// into scratch (a dictionary's, or `RLE` booleans). `PLAIN` values stored uncompressed in a direct
/// buffer (a slice of a file's mapping, a buffer from an S3 `GET`) are read from that buffer and
/// never copied; any other values are read from an array.
///
/// A fill writes its rows in page order, so the values are read with one forward cursor: [#copy]
/// moves it to the run's first row, counting the non-null values it passes, then writes the run.
/// A page whose mask drops rows passes them on the way. A fill that starts elsewhere reads through
/// a [#view()] of its own.
///
/// The ids and the definition levels live in the scratch the page is decoded with, for as long
/// as the page's fills run.
final class FlatPageValues {

    private static final VarHandle INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle FLOAT = MethodHandles.byteArrayViewVarHandle(float[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle DOUBLE = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.LITTLE_ENDIAN);
    private static final SimdOperations SIMD = VectorSupport.operations();

    private final PhysicalType type;
    /// The page's values, nulls included.
    private final int numValues;
    /// The page's definition levels, `null` when every value is present.
    private final int[] definitionLevels;
    private final int maxDefinitionLevel;
    /// The value section's bytes and where it starts, for every encoding but the ids ones and
    /// `PLAIN` values in a direct buffer; `null` for those.
    private final byte[] plain;
    private final int plainOffset;
    /// `PLAIN` values in a direct buffer: the value section, little-endian, from index `0`; `null`
    /// for values in an array. A heap buffer is read through its array, so that the reads here
    /// see direct buffer classes only and stay inlined.
    private final ByteBuffer plainValues;
    /// Dictionary ids of the non-null values, and the dictionary's values; `null` for `PLAIN`.
    private final int[] ids;
    private final Object dictionaryValues;
    /// `BYTE_STREAM_SPLIT` values: the non-null values, which is the distance between two of a
    /// value's bytes; `0` for any other encoding.
    private final int stride;
    /// `ALP` values: the value section's end and the non-null values, from which each view makes
    /// a decoder of its own; `alpLimit` is `0` for any other encoding.
    private final int alpLimit;
    private final int alpValues;
    /// `ALP` only: this view's decoder, the vector it decoded last and that vector's values.
    private AlpDecoder alp;
    private int alpVector = -1;
    private float[] alpFloats;
    private double[] alpDoubles;
    /// `DELTA_BINARY_PACKED` values: the value section's end, `0` for any other encoding.
    private final int deltaLimit;
    /// `DELTA_BINARY_PACKED` only: this view's decoder and the non-null values it has passed.
    private DeltaBinaryPackedDecoder delta;
    private int deltaPos;

    /// The page row the cursor is at, and the non-null values before it.
    private int row;
    private int nonNull;

    private FlatPageValues(PhysicalType type, int numValues, int[] definitionLevels, int maxDefinitionLevel,
                           byte[] plain, int plainOffset, ByteBuffer plainValues, int[] ids,
                           Object dictionaryValues, int stride, int alpLimit, int alpValues, int deltaLimit) {
        this.type = type;
        this.numValues = numValues;
        this.definitionLevels = definitionLevels;
        this.maxDefinitionLevel = maxDefinitionLevel;
        this.plain = plain;
        this.plainOffset = plainOffset;
        this.plainValues = plainValues;
        this.ids = ids;
        this.dictionaryValues = dictionaryValues;
        this.stride = stride;
        this.alpLimit = alpLimit;
        this.alpValues = alpValues;
        this.deltaLimit = deltaLimit;
    }

    /// `PLAIN` values of `type` in `[offset, limit)` of `data`, which must hold every non-null value.
    ///
    /// @throws ParquetReadException if the value section does not end with the page's values
    static FlatPageValues plain(PhysicalType type, int[] definitionLevels, int maxDefinitionLevel, int numValues,
                                byte[] data, int offset, int limit) {
        checkPlainSize(type, definitionLevels, maxDefinitionLevel, numValues, limit - offset);
        return new FlatPageValues(type, numValues, definitionLevels, maxDefinitionLevel, data, offset, null, null,
                null, 0, 0, 0, 0);
    }

    /// `PLAIN` values of `type` in `values` from its position to its limit, which must hold every
    /// non-null value. The values are read from `values`'s bytes, which must not change while the
    /// page's fills run.
    ///
    /// @throws ParquetReadException if the value section does not end with the page's values
    /// @throws IllegalArgumentException if `values` is neither direct nor backed by an array
    static FlatPageValues plain(PhysicalType type, int[] definitionLevels, int maxDefinitionLevel, int numValues,
                                ByteBuffer values) {
        if (values.hasArray()) {
            int offset = values.arrayOffset() + values.position();
            return plain(type, definitionLevels, maxDefinitionLevel, numValues, values.array(), offset,
                    offset + values.remaining());
        }
        if (!values.isDirect()) {
            throw new IllegalArgumentException("PLAIN values must be in a direct buffer or an array, not in "
                    + values.getClass().getSimpleName());
        }
        checkPlainSize(type, definitionLevels, maxDefinitionLevel, numValues, values.remaining());
        return new FlatPageValues(type, numValues, definitionLevels, maxDefinitionLevel, null, 0,
                values.slice().order(ByteOrder.LITTLE_ENDIAN), null, null, 0, 0, 0, 0);
    }

    /// Checks that a `PLAIN` value section of `length` bytes holds exactly the page's values.
    private static void checkPlainSize(PhysicalType type, int[] definitionLevels, int maxDefinitionLevel,
                                       int numValues, int length) {
        int nonNull = countNonNull(definitionLevels, maxDefinitionLevel, numValues);
        long bytes = type == PhysicalType.BOOLEAN ? (nonNull + 7L) / 8 : (long) nonNull * width(type);
        if (bytes > length) {
            throw new ParquetReadException("Unexpected EOF while reading " + type + " values");
        }
        if (bytes != length) {
            throw new ParquetReadException("PLAIN value section does not end with its " + nonNull + " " + type
                    + " values; leftover bytes: " + (length - bytes));
        }
    }

    /// `DELTA_BINARY_PACKED` values of `type` (`INT32` or `INT64`) from `offset` in `data`, ending
    /// at `limit`. The fill decodes them in order; a view that starts past the page's first value
    /// decodes the values before it and passes over them.
    static FlatPageValues delta(PhysicalType type, int[] definitionLevels, int maxDefinitionLevel, int numValues,
                                byte[] data, int offset, int limit) {
        // The stream header is read and checked here, so a malformed one fails with the page
        new DeltaBinaryPackedDecoder(data, offset, limit);
        return new FlatPageValues(type, numValues, definitionLevels, maxDefinitionLevel, data, offset, null, null,
                null, 0, 0, 0, limit);
    }

    /// `ALP` values of `type` (`FLOAT` or `DOUBLE`) from `offset` in `data`, ending at `limit`. Every
    /// vector header is checked here; the fill decodes one vector at a time.
    ///
    /// @throws ParquetReadException if the value section is malformed
    static FlatPageValues alp(PhysicalType type, int[] definitionLevels, int maxDefinitionLevel, int numValues,
                              byte[] data, int offset, int limit) {
        int nonNull = countNonNull(definitionLevels, maxDefinitionLevel, numValues);
        new AlpDecoder(data, offset, limit, nonNull, type).validate();
        return new FlatPageValues(type, numValues, definitionLevels, maxDefinitionLevel, data, offset, null, null,
                null, 0, limit, nonNull, 0);
    }

    /// `BYTE_STREAM_SPLIT` values of `type` from `offset` in `data`: byte `b` of the `i`th non-null
    /// value at `offset + b * nonNull + i`.
    ///
    /// @throws ParquetReadException if the value section does not hold exactly the page's values
    static FlatPageValues byteStreamSplit(PhysicalType type, int[] definitionLevels, int maxDefinitionLevel,
                                          int numValues, byte[] data, int offset, int limit) {
        int nonNull = countNonNull(definitionLevels, maxDefinitionLevel, numValues);
        long bytes = (long) nonNull * width(type);
        if (bytes != limit - offset) {
            throw new ParquetReadException("BYTE_STREAM_SPLIT value section holds " + (limit - offset)
                    + " bytes, not the " + bytes + " of its " + nonNull + " " + type + " values");
        }
        return new FlatPageValues(type, numValues, definitionLevels, maxDefinitionLevel, data, offset, null, null,
                null, Math.max(1, nonNull), 0, 0, 0);
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
        return new FlatPageValues(type, numValues, definitionLevels, maxDefinitionLevel, null, 0, null, ids, values, 0,
                0, 0, 0);
    }

    private static final boolean[] BOOLEAN_IDS = { false, true };

    /// `RLE` booleans already decoded into `ids`, one `0` or `1` per non-null value.
    static FlatPageValues rleBooleans(int numValues, int[] definitionLevels, int maxDefinitionLevel, int[] ids,
                                      int idCount) {
        for (int i = 0; i < idCount; i++) {
            if ((ids[i] & ~1) != 0) {
                throw new ParquetReadException("Invalid RLE boolean value: " + ids[i]);
            }
        }
        return new FlatPageValues(PhysicalType.BOOLEAN, numValues, definitionLevels, maxDefinitionLevel, null, 0, null,
                ids, BOOLEAN_IDS, 0, 0, 0, 0);
    }

    /// The non-null values among a page's first `numValues`.
    static int countNonNull(int[] definitionLevels, int maxDefinitionLevel, int numValues) {
        return countNonNull(definitionLevels, maxDefinitionLevel, 0, numValues);
    }

    /// The non-null values among a page's rows `[from, to)`, counted by the [SimdOperations] kernel.
    static int countNonNull(int[] definitionLevels, int maxDefinitionLevel, int from, int to) {
        return definitionLevels == null ? to - from : SIMD.countNonNulls(definitionLevels, from, to, maxDefinitionLevel);
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
        return new FlatPageValues(type, numValues, definitionLevels, maxDefinitionLevel, plain, plainOffset,
                plainValues, ids, dictionaryValues, stride, alpLimit, alpValues, deltaLimit);
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
        nonNull += countNonNull(definitionLevels, maxDefinitionLevel, row, pageRow);
        row = pageRow;
    }

    /// Writes the `length` values from the cursor's non-null position, every one present.
    private void copyDense(Object out, int destPos, int length) {
        int src = nonNull;
        if (ids != null) {
            switch (type) {
                case BOOLEAN -> {
                    boolean[] dest = (boolean[]) out;
                    boolean[] dict = (boolean[]) dictionaryValues;
                    for (int i = 0; i < length; i++) {
                        dest[destPos + i] = dict[ids[src + i]];
                    }
                }
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
        if (alpLimit > 0) {
            copyAlp(out, destPos, src, length);
            return;
        }
        if (deltaLimit > 0) {
            DeltaBinaryPackedDecoder decoder = deltaAt(src);
            if (type == PhysicalType.INT64) {
                decoder.readLongs((long[]) out, destPos, length);
            }
            else {
                decoder.readInts((int[]) out, destPos, length);
            }
            deltaPos += length;
            return;
        }
        if (stride > 0 || type == PhysicalType.BOOLEAN) {
            for (int i = 0; i < length; i++) {
                copyOne(out, destPos + i, src + i);
            }
            return;
        }
        switch (type) {
            case INT32 -> plainBytes(src * 4, length * 4).asIntBuffer().get((int[]) out, destPos, length);
            case INT64 -> plainBytes(src * 8, length * 8).asLongBuffer().get((long[]) out, destPos, length);
            case FLOAT -> plainBytes(src * 4, length * 4).asFloatBuffer().get((float[]) out, destPos, length);
            case DOUBLE -> plainBytes(src * 8, length * 8).asDoubleBuffer().get((double[]) out, destPos, length);
            default -> throw new IllegalStateException("Not a fixed-width numeric type: " + type);
        }
    }

    /// The `length` bytes of the `PLAIN` values from byte `offset`, little-endian, for a bulk copy.
    private ByteBuffer plainBytes(int offset, int length) {
        ByteBuffer bytes = plainValues != null
                ? plainValues.slice(offset, length)
                : ByteBuffer.wrap(plain, plainOffset + offset, length);
        return bytes.order(ByteOrder.LITTLE_ENDIAN);
    }

    /// This view's delta decoder, positioned at non-null value `src`.
    private DeltaBinaryPackedDecoder deltaAt(int src) {
        if (delta == null) {
            delta = new DeltaBinaryPackedDecoder(plain, plainOffset, deltaLimit);
        }
        if (src < deltaPos) {
            throw new IllegalStateException("Delta values read out of order: value " + src + " after " + deltaPos);
        }
        delta.skip(src - deltaPos);
        deltaPos = src;
        return delta;
    }

    /// Writes the non-null values `[src, src + length)` of an `ALP` page from the vectors that hold
    /// them, each decoded once into this view's vector buffer.
    private void copyAlp(Object out, int destPos, int src, int length) {
        if (alp == null) {
            alp = new AlpDecoder(plain, plainOffset, alpLimit, alpValues, type);
            if (type == PhysicalType.DOUBLE) {
                alpDoubles = new double[alp.vectorSize()];
            }
            else {
                alpFloats = new float[alp.vectorSize()];
            }
        }
        int vectorSize = alp.vectorSize();
        while (length > 0) {
            int vector = src / vectorSize;
            if (vector != alpVector) {
                alp.decodeVector(vector, alpFloats, alpDoubles);
                alpVector = vector;
            }
            int at = src - vector * vectorSize;
            int n = Math.min(length, vectorSize - at);
            System.arraycopy(alpDoubles != null ? alpDoubles : alpFloats, at, out, destPos, n);
            src += n;
            destPos += n;
            length -= n;
        }
    }

    /// The little-endian bits of `BYTE_STREAM_SPLIT` value `src`, `width` bytes wide.
    private long splitBits(int src, int width) {
        long bits = 0;
        int at = plainOffset + src;
        for (int b = 0; b < width; b++) {
            bits |= (plain[at + b * stride] & 0xFFL) << (8 * b);
        }
        return bits;
    }

    /// Writes the non-null value `src` to `out[dest]`.
    private void copyOne(Object out, int dest, int src) {
        if (ids != null) {
            int id = ids[src];
            switch (type) {
                case BOOLEAN -> ((boolean[]) out)[dest] = ((boolean[]) dictionaryValues)[id];
                case INT32 -> ((int[]) out)[dest] = ((int[]) dictionaryValues)[id];
                case INT64 -> ((long[]) out)[dest] = ((long[]) dictionaryValues)[id];
                case FLOAT -> ((float[]) out)[dest] = ((float[]) dictionaryValues)[id];
                case DOUBLE -> ((double[]) out)[dest] = ((double[]) dictionaryValues)[id];
                default -> throw new IllegalStateException("Not a fixed-width numeric type: " + type);
            }
            return;
        }
        if (alpLimit > 0) {
            copyAlp(out, dest, src, 1);
            return;
        }
        if (deltaLimit > 0) {
            long value = deltaAt(src).readLong();
            deltaPos++;
            if (type == PhysicalType.INT64) {
                ((long[]) out)[dest] = value;
            }
            else {
                ((int[]) out)[dest] = (int) value;
            }
            return;
        }
        if (stride > 0) {
            switch (type) {
                case INT32 -> ((int[]) out)[dest] = (int) splitBits(src, 4);
                case INT64 -> ((long[]) out)[dest] = splitBits(src, 8);
                case FLOAT -> ((float[]) out)[dest] = Float.intBitsToFloat((int) splitBits(src, 4));
                case DOUBLE -> ((double[]) out)[dest] = Double.longBitsToDouble(splitBits(src, 8));
                default -> throw new IllegalStateException("Not a fixed-width numeric type: " + type);
            }
            return;
        }
        if (plainValues != null) {
            copyDirect(out, dest, src);
            return;
        }
        switch (type) {
            case BOOLEAN -> ((boolean[]) out)[dest] = ((plain[plainOffset + (src >>> 3)] >>> (src & 7)) & 1) != 0;
            case INT32 -> ((int[]) out)[dest] = (int) INT.get(plain, plainOffset + src * 4);
            case INT64 -> ((long[]) out)[dest] = (long) LONG.get(plain, plainOffset + src * 8);
            case FLOAT -> ((float[]) out)[dest] = (float) FLOAT.get(plain, plainOffset + src * 4);
            case DOUBLE -> ((double[]) out)[dest] = (double) DOUBLE.get(plain, plainOffset + src * 8);
            default -> throw new IllegalStateException("Not a fixed-width numeric type: " + type);
        }
    }

    /// Writes the non-null `PLAIN` value `src` of a direct buffer to `out[dest]`.
    private void copyDirect(Object out, int dest, int src) {
        switch (type) {
            case BOOLEAN -> ((boolean[]) out)[dest] = ((plainValues.get(src >>> 3) >>> (src & 7)) & 1) != 0;
            case INT32 -> ((int[]) out)[dest] = plainValues.getInt(src * 4);
            case INT64 -> ((long[]) out)[dest] = plainValues.getLong(src * 8);
            case FLOAT -> ((float[]) out)[dest] = plainValues.getFloat(src * 4);
            case DOUBLE -> ((double[]) out)[dest] = plainValues.getDouble(src * 8);
            default -> throw new IllegalStateException("Not a fixed-width numeric type: " + type);
        }
    }
}
