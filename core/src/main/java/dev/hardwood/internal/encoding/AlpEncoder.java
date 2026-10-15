/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.util.Arrays;

/// Encoder for ALP (Adaptive Lossless floating-Point), the inverse of [AlpDecoder].
///
/// A page is split into vectors of [#VECTOR_SIZE] values. Each vector is scaled to integers by an
/// (exponent, factor) pair, `round(value * 10^exponent * 10^-factor)`, and an integer is kept only
/// where [AlpFormat]'s decode gives back the value's exact bits; every other value (NaN, the
/// infinities, `-0.0`, a value out of the integer range, one with more significant digits than the
/// pair keeps) is stored verbatim as an exception, and its slot takes the vector's first kept
/// integer so that it does not widen the frame. The integers are then stored as their distance
/// above the vector's minimum, bit-packed at the width the largest distance needs.
///
/// **Lossless whatever the pair.** The round trip is checked value by value, so the pair decides
/// only the size of the output. It is chosen in two steps: [#presetDoubles] and [#presetFloats]
/// search every pair on a sample of a column chunk and keep the few that win most often, and each
/// vector then takes whichever of those is smallest on a sample of its own values. A size is
/// estimated as the packed bits plus each exception's position and value. A vector that would
/// pack larger than its values stored as exceptions is stored that way, which bounds every page
/// at [#pageOverheadBits] plus [#maxValueBits] per value.
///
/// **One instance per column.** An encoder writes each page into a buffer it keeps, which grows
/// to the largest page it has encoded and is reused for the next, along with its per-vector
/// scratch arrays; a page costs no allocation once the buffer has grown.
///
/// @see <a href="https://github.com/apache/parquet-format/blob/master/AlpEncoding.md">ALP Encoding</a>
public final class AlpEncoder {

    /// The format's recommended default, `2^10` values.
    static final int LOG_VECTOR_SIZE = 10;
    static final int VECTOR_SIZE = 1 << LOG_VECTOR_SIZE;

    /// Vectors of a chunk sampled for its preset.
    private static final int PRESET_VECTORS = 8;
    /// Values sampled from each of those vectors.
    private static final int PRESET_SAMPLE_SIZE = 64;
    /// Values sampled from a vector to choose among the preset, at most.
    private static final int VECTOR_SAMPLE_SIZE = 256;
    /// Pairs kept in a preset.
    private static final int MAX_PRESET_SIZE = 5;

    /// The pair (0, 0), for a chunk with no sample to search.
    private static final int[] DEFAULT_PRESET = { combination(0, 0) };

    private static final int INITIAL_CAPACITY = 64 << 10;

    private final int valueWidth;
    private byte[] out = new byte[INITIAL_CAPACITY];
    private int pos;

    /// A vector's integers, then the distances above its minimum. Its length is a multiple of
    /// eight, the zeros past a short last vector padding the packer's whole bytes.
    private final long[] deltas = new long[VECTOR_SIZE];
    private final int[] exceptionPositions = new int[VECTOR_SIZE];
    private final long[] exceptionBits = new long[VECTOR_SIZE];

    /// An encoder for the pages of one column.
    ///
    /// @param valueWidth the bytes of a value: 4 for `FLOAT`, 8 for `DOUBLE`
    public AlpEncoder(int valueWidth) {
        if (valueWidth != Float.BYTES && valueWidth != Double.BYTES) {
            throw new IllegalArgumentException("ALP encodes 4- or 8-byte values, not " + valueWidth + "-byte ones");
        }
        this.valueWidth = valueWidth;
    }

    /// The most bits a value takes in an encoded page, for a page cut to charge: an exception's
    /// value and position, which [#cheaperAsExceptions] keeps every vector within, plus one bit.
    /// 1024 values' extra bits cover the offset and header of the vector after them, so a page
    /// is at most [#pageOverheadBits] plus this per value.
    ///
    /// @param valueWidth the bytes of a `FLOAT` or `DOUBLE` value
    public static long maxValueBits(int valueWidth) {
        return (long) (AlpFormat.POSITION_SIZE + valueWidth) * Byte.SIZE + 1;
    }

    /// The bits of a page beyond [#maxValueBits] per value: its header and its first vector's
    /// offset and header.
    ///
    /// @param valueWidth the bytes of a `FLOAT` or `DOUBLE` value
    public static long pageOverheadBits(int valueWidth) {
        return (long) (AlpFormat.HEADER_SIZE + AlpFormat.OFFSET_SIZE + AlpFormat.vectorHeaderSize(valueWidth))
                * Byte.SIZE;
    }

    /// The (exponent, factor) pairs the vectors of `values[from, from + count)` choose among.
    ///
    /// @return up to five pairs, most frequently best first, each as `exponent << 8 | factor`
    public static int[] presetDoubles(double[] values, int from, int count) {
        int numVectors = numVectors(count);
        int sampled = Math.min(PRESET_VECTORS, numVectors);
        int[] wins = new int[combination(AlpFormat.MAX_DOUBLE_EXPONENT + 1, 0)];
        for (int s = 0; s < sampled; s++) {
            int vector = (int) ((long) s * numVectors / sampled);
            int vectorFrom = from + vector * VECTOR_SIZE;
            int vectorCount = Math.min(VECTOR_SIZE, from + count - vectorFrom);
            int stride = stride(vectorCount, PRESET_SAMPLE_SIZE);
            int best = -1;
            long bestBits = Long.MAX_VALUE;
            for (int exponent = 0; exponent <= AlpFormat.MAX_DOUBLE_EXPONENT; exponent++) {
                for (int factor = 0; factor <= exponent; factor++) {
                    long bits = estimateDoubles(values, vectorFrom, vectorCount, stride, exponent, factor, true);
                    // Ascending order, so a tie goes to the larger exponent and factor
                    if (bits >= 0 && bits <= bestBits) {
                        best = combination(exponent, factor);
                        bestBits = bits;
                    }
                }
            }
            if (best >= 0) {
                wins[best]++;
            }
        }
        return preset(wins);
    }

    /// The FLOAT form of [#presetDoubles].
    public static int[] presetFloats(float[] values, int from, int count) {
        int numVectors = numVectors(count);
        int sampled = Math.min(PRESET_VECTORS, numVectors);
        int[] wins = new int[combination(AlpFormat.MAX_FLOAT_EXPONENT + 1, 0)];
        for (int s = 0; s < sampled; s++) {
            int vector = (int) ((long) s * numVectors / sampled);
            int vectorFrom = from + vector * VECTOR_SIZE;
            int vectorCount = Math.min(VECTOR_SIZE, from + count - vectorFrom);
            int stride = stride(vectorCount, PRESET_SAMPLE_SIZE);
            int best = -1;
            long bestBits = Long.MAX_VALUE;
            for (int exponent = 0; exponent <= AlpFormat.MAX_FLOAT_EXPONENT; exponent++) {
                for (int factor = 0; factor <= exponent; factor++) {
                    long bits = estimateFloats(values, vectorFrom, vectorCount, stride, exponent, factor, true);
                    if (bits >= 0 && bits <= bestBits) {
                        best = combination(exponent, factor);
                        bestBits = bits;
                    }
                }
            }
            if (best >= 0) {
                wins[best]++;
            }
        }
        return preset(wins);
    }

    /// Encodes `values[from, from + count)` as one ALP page into [#buffer].
    ///
    /// @param preset the pairs from [#presetDoubles]
    /// @return the page's length, the bytes of [#buffer] it occupies from index 0
    public int encodeDoubles(double[] values, int from, int count, int[] preset) {
        requireWidth(Double.BYTES);
        int numVectors = writePageHeader(count);
        for (int vector = 0; vector < numVectors; vector++) {
            int vectorFrom = from + vector * VECTOR_SIZE;
            int vectorCount = Math.min(VECTOR_SIZE, from + count - vectorFrom);
            writeOffset(vector);
            writeDoubleVector(values, vectorFrom, vectorCount, preset);
        }
        return pos;
    }

    /// The FLOAT form of [#encodeDoubles].
    ///
    /// @param preset the pairs from [#presetFloats]
    public int encodeFloats(float[] values, int from, int count, int[] preset) {
        requireWidth(Float.BYTES);
        int numVectors = writePageHeader(count);
        for (int vector = 0; vector < numVectors; vector++) {
            int vectorFrom = from + vector * VECTOR_SIZE;
            int vectorCount = Math.min(VECTOR_SIZE, from + count - vectorFrom);
            writeOffset(vector);
            writeFloatVector(values, vectorFrom, vectorCount, preset);
        }
        return pos;
    }

    /// The last page encoded, valid up to the length its encode call returned and until the next
    /// call. The array may be replaced by a larger one as pages grow, so it is read after the call.
    public byte[] buffer() {
        return out;
    }

    private void requireWidth(int width) {
        if (valueWidth != width) {
            throw new IllegalStateException("Encoder for " + valueWidth + "-byte values asked to encode "
                    + width + "-byte ones");
        }
    }

    private void writeDoubleVector(double[] values, int from, int count, int[] preset) {
        int combination = choose(preset, values, null, from, count);
        int exponent = combination >>> 8;
        int factor = combination & 0xFF;
        int numExceptions = 0;
        int firstKept = -1;
        for (int i = 0; i < count; i++) {
            double value = values[from + i];
            long encoded = encodeDouble(value, exponent, factor);
            if (roundTrips(value, encoded, exponent, factor)) {
                deltas[i] = encoded;
                if (firstKept < 0) {
                    firstKept = i;
                }
            }
            else {
                exceptionPositions[numExceptions] = i;
                exceptionBits[numExceptions++] = Double.doubleToRawLongBits(value);
            }
        }
        long placeholder = firstKept >= 0 ? deltas[firstKept] : 0;
        for (int j = 0; j < numExceptions; j++) {
            deltas[exceptionPositions[j]] = placeholder;
        }
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (int i = 0; i < count; i++) {
            min = Math.min(min, deltas[i]);
            max = Math.max(max, deltas[i]);
        }
        // Unsigned: the range of a vector spanning the type exceeds the signed maximum
        int bitWidth = Long.SIZE - Long.numberOfLeadingZeros(max - min);
        if (cheaperAsExceptions(count, bitWidth, numExceptions)) {
            for (int i = 0; i < count; i++) {
                exceptionBits[i] = Double.doubleToRawLongBits(values[from + i]);
            }
            writeAllAsExceptions(count);
            return;
        }
        for (int i = 0; i < count; i++) {
            deltas[i] -= min;
        }
        writeVector(exponent, factor, min, bitWidth, count, numExceptions);
    }

    private void writeFloatVector(float[] values, int from, int count, int[] preset) {
        int combination = choose(preset, null, values, from, count);
        int exponent = combination >>> 8;
        int factor = combination & 0xFF;
        int numExceptions = 0;
        int firstKept = -1;
        for (int i = 0; i < count; i++) {
            float value = values[from + i];
            int encoded = encodeFloat(value, exponent, factor);
            if (roundTrips(value, encoded, exponent, factor)) {
                deltas[i] = encoded;
                if (firstKept < 0) {
                    firstKept = i;
                }
            }
            else {
                exceptionPositions[numExceptions] = i;
                exceptionBits[numExceptions++] = Float.floatToRawIntBits(value);
            }
        }
        long placeholder = firstKept >= 0 ? deltas[firstKept] : 0;
        for (int j = 0; j < numExceptions; j++) {
            deltas[exceptionPositions[j]] = placeholder;
        }
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        for (int i = 0; i < count; i++) {
            min = Math.min(min, (int) deltas[i]);
            max = Math.max(max, (int) deltas[i]);
        }
        int bitWidth = Integer.SIZE - Integer.numberOfLeadingZeros(max - min);
        if (cheaperAsExceptions(count, bitWidth, numExceptions)) {
            for (int i = 0; i < count; i++) {
                exceptionBits[i] = Float.floatToRawIntBits(values[from + i]);
            }
            writeAllAsExceptions(count);
            return;
        }
        for (int i = 0; i < count; i++) {
            deltas[i] = Integer.toUnsignedLong((int) deltas[i] - min);
        }
        writeVector(exponent, factor, min, bitWidth, count, numExceptions);
    }

    /// Whether the vector is smaller with every value an exception than packed at `bitWidth`
    /// with `numExceptions` of them, which a few kept values far apart can make it. Falling back
    /// bounds a vector at [#maxValueBits] whatever the pair.
    private boolean cheaperAsExceptions(int count, int bitWidth, int numExceptions) {
        int exceptionBytes = AlpFormat.POSITION_SIZE + valueWidth;
        return AlpFormat.packedBytes(count, bitWidth) + (long) numExceptions * exceptionBytes
                > (long) count * exceptionBytes;
    }

    /// Writes the vector as exceptions alone, their bits already in [#exceptionBits]: no packed
    /// bytes, at the pair (0, 0).
    private void writeAllAsExceptions(int count) {
        for (int i = 0; i < count; i++) {
            exceptionPositions[i] = i;
            deltas[i] = 0;
        }
        writeVector(0, 0, 0, 0, count, count);
    }

    /// The preset pair smallest on a sample of the vector, from whichever of the two arrays is
    /// non-null.
    private static int choose(int[] preset, double[] doubles, float[] floats, int from, int count) {
        if (preset.length == 1) {
            return preset[0];
        }
        int stride = stride(count, VECTOR_SAMPLE_SIZE);
        int best = preset[0];
        long bestBits = Long.MAX_VALUE;
        for (int combination : preset) {
            int exponent = combination >>> 8;
            int factor = combination & 0xFF;
            long bits = doubles != null
                    ? estimateDoubles(doubles, from, count, stride, exponent, factor, false)
                    : estimateFloats(floats, from, count, stride, exponent, factor, false);
            if (bits < bestBits) {
                best = combination;
                bestBits = bits;
            }
        }
        return best;
    }

    /// The bits every `stride`-th value of `values[from, from + count)` takes at the pair: packed
    /// at the width its kept integers span, plus a position and a value per exception. `-1` where
    /// `penalize` is set and fewer than two values are kept, a pair that turns a vector into
    /// exceptions being no candidate for a preset.
    private static long estimateDoubles(double[] values, int from, int count, int stride, int exponent,
            int factor, boolean penalize) {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        int sampled = 0;
        int exceptions = 0;
        for (int i = from; i < from + count; i += stride) {
            sampled++;
            long encoded = encodeDouble(values[i], exponent, factor);
            if (roundTrips(values[i], encoded, exponent, factor)) {
                min = Math.min(min, encoded);
                max = Math.max(max, encoded);
            }
            else {
                exceptions++;
            }
        }
        return estimate(sampled, exceptions, sampled == exceptions ? 0 : max - min, Double.SIZE, penalize);
    }

    private static long estimateFloats(float[] values, int from, int count, int stride, int exponent,
            int factor, boolean penalize) {
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        int sampled = 0;
        int exceptions = 0;
        for (int i = from; i < from + count; i += stride) {
            sampled++;
            int encoded = encodeFloat(values[i], exponent, factor);
            if (roundTrips(values[i], encoded, exponent, factor)) {
                min = Math.min(min, encoded);
                max = Math.max(max, encoded);
            }
            else {
                exceptions++;
            }
        }
        long range = sampled == exceptions ? 0 : Integer.toUnsignedLong(max - min);
        return estimate(sampled, exceptions, range, Float.SIZE, penalize);
    }

    private static long estimate(int sampled, int exceptions, long unsignedRange, int valueBits,
            boolean penalize) {
        if (penalize && sampled - exceptions < 2) {
            return -1;
        }
        int bitWidth = Long.SIZE - Long.numberOfLeadingZeros(unsignedRange);
        return (long) sampled * bitWidth + (long) exceptions * (valueBits + AlpFormat.POSITION_SIZE * Byte.SIZE);
    }

    /// The nearest integer to the scaled value. Java's conversion saturates NaN to zero and an
    /// out-of-range value to the type's bound, both of which fail the round trip.
    private static long encodeDouble(double value, int exponent, int factor) {
        return (long) Math.rint(value * AlpFormat.DOUBLE_POW10[exponent] * AlpFormat.DOUBLE_NEG_POW10[factor]);
    }

    private static int encodeFloat(float value, int exponent, int factor) {
        return (int) Math.rint(value * AlpFormat.FLOAT_POW10[exponent] * AlpFormat.FLOAT_NEG_POW10[factor]);
    }

    /// Whether `encoded` decodes to exactly the bits of `value`, which `-0.0` and NaN never do.
    private static boolean roundTrips(double value, long encoded, int exponent, int factor) {
        return Double.doubleToRawLongBits(AlpFormat.decodeDouble(encoded, exponent, factor))
                == Double.doubleToRawLongBits(value);
    }

    private static boolean roundTrips(float value, int encoded, int exponent, int factor) {
        return Float.floatToRawIntBits(AlpFormat.decodeFloat(encoded, exponent, factor))
                == Float.floatToRawIntBits(value);
    }

    /// The pairs with at least one win, most wins first, a tie going to the larger exponent and
    /// then the larger factor.
    private static int[] preset(int[] wins) {
        int[] ranked = new int[MAX_PRESET_SIZE];
        int size = 0;
        while (size < MAX_PRESET_SIZE) {
            int best = -1;
            for (int combination = wins.length - 1; combination >= 0; combination--) {
                if (wins[combination] > 0 && (best < 0 || wins[combination] > wins[best])) {
                    best = combination;
                }
            }
            if (best < 0) {
                break;
            }
            ranked[size++] = best;
            wins[best] = 0;
        }
        return size == 0 ? DEFAULT_PRESET : Arrays.copyOf(ranked, size);
    }

    private static int combination(int exponent, int factor) {
        return exponent << 8 | factor;
    }

    private static int numVectors(int count) {
        return (count + VECTOR_SIZE - 1) >>> LOG_VECTOR_SIZE;
    }

    /// The step between sampled values, at most `sampleSize` of them. Odd, so that values
    /// alternating in kind (every other one a decimal, say) do not all fall on one side of it.
    private static int stride(int count, int sampleSize) {
        return Math.max(1, (count + sampleSize - 1) / sampleSize) | 1;
    }

    /// Starts a page at the front of the buffer: writes its header and leaves room for the offset
    /// array, returning the vector count.
    private int writePageHeader(int count) {
        int numVectors = numVectors(count);
        pos = 0;
        ensureCapacity(AlpFormat.HEADER_SIZE + numVectors * AlpFormat.OFFSET_SIZE);
        out[pos++] = 0; // compression mode: ALP
        out[pos++] = 0; // integer encoding: frame of reference and bit-packing
        out[pos++] = LOG_VECTOR_SIZE;
        writeLittleEndian(count, Integer.BYTES);
        pos += numVectors * AlpFormat.OFFSET_SIZE;
        return numVectors;
    }

    /// Records where `vector` starts, measured from the start of the offset array.
    private void writeOffset(int vector) {
        int offsetsStart = AlpFormat.HEADER_SIZE;
        int end = pos;
        pos = offsetsStart + vector * AlpFormat.OFFSET_SIZE;
        writeLittleEndian(end - offsetsStart, Integer.BYTES);
        pos = end;
    }

    private void writeVector(int exponent, int factor, long frameOfReference, int bitWidth, int count,
            int numExceptions) {
        int paddedCount = (count + 7) & ~7;
        Arrays.fill(deltas, count, paddedCount, 0);
        ensureCapacity(AlpFormat.vectorHeaderSize(valueWidth) + BitPacker.packedLength(paddedCount, bitWidth)
                + numExceptions * (AlpFormat.POSITION_SIZE + valueWidth));
        out[pos++] = (byte) exponent;
        out[pos++] = (byte) factor;
        writeLittleEndian(numExceptions, AlpFormat.POSITION_SIZE);
        writeLittleEndian(frameOfReference, valueWidth);
        out[pos++] = (byte) bitWidth;
        // The padding packs to zero bits after the vector's own, which the next field overwrites
        BitPacker.pack(deltas, 0, paddedCount, bitWidth, out, pos);
        pos += Math.toIntExact(AlpFormat.packedBytes(count, bitWidth));
        for (int j = 0; j < numExceptions; j++) {
            writeLittleEndian(exceptionPositions[j], AlpFormat.POSITION_SIZE);
        }
        for (int j = 0; j < numExceptions; j++) {
            writeLittleEndian(exceptionBits[j], valueWidth);
        }
    }

    private void writeLittleEndian(long value, int bytes) {
        for (int i = 0; i < bytes; i++) {
            out[pos++] = (byte) (value >>> (i * Byte.SIZE));
        }
    }

    private void ensureCapacity(int bytes) {
        if (out.length - pos < bytes) {
            out = Arrays.copyOf(out, Math.max(out.length * 2, pos + bytes));
        }
    }
}
