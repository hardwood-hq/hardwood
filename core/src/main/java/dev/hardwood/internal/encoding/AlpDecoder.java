/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ParquetReadException;

/// Decoder for ALP (Adaptive Lossless floating-Point) encoding.
///
/// A page holds the present values only, in independent vectors of `2^log_vector_size` values
/// (the last one possibly shorter):
/// ```text
/// HEADER:  compression_mode (u8) | integer_encoding (u8) | log_vector_size (u8) | num_elements (i32)
/// OFFSETS: one u32 per vector, measured from the start of the offset array
/// VECTOR:  exponent (u8) | factor (u8) | num_exceptions (u16) | frame_of_reference (i32 / i64) | bit_width (u8)
///          | packed deltas | exception positions (u16 each) | exception values (raw IEEE 754 bits)
/// ```
///
/// A value is `(frame_of_reference + delta) * 10^factor * 10^-exponent`, in the arithmetic
/// [AlpFormat] fixes, after which each exception overwrites the value at its position. Both the
/// multiplication order and the constants are normative: any other arithmetic decodes some values
/// one ulp off.
///
/// Supports FLOAT and DOUBLE physical types.
///
/// @see <a href="https://github.com/apache/parquet-format/blob/master/AlpEncoding.md">ALP Encoding</a>
public class AlpDecoder {

    private static final VarHandle LONG_LE =
            MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle INT_LE =
            MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle SHORT_LE =
            MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.LITTLE_ENDIAN);

    private final byte[] data;
    private final int limit;
    /// Bytes per value: 4 for FLOAT, 8 for DOUBLE. Also the width of the frame of reference.
    private final int valueWidth;
    private final int vectorSize;
    private final int numElements;
    private final int numVectors;
    private final int offsetsStart;

    // The vector being decoded, as set by readVectorHeader
    private int exponent;
    private int factor;
    private int numExceptions;
    private long frameOfReference;
    private int bitWidth;
    private int packedStart;
    private int exceptionsStart;

    private byte[] tailBytes;

    /// Reads and checks the page header and that the offset array fits the page. The vectors are
    /// checked as they are decoded.
    ///
    /// @param data the page bytes to decode from
    /// @param offset the position in `data` the ALP header starts at
    /// @param limit the position in `data` the page's values end at, exclusive; a buffer reused
    ///        across pages runs on past it with bytes of an earlier page
    /// @param numNonNullValues the number of values the page holds, which the header must agree with
    /// @param type the physical type of the values, `FLOAT` or `DOUBLE`
    public AlpDecoder(byte[] data, int offset, int limit, int numNonNullValues, PhysicalType type) {
        this.data = data;
        this.limit = limit;
        this.valueWidth = switch (type) {
            case FLOAT -> Float.BYTES;
            case DOUBLE -> Double.BYTES;
            default -> throw new IllegalArgumentException("ALP is not defined over " + type);
        };
        if (limit - offset < AlpFormat.HEADER_SIZE) {
            throw new ParquetReadException("Unexpected EOF reading the ALP page header: expected "
                    + AlpFormat.HEADER_SIZE + " bytes, got " + (limit - offset));
        }
        // The format reserves both fields for variants it has yet to define, so another value is a
        // gap in this release rather than a malformed file.
        int compressionMode = data[offset] & 0xFF;
        if (compressionMode != 0) {
            throw new UnsupportedOperationException("ALP compression mode not yet supported: " + compressionMode);
        }
        int integerEncoding = data[offset + 1] & 0xFF;
        if (integerEncoding != 0) {
            throw new UnsupportedOperationException("ALP integer encoding not yet supported: " + integerEncoding);
        }
        int logVectorSize = data[offset + 2] & 0xFF;
        if (logVectorSize < AlpFormat.MIN_LOG_VECTOR_SIZE || logVectorSize > AlpFormat.MAX_LOG_VECTOR_SIZE) {
            throw new ParquetReadException("Invalid ALP log vector size: " + logVectorSize
                    + ". Must be between " + AlpFormat.MIN_LOG_VECTOR_SIZE + " and " + AlpFormat.MAX_LOG_VECTOR_SIZE);
        }
        this.numElements = (int) INT_LE.get(data, offset + 3);
        if (numElements != numNonNullValues) {
            throw new ParquetReadException("ALP page header counts " + numElements
                    + " values, but the page has " + numNonNullValues + " non-null values");
        }
        this.vectorSize = 1 << logVectorSize;
        this.numVectors = Math.toIntExact(((long) numElements + vectorSize - 1) >>> logVectorSize);
        this.offsetsStart = offset + AlpFormat.HEADER_SIZE;
        long offsetsBytes = (long) numVectors * AlpFormat.OFFSET_SIZE;
        if (offsetsBytes > limit - offsetsStart) {
            throw new ParquetReadException("Unexpected EOF reading the ALP offset array: expected "
                    + offsetsBytes + " bytes, got " + (limit - offsetsStart));
        }
    }

    /// The values of each vector but the last.
    public int vectorSize() {
        return vectorSize;
    }

    /// Checks every vector's header, its exception positions, and that the vectors end the page,
    /// without decoding a value, so that a page read later vector by vector ([#decodeVector])
    /// fails when it is read.
    ///
    /// @throws ParquetReadException if a vector is malformed or the vectors do not end the page
    public void validate() {
        long expectedOffset = (long) numVectors * AlpFormat.OFFSET_SIZE;
        for (int vector = 0; vector < numVectors; vector++) {
            int count = Math.min(vectorSize, numElements - vector * vectorSize);
            expectedOffset = readVectorHeader(vector, expectedOffset, count);
            for (int j = 0; j < numExceptions; j++) {
                exceptionPosition(j, count, vector);
            }
        }
        long leftover = limit - (offsetsStart + expectedOffset);
        if (leftover != 0) {
            throw new ParquetReadException("ALP value section does not end with its " + numVectors
                    + " vectors; leftover bytes: " + leftover);
        }
    }

    /// Decodes vector `vector` of a [#validate]d page onto the front of whichever of the two arrays
    /// is non-null, which holds at least [#vectorSize()] values.
    ///
    /// @return the values the vector holds
    public int decodeVector(int vector, float[] floats, double[] doubles) {
        int first = vector * vectorSize;
        int count = Math.min(vectorSize, numElements - first);
        long storedOffset = Integer.toUnsignedLong(
                (int) INT_LE.get(data, offsetsStart + vector * AlpFormat.OFFSET_SIZE));
        readVectorHeader(vector, storedOffset, count);
        if (doubles != null) {
            decodeDoubles(doubles, 0, count);
            patchDoubleExceptions(doubles, 0, count, vector);
        }
        else {
            decodeFloats(floats, 0, count);
            patchFloatExceptions(floats, 0, count, vector);
        }
        return count;
    }

    /// Reads and checks the header of `vector`, which holds `count` values, and sets the fields
    /// the decode and patch steps read.
    ///
    /// @param expectedOffset where the vector must start, relative to the offset array
    /// @return where the vector ends, relative to the offset array
    private long readVectorHeader(int vector, long expectedOffset, int count) {
        int offsetAt = offsetsStart + vector * AlpFormat.OFFSET_SIZE;
        long storedOffset = Integer.toUnsignedLong((int) INT_LE.get(data, offsetAt));
        if (storedOffset != expectedOffset) {
            throw new ParquetReadException("ALP vector " + vector + " starts at offset " + storedOffset
                    + ", expected " + expectedOffset);
        }
        long start = offsetsStart + expectedOffset;
        int headerSize = AlpFormat.vectorHeaderSize(valueWidth);
        if (headerSize > limit - start) {
            throw new ParquetReadException("Unexpected EOF reading the header of ALP vector " + vector
                    + ": expected " + headerSize + " bytes, got " + (limit - start));
        }
        int pos = (int) start;
        exponent = data[pos] & 0xFF;
        factor = data[pos + 1] & 0xFF;
        numExceptions = (short) SHORT_LE.get(data, pos + 2) & 0xFFFF;
        frameOfReference = valueWidth == Double.BYTES
                ? (long) LONG_LE.get(data, pos + AlpFormat.ALP_INFO_SIZE)
                : (int) INT_LE.get(data, pos + AlpFormat.ALP_INFO_SIZE);
        bitWidth = data[pos + AlpFormat.ALP_INFO_SIZE + valueWidth] & 0xFF;

        int maxExponent = valueWidth == Double.BYTES ? AlpFormat.MAX_DOUBLE_EXPONENT : AlpFormat.MAX_FLOAT_EXPONENT;
        if (exponent > maxExponent) {
            throw new ParquetReadException("Invalid ALP exponent in vector " + vector + ": " + exponent
                    + ". Must be between 0 and " + maxExponent);
        }
        if (factor > exponent) {
            throw new ParquetReadException("Invalid ALP factor in vector " + vector + ": " + factor
                    + ". Must be between 0 and the exponent " + exponent);
        }
        int maxBitWidth = valueWidth * Byte.SIZE;
        if (bitWidth > maxBitWidth) {
            throw new ParquetReadException("Invalid ALP bit width in vector " + vector + ": " + bitWidth
                    + ". Must be between 0 and " + maxBitWidth);
        }
        if (numExceptions > count) {
            throw new ParquetReadException("ALP vector " + vector + " has " + numExceptions
                    + " exceptions, more than its " + count + " values");
        }

        long packedBytes = AlpFormat.packedBytes(count, bitWidth);
        long size = headerSize + packedBytes + (long) numExceptions * (AlpFormat.POSITION_SIZE + valueWidth);
        if (size > limit - start) {
            throw new ParquetReadException("Unexpected EOF reading ALP vector " + vector + ": expected "
                    + size + " bytes, got " + (limit - start));
        }
        packedStart = pos + headerSize;
        exceptionsStart = Math.toIntExact(packedStart + packedBytes);
        return expectedOffset + size;
    }

    /// Unpacks the vector's deltas and decodes them in one pass.
    ///
    /// Value `i` starts at bit `i * bitWidth` and is at most 64 bits long, so beginning at most 7
    /// bits into a byte it reaches at most 71: two overlapping reads cover every width, the
    /// doubled shift on the second one shifting the byte out entirely at a bit offset of zero. The
    /// same technique as `DeltaBinaryPackedDecoder`, which fuses a prefix sum in place of the
    /// decimal decode.
    private void decodeDoubles(double[] output, int first, int count) {
        double scale = AlpFormat.DOUBLE_POW10[factor];
        double unscale = AlpFormat.DOUBLE_NEG_POW10[exponent];
        long frame = frameOfReference;
        if (bitWidth == 0) {
            Arrays.fill(output, first, first + count, (double) frame * scale * unscale);
            return;
        }
        byte[] src = packedSource(count);
        int base = src == data ? packedStart : 0;
        long mask = bitWidth == Long.SIZE ? -1L : (1L << bitWidth) - 1;
        int bitPos = 0;
        for (int i = 0; i < count; i++) {
            int byteOffset = base + (bitPos >>> 3);
            int shift = bitPos & 7;
            long low = (long) LONG_LE.get(src, byteOffset);
            long high = src[byteOffset + Long.BYTES] & 0xFFL;
            long delta = ((low >>> shift) | ((high << (63 - shift)) << 1)) & mask;
            // Wrapping addition: the encoder computes the delta in unsigned arithmetic
            output[first + i] = (double) (frame + delta) * scale * unscale;
            bitPos += bitWidth;
        }
    }

    /// The FLOAT form of [#decodeDoubles], in binary32 throughout. A delta is at most 32 bits, so
    /// starting at most 7 bits into a byte it fits one eight-byte read.
    private void decodeFloats(float[] output, int first, int count) {
        float scale = AlpFormat.FLOAT_POW10[factor];
        float unscale = AlpFormat.FLOAT_NEG_POW10[exponent];
        int frame = (int) frameOfReference;
        if (bitWidth == 0) {
            Arrays.fill(output, first, first + count, (float) frame * scale * unscale);
            return;
        }
        byte[] src = packedSource(count);
        int base = src == data ? packedStart : 0;
        long mask = (1L << bitWidth) - 1;
        int bitPos = 0;
        for (int i = 0; i < count; i++) {
            int byteOffset = base + (bitPos >>> 3);
            int shift = bitPos & 7;
            int delta = (int) (((long) LONG_LE.get(src, byteOffset) >>> shift) & mask);
            output[first + i] = (float) (frame + delta) * scale * unscale;
            bitPos += bitWidth;
        }
    }

    /// The array the vector's packed deltas are read from: the page itself where the last read
    /// stays inside it, otherwise a zero-padded copy of what remains. The reads run past the
    /// packed bytes into the exceptions or the next vector, whose bits the mask discards.
    private byte[] packedSource(int count) {
        int bytes = ((count - 1) * bitWidth) / 8 + Long.BYTES + 1;
        if (packedStart + bytes <= limit) {
            return data;
        }
        int available = Math.min(limit - packedStart, bytes);
        if (tailBytes == null || tailBytes.length < bytes) {
            tailBytes = new byte[bytes];
        }
        System.arraycopy(data, packedStart, tailBytes, 0, available);
        Arrays.fill(tailBytes, available, tailBytes.length, (byte) 0);
        return tailBytes;
    }

    /// Overwrites each exception's position with its stored bits, NaN payloads included.
    private void patchDoubleExceptions(double[] output, int first, int count, int vector) {
        int valuesStart = exceptionsStart + numExceptions * AlpFormat.POSITION_SIZE;
        for (int j = 0; j < numExceptions; j++) {
            int position = exceptionPosition(j, count, vector);
            long bits = (long) LONG_LE.get(data, valuesStart + j * Double.BYTES);
            output[first + position] = Double.longBitsToDouble(bits);
        }
    }

    private void patchFloatExceptions(float[] output, int first, int count, int vector) {
        int valuesStart = exceptionsStart + numExceptions * AlpFormat.POSITION_SIZE;
        for (int j = 0; j < numExceptions; j++) {
            int position = exceptionPosition(j, count, vector);
            int bits = (int) INT_LE.get(data, valuesStart + j * Float.BYTES);
            output[first + position] = Float.intBitsToFloat(bits);
        }
    }

    private int exceptionPosition(int exception, int count, int vector) {
        int position = (short) SHORT_LE.get(data, exceptionsStart + exception * AlpFormat.POSITION_SIZE) & 0xFFFF;
        if (position >= count) {
            throw new ParquetReadException("ALP exception position " + position + " in vector " + vector
                    + " is outside its " + count + " values");
        }
        return position;
    }
}
