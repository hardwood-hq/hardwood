/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding.simd;

import java.util.BitSet;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/// SIMD implementation of vectorizable operations using Java Vector API.
///
/// This implementation uses the incubator Vector API available in Java 22+
/// to accelerate common Parquet decoding operations. It automatically uses
/// the preferred vector size for the current CPU (128-bit, 256-bit, or 512-bit).
///
/// Operations fall back to scalar processing for tail elements that don't
/// fill a complete vector.
public final class VectorOperations implements SimdOperations {

    // Use preferred species for best performance on current CPU
    private static final VectorSpecies<Integer> INT_SPECIES = IntVector.SPECIES_PREFERRED;
    private static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_PREFERRED;
    private static final VectorSpecies<Byte> BYTE_UNPACK_SPECIES = byteSpeciesForIntLanes(INT_SPECIES.length());

    private static final int INT_VECTOR_LENGTH = INT_SPECIES.length();
    private static final int LONG_VECTOR_LENGTH = LONG_SPECIES.length();

    // Minimum batch size to use SIMD (amortize loop overhead)
    private static final int MIN_BATCH_SIZE = INT_VECTOR_LENGTH * 2;

    @Override
    public int countNonNulls(int[] defLevels, int maxDef) {
        int len = defLevels.length;

        // Use scalar for small arrays
        if (len < MIN_BATCH_SIZE) {
            return countNonNullsScalar(defLevels, maxDef);
        }

        IntVector maxDefVec = IntVector.broadcast(INT_SPECIES, maxDef);
        int count = 0;
        int i = 0;

        // Main SIMD loop
        for (; i + INT_VECTOR_LENGTH <= len; i += INT_VECTOR_LENGTH) {
            IntVector vec = IntVector.fromArray(INT_SPECIES, defLevels, i);
            VectorMask<Integer> mask = vec.eq(maxDefVec);
            count += mask.trueCount();
        }

        // Scalar tail
        for (; i < len; i++) {
            if (defLevels[i] == maxDef) {
                count++;
            }
        }

        return count;
    }

    private int countNonNullsScalar(int[] defLevels, int maxDef) {
        int count = 0;
        for (int level : defLevels) {
            if (level == maxDef) {
                count++;
            }
        }
        return count;
    }

    @Override
    public void markNulls(BitSet nulls, int[] defLevels, int srcPos, int destPos, int count, int maxDefLevel) {
        if (nulls == null || count == 0) {
            return;
        }

        // Use scalar for small counts
        if (count < MIN_BATCH_SIZE) {
            markNullsScalar(nulls, defLevels, srcPos, destPos, count, maxDefLevel);
            return;
        }

        IntVector maxDefVec = IntVector.broadcast(INT_SPECIES, maxDefLevel);
        int i = 0;

        // Main SIMD loop
        for (; i + INT_VECTOR_LENGTH <= count; i += INT_VECTOR_LENGTH) {
            IntVector vec = IntVector.fromArray(INT_SPECIES, defLevels, srcPos + i);
            VectorMask<Integer> isNull = vec.lt(maxDefVec);

            // Extract mask bits and set null positions
            if (isNull.anyTrue()) {
                long maskBits = isNull.toLong();
                while (maskBits != 0) {
                    int bit = Long.numberOfTrailingZeros(maskBits);
                    nulls.set(destPos + i + bit);
                    maskBits &= maskBits - 1;
                }
            }
        }

        // Scalar tail
        for (; i < count; i++) {
            if (defLevels[srcPos + i] < maxDefLevel) {
                nulls.set(destPos + i);
            }
        }
    }

    private void markNullsScalar(BitSet nulls, int[] defLevels, int srcPos, int destPos, int count, int maxDefLevel) {
        for (int i = 0; i < count; i++) {
            if (defLevels[srcPos + i] < maxDefLevel) {
                nulls.set(destPos + i);
            }
        }
    }

    @Override
    public int unpackBitWidth1(byte[] data, int dataPos, int[] output, int outPos, int count) {
        int bytesConsumed = 0;

        while (count >= 8 && dataPos < data.length) {
            int b = data[dataPos++] & 0xFF;
            output[outPos] = b & 1;
            output[outPos + 1] = (b >> 1) & 1;
            output[outPos + 2] = (b >> 2) & 1;
            output[outPos + 3] = (b >> 3) & 1;
            output[outPos + 4] = (b >> 4) & 1;
            output[outPos + 5] = (b >> 5) & 1;
            output[outPos + 6] = (b >> 6) & 1;
            output[outPos + 7] = (b >> 7) & 1;
            outPos += 8;
            count -= 8;
            bytesConsumed++;
        }

        return bytesConsumed;
    }

    private static VectorSpecies<Byte> byteSpeciesForIntLanes(int intLanes) {
        if (intLanes <= ByteVector.SPECIES_64.length()) {
            return ByteVector.SPECIES_64;
        }
        if (intLanes <= ByteVector.SPECIES_128.length()) {
            return ByteVector.SPECIES_128;
        }
        if (intLanes <= ByteVector.SPECIES_256.length()) {
            return ByteVector.SPECIES_256;
        }
        return ByteVector.SPECIES_512;
    }

    @Override
    public int unpackBitWidthN(byte[] data, int dataPos, int[] output, int outPos, int count, int bitWidth) {
        int mask = (1 << bitWidth) - 1;
        int bytesConsumed = 0;

        if (bitWidth == 8) {
            while (count >= INT_VECTOR_LENGTH && dataPos + INT_VECTOR_LENGTH <= data.length) {
                IntVector bytes = (IntVector) ByteVector.fromArray(BYTE_UNPACK_SPECIES, data, dataPos)
                        .convertShape(VectorOperators.ZERO_EXTEND_B2I, INT_SPECIES, 0);
                bytes.intoArray(output, outPos);

                outPos += INT_VECTOR_LENGTH;
                count -= INT_VECTOR_LENGTH;
                dataPos += INT_VECTOR_LENGTH;
                bytesConsumed += INT_VECTOR_LENGTH;
            }
        }

        // Process 8 values at a time using long read
        while (count >= 8 && dataPos + bitWidth <= data.length) {
            long bits = dataPos + 8 <= data.length
                    ? readLittleEndianLong(data, dataPos)
                    : readLittleEndianBytes(data, dataPos, bitWidth);

            output[outPos] = (int) (bits & mask);
            bits >>>= bitWidth;
            output[outPos + 1] = (int) (bits & mask);
            bits >>>= bitWidth;
            output[outPos + 2] = (int) (bits & mask);
            bits >>>= bitWidth;
            output[outPos + 3] = (int) (bits & mask);
            bits >>>= bitWidth;
            output[outPos + 4] = (int) (bits & mask);
            bits >>>= bitWidth;
            output[outPos + 5] = (int) (bits & mask);
            bits >>>= bitWidth;
            output[outPos + 6] = (int) (bits & mask);
            bits >>>= bitWidth;
            output[outPos + 7] = (int) (bits & mask);

            outPos += 8;
            count -= 8;
            dataPos += bitWidth;
            bytesConsumed += bitWidth;
        }

        return bytesConsumed;
    }

    private static long readLittleEndianLong(byte[] data, int dataPos) {
        return ((long) data[dataPos] & 0xFF)
                | (((long) data[dataPos + 1] & 0xFF) << 8)
                | (((long) data[dataPos + 2] & 0xFF) << 16)
                | (((long) data[dataPos + 3] & 0xFF) << 24)
                | (((long) data[dataPos + 4] & 0xFF) << 32)
                | (((long) data[dataPos + 5] & 0xFF) << 40)
                | (((long) data[dataPos + 6] & 0xFF) << 48)
                | (((long) data[dataPos + 7] & 0xFF) << 56);
    }

    private static long readLittleEndianBytes(byte[] data, int dataPos, int byteCount) {
        long bits = 0;
        for (int i = 0; i < byteCount; i++) {
            bits |= ((long) (data[dataPos + i] & 0xFF)) << (i * 8);
        }
        return bits;
    }

    @Override
    public void applyDictionaryLongs(long[] output, long[] dict, int[] indices, int count) {
        if (count < MIN_BATCH_SIZE) {
            applyDictionaryLongsScalar(output, dict, indices, count);
            return;
        }

        // Dictionary lookups with gather are challenging in Vector API
        // since true gather requires indices to be in a vector.
        // For now, use unrolled scalar which is actually quite efficient
        // for dictionary lookups due to L1 cache hits on small dictionaries.
        applyDictionaryLongsScalar(output, dict, indices, count);
    }

    private void applyDictionaryLongsScalar(long[] output, long[] dict, int[] indices, int count) {
        int i = 0;
        for (; i + 4 <= count; i += 4) {
            output[i] = dict[indices[i]];
            output[i + 1] = dict[indices[i + 1]];
            output[i + 2] = dict[indices[i + 2]];
            output[i + 3] = dict[indices[i + 3]];
        }
        for (; i < count; i++) {
            output[i] = dict[indices[i]];
        }
    }

    @Override
    public void applyDictionaryDoubles(double[] output, double[] dict, int[] indices, int count) {
        // Similar to longs - unrolled scalar is efficient for cache-friendly access
        int i = 0;
        for (; i + 4 <= count; i += 4) {
            output[i] = dict[indices[i]];
            output[i + 1] = dict[indices[i + 1]];
            output[i + 2] = dict[indices[i + 2]];
            output[i + 3] = dict[indices[i + 3]];
        }
        for (; i < count; i++) {
            output[i] = dict[indices[i]];
        }
    }

    @Override
    public void applyDictionaryInts(int[] output, int[] dict, int[] indices, int count) {
        if (count < MIN_BATCH_SIZE) {
            applyDictionaryIntsScalar(output, dict, indices, count);
            return;
        }

        // For small dictionaries (common case), try SIMD gather-like pattern
        // using rearrange operations if dictionary fits in vector
        if (dict.length <= INT_VECTOR_LENGTH) {
            applyDictionaryIntsSmallDict(output, dict, indices, count);
            return;
        }

        applyDictionaryIntsScalar(output, dict, indices, count);
    }

    private void applyDictionaryIntsSmallDict(int[] output, int[] dict, int[] indices, int count) {
        // For dictionaries smaller than vector length, we can use rearrange
        // This is a simplified approach - full implementation would use proper shuffles

        // Pad dictionary to vector length
        int[] paddedDict = new int[INT_VECTOR_LENGTH];
        System.arraycopy(dict, 0, paddedDict, 0, dict.length);
        IntVector dictVec = IntVector.fromArray(INT_SPECIES, paddedDict, 0);

        int i = 0;
        // Process vector-length values at a time using lane extraction
        // Note: This is a simplified approach; true gather would be more complex
        for (; i + INT_VECTOR_LENGTH <= count; i += INT_VECTOR_LENGTH) {
            // Load indices and look up each one
            // (Vector API doesn't have simple gather, so we do lane-by-lane)
            for (int j = 0; j < INT_VECTOR_LENGTH; j++) {
                output[i + j] = dict[indices[i + j]];
            }
        }

        // Scalar tail
        for (; i < count; i++) {
            output[i] = dict[indices[i]];
        }
    }

    private void applyDictionaryIntsScalar(int[] output, int[] dict, int[] indices, int count) {
        int i = 0;
        for (; i + 4 <= count; i += 4) {
            output[i] = dict[indices[i]];
            output[i + 1] = dict[indices[i + 1]];
            output[i + 2] = dict[indices[i + 2]];
            output[i + 3] = dict[indices[i + 3]];
        }
        for (; i < count; i++) {
            output[i] = dict[indices[i]];
        }
    }

    @Override
    public void applyDictionaryFloats(float[] output, float[] dict, int[] indices, int count) {
        int i = 0;
        for (; i + 4 <= count; i += 4) {
            output[i] = dict[indices[i]];
            output[i + 1] = dict[indices[i + 1]];
            output[i + 2] = dict[indices[i + 2]];
            output[i + 3] = dict[indices[i + 3]];
        }
        for (; i < count; i++) {
            output[i] = dict[indices[i]];
        }
    }
}
