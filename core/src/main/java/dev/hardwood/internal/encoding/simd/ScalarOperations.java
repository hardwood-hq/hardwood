/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding.simd;

import java.util.BitSet;

/// Scalar (non-SIMD) implementation of vectorizable operations.
///
/// This is the fallback implementation used when Vector API is not available
/// (Java 21) or when SIMD is explicitly disabled. The implementations here use
/// loop unrolling and other scalar optimizations for reasonable performance.
public final class ScalarOperations implements SimdOperations {

    @Override
    public int countNonNulls(int[] defLevels, int maxDef) {
        int count0 = 0, count1 = 0, count2 = 0, count3 = 0;
        int i = 0;
        int len = defLevels.length;

        // Process 8 elements at a time with 4 accumulators to minimize dependencies
        for (; i + 8 <= len; i += 8) {
            count0 += (defLevels[i] == maxDef ? 1 : 0) + (defLevels[i + 4] == maxDef ? 1 : 0);
            count1 += (defLevels[i + 1] == maxDef ? 1 : 0) + (defLevels[i + 5] == maxDef ? 1 : 0);
            count2 += (defLevels[i + 2] == maxDef ? 1 : 0) + (defLevels[i + 6] == maxDef ? 1 : 0);
            count3 += (defLevels[i + 3] == maxDef ? 1 : 0) + (defLevels[i + 7] == maxDef ? 1 : 0);
        }

        int count = count0 + count1 + count2 + count3;

        // Handle remaining elements
        for (; i < len; i++) {
            if (defLevels[i] == maxDef) {
                count++;
            }
        }

        return count;
    }

    @Override
    public void markNulls(BitSet nulls, int[] defLevels, int srcPos, int destPos, int count, int maxDefLevel) {
        if (nulls == null) {
            return;
        }
        for (int i = 0; i < count; i++) {
            if (defLevels[srcPos + i] < maxDefLevel) {
                nulls.set(destPos + i);
            }
        }
    }

    @Override
    public int unpackBitWidth1(byte[] data, int dataPos, int[] output, int outPos, int count) {
        int bytesConsumed = 0;

        // Process 8 values per byte
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

    @Override
    public int unpackBitWidthN(byte[] data, int dataPos, int[] output, int outPos, int count, int bitWidth) {
        int mask = (1 << bitWidth) - 1;
        int bytesConsumed = 0;

        // Process 8 values at a time
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
        // Unroll 4x to reduce loop overhead
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
