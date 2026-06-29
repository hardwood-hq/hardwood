/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.util.Arrays;

import dev.hardwood.internal.encoding.simd.SimdOperations;
import dev.hardwood.internal.encoding.simd.VectorSupport;

/// Decoder for RLE/Bit-Packing Hybrid encoding.
/// Used primarily for definition/repetition levels and dictionary indices.
public class RleBitPackingHybridDecoder {

    private static final SimdOperations SIMD_OPS = VectorSupport.operations();

    // Thread-local reusable buffer for temporary index arrays in dictionary decoding.
    // Safe because the executor is a fixed platform thread pool, so buffers persist
    // across page decodes on the same thread with zero synchronization overhead.
    private static final ThreadLocal<int[]> TEMP_INDICES = new ThreadLocal<>();

    private final byte[] data;
    private final int dataEnd;
    private final int bitWidth;
    private final int bitMask;
    private int pos;

    // Run state
    private int currentValue;
    private int remainingInRun;
    private boolean isRleRun;

    // Bit buffer for packed values
    private long bitBuffer;
    private int bitsInBuffer;

    public RleBitPackingHybridDecoder(byte[] data, int bitWidth) {
        this(data, 0, data.length, bitWidth);
    }

    public RleBitPackingHybridDecoder(byte[] data, int offset, int length, int bitWidth) {
        if (bitWidth < 0 || bitWidth > 32) {
            throw new IllegalArgumentException("Invalid RLE bit width: " + bitWidth
                    + ". Must be between 0 and 32");
        }
        this.data = data;
        this.dataEnd = offset + length;
        this.pos = offset;
        this.bitWidth = bitWidth;
        this.bitMask = (bitWidth == 0) ? 0 : (1 << bitWidth) - 1;
    }

    public void readInts(int[] buffer, int offset, int count) {
        if (bitWidth == 0 || pos >= dataEnd) {
            return;
        }

        int outPos = offset;
        int remaining = count;

        while (remaining > 0) {
            if (remainingInRun == 0) {
                readNextRun();
                if (remainingInRun == 0) {
                    break;
                }
            }

            int toRead = Math.min(remaining, remainingInRun);

            if (isRleRun) {
                Arrays.fill(buffer, outPos, outPos + toRead, currentValue);
            }
            else {
                decodeBitPacked(buffer, outPos, toRead);
            }

            outPos += toRead;
            remainingInRun -= toRead;
            remaining -= toRead;
        }

        if (remaining > 0) {
            throw new IllegalStateException("Insufficient RLE/Bit-Packing data: decoded "
                    + (count - remaining) + " of " + count + " requested values");
        }
    }

    // Type-specific dictionary lookups to avoid boxing

    public void readDictionaryLongs(long[] output, long[] dictionary, int[] defLevels, int maxDef) {
        int[] indices = decodeIndices(output.length, defLevels, maxDef);
        applyDictionary(output, dictionary, indices, defLevels, maxDef);
    }

    public void readDictionaryDoubles(double[] output, double[] dictionary, int[] defLevels, int maxDef) {
        int[] indices = decodeIndices(output.length, defLevels, maxDef);
        applyDictionary(output, dictionary, indices, defLevels, maxDef);
    }

    public void readDictionaryInts(int[] output, int[] dictionary, int[] defLevels, int maxDef) {
        int[] indices = decodeIndices(output.length, defLevels, maxDef);
        applyDictionary(output, dictionary, indices, defLevels, maxDef);
    }

    public void readDictionaryFloats(float[] output, float[] dictionary, int[] defLevels, int maxDef) {
        int[] indices = decodeIndices(output.length, defLevels, maxDef);
        applyDictionary(output, dictionary, indices, defLevels, maxDef);
    }

    public void readDictionaryByteArrays(byte[][] output, byte[][] dictionary, int[] defLevels, int maxDef) {
        int[] indices = decodeIndices(output.length, defLevels, maxDef);
        applyDictionary(output, dictionary, indices, defLevels, maxDef);
    }

    public void readBooleans(boolean[] output, int[] defLevels, int maxDef) {
        int[] indices = decodeIndices(output.length, defLevels, maxDef);
        if (defLevels == null) {
            for (int i = 0; i < output.length; i++) {
                output[i] = indices[i] != 0;
            }
        }
        else {
            int idx = 0;
            for (int i = 0; i < output.length; i++) {
                if (defLevels[i] == maxDef) {
                    output[i] = indices[idx++] != 0;
                }
            }
        }
    }

    private int[] decodeIndices(int len, int[] defLevels, int maxDef) {
        int count = defLevels == null ? len : countNonNulls(defLevels, maxDef);
        int[] indices = borrowTemp(count);
        if (bitWidth == 0) {
            Arrays.fill(indices, 0, count, 0);
        }
        else {
            readInts(indices, 0, count);
        }
        return indices;
    }

    private static int[] borrowTemp(int minSize) {
        int[] buf = TEMP_INDICES.get();
        if (buf == null || buf.length < minSize) {
            buf = new int[minSize];
            TEMP_INDICES.set(buf);
        }
        return buf;
    }

    private void applyDictionary(long[] output, long[] dict, int[] indices, int[] defLevels, int maxDef) {
        if (defLevels == null) {
            SIMD_OPS.applyDictionaryLongs(output, dict, indices, output.length);
        }
        else {
            int idx = 0;
            for (int i = 0; i < output.length; i++) {
                if (defLevels[i] == maxDef) {
                    output[i] = dict[indices[idx++]];
                }
            }
        }
    }

    private void applyDictionary(double[] output, double[] dict, int[] indices, int[] defLevels, int maxDef) {
        if (defLevels == null) {
            SIMD_OPS.applyDictionaryDoubles(output, dict, indices, output.length);
        }
        else {
            int idx = 0;
            for (int i = 0; i < output.length; i++) {
                if (defLevels[i] == maxDef) {
                    output[i] = dict[indices[idx++]];
                }
            }
        }
    }

    private void applyDictionary(int[] output, int[] dict, int[] indices, int[] defLevels, int maxDef) {
        if (defLevels == null) {
            SIMD_OPS.applyDictionaryInts(output, dict, indices, output.length);
        }
        else {
            int idx = 0;
            for (int i = 0; i < output.length; i++) {
                if (defLevels[i] == maxDef) {
                    output[i] = dict[indices[idx++]];
                }
            }
        }
    }

    private void applyDictionary(float[] output, float[] dict, int[] indices, int[] defLevels, int maxDef) {
        if (defLevels == null) {
            SIMD_OPS.applyDictionaryFloats(output, dict, indices, output.length);
        }
        else {
            int idx = 0;
            for (int i = 0; i < output.length; i++) {
                if (defLevels[i] == maxDef) {
                    output[i] = dict[indices[idx++]];
                }
            }
        }
    }

    private void applyDictionary(byte[][] output, byte[][] dict, int[] indices, int[] defLevels, int maxDef) {
        if (defLevels == null) {
            for (int i = 0; i < output.length; i++) {
                output[i] = dict[indices[i]];
            }
        }
        else {
            int idx = 0;
            for (int i = 0; i < output.length; i++) {
                if (defLevels[i] == maxDef) {
                    output[i] = dict[indices[idx++]];
                }
            }
        }
    }

    private static int countNonNulls(int[] defLevels, int maxDef) {
        return SIMD_OPS.countNonNulls(defLevels, maxDef);
    }

    private void readNextRun() {
        if (pos >= dataEnd) {
            remainingInRun = 0;
            return;
        }

        long header = readUnsignedVarInt();

        if ((header & 1) == 1) {
            // Bit-packed: header >> 1 = number of 8-value groups
            remainingInRun = (int) (header >> 1) * 8;
            isRleRun = false;
        }
        else {
            // RLE: header >> 1 = repeat count
            remainingInRun = (int) (header >> 1);
            currentValue = readRleValue();
            isRleRun = true;
        }
    }

    private int readRleValue() {
        int bytesNeeded = (bitWidth + 7) / 8;
        int value = 0;
        for (int i = 0; i < bytesNeeded && pos < dataEnd; i++) {
            value |= (data[pos++] & 0xFF) << (i * 8);
        }
        return value & bitMask;
    }

    /// Batch decode bit-packed values. Optimized paths for common bit widths.
    private void decodeBitPacked(int[] output, int outPos, int count) {
        final int width = bitWidth;
        final int mask = bitMask;

        // Drain leftover bits first
        while (bitsInBuffer >= width && count > 0) {
            output[outPos++] = (int) (bitBuffer & mask);
            bitBuffer >>>= width;
            bitsInBuffer -= width;
            count--;
        }

        // Bulk decode complete 8-value groups only while byte-aligned. Tails
        // stay on the bit-buffer path below so split reads preserve state.
        if (bitsInBuffer == 0 && width == 1) {
            int groups = Math.min(count / 8, dataEnd - pos);
            if (groups > 0) {
                int values = groups * 8;
                int bytesConsumed = SIMD_OPS.unpackBitWidth1(data, pos, output, outPos, values);
                pos += bytesConsumed;
                outPos += bytesConsumed * 8;
                count -= bytesConsumed * 8;
            }
        }
        else if (bitsInBuffer == 0 && width <= 8) {
            int groups = Math.min(count / 8, (dataEnd - pos) / width);
            if (groups > 0) {
                int bytesConsumed = SIMD_OPS.unpackBitWidthN(data, pos, output, outPos, groups * 8, width);
                pos += bytesConsumed;
                outPos += groups * 8;
                count -= groups * 8;
            }
        }

        // Handle remaining values
        while (count > 0) {
            while (bitsInBuffer < width && pos < dataEnd) {
                bitBuffer |= ((long) (data[pos++] & 0xFF)) << bitsInBuffer;
                bitsInBuffer += 8;
            }
            if (bitsInBuffer < width) {
                break;
            }
            output[outPos++] = (int) (bitBuffer & mask);
            bitBuffer >>>= width;
            bitsInBuffer -= width;
            count--;
        }
    }

    private long readUnsignedVarInt() {
        long result = 0;
        int shift = 0;
        while (pos < dataEnd) {
            int b = data[pos++] & 0xFF;
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                break;
            }
            shift += 7;
        }
        return result;
    }
}
