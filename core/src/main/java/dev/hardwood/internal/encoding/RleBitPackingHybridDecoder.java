/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import dev.hardwood.internal.encoding.simd.SimdOperations;
import dev.hardwood.internal.encoding.simd.VectorSupport;

/**
 * Decoder for RLE/Bit-Packing Hybrid encoding.
 * Used primarily for definition/repetition levels and dictionary indices.
 */
public class RleBitPackingHybridDecoder {

    private static final SimdOperations SIMD_OPS = VectorSupport.operations();

    private final byte[] data;
    private final ByteBuffer dataBuffer;
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

    public RleBitPackingHybridDecoder(InputStream input, int bitWidth) throws IOException {
        this(input.readAllBytes(), bitWidth);
    }

    public RleBitPackingHybridDecoder(byte[] data, int bitWidth) {
        this.data = data;
        this.dataBuffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        this.bitWidth = bitWidth;
        this.bitMask = (bitWidth == 0) ? 0 : (1 << bitWidth) - 1;
    }

    public void readInts(int[] buffer, int offset, int count) {
        if (bitWidth == 0 || data.length == 0) {
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
        if (defLevels == null) {
            int[] indices = new int[len];
            readInts(indices, 0, len);
            return indices;
        }
        int nonNullCount = countNonNulls(defLevels, maxDef);
        int[] indices = new int[nonNullCount];
        readInts(indices, 0, nonNullCount);
        return indices;
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
        if (pos >= data.length) {
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
        for (int i = 0; i < bytesNeeded && pos < data.length; i++) {
            value |= (data[pos++] & 0xFF) << (i * 8);
        }
        return value & bitMask;
    }

    /**
     * Batch decode bit-packed values. Optimized paths for common bit widths.
     */
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

        // Fast path for bit width 1 (common for definition levels)
        if (width == 1) {
            while (count >= 8 && pos < data.length) {
                int b = data[pos++] & 0xFF;
                output[outPos]     = b & 1;
                output[outPos + 1] = (b >> 1) & 1;
                output[outPos + 2] = (b >> 2) & 1;
                output[outPos + 3] = (b >> 3) & 1;
                output[outPos + 4] = (b >> 4) & 1;
                output[outPos + 5] = (b >> 5) & 1;
                output[outPos + 6] = (b >> 6) & 1;
                output[outPos + 7] = (b >> 7) & 1;
                outPos += 8;
                count -= 8;
            }
        }
        // For widths 2-8: read 8 bytes at once when possible, extract 8 values
        else if (width <= 8) {
            // Process 8 values at a time using bulk long reads when we have enough data
            while (count >= 8 && pos + 8 <= data.length) {
                long bits = dataBuffer.getLong(pos);
                pos += width; // Only consume 'width' bytes for 8 values

                output[outPos]     = (int) (bits & mask); bits >>>= width;
                output[outPos + 1] = (int) (bits & mask); bits >>>= width;
                output[outPos + 2] = (int) (bits & mask); bits >>>= width;
                output[outPos + 3] = (int) (bits & mask); bits >>>= width;
                output[outPos + 4] = (int) (bits & mask); bits >>>= width;
                output[outPos + 5] = (int) (bits & mask); bits >>>= width;
                output[outPos + 6] = (int) (bits & mask); bits >>>= width;
                output[outPos + 7] = (int) (bits & mask);
                outPos += 8;
                count -= 8;
            }
            // Fallback when near end of buffer
            while (count >= 8 && pos + width <= data.length) {
                long bits = 0;
                for (int i = 0; i < width; i++) {
                    bits |= ((long) (data[pos++] & 0xFF)) << (i * 8);
                }
                output[outPos]     = (int) (bits & mask); bits >>>= width;
                output[outPos + 1] = (int) (bits & mask); bits >>>= width;
                output[outPos + 2] = (int) (bits & mask); bits >>>= width;
                output[outPos + 3] = (int) (bits & mask); bits >>>= width;
                output[outPos + 4] = (int) (bits & mask); bits >>>= width;
                output[outPos + 5] = (int) (bits & mask); bits >>>= width;
                output[outPos + 6] = (int) (bits & mask); bits >>>= width;
                output[outPos + 7] = (int) (bits & mask);
                outPos += 8;
                count -= 8;
            }
        }

        // Handle remaining values
        while (count > 0) {
            while (bitsInBuffer < width && pos < data.length) {
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
        while (pos < data.length) {
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
