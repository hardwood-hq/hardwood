/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
    private final ByteBuffer dataBuffer;
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
        this.dataBuffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        this.dataEnd = offset + length;
        this.pos = offset;
        this.bitWidth = bitWidth;
        // (1 << 32) - 1 wraps to 0 in Java (the shift count is taken mod 32), so a width of
        // 32 needs an explicit all-ones mask, matching the encoder's 32-bit handling.
        this.bitMask = (bitWidth == 0) ? 0 : (bitWidth == 32) ? -1 : (1 << bitWidth) - 1;
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

    /// Resolves dictionary-encoded byte-array values, also writing each value's
    /// dictionary entry index into `outDictIndices` (`-1` at null positions) so the
    /// row reader can intern values by entry.
    public void readDictionaryByteArrays(byte[][] output, int[] outDictIndices, byte[][] dictionary,
                                         int[] defLevels, int maxDef) {
        int[] indices = decodeIndices(output.length, defLevels, maxDef);
        applyDictionary(output, outDictIndices, dictionary, indices, defLevels, maxDef);
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
        int count = defLevels == null ? len : countNonNulls(defLevels, len, maxDef);
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

    private void applyDictionary(byte[][] output, int[] outDictIndices, byte[][] dict, int[] indices,
                                 int[] defLevels, int maxDef) {
        if (defLevels == null) {
            for (int i = 0; i < output.length; i++) {
                int d = indices[i];
                output[i] = dict[d];
                outDictIndices[i] = d;
            }
        }
        else {
            int idx = 0;
            for (int i = 0; i < output.length; i++) {
                if (defLevels[i] == maxDef) {
                    int d = indices[idx++];
                    output[i] = dict[d];
                    outDictIndices[i] = d;
                }
                else {
                    outDictIndices[i] = -1;
                }
            }
        }
    }

    private static int countNonNulls(int[] defLevels, int len, int maxDef) {
        return SIMD_OPS.countNonNulls(defLevels, len, maxDef);
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

        // Fast path for bit width 1 (common for definition levels)
        if (width == 1) {
            while (count >= 8 && pos < dataEnd) {
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
            while (count >= 8 && pos + 8 <= dataEnd) {
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
            while (count >= 8 && pos + width <= dataEnd) {
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

    /// Returns `true` if the first `count` values of this stream are all equal
    /// to `value`, as proven by a single leading RLE run that covers them.
    ///
    /// Used to detect all-present definition levels (one RLE run of `maxDef`)
    /// without materializing the level array. This is a read-only probe: it
    /// inspects the first run and then restores the decoder to its original
    /// state, so the same instance can afterwards be read from the beginning —
    /// via [#readInts] or otherwise — exactly as if the probe had never run.
    public boolean isSingleRleRunOf(int value, int count) {
        if (count == 0) {
            return true;
        }
        if (bitWidth == 0 || pos >= dataEnd) {
            return false;
        }
        // Snapshot the run state readNextRun is about to mutate, then restore it
        // before returning so the probe leaves no trace. bitBuffer/bitsInBuffer
        // are untouched by readNextRun, so they need no saving.
        int savedPos = pos;
        int savedRemainingInRun = remainingInRun;
        int savedCurrentValue = currentValue;
        boolean savedIsRleRun = isRleRun;

        readNextRun();
        boolean match = isRleRun && currentValue == value && remainingInRun >= count;

        pos = savedPos;
        remainingInRun = savedRemainingInRun;
        currentValue = savedCurrentValue;
        isRleRun = savedIsRleRun;
        return match;
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
