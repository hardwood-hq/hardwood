/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.util.Arrays;

/// Encoder for RLE/Bit-Packing Hybrid encoding, the inverse of
/// [RleBitPackingHybridDecoder]. Used for definition/repetition levels (via
/// [LevelEncoder]) and, later, dictionary indices.
///
/// Values that repeat at least eight times in a row are emitted as an RLE run; shorter
/// stretches are bit-packed in groups of eight. Emitting a single RLE run for a constant
/// stream is what lets the reader take its all-present fast path on a fully-populated
/// optional column. The bit-packed byte layout is little-endian bit order, matching the
/// decoder: value `i` of a group occupies bits `[i·bitWidth, (i+1)·bitWidth)`.
public final class RleBitPackingHybridEncoder {

    private final int bitWidth;
    private final long mask;

    private byte[] buffer = new byte[64];
    private int length;

    // Values buffered for the current, not-yet-decided run (up to one group of eight).
    private final int[] bufferedValues = new int[8];
    private int numBufferedValues;

    // Run-length tracking for the value currently being repeated.
    private int previousValue;
    private int repeatCount;

    // Open bit-packed run: the index of its header byte (-1 when none is open) and the
    // number of eight-value groups written into it so far.
    private int bitPackedRunHeaderIndex = -1;
    private int bitPackedGroupCount;

    private boolean finished;

    /// @param bitWidth number of bits per value, 0–32
    public RleBitPackingHybridEncoder(int bitWidth) {
        if (bitWidth < 0 || bitWidth > 32) {
            throw new IllegalArgumentException("Invalid RLE bit width: " + bitWidth + ". Must be between 0 and 32");
        }
        this.bitWidth = bitWidth;
        this.mask = bitWidth == 0 ? 0 : (bitWidth == 32 ? 0xFFFFFFFFL : (1L << bitWidth) - 1);
    }

    /// Appends `count` values starting at `offset`.
    public void writeInts(int[] values, int offset, int count) {
        for (int i = 0; i < count; i++) {
            writeInt(values[offset + i]);
        }
    }

    /// Appends a single value.
    public void writeInt(int value) {
        if (finished) {
            throw new IllegalStateException("Encoder already finished");
        }
        if (bitWidth == 0) {
            // A 0-bit stream carries no information — the decoder reads nothing — so there
            // is nothing to buffer or emit.
            return;
        }
        if (value == previousValue) {
            repeatCount++;
            if (repeatCount >= 8) {
                // Certain to become an RLE run; keep counting and defer the emit.
                return;
            }
        }
        else {
            if (repeatCount >= 8) {
                writeRleRun();
            }
            repeatCount = 1;
            previousValue = value;
        }

        bufferedValues[numBufferedValues++] = value;
        if (numBufferedValues == 8) {
            writeOrAppendBitPackedRun();
        }
    }

    /// Finishes the stream and returns the encoded bytes. The encoder must not be written
    /// to afterwards.
    public byte[] toByteArray() {
        if (!finished) {
            if (repeatCount >= 8) {
                writeRleRun();
            }
            else if (numBufferedValues > 0) {
                Arrays.fill(bufferedValues, numBufferedValues, 8, 0);
                writeOrAppendBitPackedRun();
                endPreviousBitPackedRun();
            }
            else {
                endPreviousBitPackedRun();
            }
            finished = true;
        }
        return Arrays.copyOf(buffer, length);
    }

    private void writeOrAppendBitPackedRun() {
        if (bitPackedGroupCount >= 63) {
            // A bit-packed run header counts groups in the upper bits of one byte, so a
            // run holds at most 63 groups; start a fresh one past that.
            endPreviousBitPackedRun();
        }
        if (bitPackedRunHeaderIndex == -1) {
            // Reserve the header byte; its final value is only known once the run ends.
            write(0);
            bitPackedRunHeaderIndex = length - 1;
        }
        packGroup();
        numBufferedValues = 0;
        // The buffered values are now written as a bit-packed group, so they must not also
        // be counted toward a later RLE run.
        repeatCount = 0;
        bitPackedGroupCount++;
    }

    private void endPreviousBitPackedRun() {
        if (bitPackedRunHeaderIndex == -1) {
            return;
        }
        buffer[bitPackedRunHeaderIndex] = (byte) ((bitPackedGroupCount << 1) | 1);
        bitPackedRunHeaderIndex = -1;
        bitPackedGroupCount = 0;
    }

    private void writeRleRun() {
        // Close any open bit-packed run before switching to an RLE run.
        endPreviousBitPackedRun();
        writeUnsignedVarInt(repeatCount << 1);
        int byteCount = (bitWidth + 7) / 8;
        for (int i = 0; i < byteCount; i++) {
            write((previousValue >>> (i * 8)) & 0xFF);
        }
        repeatCount = 0;
        numBufferedValues = 0;
    }

    /// Packs the eight buffered values into `bitWidth` bytes, LSB-first, matching the
    /// decoder's little-endian bit order.
    private void packGroup() {
        long acc = 0;
        int bits = 0;
        for (int i = 0; i < 8; i++) {
            acc |= (bufferedValues[i] & mask) << bits;
            bits += bitWidth;
            while (bits >= 8) {
                write((int) (acc & 0xFF));
                acc >>>= 8;
                bits -= 8;
            }
        }
        // 8·bitWidth is a whole number of bytes, so no partial byte remains.
    }

    private void writeUnsignedVarInt(int value) {
        int v = value;
        while ((v & ~0x7F) != 0) {
            write((v & 0x7F) | 0x80);
            v >>>= 7;
        }
        write(v);
    }

    private void write(int b) {
        if (length == buffer.length) {
            buffer = Arrays.copyOf(buffer, buffer.length * 2);
        }
        buffer[length++] = (byte) b;
    }
}
