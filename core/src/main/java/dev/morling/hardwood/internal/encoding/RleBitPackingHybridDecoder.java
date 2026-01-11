/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.encoding;

import java.io.IOException;
import java.io.InputStream;

/**
 * Decoder for RLE/Bit-Packing Hybrid encoding.
 * Used primarily for definition and repetition levels.
 */
public class RleBitPackingHybridDecoder {

    private final InputStream input;
    private final int bitWidth;

    // Current run state
    private int currentValue = 0;
    private int remainingInRun = 0;
    private boolean isRleRun = false;

    // Bit-packed state
    private long bitPackedBuffer = 0;
    private int bitsInBuffer = 0;

    public RleBitPackingHybridDecoder(InputStream input, int bitWidth) {
        this.input = input;
        this.bitWidth = bitWidth;
    }

    /**
     * Read the next level value.
     */
    public int readInt() throws IOException {
        if (bitWidth == 0) {
            // All values are 0
            return 0;
        }

        if (remainingInRun == 0) {
            readNextRun();
        }

        remainingInRun--;

        if (isRleRun) {
            return currentValue;
        }
        else {
            return readBitPackedValue();
        }
    }

    /**
     * Read multiple values into a buffer.
     */
    public void readInts(int[] buffer, int offset, int count) throws IOException {
        for (int i = 0; i < count; i++) {
            buffer[offset + i] = readInt();
        }
    }

    private void readNextRun() throws IOException {
        // Read header varint
        long header = readUnsignedVarInt();

        if ((header & 1) == 1) {
            // Bit-packed run: header >> 1 gives number of groups of 8 values
            // Note: LSB=1 means bit-packed (header = <count> << 1 | 1)
            int numGroups = (int) (header >> 1);
            remainingInRun = numGroups * 8;
            isRleRun = false;
        }
        else {
            // RLE run: header >> 1 gives count, followed by value
            // Note: LSB=0 means RLE (header = <count> << 1)
            remainingInRun = (int) (header >> 1);
            currentValue = readRleValue();
            isRleRun = true;
        }
    }

    /**
     * Read RLE value - uses byte-aligned reading, taking minimum bytes needed for bit width.
     */
    private int readRleValue() throws IOException {
        int bytesNeeded = (bitWidth + 7) / 8;
        int value = 0;

        for (int i = 0; i < bytesNeeded; i++) {
            int b = input.read();
            if (b == -1) {
                throw new IOException("Unexpected EOF while reading RLE value");
            }
            value |= (b & 0xFF) << (i * 8);
        }

        // Mask to bit width
        return value & ((1 << bitWidth) - 1);
    }

    private int readBitPackedValue() throws IOException {
        return readBits(bitWidth);
    }

    private int readBits(int numBits) throws IOException {
        while (bitsInBuffer < numBits) {
            int b = input.read();
            if (b == -1) {
                throw new IOException("Unexpected EOF while reading bits");
            }
            bitPackedBuffer |= ((long) b & 0xFF) << bitsInBuffer;
            bitsInBuffer += 8;
        }

        int value = (int) (bitPackedBuffer & ((1L << numBits) - 1));
        bitPackedBuffer >>>= numBits;
        bitsInBuffer -= numBits;
        return value;
    }

    private long readUnsignedVarInt() throws IOException {
        long result = 0;
        int shift = 0;
        while (true) {
            int b = input.read();
            if (b == -1) {
                throw new IOException("Unexpected EOF while reading varint");
            }
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                break;
            }
            shift += 7;
        }
        return result;
    }
}
