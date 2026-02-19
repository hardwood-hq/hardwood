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

/**
 * Decoder for DELTA_BINARY_PACKED encoding.
 * <p>
 * This encoding stores integers as deltas from consecutive values, organized in blocks
 * and miniblocks. Each block has a minimum delta, and values are stored as
 * (actual_delta - min_delta) to ensure non-negative values that can be efficiently bit-packed.
 * <p>
 * Format:
 * <pre>
 * HEADER: block_size (ULEB128) | miniblock_count (ULEB128) | total_count (ULEB128) | first_value (zigzag)
 * BLOCK:  min_delta (zigzag) | bitwidths[miniblock_count] | miniblock_data...
 * </pre>
 * <p>
 * Supports INT32 and INT64 physical types.
 *
 * @see <a href="https://github.com/apache/parquet-format/blob/master/Encodings.md">Parquet Encodings</a>
 */
public class DeltaBinaryPackedDecoder implements ValueDecoder {

    private final InputStream input;

    // Header values
    private int blockSize;
    private int miniblockCount;
    private int totalValueCount;
    private long firstValue;
    private int valuesPerMiniblock;

    // Reading state
    private int valuesRead;
    private long lastValue;
    private boolean headerRead;

    // Current block state
    private long minDelta;
    private int[] bitWidths;
    private int currentMiniblock;
    private int valuesInCurrentMiniblock;

    // Bit unpacking buffer
    private byte[] miniblockData;
    private int bitPosition;

    public DeltaBinaryPackedDecoder(InputStream input) {
        this.input = input;
        this.headerRead = false;
        this.valuesRead = 0;
    }

    /**
     * Read a single INT32 value from the stream.
     */
    public int readInt() throws IOException {
        return (int) readLongValue();
    }

    /**
     * Read a single INT64 value from the stream.
     */
    public long readLong() throws IOException {
        return readLongValue();
    }

    /**
     * Read INT64 values directly into a primitive long array.
     */
    @Override
    public void readLongs(long[] output, int[] definitionLevels, int maxDefLevel) throws IOException {
        if (definitionLevels == null) {
            for (int i = 0; i < output.length; i++) {
                output[i] = readLongValue();
            }
        }
        else {
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    output[i] = readLongValue();
                }
            }
        }
    }

    /**
     * Read INT32 values directly into a primitive int array.
     */
    @Override
    public void readInts(int[] output, int[] definitionLevels, int maxDefLevel) throws IOException {
        if (definitionLevels == null) {
            for (int i = 0; i < output.length; i++) {
                output[i] = (int) readLongValue();
            }
        }
        else {
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    output[i] = (int) readLongValue();
                }
            }
        }
    }

    /**
     * Read a single value as a primitive long (no boxing).
     */
    private long readLongValue() throws IOException {
        if (!headerRead) {
            readHeader();
            headerRead = true;
        }

        if (valuesRead == 0) {
            valuesRead = 1;
            return firstValue;
        }

        if (valuesRead >= totalValueCount) {
            throw new IOException("No more values to read");
        }

        // Check if we need to start a new block (first value after header doesn't count)
        int valuesAfterFirst = valuesRead - 1;
        if (valuesAfterFirst % blockSize == 0) {
            readBlockHeader();
        }

        // Check if we need a new miniblock
        if (valuesInCurrentMiniblock >= valuesPerMiniblock) {
            currentMiniblock++;
            valuesInCurrentMiniblock = 0;
            if (currentMiniblock < miniblockCount) {
                loadMiniblock();
            }
        }

        // Unpack delta and reconstruct value
        long packedDelta = unpackValue(bitWidths[currentMiniblock]);
        long delta = minDelta + packedDelta;
        lastValue += delta;
        valuesInCurrentMiniblock++;
        valuesRead++;

        return lastValue;
    }

    private void readHeader() throws IOException {
        blockSize = readUleb128();
        miniblockCount = readUleb128();
        totalValueCount = readUleb128();
        firstValue = readZigzagUleb128();

        if (miniblockCount == 0) {
            throw new IOException("Invalid miniblock count: 0");
        }
        valuesPerMiniblock = blockSize / miniblockCount;
        bitWidths = new int[miniblockCount];

        lastValue = firstValue;
    }

    private void readBlockHeader() throws IOException {
        minDelta = readZigzagUleb128();

        // Read bit widths for all miniblocks
        for (int i = 0; i < miniblockCount; i++) {
            int b = input.read();
            if (b < 0) {
                throw new IOException("Unexpected EOF reading bitwidths");
            }
            bitWidths[i] = b;
        }

        currentMiniblock = 0;
        valuesInCurrentMiniblock = 0;
        loadMiniblock();
    }

    private void loadMiniblock() throws IOException {
        int bitWidth = bitWidths[currentMiniblock];
        if (bitWidth == 0) {
            // All values in this miniblock have the same delta (minDelta)
            miniblockData = new byte[0];
        }
        else {
            int bytesNeeded = (valuesPerMiniblock * bitWidth + 7) / 8;
            miniblockData = new byte[bytesNeeded];
            int read = input.read(miniblockData);
            if (read != bytesNeeded) {
                throw new IOException("Unexpected EOF reading miniblock data: expected " + bytesNeeded + ", got " + read);
            }
        }
        bitPosition = 0;
    }

    private long unpackValue(int bitWidth) {
        if (bitWidth == 0) {
            return 0;
        }

        long value = 0;
        int bitsRemaining = bitWidth;

        while (bitsRemaining > 0) {
            int byteOffset = bitPosition / 8;
            int bitOffset = bitPosition % 8;
            int bitsAvailable = 8 - bitOffset;
            int bitsToRead = Math.min(bitsAvailable, bitsRemaining);

            int mask = (1 << bitsToRead) - 1;
            long bits = (miniblockData[byteOffset] >>> bitOffset) & mask;
            value |= bits << (bitWidth - bitsRemaining);

            bitPosition += bitsToRead;
            bitsRemaining -= bitsToRead;
        }

        return value;
    }

    private int readUleb128() throws IOException {
        int result = 0;
        int shift = 0;
        int b;
        do {
            b = input.read();
            if (b < 0) {
                throw new IOException("Unexpected EOF in ULEB128");
            }
            result |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return result;
    }

    private long readUleb128Long() throws IOException {
        long result = 0;
        int shift = 0;
        int b;
        do {
            b = input.read();
            if (b < 0) {
                throw new IOException("Unexpected EOF in ULEB128");
            }
            result |= (long) (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return result;
    }

    private long readZigzagUleb128() throws IOException {
        long encoded = readUleb128Long();
        // Zigzag decode: (n >>> 1) ^ -(n & 1)
        return (encoded >>> 1) ^ -(encoded & 1);
    }
}
