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

import dev.morling.hardwood.metadata.PhysicalType;

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
public class DeltaBinaryPackedDecoder {

    private final InputStream input;
    private final PhysicalType type;

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

    public DeltaBinaryPackedDecoder(InputStream input, PhysicalType type) {
        this.input = input;
        this.type = type;
        this.headerRead = false;
        this.valuesRead = 0;
    }

    /**
     * Read a single value from the stream.
     */
    public Object readValue() throws IOException {
        if (!headerRead) {
            readHeader();
            headerRead = true;
        }

        if (valuesRead == 0) {
            valuesRead = 1;
            return boxValue(firstValue);
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

        return boxValue(lastValue);
    }

    /**
     * Read multiple values into a buffer.
     */
    public void readValues(Object[] buffer, int offset, int count) throws IOException {
        for (int i = 0; i < count; i++) {
            buffer[offset + i] = readValue();
        }
    }

    private Object boxValue(long value) {
        // Must use explicit boxing to avoid type widening in ternary expression
        if (type == PhysicalType.INT32) {
            return Integer.valueOf((int) value);
        }
        return Long.valueOf(value);
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
