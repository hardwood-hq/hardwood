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

import dev.hardwood.internal.conversion.FixedWidths;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ParquetReadException;

/// Decoder for BYTE_STREAM_SPLIT encoding.
///
/// BYTE_STREAM_SPLIT scatters bytes of values across K streams (where K is the byte width).
/// For N values of K bytes each:
/// - Stream 0 contains byte 0 of all N values
/// - Stream 1 contains byte 1 of all N values
/// - etc.
///
/// The encoded data is the concatenation of all streams: `[stream0][stream1]...[streamK-1]`
///
/// To decode value i, gather `byte[i]` from each stream and reassemble.
public class ByteStreamSplitDecoder implements ValueDecoder {

    private final byte[] data;
    private final int baseOffset;
    private final int numValues;
    private final int byteWidth;
    private int currentIndex = 0;

    /// @param data the page bytes to decode from
    /// @param offset the position in `data` the streams start at
    /// @param limit the position in `data` the page's bytes end at, exclusive; a buffer reused
    ///        across pages runs on past it with bytes of an earlier page
    /// @param numValues the number of values each stream holds
    /// @param type the physical type of the values
    /// @param typeLength the value width for `FIXED_LEN_BYTE_ARRAY`, `null` for every other type
    public ByteStreamSplitDecoder(byte[] data, int offset, int limit, int numValues, PhysicalType type,
            Integer typeLength) {
        this.data = data;
        this.baseOffset = offset;
        this.numValues = numValues;
        this.byteWidth = getByteWidth(type, typeLength);

        // Validate that the data buffer has enough room for the expected values
        long expectedLength = (long) numValues * byteWidth;
        int availableLength = limit - offset;
        if (availableLength < expectedLength) {
            throw new ParquetReadException(
                    "Insufficient data: expected at least " + expectedLength + " bytes for " +
                            numValues + " values of " + byteWidth + " bytes, got " + availableLength);
        }
    }

    private static int getByteWidth(PhysicalType type, Integer typeLength) {
        return switch (type) {
            case FLOAT, INT32, DOUBLE, INT64 -> FixedWidths.of(type);
            case FIXED_LEN_BYTE_ARRAY -> {
                if (typeLength == null) {
                    throw new IllegalArgumentException("FIXED_LEN_BYTE_ARRAY requires typeLength");
                }
                yield typeLength;
            }
            case BOOLEAN, INT96, BYTE_ARRAY -> throw new IllegalArgumentException(
                    "BYTE_STREAM_SPLIT is not defined over " + type);
        };
    }

    /// Read DOUBLE values directly into a primitive double array.
    @Override
    public void readDoubles(double[] output, int[] definitionLevels, int maxDefLevel) {
        byte[] valueBytes = new byte[8];
        ByteBuffer buffer = ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN);

        if (definitionLevels == null) {
            for (int i = 0; i < output.length; i++) {
                gatherBytes(valueBytes);
                buffer.rewind();
                output[i] = buffer.getDouble();
            }
        }
        else {
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    gatherBytes(valueBytes);
                    buffer.rewind();
                    output[i] = buffer.getDouble();
                }
            }
        }
    }

    /// Read INT64 values directly into a primitive long array.
    @Override
    public void readLongs(long[] output, int[] definitionLevels, int maxDefLevel) {
        byte[] valueBytes = new byte[8];
        ByteBuffer buffer = ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN);

        if (definitionLevels == null) {
            for (int i = 0; i < output.length; i++) {
                gatherBytes(valueBytes);
                buffer.rewind();
                output[i] = buffer.getLong();
            }
        }
        else {
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    gatherBytes(valueBytes);
                    buffer.rewind();
                    output[i] = buffer.getLong();
                }
            }
        }
    }

    /// Read INT32 values directly into a primitive int array.
    @Override
    public void readInts(int[] output, int[] definitionLevels, int maxDefLevel) {
        byte[] valueBytes = new byte[4];
        ByteBuffer buffer = ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN);

        if (definitionLevels == null) {
            for (int i = 0; i < output.length; i++) {
                gatherBytes(valueBytes);
                buffer.rewind();
                output[i] = buffer.getInt();
            }
        }
        else {
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    gatherBytes(valueBytes);
                    buffer.rewind();
                    output[i] = buffer.getInt();
                }
            }
        }
    }

    /// Read FLOAT values directly into a primitive float array.
    @Override
    public void readFloats(float[] output, int[] definitionLevels, int maxDefLevel) {
        byte[] valueBytes = new byte[4];
        ByteBuffer buffer = ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN);

        if (definitionLevels == null) {
            for (int i = 0; i < output.length; i++) {
                gatherBytes(valueBytes);
                buffer.rewind();
                output[i] = buffer.getFloat();
            }
        }
        else {
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    gatherBytes(valueBytes);
                    buffer.rewind();
                    output[i] = buffer.getFloat();
                }
            }
        }
    }

    /// Read FIXED_LEN_BYTE_ARRAY values directly into a byte[][] array.
    @Override
    public void readByteArrays(byte[][] output, int[] definitionLevels, int maxDefLevel) {
        if (definitionLevels == null) {
            for (int i = 0; i < output.length; i++) {
                byte[] valueBytes = new byte[byteWidth];
                gatherBytes(valueBytes);
                output[i] = valueBytes;
            }
        }
        else {
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    byte[] valueBytes = new byte[byteWidth];
                    gatherBytes(valueBytes);
                    output[i] = valueBytes;
                }
            }
        }
    }

    /// Move to non-null value `index` without decoding.
    ///
    /// A page that straddles a batch rebuilds this decoder at the start of the
    /// streams; seeking here is what keeps that resume linear. `index` is a
    /// non-null index, not a slot index, and may equal [#numValues] when every
    /// value has already been consumed.
    public void positionAt(int index) {
        if (index < 0 || index > numValues) {
            throw new ParquetReadException(
                    "BYTE_STREAM_SPLIT position " + index + " is outside 0.." + numValues);
        }
        currentIndex = index;
    }

    /// Gather bytes for the current value from byte streams and advance the index.
    private void gatherBytes(byte[] valueBytes) {
        if (currentIndex >= numValues) {
            throw new ParquetReadException("No more values to read");
        }
        for (int k = 0; k < valueBytes.length; k++) {
            int streamOffset = k * numValues;
            valueBytes[k] = data[baseOffset + streamOffset + currentIndex];
        }
        currentIndex++;
    }

    // -----------------------------------------------------------------------
    // Unified direct-into-batch overloads
    //
    // defLevels == null  →  all-present: decode `count` values via gatherBytes;
    //                       returns count.
    // defLevels != null  →  nullable: call gatherBytes only for non-null slots;
    //                       returns non-null count.
    // -----------------------------------------------------------------------

    /// Decode up to `count` DOUBLE values into dest[destOffset+i].
    @Override
    public int readDoubles(double[] dest, int destOffset, int count,
                           int[] defLevels, int defLevelOffset, int maxDefLevel) {
        byte[] valueBytes = new byte[8];
        ByteBuffer buffer = ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN);
        if (defLevels == null) {
            for (int i = 0; i < count; i++) {
                gatherBytes(valueBytes);
                buffer.rewind();
                dest[destOffset + i] = buffer.getDouble();
            }
            return count;
        }
        int decoded = 0;
        for (int i = 0; i < count; i++) {
            if (defLevels[defLevelOffset + i] == maxDefLevel) {
                gatherBytes(valueBytes);
                buffer.rewind();
                dest[destOffset + i] = buffer.getDouble();
                decoded++;
            }
        }
        return decoded;
    }

    /// Decode up to `count` INT64 values into dest[destOffset+i].
    @Override
    public int readLongs(long[] dest, int destOffset, int count,
                         int[] defLevels, int defLevelOffset, int maxDefLevel) {
        byte[] valueBytes = new byte[8];
        ByteBuffer buffer = ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN);
        if (defLevels == null) {
            for (int i = 0; i < count; i++) {
                gatherBytes(valueBytes);
                buffer.rewind();
                dest[destOffset + i] = buffer.getLong();
            }
            return count;
        }
        int decoded = 0;
        for (int i = 0; i < count; i++) {
            if (defLevels[defLevelOffset + i] == maxDefLevel) {
                gatherBytes(valueBytes);
                buffer.rewind();
                dest[destOffset + i] = buffer.getLong();
                decoded++;
            }
        }
        return decoded;
    }

    /// Decode up to `count` INT32 values into dest[destOffset+i].
    @Override
    public int readInts(int[] dest, int destOffset, int count,
                        int[] defLevels, int defLevelOffset, int maxDefLevel) {
        byte[] valueBytes = new byte[4];
        ByteBuffer buffer = ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN);
        if (defLevels == null) {
            for (int i = 0; i < count; i++) {
                gatherBytes(valueBytes);
                buffer.rewind();
                dest[destOffset + i] = buffer.getInt();
            }
            return count;
        }
        int decoded = 0;
        for (int i = 0; i < count; i++) {
            if (defLevels[defLevelOffset + i] == maxDefLevel) {
                gatherBytes(valueBytes);
                buffer.rewind();
                dest[destOffset + i] = buffer.getInt();
                decoded++;
            }
        }
        return decoded;
    }

    /// Decode up to `count` FLOAT values into dest[destOffset+i].
    @Override
    public int readFloats(float[] dest, int destOffset, int count,
                          int[] defLevels, int defLevelOffset, int maxDefLevel) {
        byte[] valueBytes = new byte[4];
        ByteBuffer buffer = ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN);
        if (defLevels == null) {
            for (int i = 0; i < count; i++) {
                gatherBytes(valueBytes);
                buffer.rewind();
                dest[destOffset + i] = buffer.getFloat();
            }
            return count;
        }
        int decoded = 0;
        for (int i = 0; i < count; i++) {
            if (defLevels[defLevelOffset + i] == maxDefLevel) {
                gatherBytes(valueBytes);
                buffer.rewind();
                dest[destOffset + i] = buffer.getFloat();
                decoded++;
            }
        }
        return decoded;
    }
}
