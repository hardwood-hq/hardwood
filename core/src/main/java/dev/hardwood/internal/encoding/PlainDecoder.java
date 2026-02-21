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

import dev.hardwood.metadata.PhysicalType;

/**
 * Decoder for PLAIN encoding.
 * PLAIN encoding stores values in their native binary representation.
 */
public class PlainDecoder implements ValueDecoder {

    private final InputStream input;
    private final PhysicalType type;
    private final Integer typeLength;

    // For bit-packed boolean reading
    private int currentByte = 0;
    private int bitPosition = 8; // 8 means we need to read a new byte

    public PlainDecoder(InputStream input, PhysicalType type, Integer typeLength) {
        this.input = input;
        this.type = type;
        this.typeLength = typeLength;
    }

    /**
     * Read a fixed-length byte array value.
     */
    public byte[] readFixedLenByteArray(int length) throws IOException {
        byte[] bytes = input.readNBytes(length);
        if (bytes.length != length) {
            throw new IOException("Unexpected EOF while reading fixed-length byte array");
        }
        return bytes;
    }

    /**
     * Read INT64 values directly into a primitive long array.
     */
    @Override
    public void readLongs(long[] output, int[] definitionLevels, int maxDefLevel) throws IOException {
        if (definitionLevels == null) {
            int numBytes = output.length * 8;
            byte[] bytes = input.readNBytes(numBytes);
            if (bytes.length != numBytes) {
                throw new IOException("Unexpected EOF while reading INT64 values");
            }
            ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().get(output);
        }
        else {
            int numDefined = 0;
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    numDefined++;
                }
            }
            int numBytes = numDefined * 8;
            byte[] bytes = input.readNBytes(numBytes);
            if (bytes.length != numBytes) {
                throw new IOException("Unexpected EOF while reading INT64 values");
            }
            var longBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    output[i] = longBuffer.get();
                }
            }
        }
    }

    /**
     * Read DOUBLE values directly into a primitive double array.
     */
    @Override
    public void readDoubles(double[] output, int[] definitionLevels, int maxDefLevel) throws IOException {
        if (definitionLevels == null) {
            int numBytes = output.length * 8;
            byte[] bytes = input.readNBytes(numBytes);
            if (bytes.length != numBytes) {
                throw new IOException("Unexpected EOF while reading DOUBLE values");
            }
            ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer().get(output);
        }
        else {
            int numDefined = 0;
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    numDefined++;
                }
            }
            int numBytes = numDefined * 8;
            byte[] bytes = input.readNBytes(numBytes);
            if (bytes.length != numBytes) {
                throw new IOException("Unexpected EOF while reading DOUBLE values");
            }
            var doubleBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer();
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    output[i] = doubleBuffer.get();
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
            int numBytes = output.length * 4;
            byte[] bytes = input.readNBytes(numBytes);
            if (bytes.length != numBytes) {
                throw new IOException("Unexpected EOF while reading INT32 values");
            }
            ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get(output);
        }
        else {
            int numDefined = 0;
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    numDefined++;
                }
            }
            int numBytes = numDefined * 4;
            byte[] bytes = input.readNBytes(numBytes);
            if (bytes.length != numBytes) {
                throw new IOException("Unexpected EOF while reading INT32 values");
            }
            var intBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    output[i] = intBuffer.get();
                }
            }
        }
    }

    /**
     * Read FLOAT values directly into a primitive float array.
     */
    @Override
    public void readFloats(float[] output, int[] definitionLevels, int maxDefLevel) throws IOException {
        if (definitionLevels == null) {
            int numBytes = output.length * 4;
            byte[] bytes = input.readNBytes(numBytes);
            if (bytes.length != numBytes) {
                throw new IOException("Unexpected EOF while reading FLOAT values");
            }
            ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(output);
        }
        else {
            int numDefined = 0;
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    numDefined++;
                }
            }
            int numBytes = numDefined * 4;
            byte[] bytes = input.readNBytes(numBytes);
            if (bytes.length != numBytes) {
                throw new IOException("Unexpected EOF while reading FLOAT values");
            }
            var floatBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer();
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    output[i] = floatBuffer.get();
                }
            }
        }
    }

    /**
     * Read BOOLEAN values directly into a primitive boolean array.
     */
    @Override
    public void readBooleans(boolean[] output, int[] definitionLevels, int maxDefLevel) throws IOException {
        if (definitionLevels == null) {
            for (int i = 0; i < output.length; i++) {
                output[i] = readBoolean();
            }
        }
        else {
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    output[i] = readBoolean();
                }
            }
        }
    }

    /**
     * Read BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, or INT96 values directly into a byte[][] array.
     */
    @Override
    public void readByteArrays(byte[][] output, int[] definitionLevels, int maxDefLevel) throws IOException {
        if (definitionLevels == null) {
            for (int i = 0; i < output.length; i++) {
                output[i] = readByteArrayValue();
            }
        }
        else {
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    output[i] = readByteArrayValue();
                }
            }
        }
    }

    /**
     * Read a single byte array value based on the physical type.
     */
    private byte[] readByteArrayValue() throws IOException {
        return switch (type) {
            case BYTE_ARRAY -> readByteArray();
            case FIXED_LEN_BYTE_ARRAY -> {
                if (typeLength == null) {
                    throw new IOException("FIXED_LEN_BYTE_ARRAY requires type_length in schema");
                }
                yield readFixedLenByteArray(typeLength);
            }
            case INT96 -> readInt96();
            default -> throw new IOException("readByteArrays not supported for type: " + type);
        };
    }

    private boolean readBoolean() throws IOException {
        // Booleans are bit-packed in PLAIN encoding (8 values per byte, LSB first)
        if (bitPosition == 8) {
            // Need to read a new byte
            currentByte = input.read();
            if (currentByte == -1) {
                throw new IOException("Unexpected EOF while reading boolean");
            }
            bitPosition = 0;
        }

        // Extract the bit at the current position
        boolean value = ((currentByte >> bitPosition) & 1) != 0;
        bitPosition++;
        return value;
    }

    private byte[] readInt96() throws IOException {
        byte[] bytes = input.readNBytes(12);
        if (bytes.length != 12) {
            throw new IOException("Unexpected EOF while reading INT96");
        }
        return bytes;
    }

    private byte[] readByteArray() throws IOException {
        // Read length (4 bytes, little-endian)
        byte[] lengthBytes = input.readNBytes(4);
        if (lengthBytes.length != 4) {
            throw new IOException("Unexpected EOF while reading BYTE_ARRAY length");
        }
        int length = ByteBuffer.wrap(lengthBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();

        if (length < 0) {
            throw new IOException("Invalid BYTE_ARRAY length: " + length);
        }

        if (length == 0) {
            return new byte[0];
        }

        // Read data
        byte[] data = input.readNBytes(length);
        if (data.length != length) {
            throw new IOException("Unexpected EOF while reading BYTE_ARRAY data");
        }
        return data;
    }
}
