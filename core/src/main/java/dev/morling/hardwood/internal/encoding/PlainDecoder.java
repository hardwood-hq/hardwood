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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import dev.morling.hardwood.metadata.PhysicalType;

/**
 * Decoder for PLAIN encoding.
 * PLAIN encoding stores values in their native binary representation.
 */
public class PlainDecoder {

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
     * Read a single value from the stream.
     */
    public Object readValue() throws IOException {
        return switch (type) {
            case BOOLEAN -> readBoolean();
            case INT32 -> readInt32();
            case INT64 -> readInt64();
            case INT96 -> readInt96();
            case FLOAT -> readFloat();
            case DOUBLE -> readDouble();
            case BYTE_ARRAY -> readByteArray();
            case FIXED_LEN_BYTE_ARRAY -> {
                if (typeLength == null) {
                    throw new IOException("FIXED_LEN_BYTE_ARRAY requires type_length in schema");
                }
                yield readFixedLenByteArray(typeLength);
            }
        };
    }

    /**
     * Read a fixed-length byte array value.
     */
    public byte[] readFixedLenByteArray(int length) throws IOException {
        byte[] bytes = new byte[length];
        int read = input.read(bytes);
        if (read != length) {
            throw new IOException("Unexpected EOF while reading fixed-length byte array");
        }
        return bytes;
    }

    /**
     * Read multiple values into a buffer.
     */
    public void readValues(Object[] buffer, int offset, int count) throws IOException {
        for (int i = 0; i < count; i++) {
            buffer[offset + i] = readValue();
        }
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

    private int readInt32() throws IOException {
        byte[] bytes = new byte[4];
        int read = input.read(bytes);
        if (read != 4) {
            throw new IOException("Unexpected EOF while reading INT32");
        }
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    private long readInt64() throws IOException {
        byte[] bytes = new byte[8];
        int read = input.read(bytes);
        if (read != 8) {
            throw new IOException("Unexpected EOF while reading INT64");
        }
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    private byte[] readInt96() throws IOException {
        byte[] bytes = new byte[12];
        int read = input.read(bytes);
        if (read != 12) {
            throw new IOException("Unexpected EOF while reading INT96");
        }
        return bytes;
    }

    private float readFloat() throws IOException {
        byte[] bytes = new byte[4];
        int read = input.read(bytes);
        if (read != 4) {
            throw new IOException("Unexpected EOF while reading FLOAT");
        }
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getFloat();
    }

    private double readDouble() throws IOException {
        byte[] bytes = new byte[8];
        int read = input.read(bytes);
        if (read != 8) {
            throw new IOException("Unexpected EOF while reading DOUBLE");
        }
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getDouble();
    }

    private byte[] readByteArray() throws IOException {
        // Read length (4 bytes, little-endian)
        byte[] lengthBytes = new byte[4];
        int read = input.read(lengthBytes);
        if (read != 4) {
            throw new IOException("Unexpected EOF while reading BYTE_ARRAY length");
        }
        int length = ByteBuffer.wrap(lengthBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();

        if (length < 0) {
            throw new IOException("Invalid BYTE_ARRAY length: " + length);
        }

        // Read data
        byte[] data = new byte[length];
        read = input.read(data);
        if (read != length) {
            throw new IOException("Unexpected EOF while reading BYTE_ARRAY data");
        }
        return data;
    }
}
