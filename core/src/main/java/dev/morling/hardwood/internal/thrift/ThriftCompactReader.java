/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.thrift;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Reader for Thrift Compact Protocol.
 * Reference: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md
 */
public class ThriftCompactReader {

    private static final byte PROTOCOL_ID = (byte) 0x82;
    private static final byte VERSION = 0x01;
    private static final byte TYPE_BOOLEAN_TRUE = 0x01;
    private static final byte TYPE_BOOLEAN_FALSE = 0x02;
    private static final byte TYPE_BYTE = 0x03;
    private static final byte TYPE_I16 = 0x04;
    private static final byte TYPE_I32 = 0x05;
    private static final byte TYPE_I64 = 0x06;
    private static final byte TYPE_DOUBLE = 0x07;
    private static final byte TYPE_BINARY = 0x08;
    private static final byte TYPE_LIST = 0x09;
    private static final byte TYPE_SET = 0x0A;
    private static final byte TYPE_MAP = 0x0B;
    private static final byte TYPE_STRUCT = 0x0C;

    private final InputStream input;
    private short lastFieldId = 0;

    public ThriftCompactReader(InputStream input) {
        this.input = input;
    }

    /**
     * Read an unsigned varint from the stream.
     */
    public long readVarint() throws IOException {
        long result = 0;
        int shift = 0;
        while (true) {
            int b = input.read();
            if (b == -1) {
                throw new EOFException("Unexpected EOF while reading varint");
            }
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                break;
            }
            shift += 7;
        }
        return result;
    }

    /**
     * Read a zigzag-encoded signed integer.
     */
    public long readZigzag() throws IOException {
        long n = readVarint();
        return (n >>> 1) ^ -(n & 1);
    }

    /**
     * Read a single byte.
     */
    public byte readByte() throws IOException {
        int b = input.read();
        if (b == -1) {
            throw new EOFException("Unexpected EOF while reading byte");
        }
        return (byte) b;
    }

    /**
     * Read multiple bytes into a buffer.
     */
    public void readBytes(byte[] buffer) throws IOException {
        int offset = 0;
        while (offset < buffer.length) {
            int read = input.read(buffer, offset, buffer.length - offset);
            if (read == -1) {
                throw new EOFException("Unexpected EOF while reading bytes");
            }
            offset += read;
        }
    }

    /**
     * Read a boolean value.
     */
    public boolean readBoolean() throws IOException {
        byte b = readByte();
        if (b == TYPE_BOOLEAN_TRUE) {
            return true;
        }
        else if (b == TYPE_BOOLEAN_FALSE) {
            return false;
        }
        throw new IOException("Invalid boolean value: " + b);
    }

    /**
     * Read an i32 value (zigzag encoded).
     */
    public int readI32() throws IOException {
        return (int) readZigzag();
    }

    /**
     * Read an i64 value (zigzag encoded).
     */
    public long readI64() throws IOException {
        return readZigzag();
    }

    /**
     * Read a double value (8 bytes, little-endian).
     */
    public double readDouble() throws IOException {
        long bits = 0;
        for (int i = 0; i < 8; i++) {
            int b = input.read();
            if (b == -1) {
                throw new EOFException("Unexpected EOF while reading double");
            }
            bits |= ((long) b & 0xFF) << (i * 8);
        }
        return Double.longBitsToDouble(bits);
    }

    /**
     * Read a binary/string value (length-prefixed).
     */
    public byte[] readBinary() throws IOException {
        int length = (int) readVarint();
        byte[] data = new byte[length];
        readBytes(data);
        return data;
    }

    /**
     * Read a string value.
     */
    public String readString() throws IOException {
        return new String(readBinary(), StandardCharsets.UTF_8);
    }

    /**
     * Read a field header and return field info.
     * Returns null when STOP field is encountered.
     */
    public FieldHeader readFieldHeader() throws IOException {
        byte b = readByte();

        if (b == 0) {
            // STOP field
            lastFieldId = 0;
            return null;
        }

        byte type = (byte) (b & 0x0F);
        int fieldIdDelta = (b & 0xF0) >> 4;

        short fieldId;
        if (fieldIdDelta == 0) {
            // Field ID is encoded separately
            fieldId = (short) readZigzag();
        }
        else {
            // Field ID is delta from last field
            fieldId = (short) (lastFieldId + fieldIdDelta);
        }

        lastFieldId = fieldId;
        return new FieldHeader(fieldId, type);
    }

    /**
     * Read a list/set header.
     */
    public CollectionHeader readListHeader() throws IOException {
        byte sizeAndType = readByte();
        int size = (sizeAndType >> 4) & 0x0F;
        byte elementType = (byte) (sizeAndType & 0x0F);

        if (size == 15) {
            // Size is encoded separately
            size = (int) readVarint();
        }

        return new CollectionHeader(elementType, size);
    }

    /**
     * Skip a field of the given type.
     */
    public void skipField(byte type) throws IOException {
        switch (type) {
            case TYPE_BOOLEAN_TRUE:
            case TYPE_BOOLEAN_FALSE:
                // Boolean value is in the type byte itself
                break;
            case TYPE_BYTE:
                readByte();
                break;
            case TYPE_I16:
            case TYPE_I32:
            case TYPE_I64:
                readZigzag();
                break;
            case TYPE_DOUBLE:
                readDouble();
                break;
            case TYPE_BINARY:
                readBinary();
                break;
            case TYPE_LIST:
            case TYPE_SET:
                CollectionHeader listHeader = readListHeader();
                for (int i = 0; i < listHeader.size(); i++) {
                    skipField(listHeader.elementType());
                }
                break;
            case TYPE_MAP:
                int mapSize = (int) readVarint();
                if (mapSize > 0) {
                    byte keyType = readByte();
                    byte valueType = readByte();
                    for (int i = 0; i < mapSize; i++) {
                        skipField(keyType);
                        skipField(valueType);
                    }
                }
                break;
            case TYPE_STRUCT:
                skipStruct();
                break;
            default:
                throw new IOException("Unknown field type: " + type);
        }
    }

    /**
     * Skip an entire struct (read until STOP field).
     */
    public void skipStruct() throws IOException {
        // Save and reset field ID context for nested struct
        short saved = pushFieldIdContext();
        try {
            while (true) {
                FieldHeader header = readFieldHeader();
                if (header == null) {
                    break;
                }
                skipField(header.type());
            }
        }
        finally {
            popFieldIdContext(saved);
        }
    }

    /**
     * Reset the last field ID (call when starting to read a new struct).
     */
    public void resetLastFieldId() {
        lastFieldId = 0;
    }

    /**
     * Save the current last field ID and reset it for reading a nested struct.
     */
    public short pushFieldIdContext() {
        short saved = lastFieldId;
        lastFieldId = 0;
        return saved;
    }

    /**
     * Restore the last field ID after reading a nested struct.
     */
    public void popFieldIdContext(short savedFieldId) {
        lastFieldId = savedFieldId;
    }

    public static record FieldHeader(short fieldId, byte type) {
    }

    public static record CollectionHeader(byte elementType, int size) {
    }
}
