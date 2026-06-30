/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/// Writer for Thrift Compact Protocol, the inverse of [ThriftCompactReader].
/// Reference: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md
public class ThriftCompactWriter {


    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private short lastFieldId = 0;

    /// Write a field header. Field ids within a struct must be written in
    /// increasing order so the compact short-form delta encoding applies.
    ///
    /// @param fieldId the Thrift field id
    /// @param type the field type
    public void writeFieldBegin(int fieldId, ThriftCompactConstants.FieldType type) {
        int delta = fieldId - lastFieldId;
        if (delta > 0 && delta <= 15) {
            out.write((delta << 4) | type.code());
        }
        else {
            out.write(type.code());
            writeZigzag(fieldId);
        }
        lastFieldId = (short) fieldId;
    }

    /// Write the STOP marker that terminates a struct's fields.
    public void writeFieldStop() {
        out.write(0);
    }

    /// Write a list header.
    ///
    /// @param size number of elements
    /// @param elementType the element type
    public void writeListBegin(int size, ThriftCompactConstants.ElementType elementType) {
        if (size < 15) {
            out.write((size << 4) | elementType.code());
        }
        else {
            out.write(0xF0 | elementType.code());
            writeVarint(size);
        }
    }

    /// Write a zigzag-encoded i32 value (no field header).
    public void writeI32(int value) {
        writeZigzag(value);
    }

    /// Write a zigzag-encoded i64 value (no field header).
    public void writeI64(long value) {
        writeZigzag(value);
    }

    /// Write a length-prefixed binary value (no field header).
    public void writeBinary(byte[] value) {
        writeVarint(value.length);
        out.writeBytes(value);
    }

    /// Write a length-prefixed UTF-8 string value (no field header).
    public void writeString(String value) {
        writeBinary(value.getBytes(StandardCharsets.UTF_8));
    }

    /// Save the current last field id and reset it for writing a nested struct,
    /// mirroring [ThriftCompactReader#pushFieldIdContext].
    public short pushFieldIdContext() {
        short saved = lastFieldId;
        lastFieldId = 0;
        return saved;
    }

    /// Restore the last field id after writing a nested struct.
    public void popFieldIdContext(short savedFieldId) {
        lastFieldId = savedFieldId;
    }

    /// Returns the serialized bytes written so far.
    public byte[] toByteArray() {
        return out.toByteArray();
    }

    /// Returns the number of bytes written so far.
    public int size() {
        return out.size();
    }

    private void writeZigzag(long value) {
        writeVarint((value << 1) ^ (value >> 63));
    }

    private void writeVarint(long value) {
        long v = value;
        while ((v & ~0x7FL) != 0) {
            out.write((int) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        out.write((int) (v & 0x7F));
    }
}
