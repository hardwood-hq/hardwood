/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for ThriftCompactReader, particularly field skipping for complex types.
 */
class ThriftCompactReaderTest {

    /**
     * Verifies that MAP fields are skipped correctly per the Thrift Compact Protocol spec.
     * <p>
     * The spec encodes MAP as: varint(size), then if size > 0, a single byte with
     * key type in the high nibble and value type in the low nibble, followed by
     * size key-value pairs.
     * </p>
     * <p>
     * A previous bug read two separate bytes for key/value types instead of one packed byte.
     * </p>
     */
    @Test
    void skipFieldHandlesMapWithPackedKeyValueTypes() throws IOException {
        // Build a Thrift MAP with 2 entries: map<i32, i32>
        // Format: varint(2), byte(key_type << 4 | value_type), then 2 pairs of zigzag i32
        ByteBuffer buffer = ByteBuffer.allocate(64).order(ByteOrder.LITTLE_ENDIAN);

        // MAP size = 2 (varint)
        buffer.put((byte) 2);

        // Key/value types packed: i32 (0x05) << 4 | i32 (0x05) = 0x55
        buffer.put((byte) 0x55);

        // Entry 1: key=10 (zigzag=20), value=20 (zigzag=40)
        buffer.put((byte) 20); // zigzag(10) = 20
        buffer.put((byte) 40); // zigzag(20) = 40

        // Entry 2: key=30 (zigzag=60), value=40 (zigzag=80)
        buffer.put((byte) 60); // zigzag(30) = 60
        buffer.put((byte) 80); // zigzag(40) = 80

        // Sentinel byte to verify we stopped at the right position
        buffer.put((byte) 0xFF);

        buffer.flip();

        ThriftCompactReader reader = new ThriftCompactReader(buffer);
        reader.skipField((byte) 0x0B); // TYPE_MAP

        // Should have consumed exactly 6 bytes: 1 (size) + 1 (types) + 4 (entries)
        assertThat(reader.getBytesRead()).isEqualTo(6);
    }

    /**
     * Verifies that empty MAP (size=0) is skipped correctly.
     * An empty map is just a single varint(0) with no type byte.
     */
    @Test
    void skipFieldHandlesEmptyMap() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);

        // MAP size = 0
        buffer.put((byte) 0);

        // Sentinel
        buffer.put((byte) 0xFF);

        buffer.flip();

        ThriftCompactReader reader = new ThriftCompactReader(buffer);
        reader.skipField((byte) 0x0B); // TYPE_MAP

        // Should have consumed exactly 1 byte (the zero size varint)
        assertThat(reader.getBytesRead()).isEqualTo(1);
    }

    /**
     * Verifies that MAP with string keys and struct values is skipped correctly.
     * This exercises the recursive skipping through complex nested types.
     */
    @Test
    void skipFieldHandlesMapWithStringKeysAndStructValues() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(64).order(ByteOrder.LITTLE_ENDIAN);

        // MAP size = 1
        buffer.put((byte) 1);

        // Key/value types packed: BINARY (0x08) << 4 | STRUCT (0x0C) = 0x8C
        buffer.put((byte) 0x8C);

        // Entry 1 key: binary string "ab" (length=2, then 2 bytes)
        buffer.put((byte) 2); // length varint
        buffer.put((byte) 'a');
        buffer.put((byte) 'b');

        // Entry 1 value: struct with one i32 field (field id=1), then STOP
        // Field header: delta=1, type=i32(0x05) -> byte = 0x15
        buffer.put((byte) 0x15);
        buffer.put((byte) 42); // zigzag(21) = 42
        buffer.put((byte) 0x00); // STOP

        // Sentinel
        buffer.put((byte) 0xFF);

        buffer.flip();

        ThriftCompactReader reader = new ThriftCompactReader(buffer);
        reader.skipField((byte) 0x0B); // TYPE_MAP

        // 1 (size) + 1 (types) + 3 (key) + 3 (struct) = 8 bytes
        assertThat(reader.getBytesRead()).isEqualTo(8);
    }

    /**
     * Verifies that nested struct skipping correctly saves/restores field ID context.
     */
    @Test
    void skipStructPreservesFieldIdContext() throws IOException {
        // Build a struct with fields 1 (i32) and 2 (struct with field 1 (i32)) and 3 (i32)
        ByteBuffer buffer = ByteBuffer.allocate(64).order(ByteOrder.LITTLE_ENDIAN);

        // Field 1: type i32 (0x05), delta 1 -> 0x15
        buffer.put((byte) 0x15);
        buffer.put((byte) 10); // zigzag(5) = 10

        // Field 2: type struct (0x0C), delta 1 -> 0x1C
        buffer.put((byte) 0x1C);
        // Nested struct: field 1 i32
        buffer.put((byte) 0x15);
        buffer.put((byte) 20); // zigzag(10) = 20
        buffer.put((byte) 0x00); // STOP nested struct

        // Field 3: type i32 (0x05), delta 1 -> 0x15
        buffer.put((byte) 0x15);
        buffer.put((byte) 30); // zigzag(15) = 30

        // STOP outer struct
        buffer.put((byte) 0x00);

        buffer.flip();

        ThriftCompactReader reader = new ThriftCompactReader(buffer);

        // Read field 1
        ThriftCompactReader.FieldHeader f1 = reader.readFieldHeader();
        assertThat(f1.fieldId()).isEqualTo((short) 1);
        int val1 = reader.readI32();
        assertThat(val1).isEqualTo(5);

        // Skip field 2 (struct)
        ThriftCompactReader.FieldHeader f2 = reader.readFieldHeader();
        assertThat(f2.fieldId()).isEqualTo((short) 2);
        reader.skipField(f2.type());

        // Read field 3 - should correctly be field 3, not field 1
        ThriftCompactReader.FieldHeader f3 = reader.readFieldHeader();
        assertThat(f3.fieldId()).isEqualTo((short) 3);
        int val3 = reader.readI32();
        assertThat(val3).isEqualTo(15);

        // Should be at STOP
        ThriftCompactReader.FieldHeader stop = reader.readFieldHeader();
        assertThat(stop).isNull();
    }
}
