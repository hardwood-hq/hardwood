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

import dev.hardwood.metadata.ColumnIndex;

import static org.assertj.core.api.Assertions.assertThat;

class ColumnIndexReaderTest {

    private static final byte TYPE_I32 = 0x05;
    private static final byte TYPE_LIST = 0x09;

    // Thrift Compact Protocol list element type codes.
    private static final byte ELEM_BOOL = 0x01;
    private static final byte ELEM_I64 = 0x06;
    private static final byte ELEM_BINARY = 0x08;
    private static final byte ELEM_STRUCT = 0x0C;

    @Test
    void readsCoreFieldsAndSkipsHistogramAndNanCountFields() throws IOException {
        // Fields 6 (repetition_level_histograms), 7 (definition_level_histograms) and
        // 8 (nan_counts) are all list<i64> and must be skipped without affecting fields 1-5.
        byte[] thrift = struct()
                .field(1, TYPE_LIST).boolList(false, true, false)
                .field(2, TYPE_LIST).binaryList(bytes(1), bytes(0), bytes(21))
                .field(3, TYPE_LIST).binaryList(bytes(10), bytes(0), bytes(30))
                .field(4, TYPE_I32).i32(1) // ASCENDING
                .field(5, TYPE_LIST).i64List(0, 3, 0)
                .field(6, TYPE_LIST).i64List(1, 2, 3, 4)
                .field(7, TYPE_LIST).i64List(5, 6, 7, 8)
                .field(8, TYPE_LIST).i64List(0, 0, 0)
                .stop().build();

        ColumnIndex index = ColumnIndexReader.read(new ThriftCompactReader(ByteBuffer.wrap(thrift)));

        assertThat(index.nullPages()).containsExactly(false, true, false);
        assertThat(index.minValues()).hasSize(3);
        assertThat(index.minValues().get(0)).isEqualTo(bytes(1));
        assertThat(index.maxValues().get(2)).isEqualTo(bytes(30));
        assertThat(index.boundaryOrder()).isEqualTo(ColumnIndex.BoundaryOrder.ASCENDING);
        assertThat(index.nullCounts()).containsExactly(0L, 3L, 0L);
        assertThat(index.getPageCount()).isEqualTo(3);
    }

    @Test
    void skipsStructTypedFieldSevenWithoutCorruptingLaterFields() throws IOException {
        // A malformed/adversarial file places STRUCT-typed elements at field 7. The reader must
        // skip them cleanly (no geospatial decode) and still parse the surrounding fields.
        byte[] thrift = struct()
                .field(1, TYPE_LIST).boolList(false, false)
                .field(2, TYPE_LIST).binaryList(bytes(1), bytes(2))
                .field(3, TYPE_LIST).binaryList(bytes(9), bytes(9))
                .field(4, TYPE_I32).i32(0)
                .field(5, TYPE_LIST).i64List(2, 5)
                .field(7, TYPE_LIST).emptyStructList(2)
                .field(8, TYPE_LIST).i64List(0, 0)
                .stop().build();

        ColumnIndex index = ColumnIndexReader.read(new ThriftCompactReader(ByteBuffer.wrap(thrift)));

        assertThat(index.nullPages()).containsExactly(false, false);
        assertThat(index.nullCounts()).containsExactly(2L, 5L);
        assertThat(index.getPageCount()).isEqualTo(2);
    }

    private static ThriftBuilder struct() {
        return new ThriftBuilder();
    }

    private static byte[] bytes(int... values) {
        byte[] out = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = (byte) values[i];
        }
        return out;
    }

    /// Hand-rolled Thrift Compact Protocol struct builder for tests.
    private static final class ThriftBuilder {
        private final ByteBuffer buffer = ByteBuffer.allocate(512).order(ByteOrder.LITTLE_ENDIAN);
        private short lastFieldId;

        ThriftBuilder field(int id, byte type) {
            short delta = (short) (id - lastFieldId);
            if (delta > 0 && delta <= 15) {
                buffer.put((byte) ((delta << 4) | (type & 0x0F)));
            }
            else {
                buffer.put(type);
                writeZigzag(id);
            }
            lastFieldId = (short) id;
            return this;
        }

        ThriftBuilder boolList(boolean... values) {
            listHeader(values.length, ELEM_BOOL);
            for (boolean value : values) {
                buffer.put((byte) (value ? 0x01 : 0x02));
            }
            return this;
        }

        ThriftBuilder binaryList(byte[]... values) {
            listHeader(values.length, ELEM_BINARY);
            for (byte[] value : values) {
                writeVarint(value.length);
                buffer.put(value);
            }
            return this;
        }

        ThriftBuilder i64List(long... values) {
            listHeader(values.length, ELEM_I64);
            for (long value : values) {
                writeZigzag(value);
            }
            return this;
        }

        ThriftBuilder emptyStructList(int count) {
            listHeader(count, ELEM_STRUCT);
            for (int i = 0; i < count; i++) {
                buffer.put((byte) 0); // empty struct: immediate STOP
            }
            return this;
        }

        ThriftBuilder i32(int value) {
            writeZigzag(value);
            return this;
        }

        ThriftBuilder stop() {
            buffer.put((byte) 0);
            return this;
        }

        byte[] build() {
            byte[] out = new byte[buffer.position()];
            buffer.flip();
            buffer.get(out);
            return out;
        }

        private void listHeader(int size, byte elementType) {
            if (size < 15) {
                buffer.put((byte) ((size << 4) | (elementType & 0x0F)));
            }
            else {
                buffer.put((byte) (0xF0 | (elementType & 0x0F)));
                writeVarint(size);
            }
        }

        private void writeVarint(long value) {
            long v = value;
            while ((v & ~0x7FL) != 0) {
                buffer.put((byte) ((v & 0x7F) | 0x80));
                v >>>= 7;
            }
            buffer.put((byte) (v & 0x7F));
        }

        private void writeZigzag(long value) {
            writeVarint((value << 1) ^ (value >> 63));
        }
    }
}
