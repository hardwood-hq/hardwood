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

import dev.hardwood.metadata.BoundingBox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BoundingBoxReaderTest {

    private static final byte TYPE_DOUBLE = 0x07;

    @Test
    void readsAllFieldsWhenPresent() throws IOException {
        byte[] thrift = bbox()
                .field(1, TYPE_DOUBLE).doubleValue(-4.0)
                .field(2, TYPE_DOUBLE).doubleValue(7.5)
                .field(3, TYPE_DOUBLE).doubleValue(20.96)
                .field(4, TYPE_DOUBLE).doubleValue(77.08)
                .field(5, TYPE_DOUBLE).doubleValue(10.5)
                .field(6, TYPE_DOUBLE).doubleValue(90.0)
                .stop().build();

        BoundingBox box = BoundingBoxReader.read(new ThriftCompactReader(ByteBuffer.wrap(thrift)));

        assertThat(box.xmin()).isEqualTo(-4.0);
        assertThat(box.xmax()).isEqualTo(7.5);
        assertThat(box.ymin()).isEqualTo(20.96);
        assertThat(box.ymax()).isEqualTo(77.08);
        assertThat(box.zmin()).isEqualTo(10.5);
        assertThat(box.zmax()).isEqualTo(90.0);
        assertThat(box.mmin()).isNull();
        assertThat(box.mmax()).isNull();
    }

    @Test
    void throwsWhenRequiredFieldMissing() {
        // Missing field 4 (ymax)
        byte[] thrift = bbox()
                .field(1, TYPE_DOUBLE).doubleValue(0.0)
                .field(2, TYPE_DOUBLE).doubleValue(1.0)
                .field(3, TYPE_DOUBLE).doubleValue(0.0)
                .stop().build();

        assertThatThrownBy(() -> BoundingBoxReader.read(new ThriftCompactReader(ByteBuffer.wrap(thrift))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ymax");
    }

    @Test
    void throwsOnWrongWireTypeForRequiredField() {
        // Field 1 (xmin) declared as I32 (0x05) instead of DOUBLE (0x07)
        byte[] thrift = bbox()
                .field(1, (byte) 0x05).i32(0)
                .stop().build();

        assertThatThrownBy(() -> BoundingBoxReader.read(new ThriftCompactReader(ByteBuffer.wrap(thrift))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("xmin");
    }

    private static ThriftBuilder bbox() {
        return new ThriftBuilder();
    }

    /// Hand-rolled Thrift Compact Protocol struct builder for tests.
    private static final class ThriftBuilder {
        private final ByteBuffer buffer = ByteBuffer.allocate(256).order(ByteOrder.LITTLE_ENDIAN);
        private short lastFieldId;
        private byte pendingFieldType;

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
            pendingFieldType = type;
            return this;
        }

        ThriftBuilder doubleValue(double v) {
            buffer.putDouble(v);
            pendingFieldType = 0;
            return this;
        }

        ThriftBuilder i32(int v) {
            writeZigzag(v);
            pendingFieldType = 0;
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

        private void writeZigzag(long v) {
            long zz = (v << 1) ^ (v >> 63);
            while ((zz & ~0x7FL) != 0) {
                buffer.put((byte) ((zz & 0x7F) | 0x80));
                zz >>>= 7;
            }
            buffer.put((byte) (zz & 0x7F));
        }
    }
}
