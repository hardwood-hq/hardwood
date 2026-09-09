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

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType;
import dev.hardwood.metadata.BoundingBox;
import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BoundingBoxReaderTest {

    @Test
    void readsAllFieldsWhenPresent() throws IOException {
        byte[] thrift = bbox()
                .field(1, FieldType.DOUBLE).doubleValue(-4.0)
                .field(2, FieldType.DOUBLE).doubleValue(7.5)
                .field(3, FieldType.DOUBLE).doubleValue(20.96)
                .field(4, FieldType.DOUBLE).doubleValue(77.08)
                .field(5, FieldType.DOUBLE).doubleValue(10.5)
                .field(6, FieldType.DOUBLE).doubleValue(90.0)
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
                .field(1, FieldType.DOUBLE).doubleValue(0.0)
                .field(2, FieldType.DOUBLE).doubleValue(1.0)
                .field(3, FieldType.DOUBLE).doubleValue(0.0)
                .stop().build();

        assertThatThrownBy(() -> BoundingBoxReader.read(new ThriftCompactReader(ByteBuffer.wrap(thrift))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("BoundingBox is missing required field: ymax");
    }

    @Test
    void throwsOnWrongWireTypeForRequiredField() {
        // Field 1 (xmin) declared as I32 instead of DOUBLE
        byte[] thrift = bbox()
                .field(1, FieldType.I32).i32(0)
                .stop().build();

        assertThatThrownBy(() -> BoundingBoxReader.read(new ThriftCompactReader(ByteBuffer.wrap(thrift))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("BoundingBox.xmin \u2014 wrong Thrift wire type 0x5 (expected 0x7)");
    }

    private static ThriftStructBuilder bbox() {
        return new ThriftStructBuilder();
    }
}
