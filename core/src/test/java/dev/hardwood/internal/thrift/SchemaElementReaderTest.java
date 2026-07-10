/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.SchemaElement;

import static org.assertj.core.api.Assertions.assertThat;

/// Unit tests for [SchemaElementReader]. A SchemaElement annotated with a
/// logical type the reader does not recognize must still parse, exposing its
/// physical type with a null logical type.
class SchemaElementReaderTest {

    @Test
    void unknownLogicalTypeExposesPhysicalType() throws Exception {
        ThriftCompactWriter writer = new ThriftCompactWriter();
        // field 1: type = INT32 (Thrift physical-type enum value 1)
        writer.writeFieldBegin(1, FieldType.I32);
        writer.writeI32(1);
        // field 4: name
        writer.writeFieldBegin(4, FieldType.BINARY);
        writer.writeString("col");
        // field 10: logicalType, an unrecognized parameterized union member.
        // Nesting mirrors the reader's field-id context handling so the delta
        // encoding matches: SchemaElement -> union -> member struct.
        writer.writeFieldBegin(10, FieldType.STRUCT);
        short savedUnion = writer.pushFieldIdContext();
        writer.writeFieldBegin(20, FieldType.STRUCT);
        short savedMember = writer.pushFieldIdContext();
        writer.writeFieldBegin(1, FieldType.I32);
        writer.writeI32(99);
        writer.writeFieldStop(); // member struct STOP
        writer.popFieldIdContext(savedMember);
        writer.writeFieldStop(); // union STOP
        writer.popFieldIdContext(savedUnion);
        writer.writeFieldStop(); // SchemaElement STOP

        SchemaElement element = SchemaElementReader.read(
                new ThriftCompactReader(ByteBuffer.wrap(writer.toByteArray())));

        assertThat(element.type()).isEqualTo(PhysicalType.INT32);
        assertThat(element.logicalType()).isNull();
        assertThat(element.name()).isEqualTo("col");
    }
}
