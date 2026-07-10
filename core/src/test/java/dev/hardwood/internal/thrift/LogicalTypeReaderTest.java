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
import dev.hardwood.metadata.LogicalType;

import static org.assertj.core.api.Assertions.assertThat;

/// Unit tests for [LogicalTypeReader], focused on a logical-type union member
/// the reader does not recognize. A future or bespoke logical type must decode
/// to a null [LogicalType] rather than failing the read, so the column's
/// physical type is what gets exposed.
class LogicalTypeReaderTest {

    @Test
    void unknownUnionMemberDecodesToNull() throws Exception {
        ThriftCompactWriter writer = new ThriftCompactWriter();
        // Unrecognized field id 19 carrying an empty marker struct, the shape
        // most known logical types take.
        writer.writeFieldBegin(19, FieldType.STRUCT);
        writer.writeFieldStop(); // empty member struct
        writer.writeFieldStop(); // union STOP

        assertThat(read(writer)).isNull();
    }

    @Test
    void unknownParameterizedUnionMemberSkippedCleanly() throws Exception {
        ThriftCompactWriter writer = new ThriftCompactWriter();
        // Unknown member (field id 20) whose struct carries a parameter, the
        // shape a future parameterized logical type would take. The reader must
        // skip the whole nested struct without desyncing the surrounding parse.
        writer.writeFieldBegin(20, FieldType.STRUCT);
        short savedMember = writer.pushFieldIdContext();
        writer.writeFieldBegin(1, FieldType.I32);
        writer.writeI32(42);
        writer.writeFieldStop(); // member struct STOP
        writer.popFieldIdContext(savedMember);
        writer.writeFieldStop(); // union STOP

        // A sentinel field after the union proves the reader stopped exactly at
        // the union STOP: a skip that over- or under-ran would not read it back.
        writer.pushFieldIdContext(); // sentinel is relative to a fresh struct
        writer.writeFieldBegin(1, FieldType.I32);
        writer.writeI32(7);

        ThriftCompactReader reader = new ThriftCompactReader(ByteBuffer.wrap(writer.toByteArray()));
        assertThat(LogicalTypeReader.read(reader)).isNull();

        ThriftCompactReader.FieldHeader sentinel = reader.readFieldHeader();
        assertThat(sentinel.fieldId()).isEqualTo((short) 1);
        assertThat(reader.readI32()).isEqualTo(7);
    }

    private static LogicalType read(ThriftCompactWriter writer) throws Exception {
        return LogicalTypeReader.read(new ThriftCompactReader(ByteBuffer.wrap(writer.toByteArray())));
    }
}
