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
import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

        int sentinel = reader.readFieldHeader();
        assertThat(ThriftCompactReader.fieldId(sentinel)).isEqualTo((short) 1);
        assertThat(reader.readI32()).isEqualTo(7);
    }

    /// `INT` names one of four bit widths. A footer naming any other describes an annotation
    /// the format does not define, which is the file's fault and not the caller's, so it is
    /// reported as a read failure rather than as the record's `IllegalArgumentException`.
    @Test
    void anUndefinedBitWidthIsAReadFailure() throws Exception {
        assertThatThrownBy(() -> read(intType((byte) 7, true)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid IntType: bitWidth=7");
    }

    /// `bitWidth` is a required field, so a member struct that omits it states no width at
    /// all. Defaulting it would read the column as an `INT(8)` the footer never declared.
    @Test
    void anAbsentBitWidthIsAReadFailure() throws Exception {
        ThriftCompactWriter writer = new ThriftCompactWriter();
        writer.writeFieldBegin(10, FieldType.STRUCT);
        short savedMember = writer.pushFieldIdContext();
        writer.writeBool(2, false);
        writer.writeFieldStop(); // member struct STOP
        writer.popFieldIdContext(savedMember);
        writer.writeFieldStop(); // union STOP

        assertThatThrownBy(() -> read(writer))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid IntType: bitWidth=-1");
    }

    @Test
    void everyDefinedBitWidthDecodes() throws Exception {
        for (byte bitWidth : new byte[] { 8, 16, 32, 64 }) {
            assertThat(read(intType(bitWidth, false)))
                    .isEqualTo(LogicalType.intType(bitWidth, false));
        }
    }

    /// The `INT` union member, as a footer carries it: field id 10 holding an `IntType`
    /// struct of an i8 `bitWidth` and a bool `isSigned`.
    private static ThriftCompactWriter intType(byte bitWidth, boolean isSigned) {
        ThriftCompactWriter writer = new ThriftCompactWriter();
        writer.writeFieldBegin(10, FieldType.STRUCT);
        short savedMember = writer.pushFieldIdContext();
        writer.writeFieldBegin(1, FieldType.BYTE);
        writer.writeByte(bitWidth);
        writer.writeBool(2, isSigned);
        writer.writeFieldStop(); // member struct STOP
        writer.popFieldIdContext(savedMember);
        writer.writeFieldStop(); // union STOP
        return writer;
    }

    private static LogicalType read(ThriftCompactWriter writer) throws Exception {
        return LogicalTypeReader.read(new ThriftCompactReader(ByteBuffer.wrap(writer.toByteArray())));
    }
}
