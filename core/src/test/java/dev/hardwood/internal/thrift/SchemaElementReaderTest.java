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
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    /// A legacy `DECIMAL` takes its digits from the element's own `precision` and `scale`. A footer
    /// whose pair the format does not admit is a malformed file, refused as the union's
    /// `DecimalType` is, where it is parsed.
    @Test
    void aLegacyDecimalWithoutAPrecisionIsAReadFailure() {
        assertThatThrownBy(() -> read(legacyDecimal(2, null, Union.NONE)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid DECIMAL converted type on column 'col': scale=2, precision=absent");
    }

    @Test
    void aLegacyDecimalWithAScaleAboveItsPrecisionIsAReadFailure() {
        assertThatThrownBy(() -> read(legacyDecimal(5, 3, Union.NONE)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid DECIMAL converted type on column 'col': scale=5, precision=3");
    }

    /// Beside a union `DECIMAL`, which carries its own digits, the element's pair decides nothing.
    @Test
    void aLegacyDecimalBesideTheUnionIsNotWeighed() throws Exception {
        assertThat(read(legacyDecimal(5, 3, Union.DECIMAL)).logicalType()).isEqualTo(LogicalType.decimal(9, 2));
    }

    /// Beside `UNKNOWN`, or a member this release does not recognize, the converted type decides,
    /// so its pair is weighed as when it stands alone.
    @Test
    void aLegacyDecimalBesideUnknownOrAnUnrecognizedMemberIsWeighed() {
        assertThatThrownBy(() -> read(legacyDecimal(5, 3, Union.UNKNOWN)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid DECIMAL converted type on column 'col': scale=5, precision=3");
        assertThatThrownBy(() -> read(legacyDecimal(5, 3, Union.UNRECOGNIZED)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid DECIMAL converted type on column 'col': scale=5, precision=3");
    }

    /// A group carries no `DECIMAL`, so its element's digits are never read; the schema drops the
    /// annotation rather than the footer failing to parse.
    @Test
    void aDecimalConvertedTypeOnAGroupIsNotWeighed() throws Exception {
        ThriftCompactWriter writer = new ThriftCompactWriter();
        writer.writeFieldBegin(4, FieldType.BINARY);
        writer.writeString("g");
        writer.writeFieldBegin(5, FieldType.I32);
        writer.writeI32(1);
        writer.writeFieldBegin(6, FieldType.I32);
        writer.writeI32(5); // ConvertedType.DECIMAL, with no precision
        writer.writeFieldStop();

        assertThat(read(writer).numChildren()).isEqualTo(1);
    }

    @Test
    void aNegativeChildCountIsAReadFailure() {
        ThriftCompactWriter writer = new ThriftCompactWriter();
        writer.writeFieldBegin(4, FieldType.BINARY);
        writer.writeString("g");
        writer.writeFieldBegin(5, FieldType.I32);
        writer.writeI32(-1);
        writer.writeFieldStop();

        assertThatThrownBy(() -> read(writer))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("SchemaElement.num_children — must be non-negative but was -1");
    }

    @Test
    void aLegacyDecimalWithAnAdmittedPairParses() throws Exception {
        SchemaElement element = read(legacyDecimal(null, 9, Union.NONE));
        assertThat(element.precision()).isEqualTo(9);
        assertThat(element.scale()).isNull();
    }

    /// The union member written beside a legacy `DECIMAL`.
    private enum Union {
        NONE,
        DECIMAL,
        UNKNOWN,
        UNRECOGNIZED
    }

    /// An `INT32` element named `col` annotated with the legacy `DECIMAL` converted type, beside
    /// the union member `union` names: `DECIMAL(9, 2)`, `UNKNOWN`, or one with field id 30.
    private static ThriftCompactWriter legacyDecimal(Integer scale, Integer precision, Union union) {
        ThriftCompactWriter writer = new ThriftCompactWriter();
        writer.writeFieldBegin(1, FieldType.I32);
        writer.writeI32(1);
        writer.writeFieldBegin(4, FieldType.BINARY);
        writer.writeString("col");
        writer.writeFieldBegin(6, FieldType.I32);
        writer.writeI32(5); // ConvertedType.DECIMAL
        if (scale != null) {
            writer.writeFieldBegin(7, FieldType.I32);
            writer.writeI32(scale);
        }
        if (precision != null) {
            writer.writeFieldBegin(8, FieldType.I32);
            writer.writeI32(precision);
        }
        if (union != Union.NONE) {
            writer.writeFieldBegin(10, FieldType.STRUCT);
            short savedUnion = writer.pushFieldIdContext();
            int memberFieldId = switch (union) {
                case DECIMAL -> 5;
                case UNKNOWN -> 11;
                case UNRECOGNIZED -> 30;
                case NONE -> throw new IllegalStateException();
            };
            writer.writeFieldBegin(memberFieldId, FieldType.STRUCT);
            short savedMember = writer.pushFieldIdContext();
            if (union == Union.DECIMAL) {
                writer.writeFieldBegin(1, FieldType.I32);
                writer.writeI32(2);
                writer.writeFieldBegin(2, FieldType.I32);
                writer.writeI32(9);
            }
            writer.writeFieldStop();
            writer.popFieldIdContext(savedMember);
            writer.writeFieldStop();
            writer.popFieldIdContext(savedUnion);
        }
        writer.writeFieldStop();
        return writer;
    }

    private static SchemaElement read(ThriftCompactWriter writer) throws Exception {
        return SchemaElementReader.read(new ThriftCompactReader(ByteBuffer.wrap(writer.toByteArray())));
    }
}
