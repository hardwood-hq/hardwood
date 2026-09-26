/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType;
import dev.hardwood.reader.ParquetReadException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The required fields of the schema, logical-type and page-index structs fail where they
/// arrive carrying the wrong wire type, and are named at the struct's STOP when they never
/// arrive, rather than taking a default. A malformed `KeyValue` entry instead drops the
/// optional list it sits in.
class MetadataRequiredFieldsTest {

    private static ThriftCompactReader reader(byte[] bytes) {
        return new ThriftCompactReader(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN));
    }

    /// A `LogicalType` union whose member `arm` holds `member`.
    private static byte[] logicalType(int arm, byte[] member) {
        return new ThriftStructBuilder()
                .field(arm, FieldType.STRUCT).nested(member)
                .stop().build();
    }

    private static byte[] timeUnitMillis() {
        byte[] empty = new ThriftStructBuilder().stop().build();
        return new ThriftStructBuilder()
                .field(1, FieldType.STRUCT).nested(empty)
                .stop().build();
    }

    @Test
    void schemaElementNameOfWrongWireTypeRejected() {
        byte[] element = new ThriftStructBuilder()
                .field(4, FieldType.I32).i32(1)
                .stop().build();
        assertThatThrownBy(() -> SchemaElementReader.read(reader(element)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("SchemaElement.name — wrong Thrift wire type 0x5 (expected 0x8)");
    }

    @Test
    void schemaElementWithoutNameRejected() {
        byte[] element = new ThriftStructBuilder()
                .field(5, FieldType.I32).i32(0)
                .stop().build();
        assertThatThrownBy(() -> SchemaElementReader.read(reader(element)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("SchemaElement is missing required field: name");
    }

    /// `key_value_metadata` is optional wherever it appears, so an entry without its required
    /// key drops the whole list, as a writer that omits the field would leave it, and the
    /// struct around it reads on.
    @Test
    void keyValueWithoutKeyDropsTheList() {
        byte[] valid = new ThriftStructBuilder()
                .field(1, FieldType.BINARY).binary("k".getBytes(UTF_8))
                .field(2, FieldType.BINARY).binary("v".getBytes(UTF_8))
                .stop().build();
        byte[] keyless = new ThriftStructBuilder()
                .field(2, FieldType.BINARY).binary("v".getBytes(UTF_8))
                .stop().build();
        byte[] list = new ThriftStructBuilder().structList(valid, keyless).raw(0x7f).build();
        ThriftCompactReader reader = reader(list);

        assertThat(KeyValueMetadataReader.read(reader)).isEmpty();
        assertThat(reader.readByte()).isEqualTo((byte) 0x7f);
    }

    @Test
    void keyValueKeyOfWrongWireTypeDropsTheList() {
        byte[] entry = new ThriftStructBuilder()
                .field(1, FieldType.I32).i32(1)
                .field(2, FieldType.BINARY).binary("v".getBytes(UTF_8))
                .stop().build();
        byte[] list = new ThriftStructBuilder().structList(entry).raw(0x7f).build();
        ThriftCompactReader reader = reader(list);

        assertThat(KeyValueMetadataReader.read(reader)).isEmpty();
        assertThat(reader.readByte()).isEqualTo((byte) 0x7f);
    }

    @Test
    void decimalTypeWithoutScaleRejected() {
        byte[] decimal = new ThriftStructBuilder()
                .field(2, FieldType.I32).i32(10)
                .stop().build();
        assertThatThrownBy(() -> LogicalTypeReader.read(reader(logicalType(5, decimal))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("DecimalType is missing required field: scale");
    }

    @Test
    void decimalTypePrecisionOfWrongWireTypeRejected() {
        byte[] decimal = new ThriftStructBuilder()
                .field(1, FieldType.I32).i32(2)
                .field(2, FieldType.I64).i64(10)
                .stop().build();
        assertThatThrownBy(() -> LogicalTypeReader.read(reader(logicalType(5, decimal))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("DecimalType.precision — wrong Thrift wire type 0x6 (expected 0x5)");
    }

    @Test
    void timeTypeWithoutIsAdjustedToUtcRejected() {
        // Defaulting it would read local times as UTC instants.
        byte[] time = new ThriftStructBuilder()
                .field(2, FieldType.STRUCT).nested(timeUnitMillis())
                .stop().build();
        assertThatThrownBy(() -> LogicalTypeReader.read(reader(logicalType(7, time))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("TimeType is missing required field: isAdjustedToUTC");
    }

    @Test
    void timeTypeUnitOfWrongWireTypeRejected() {
        byte[] time = new ThriftStructBuilder()
                .field(1, FieldType.BOOLEAN_TRUE)
                .field(2, FieldType.I32).i32(1)
                .stop().build();
        assertThatThrownBy(() -> LogicalTypeReader.read(reader(logicalType(7, time))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("TimeType.unit — wrong Thrift wire type 0x5 (expected 0xc)");
    }

    @Test
    void timestampTypeWithoutUnitRejected() {
        // Defaulting it would read the values at a scale the footer never declared.
        byte[] timestamp = new ThriftStructBuilder()
                .field(1, FieldType.BOOLEAN_FALSE)
                .stop().build();
        assertThatThrownBy(() -> LogicalTypeReader.read(reader(logicalType(8, timestamp))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("TimestampType is missing required field: unit");
    }

    @Test
    void timestampTypeIsAdjustedToUtcOfWrongWireTypeRejected() {
        byte[] timestamp = new ThriftStructBuilder()
                .field(1, FieldType.I32).i32(1)
                .field(2, FieldType.STRUCT).nested(timeUnitMillis())
                .stop().build();
        assertThatThrownBy(() -> LogicalTypeReader.read(reader(logicalType(8, timestamp))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("TimestampType.isAdjustedToUTC — wrong Thrift wire type 0x5 (expected 0x1 or 0x2)");
    }

    @Test
    void intTypeWithoutIsSignedRejected() {
        byte[] intType = new ThriftStructBuilder()
                .field(1, FieldType.BYTE).raw(32)
                .stop().build();
        assertThatThrownBy(() -> LogicalTypeReader.read(reader(logicalType(10, intType))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("IntType is missing required field: isSigned");
    }

    @Test
    void intTypeBitWidthOfWrongWireTypeRejected() {
        byte[] intType = new ThriftStructBuilder()
                .field(1, FieldType.I32).i32(32)
                .field(2, FieldType.BOOLEAN_TRUE)
                .stop().build();
        assertThatThrownBy(() -> LogicalTypeReader.read(reader(logicalType(10, intType))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("IntType.bitWidth — wrong Thrift wire type 0x5 (expected 0x3)");
    }

    @Test
    void logicalTypeMemberArmOfWrongWireTypeRejected() {
        // Parsed as a DecimalType anyway, the arm's varint would be read as field headers.
        byte[] logicalType = new ThriftStructBuilder()
                .field(5, FieldType.I32).i32(10)
                .stop().build();
        assertThatThrownBy(() -> LogicalTypeReader.read(reader(logicalType)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("LogicalType.DECIMAL — wrong Thrift wire type 0x5 (expected 0xc)");
    }

    @Test
    void logicalTypeEmptyArmOfWrongWireTypeRejected() {
        byte[] logicalType = new ThriftStructBuilder()
                .field(1, FieldType.I32).i32(0)
                .stop().build();
        assertThatThrownBy(() -> LogicalTypeReader.read(reader(logicalType)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("LogicalType.STRING — wrong Thrift wire type 0x5 (expected 0xc)");
    }

    @Test
    void columnIndexWithoutBoundaryOrderRejected() {
        byte[] index = new ThriftStructBuilder()
                .field(1, FieldType.LIST).boolList(false)
                .field(2, FieldType.LIST).binaryList(new byte[]{ 1 })
                .field(3, FieldType.LIST).binaryList(new byte[]{ 2 })
                .stop().build();
        assertThatThrownBy(() -> ColumnIndexReader.read(reader(index)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("ColumnIndex is missing required field: boundary_order");
    }

    @Test
    void columnIndexWithoutPerPageListsRejected() {
        // Without null_pages the index describes zero pages, which every per-page length
        // check would then agree with.
        byte[] index = new ThriftStructBuilder()
                .field(4, FieldType.I32).i32(0)
                .stop().build();
        assertThatThrownBy(() -> ColumnIndexReader.read(reader(index)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("ColumnIndex is missing required fields: null_pages, min_values, max_values");
    }

    @Test
    void columnIndexMinValuesOfWrongWireTypeRejected() {
        byte[] index = new ThriftStructBuilder()
                .field(1, FieldType.LIST).boolList(false)
                .field(2, FieldType.SET).binaryList(new byte[]{ 1 })
                .stop().build();
        assertThatThrownBy(() -> ColumnIndexReader.read(reader(index)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("ColumnIndex.min_values — wrong Thrift wire type 0xa (expected 0x9)");
    }

    @Test
    void offsetIndexWithoutPageLocationsRejected() {
        // Parsed as zero pages, it left a column chunk with nothing to read.
        byte[] index = new ThriftStructBuilder()
                .field(2, FieldType.LIST).i64List(4L)
                .stop().build();
        assertThatThrownBy(() -> OffsetIndexReader.read(reader(index)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("OffsetIndex is missing required field: page_locations");
    }

    @Test
    void offsetIndexPageLocationsOfWrongWireTypeRejected() {
        byte[] index = new ThriftStructBuilder()
                .field(1, FieldType.SET).emptyStructList(0)
                .stop().build();
        assertThatThrownBy(() -> OffsetIndexReader.read(reader(index)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("OffsetIndex.page_locations — wrong Thrift wire type 0xa (expected 0x9)");
    }

    @Test
    void pageLocationWithoutRequiredFieldsRejected() {
        byte[] location = new ThriftStructBuilder()
                .field(2, FieldType.I32).i32(100)
                .stop().build();
        assertThatThrownBy(() -> PageLocationReader.read(reader(location)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("PageLocation is missing required fields: offset, first_row_index");
    }

    @Test
    void pageLocationOffsetOfWrongWireTypeRejected() {
        byte[] location = new ThriftStructBuilder()
                .field(1, FieldType.I32).i32(4)
                .stop().build();
        assertThatThrownBy(() -> PageLocationReader.read(reader(location)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("PageLocation.offset — wrong Thrift wire type 0x5 (expected 0x6)");
    }
}
