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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The required fields of the page header structs (`PageHeader`, `DataPageHeader`,
/// `DataPageHeaderV2`, `DictionaryPageHeader`) fail where they arrive carrying the wrong wire
/// type, and are named at the struct's STOP when they never arrive, rather than taking a
/// default.
class PageHeaderRequiredFieldsTest {

    private static ThriftCompactReader reader(byte[] bytes) {
        return new ThriftCompactReader(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN));
    }

    @Test
    void pageHeaderCompressedSizeOfWrongWireTypeRejected() {
        byte[] header = new ThriftStructBuilder()
                .field(1, FieldType.I32).i32(0)
                .field(2, FieldType.I32).i32(10)
                .field(3, FieldType.I64).i64(8)
                .stop().build();
        assertThatThrownBy(() -> PageHeaderReader.read(reader(header)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("PageHeader.compressed_page_size — wrong Thrift wire type 0x6 (expected 0x5)");
    }

    @Test
    void pageHeaderWithoutRequiredFieldsRejected() {
        // Without its sizes a page would be read as zero bytes long.
        byte[] header = new ThriftStructBuilder()
                .field(1, FieldType.I32).i32(0)
                .stop().build();
        assertThatThrownBy(() -> PageHeaderReader.read(reader(header)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("PageHeader is missing required fields: uncompressed_page_size, "
                        + "compressed_page_size");
    }

    @Test
    void dataPageHeaderNumValuesOfWrongWireTypeRejected() {
        byte[] header = new ThriftStructBuilder()
                .field(1, FieldType.I64).i64(100)
                .stop().build();
        assertThatThrownBy(() -> DataPageHeaderReader.read(reader(header)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("DataPageHeader.num_values — wrong Thrift wire type 0x6 (expected 0x5)");
    }

    @Test
    void dataPageHeaderWithoutRequiredFieldsRejected() {
        byte[] header = new ThriftStructBuilder()
                .field(2, FieldType.I32).i32(0)
                .stop().build();
        assertThatThrownBy(() -> DataPageHeaderReader.read(reader(header)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("DataPageHeader is missing required fields: num_values, "
                        + "definition_level_encoding, repetition_level_encoding");
    }

    @Test
    void dataPageHeaderV2NumRowsOfWrongWireTypeRejected() {
        byte[] header = new ThriftStructBuilder()
                .field(3, FieldType.I64).i64(100)
                .stop().build();
        assertThatThrownBy(() -> DataPageHeaderV2Reader.read(reader(header)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("DataPageHeaderV2.num_rows — wrong Thrift wire type 0x6 (expected 0x5)");
    }

    @Test
    void dataPageHeaderV2WithoutRequiredFieldsRejected() {
        byte[] header = new ThriftStructBuilder()
                .field(1, FieldType.I32).i32(100)
                .field(4, FieldType.I32).i32(0)
                .stop().build();
        assertThatThrownBy(() -> DataPageHeaderV2Reader.read(reader(header)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("DataPageHeaderV2 is missing required fields: num_nulls, num_rows, "
                        + "definition_levels_byte_length, repetition_levels_byte_length");
    }

    @Test
    void dictionaryPageHeaderEncodingOfWrongWireTypeRejected() {
        byte[] header = new ThriftStructBuilder()
                .field(1, FieldType.I32).i32(3)
                .field(2, FieldType.BINARY).binary(new byte[0])
                .stop().build();
        assertThatThrownBy(() -> DictionaryPageHeaderReader.read(reader(header)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("DictionaryPageHeader.encoding — wrong Thrift wire type 0x8 (expected 0x5)");
    }

    @Test
    void dictionaryPageHeaderWithoutRequiredFieldsRejected() {
        byte[] header = new ThriftStructBuilder().stop().build();
        assertThatThrownBy(() -> DictionaryPageHeaderReader.read(reader(header)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("DictionaryPageHeader is missing required fields: num_values, encoding");
    }
}
