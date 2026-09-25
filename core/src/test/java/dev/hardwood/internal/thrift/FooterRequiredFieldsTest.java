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

import dev.hardwood.internal.thrift.ThriftCompactConstants.ElementType;
import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType;
import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The required fields of the footer structs (`FileMetaData`, `RowGroup`, `ColumnChunk`,
/// `ColumnMetaData`) fail where they arrive carrying the wrong wire type, and are named at
/// the struct's STOP when they never arrive, rather than taking a default.
class FooterRequiredFieldsTest {

    private static ThriftCompactReader reader(byte[] bytes) {
        return new ThriftCompactReader(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN));
    }

    @Test
    void rowGroupColumnsOfWrongWireTypeRejected() {
        // columns sent as an empty set: skipped, it would leave a row group of 100 rows
        // and no columns, which a query answers with zero rows.
        byte[] rowGroup = new ThriftStructBuilder()
                .field(1, FieldType.SET).listHeaderOnly(0, ElementType.STRUCT)
                .field(2, FieldType.I64).i64(1024)
                .field(3, FieldType.I64).i64(100)
                .stop().build();
        assertThatThrownBy(() -> RowGroupReader.read(reader(rowGroup)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("RowGroup.columns — wrong Thrift wire type 0xa (expected 0x9)");
    }

    @Test
    void rowGroupWithoutRequiredFieldsRejected() {
        byte[] rowGroup = new ThriftStructBuilder()
                .field(2, FieldType.I64).i64(1024)
                .stop().build();
        assertThatThrownBy(() -> RowGroupReader.read(reader(rowGroup)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("RowGroup is missing required fields: columns, num_rows");
    }

    @Test
    void fileMetaDataNumRowsOfWrongWireTypeRejected() {
        byte[] footer = new ThriftStructBuilder()
                .field(1, FieldType.I32).i32(1)
                .field(3, FieldType.I32).i32(100)
                .stop().build();
        assertThatThrownBy(() -> FileMetaDataReader.read(reader(footer)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("FileMetaData.num_rows — wrong Thrift wire type 0x5 (expected 0x6)");
    }

    @Test
    void fileMetaDataWithoutRequiredFieldsRejected() {
        byte[] footer = new ThriftStructBuilder().stop().build();
        assertThatThrownBy(() -> FileMetaDataReader.read(reader(footer)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("FileMetaData is missing required fields: version, schema, num_rows, "
                        + "row_groups");
    }

    @Test
    void columnChunkFileOffsetOfWrongWireTypeRejected() {
        byte[] chunk = new ThriftStructBuilder()
                .field(2, FieldType.I32).i32(4)
                .stop().build();
        assertThatThrownBy(() -> ColumnChunkReader.read(reader(chunk)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("ColumnChunk.file_offset — wrong Thrift wire type 0x5 (expected 0x6)");
    }

    @Test
    void columnChunkWithoutFileOffsetRejected() {
        byte[] chunk = new ThriftStructBuilder()
                .field(5, FieldType.I32).i32(64)
                .stop().build();
        assertThatThrownBy(() -> ColumnChunkReader.read(reader(chunk)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("ColumnChunk is missing required field: file_offset");
    }

    @Test
    void columnMetaDataCodecOfWrongWireTypeRejected() {
        byte[] metaData = new ThriftStructBuilder()
                .field(4, FieldType.I64).i64(1)
                .stop().build();
        assertThatThrownBy(() -> ColumnMetaDataReader.read(reader(metaData)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("ColumnMetaData.codec — wrong Thrift wire type 0x6 (expected 0x5)");
    }

    @Test
    void columnMetaDataWithoutRequiredFieldsRejected() {
        byte[] metaData = new ThriftStructBuilder()
                .field(1, FieldType.I32).i32(1)
                .field(5, FieldType.I64).i64(10)
                .stop().build();
        assertThatThrownBy(() -> ColumnMetaDataReader.read(reader(metaData)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("ColumnMetaData is missing required fields: encodings, path_in_schema, "
                        + "codec, total_uncompressed_size, total_compressed_size, data_page_offset");
    }
}
