/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Float16LogicalTypeTest {

    private static final Path FILE = Paths.get("src/test/resources/float16_logical_type_test.parquet");

    // Row 0: 0.0
    // Row 1: 1.0
    // Row 2: -1.5
    // Row 3: 65504.0  (max finite binary16)
    // Row 4: +Infinity
    // Row 5: NaN
    // Row 6: null

    private ColumnSchema halfColumn;
    private int halfIdx;
    private Float row0;
    private Float row1;
    private Float row2;
    private Float row3;
    private Float row4;
    private Float row5;
    private Float row6;

    @BeforeAll
    void readAll() throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FILE));
             RowReader rowReader = fileReader.rowReader()) {
            halfColumn = fileReader.getFileSchema().getColumn("half");
            halfIdx = halfColumn.columnIndex();
            rowReader.next();
            row0 = rowReader.getFloat16("half");
            rowReader.next();
            row1 = rowReader.getFloat16("half");
            rowReader.next();
            row2 = rowReader.getFloat16("half");
            rowReader.next();
            row3 = rowReader.getFloat16("half");
            rowReader.next();
            row4 = rowReader.getFloat16("half");
            rowReader.next();
            row5 = rowReader.getFloat16("half");
            rowReader.next();
            row6 = rowReader.getFloat16("half");
        }
    }

    @Test
    void testSchemaReportsFloat16LogicalTypeOnFixedLenByteArray() {
        assertThat(halfColumn.type()).isEqualTo(PhysicalType.FIXED_LEN_BYTE_ARRAY);
        assertThat(halfColumn.typeLength()).isEqualTo(2);
        assertThat(halfColumn.logicalType()).isInstanceOf(LogicalType.Float16Type.class);
    }

    @Test
    void testGetFloat16ReturnsDecodedValues() {
        assertThat(row0).isEqualTo(0.0f);
        assertThat(row1).isEqualTo(1.0f);
        assertThat(row2).isEqualTo(-1.5f);
        assertThat(row3).isEqualTo(65504.0f);
        assertThat(row4).isEqualTo(Float.POSITIVE_INFINITY);
        assertThat(row5).isNotNull();
        assertThat(Float.isNaN(row5)).isTrue();
    }

    @Test
    void testGetFloat16ByIndexReturnsSameValue() throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FILE));
             RowReader rowReader = fileReader.rowReader()) {
            rowReader.next();
            assertThat(rowReader.getFloat16(halfIdx)).isEqualTo(row0);
        }
    }

    @Test
    void testNullFieldReturnsNull() {
        assertThat(row6).isNull();
    }

    @Test
    void testConvertToFloat16RejectsWrongPhysicalType() {
        assertThatThrownBy(() ->
                LogicalTypeConverter.convertToFloat16(0L, PhysicalType.INT64))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("FIXED_LEN_BYTE_ARRAY");
    }

    @Test
    void testConvertToFloat16RejectsWrongByteLength() {
        assertThatThrownBy(() ->
                LogicalTypeConverter.convertToFloat16(new byte[4], PhysicalType.FIXED_LEN_BYTE_ARRAY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("2 bytes");
    }
}
