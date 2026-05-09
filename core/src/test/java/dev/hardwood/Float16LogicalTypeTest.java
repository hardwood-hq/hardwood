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
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.FilterPredicate;
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

    /// `getFloat16` against a column whose physical type is FLBA but whose payload
    /// is not a 2-byte half-precision value (here: an INTERVAL column, FLBA(12))
    /// must surface an `IllegalArgumentException` end-to-end through the row reader,
    /// not a `ClassCastException` or a silently misdecoded value.
    @Test
    void testGetFloat16OnNonFloat16ColumnRaisesIllegalArgumentException() throws IOException {
        Path intervalFile = Paths.get("src/test/resources/interval_logical_type_test.parquet");
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(intervalFile));
             RowReader rowReader = fileReader.rowReader()) {
            rowReader.next();
            assertThatThrownBy(() -> rowReader.getFloat16("duration"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("2 bytes");
        }
    }

    /// `FilterPredicate.gt(col, 0.5f)` against a FLOAT16 column dispatches in the
    /// resolver to a `Float16Predicate`, which decodes the 2-byte payload before
    /// comparing. From the caller's perspective the call is identical to filtering
    /// a physical FLOAT column.
    @Test
    void testFloatPredicateOnFloat16ColumnFiltersByDecodedValue() throws IOException {
        // half values: 0.0, 1.0, -1.5, 65504.0, +Inf, NaN, null.
        // Float.compare orders NaN after all finite values and +Inf, and treats
        // +0.0 > -0.0; with `gt 0.5f` we keep 1.0, 65504.0, +Inf, NaN — null drops.
        List<Float> kept = new ArrayList<>();
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FILE));
             RowReader rowReader = fileReader.buildRowReader()
                     .filter(FilterPredicate.gt("half", 0.5f))
                     .build()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                kept.add(rowReader.getFloat16("half"));
            }
        }
        assertThat(kept).hasSize(4);
        assertThat(kept.get(0)).isEqualTo(1.0f);
        assertThat(kept.get(1)).isEqualTo(65504.0f);
        assertThat(kept.get(2)).isEqualTo(Float.POSITIVE_INFINITY);
        assertThat(Float.isNaN(kept.get(3))).isTrue();
    }

    /// `getFloat16` must work through the [dev.hardwood.internal.reader.FilteredRowReader]
    /// delegating wrapper installed by `buildRowReader().filter(...)`.
    @Test
    void testGetFloat16ThroughFilteredRowReader() throws IOException {
        // id=1..7 maps to half=0.0, 1.0, -1.5, 65504.0, +Inf, NaN, null. With id>3
        // the filtered reader yields rows id=4..7.
        List<Float> filtered = new ArrayList<>();
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FILE));
             RowReader rowReader = fileReader.buildRowReader()
                     .filter(FilterPredicate.gt("id", 3))
                     .build()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                filtered.add(rowReader.getFloat16("half"));
            }
        }
        assertThat(filtered).hasSize(4);
        assertThat(filtered.get(0)).isEqualTo(65504.0f);
        assertThat(filtered.get(1)).isEqualTo(Float.POSITIVE_INFINITY);
        assertThat(filtered.get(2)).isNotNull();
        assertThat(Float.isNaN(filtered.get(2))).isTrue();
        assertThat(filtered.get(3)).isNull();
    }
}
