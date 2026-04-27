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
import dev.hardwood.row.PqInterval;
import dev.hardwood.schema.ColumnSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IntervalLogicalTypeTest {

    private static final Path FILE = Paths.get("src/test/resources/interval_logical_type_test.parquet");

    // Row 0: 1 month, 15 days, 1 hour (3_600_000 ms)
    // Row 1: 0 months, 30 days, 0 ms
    // Row 2: null

    private ColumnSchema durationColumn;
    private int durationIdx;
    private PqInterval row0;
    private PqInterval row1;
    private PqInterval row2;

    @BeforeAll
    void readAll() throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FILE));
             RowReader rowReader = fileReader.rowReader()) {
            durationColumn = fileReader.getFileSchema().getColumn("duration");
            durationIdx = durationColumn.columnIndex();
            rowReader.next();
            row0 = rowReader.getInterval("duration");
            rowReader.next();
            row1 = rowReader.getInterval("duration");
            rowReader.next();
            row2 = rowReader.getInterval("duration");
        }
    }

    @Test
    void testSchemaReportsIntervalLogicalTypeOnFixedLenByteArray() {
        assertThat(durationColumn.type()).isEqualTo(PhysicalType.FIXED_LEN_BYTE_ARRAY);
        assertThat(durationColumn.logicalType()).isInstanceOf(LogicalType.IntervalType.class);
    }

    @Test
    void testGetIntervalReturnsComponents() {
        assertThat(row0).isNotNull();
        assertThat(row0.months()).isEqualTo(1);
        assertThat(row0.days()).isEqualTo(15);
        assertThat(row0.milliseconds()).isEqualTo(3_600_000);

        assertThat(row1).isNotNull();
        assertThat(row1.months()).isEqualTo(0);
        assertThat(row1.days()).isEqualTo(30);
        assertThat(row1.milliseconds()).isEqualTo(0);
    }

    @Test
    void testGetIntervalByIndexReturnsSameValue() throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FILE));
             RowReader rowReader = fileReader.rowReader()) {
            rowReader.next();
            assertThat(rowReader.getInterval(durationIdx)).isEqualTo(row0);
        }
    }

    @Test
    void testNullFieldReturnsNull() {
        assertThat(row2).isNull();
    }

    @Test
    void testConvertToIntervalRejectsWrongPhysicalType() {
        assertThatThrownBy(() ->
                LogicalTypeConverter.convertToInterval(0L, PhysicalType.INT64))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("FIXED_LEN_BYTE_ARRAY");
    }

    @Test
    void testConvertToIntervalRejectsWrongByteLength() {
        assertThatThrownBy(() ->
                LogicalTypeConverter.convertToInterval(new byte[8], PhysicalType.FIXED_LEN_BYTE_ARRAY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("12 bytes");
    }

    /// Files written by older parquet-mr / Spark / Hive set only the legacy
    /// `converted_type=INTERVAL` annotation, not the modern `LogicalType.IntervalType`
    /// union member. The schema builder must promote those to `IntervalType` so
    /// that `getInterval` works against such files.
    @Test
    void testLegacyConvertedTypeIsPromotedToIntervalLogicalType() throws IOException {
        Path legacyFile = Paths.get("src/test/resources/interval_legacy_converted_type_test.parquet");
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(legacyFile));
             RowReader rowReader = fileReader.rowReader()) {
            ColumnSchema column = fileReader.getFileSchema().getColumn("duration");
            assertThat(column.logicalType()).isInstanceOf(LogicalType.IntervalType.class);

            rowReader.next();
            PqInterval first = rowReader.getInterval("duration");
            assertThat(first.months()).isEqualTo(1);
            assertThat(first.days()).isEqualTo(15);
            assertThat(first.milliseconds()).isEqualTo(3_600_000);
        }
    }
}
