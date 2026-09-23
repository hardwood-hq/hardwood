/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqInterval;
import dev.hardwood.schema.ColumnSchema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

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
    void testUnsignedValuesAboveIntegerMaxValueAreReturnedAsPositiveLongs() {
        // 0xFFFFFFFF = 4_294_967_295 — max unsigned 32-bit value, would be -1 as signed int
        long maxUint32 = 0xFFFFFFFFL;
        ByteBuffer buf = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt((int) maxUint32);
        buf.putInt((int) maxUint32);
        buf.putInt((int) maxUint32);
        PqInterval interval = LogicalTypeConverter.bytesToInterval(buf.array());
        assertThat(interval.months()).isEqualTo(maxUint32);
        assertThat(interval.days()).isEqualTo(maxUint32);
        assertThat(interval.milliseconds()).isEqualTo(maxUint32);
    }

    @Test
    void testIntervalRejectsWrongByteLength() {
        assertThatThrownBy(() ->
                LogicalTypeConverter.bytesToInterval(new byte[8]))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("INTERVAL requires exactly 12 bytes, got 8");
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

    /// parquet-java writes `converted_type=INTERVAL` beside the `LogicalType` union's `UNKNOWN`
    /// member, as the union has no `INTERVAL` member. The converted type decides the column's
    /// annotation, so the column reads and filters as an interval rather than as a `NULL` column.
    @Test
    void testParquetJavaFooterIsReadAsInterval() throws IOException {
        Path parquetJavaFile = Paths.get("src/test/resources/interval_parquet_java_test.parquet");
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(parquetJavaFile))) {
            ColumnSchema column = fileReader.getFileSchema().getColumn("duration");
            assertThat(column.logicalType()).isEqualTo(LogicalType.interval());

            try (RowReader rowReader = fileReader.rowReader()) {
                rowReader.next();
                assertThat(rowReader.getInterval("duration")).isEqualTo(row0);
                assertThat(rowReader.getValue("duration")).isEqualTo(row0);
            }

            assertThat(filteredIds(fileReader, FilterPredicate.eq("duration", row1))).containsExactly(2);
            assertThat(filteredIds(fileReader, FilterPredicate.notEq("duration", row1))).containsExactly(1);
            assertThat(filteredIds(fileReader, FilterPredicate.in("duration", row0, row1))).containsExactly(1, 2);
        }
    }

    private static List<Integer> filteredIds(ParquetFileReader fileReader, FilterPredicate filter)
            throws IOException {
        List<Integer> ids = new ArrayList<>();
        try (RowReader rowReader = fileReader.buildRowReader().filter(filter).build()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                ids.add(rowReader.getInt("id"));
            }
        }
        return ids;
    }
}
