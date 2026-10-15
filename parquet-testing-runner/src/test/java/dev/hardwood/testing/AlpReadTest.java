/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.InputFile;
import dev.hardwood.Validity;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// Reads the ALP columns of `alp_extended.zstd.parquet` from apache/parquet-testing, written by
/// Arrow C++. Every column of the file holds the same 9032 values, among them NaNs of three bit
/// patterns, both infinities, `-0.0`, subnormals, nulls and exceptions at vector boundaries, so
/// each ALP column is compared bit for bit against its `PLAIN` sibling in the same file. The
/// columns cover vectors of 32, 1024 and 4096 values.
///
/// The fixture is skipped by [ParquetComparisonTest] because no released parquet-java can read
/// it — its generated `Encoding` enum stops at `BYTE_STREAM_SPLIT` — so this is the only coverage
/// the file gets.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AlpReadTest {

    private Path file;

    @BeforeAll
    void locateFixture() throws IOException {
        file = ParquetTestingRepoCloner.getTestFile("data/alp_extended.zstd.parquet");
    }

    @ParameterizedTest
    @ValueSource(strings = { "float_alp_32", "float_alp_1024", "float_alp_4096",
            "double_alp_32", "double_alp_1024", "double_alp_4096" })
    void columnsAreAlpEncoded(String column) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            for (RowGroup rowGroup : reader.getFileMetaData().rowGroups()) {
                assertThat(chunk(rowGroup, column).encodings()).as("encodings of %s", column)
                        .contains(Encoding.ALP);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "float_alp_32", "float_alp_1024", "float_alp_4096" })
    void floatColumnMatchesItsPlainSibling(String column) throws IOException {
        List<Long> expected = readFloatBits("float_plain");
        assertThat(expected).hasSize(9032).containsNull();

        assertThat(readFloatBits(column)).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = { "double_alp_32", "double_alp_1024", "double_alp_4096" })
    void doubleColumnMatchesItsPlainSibling(String column) throws IOException {
        List<Long> expected = readDoubleBits("double_plain");
        assertThat(expected).hasSize(9032).containsNull();

        assertThat(readDoubleBits(column)).isEqualTo(expected);
    }

    /// The row-oriented reader over the same pages, for every ALP column at once.
    @ParameterizedTest
    @ValueSource(strings = { "32", "1024", "4096" })
    void rowReaderMatchesThePlainColumns(String vectorSize) throws IOException {
        String floatColumn = "float_alp_" + vectorSize;
        String doubleColumn = "double_alp_" + vectorSize;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             RowReader rows = reader.rowReader()) {
            int row = 0;
            while (rows.hasNext()) {
                rows.next();
                assertThat(rows.isNull(floatColumn)).as("row %d", row).isEqualTo(rows.isNull("float_plain"));
                assertThat(rows.isNull(doubleColumn)).as("row %d", row).isEqualTo(rows.isNull("double_plain"));
                if (!rows.isNull("float_plain")) {
                    assertThat(Float.floatToRawIntBits(rows.getFloat(floatColumn))).as("row %d", row)
                            .isEqualTo(Float.floatToRawIntBits(rows.getFloat("float_plain")));
                }
                if (!rows.isNull("double_plain")) {
                    assertThat(Double.doubleToRawLongBits(rows.getDouble(doubleColumn))).as("row %d", row)
                            .isEqualTo(Double.doubleToRawLongBits(rows.getDouble("double_plain")));
                }
                row++;
            }
            assertThat(row).isEqualTo(9032);
        }
    }

    /// The column's raw float bits, `null` for a null value.
    private List<Long> readFloatBits(String column) throws IOException {
        List<Long> bits = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             ColumnReader columnReader = reader.columnReader(column)) {
            while (columnReader.nextBatch()) {
                float[] values = columnReader.getFloats();
                Validity validity = columnReader.getLeafValidity();
                for (int i = 0; i < columnReader.getValueCount(); i++) {
                    bits.add(validity.isNull(i) ? null : (long) Float.floatToRawIntBits(values[i]));
                }
            }
        }
        return bits;
    }

    /// The column's raw double bits, `null` for a null value.
    private List<Long> readDoubleBits(String column) throws IOException {
        List<Long> bits = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             ColumnReader columnReader = reader.columnReader(column)) {
            while (columnReader.nextBatch()) {
                double[] values = columnReader.getDoubles();
                Validity validity = columnReader.getLeafValidity();
                for (int i = 0; i < columnReader.getValueCount(); i++) {
                    bits.add(validity.isNull(i) ? null : Double.doubleToRawLongBits(values[i]));
                }
            }
        }
        return bits;
    }

    private static ColumnMetaData chunk(RowGroup rowGroup, String column) {
        return rowGroup.columns().stream()
                .map(ColumnChunk::metaData)
                .filter(cmd -> cmd.pathInSchema().matchesDottedName(column))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No column chunk for " + column));
    }
}
