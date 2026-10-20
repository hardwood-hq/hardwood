/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.BinaryValues;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Reads binary columns through the views of [ColumnReader]: each value reads back through its
/// own `[start, end)` range, a null's range is empty, and values of a dictionary-encoded column
/// naming the same entry cover the same bytes rather than a copy each.
class ColumnReaderBinaryViewsTest {

    private static final String[] STATIONS = { "Hamburg", "Oslo", "Abha" };
    private static final int ROWS = 3_000;

    private static byte[] file;
    private static byte[] fixedLengthFile;

    @BeforeAll
    static void writeFiles() throws IOException {
        FileSchema schema = FileSchema.builder("measurements")
                .addColumn("station", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, new LogicalType.StringType())
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            for (int i = 0; i < ROWS; i++) {
                String station = station(i);
                writer.rowWriter().writeRow(row -> row.setString("station", station));
            }
        }
        file = InMemoryFiles.toByteArray(out);

        FileSchema fixedLengthSchema = FileSchema.builder("codes")
                .addColumn("code", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL, 4)
                .build();
        InMemoryOutputFile fixedLengthOut = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(fixedLengthOut, fixedLengthSchema)) {
            for (int i = 0; i < ROWS; i++) {
                byte[] code = code(i);
                writer.rowWriter().writeRow(row -> {
                    if (code != null) {
                        row.setBinary("code", code);
                    }
                });
            }
        }
        fixedLengthFile = InMemoryFiles.toByteArray(fixedLengthOut);
    }

    @Test
    void readsEachValueThroughItsView() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
             ColumnReader col = reader.columnReader("station")) {

            int row = 0;
            while (col.nextBatch()) {
                BinaryValues values = col.getBinaryValues();
                assertThat(values.size()).isEqualTo(col.getValueCount());
                for (int i = 0; i < values.size(); i++, row++) {
                    String expected = station(row);
                    if (expected == null) {
                        assertThat(col.getLeafValidity().isNull(i)).isTrue();
                        assertThat(values.end(i) - values.start(i)).isEqualTo(0);
                    }
                    else {
                        assertThat(new String(values.buffer(i), values.start(i), values.end(i) - values.start(i),
                                StandardCharsets.UTF_8)).isEqualTo(expected);
                    }
                }
            }
            assertThat(row).isEqualTo(ROWS);
        }
    }

    @Test
    void valuesOfOneDictionaryEntryCoverTheSameBytes() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
             ColumnReader col = reader.columnReader("station")) {

            assertThat(col.nextBatch()).isTrue();
            BinaryValues values = col.getBinaryValues();

            // Rows 0, 4 and 8 all hold "Hamburg".
            assertThat(values.buffer(4)).isSameAs(values.buffer(0));
            assertThat(values.start(4)).isEqualTo(values.start(0));
            assertThat(values.buffer(8)).isSameAs(values.buffer(0));
            assertThat(values.start(8)).isEqualTo(values.start(0));
        }
    }

    /// A `FIXED_LEN_BYTE_ARRAY` null has an empty range like any other null, not a slot of the
    /// column's width.
    @Test
    void aFixedLengthNullHasAnEmptyRange() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(fixedLengthFile)));
             ColumnReader col = reader.columnReader("code")) {

            int row = 0;
            while (col.nextBatch()) {
                BinaryValues values = col.getBinaryValues();
                for (int i = 0; i < values.size(); i++, row++) {
                    byte[] expected = code(row);
                    if (expected == null) {
                        assertThat(col.getLeafValidity().isNull(i)).isTrue();
                        assertThat(values.length(i)).isEqualTo(0);
                    }
                    else {
                        assertThat(Arrays.copyOfRange(values.buffer(i), values.start(i), values.end(i)))
                                .isEqualTo(expected);
                    }
                }
            }
            assertThat(row).isEqualTo(ROWS);
        }
    }

    /// The conveniences read the same bytes as the window.
    @Test
    void conveniencesReadTheWindow() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
             ColumnReader col = reader.columnReader("station")) {

            assertThat(col.nextBatch()).isTrue();
            BinaryValues values = col.getBinaryValues();
            byte[] copy = new byte[64];
            for (int i = 0; i < values.size(); i++) {
                byte[] window = Arrays.copyOfRange(values.buffer(i), values.start(i), values.end(i));
                assertThat(values.length(i)).isEqualTo(window.length);
                assertThat(values.getBinary(i)).isEqualTo(window);
                values.copyTo(i, copy, 3);
                assertThat(Arrays.copyOfRange(copy, 3, 3 + window.length)).isEqualTo(window);
                assertThat(values.contentEquals(i, copy, 3, window.length)).isTrue();
                assertThat(values.contentEquals(i, copy, 2, window.length)).isEqualTo(window.length == 0);
            }
        }
    }

    @Test
    void refusesAValueOutsideTheBatch() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
             ColumnReader col = reader.columnReader("station")) {

            assertThat(col.nextBatch()).isTrue();
            BinaryValues values = col.getBinaryValues();
            int size = values.size();
            assertThatThrownBy(() -> values.buffer(size))
                    .isInstanceOf(IndexOutOfBoundsException.class)
                    .hasMessage("Value " + size + " out of bounds for " + size + " values");
            assertThatThrownBy(() -> values.buffer(-1))
                    .isInstanceOf(IndexOutOfBoundsException.class)
                    .hasMessage("Value -1 out of bounds for " + size + " values");
            String arrayBound = "Index " + size + " out of bounds for length " + size;
            assertThatThrownBy(() -> values.start(size)).isInstanceOf(IndexOutOfBoundsException.class)
                    .hasMessage(arrayBound);
            assertThatThrownBy(() -> values.end(size)).isInstanceOf(IndexOutOfBoundsException.class)
                    .hasMessage(arrayBound);
            assertThatThrownBy(() -> values.length(size)).isInstanceOf(IndexOutOfBoundsException.class)
                    .hasMessage(arrayBound);
            assertThatThrownBy(() -> values.getBinary(size)).isInstanceOf(IndexOutOfBoundsException.class)
                    .hasMessage(arrayBound);
            assertThatThrownBy(() -> values.copyTo(size, new byte[64], 0)).isInstanceOf(IndexOutOfBoundsException.class)
                    .hasMessage(arrayBound);
            assertThatThrownBy(() -> values.contentEquals(size, new byte[64], 0, 1))
                    .isInstanceOf(IndexOutOfBoundsException.class)
                    .hasMessage(arrayBound);
        }
    }

    /// Every third row null, the others the row number as four big-endian bytes.
    private static byte[] code(int row) {
        return row % 3 == 2 ? null : ByteBuffer.allocate(4).putInt(row).array();
    }

    /// Cycles `Hamburg`, `Oslo`, `Abha`, null: rows 0, 4 and 8 hold `Hamburg`, so the second
    /// test can compare their views.
    private static String station(int row) {
        return row % 4 == 3 ? null : STATIONS[row % 4];
    }
}
