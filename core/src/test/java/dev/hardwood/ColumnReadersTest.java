/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.Validity;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Tests for [ColumnReaders] and cross-file column-oriented prefetching.
class ColumnReadersTest {

    @Test
    void testReadSingleFile() throws Exception {
        Path filePath = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(List.of(filePath)));
             ColumnReaders columns = parquet.columnReaders(ColumnProjection.columns("id", "value"))) {

            ColumnReader idReader = columns.getColumnReader("id");
            ColumnReader valueReader = columns.getColumnReader("value");

            assertThat(idReader.nextBatch() & valueReader.nextBatch()).isTrue();
            assertThat(idReader.getRecordCount()).isEqualTo(3);
            assertThat(valueReader.getRecordCount()).isEqualTo(3);

            long[] ids = idReader.getLongs();
            assertThat(ids[0]).isEqualTo(1L);
            assertThat(ids[1]).isEqualTo(2L);
            assertThat(ids[2]).isEqualTo(3L);

            long[] values = valueReader.getLongs();
            assertThat(values[0]).isEqualTo(100L);
            assertThat(values[1]).isEqualTo(200L);
            assertThat(values[2]).isEqualTo(300L);

            assertThat(idReader.nextBatch() & valueReader.nextBatch()).isFalse();
        }
    }

    @Test
    void testReadMultipleIdenticalFiles() throws Exception {
        Path filePath = Paths.get("src/test/resources/plain_uncompressed.parquet");
        List<Path> files = List.of(filePath, filePath, filePath);

        long idSum = 0;
        long valueSum = 0;
        long rowCount = 0;

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(files));
             ColumnReaders columns = parquet.columnReaders(ColumnProjection.columns("id", "value"))) {

            ColumnReader idReader = columns.getColumnReader("id");
            ColumnReader valueReader = columns.getColumnReader("value");

            while (idReader.nextBatch() & valueReader.nextBatch()) {
                int count = idReader.getRecordCount();
                long[] ids = idReader.getLongs();
                long[] values = valueReader.getLongs();

                for (int i = 0; i < count; i++) {
                    idSum += ids[i];
                    valueSum += values[i];
                }
                rowCount += count;
            }
        }

        // 3 rows x 3 files = 9 rows
        assertThat(rowCount).isEqualTo(9);
        // id sum = (1+2+3) * 3 = 18
        assertThat(idSum).isEqualTo(18L);
        // value sum = (100+200+300) * 3 = 1800
        assertThat(valueSum).isEqualTo(1800L);
    }

    @Test
    void testGetColumnReaderByIndex() throws Exception {
        Path filePath = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(List.of(filePath)));
             ColumnReaders columns = parquet.columnReaders(ColumnProjection.columns("id", "value"))) {

            assertThat(columns.getColumnReader(0).getColumnSchema().name()).isEqualTo("id");
            assertThat(columns.getColumnReader(1).getColumnSchema().name()).isEqualTo("value");
        }
    }

    @Test
    void testUnknownColumnNameThrows() throws Exception {
        Path filePath = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(List.of(filePath)));
             ColumnReaders columns = parquet.columnReaders(ColumnProjection.columns("id"))) {

            assertThatThrownBy(() -> columns.getColumnReader("nonexistent"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("nonexistent");
        }
    }

    @Test
    void testReadFileWithNulls() throws Exception {
        Path filePath = Paths.get("src/test/resources/plain_uncompressed_with_nulls.parquet");
        List<Path> files = List.of(filePath);

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(files));
             ColumnReaders columns = parquet.columnReaders(ColumnProjection.columns("id", "name"))) {

            ColumnReader idReader = columns.getColumnReader("id");
            ColumnReader nameReader = columns.getColumnReader("name");

            assertThat(idReader.nextBatch() & nameReader.nextBatch()).isTrue();

            // id column — required, no nulls
            assertThat(idReader.getRecordCount()).isEqualTo(3);
            assertThat(idReader.getLeafValidity().hasNulls()).isFalse();

            // name column — optional, row 1 is null
            assertThat(nameReader.getRecordCount()).isEqualTo(3);
            Validity validity = nameReader.getLeafValidity();
            assertThat(validity.hasNulls()).isTrue();
            assertThat(validity.isNotNull(0)).isTrue();
            assertThat(validity.isNull(1)).isTrue();
            assertThat(validity.isNotNull(2)).isTrue();

            String[] names = nameReader.getStrings();
            assertThat(names[0]).isEqualTo("alice");
            assertThat(names[1]).isNull();
            assertThat(names[2]).isEqualTo("charlie");

            assertThat(idReader.nextBatch() & nameReader.nextBatch()).isFalse();
        }
    }

    @Test
    void testMultipleFilesPreservesDataIntegrity() throws Exception {
        Path filePath = Paths.get("src/test/resources/plain_uncompressed.parquet");
        List<Path> files = List.of(filePath, filePath);

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(files));
             ColumnReaders columns = parquet.columnReaders(ColumnProjection.columns("id", "value"))) {

            ColumnReader idReader = columns.getColumnReader("id");
            ColumnReader valueReader = columns.getColumnReader("value");

            long runningSum = 0;
            int rowCount = 0;

            while (idReader.nextBatch() & valueReader.nextBatch()) {
                int count = idReader.getRecordCount();
                long[] ids = idReader.getLongs();
                long[] values = valueReader.getLongs();

                for (int i = 0; i < count; i++) {
                    // Verify the relationship holds (value = id * 100 in this test file)
                    assertThat(values[i]).isEqualTo(ids[i] * 100);
                    runningSum += values[i];
                }
                rowCount += count;
            }

            assertThat(rowCount).isEqualTo(6);
            // Sum should be (100 + 200 + 300) * 2 = 1200
            assertThat(runningSum).isEqualTo(1200L);
        }
    }

    @Test
    void testRowCountMatchesSingleFileReading() throws Exception {
        Path filePath = Paths.get("src/test/resources/plain_uncompressed.parquet");
        List<Path> files = List.of(filePath, filePath);

        // Count rows using ColumnReaders
        long multiFileCount = 0;
        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(files));
             ColumnReaders columns = parquet.columnReaders(ColumnProjection.columns("id"))) {
            ColumnReader reader = columns.getColumnReader("id");
            while (reader.nextBatch()) {
                multiFileCount += reader.getRecordCount();
            }
        }

        // Count rows using individual single-file ColumnReaders
        long singleFileCount = 0;
        try (Hardwood hardwood = Hardwood.create()) {
            for (Path file : files) {
                try (ParquetFileReader fileReader = hardwood.open(InputFile.of(file));
                     ColumnReader reader = fileReader.columnReader("id")) {
                    while (reader.nextBatch()) {
                        singleFileCount += reader.getRecordCount();
                    }
                }
            }
        }

        assertThat(multiFileCount).isEqualTo(singleFileCount);
    }

    @Test
    void testMultipleTypedColumns() throws Exception {
        Path filePath = Paths.get("src/test/resources/primitive_types_test.parquet");
        List<Path> files = List.of(filePath, filePath);

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(files));
             ColumnReaders columns = parquet.columnReaders(ColumnProjection.columns("int_col", "long_col", "float_col", "double_col", "bool_col", "string_col"))) {

            ColumnReader intReader = columns.getColumnReader("int_col");
            ColumnReader longReader = columns.getColumnReader("long_col");
            ColumnReader floatReader = columns.getColumnReader("float_col");
            ColumnReader doubleReader = columns.getColumnReader("double_col");
            ColumnReader boolReader = columns.getColumnReader("bool_col");
            ColumnReader stringReader = columns.getColumnReader("string_col");

            long intSum = 0;
            long longSum = 0;
            double floatSum = 0;
            double doubleSum = 0;
            int trueCount = 0;
            int rowCount = 0;

            while (intReader.nextBatch() & longReader.nextBatch()
                    & floatReader.nextBatch() & doubleReader.nextBatch()
                    & boolReader.nextBatch() & stringReader.nextBatch()) {

                int count = intReader.getRecordCount();
                int[] ints = intReader.getInts();
                long[] longs = longReader.getLongs();
                float[] floats = floatReader.getFloats();
                double[] doubles = doubleReader.getDoubles();
                boolean[] bools = boolReader.getBooleans();

                for (int i = 0; i < count; i++) {
                    intSum += ints[i];
                    longSum += longs[i];
                    floatSum += floats[i];
                    doubleSum += doubles[i];
                    if (bools[i]) trueCount++;
                }
                rowCount += count;
            }

            // 3 rows x 2 files = 6 rows
            assertThat(rowCount).isEqualTo(6);
            // int_col: (1+2+3) * 2 = 12
            assertThat(intSum).isEqualTo(12);
            // long_col: (100+200+300) * 2 = 1200
            assertThat(longSum).isEqualTo(1200L);
            // float_col: (1.5+2.5+3.5) * 2 = 15.0
            assertThat(floatSum).isCloseTo(15.0, org.assertj.core.data.Offset.offset(0.01));
            // double_col: (10.5+20.5+30.5) * 2 = 123.0
            assertThat(doubleSum).isCloseTo(123.0, org.assertj.core.data.Offset.offset(0.001));
            // bool_col: (true, false, true) * 2 = 4 trues
            assertThat(trueCount).isEqualTo(4);
        }
    }

    @Test
    void testNullsAcrossFileBoundary() throws Exception {
        Path filePath = Paths.get("src/test/resources/plain_uncompressed_with_nulls.parquet");
        List<Path> files = List.of(filePath, filePath);

        int nullCount = 0;
        int rowCount = 0;

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(files));
             ColumnReaders columns = parquet.columnReaders(ColumnProjection.columns("name"))) {

            ColumnReader nameReader = columns.getColumnReader("name");

            while (nameReader.nextBatch()) {
                int count = nameReader.getRecordCount();
                Validity validity = nameReader.getLeafValidity();

                for (int i = 0; i < count; i++) {
                    if (validity.isNull(i)) {
                        nullCount++;
                    }
                }
                rowCount += count;
            }
        }

        // 3 rows x 2 files = 6 rows, 1 null per file = 2 nulls
        assertThat(rowCount).isEqualTo(6);
        assertThat(nullCount).isEqualTo(2);
    }

    @Test
    void testSingleColumnSubset() throws Exception {
        Path filePath = Paths.get("src/test/resources/plain_uncompressed.parquet");
        List<Path> files = List.of(filePath, filePath, filePath);

        long idSum = 0;
        long rowCount = 0;

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(files));
             ColumnReaders columns = parquet.columnReaders(ColumnProjection.columns("id"))) {

            ColumnReader idReader = columns.getColumnReader("id");

            while (idReader.nextBatch()) {
                int count = idReader.getRecordCount();
                long[] ids = idReader.getLongs();
                for (int i = 0; i < count; i++) {
                    idSum += ids[i];
                }
                rowCount += count;
            }
        }

        // 3 rows x 3 files = 9 rows
        assertThat(rowCount).isEqualTo(9);
        // (1+2+3) * 3 = 18
        assertThat(idSum).isEqualTo(18L);
    }

    @Test
    void testLayerCountIsZeroForFlat() throws Exception {
        Path filePath = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(List.of(filePath)));
             ColumnReaders columns = parquet.columnReaders(ColumnProjection.columns("id"))) {

            ColumnReader reader = columns.getColumnReader("id");
            assertThat(reader.getLayerCount()).isEqualTo(0);
        }
    }

    /// `primitive_lists_test.parquet` row 2 has `int_list = []` (empty) and
    /// row 3 has `int_list = null`. The layer model encodes "empty" as
    /// equal-offsets at the REPEATED layer (`offsets[r+1] - offsets[r] == 0`)
    /// and "null" as a cleared validity bit; this pins that both states are
    /// reachable independently through the public `ColumnReader` API.
    @Test
    void testEmptyAndNullListsAreDistinguishable() throws Exception {
        Path filePath = Paths.get("src/test/resources/primitive_lists_test.parquet");

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(List.of(filePath)));
             ColumnReaders columns = parquet.columnReaders(ColumnProjection.columns("int_list.list.element"))) {

            ColumnReader reader = columns.getColumnReader("int_list.list.element");
            assertThat(reader.nextBatch()).isTrue();
            assertThat(reader.getRecordCount()).isEqualTo(4);

            Validity listValidity = reader.getLayerValidity(0);
            int[] offsets = reader.getLayerOffsets(0);
            assertThat(offsets).hasSize(reader.getRecordCount() + 1);

            assertThat(listValidity.hasNulls()).isTrue();
            assertThat(listValidity.isNull(3)).as("row 3 is null list").isTrue();
            assertThat(listValidity.isNotNull(2)).as("row 2 is empty (present), not null").isTrue();

            assertThat(offsets[3] - offsets[2]).as("row 2 has zero items (empty)").isEqualTo(0);
            assertThat(offsets[4] - offsets[3]).as("row 3 has zero items (null)").isEqualTo(0);
            assertThat(offsets[1] - offsets[0]).as("row 0 has values").isGreaterThan(0);
            assertThat(offsets[2] - offsets[1]).as("row 1 has values").isGreaterThan(0);
        }
    }

    @Test
    void testGetLayerOffsetsThrowsBeforeNextBatch() throws Exception {
        Path filePath = Paths.get("src/test/resources/primitive_lists_test.parquet");

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(List.of(filePath)));
             ColumnReaders columns = parquet.columnReaders(ColumnProjection.columns("int_list.list.element"))) {

            ColumnReader reader = columns.getColumnReader("int_list.list.element");
            assertThatThrownBy(() -> reader.getLayerOffsets(0))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("No batch available");
        }
    }

    @Test
    void testGetLayerOffsetsThrowsForFlatColumn() throws Exception {
        Path filePath = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(List.of(filePath)));
             ColumnReaders columns = parquet.columnReaders(ColumnProjection.columns("id"))) {

            ColumnReader reader = columns.getColumnReader("id");
            assertThat(reader.nextBatch()).isTrue();
            assertThatThrownBy(() -> reader.getLayerOffsets(0))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("out of range");
        }
    }

    @Test
    void testGetLayerOffsetsThrowsForLayerOutOfRange() throws Exception {
        Path filePath = Paths.get("src/test/resources/primitive_lists_test.parquet");

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(List.of(filePath)));
             ColumnReaders columns = parquet.columnReaders(ColumnProjection.columns("int_list.list.element"))) {

            ColumnReader reader = columns.getColumnReader("int_list.list.element");
            assertThat(reader.nextBatch()).isTrue();
            assertThatThrownBy(() -> reader.getLayerOffsets(5))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("out of range");
        }
    }

    // ==================== Coordinated nextBatch() ====================

    @Test
    void testCoordinatedNextBatchAdvancesAllReadersInLockstep() throws Exception {
        Path filePath = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(List.of(filePath)));
             ColumnReaders columns = parquet.columnReaders(ColumnProjection.columns("id", "value"))) {

            assertThat(columns.nextBatch()).isTrue();
            assertThat(columns.getRecordCount()).isEqualTo(3);
            // Per-reader counts also reflect the same batch.
            assertThat(columns.getColumnReader("id").getRecordCount()).isEqualTo(3);
            assertThat(columns.getColumnReader("value").getRecordCount()).isEqualTo(3);
            assertThat(columns.getColumnReader("id").getLongs()[0]).isEqualTo(1L);
            assertThat(columns.getColumnReader("value").getLongs()[0]).isEqualTo(100L);

            assertThat(columns.nextBatch()).isFalse();
        }
    }

    @Test
    void testCoordinatedNextBatchAcrossMultipleFiles() throws Exception {
        Path filePath = Paths.get("src/test/resources/plain_uncompressed.parquet");
        List<Path> files = List.of(filePath, filePath, filePath);

        long idSum = 0;
        long valueSum = 0;
        long rowCount = 0;

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(files));
             ColumnReaders columns = parquet.columnReaders(ColumnProjection.columns("id", "value"))) {

            while (columns.nextBatch()) {
                int count = columns.getRecordCount();
                long[] ids = columns.getColumnReader("id").getLongs();
                long[] values = columns.getColumnReader("value").getLongs();
                for (int i = 0; i < count; i++) {
                    idSum += ids[i];
                    valueSum += values[i];
                }
                rowCount += count;
            }
        }

        assertThat(rowCount).isEqualTo(9);
        assertThat(idSum).isEqualTo(18L);
        assertThat(valueSum).isEqualTo(1800L);
    }

    @Test
    void testGetRecordCountBeforeNextBatchThrows() throws Exception {
        Path filePath = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(List.of(filePath)));
             ColumnReaders columns = parquet.columnReaders(ColumnProjection.columns("id"))) {

            assertThatThrownBy(columns::getRecordCount)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("nextBatch()");
        }
    }

    @Test
    void testGetRecordCountAfterExhaustionThrows() throws Exception {
        Path filePath = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(List.of(filePath)));
             ColumnReaders columns = parquet.columnReaders(ColumnProjection.columns("id"))) {

            assertThat(columns.nextBatch()).isTrue();
            assertThat(columns.nextBatch()).isFalse();

            // After exhaustion, getRecordCount() should fail rather than return a stale value.
            assertThatThrownBy(columns::getRecordCount)
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void testCoordinatedNextBatchOnMultiRowGroupFile() throws Exception {
        // filter_pushdown_int.parquet has 3 row groups (id 1-100, 101-200, 201-300).
        Path filePath = Paths.get("src/test/resources/filter_pushdown_int.parquet");

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(List.of(filePath)));
             ColumnReaders columns = parquet.columnReaders(ColumnProjection.columns("id"))) {

            int batches = 0;
            long totalRows = 0;
            while (columns.nextBatch()) {
                batches++;
                totalRows += columns.getRecordCount();
            }
            assertThat(totalRows).isEqualTo(300);
            assertThat(batches).isGreaterThanOrEqualTo(1);
        }
    }
}
