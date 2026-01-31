/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.morling.hardwood.reader.ColumnProjection;
import dev.morling.hardwood.reader.Hardwood;
import dev.morling.hardwood.reader.MultiFileRowReader;
import dev.morling.hardwood.reader.ParquetFileReader;
import dev.morling.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for MultiFileRowReader and cross-file prefetching.
 */
class MultiFileRowReaderTest {

    private static final Path TEST_FILE = Paths.get("src/test/resources/plain_uncompressed.parquet");
    private static final Path TEST_FILE_WITH_NULLS = Paths.get("src/test/resources/plain_uncompressed_with_nulls.parquet");

    @Test
    void testReadSingleFile() throws Exception {
        List<Path> files = List.of(TEST_FILE);

        try (Hardwood hardwood = Hardwood.create();
             MultiFileRowReader reader = hardwood.openAll(files)) {

            List<Long> ids = new ArrayList<>();
            List<Long> values = new ArrayList<>();

            while (reader.hasNext()) {
                reader.next();
                ids.add(reader.getLong("id"));
                values.add(reader.getLong("value"));
            }

            assertThat(ids).containsExactly(1L, 2L, 3L);
            assertThat(values).containsExactly(100L, 200L, 300L);
        }
    }

    @Test
    void testReadMultipleIdenticalFiles() throws Exception {
        // Read the same file multiple times to test cross-file prefetching
        List<Path> files = List.of(TEST_FILE, TEST_FILE, TEST_FILE);

        try (Hardwood hardwood = Hardwood.create();
             MultiFileRowReader reader = hardwood.openAll(files)) {

            List<Long> ids = new ArrayList<>();
            List<Long> values = new ArrayList<>();

            while (reader.hasNext()) {
                reader.next();
                ids.add(reader.getLong("id"));
                values.add(reader.getLong("value"));
            }

            // Should have 9 rows total (3 rows x 3 files)
            assertThat(ids).hasSize(9);
            assertThat(values).hasSize(9);

            // Verify values repeat as expected
            assertThat(ids).containsExactly(1L, 2L, 3L, 1L, 2L, 3L, 1L, 2L, 3L);
            assertThat(values).containsExactly(100L, 200L, 300L, 100L, 200L, 300L, 100L, 200L, 300L);
        }
    }

    @Test
    void testReadMultipleFilesWithProjection() throws Exception {
        List<Path> files = List.of(TEST_FILE, TEST_FILE);

        try (Hardwood hardwood = Hardwood.create();
             MultiFileRowReader reader = hardwood.openAll(files, ColumnProjection.columns("id"))) {

            List<Long> ids = new ArrayList<>();

            while (reader.hasNext()) {
                reader.next();
                ids.add(reader.getLong("id"));
            }

            // Should have 6 rows total (3 rows x 2 files)
            assertThat(ids).hasSize(6);
            assertThat(ids).containsExactly(1L, 2L, 3L, 1L, 2L, 3L);
        }
    }

    @Test
    void testFieldCount() throws Exception {
        List<Path> files = List.of(TEST_FILE);

        try (Hardwood hardwood = Hardwood.create();
             MultiFileRowReader reader = hardwood.openAll(files)) {

            assertThat(reader.getFieldCount()).isEqualTo(2);
            assertThat(reader.getFieldName(0)).isEqualTo("id");
            assertThat(reader.getFieldName(1)).isEqualTo("value");
        }
    }

    @Test
    void testFieldCountWithProjection() throws Exception {
        List<Path> files = List.of(TEST_FILE);

        try (Hardwood hardwood = Hardwood.create();
             MultiFileRowReader reader = hardwood.openAll(files, ColumnProjection.columns("value"))) {

            assertThat(reader.getFieldCount()).isEqualTo(1);
            assertThat(reader.getFieldName(0)).isEqualTo("value");
        }
    }

    @Test
    void testAccessByIndex() throws Exception {
        List<Path> files = List.of(TEST_FILE);

        try (Hardwood hardwood = Hardwood.create();
             MultiFileRowReader reader = hardwood.openAll(files)) {

            reader.hasNext();
            reader.next();

            // Access by projected index
            assertThat(reader.getLong(0)).isEqualTo(1L); // id
            assertThat(reader.getLong(1)).isEqualTo(100L); // value
        }
    }

    @Test
    void testEmptyFileListThrows() {
        try (Hardwood hardwood = Hardwood.create()) {
            assertThatThrownBy(() -> hardwood.openAll(List.of()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("At least one file");
        }
    }

    @Test
    void testRowCountMatchesSingleFileReading() throws Exception {
        List<Path> files = List.of(TEST_FILE, TEST_FILE);

        // Count rows using MultiFileRowReader
        long multiFileCount = 0;
        try (Hardwood hardwood = Hardwood.create();
             MultiFileRowReader reader = hardwood.openAll(files)) {
            while (reader.hasNext()) {
                reader.next();
                multiFileCount++;
            }
        }

        // Count rows using individual readers
        long singleFileCount = 0;
        try (Hardwood hardwood = Hardwood.create()) {
            for (Path file : files) {
                try (ParquetFileReader fileReader = hardwood.open(file);
                     RowReader rowReader = fileReader.createRowReader()) {
                    while (rowReader.hasNext()) {
                        rowReader.next();
                        singleFileCount++;
                    }
                }
            }
        }

        assertThat(multiFileCount).isEqualTo(singleFileCount);
    }

    @Test
    void testReadFileWithNulls() throws Exception {
        List<Path> files = List.of(TEST_FILE_WITH_NULLS);

        try (Hardwood hardwood = Hardwood.create();
             MultiFileRowReader reader = hardwood.openAll(files)) {

            int rowCount = 0;
            int nullCount = 0;

            while (reader.hasNext()) {
                reader.next();
                rowCount++;
                // The file has "id" and "name" columns, with "name" having nulls
                if (reader.isNull("name")) {
                    nullCount++;
                }
            }

            assertThat(rowCount).isEqualTo(3);
            // Second row has null name
            assertThat(nullCount).isEqualTo(1);
        }
    }

    @Test
    void testReadMultipleFilesPreservesDataIntegrity() throws Exception {
        // Test that data is not corrupted across file boundaries
        List<Path> files = List.of(TEST_FILE, TEST_FILE);

        try (Hardwood hardwood = Hardwood.create();
             MultiFileRowReader reader = hardwood.openAll(files)) {

            long runningSum = 0;
            int rowCount = 0;

            while (reader.hasNext()) {
                reader.next();
                long id = reader.getLong("id");
                long value = reader.getLong("value");

                // Verify the relationship holds (value = id * 100 in this test file)
                assertThat(value).isEqualTo(id * 100);

                runningSum += value;
                rowCount++;
            }

            assertThat(rowCount).isEqualTo(6);
            // Sum should be (100 + 200 + 300) * 2 = 1200
            assertThat(runningSum).isEqualTo(1200L);
        }
    }
}
