/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.MultiFileColumnReaders;
import dev.hardwood.reader.MultiFileParquetReader;
import dev.hardwood.reader.MultiFileRowReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Verifies that exceptions from readers include the originating file name (issue #90).
public class FileNameInExceptionTest {

    private static final Path TEST_FILE = Paths.get("src/test/resources/plain_uncompressed.parquet");
    private static final Path NULLABLE_FILE = Paths.get("src/test/resources/nullable_primitives_test.parquet");

    // ==================== Single-file RowReader ====================

    @Test
    void rowReaderNullColumnExceptionIncludesFileName() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(TEST_FILE));
             RowReader rowReader = fileReader.createRowReader()) {

            assertThat(rowReader.hasNext()).isTrue();
            rowReader.next();

            // The file has non-nullable columns; try accessing a non-existent column by name
            // to trigger an exception from the data view
            assertThatThrownBy(() -> rowReader.getString("nonexistent_column"))
                    .hasMessageContaining(TEST_FILE.getFileName().toString());
        }
    }

    @Test
    void rowReaderAccessorExceptionIncludesFileName() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(TEST_FILE));
             RowReader rowReader = fileReader.createRowReader()) {

            assertThat(rowReader.hasNext()).isTrue();
            rowReader.next();

            // Try reading a non-existent column name
            assertThatThrownBy(() -> rowReader.getLong("does_not_exist"))
                    .hasMessageContaining(TEST_FILE.getFileName().toString());
        }
    }

    // ==================== Single-file ColumnReader ====================

    @Test
    void columnReaderTypeMismatchExceptionIncludesFileName() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(TEST_FILE));
             ColumnReader columnReader = fileReader.createColumnReader("id")) {

            assertThat(columnReader.nextBatch()).isTrue();

            // id is a LONG column, but we request ints — should throw with file name
            assertThatThrownBy(columnReader::getInts)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(TEST_FILE.getFileName().toString());
        }
    }

    @Test
    void columnReaderNoBatchExceptionIncludesFileName() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(TEST_FILE));
             ColumnReader columnReader = fileReader.createColumnReader("id")) {

            // Don't call nextBatch() — accessor should throw with file name
            assertThatThrownBy(columnReader::getLongs)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(TEST_FILE.getFileName().toString());
        }
    }

    @Test
    void exceptionPreservesOriginalType() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(TEST_FILE));
             ColumnReader columnReader = fileReader.createColumnReader("id")) {

            assertThat(columnReader.nextBatch()).isTrue();

            // Type mismatch should remain IllegalStateException
            assertThatThrownBy(columnReader::getInts)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("not int")
                    .hasMessageContaining(TEST_FILE.getFileName().toString());
        }
    }

    @Test
    void exceptionMessageFormatIncludesBracketedFileName() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(TEST_FILE));
             ColumnReader columnReader = fileReader.createColumnReader("id")) {

            assertThat(columnReader.nextBatch()).isTrue();

            assertThatThrownBy(columnReader::getInts)
                    .hasMessageStartingWith("[" + TEST_FILE.getFileName().toString() + "] ");
        }
    }

    // ==================== Multi-file RowReader ====================

    /// Second file with the same schema but a different name, used to verify
    /// that the correct file name is reported in multi-file exceptions.
    private static final Path TEST_FILE_2 = Paths.get("src/test/resources/plain_snappy.parquet");
    private static final Path MULTI_ROW_GROUP_FILE = Paths.get("src/test/resources/filter_pushdown_int.parquet");

    @Test
    void multiFileRowReaderExceptionIncludesFileName() throws IOException {
        try (Hardwood hardwood = Hardwood.create();
             MultiFileParquetReader parquet = hardwood.openAll(
                     InputFile.ofPaths(TEST_FILE, TEST_FILE_2));
             MultiFileRowReader reader = parquet.createRowReader()) {

            assertThat(reader.hasNext()).isTrue();
            reader.next();

            // Access a non-existent column — exception should include the first file name
            assertThatThrownBy(() -> reader.getString("nonexistent_column"))
                    .hasMessageContaining(TEST_FILE.getFileName().toString());
        }
    }

    @Test
    void multiFileRowReaderExceptionFormatIsBracketed() throws IOException {
        try (Hardwood hardwood = Hardwood.create();
             MultiFileParquetReader parquet = hardwood.openAll(
                     InputFile.ofPaths(TEST_FILE, TEST_FILE_2));
             MultiFileRowReader reader = parquet.createRowReader()) {

            assertThat(reader.hasNext()).isTrue();
            reader.next();

            assertThatThrownBy(() -> reader.getString("nonexistent_column"))
                    .hasMessageStartingWith("[" + TEST_FILE.getFileName().toString() + "] ");
        }
    }

    // ==================== Multi-file ColumnReader ====================

    @Test
    void multiFileColumnReaderExceptionIncludesFileName() throws IOException {
        try (Hardwood hardwood = Hardwood.create();
             MultiFileParquetReader parquet = hardwood.openAll(
                     InputFile.ofPaths(TEST_FILE, TEST_FILE_2));
             MultiFileColumnReaders columns = parquet.createColumnReaders(
                     ColumnProjection.columns("id"))) {

            ColumnReader idReader = columns.getColumnReader("id");
            assertThat(idReader.nextBatch()).isTrue();

            // id is LONG — requesting ints should throw with file name
            assertThatThrownBy(idReader::getInts)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(TEST_FILE.getFileName().toString());
        }
    }

    @Test
    void multiFileColumnReaderNoBatchExceptionIncludesFileName() throws IOException {
        try (Hardwood hardwood = Hardwood.create();
             MultiFileParquetReader parquet = hardwood.openAll(
                     InputFile.ofPaths(TEST_FILE, TEST_FILE_2));
             MultiFileColumnReaders columns = parquet.createColumnReaders(
                     ColumnProjection.columns("id"))) {

            ColumnReader idReader = columns.getColumnReader("id");

            // No nextBatch() called — should throw with file name
            assertThatThrownBy(idReader::getRecordCount)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(TEST_FILE.getFileName().toString());
        }
    }

    // ==================== Second-file attribution ====================

    @Test
    void multiFileRowReaderSecondFileExceptionIncludesSecondFileName() throws IOException {
        try (Hardwood hardwood = Hardwood.create();
             MultiFileParquetReader parquet = hardwood.openAll(
                     InputFile.ofPaths(TEST_FILE, TEST_FILE_2));
             MultiFileRowReader reader = parquet.createRowReader()) {

            // Consume all 3 rows from file 1
            for (int i = 0; i < 3; i++) {
                assertThat(reader.hasNext()).isTrue();
                reader.next();
            }

            // Now advance into file 2
            assertThat(reader.hasNext()).isTrue();
            reader.next();

            // Exception should report the second file
            assertThatThrownBy(() -> reader.getString("nonexistent_column"))
                    .hasMessageContaining(TEST_FILE_2.getFileName().toString());
        }
    }

    @Test
    void multiFileColumnReaderSecondFileExceptionIncludesSecondFileName() throws IOException {
        try (Hardwood hardwood = Hardwood.create();
             MultiFileParquetReader parquet = hardwood.openAll(
                     InputFile.ofPaths(TEST_FILE, TEST_FILE_2));
             MultiFileColumnReaders columns = parquet.createColumnReaders(
                     ColumnProjection.columns("id"))) {

            ColumnReader idReader = columns.getColumnReader("id");

            // First batch should be from file 1 (3 rows)
            assertThat(idReader.nextBatch()).isTrue();
            assertThat(idReader.getRecordCount()).isEqualTo(3);

            // Second batch should be from file 2 (3 rows)
            assertThat(idReader.nextBatch()).isTrue();
            assertThat(idReader.getRecordCount()).isEqualTo(3);

            // Exception should report the second file
            assertThatThrownBy(idReader::getInts)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(TEST_FILE_2.getFileName().toString());
        }
    }

    @Test
    void multiFileLaterRowGroupsInSecondFileKeepSecondFileName() throws IOException {
        Path copiedFile = Files.createTempFile("issue90-multi-row-groups-", ".parquet");
        Files.copy(MULTI_ROW_GROUP_FILE, copiedFile, StandardCopyOption.REPLACE_EXISTING);

        try (Hardwood hardwood = Hardwood.create();
             MultiFileParquetReader parquet = hardwood.openAll(
                     InputFile.ofPaths(MULTI_ROW_GROUP_FILE, copiedFile));
             MultiFileColumnReaders columns = parquet.createColumnReaders(
                     ColumnProjection.columns("id"))) {

            ColumnReader idReader = columns.getColumnReader("id");

            // `filter_pushdown_int.parquet` has 300 rows across 3 row groups, and
            // the batch size is much larger than that. We should therefore see one
            // 300-row batch per file, not spurious splits inside the second file.
            assertThat(idReader.nextBatch()).isTrue();
            assertThat(idReader.getRecordCount()).isEqualTo(300);

            assertThat(idReader.nextBatch()).isTrue();
            assertThat(idReader.getRecordCount()).isEqualTo(300);

            assertThatThrownBy(idReader::getInts)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(copiedFile.getFileName().toString());

            assertThat(idReader.nextBatch()).isFalse();
        }
        finally {
            Files.deleteIfExists(copiedFile);
        }
    }
}
