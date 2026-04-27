/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Tests that every exception escaping a reader includes the originating file name.
/// See <a href="https://github.com/hardwood-hq/hardwood/issues/90">issue #90</a>.
class FileNameInExceptionTest {

    private static final Path TEST_FILE = Paths.get("src/test/resources/plain_uncompressed.parquet");
    private static final String FILE_NAME = "plain_uncompressed.parquet";

    // ==================== RowReader (single file) ====================

    @Test
    void rowReaderTypeMismatchIncludesFileName() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(TEST_FILE));
             RowReader reader = fileReader.rowReader()) {

            assertThat(reader.hasNext()).isTrue();
            reader.next();

            // "id" column is LONG — requesting INT throws via resolveAndValidate
            assertThatThrownBy(() -> reader.getInt("id"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[" + FILE_NAME + "] Field 'id' has physical type INT64, expected INT32");
        }
    }

    @Test
    void rowReaderMissingColumnIncludesFileName() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(TEST_FILE));
             RowReader reader = fileReader.rowReader()) {

            assertThat(reader.hasNext()).isTrue();
            reader.next();

            // Non-existent column throws via resolveIndex
            assertThatThrownBy(() -> reader.getLong("nonexistent"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[" + FILE_NAME + "] Column not in projection: nonexistent");
        }
    }

    // ==================== RowReader (multi-file) ====================

    @Test
    void multiFileRowReaderAttributesEachFileToItsOwnName(@TempDir Path tempDir) throws Exception {
        // Copy the test file under a second, distinct name so the file-boundary
        // detection in ColumnWorker is actually exercised — not just the same name twice.
        // plain_uncompressed.parquet has 3 rows; the multi-file reader should emit
        // rows from TEST_FILE first (rows 0–2), then from second_file (rows 3–5).
        Path secondFile = tempDir.resolve("second_file.parquet");
        Files.copy(TEST_FILE, secondFile);

        String firstFileError = "[" + FILE_NAME + "] Field 'id' has physical type INT64, expected INT32";
        String secondFileError = "[second_file.parquet] Field 'id' has physical type INT64, expected INT32";

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(
                     InputFile.ofPaths(List.of(TEST_FILE, secondFile)));
             RowReader reader = parquet.rowReader()) {

            // Rows 0–2 come from TEST_FILE
            for (int i = 0; i < 3; i++) {
                assertThat(reader.hasNext()).isTrue();
                reader.next();
                assertThatThrownBy(() -> reader.getInt("id"))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage(firstFileError);
            }

            // Rows 3–5 come from secondFile
            for (int i = 0; i < 3; i++) {
                assertThat(reader.hasNext()).isTrue();
                reader.next();
                assertThatThrownBy(() -> reader.getInt("id"))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage(secondFileError);
            }

            assertThat(reader.hasNext()).isFalse();
        }
    }

    // ==================== ColumnReader (single file) ====================

    @Test
    void columnReaderTypeMismatchIncludesFileName() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(TEST_FILE));
             ColumnReader reader = fileReader.columnReader("id")) {

            assertThat(reader.nextBatch()).isTrue();

            // "id" is LONG — requesting INTs throws via typeMismatch
            assertThatThrownBy(reader::getInts)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("[" + FILE_NAME + "] Column 'id' is INT64, not int");
        }
    }

    @Test
    void columnReaderNoBatchHasNoFileName() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(TEST_FILE));
             ColumnReader reader = fileReader.columnReader("id")) {

            // No nextBatch() called — no batch loaded, so no file name is available.
            // The message has no `[fileName]` prefix.
            assertThatThrownBy(reader::getLongs)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("No batch available. Call nextBatch() first.");
        }
    }

    // ==================== ColumnReader (multi-file) ====================

    @Test
    void multiFileColumnReaderAttributesEachBatchToItsFileName(@TempDir Path tempDir) throws Exception {
        Path secondFile = tempDir.resolve("second_file.parquet");
        Files.copy(TEST_FILE, secondFile);

        String firstFileError = "[" + FILE_NAME + "] Column 'id' is INT64, not int";
        String secondFileError = "[second_file.parquet] Column 'id' is INT64, not int";

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(
                     InputFile.ofPaths(List.of(TEST_FILE, secondFile)));
             ColumnReaders columns = parquet.columnReaders(ColumnProjection.columns("id"))) {

            ColumnReader idReader = columns.getColumnReader("id");

            // plain_uncompressed.parquet has 3 rows — well below batch capacity — so
            // each file yields exactly one batch. File-boundary detection in
            // ColumnWorker ensures the first batch is from TEST_FILE and the second
            // from secondFile, each carrying its own file name.
            assertThat(idReader.nextBatch()).isTrue();
            assertThatThrownBy(idReader::getInts)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(firstFileError);

            assertThat(idReader.nextBatch()).isTrue();
            assertThatThrownBy(idReader::getInts)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(secondFileError);

            assertThat(idReader.nextBatch()).isFalse();
        }
    }
}
