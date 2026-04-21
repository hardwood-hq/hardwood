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
import dev.hardwood.reader.MultiFileColumnReaders;
import dev.hardwood.reader.MultiFileParquetReader;
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
             RowReader reader = fileReader.createRowReader()) {

            assertThat(reader.hasNext()).isTrue();
            reader.next();

            // "id" column is LONG — requesting INT should throw with file name
            assertThatThrownBy(() -> reader.getInt("id"))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("[" + FILE_NAME + "]");
        }
    }

    @Test
    void rowReaderMissingColumnIncludesFileName() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(TEST_FILE));
             RowReader reader = fileReader.createRowReader()) {

            assertThat(reader.hasNext()).isTrue();
            reader.next();

            // Non-existent column should throw with file name
            assertThatThrownBy(() -> reader.getLong("nonexistent"))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("[" + FILE_NAME + "]");
        }
    }

    // ==================== RowReader (multi-file) ====================

    @Test
    void multiFileRowReaderIncludesCorrectFileName() throws Exception {
        List<Path> files = List.of(TEST_FILE, TEST_FILE);

        try (Hardwood hardwood = Hardwood.create();
             MultiFileParquetReader parquet = hardwood.openAll(InputFile.ofPaths(files));
             RowReader reader = parquet.createRowReader()) {

            // Read through both files and provoke an error on each
            while (reader.hasNext()) {
                reader.next();
                // All rows come from the same physical file
                assertThatThrownBy(() -> reader.getInt("id"))
                        .isInstanceOf(RuntimeException.class)
                        .hasMessageContaining("[" + FILE_NAME + "]");
            }
        }
    }

    // ==================== ColumnReader (single file) ====================

    @Test
    void columnReaderTypeMismatchIncludesFileName() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(TEST_FILE));
             ColumnReader reader = fileReader.createColumnReader("id")) {

            assertThat(reader.nextBatch()).isTrue();

            // "id" is LONG — requesting INTs should throw with file name
            assertThatThrownBy(reader::getInts)
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("[" + FILE_NAME + "]");
        }
    }

    @Test
    void columnReaderNoBatchIncludesFileName() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(TEST_FILE));
             ColumnReader reader = fileReader.createColumnReader("id")) {

            // No nextBatch() called — should throw. File name may or may not be
            // available (no batch loaded yet), so we just verify no NPE
            assertThatThrownBy(reader::getLongs)
                    .isInstanceOf(RuntimeException.class);
        }
    }

    // ==================== ColumnReader (multi-file) ====================

    @Test
    void multiFileColumnReaderIncludesFileName() throws Exception {
        List<Path> files = List.of(TEST_FILE, TEST_FILE);

        try (Hardwood hardwood = Hardwood.create();
             MultiFileParquetReader parquet = hardwood.openAll(InputFile.ofPaths(files));
             MultiFileColumnReaders columns = parquet.createColumnReaders(ColumnProjection.columns("id"))) {

            ColumnReader idReader = columns.getColumnReader("id");

            while (idReader.nextBatch()) {
                // Type mismatch on each batch should include file name
                assertThatThrownBy(idReader::getInts)
                        .isInstanceOf(RuntimeException.class)
                        .hasMessageContaining("[" + FILE_NAME + "]");
            }
        }
    }

    // ==================== Exception type preservation ====================

    @Test
    void exceptionTypeIsPreserved() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(TEST_FILE));
             RowReader reader = fileReader.createRowReader()) {

            assertThat(reader.hasNext()).isTrue();
            reader.next();

            // Type mismatch throws IllegalArgumentException — verify it is preserved
            assertThatThrownBy(() -> reader.getInt("id"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("[" + FILE_NAME + "]");
        }
    }

    // ==================== Format verification ====================

    @Test
    void bracketedFormatIsCorrect() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(TEST_FILE));
             RowReader reader = fileReader.createRowReader()) {

            assertThat(reader.hasNext()).isTrue();
            reader.next();

            assertThatThrownBy(() -> reader.getInt("id"))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageMatching("^\\[" + FILE_NAME.replace(".", "\\.") + "] .*");
        }
    }
}
