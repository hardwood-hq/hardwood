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
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/// Comparison tests that validate Hardwood's output against the reference
/// parquet-java implementation by comparing parsed results row-by-row, field-by-field.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ParquetComparisonTest {

    @BeforeAll
    void setUp() throws IOException {
        ParquetTestingRepoCloner.ensureCloned();
    }

    /// Directories containing test parquet files.
    private static final List<String> TEST_DIRECTORIES = List.of(
            "data",
            "bad_data",
            "shredded_variant");

    @ParameterizedTest(name = "{0}")
    @MethodSource("dev.hardwood.testing.Utils#parquetTestFiles")
    void compareWithReference(Path testFile) throws IOException {
        String fileName = testFile.getFileName().toString();

        // Skip individual files
        assumeFalse(Utils.SKIPPED_FILES.contains(fileName),
                "Skipping " + fileName + " (in skip list)");

        compareParquetFile(testFile);
    }

    @ParameterizedTest(name = "column: {0}")
    @MethodSource("dev.hardwood.testing.Utils#parquetTestFiles")
    void compareColumnsWithReference(Path testFile) throws IOException {
        String fileName = testFile.getFileName().toString();

        // Skip files that are in either skip list
        assumeFalse(Utils.SKIPPED_FILES.contains(fileName),
                "Skipping " + fileName + " (in skip list)");
        assumeFalse(Utils.COLUMN_SKIPPED_FILES.contains(fileName),
                "Skipping " + fileName + " (in column skip list)");

        compareColumnsParquetFile(testFile);
    }

    // ==================== Bad Data Tests ====================

    @Test
    void rejectParquet1481() throws IOException {
        // Corrupted schema Thrift value: physical type field is -7
        assertBadDataRejected("PARQUET-1481.parquet",
                "Invalid or corrupt physical type value: -7");
    }

    @Test
    void rejectDictheader() throws IOException {
        // Dictionary page header has negative numValues.
        // All 4 columns are corrupted differently; parallel column scanning
        // means any column's error may surface first.
        assertBadDataRejected("ARROW-RS-GH-6229-DICTHEADER.parquet");
    }

    @Test
    void rejectLevels() throws IOException {
        // Page has insufficient repetition levels: the page header declares
        // 21 values but column metadata expects only 1.
        assertBadDataRejected("ARROW-RS-GH-6229-LEVELS.parquet",
                "Value count mismatch for column 'c': metadata declares 1 values but pages contain 21");
    }

    @Test
    void rejectArrowGH41317() throws IOException {
        // Columns do not have the same size: timestamp_us_no_tz has no data
        // pages (0 values vs 3 declared in metadata).
        assertBadDataRejected("ARROW-GH-41317.parquet",
                "Value count mismatch for column 'timestamp_us_no_tz': metadata declares 3 values but pages contain 0");
    }

    @Test
    void rejectArrowGH41321() throws IOException {
        // Decoded rep/def levels less than num_values in page header.
        // Column 'value' also has negative dictionary numValues which is
        // caught first during page scanning.
        assertBadDataRejected("ARROW-GH-41321.parquet",
                "Invalid dictionary page for column 'value': negative numValues");
    }

    @Test
    void rejectArrowGH45185() throws IOException {
        // Repetition levels start with 1 instead of the required 0
        assertBadDataRejected("ARROW-GH-45185.parquet",
                "first repetition level must be 0 but was 1");
    }

    @Test
    void rejectCorruptChecksum() throws IOException {
        // Intentionally corrupted CRC checksums in data pages
        assertCorruptChecksumRejected("data/datapage_v1-corrupt-checksum.parquet",
                "CRC mismatch");
    }

    @Test
    void rejectCorruptDictionaryChecksum() throws IOException {
        // Intentionally corrupted CRC checksum in dictionary page
        assertCorruptChecksumRejected("data/rle-dict-uncompressed-corrupt-checksum.parquet",
                "CRC mismatch");
    }

    @Test
    void acceptArrowGH43605() throws IOException {
        // Dictionary index page uses RLE encoding with bit-width 0.
        // This is valid for a single-entry dictionary (ceil(log2(1)) = 0);
        // parquet-java also accepts this file.
        Path repoDir = ParquetTestingRepoCloner.ensureCloned();
        Path testFile = repoDir.resolve("bad_data/ARROW-GH-43605.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(testFile));
             RowReader rowReader = fileReader.createRowReader()) {
            int count = 0;
            while (rowReader.hasNext()) {
                rowReader.next();
                count++;
            }
            assertThat(count).isGreaterThan(0);
        }
    }

    private void assertCorruptChecksumRejected(String relativePath, String expectedMessage) throws IOException {
        Path repoDir = ParquetTestingRepoCloner.ensureCloned();
        Path testFile = repoDir.resolve(relativePath);

        assertThatThrownBy(() -> {
            try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(testFile));
                 RowReader rowReader = fileReader.createRowReader()) {
                while (rowReader.hasNext()) {
                    rowReader.next();
                }
            }
        }).as("Expected %s to be rejected due to corrupt checksum", relativePath)
          .hasStackTraceContaining(expectedMessage);
    }

    private ThrowableAssert.ThrowingCallable singleFileReadAction(String fileName) throws IOException {
        Path testFile = ParquetTestingRepoCloner.ensureCloned().resolve("bad_data/" + fileName);
        return () -> {
            try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(testFile));
                 RowReader rowReader = fileReader.createRowReader()) {
                while (rowReader.hasNext()) {
                    rowReader.next();
                }
            }
        };
    }

    private void assertBadDataRejected(String fileName) throws IOException {
        Utils.assertBadDataRejected(fileName, singleFileReadAction(fileName));
    }

    private void assertBadDataRejected(String fileName, String expectedMessage) throws IOException {
        Utils.assertBadDataRejected(fileName, expectedMessage, singleFileReadAction(fileName));
    }

    /// Compare a Parquet file column-by-column using the batch ColumnReader API
    /// against parquet-java reference data.
    private void compareColumnsParquetFile(Path testFile) throws IOException {
        System.out.println("Column comparing: " + testFile.getFileName());

        List<GenericRecord> referenceRows = Utils.readWithParquetJava(testFile);

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(testFile))) {
            Utils.compareColumns(fileReader.getFileSchema(), fileReader::createColumnReader, referenceRows);
        }

        System.out.println("  Column comparison passed!");
    }

    /// Compare a Parquet file using both implementations.
    private void compareParquetFile(Path testFile) throws IOException {
        System.out.println("Comparing: " + testFile.getFileName());

        // Read with parquet-java (reference)
        List<GenericRecord> referenceRows = Utils.readWithParquetJava(testFile);
        System.out.println("  parquet-java rows: " + referenceRows.size());

        // Compare with Hardwood row by row
        int hardwoodRowCount = compareWithHardwood(testFile, referenceRows);
        System.out.println("  Hardwood rows: " + hardwoodRowCount);

        // Verify row counts match
        assertThat(hardwoodRowCount)
                .as("Row count mismatch")
                .isEqualTo(referenceRows.size());

        System.out.println("  All " + referenceRows.size() + " rows match!");
    }

    /// Read with Hardwood and compare row by row against reference.
    /// Returns the number of rows read.
    private int compareWithHardwood(Path file, List<GenericRecord> referenceRows) throws IOException {
        int rowIndex = 0;

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file));
             RowReader rowReader = fileReader.createRowReader()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                assertThat(rowIndex)
                        .as("Hardwood has more rows than reference")
                        .isLessThan(referenceRows.size());
                Utils.compareRow(rowIndex, referenceRows.get(rowIndex), rowReader);
                rowIndex++;
            }
        }

        return rowIndex;
    }
}
