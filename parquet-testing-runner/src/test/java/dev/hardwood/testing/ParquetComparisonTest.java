/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Comparison tests that validate Hardwood's output against the reference
 * parquet-java implementation by comparing parsed results row-by-row, field-by-field.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ParquetComparisonTest {

    @BeforeAll
    void setUp() throws IOException {
        ParquetTestingRepoCloner.ensureCloned();
    }

    /**
     * Directories containing test parquet files.
     */
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

    /**
     * Additional files to skip in column-level comparison tests.
     */
    private static final Set<String> COLUMN_SKIPPED_FILES = Set.of(
            // Files with nested/repeated columns where column-level comparison
            // requires list reconstruction from offsets (deferred)
            "list_columns.parquet",
            "nested_lists.snappy.parquet",
            "nested_maps.snappy.parquet",
            "nested_structs.rust.parquet",
            "nonnullable.impala.parquet",
            "nullable.impala.parquet",
            "null_list.parquet",
            "old_list_structure.parquet",
            "repeated_no_annotation.parquet",
            "repeated_primitive_no_list.parquet",
            "incorrect_map_schema.parquet",
            "map_no_value.parquet"
    );

    @ParameterizedTest(name = "column: {0}")
    @MethodSource("dev.hardwood.testing.Utils#parquetTestFiles")
    void compareColumnsWithReference(Path testFile) throws IOException {
        String fileName = testFile.getFileName().toString();

        // Skip files that are in either skip list
        assumeFalse(Utils.SKIPPED_FILES.contains(fileName),
                "Skipping " + fileName + " (in skip list)");
        assumeFalse(COLUMN_SKIPPED_FILES.contains(fileName),
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

    /**
     * Compare a Parquet file column-by-column using the batch ColumnReader API
     * against parquet-java reference data.
     */
    private void compareColumnsParquetFile(Path testFile) throws IOException {
        System.out.println("Column comparing: " + testFile.getFileName());

        // Read reference data with parquet-java
        List<GenericRecord> referenceRows = Utils.readWithParquetJava(testFile);

        // Read with Hardwood column-by-column and compare
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(testFile))) {
            FileSchema schema = fileReader.getFileSchema();

            for (int colIdx = 0; colIdx < schema.getColumnCount(); colIdx++) {
                ColumnSchema colSchema = schema.getColumn(colIdx);

                // Skip nested/repeated columns for now
                if (colSchema.maxRepetitionLevel() > 0) {
                    continue;
                }

                String colName = colSchema.name();
                try (ColumnReader columnReader = fileReader.createColumnReader(colIdx)) {
                    int rowIdx = 0;

                    while (columnReader.nextBatch()) {
                        int count = columnReader.getRecordCount();
                        BitSet nulls = columnReader.getElementNulls();

                        for (int i = 0; i < count; i++) {
                            if (rowIdx >= referenceRows.size()) {
                                break;
                            }
                            GenericRecord refRow = referenceRows.get(rowIdx);
                            Object refValue = getRefColumnValue(refRow, colName);

                            if (refValue == null || refValue == Utils.SkipMarker.INSTANCE) {
                                // null or skipped
                                if (refValue == null) {
                                    assertThat(nulls != null && nulls.get(i))
                                            .as("Row %d, column '%s' should be null", rowIdx, colName)
                                            .isTrue();
                                }
                            } else if (nulls != null && nulls.get(i)) {
                                assertThat(refValue)
                                        .as("Row %d, column '%s' should not be null", rowIdx, colName)
                                        .isNull();
                            } else {
                                compareColumnValue(rowIdx, colName, refValue, columnReader, i);
                            }
                            rowIdx++;
                        }
                    }

                    assertThat(rowIdx)
                            .as("Column '%s' row count", colName)
                            .isEqualTo(referenceRows.size());
                }
            }
        }

        System.out.println("  Column comparison passed!");
    }

    /**
     * Get reference value for a flat column from a GenericRecord.
     */
    private Object getRefColumnValue(GenericRecord record, String fieldName) {
        var field = record.getSchema().getField(fieldName);
        if (field == null) {
            return Utils.SkipMarker.INSTANCE;
        }
        Object value = record.get(fieldName);
        if (value == null) {
            return null;
        }
        // Handle union types (nullable fields)
        var fieldSchema = field.schema();
        if (fieldSchema.getType() == org.apache.avro.Schema.Type.UNION) {
            for (var subSchema : fieldSchema.getTypes()) {
                if (subSchema.getType() != org.apache.avro.Schema.Type.NULL) {
                    fieldSchema = subSchema;
                    break;
                }
            }
        }
        // Skip complex types
        return switch (fieldSchema.getType()) {
            case RECORD, ARRAY, MAP -> Utils.SkipMarker.INSTANCE;
            case FIXED -> Utils.SkipMarker.INSTANCE; // INT96
            default -> Utils.convertToComparable(value);
        };
    }

    /**
     * Compare a single column value from the ColumnReader batch against the reference.
     */
    private void compareColumnValue(int rowIdx, String colName, Object refValue,
                                    ColumnReader reader, int batchIdx) {
        String context = String.format("Row %d, column '%s'", rowIdx, colName);
        Object actual = getColumnReaderValue(reader, batchIdx);
        Object comparableActual = Utils.convertToComparable(actual);

        if (refValue instanceof String refStr && comparableActual instanceof byte[] actualBytes) {
            // FIXED_LEN_BYTE_ARRAY without STRING logical type — Avro reader may produce String
            assertThat(new String(actualBytes, java.nio.charset.StandardCharsets.UTF_8))
                    .as(context).isEqualTo(refStr);
        } else if (refValue instanceof Float f) {
            assertThat((Float) comparableActual).as(context).isCloseTo(f, within(0.0001f));
        } else if (refValue instanceof Double d) {
            assertThat((Double) comparableActual).as(context).isCloseTo(d, within(0.0000001d));
        } else if (refValue instanceof byte[] refBytes) {
            assertThat((byte[]) comparableActual).as(context).isEqualTo(refBytes);
        } else {
            assertThat(comparableActual).as(context).isEqualTo(refValue);
        }
    }

    /**
     * Get a value from the ColumnReader at a batch index, using the appropriate typed array.
     * Applies logical type conversion (e.g. BYTE_ARRAY + STRING → String).
     */
    private Object getColumnReaderValue(ColumnReader reader, int index) {
        var colSchema = reader.getColumnSchema();
        return switch (colSchema.type()) {
            case INT32 -> reader.getInts()[index];
            case INT64 -> reader.getLongs()[index];
            case FLOAT -> reader.getFloats()[index];
            case DOUBLE -> reader.getDoubles()[index];
            case BOOLEAN -> reader.getBooleans()[index];
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> {
                // Use logical type conversion for STRING/JSON/BSON columns
                if (colSchema.logicalType() instanceof LogicalType.StringType
                        || colSchema.logicalType() instanceof LogicalType.JsonType
                        || colSchema.logicalType() instanceof LogicalType.BsonType) {
                    yield reader.getStrings()[index];
                }
                yield reader.getBinaries()[index];
            }
        };
    }

    /**
     * Compare a Parquet file using both implementations.
     */
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

    /**
     * Read with Hardwood and compare row by row against reference.
     * Returns the number of rows read.
     */
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
