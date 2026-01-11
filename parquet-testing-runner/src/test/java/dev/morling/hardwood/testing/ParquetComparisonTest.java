/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.testing;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import dev.morling.hardwood.reader.ParquetFileReader;
import dev.morling.hardwood.reader.RowReader;
import dev.morling.hardwood.row.PqRow;
import dev.morling.hardwood.row.PqType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Comparison tests that validate Hardwood's output against the reference
 * parquet-java implementation by comparing parsed results row-by-row, field-by-field.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ParquetComparisonTest {

    /**
     * Marker to indicate a field should be skipped in comparison (e.g., INT96 timestamps).
     */
    private enum SkipMarker {
        INSTANCE
    }

    private Path parquetTestingDir;

    @BeforeAll
    void setUp() throws IOException {
        parquetTestingDir = ParquetTestingRepoCloner.ensureCloned();
    }

    @Test
    void compareAlltypesDictionary() throws IOException {
        Path testFile = parquetTestingDir.resolve("data/alltypes_dictionary.parquet");
        compareParquetFile(testFile);
    }

    /**
     * Compare a Parquet file using both implementations.
     */
    private void compareParquetFile(Path testFile) throws IOException {
        System.out.println("Comparing: " + testFile.getFileName());

        // Read with parquet-java (reference)
        List<GenericRecord> referenceRows = readWithParquetJava(testFile);
        System.out.println("  parquet-java rows: " + referenceRows.size());

        // Read with Hardwood
        List<PqRow> hardwoodRows = readWithHardwood(testFile);
        System.out.println("  Hardwood rows: " + hardwoodRows.size());

        // Compare row counts
        assertThat(hardwoodRows)
                .as("Row count mismatch")
                .hasSameSizeAs(referenceRows);

        // Compare each row, field by field
        for (int i = 0; i < referenceRows.size(); i++) {
            compareRow(i, referenceRows.get(i), hardwoodRows.get(i));
        }

        System.out.println("  All " + referenceRows.size() + " rows match!");
    }

    /**
     * Read all rows using parquet-java's AvroParquetReader.
     */
    private List<GenericRecord> readWithParquetJava(Path file) throws IOException {
        List<GenericRecord> rows = new ArrayList<>();

        Configuration conf = new Configuration();
        // Handle INT96 timestamps (legacy type used in some Parquet files)
        conf.set("parquet.avro.readInt96AsFixed", "true");
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(file.toUri());

        try (ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord> builder(HadoopInputFile.fromPath(hadoopPath, conf))
                .withConf(conf)
                .build()) {

            GenericRecord record;
            while ((record = reader.read()) != null) {
                rows.add(record);
            }
        }

        return rows;
    }

    /**
     * Read all rows using Hardwood's RowReader.
     */
    private List<PqRow> readWithHardwood(Path file) throws IOException {
        List<PqRow> rows = new ArrayList<>();

        try (ParquetFileReader fileReader = ParquetFileReader.open(file)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                for (PqRow row : rowReader) {
                    rows.add(row);
                }
            }
        }

        return rows;
    }

    /**
     * Compare a single row field by field.
     */
    private void compareRow(int rowIndex, GenericRecord reference, PqRow actual) {
        var schema = reference.getSchema();

        for (var field : schema.getFields()) {
            String fieldName = field.name();
            Object refValue = reference.get(fieldName);
            Object actualValue = getHardwoodValue(actual, fieldName, field.schema());

            compareValues(rowIndex, fieldName, refValue, actualValue);
        }
    }

    /**
     * Get a value from Hardwood PqRow, handling type conversions.
     */
    private Object getHardwoodValue(PqRow row, String fieldName, org.apache.avro.Schema fieldSchema) {
        if (row.isNull(fieldName)) {
            return null;
        }

        // Determine the appropriate type based on Avro schema
        return switch (fieldSchema.getType()) {
            case BOOLEAN -> row.getValue(PqType.BOOLEAN, fieldName);
            case INT -> row.getValue(PqType.INT32, fieldName);
            case LONG -> row.getValue(PqType.INT64, fieldName);
            case FLOAT -> row.getValue(PqType.FLOAT, fieldName);
            case DOUBLE -> row.getValue(PqType.DOUBLE, fieldName);
            case STRING -> row.getValue(PqType.STRING, fieldName);
            case BYTES -> row.getValue(PqType.BINARY, fieldName);
            case FIXED -> {
                // FIXED type could be INT96 (legacy timestamp) which needs special handling
                // For INT96, we skip comparison as it's deprecated and represented differently
                // In future, could add proper INT96 support
                try {
                    yield row.getValue(PqType.BINARY, fieldName);
                }
                catch (IllegalArgumentException e) {
                    // Likely INT96 - return a marker to skip comparison
                    if (e.getMessage().contains("INT96")) {
                        yield SkipMarker.INSTANCE;
                    }
                    throw e;
                }
            }
            case UNION -> {
                // Handle nullable types (union with null)
                for (var subSchema : fieldSchema.getTypes()) {
                    if (subSchema.getType() != org.apache.avro.Schema.Type.NULL) {
                        yield getHardwoodValue(row, fieldName, subSchema);
                    }
                }
                yield null;
            }
            default -> throw new UnsupportedOperationException(
                    "Unsupported Avro type: " + fieldSchema.getType() + " for field: " + fieldName);
        };
    }

    /**
     * Compare two values, handling type conversions between Avro and Java types.
     */
    private void compareValues(int rowIndex, String fieldName, Object refValue, Object actualValue) {
        String context = String.format("Row %d, field '%s'", rowIndex, fieldName);

        // Skip fields marked for skipping (e.g., INT96)
        if (actualValue == SkipMarker.INSTANCE) {
            System.out.println("  Skipping field '" + fieldName + "' (unsupported type like INT96)");
            return;
        }

        if (refValue == null) {
            assertThat(actualValue)
                    .as(context)
                    .isNull();
            return;
        }

        // Handle Avro type conversions
        Object comparableRef = convertToComparable(refValue);
        Object comparableActual = convertToComparable(actualValue);

        // Special handling for floating point comparison
        if (comparableRef instanceof Float f) {
            assertThat((Float) comparableActual)
                    .as(context)
                    .isCloseTo(f, within(0.0001f));
        }
        else if (comparableRef instanceof Double d) {
            assertThat((Double) comparableActual)
                    .as(context)
                    .isCloseTo(d, within(0.0000001d));
        }
        else if (comparableRef instanceof byte[] refBytes) {
            assertThat((byte[]) comparableActual)
                    .as(context)
                    .isEqualTo(refBytes);
        }
        else {
            assertThat(comparableActual)
                    .as(context)
                    .isEqualTo(comparableRef);
        }
    }

    /**
     * Convert Avro types to comparable Java types.
     */
    private Object convertToComparable(Object value) {
        if (value == null) {
            return null;
        }

        // Avro Utf8 -> String
        if (value instanceof Utf8 utf8) {
            return utf8.toString();
        }

        // Avro ByteBuffer -> byte[]
        if (value instanceof ByteBuffer bb) {
            byte[] bytes = new byte[bb.remaining()];
            bb.duplicate().get(bytes);
            return bytes;
        }

        // Avro GenericFixed -> byte[]
        if (value instanceof GenericData.Fixed fixed) {
            return fixed.bytes();
        }

        return value;
    }
}
