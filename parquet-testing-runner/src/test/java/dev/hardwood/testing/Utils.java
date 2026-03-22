/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.assertj.core.api.ThrowableAssert;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.*;

public class Utils {

    /**
     * Marker to indicate a field should be skipped in comparison (e.g., INT96 timestamps).
     */
    enum SkipMarker {
        INSTANCE
    }

    /**
     * Files to skip in comparison tests.
     * Add files here with a comment explaining why they are skipped.
     */
     static final Set<String> SKIPPED_FILES = Set.of(
            // parquet-java Avro reader schema parsing issues
            "delta_encoding_required_column.parquet", // Illegal character in field name (c_customer_sk:)
            "hadoop_lz4_compressed.parquet", // Empty field name in schema

            // parquet-java Avro reader decoding issues
            "fixed_length_byte_array.parquet", // ParquetDecodingException
            "large_string_map.brotli.parquet", // ParquetDecodingException (block -1)
            "non_hadoop_lz4_compressed.parquet", // ParquetDecodingException (block -1)
            "nation.dict-malformed.parquet", // EOF error (intentionally malformed)

            // parquet-java Avro reader type conversion issues
            "map_no_value.parquet", // Map key type must be binary (UTF8)
            "nested_maps.snappy.parquet", // Map key type must be binary (UTF8)
            "repeated_no_annotation.parquet", // ClassCast: int64 number is not a group
            "repeated_primitive_no_list.parquet", // ClassCast: int32 Int32_list is not a group
            "unknown-logical-type.parquet", // Unknown logical type

            // shredded_variant files with parquet-java issues
            "case-040.parquet", // ParquetDecodingException
            "case-041.parquet", // NullPointer on Schema field
            "case-042.parquet", // ParquetDecodingException
            "case-087.parquet", // ParquetDecodingException
            "case-127.parquet", // Unsupported shredded value type: INTEGER(32,false)
            "case-128.parquet", // ParquetDecodingException
            "case-131.parquet", // NullPointer on Schema field
            "case-137.parquet", // Unsupported shredded value type
            "case-138.parquet", // NullPointer on Schema field
            // Intentionally corrupted CRC checksums (rejected by Hardwood CRC validation)
            "datapage_v1-corrupt-checksum.parquet",
            "rle-dict-uncompressed-corrupt-checksum.parquet",

            // bad_data files (intentionally malformed, rejected by Hardwood)
            "PARQUET-1481.parquet",
            "ARROW-RS-GH-6229-DICTHEADER.parquet",
            "ARROW-RS-GH-6229-LEVELS.parquet",
            "ARROW-GH-41317.parquet",
            "ARROW-GH-41321.parquet",
            "ARROW-GH-45185.parquet"
    );

    /**
     * Directories containing test parquet files.
     */
    private static final List<String> TEST_DIRECTORIES = List.of(
            "data",
            "bad_data",
            "shredded_variant");

    /**
     * Provides all .parquet files from the parquet-testing test directories.
     */
    static Stream<Path> parquetTestFiles() throws IOException {
        Path repoDir = ParquetTestingRepoCloner.ensureCloned();
        return TEST_DIRECTORIES.stream()
                .map(repoDir::resolve)
                .filter(Files::exists)
                .flatMap(dir -> {
                    try {
                        return Files.list(dir);
                    }
                    catch (IOException e) {
                        return Stream.empty();
                    }
                })
                .filter(p -> p.toString().endsWith(".parquet"))
                .sorted();
    }

    static void assertBadDataRejected(String fileName, ThrowableAssert.ThrowingCallable action) throws IOException {
        assertThatThrownBy(action)
                .as("Expected %s to be rejected", fileName);
    }

    static void assertBadDataRejected(String fileName, String expectedMessage, ThrowableAssert.ThrowingCallable action) throws IOException {
        assertThatThrownBy(action)
                .as("Expected %s to be rejected", fileName)
                .hasStackTraceContaining(expectedMessage);
    }

    /**
     * Read all rows using parquet-java's AvroParquetReader.
     */
    static List<GenericRecord> readWithParquetJava(Path file) throws IOException {
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
     * Compare a single row field by field.
     */
    static void compareRow(int rowIndex, GenericRecord reference, RowReader rowReader) {
        var schema = reference.getSchema();

        for (var field : schema.getFields()) {
            String fieldName = field.name();
            Object refValue = reference.get(fieldName);
            Object actualValue = getHardwoodValue(rowReader, fieldName, field.schema());

            compareValues(rowIndex, fieldName, refValue, actualValue);
        }
    }

    /**
     * Get a value from Hardwood RowReader, handling type conversions.
     */
    static Object getHardwoodValue(RowReader rowReader, String fieldName, org.apache.avro.Schema fieldSchema) {
        if (rowReader.isNull(fieldName)) {
            return null;
        }

        // Determine the appropriate type based on Avro schema
        return switch (fieldSchema.getType()) {
            case BOOLEAN -> rowReader.getBoolean(fieldName);
            case INT -> rowReader.getInt(fieldName);
            case LONG -> rowReader.getLong(fieldName);
            case FLOAT -> rowReader.getFloat(fieldName);
            case DOUBLE -> rowReader.getDouble(fieldName);
            case STRING -> rowReader.getString(fieldName);
            case BYTES -> rowReader.getBinary(fieldName);
            case FIXED -> {
                // FIXED type could be INT96 (legacy timestamp) which needs special handling
                // For INT96, we skip comparison as it's deprecated and represented differently
                try {
                    yield rowReader.getBinary(fieldName);
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
                        yield getHardwoodValue(rowReader, fieldName, subSchema);
                    }
                }
                yield null;
            }
            case RECORD -> {
                // Nested struct - return marker to skip for now
                // TODO: implement nested struct comparison
                yield SkipMarker.INSTANCE;
            }
            case ARRAY -> {
                // List type - return marker to skip for now
                // TODO: implement list comparison
                yield SkipMarker.INSTANCE;
            }
            case MAP -> {
                // Map type - return marker to skip for now
                // TODO: implement map comparison
                yield SkipMarker.INSTANCE;
            }
            case ENUM -> {
                // Enum type - read as string
                yield rowReader.getString(fieldName);
            }
            default -> throw new UnsupportedOperationException(
                    "Unsupported Avro type: " + fieldSchema.getType() + " for field: " + fieldName);
        };
    }

    /**
     * Compare two values, handling type conversions between Avro and Java types.
     */
    static void compareValues(int rowIndex, String fieldName, Object refValue, Object actualValue) {
        String context = String.format("Row %d, field '%s'", rowIndex, fieldName);

        // Skip fields marked for skipping (e.g., INT96, nested types)
        if (actualValue == SkipMarker.INSTANCE) {
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
    static Object convertToComparable(Object value) {
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

    // ==================== Column-Level Comparison ====================

    /**
     * Additional files to skip in column-level comparison tests.
     * Files with nested/repeated columns where column-level comparison
     * requires list reconstruction from offsets (deferred).
     */
    static final Set<String> COLUMN_SKIPPED_FILES = Set.of(
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

    /**
     * Compare a Parquet file column-by-column using ColumnReaders against parquet-java reference data.
     * Each column reader is created via the factory and closed after use.
     */
    static void compareColumns(FileSchema schema, ColumnReaderFactory readerFactory, List<GenericRecord> referenceRows)
            throws IOException {

        for (int colIdx = 0; colIdx < schema.getColumnCount(); colIdx++) {
            ColumnSchema colSchema = schema.getColumn(colIdx);

            // Skip nested/repeated columns
            if (colSchema.maxRepetitionLevel() > 0) {
                continue;
            }

            String colName = colSchema.name();
            try (ColumnReader columnReader = readerFactory.createColumnReader(colIdx)) {
                compareColumnReader(colName, columnReader, referenceRows);
            }
        }
    }

    /**
     * Compare a single ColumnReader's data against reference rows.
     */
    static void compareColumnReader(String colName, ColumnReader columnReader, List<GenericRecord> referenceRows) {
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

                if (refValue == null || refValue == SkipMarker.INSTANCE) {
                    if (refValue == null) {
                        assertThat(nulls != null && nulls.get(i))
                                .as("Row %d, column '%s' should be null", rowIdx, colName)
                                .isTrue();
                    }
                }
                else if (nulls != null && nulls.get(i)) {
                    assertThat(refValue)
                            .as("Row %d, column '%s' should not be null", rowIdx, colName)
                            .isNull();
                }
                else {
                    compareColumnValue(rowIdx, colName, refValue, columnReader, i);
                }
                rowIdx++;
            }
        }

        assertThat(rowIdx)
                .as("Column '%s' row count", colName)
                .isEqualTo(referenceRows.size());
    }

    /**
     * Functional interface for creating column readers (abstracts single-file vs multi-file).
     */
    @FunctionalInterface
    interface ColumnReaderFactory {
        ColumnReader createColumnReader(int columnIndex) throws IOException;
    }

    /**
     * Get reference value for a flat column from a GenericRecord.
     */
    private static Object getRefColumnValue(GenericRecord record, String fieldName) {
        var field = record.getSchema().getField(fieldName);
        if (field == null) {
            return SkipMarker.INSTANCE;
        }
        Object value = record.get(fieldName);
        if (value == null) {
            return null;
        }
        var fieldSchema = field.schema();
        if (fieldSchema.getType() == org.apache.avro.Schema.Type.UNION) {
            for (var subSchema : fieldSchema.getTypes()) {
                if (subSchema.getType() != org.apache.avro.Schema.Type.NULL) {
                    fieldSchema = subSchema;
                    break;
                }
            }
        }
        return switch (fieldSchema.getType()) {
            case RECORD, ARRAY, MAP -> SkipMarker.INSTANCE;
            case FIXED -> SkipMarker.INSTANCE; // INT96
            default -> convertToComparable(value);
        };
    }

    /**
     * Compare a single column value from the ColumnReader batch against the reference.
     */
    private static void compareColumnValue(int rowIdx, String colName, Object refValue,
                                           ColumnReader reader, int batchIdx) {
        String context = String.format("Row %d, column '%s'", rowIdx, colName);
        Object actual = getColumnReaderValue(reader, batchIdx);
        Object comparableActual = convertToComparable(actual);

        if (refValue instanceof String refStr && comparableActual instanceof byte[] actualBytes) {
            assertThat(new String(actualBytes, java.nio.charset.StandardCharsets.UTF_8))
                    .as(context).isEqualTo(refStr);
        }
        else if (refValue instanceof Float f) {
            assertThat((Float) comparableActual).as(context).isCloseTo(f, within(0.0001f));
        }
        else if (refValue instanceof Double d) {
            assertThat((Double) comparableActual).as(context).isCloseTo(d, within(0.0000001d));
        }
        else if (refValue instanceof byte[] refBytes) {
            assertThat((byte[]) comparableActual).as(context).isEqualTo(refBytes);
        }
        else {
            assertThat(comparableActual).as(context).isEqualTo(refValue);
        }
    }

    /**
     * Get a value from the ColumnReader at a batch index, using the appropriate typed array.
     */
    private static Object getColumnReaderValue(ColumnReader reader, int index) {
        var colSchema = reader.getColumnSchema();
        return switch (colSchema.type()) {
            case INT32 -> reader.getInts()[index];
            case INT64 -> reader.getLongs()[index];
            case FLOAT -> reader.getFloats()[index];
            case DOUBLE -> reader.getDoubles()[index];
            case BOOLEAN -> reader.getBooleans()[index];
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> {
                if (colSchema.logicalType() instanceof LogicalType.StringType
                        || colSchema.logicalType() instanceof LogicalType.JsonType
                        || colSchema.logicalType() instanceof LogicalType.BsonType) {
                    yield reader.getStrings()[index];
                }
                yield reader.getBinaries()[index];
            }
        };
    }
}
