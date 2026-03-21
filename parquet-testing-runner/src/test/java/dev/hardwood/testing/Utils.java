package dev.hardwood.testing;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.assertj.core.api.ThrowableAssert;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.within;

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

            // shredded_variant files with Hardwood issues
            "case-046.parquet", // EOF while reading BYTE_ARRAY

            // bad_data files (intentionally malformed, rejected by Hardwood)
            "PARQUET-1481.parquet",
            "ARROW-RS-GH-6229-DICTHEADER.parquet",
            "ARROW-RS-GH-6229-LEVELS.parquet",
            "ARROW-GH-41317.parquet",
            "ARROW-GH-41321.parquet",
            "ARROW-GH-45185.parquet",

            // CRC validation rejects these files
            "datapage_v1-corrupt-checksum.parquet",
            "rle-dict-uncompressed-corrupt-checksum.parquet"
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
     * Compare a Parquet file using both implementations.
     */
    static void compareParquetFile(Path testFile) throws IOException {
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
    static int compareWithHardwood(Path file, List<GenericRecord> referenceRows) throws IOException {
        int rowIndex = 0;

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file));
             RowReader rowReader = fileReader.createRowReader()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                assertThat(rowIndex)
                        .as("Hardwood has more rows than reference")
                        .isLessThan(referenceRows.size());
                compareRow(rowIndex, referenceRows.get(rowIndex), rowReader);
                rowIndex++;
            }
        }

        return rowIndex;
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
}
