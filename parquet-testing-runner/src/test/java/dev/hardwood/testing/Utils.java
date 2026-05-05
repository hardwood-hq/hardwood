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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.assertj.core.api.ThrowableAssert;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.row.StructAccessor;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

import static org.assertj.core.api.Assertions.*;

public class Utils {

    /// Marker to indicate a field should be skipped in comparison (e.g., INT96 timestamps).
    enum SkipMarker {
        INSTANCE
    }

    /// Files to skip in comparison tests.
    /// Add files here with a comment explaining why they are skipped.
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

            // shredded_variant INVALID fixtures: the parquet-testing cases.json
            // flags these as "not valid according to the spec; implementations
            // can choose to error or read the shredded value." parquet-java
            // and Hardwood diverge in which interpretation they pick, so row-
            // level comparison is not meaningful.
            "case-043-INVALID.parquet",
            "case-125-INVALID.parquet",

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

    /// Returns a GitHub issue reference blocking row-level nested comparison for
    /// `testFile`, or `null` if the file can be compared. No files are currently
    /// blocked — shredded Variant reassembly (#286) is now handled end-to-end,
    /// so `shredded_variant/*.parquet` files round-trip through the comparison.
    static String rowComparisonSkipReason(Path testFile) {
        return null;
    }

    /// Directories containing test parquet files.
    private static final List<String> TEST_DIRECTORIES = List.of(
            "data",
            "bad_data",
            "shredded_variant");

    /// Generates `data/good_c.parquet` under the given cloned repo if not already present.
    /// The file is a valid single-column (`required int32 c`) file sharing its schema with
    /// `bad_data/ARROW-RS-GH-6229-LEVELS.parquet`, so the mid-sequence bad-data tests can
    /// read a good prefix followed by a failing file. Idempotent.
    static void ensureGoodCFile(Path repoDir) throws IOException {
        Path output = repoDir.resolve("data/good_c.parquet");
        if (Files.exists(output)) {
            return;
        }
        MessageType schema = MessageTypeParser.parseMessageType(
                "message schema { required int32 c; }");
        Configuration conf = new Configuration();
        org.apache.hadoop.fs.Path hadoopPath =
                new org.apache.hadoop.fs.Path(output.toUri());
        try (ParquetWriter<Group> writer = ExampleParquetWriter
                .builder(hadoopPath)
                .withConf(conf)
                .withType(schema)
                .build()) {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            writer.write(factory.newGroup().append("c", 1));
            writer.write(factory.newGroup().append("c", 2));
            writer.write(factory.newGroup().append("c", 3));
        }
    }

    /// Provides all .parquet files from the parquet-testing test directories.
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
                .hasMessage(expectedMessage);
    }

    /// Read all rows using parquet-java's AvroParquetReader.
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

    /// Compare a single row field by field.
    static void compareRow(int rowIndex, GenericRecord reference, RowReader rowReader, FileSchema schema) {
        compareStruct("Row " + rowIndex, reference, rowReader, schema.getRootNode());
    }

    /// Compare all fields of a struct-like value against an Avro [GenericRecord].
    /// The `group` parameter carries the corresponding Hardwood schema node so
    /// Variant-annotated fields can be dispatched via [SchemaNode.GroupNode#isVariant()]
    /// instead of a runtime accessor check. May be `null` when schema context is
    /// unavailable (inside list/map elements); in that case Variant-in-list dispatch
    /// falls back to the struct path.
    private static void compareStruct(String path, GenericRecord reference, StructAccessor actual,
            SchemaNode.GroupNode group) {
        org.apache.avro.Schema schema = reference.getSchema();
        for (org.apache.avro.Schema.Field field : schema.getFields()) {
            String fieldName = field.name();
            Object refValue = reference.get(fieldName);
            String fieldPath = path + ", field '" + fieldName + "'";
            compareField(fieldPath, fieldName, field.schema(), refValue, actual, group);
        }
    }

    /// Compare a single named field against its reference value, dispatching on Avro type.
    private static void compareField(String context, String fieldName,
            org.apache.avro.Schema fieldSchema, Object refValue, StructAccessor actual,
            SchemaNode.GroupNode parentGroup) {
        org.apache.avro.Schema resolved = resolveNonNull(fieldSchema);

        if (refValue == null) {
            assertThat(actual.isNull(fieldName))
                    .as(context + " should be null")
                    .isTrue();
            return;
        }
        assertThat(actual.isNull(fieldName))
                .as(context + " should not be null")
                .isFalse();

        SchemaNode childNode = findChild(parentGroup, fieldName);

        switch (resolved.getType()) {
            case RECORD -> {
                if (childNode instanceof SchemaNode.GroupNode childGroup && childGroup.isVariant()) {
                    compareVariant(context, (GenericRecord) refValue, actual.getVariant(fieldName));
                }
                else {
                    PqStruct nested = actual.getStruct(fieldName);
                    assertThat(nested).as(context).isNotNull();
                    SchemaNode.GroupNode nestedGroup = childNode instanceof SchemaNode.GroupNode g ? g : null;
                    compareStruct(context, (GenericRecord) refValue, nested, nestedGroup);
                }
            }
            case ARRAY -> {
                PqList list = actual.getList(fieldName);
                assertThat(list).as(context).isNotNull();
                compareList(context, resolved.getElementType(), (Collection<?>) refValue, list);
            }
            case MAP -> {
                PqMap map = actual.getMap(fieldName);
                assertThat(map).as(context).isNotNull();
                compareMap(context, resolved.getValueType(), (Map<?, ?>) refValue, map);
            }
            default -> {
                Object actualValue = getLeafValue(actual, fieldName, resolved);
                compareLeafValues(context, refValue, actualValue);
            }
        }
    }

    private static SchemaNode findChild(SchemaNode.GroupNode parentGroup, String name) {
        if (parentGroup == null) {
            return null;
        }
        for (SchemaNode child : parentGroup.children()) {
            if (child.name().equals(name)) {
                return child;
            }
        }
        return null;
    }

    /// Compare a Variant-annotated column by byte-comparing the canonical
    /// `metadata` and `value` binaries against those surfaced by parquet-java's
    /// Avro reader (which sees the Variant group as a plain record).
    private static void compareVariant(String context, GenericRecord reference, PqVariant actual) {
        assertThat(actual).as(context + " variant").isNotNull();
        byte[] refMetadata = toBytes(reference.get("metadata"));
        byte[] refValue = toBytes(reference.get("value"));
        assertThat(actual.metadata())
                .as(context + " variant metadata bytes")
                .isEqualTo(refMetadata);
        assertThat(actual.value())
                .as(context + " variant value bytes")
                .isEqualTo(refValue);
    }

    private static byte[] toBytes(Object avroBinary) {
        if (avroBinary == null) {
            return null;
        }
        if (avroBinary instanceof ByteBuffer bb) {
            byte[] out = new byte[bb.remaining()];
            bb.duplicate().get(out);
            return out;
        }
        if (avroBinary instanceof byte[] b) {
            return b;
        }
        throw new IllegalArgumentException("Unexpected Avro binary value type: " + avroBinary.getClass());
    }

    /// Unwrap a nullable UNION to its single non-null branch. Returns the input
    /// unchanged for non-union schemas.
    private static org.apache.avro.Schema resolveNonNull(org.apache.avro.Schema schema) {
        if (schema.getType() != org.apache.avro.Schema.Type.UNION) {
            return schema;
        }
        for (org.apache.avro.Schema sub : schema.getTypes()) {
            if (sub.getType() != org.apache.avro.Schema.Type.NULL) {
                return sub;
            }
        }
        return schema;
    }

    /// Read a leaf (non-nested) field value from a [StructAccessor] based on its Avro type.
    private static Object getLeafValue(StructAccessor actual, String fieldName,
            org.apache.avro.Schema fieldSchema) {
        return switch (fieldSchema.getType()) {
            case BOOLEAN -> actual.getBoolean(fieldName);
            case INT -> actual.getInt(fieldName);
            case LONG -> actual.getLong(fieldName);
            case FLOAT -> actual.getFloat(fieldName);
            case DOUBLE -> actual.getDouble(fieldName);
            case STRING -> actual.getString(fieldName);
            case BYTES -> actual.getBinary(fieldName);
            case FIXED -> {
                // FIXED type could be INT96 (legacy timestamp) which needs special handling;
                // INT96 is deprecated and represented differently, so we skip comparison.
                try {
                    yield actual.getBinary(fieldName);
                }
                catch (IllegalArgumentException e) {
                    if (e.getMessage().contains("INT96")) {
                        yield SkipMarker.INSTANCE;
                    }
                    throw e;
                }
            }
            case ENUM -> actual.getString(fieldName);
            default -> throw new UnsupportedOperationException(
                    "Unsupported Avro type: " + fieldSchema.getType() + " for field: " + fieldName);
        };
    }

    /// Compare a leaf (non-nested) reference value against the Hardwood-side value.
    static void compareLeafValues(String context, Object refValue, Object actualValue) {
        if (actualValue == SkipMarker.INSTANCE) {
            return;
        }
        if (refValue == null) {
            assertThat(actualValue).as(context).isNull();
            return;
        }

        Object comparableRef = convertToComparable(refValue);
        Object comparableActual = convertToComparable(actualValue);

        if (comparableRef instanceof Float f) {
            assertThat((Float) comparableActual).as(context).isCloseTo(f, within(0.0001f));
        }
        else if (comparableRef instanceof Double d) {
            assertThat((Double) comparableActual).as(context).isCloseTo(d, within(0.0000001d));
        }
        else if (comparableRef instanceof byte[] refBytes) {
            assertThat((byte[]) comparableActual).as(context).isEqualTo(refBytes);
        }
        else {
            assertThat(comparableActual).as(context).isEqualTo(comparableRef);
        }
    }

    /// Compare an Avro collection against a Hardwood [PqList] element-by-element.
    /// Element dispatch follows the Avro element schema, recursing for RECORD/ARRAY/MAP.
    private static void compareList(String context, org.apache.avro.Schema elementSchema,
            Collection<?> refList, PqList actualList) {
        assertThat(actualList.size()).as(context + " size").isEqualTo(refList.size());
        org.apache.avro.Schema resolvedElement = resolveNonNull(elementSchema);

        // parquet-java's AvroParquetReader wraps 3-level Parquet LIST elements in a
        // synthetic single-field record (repeated group's child, typically named
        // "element", "item", or "array_element" depending on the writer). Hardwood
        // flattens these, so unwrap the Avro side to match before recursing.
        if (resolvedElement.getType() == org.apache.avro.Schema.Type.RECORD
                && resolvedElement.getFields().size() == 1) {
            org.apache.avro.Schema.Field wrapped = resolvedElement.getFields().get(0);
            List<Object> unwrappedList = new ArrayList<>(refList.size());
            for (Object e : refList) {
                unwrappedList.add(e == null ? null : ((GenericRecord) e).get(wrapped.name()));
            }
            compareList(context, wrapped.schema(), unwrappedList, actualList);
            return;
        }

        int i = 0;
        Iterator<?> refIt = refList.iterator();
        Iterator<PqStruct> structIt = null;
        Iterator<PqList> listIt = null;
        Iterator<PqMap> mapIt = null;
        switch (resolvedElement.getType()) {
            case RECORD -> structIt = actualList.structs().iterator();
            case ARRAY -> listIt = actualList.lists().iterator();
            case MAP -> mapIt = actualList.maps().iterator();
            default -> { /* leaf: access via typed index below */ }
        }

        while (refIt.hasNext()) {
            String elemContext = context + "[" + i + "]";
            Object refElement = refIt.next();
            boolean actualNull = actualList.isNull(i);

            if (refElement == null) {
                assertThat(actualNull).as(elemContext + " should be null").isTrue();
                switch (resolvedElement.getType()) {
                    case RECORD -> structIt.next();
                    case ARRAY -> listIt.next();
                    case MAP -> mapIt.next();
                    default -> { /* no-op for leaf */ }
                }
            }
            else {
                assertThat(actualNull).as(elemContext + " should not be null").isFalse();
                switch (resolvedElement.getType()) {
                    case RECORD -> compareStruct(elemContext, (GenericRecord) refElement, structIt.next(), null);
                    case ARRAY -> compareList(elemContext, resolvedElement.getElementType(),
                            (Collection<?>) refElement, listIt.next());
                    case MAP -> compareMap(elemContext, resolvedElement.getValueType(),
                            (Map<?, ?>) refElement, mapIt.next());
                    default -> compareLeafValues(elemContext, refElement,
                            getLeafListElement(actualList, i, resolvedElement));
                }
            }
            i++;
        }
    }

    /// Read a leaf element at `index` from a [PqList], picking the typed accessor
    /// matching the Avro element schema. Requires a full-iterator scan per call —
    /// acceptable for test code where list sizes are small.
    private static Object getLeafListElement(PqList list, int index, org.apache.avro.Schema elementSchema) {
        Iterable<?> iterable = switch (elementSchema.getType()) {
            case BOOLEAN -> list.booleans();
            case INT -> list.ints();
            case LONG -> list.longs();
            case FLOAT -> list.floats();
            case DOUBLE -> list.doubles();
            case STRING, ENUM -> list.strings();
            case BYTES, FIXED -> list.binaries();
            default -> throw new UnsupportedOperationException(
                    "Unsupported Avro list element type: " + elementSchema.getType());
        };
        Iterator<?> it = iterable.iterator();
        for (int i = 0; i < index; i++) {
            it.next();
        }
        return it.next();
    }

    /// Compare an Avro map against a Hardwood [PqMap] entry-by-entry.
    /// Parquet-java's Avro reader only supports UTF8 string keys (other key types
    /// are in [SKIPPED_FILES]), so we match entries by string key via a lookup map.
    private static void compareMap(String context, org.apache.avro.Schema valueSchema,
            Map<?, ?> refMap, PqMap actualMap) {
        assertThat(actualMap.size()).as(context + " size").isEqualTo(refMap.size());
        org.apache.avro.Schema resolvedValue = resolveNonNull(valueSchema);

        java.util.Map<String, PqMap.Entry> actualByKey = new java.util.HashMap<>();
        for (PqMap.Entry entry : actualMap.getEntries()) {
            actualByKey.put(entry.getStringKey(), entry);
        }

        for (Map.Entry<?, ?> refEntry : refMap.entrySet()) {
            String key = refEntry.getKey().toString();
            String entryContext = context + "[key='" + key + "']";
            PqMap.Entry actualEntry = actualByKey.get(key);
            assertThat(actualEntry)
                    .as(entryContext + " missing from Hardwood map")
                    .isNotNull();

            Object refValue = refEntry.getValue();
            if (refValue == null) {
                assertThat(actualEntry.isValueNull())
                        .as(entryContext + " value should be null")
                        .isTrue();
                continue;
            }
            assertThat(actualEntry.isValueNull())
                    .as(entryContext + " value should not be null")
                    .isFalse();

            switch (resolvedValue.getType()) {
                case RECORD -> compareStruct(entryContext, (GenericRecord) refValue,
                        actualEntry.getStructValue(), null);
                case ARRAY -> compareList(entryContext, resolvedValue.getElementType(),
                        (Collection<?>) refValue, actualEntry.getListValue());
                case MAP -> compareMap(entryContext, resolvedValue.getValueType(),
                        (Map<?, ?>) refValue, actualEntry.getMapValue());
                default -> compareLeafValues(entryContext, refValue,
                        getLeafMapValue(actualEntry, resolvedValue));
            }
        }
    }

    /// Read a leaf (non-nested) map value via the typed accessor matching the Avro schema.
    private static Object getLeafMapValue(PqMap.Entry entry, org.apache.avro.Schema valueSchema) {
        return switch (valueSchema.getType()) {
            case BOOLEAN -> entry.getBooleanValue();
            case INT -> entry.getIntValue();
            case LONG -> entry.getLongValue();
            case FLOAT -> entry.getFloatValue();
            case DOUBLE -> entry.getDoubleValue();
            case STRING, ENUM -> entry.getStringValue();
            case BYTES, FIXED -> entry.getBinaryValue();
            default -> throw new UnsupportedOperationException(
                    "Unsupported Avro map value type: " + valueSchema.getType());
        };
    }

    /// Convert Avro types to comparable Java types.
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

    /// Additional files to skip in column-level comparison tests beyond [#SKIPPED_FILES].
    /// The remaining entry exercises Avro HashMap-backed map iteration order
    /// against Hardwood's column-stream order, which the column-level
    /// comparator can't yet canonicalize.
    static final Set<String> COLUMN_SKIPPED_FILES = Set.of(
            "nullable.impala.parquet"
    );

    /// Compare a Parquet file column-by-column using [ColumnReader]s against parquet-java reference data.
    static void compareColumns(ParquetFileReader fileReader, List<GenericRecord> referenceRows)
            throws IOException {
        FileSchema schema = fileReader.getFileSchema();
        for (int colIdx = 0; colIdx < schema.getColumnCount(); colIdx++) {
            ColumnSchema colSchema = schema.getColumn(colIdx);
            // parquet-java's AvroParquetReader collapses Variant `typed_value`
            // subtrees into the canonical `value` binary, so reference data for
            // shredded subcolumns is unavailable. Skip them.
            if (isUnderVariantShred(schema, colSchema)) {
                continue;
            }
            try (ColumnReader columnReader = fileReader.columnReader(colIdx)) {
                if (colSchema.maxRepetitionLevel() == 0) {
                    compareColumnReader(colSchema.name(), columnReader, referenceRows);
                }
                else {
                    compareNestedColumnReader(colSchema, columnReader, referenceRows);
                }
            }
        }
    }

    /// Returns true when the column lives inside a Variant group's `typed_value`
    /// shred subtree. Walks the schema along the column's [dev.hardwood.metadata.FieldPath]
    /// looking for a `typed_value` step that occurs after a Variant-annotated ancestor.
    private static boolean isUnderVariantShred(FileSchema schema, ColumnSchema colSchema) {
        SchemaNode current = schema.getRootNode();
        boolean variantAncestor = false;
        for (String name : colSchema.fieldPath().elements()) {
            if (!(current instanceof SchemaNode.GroupNode group)) {
                return false;
            }
            if (group.isVariant()) {
                variantAncestor = true;
            }
            if (variantAncestor && "typed_value".equals(name)) {
                return true;
            }
            SchemaNode next = null;
            for (SchemaNode child : group.children()) {
                if (child.name().equals(name)) {
                    next = child;
                    break;
                }
            }
            if (next == null) {
                return false;
            }
            current = next;
        }
        return false;
    }

    /// Compare a single ColumnReader's data against reference rows.
    static void compareColumnReader(String colName, ColumnReader columnReader, List<GenericRecord> referenceRows) {
        int rowIdx = 0;

        while (columnReader.nextBatch()) {
            int count = columnReader.getRecordCount();
            BitSet nulls = columnReader.getElementNulls();

            for (int i = 0; i < count; i++) {
                assertThat(rowIdx)
                        .as("Column '%s' produced more records than reference", colName)
                        .isLessThan(referenceRows.size());
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

    /// Compare a nested column (`maxRepetitionLevel > 0`) against parquet-java by
    /// reconstructing each top-level record's value from the [ColumnReader]'s
    /// offsets/level-null/element-null arrays and matching it to the same value
    /// extracted from the Avro [GenericRecord] along the column's [dev.hardwood.metadata.FieldPath].
    static void compareNestedColumnReader(ColumnSchema colSchema, ColumnReader reader,
                                          List<GenericRecord> referenceRows) {
        int maxRep = colSchema.maxRepetitionLevel();
        List<String> path = colSchema.fieldPath().elements();
        int rowIdx = 0;
        while (reader.nextBatch()) {
            int recordCount = reader.getRecordCount();
            for (int r = 0; r < recordCount; r++) {
                assertThat(rowIdx)
                        .as("Column '%s' produced more records than reference", colSchema.name())
                        .isLessThan(referenceRows.size());
                Object reconstructed = reconstructNested(reader, 0, r, maxRep);
                Object expected = extractAvroAlongPath(referenceRows.get(rowIdx), path, 0);
                String ctx = String.format("Row %d, column '%s'", rowIdx, colSchema.name());
                deepCompareNested(ctx, expected, reconstructed);
                rowIdx++;
            }
        }
        assertThat(rowIdx)
                .as("Column '%s' row count", colSchema.name())
                .isEqualTo(referenceRows.size());
    }

    /// Reconstruct one container's value from a nested [ColumnReader]'s
    /// offsets / level-nulls / empty-list markers / element-nulls. Returns
    /// `null` when the container at `level` is null, an empty `List` when the
    /// container is present-but-empty, or a populated `List` otherwise.
    private static Object reconstructNested(ColumnReader reader, int level, int idx, int maxRep) {
        BitSet levelNulls = reader.getLevelNulls(level);
        if (levelNulls != null && levelNulls.get(idx)) {
            return null;
        }
        BitSet emptyMarkers = reader.getEmptyListMarkers(level);
        if (emptyMarkers != null && emptyMarkers.get(idx)) {
            return List.of();
        }
        int[] offsets = reader.getOffsets(level);
        int start = offsets[idx];
        int end = (idx + 1 < offsets.length) ? offsets[idx + 1]
                : (level + 1 == maxRep ? reader.getValueCount() : reader.getOffsets(level + 1).length);
        if (level + 1 == maxRep) {
            BitSet elementNulls = reader.getElementNulls();
            List<Object> leaves = new ArrayList<>(end - start);
            for (int v = start; v < end; v++) {
                leaves.add(elementNulls != null && elementNulls.get(v)
                        ? null : convertToComparable(getColumnReaderValue(reader, v)));
            }
            return leaves;
        }
        List<Object> items = new ArrayList<>(end - start);
        for (int j = start; j < end; j++) {
            items.add(reconstructNested(reader, level + 1, j, maxRep));
        }
        return items;
    }

    /// Path components that name Parquet structural groups Avro elides when it
    /// lifts a LIST/MAP into native `array[]` / `map[]`. Includes the standard
    /// 3-level encoding (`list`, `element`, `key_value`) and the legacy
    /// 2-level LIST encoding rule 3a (`array`); rule 3b's `<list-name>_tuple` is
    /// matched by suffix, see [#isStructuralListGroup].
    /// Any other unknown name is treated as a real bug, not silently elided.
    private static final Set<String> STRUCTURAL_GROUP_NAMES = Set.of(
            "list", "element", "key_value", "array");

    /// Walk a parquet-java [GenericRecord] along a leaf column's
    /// [dev.hardwood.metadata.FieldPath]. Avro elides Parquet's structural
    /// group names (`list`, `element`, `key_value`, `array`, `<name>_tuple`)
    /// when it lifts them into `array[]` / `map[]`; those components are
    /// skipped during the walk. Any other path component that doesn't match
    /// a field on the current record is a bug — fail loudly rather than
    /// silently no-op. List elements receive the rest of the path applied
    /// per-element; map keys/values are projected by name.
    private static Object extractAvroAlongPath(Object current, List<String> path, int pathIdx) {
        while (pathIdx < path.size() && current != null) {
            String name = path.get(pathIdx);
            if (current instanceof GenericRecord rec) {
                if (rec.getSchema().getField(name) != null) {
                    current = rec.get(name);
                    pathIdx++;
                    continue;
                }
                if (isStructuralListGroup(name)) {
                    pathIdx++;
                    continue;
                }
                throw new IllegalStateException(
                        "Path component '" + name + "' is neither a field of Avro record '"
                                + rec.getSchema().getName() + "' nor a known Parquet structural"
                                + " group name; full path: " + path);
            }
            if (current instanceof Map<?, ?> map) {
                if ("key".equals(name)) {
                    List<Object> keys = new ArrayList<>(map.size());
                    for (Object k : map.keySet()) {
                        keys.add(convertToComparable(k));
                    }
                    return keys;
                }
                if ("value".equals(name)) {
                    List<Object> values = new ArrayList<>(map.size());
                    for (Object v : map.values()) {
                        values.add(extractAvroAlongPath(v, path, pathIdx + 1));
                    }
                    return values;
                }
                // `key_value` is the standard structural group name for MAPs;
                // `map` is the legacy alias used by older writers (e.g. Hive,
                // Impala). Both elide on the Avro side.
                if ("key_value".equals(name) || "map".equals(name)) {
                    pathIdx++;
                    continue;
                }
                // A map can sit inside a LIST element (e.g. `arr.list.element.map.key_value...`).
                // After the outer list distributes, the recursive walk arrives
                // here with the list's structural names still ahead; elide them.
                if (isStructuralListGroup(name)) {
                    pathIdx++;
                    continue;
                }
                throw new IllegalStateException(
                        "Path component '" + name + "' is not a valid map projection (expected"
                                + " 'key', 'value', 'key_value', or 'map'); full path: " + path);
            }
            if (current instanceof List<?> list) {
                List<Object> mapped = new ArrayList<>(list.size());
                for (Object e : list) {
                    mapped.add(extractAvroAlongPath(e, path, pathIdx));
                }
                return mapped;
            }
            // Reached a leaf primitive before the path was exhausted
            break;
        }
        return current == null ? null : convertToComparable(current);
    }

    private static boolean isStructuralListGroup(String name) {
        return STRUCTURAL_GROUP_NAMES.contains(name) || name.endsWith("_tuple");
    }

    /// Deep-compare two reconstructed values (each null, a leaf, or a `List`).
    /// Both `expected` and `actual` have already been passed through
    /// [#convertToComparable] at construction time (see [#extractAvroAlongPath]
    /// and [#reconstructNested]); leaves are delegated to [#compareLeaf].
    private static void deepCompareNested(String ctx, Object expected, Object actual) {
        if (expected == null) {
            assertThat(actual).as(ctx + " should be null").isNull();
            return;
        }
        assertThat(actual).as(ctx + " should not be null").isNotNull();
        if (expected instanceof List<?> expList && actual instanceof List<?> actList) {
            assertThat(actList).as(ctx + " size").hasSize(expList.size());
            for (int i = 0; i < expList.size(); i++) {
                deepCompareNested(ctx + "[" + i + "]", expList.get(i), actList.get(i));
            }
            return;
        }
        compareLeaf(ctx, expected, actual);
    }

    /// Get reference value for a flat column from a GenericRecord.
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

    /// Compare a single column value from the ColumnReader batch against the reference.
    private static void compareColumnValue(int rowIdx, String colName, Object refValue,
                                           ColumnReader reader, int batchIdx) {
        String context = String.format("Row %d, column '%s'", rowIdx, colName);
        Object actual = getColumnReaderValue(reader, batchIdx);
        compareLeaf(context, refValue, convertToComparable(actual));
    }

    /// Leaf-value comparison shared by flat ([#compareColumnValue]) and nested
    /// ([#deepCompareNested]) paths. Both arguments are expected to have already
    /// been run through [#convertToComparable] by the caller. Applies numeric
    /// tolerance for `Float`/`Double` and decodes UTF-8 when the reference is a
    /// `String` but the actual is a raw `byte[]`.
    private static void compareLeaf(String context, Object expected, Object actual) {
        if (expected instanceof String refStr && actual instanceof byte[] actualBytes) {
            assertThat(new String(actualBytes, StandardCharsets.UTF_8))
                    .as(context).isEqualTo(refStr);
        }
        else if (expected instanceof Float f) {
            assertThat((Float) actual).as(context).isCloseTo(f, within(0.0001f));
        }
        else if (expected instanceof Double d) {
            assertThat((Double) actual).as(context).isCloseTo(d, within(0.0000001d));
        }
        else if (expected instanceof byte[] refBytes) {
            assertThat((byte[]) actual).as(context).isEqualTo(refBytes);
        }
        else {
            assertThat(actual).as(context).isEqualTo(expected);
        }
    }

    /// Get a value from the ColumnReader at a batch index, using the appropriate typed array.
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
                        || colSchema.logicalType() instanceof LogicalType.JsonType) {
                    yield reader.getStrings()[index];
                }
                yield reader.getBinaries()[index];
            }
        };
    }
}
