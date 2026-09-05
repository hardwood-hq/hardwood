/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.avro;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.avro.internal.AvroPlanNode;
import dev.hardwood.avro.internal.AvroSchemaConverter;
import dev.hardwood.internal.reader.FileAwareRowReader;
import dev.hardwood.metadata.LogicalType.IntType;
import dev.hardwood.metadata.LogicalType.ListType;
import dev.hardwood.metadata.LogicalType.MapType;
import dev.hardwood.metadata.LogicalType.NullType;
import dev.hardwood.metadata.LogicalType.StringType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.row.VariantType;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AvroRowReaderTest {

    private static final Path TEST_RESOURCES = Path.of("").toAbsolutePath()
            .resolve("../core/src/test/resources").normalize();

    @Test
    void rejectMapsWhoseKeysCannotBeRepresentedByAvro() throws Exception {
        assertUnsupportedMapKey("map_types_test.parquet", "int_map", "INT32");
        assertUnsupportedMapKey("typed_accessors_issue_445.parquet", "time_keyed", "INT32");
        assertUnsupportedMapKey("map_typed_keys_test.parquet", "long_keyed", "INT64");
    }

    /// The rejection covers the projected schema, so an unreadable map costs only
    /// its own column: the remaining columns of the same file still read.
    @Test
    void readsRemainingColumnsOfAFileCarryingAnUnsupportedMap() throws Exception {
        // map_types_test.parquet: id INT32, string_map, int_map (map<int32, int64>), bool_map — 3 rows
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("map_types_test.parquet")));
                AvroRowReader reader = AvroReaders.buildRowReader(fileReader)
                        .projection(ColumnProjection.columns("id")).build()) {
            List<Object> ids = new ArrayList<>();
            while (reader.hasNext()) {
                ids.add(reader.next().get("id"));
            }
            assertThat(ids).containsExactly(1, 2, 3);
        }
    }

    private static void assertUnsupportedMapKey(String fixture, String column, String keyType)
            throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve(fixture)))) {
            assertThatThrownBy(() -> {
                try (AvroRowReader ignored = AvroReaders.buildRowReader(fileReader)
                        .projection(ColumnProjection.columns(column)).build()) {
                    // Schema conversion happens while the reader is built.
                }
            })
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(column)
                    .hasMessageContaining(keyType);
        }
    }

    @Test
    void wrongRawTypeNamesRootFieldAndAvroType() {
        FileSchema schema = primitiveSchema("value", PhysicalType.INT32, RepetitionType.REQUIRED);
        AvroPlanNode plan = AvroSchemaConverter.plan(schema, ColumnProjection.all());
        RowReader rows = proxy(RowReader.class, values(
                "next", null,
                "isNull", false,
                "getRawValue", "wrong"));

        assertThatThrownBy(() -> new AvroRowReader(rows, plan).next())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("field 'value'")
                .hasMessageContaining("Avro INT")
                .hasMessageContaining("java.lang.String");
    }

    @Test
    void rewrittenFieldUsesParquetAccessor() {
        FileSchema schema = primitiveSchema("a-b", PhysicalType.INT32, RepetitionType.REQUIRED);
        AvroPlanNode plan = AvroSchemaConverter.plan(schema, ColumnProjection.all());
        RowReader rows = (RowReader) Proxy.newProxyInstance(
                RowReader.class.getClassLoader(),
                new Class<?>[] { RowReader.class },
                (proxy, method, arguments) -> {
                    if (method.getName().equals("isNull") || method.getName().equals("getRawValue")) {
                        if (arguments == null || arguments.length != 1 || !"a-b".equals(arguments[0])) {
                            throw new AssertionError("unexpected accessor key: "
                                    + (arguments == null ? null : arguments[0]));
                        }
                        return method.getName().equals("isNull") ? false : 42;
                    }
                    if (method.getName().equals("next")) {
                        return null;
                    }
                    if (method.getName().equals("close")) {
                        return null;
                    }
                    throw new AssertionError("Unexpected accessor: " + method.getName());
                });

        try (AvroRowReader reader = new AvroRowReader(rows, plan)) {
            GenericRecord record = reader.next();
            assertThat(record.getSchema().getField("a_b").getProp(AvroSchemaConverter.PARQUET_NAME_PROP))
                    .isEqualTo("a-b");
            assertThat(record.get("a_b")).isEqualTo(42);
        }
    }

    @Test
    void wrongRawTypeNamesNestedStructField() {
        FileSchema schema = nestedPrimitiveSchema();
        AvroPlanNode plan = AvroSchemaConverter.plan(schema, ColumnProjection.all());
        PqStruct struct = proxy(PqStruct.class, values(
                "isNull", false,
                "getRawValue", "wrong"));
        RowReader rows = proxy(RowReader.class, values(
                "next", null,
                "isNull", false,
                "getStruct", struct));

        assertThatThrownBy(() -> new AvroRowReader(rows, plan).next())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("struct field 'value'")
                .hasMessageContaining("Avro INT")
                .hasMessageContaining("java.lang.String");
    }

    @Test
    void wrongDecodedTypeNamesListElement() {
        FileSchema schema = listStringSchema();
        AvroPlanNode plan = AvroSchemaConverter.plan(schema, ColumnProjection.all());
        PqList list = proxy(PqList.class, values(
                "size", 1,
                "isNull", false,
                "get", 42));
        RowReader rows = proxy(RowReader.class, values(
                "next", null,
                "isNull", false,
                "getList", list));

        assertThatThrownBy(() -> new AvroRowReader(rows, plan).next())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("list element 0")
                .hasMessageContaining("Avro STRING")
                .hasMessageContaining("java.lang.Integer");
    }

    @Test
    void wrongFixedWidthNamesRootField() {
        SchemaElement root = SchemaElement.root("root", 1);
        SchemaElement value = SchemaElement.fixedLengthPrimitive("value", 4, RepetitionType.REQUIRED);
        FileSchema schema = FileSchema.fromSchemaElements(List.of(root, value));
        AvroPlanNode plan = AvroSchemaConverter.plan(schema, ColumnProjection.all());
        RowReader rows = proxy(RowReader.class, values(
                "next", null,
                "isNull", false,
                "getRawValue", new byte[] { 1, 2 }));

        assertThatThrownBy(() -> new AvroRowReader(rows, plan).next())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("field 'value'")
                .hasMessageContaining("Avro FIXED")
                .hasMessageContaining("required fixed width 4")
                .hasMessageContaining("byte[]");
    }

    @Test
    void nullDecodedTypeNamesMapValue() {
        FileSchema schema = mapStringSchema();
        AvroPlanNode plan = AvroSchemaConverter.plan(schema, ColumnProjection.all());
        PqMap.Entry entry = proxy(PqMap.Entry.class, values(
                "getStringKey", "key",
                "isValueNull", false,
                "getStringValue", null));
        PqMap map = proxy(PqMap.class, values("getEntries", List.of(entry), "size", 1));
        RowReader rows = proxy(RowReader.class, values(
                "next", null,
                "isNull", false,
                "getMap", map));

        assertThatThrownBy(() -> new AvroRowReader(rows, plan).next())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("map value for key 'key'")
                .hasMessageContaining("Avro STRING")
                .hasMessageContaining("actual Java value type null");
    }

    @Test
    void nonNullValueForNullLogicalTypeFailsAtRootField() {
        SchemaElement root = SchemaElement.root("root", 1);
        SchemaElement value = SchemaElement.primitive("value", PhysicalType.INT32, RepetitionType.OPTIONAL,
                new NullType());
        FileSchema schema = FileSchema.fromSchemaElements(List.of(root, value));
        AvroPlanNode plan = AvroSchemaConverter.plan(schema, ColumnProjection.all());
        RowReader rows = proxy(RowReader.class, values(
                "next", null,
                "isNull", false,
                "getValue", 42));

        assertThatThrownBy(() -> new AvroRowReader(rows, plan).next())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("field 'value'")
                .hasMessageContaining("Avro NULL")
                .hasMessageContaining("java.lang.Integer")
                .hasMessageContaining("NULL has no non-null materialization");
    }

    @Test
    void nonNullValueForNullLogicalTypeFailsAtListElement() {
        AvroPlanNode plan = AvroSchemaConverter.plan(listOfNullSchema(), ColumnProjection.all());
        PqList list = proxy(PqList.class, values(
                "size", 1,
                "isNull", false,
                "get", 42));
        RowReader rows = proxy(RowReader.class, values(
                "next", null,
                "isNull", false,
                "getList", list));

        assertThatThrownBy(() -> new AvroRowReader(rows, plan).next())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("list element 0")
                .hasMessageContaining("Avro NULL")
                .hasMessageContaining("java.lang.Integer")
                .hasMessageContaining("NULL has no non-null materialization");
    }

    @Test
    void nonNullValueForNullLogicalTypeFailsAtMapValue() {
        AvroPlanNode plan = AvroSchemaConverter.plan(mapOfNullSchema(), ColumnProjection.all());
        PqMap.Entry entry = proxy(PqMap.Entry.class, values(
                "getStringKey", "key",
                "isValueNull", false,
                "getValue", 42));
        PqMap map = proxy(PqMap.class, values("getEntries", List.of(entry), "size", 1));
        RowReader rows = proxy(RowReader.class, values(
                "next", null,
                "isNull", false,
                "getMap", map));

        assertThatThrownBy(() -> new AvroRowReader(rows, plan).next())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("map value for key 'key'")
                .hasMessageContaining("Avro NULL")
                .hasMessageContaining("java.lang.Integer")
                .hasMessageContaining("NULL has no non-null materialization");
    }

    /// A materialization failure is attributable to a file, the way the core readers
    /// already mark their own exceptions — otherwise a multi-file read reports a row
    /// position with no file to pin it to.
    @Test
    void materializationFailureNamesTheFileOfTheCurrentRow() {
        FileSchema schema = primitiveSchema("value", PhysicalType.INT32, RepetitionType.REQUIRED);
        AvroPlanNode plan = AvroSchemaConverter.plan(schema, ColumnProjection.all());
        RowReader rows = proxy(FileAwareRowReader.class, values(
                "next", null,
                "isNull", false,
                "currentFileName", "part-01.parquet",
                "getRawValue", "wrong"));

        assertThatThrownBy(() -> new AvroRowReader(rows, plan).next())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("[part-01.parquet] ")
                .hasMessageContaining("field 'value'");
    }

    /// The shape issue #897 reported: `UINT32` stores as `INT32` but converts to Avro
    /// `long`, so an accessor picked off the Avro type serves a `Long` the widening
    /// step cannot use.
    @Test
    void wrongRawTypeForUnsignedIntNamesRootField() {
        SchemaElement root = SchemaElement.root("root", 1);
        SchemaElement count = SchemaElement.primitive("count", PhysicalType.INT32, RepetitionType.REQUIRED,
                new IntType(32, false));
        FileSchema schema = FileSchema.fromSchemaElements(List.of(root, count));
        AvroPlanNode plan = AvroSchemaConverter.plan(schema, ColumnProjection.all());
        RowReader rows = proxy(RowReader.class, values(
                "next", null,
                "isNull", false,
                "getRawValue", 7L));

        assertThatThrownBy(() -> new AvroRowReader(rows, plan).next())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("field 'count'")
                // Naming the kind keeps "Avro LONG … required java.lang.Integer" from
                // reading as a contradiction.
                .hasMessageContaining("Avro LONG (UNSIGNED_INT32)")
                .hasMessageContaining("java.lang.Long")
                .hasMessageContaining("required java.lang.Integer");
    }

    @Test
    void readFlatSchema() throws Exception {
        // plain_uncompressed.parquet: id INT64, value INT64 — 3 rows
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("plain_uncompressed.parquet")));
             AvroRowReader reader = AvroReaders.rowReader(fileReader)) {

            Schema schema = reader.getSchema();
            assertThat(schema.getType()).isEqualTo(Schema.Type.RECORD);
            assertThat(schema.getFields()).hasSize(2);

            List<GenericRecord> records = readAll(reader);
            assertThat(records).hasSize(3);

            assertThat(records.get(0).get("id")).isEqualTo(1L);
            assertThat(records.get(0).get("value")).isEqualTo(100L);
            assertThat(records.get(1).get("id")).isEqualTo(2L);
            assertThat(records.get(2).get("id")).isEqualTo(3L);
        }
    }

    @Test
    void readNullableFields() throws Exception {
        // plain_uncompressed_with_nulls.parquet: id INT64, name STRING (optional)
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("plain_uncompressed_with_nulls.parquet")));
             AvroRowReader reader = AvroReaders.rowReader(fileReader)) {

            List<GenericRecord> records = readAll(reader);
            assertThat(records).hasSize(3);

            // Verify nullable field schema is union [null, string]
            Schema nameSchema = reader.getSchema().getField("name").schema();
            assertThat(nameSchema.getType()).isEqualTo(Schema.Type.UNION);

            // Check that we can read without errors and nulls are handled
            for (GenericRecord record : records) {
                assertThat(record.get("id")).isNotNull();
                // name may or may not be null
            }
        }
    }

    @Test
    void readNestedStruct() throws Exception {
        // nested_struct_test.parquet: id INT32, address { street STRING, city STRING, zip INT32 }
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("nested_struct_test.parquet")));
             AvroRowReader reader = AvroReaders.rowReader(fileReader)) {

            List<GenericRecord> records = readAll(reader);
            assertThat(records).isNotEmpty();

            GenericRecord first = records.getFirst();
            assertThat(first.get("id")).isEqualTo(1);

            Object addressObj = first.get("address");
            assertThat(addressObj).isInstanceOf(GenericRecord.class);

            GenericRecord address = (GenericRecord) addressObj;
            assertThat(address.get("street").toString()).isEqualTo("123 Main St");
            assertThat(address.get("city").toString()).isEqualTo("New York");
            assertThat(address.get("zip")).isEqualTo(10001);
        }
    }

    /// The names the converter rewrites have to still address values on a real file: the
    /// two `address` records are told apart by namespace only, and both `acme.address` and
    /// `a-b` reach their columns through their Parquet names while the record exposes them
    /// under the rewritten Avro names.
    @Test
    void readsRecordsWhoseNamesTheConverterRewrote() throws Exception {
        // avro_name_resolution.parquet: home.address.city, work.address.zip,
        // 'acme.address'.city, 'a-b' — 2 rows
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("avro_name_resolution.parquet")));
             AvroRowReader reader = AvroReaders.rowReader(fileReader)) {

            Schema schema = reader.getSchema();
            assertThat(schema.toString()).contains("\"name\":\"address\",\"namespace\":\"schema.home\"",
                    "\"name\":\"address\",\"namespace\":\"schema.work\"");
            assertThat(schema.getField("acme_address").getProp(AvroSchemaConverter.PARQUET_NAME_PROP))
                    .isEqualTo("acme.address");
            assertThat(schema.getField("a_b").getProp(AvroSchemaConverter.PARQUET_NAME_PROP))
                    .isEqualTo("a-b");

            List<GenericRecord> records = readAll(reader);
            assertThat(records).hasSize(2);

            GenericRecord first = records.getFirst();
            assertThat(nested(nested(first, "home"), "address").get("city").toString()).isEqualTo("Timbuktu");
            assertThat(nested(nested(first, "work"), "address").get("zip")).isEqualTo(73);
            assertThat(nested(first, "acme_address").get("city").toString()).isEqualTo("Valparaiso");
            assertThat(first.get("a_b")).isEqualTo(42);

            GenericRecord second = records.get(1);
            assertThat(nested(nested(second, "home"), "address").get("city").toString()).isEqualTo("Reykjavik");
            assertThat(second.get("acme_address")).isNull();
            assertThat(second.get("a_b")).isEqualTo(7);
        }
    }

    /// A record read through a nullable field, which the converter wraps in a union.
    private static GenericRecord nested(GenericRecord record, String field) {
        return (GenericRecord) record.get(field);
    }

    @Test
    void readList() throws Exception {
        // list_basic_test.parquet: id INT32, tags list<string>, scores list<int>
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("list_basic_test.parquet")));
             AvroRowReader reader = AvroReaders.rowReader(fileReader)) {

            List<GenericRecord> records = readAll(reader);
            assertThat(records).isNotEmpty();

            GenericRecord first = records.getFirst();
            assertThat(first.get("id")).isEqualTo(1);

            Object tags = first.get("tags");
            assertThat(tags).isInstanceOf(List.class);

            @SuppressWarnings("unchecked")
            List<Object> tagList = (List<Object>) tags;
            assertThat(tagList).isNotEmpty();
        }
    }

    @Test
    void readMap() throws Exception {
        // simple_map_test.parquet: id INT32, attributes map<string, string>
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("simple_map_test.parquet")));
             AvroRowReader reader = AvroReaders.rowReader(fileReader)) {

            List<GenericRecord> records = readAll(reader);
            assertThat(records).isNotEmpty();

            GenericRecord first = records.getFirst();
            Object attrs = first.get("attributes");
            assertThat(attrs).isInstanceOf(Map.class);

            @SuppressWarnings("unchecked")
            Map<String, Object> attrMap = (Map<String, Object>) attrs;
            assertThat(attrMap).isNotEmpty();
        }
    }

    @Test
    void readKeyOnlyMapAsNullValuedMap() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("map_key_only_test.parquet")));
             AvroRowReader reader = AvroReaders.rowReader(fileReader)) {

            Schema mapSchema = resolveNullable(reader.getSchema().getField("tags").schema());
            assertThat(mapSchema.getType()).isEqualTo(Schema.Type.MAP);
            assertThat(mapSchema.getValueType().getType()).isEqualTo(Schema.Type.NULL);

            List<GenericRecord> records = readAll(reader);
            assertThat(records).hasSize(2);
            @SuppressWarnings("unchecked")
            Map<String, Object> firstTags = (Map<String, Object>) records.get(0).get("tags");
            @SuppressWarnings("unchecked")
            Map<String, Object> secondTags = (Map<String, Object>) records.get(1).get("tags");
            assertThat(firstTags)
                    .containsEntry("a", null)
                    .containsEntry("b", null);
            assertThat(secondTags).containsEntry("c", null);
        }
    }

    @Test
    void readWithFilter() throws Exception {
        // filter_pushdown_int.parquet: 3 row groups, id 1-100, 101-200, 201-300
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("filter_pushdown_int.parquet")));
             AvroRowReader reader = AvroReaders.buildRowReader(fileReader)
                     .filter(FilterPredicate.gt("id", 200L)).build()) {

            List<GenericRecord> records = readAll(reader);
            assertThat(records).hasSize(100);

            for (GenericRecord record : records) {
                assertThat((Long) record.get("id")).isGreaterThan(200L);
            }
        }
    }

    @Test
    void schemaConversion() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("nested_struct_test.parquet")))) {

            Schema schema = AvroSchemaConverter.plan(
                    fileReader.getFileSchema(), ColumnProjection.all()).avro();
            assertThat(schema.getType()).isEqualTo(Schema.Type.RECORD);

            // id field — INT32 → Avro INT
            Schema.Field idField = schema.getField("id");
            assertThat(idField).isNotNull();

            // address field — struct → Avro RECORD (nullable)
            Schema.Field addressField = schema.getField("address");
            assertThat(addressField).isNotNull();
        }
    }

    @Test
    void readShreddedVariantColumn() throws Exception {
        // variant_shredded_test.parquet (generated by tools/simple-datagen.py) has a
        // VARIANT-annotated group with {metadata, value, typed_value:int64} and
        // four rows exercising the distinct reassembly outcomes. The Avro view
        // follows parquet-java's AvroParquetReader shape: a two-field
        // RECORD{metadata: bytes, value: bytes} carrying the canonical Variant
        // bytes. Typed access to the payload is available via the file
        // reader's PqVariant API; this test exercises the raw Avro surface.
        Path fixture = Path.of("").toAbsolutePath()
                .resolve("../core/src/test/resources/variant_shredded_test.parquet").normalize();

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(fixture));
             AvroRowReader reader = AvroReaders.rowReader(fileReader)) {

            Schema schema = reader.getSchema();
            Schema.Field varField = schema.getField("var");
            Schema varRecord = varField.schema().getTypes().stream()
                    .filter(s -> s.getType() == Schema.Type.RECORD)
                    .findFirst()
                    .orElseThrow();
            assertThat(varRecord.getFields()).hasSize(2);
            assertThat(varRecord.getField("metadata").schema().getType()).isEqualTo(Schema.Type.BYTES);
            assertThat(varRecord.getField("value").schema().getType()).isEqualTo(Schema.Type.BYTES);

            List<GenericRecord> rows = readAll(reader);
            assertThat(rows).hasSize(4);

            // Row 1: shredded INT64(42) → canonical value = [0x18, 42, 0, 0, 0, 0, 0, 0, 0].
            GenericRecord row1Var = (GenericRecord) rows.get(0).get("var");
            assertThat(bytes(row1Var.get("metadata"))).containsExactly(0x01, 0x00, 0x00);
            assertThat(bytes(row1Var.get("value")))
                    .containsExactly(0x18, 42, 0, 0, 0, 0, 0, 0, 0);

            // Row 2: unshredded — value passthrough (BOOLEAN_TRUE = 0x04).
            GenericRecord row2Var = (GenericRecord) rows.get(1).get("var");
            assertThat(bytes(row2Var.get("value"))).containsExactly(0x04);

            // Row 3: both null at non-null group → Variant NULL (single 0x00 byte).
            GenericRecord row3Var = (GenericRecord) rows.get(2).get("var");
            assertThat(bytes(row3Var.get("value"))).containsExactly(0x00);

            // Row 4: shredded INT64(10^12) → canonical value = [0x18] + 8 LE bytes.
            GenericRecord row4Var = (GenericRecord) rows.get(3).get("var");
            byte[] row4Value = bytes(row4Var.get("value"));
            assertThat(row4Value[0]).isEqualTo((byte) 0x18);
            long decoded = 0L;
            for (int i = 0; i < 8; i++) {
                decoded |= ((long) (row4Value[1 + i] & 0xFF)) << (8 * i);
            }
            assertThat(decoded).isEqualTo(1_000_000_000_000L);
        }
    }

    @Test
    void readVariantInListAndMapPositions() throws Exception {
        // variant_in_repeated_test.parquet carries the same VARIANT group at the
        // top level, as a map value, and as a list element. All three convert to
        // the RECORD{metadata, value} shape, so all three must materialize it —
        // a list element surfaces as PqVariant and a map value has to be read
        // through getVariantValue rather than getStructValue.
        Path fixture = TEST_RESOURCES.resolve("variant_in_repeated_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(fixture));
             AvroRowReader reader = AvroReaders.rowReader(fileReader)) {

            Schema schema = reader.getSchema();
            List<GenericRecord> rows = readAll(reader);
            assertThat(rows).hasSize(2);

            // Row 1 payloads: BOOLEAN_TRUE (0x04), INT32(7), short string "hi".
            Map<?, ?> varMap = (Map<?, ?>) rows.get(0).get("var_map");
            assertThat(varMap).hasSize(2);
            assertThat(bytes(((GenericRecord) varMap.get("a")).get("value"))).containsExactly(0x04);
            assertThat(bytes(((GenericRecord) varMap.get("b")).get("value")))
                    .containsExactly(0x14, 7, 0, 0, 0);

            List<?> varList = (List<?>) rows.get(0).get("var_list");
            assertThat(varList).hasSize(3);
            assertThat(bytes(((GenericRecord) varList.get(0)).get("value"))).containsExactly(0x04);
            assertThat(bytes(((GenericRecord) varList.get(2)).get("value")))
                    .containsExactly(0x09, 'h', 'i');

            assertThatCode(() -> serialize(schema, rows)).doesNotThrowAnyException();
        }
    }

    @Test
    void ordinaryVariantShapedRecordsKeepStructIdentity() throws Exception {
        Path fixture = TEST_RESOURCES.resolve("avro_variant_identity_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(fixture));
             AvroRowReader reader = AvroReaders.rowReader(fileReader)) {

            List<GenericRecord> rows = readAll(reader);
            assertThat(rows).hasSize(1);
            GenericRecord row = rows.get(0);

            GenericRecord plainTop = (GenericRecord) row.get("plain_top");
            assertThat(bytes(plainTop.get("metadata"))).containsExactly("plain-top-metadata".getBytes(StandardCharsets.UTF_8));
            assertThat(bytes(plainTop.get("value"))).containsExactly("plain-top-value".getBytes(StandardCharsets.UTF_8));

            GenericRecord plainContainer = (GenericRecord) row.get("plain_container");
            GenericRecord plainNested = (GenericRecord) plainContainer.get("plain_nested");
            assertThat(bytes(plainNested.get("metadata"))).containsExactly("plain-nested-metadata".getBytes(StandardCharsets.UTF_8));
            assertThat(bytes(plainNested.get("value"))).containsExactly("plain-nested-value".getBytes(StandardCharsets.UTF_8));

            GenericRecord plainListElement = (GenericRecord) ((List<?>) row.get("plain_list")).get(0);
            assertThat(bytes(plainListElement.get("metadata"))).containsExactly("plain-list-metadata".getBytes(StandardCharsets.UTF_8));
            assertThat(bytes(plainListElement.get("value"))).containsExactly("plain-list-value".getBytes(StandardCharsets.UTF_8));

            GenericRecord plainMapValue = (GenericRecord) ((Map<?, ?>) row.get("plain_map")).get("plain-key");
            assertThat(bytes(plainMapValue.get("metadata"))).containsExactly("plain-map-metadata".getBytes(StandardCharsets.UTF_8));
            assertThat(bytes(plainMapValue.get("value"))).containsExactly("plain-map-value".getBytes(StandardCharsets.UTF_8));

            GenericRecord realVariant = (GenericRecord) row.get("real_variant");
            assertThat(bytes(realVariant.get("metadata"))).containsExactly(0x01, 0x00, 0x00);
            assertThat(bytes(realVariant.get("value"))).containsExactly(0x04);

            GenericRecord variantContainer = (GenericRecord) row.get("variant_container");
            GenericRecord realNestedVariant = (GenericRecord) variantContainer.get("real_nested_variant");
            assertThat(bytes(realNestedVariant.get("metadata"))).containsExactly(0x01, 0x00, 0x00);
            assertThat(bytes(realNestedVariant.get("value"))).containsExactly(0x14, 42, 0, 0, 0);

            assertThatCode(() -> serialize(reader.getSchema(), rows)).doesNotThrowAnyException();
        }
    }

    @Test
    void shreddedVariantExposesTypedAccessViaPqVariant() throws Exception {
        // Companion to readShreddedVariantColumn, asserting the PqVariant API
        // surface directly rather than the Avro record form. Same fixture, same
        // four rows; this pins that reassembly produces the expected VariantType
        // for each row, including the SQL-null-vs-Variant-NULL distinction on
        // row 3 (both value and typed_value absent → Variant NULL, not SQL null).
        Path fixture = Path.of("").toAbsolutePath()
                .resolve("../core/src/test/resources/variant_shredded_test.parquet").normalize();

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(fixture));
                RowReader rowReader = fileReader.rowReader()) {

            // Row 1: shredded INT64(42).
            rowReader.next();
            PqVariant v1 = rowReader.getVariant("var");
            assertThat(v1).isNotNull();
            assertThat(v1.type()).isEqualTo(VariantType.INT64);
            assertThat(v1.asLong()).isEqualTo(42L);

            // Row 2: unshredded BOOLEAN_TRUE.
            rowReader.next();
            PqVariant v2 = rowReader.getVariant("var");
            assertThat(v2.type()).isEqualTo(VariantType.BOOLEAN_TRUE);
            assertThat(v2.asBoolean()).isTrue();

            // Row 3: Variant NULL (not SQL null — the group is present, both
            // value and typed_value absent → canonical single 0x00 byte).
            rowReader.next();
            PqVariant v3 = rowReader.getVariant("var");
            assertThat(v3).as("row 3 group is non-null").isNotNull();
            assertThat(v3.type()).isEqualTo(VariantType.NULL);
            assertThat(v3.isNull()).isTrue();
            assertThat(v3.value()).containsExactly(0x00);

            // Row 4: shredded INT64(10^12).
            rowReader.next();
            PqVariant v4 = rowReader.getVariant("var");
            assertThat(v4.type()).isEqualTo(VariantType.INT64);
            assertThat(v4.asLong()).isEqualTo(1_000_000_000_000L);

            assertThat(rowReader.hasNext()).isFalse();
        }
    }

    private static byte[] bytes(Object avroBinary) {
        if (avroBinary instanceof ByteBuffer bb) {
            byte[] out = new byte[bb.remaining()];
            bb.duplicate().get(out);
            return out;
        }
        if (avroBinary instanceof byte[] b) {
            return b;
        }
        throw new IllegalArgumentException("Unexpected Avro binary value type: "
                + (avroBinary == null ? "null" : avroBinary.getClass()));
    }

    @Test
    void readUnsignedIntColumns() throws Exception {
        // unsigned_int_test.parquet: id UINT32, uint32_val UINT32, uint64_val UINT64
        // UINT32 maps to Avro LONG (signed Avro INT can't carry 4294967295). The
        // materializer must dispatch by the source column's physical type (INT32),
        // read the int, and widen with an unsigned mask — not call getLong against
        // an int[]-backed batch.
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("unsigned_int_test.parquet")));
             AvroRowReader reader = AvroReaders.rowReader(fileReader)) {

            Schema schema = reader.getSchema();
            Schema idSchema = schema.getField("id").schema();
            Schema uint32Schema = schema.getField("uint32_val").schema();
            Schema uint64Schema = schema.getField("uint64_val").schema();
            assertThat(idSchema.getType()).isEqualTo(Schema.Type.LONG);
            assertThat(uint32Schema.getType()).isEqualTo(Schema.Type.LONG);
            assertThat(uint64Schema.getType()).isEqualTo(Schema.Type.LONG);

            // The converted schema records the unsignedness Avro cannot express, so a
            // consumer holding only the schema can still tell a widened UINT_32 from a
            // UINT_64, which is long[]-backed and carries no marker.
            assertThat(idSchema.getObjectProp(AvroSchemaConverter.UNSIGNED_INT32_PROP))
                    .isEqualTo(Boolean.TRUE);
            assertThat(uint32Schema.getObjectProp(AvroSchemaConverter.UNSIGNED_INT32_PROP))
                    .isEqualTo(Boolean.TRUE);
            assertThat(uint64Schema.getObjectProp(AvroSchemaConverter.UNSIGNED_INT32_PROP))
                    .isNull();

            List<GenericRecord> records = readAll(reader);
            assertThat(records).hasSize(3);

            assertThat(records.getFirst().get("id")).isEqualTo(1L);
            assertThat(records.getFirst().get("uint32_val")).isEqualTo(0L);
            assertThat(records.getFirst().get("uint64_val")).isEqualTo(0L);

            assertThat(records.get(1).get("id")).isEqualTo(2L);
            assertThat(records.get(1).get("uint32_val")).isEqualTo(2147483647L);
            assertThat(records.get(1).get("uint64_val")).isEqualTo(9223372036854775807L);

            // Row 3 covers the values where signed-vs-unsigned matters:
            //   uint32_val = 4294967295 (0xFFFFFFFF) must NOT surface as -1L,
            //   uint64_val = 18446744073709551615 (0xFFFFFFFFFFFFFFFF) wraps to -1L
            //   in Avro since Avro has no native unsigned long type.
            assertThat(records.get(2).get("id")).isEqualTo(3L);
            assertThat(records.get(2).get("uint32_val")).isEqualTo(4294967295L);
            assertThat(records.get(2).get("uint64_val")).isEqualTo(-1L);
        }
    }

    @Test
    void fixedBackedColumnsMaterializeAsAvroFixed() throws Exception {
        // INT96, INTERVAL under either annotation, and FLOAT16 all convert to Avro `fixed`
        // and read as their on-disk bytes — 12, 12 and 2 wide. They share one plan kind with
        // FIXED_LEN_BYTE_ARRAY decimals, and nothing pinned them through the reader.
        assertFixedColumn("int96_timestamp_test.parquet", "ts", 12);
        assertFixedColumn("interval_logical_type_test.parquet", "duration", 12);
        assertFixedColumn("interval_legacy_converted_type_test.parquet", "duration", 12);
        assertFixedColumn("float16_logical_type_test.parquet", "half", 2);
    }

    @Test
    void fixedBackedListElementsMaterializeAsAvroFixed() throws Exception {
        // list_float16_test.parquet: scores is a list<float16>, so the element takes the
        // same `fixed` representation the scalar positions do.
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("list_float16_test.parquet")));
             AvroRowReader reader = AvroReaders.rowReader(fileReader)) {

            List<GenericRecord> rows = readAll(reader);
            @SuppressWarnings("unchecked")
            List<Object> scores = (List<Object>) rows.getFirst().get("scores");
            assertThat(scores).isNotEmpty();
            assertThat(scores.getFirst()).isInstanceOf(GenericData.Fixed.class);
            assertThat(((GenericData.Fixed) scores.getFirst()).bytes()).hasSize(2);

            assertThatCode(() -> serialize(reader.getSchema(), rows)).doesNotThrowAnyException();
        }
    }

    /// Assert that `field` of `fixture` converts to an Avro `fixed` of `size` bytes and
    /// materializes as a [GenericData.Fixed] of that width.
    private static void assertFixedColumn(String fixture, String field, int size) throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve(fixture)));
             AvroRowReader reader = AvroReaders.rowReader(fileReader)) {

            Schema fieldSchema = reader.getSchema().getField(field).schema();
            if (fieldSchema.isUnion()) {
                fieldSchema = fieldSchema.getTypes().stream()
                        .filter(s -> s.getType() != Schema.Type.NULL)
                        .findFirst()
                        .orElseThrow();
            }
            assertThat(fieldSchema.getType()).isEqualTo(Schema.Type.FIXED);
            assertThat(fieldSchema.getFixedSize()).isEqualTo(size);

            List<GenericRecord> rows = readAll(reader);
            assertThat(rows).isNotEmpty();
            assertThat(rows.getFirst().get(field)).isInstanceOf(GenericData.Fixed.class);
            assertThat(((GenericData.Fixed) rows.getFirst().get(field)).bytes()).hasSize(size);

            assertThatCode(() -> serialize(reader.getSchema(), rows)).doesNotThrowAnyException();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void temporalValuesReadTheSameWayAtEveryPosition() throws Exception {
        // local_timestamp_test.parquet carries the same TIMESTAMP(MICROS, NTZ) column as a
        // field, a list element and a map value. Avro types a timestamp as a `long` under a
        // logical type, so every position must yield the epoch micros — a decoded
        // LocalDateTime would contradict the very schema the record is built from and fail
        // to serialize.
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("local_timestamp_test.parquet")));
             AvroRowReader reader = AvroReaders.rowReader(fileReader)) {

            GenericRecord row = readAll(reader).getFirst();

            assertThat(row.get("local_micros")).isEqualTo(1772703000123456L);
            assertThat((List<Object>) row.get("local_ts_list"))
                    .containsExactly(1772703000000000L, 1772704800000000L);
            Map<Object, Object> tsMap = (Map<Object, Object>) row.get("local_ts_map");
            assertThat(tsMap)
                    .containsEntry("start", 1772703000000000L)
                    .containsEntry("end", 1772704800000000L);

            assertThatCode(() -> serialize(reader.getSchema(), List.of(row))).doesNotThrowAnyException();
        }
    }

    @Test
    void readNestedUnsignedIntColumns() throws Exception {
        // unsigned_int_nested_test.parquet: id INT32, point STRUCT{x UINT32, y UINT32},
        // flags LIST<UINT32>, counters MAP<STRING, UINT32>. Exercises the
        // struct, list, and map dispatch paths — all share the same
        // Avro-type-based switch that mishandles UINT32 → LONG when the
        // physical column is INT32.
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("unsigned_int_nested_test.parquet")));
             AvroRowReader reader = AvroReaders.rowReader(fileReader)) {

            List<GenericRecord> records = readAll(reader);
            assertThat(records).hasSize(2);

            GenericRecord row1 = records.get(0);
            GenericRecord point1 = (GenericRecord) row1.get("point");
            assertThat(point1.get("x")).isEqualTo(0L);
            assertThat(point1.get("y")).isEqualTo(2147483647L);

            @SuppressWarnings("unchecked")
            List<Object> flags1 = (List<Object>) row1.get("flags");
            assertThat(flags1).containsExactly(0L, 2147483647L, 4294967295L);

            @SuppressWarnings("unchecked")
            Map<String, Object> counters1 = (Map<String, Object>) row1.get("counters");
            assertThat(counters1).containsEntry("zero", 0L)
                    .containsEntry("mid", 2147483647L)
                    .containsEntry("max", 4294967295L);

            GenericRecord row2 = records.get(1);
            GenericRecord point2 = (GenericRecord) row2.get("point");
            assertThat(point2.get("x")).isEqualTo(4294967295L);
            assertThat(point2.get("y")).isEqualTo(1L);
        }
    }

    @Test
    void readDecimalBackedByInt64Column() throws Exception {
        // legacy_converted_types_test.parquet: decimal_col is a DECIMAL(18,2)
        // whose physical storage is INT64 ([12345, -100] => 123.45, -1.00).
        // DECIMAL maps to Avro BYTES, but the materializer must dispatch on the
        // source column's physical type — read the decimal and emit the unscaled
        // two's-complement bytes, not call getBinary against a long[]-backed batch.
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("legacy_converted_types_test.parquet")));
             AvroRowReader reader = AvroReaders.rowReader(fileReader)) {

            Schema decimalSchema = reader.getSchema().getField("decimal_col").schema()
                    .getTypes().stream()
                    .filter(s -> s.getType() != Schema.Type.NULL)
                    .findFirst().orElseThrow();
            assertThat(decimalSchema.getType()).isEqualTo(Schema.Type.BYTES);
            assertThat(decimalSchema.getLogicalType()).isInstanceOf(LogicalTypes.Decimal.class);

            List<GenericRecord> records = readAll(reader);
            assertThat(records).hasSize(2);
            assertThat(decimalAt(records.get(0), "decimal_col", 2)).isEqualByComparingTo("123.45");
            assertThat(decimalAt(records.get(1), "decimal_col", 2)).isEqualByComparingTo("-1.00");
        }
    }

    @Test
    void readUuidColumn() throws Exception {
        // logical_types_test.parquet: account_id is a UUID (FIXED_LEN_BYTE_ARRAY(16)).
        // UUID maps to an Avro STRING carrying the uuid logical type, so the
        // materializer must surface the canonical UUID string — not decode the 16
        // raw bytes as a UTF-8 string via getString.
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("logical_types_test.parquet")));
             AvroRowReader reader = AvroReaders.rowReader(fileReader)) {

            Schema accountId = reader.getSchema().getField("account_id").schema();
            assertThat(accountId.getType()).isEqualTo(Schema.Type.STRING);
            assertThat(accountId.getLogicalType()).isNotNull();
            assertThat(accountId.getLogicalType().getName()).isEqualTo("uuid");

            List<GenericRecord> records = readAll(reader);
            assertThat(records).hasSize(3);
            assertThat(records.get(0).get("account_id").toString())
                    .isEqualTo("12345678-1234-5678-1234-567812345678");
            assertThat(records.get(1).get("account_id").toString())
                    .isEqualTo("87654321-4321-8765-4321-876543218765");
            assertThat(records.get(2).get("account_id").toString())
                    .isEqualTo("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
        }
    }

    @Test
    void readUuidListElements() throws Exception {
        // uuid_list_test.parquet: session_ids is a list<uuid>. The list element
        // path shares the Avro STRING arm with UTF-8 columns, so the uuid logical
        // type has to keep it off the plain string decode — otherwise the 16 raw
        // bytes surface as mojibake instead of the canonical UUID string.
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("uuid_list_test.parquet")));
             AvroRowReader reader = AvroReaders.rowReader(fileReader)) {

            Schema element = resolveNullable(
                    resolveNullable(reader.getSchema().getField("session_ids").schema()).getElementType());
            assertThat(element.getType()).isEqualTo(Schema.Type.STRING);
            assertThat(element.getLogicalType().getName()).isEqualTo("uuid");

            List<GenericRecord> records = readAll(reader);
            assertThat(records).hasSize(3);
            List<?> firstIds = (List<?>) records.get(0).get("session_ids");
            assertThat(firstIds).hasSize(2);
            assertThat(firstIds.get(0)).isEqualTo("12345678-1234-5678-1234-567812345678");
            assertThat(firstIds.get(1)).isEqualTo("87654321-4321-8765-4321-876543218765");
            List<?> secondIds = (List<?>) records.get(1).get("session_ids");
            assertThat(secondIds).hasSize(1);
            assertThat(secondIds.get(0)).isNull();
            assertThat((List<?>) records.get(2).get("session_ids")).isEmpty();
        }
    }

    private static BigDecimal decimalAt(GenericRecord record, String field, int scale) {
        return new BigDecimal(new BigInteger(bytes(record.get(field))), scale);
    }

    @Test
    void readNullLogicalTypeColumn() throws Exception {
        // null_logical_type_test.parquet: id INT32, nothing INT32 annotated NULL
        // — every row's `nothing` is null. Avro schema field must be bare NULL.
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("null_logical_type_test.parquet")));
             AvroRowReader reader = AvroReaders.rowReader(fileReader)) {

            Schema.Field nothing = reader.getSchema().getField("nothing");
            assertThat(nothing.schema().getType()).isEqualTo(Schema.Type.NULL);
            assertThat(nothing.schema().isUnion()).isFalse();

            List<GenericRecord> records = readAll(reader);
            assertThat(records).hasSize(3);
            for (GenericRecord record : records) {
                assertThat(record.get("nothing")).isNull();
            }
        }
    }

    @Test
    void readSubsetProjectionTopLevel() throws Exception {
        // plain_uncompressed.parquet: id INT64, value INT64 — 3 rows.
        // Project only `value`: the materialized schema and records must contain
        // `value` and not `id`. Regression for hardwood-hq/hardwood#692.
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("plain_uncompressed.parquet")));
             AvroRowReader reader = AvroReaders.buildRowReader(fileReader)
                     .projection(ColumnProjection.columns("value")).build()) {

            Schema schema = reader.getSchema();
            assertThat(schema.getFields()).hasSize(1);
            assertThat(schema.getField("value")).isNotNull();
            assertThat(schema.getField("id")).isNull();

            List<GenericRecord> records = readAll(reader);
            assertThat(records).hasSize(3);
            assertThat(records.getFirst().get("value")).isEqualTo(100L);
        }
    }

    @Test
    void decimalOnFixedUsesByteLengthAsFixedSize() throws Exception {
        // compat_decimal_10_2.parquet: id INT64, amount decimal(10,2) stored as
        // FIXED_LEN_BYTE_ARRAY of length 5 at leaf index 1. Building the reader
        // converts the schema; the decimal's Avro `fixed` size must be the column's
        // byte length (5), not its leaf index (1) — the latter cannot hold 10 digits.
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("compat_decimal_10_2.parquet")));
             AvroRowReader reader = AvroReaders.rowReader(fileReader)) {

            Schema amount = reader.getSchema().getField("amount").schema();
            assertThat(amount.getType()).isEqualTo(Schema.Type.FIXED);
            assertThat(amount.getFixedSize()).isEqualTo(5);

            LogicalType logicalType = amount.getLogicalType();
            assertThat(logicalType).isInstanceOf(LogicalTypes.Decimal.class);
            LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
            assertThat(decimal.getPrecision()).isEqualTo(10);
            assertThat(decimal.getScale()).isEqualTo(2);
        }
    }

    @Test
    void fixedColumnMaterializesAsGenericFixedAndSerializes() throws Exception {
        // A `fixed`-typed Avro field requires a GenericFixed value, not a bare
        // ByteBuffer. GenericRecord.put accepts a ByteBuffer silently, so the defect
        // only surfaces on serialization — assert the materialized value is a
        // GenericData.Fixed of the declared size and that the records round-trip
        // through a GenericDatumWriter.
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("compat_decimal_10_2.parquet")));
             AvroRowReader reader = AvroReaders.rowReader(fileReader)) {

            Schema schema = reader.getSchema();
            List<GenericRecord> records = readAll(reader);
            assertThat(records).isNotEmpty();

            for (GenericRecord record : records) {
                Object amount = record.get("amount");
                if (amount != null) {
                    assertThat(amount).isInstanceOf(GenericData.Fixed.class);
                    assertThat(((GenericData.Fixed) amount).bytes()).hasSize(5);
                }
            }

            assertThatCode(() -> serialize(schema, records)).doesNotThrowAnyException();
        }
    }

    @Test
    void readNestedFieldProjection() throws Exception {
        // nested_struct_test.parquet: id INT32, address { street, city, zip }.
        // Project a single nested field `address.city`: the materialized schema
        // must expose only `address` carrying only `city`, and reading must not
        // touch the unprojected siblings.
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("nested_struct_test.parquet")));
             AvroRowReader reader = AvroReaders.buildRowReader(fileReader)
                     .projection(ColumnProjection.columns("address.city")).build()) {

            Schema schema = reader.getSchema();
            assertThat(schema.getFields()).hasSize(1);
            Schema.Field addressField = schema.getField("address");
            assertThat(addressField).isNotNull();
            Schema addressRecord = addressField.schema().getTypes().stream()
                    .filter(s -> s.getType() == Schema.Type.RECORD)
                    .findFirst()
                    .orElseThrow();
            assertThat(addressRecord.getFields()).hasSize(1);
            assertThat(addressRecord.getField("city")).isNotNull();

            List<GenericRecord> records = readAll(reader);
            assertThat(records).isNotEmpty();
            GenericRecord address = (GenericRecord) records.get(0).get("address");
            assertThat(address.get("city").toString()).isEqualTo("New York");
        }
    }

    @Test
    void readHead() throws Exception {
        // plain_uncompressed.parquet: id INT64, value INT64 — 3 rows.
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("plain_uncompressed.parquet")));
             AvroRowReader reader = AvroReaders.buildRowReader(fileReader).head(2).build()) {

            List<GenericRecord> records = readAll(reader);
            assertThat(records).hasSize(2);
            assertThat(records.get(0).get("id")).isEqualTo(1L);
            assertThat(records.get(1).get("id")).isEqualTo(2L);
        }
    }

    @Test
    void readTail() throws Exception {
        // plain_uncompressed.parquet: id INT64, value INT64 — 3 rows.
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("plain_uncompressed.parquet")));
             AvroRowReader reader = AvroReaders.buildRowReader(fileReader).tail(1).build()) {

            List<GenericRecord> records = readAll(reader);
            assertThat(records).hasSize(1);
            assertThat(records.getFirst().get("id")).isEqualTo(3L);
        }
    }

    @Test
    void readProjectionWithFilter() throws Exception {
        // filter_pushdown_int.parquet: id 1-300 across 3 row groups. Projecting
        // `id` together with a filter on `id` must still read correctly.
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("filter_pushdown_int.parquet")));
             AvroRowReader reader = AvroReaders.buildRowReader(fileReader)
                     .projection(ColumnProjection.columns("id"))
                     .filter(FilterPredicate.gt("id", 200L)).build()) {

            assertThat(reader.getSchema().getFields()).hasSize(1);
            List<GenericRecord> records = readAll(reader);
            assertThat(records).hasSize(100);
            for (GenericRecord record : records) {
                assertThat((Long) record.get("id")).isGreaterThan(200L);
            }
        }
    }

    @Test
    void readProjectionWithFilterAndHead() throws Exception {
        // filter_pushdown_int.parquet: id 1-300 across 3 row groups. Combine all
        // three controls: project `id`, filter id > 200 (matches 201-300), then
        // head(10) to keep the first 10 matched rows (201-210).
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("filter_pushdown_int.parquet")));
             AvroRowReader reader = AvroReaders.buildRowReader(fileReader)
                     .projection(ColumnProjection.columns("id"))
                     .filter(FilterPredicate.gt("id", 200L))
                     .head(10).build()) {

            assertThat(reader.getSchema().getFields()).hasSize(1);
            List<GenericRecord> records = readAll(reader);
            assertThat(records).hasSize(10);
            assertThat(records.get(0).get("id")).isEqualTo(201L);
            assertThat(records.get(9).get("id")).isEqualTo(210L);
        }
    }

    @Test
    void readWholeNestedGroupProjection() throws Exception {
        // nested_struct_test.parquet: id INT32, address { street, city, zip }.
        // Projecting the parent group `address` (no dot) must retain all of its
        // children — the parent-group-expands-to-all-children path under pruning.
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("nested_struct_test.parquet")));
             AvroRowReader reader = AvroReaders.buildRowReader(fileReader)
                     .projection(ColumnProjection.columns("address")).build()) {

            Schema schema = reader.getSchema();
            assertThat(schema.getFields()).hasSize(1);
            Schema addressRecord = schema.getField("address").schema().getTypes().stream()
                    .filter(s -> s.getType() == Schema.Type.RECORD)
                    .findFirst()
                    .orElseThrow();
            assertThat(addressRecord.getFields()).hasSize(3);

            GenericRecord address = (GenericRecord) readAll(reader).getFirst().get("address");
            assertThat(address.get("street").toString()).isEqualTo("123 Main St");
            assertThat(address.get("city").toString()).isEqualTo("New York");
            assertThat(address.get("zip")).isEqualTo(10001);
        }
    }

    @Test
    void readListColumnProjection() throws Exception {
        // list_basic_test.parquet: id INT32, tags list<string>, scores list<int>.
        // Project only the list column `tags` (excluding `id` and `scores`): list
        // columns are retained wholesale, so the schema must carry only `tags` and
        // reading the array must work under the narrowed schema.
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("list_basic_test.parquet")));
             AvroRowReader reader = AvroReaders.buildRowReader(fileReader)
                     .projection(ColumnProjection.columns("tags")).build()) {

            Schema schema = reader.getSchema();
            assertThat(schema.getFields()).hasSize(1);
            assertThat(schema.getField("tags")).isNotNull();

            List<GenericRecord> records = readAll(reader);
            assertThat(records).isNotEmpty();
            assertThat((List<?>) records.getFirst().get("tags")).isNotEmpty();
        }
    }

    @Test
    void readMapColumnProjection() throws Exception {
        // simple_map_test.parquet: id INT32, name STRING, attributes map<string,int>.
        // Project only the map column `attributes`: like lists, maps are retained
        // wholesale, so the schema must carry only `attributes`.
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("simple_map_test.parquet")));
             AvroRowReader reader = AvroReaders.buildRowReader(fileReader)
                     .projection(ColumnProjection.columns("attributes")).build()) {

            Schema schema = reader.getSchema();
            assertThat(schema.getFields()).hasSize(1);
            assertThat(schema.getField("attributes")).isNotNull();

            List<GenericRecord> records = readAll(reader);
            assertThat(records).isNotEmpty();
            assertThat(records.getFirst().get("attributes")).isInstanceOf(Map.class);
        }
    }

    @Test
    void readListOfStructSubFieldProjection() throws Exception {
        // list_struct_test.parquet: id INT32, items list<struct{name, quantity}>.
        // Project a single sub-field inside the list element (items.list.element.
        // quantity). The row reader serves partial element structs holding only
        // `quantity`, so the Avro element record must narrow the same way —
        // otherwise materialization would read the unprojected `name`.
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("list_struct_test.parquet")));
             AvroRowReader reader = AvroReaders.buildRowReader(fileReader)
                     .projection(ColumnProjection.columns("items.list.element.quantity")).build()) {

            Schema schema = reader.getSchema();
            assertThat(schema.getFields()).hasSize(1);
            Schema itemsArray = schema.getField("items").schema().getTypes().stream()
                    .filter(s -> s.getType() == Schema.Type.ARRAY)
                    .findFirst()
                    .orElseThrow();
            Schema elementRecord = itemsArray.getElementType().getTypes().stream()
                    .filter(s -> s.getType() == Schema.Type.RECORD)
                    .findFirst()
                    .orElseThrow();
            assertThat(elementRecord.getFields()).hasSize(1);
            assertThat(elementRecord.getField("quantity")).isNotNull();
            assertThat(elementRecord.getField("name")).isNull();

            List<GenericRecord> records = readAll(reader);
            assertThat(records).hasSize(3);

            @SuppressWarnings("unchecked")
            List<GenericRecord> firstItems = (List<GenericRecord>) records.getFirst().get("items");
            assertThat(firstItems).hasSize(2);
            assertThat(firstItems.get(0).get("quantity")).isEqualTo(5);
            assertThat(firstItems.get(1).get("quantity")).isEqualTo(10);
        }
    }

    @Test
    void readMapValueSubFieldProjection() throws Exception {
        // map_struct_value_test.parquet: id, people map<string, struct{name, age}>.
        // Row 0 people = {employee1:{Alice,30}, employee2:{Bob,25}}.
        //
        // Projecting only people.key_value.value.age must materialize a map of
        // {key -> {age}}: the Avro schema narrows the map value record to a single
        // `age` field (no `name`), and the map's key is force-included by the
        // core resolver so the reader can still assemble the map.
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("map_struct_value_test.parquet")));
             AvroRowReader reader = AvroReaders.buildRowReader(fileReader)
                     .projection(ColumnProjection.columns("people.key_value.value.age")).build()) {

            Schema schema = reader.getSchema();
            assertThat(schema.getFields()).hasSize(1);
            Schema peopleMap = schema.getField("people").schema().getTypes().stream()
                    .filter(s -> s.getType() == Schema.Type.MAP)
                    .findFirst()
                    .orElseThrow();
            Schema valueRecord = peopleMap.getValueType().getTypes().stream()
                    .filter(s -> s.getType() == Schema.Type.RECORD)
                    .findFirst()
                    .orElseThrow();
            assertThat(valueRecord.getFields()).hasSize(1);
            assertThat(valueRecord.getField("age")).isNotNull();
            assertThat(valueRecord.getField("name")).isNull();

            List<GenericRecord> records = readAll(reader);
            assertThat(records).hasSize(3);

            @SuppressWarnings("unchecked")
            Map<String, GenericRecord> people = (Map<String, GenericRecord>) records.get(0).get("people");
            assertThat(people).containsOnlyKeys("employee1", "employee2");
            assertThat(people.get("employee1").get("age")).isEqualTo(30);
            assertThat(people.get("employee2").get("age")).isEqualTo(25);
        }
    }

    @Test
    void compatibilityPlanMaterializesNestedProjectionLikeNativePlan() throws Exception {
        ColumnProjection projection = ColumnProjection.columns("people.key_value.value.age");
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("map_struct_value_test.parquet")));
             AvroRowReader nativeReader = AvroReaders.buildRowReader(fileReader).projection(projection).build();
             RowReader compatibilityRowReader = fileReader.buildRowReader().projection(projection).build();
             AvroRowReader compatibilityReader = new AvroRowReader(compatibilityRowReader,
                     AvroSchemaConverter.planForParquetAvroCompatibility(fileReader.getFileSchema(), projection))) {

            List<GenericRecord> nativeRows = readAll(nativeReader);
            List<GenericRecord> compatibilityRows = readAll(compatibilityReader);
            assertEquivalentRecords(nativeRows, compatibilityRows);
        }
    }

    @Test
    void compatibilityPlanMaterializesProjectedListRecordLikeNativePlan() throws Exception {
        ColumnProjection projection = ColumnProjection.columns("items.list.element.quantity");
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("list_struct_test.parquet")));
             AvroRowReader nativeReader = AvroReaders.buildRowReader(fileReader).projection(projection).build();
             RowReader compatibilityRowReader = fileReader.buildRowReader().projection(projection).build();
             AvroRowReader compatibilityReader = new AvroRowReader(compatibilityRowReader,
                     AvroSchemaConverter.planForParquetAvroCompatibility(fileReader.getFileSchema(), projection))) {
            assertEquivalentRecords(readAll(nativeReader), readAll(compatibilityReader));
        }
    }

    @Test
    void compatibilityPlanMaterializesKeyOnlyMapWithNullValues() throws Exception {
        ColumnProjection projection = ColumnProjection.columns("people.key_value.key");
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("map_struct_value_test.parquet")));
             AvroRowReader nativeReader = AvroReaders.buildRowReader(fileReader).projection(projection).build();
             RowReader compatibilityRowReader = fileReader.buildRowReader().projection(projection).build();
             AvroRowReader compatibilityReader = new AvroRowReader(compatibilityRowReader,
                     AvroSchemaConverter.planForParquetAvroCompatibility(fileReader.getFileSchema(), projection))) {

            Schema nativeValue = resolveNullable(resolveNullable(nativeReader.getSchema().getField("people").schema())
                    .getValueType());
            Schema compatibilityValue = resolveNullable(resolveNullable(
                    compatibilityReader.getSchema().getField("people").schema()).getValueType());
            assertThat(nativeValue.getType()).isEqualTo(Schema.Type.RECORD);
            assertThat(compatibilityValue.getType()).isEqualTo(Schema.Type.RECORD);
            assertThat(nativeValue.getName()).isEqualTo("value");
            assertThat(compatibilityValue.getName()).isEqualTo("value");

            List<GenericRecord> nativeRows = readAll(nativeReader);
            List<GenericRecord> compatibilityRows = readAll(compatibilityReader);
            assertEquivalentRecords(nativeRows, compatibilityRows);
            for (GenericRecord row : compatibilityRows) {
                @SuppressWarnings("unchecked")
                Map<String, Object> people = (Map<String, Object>) row.get("people");
                assertThat(people.values()).allMatch(value -> value == null);
            }
        }
    }

    @Test
    void readVariantSubFieldProjection() throws Exception {
        // variant_shredded_test.parquet: var is a shredded Variant
        // {metadata, value, typed_value:int64}; row 0 holds INT64(42) whose
        // canonical value bytes are [0x18, 42, 0, 0, 0, 0, 0, 0, 0].
        //
        // Variants are read atomically: projecting any leaf inside a Variant
        // pulls the whole column, and the Avro view stays the canonical
        // {metadata: bytes, value: bytes} record carrying the reassembled bytes
        // — identical to the unprojected shape exercised by readShreddedVariantColumn.
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("variant_shredded_test.parquet")));
             AvroRowReader reader = AvroReaders.buildRowReader(fileReader)
                     .projection(ColumnProjection.columns("var.typed_value")).build()) {

            Schema schema = reader.getSchema();
            assertThat(schema.getFields()).hasSize(1);
            Schema varRecord = schema.getField("var").schema().getTypes().stream()
                    .filter(s -> s.getType() == Schema.Type.RECORD)
                    .findFirst()
                    .orElseThrow();
            assertThat(varRecord.getFields()).hasSize(2);
            assertThat(varRecord.getField("metadata").schema().getType()).isEqualTo(Schema.Type.BYTES);
            assertThat(varRecord.getField("value").schema().getType()).isEqualTo(Schema.Type.BYTES);

            List<GenericRecord> records = readAll(reader);
            GenericRecord var0 = (GenericRecord) records.get(0).get("var");
            assertThat(var0).isNotNull();
            assertThat(bytes(var0.get("value")))
                    .containsExactly(0x18, 42, 0, 0, 0, 0, 0, 0, 0);
        }
    }

    @Test
    void plainFixedUsesByteLengthAsFixedSize() throws Exception {
        // delta_byte_array_flba_test.parquet: id INT32, tag_req/tag_opt stored as
        // plain FIXED_LEN_BYTE_ARRAY of length 4 (no decimal annotation). The Avro
        // `fixed` size must follow the column's byte length, not the hardcoded 12.
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("delta_byte_array_flba_test.parquet")));
             AvroRowReader reader = AvroReaders.rowReader(fileReader)) {

            Schema tagReq = reader.getSchema().getField("tag_req").schema();
            assertThat(tagReq.getType()).isEqualTo(Schema.Type.FIXED);
            assertThat(tagReq.getFixedSize()).isEqualTo(4);

            // tag_opt is OPTIONAL → union [null, fixed(4)].
            Schema tagOpt = reader.getSchema().getField("tag_opt").schema();
            Schema fixed = tagOpt.getTypes().stream()
                    .filter(s -> s.getType() == Schema.Type.FIXED)
                    .findFirst()
                    .orElseThrow();
            assertThat(fixed.getFixedSize()).isEqualTo(4);

            // Plain (non-decimal) FIXED must also materialize as GenericFixed and
            // round-trip through serialization.
            List<GenericRecord> records = readAll(reader);
            assertThat(records).isNotEmpty();
            for (GenericRecord record : records) {
                assertThat(record.get("tag_req")).isInstanceOf(GenericData.Fixed.class);
            }
            assertThatCode(() -> serialize(reader.getSchema(), records))
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void nestedFixedColumnsMaterializeAsGenericFixedAndSerialize() throws Exception {
        // nested_flba_test.parquet: id INT32, s struct<tag: fixed(4)>, l list<fixed(4)>,
        // m map<string, fixed(4)>. Exercises the struct, list, and map-value FIXED
        // materialize paths — each must yield a GenericData.Fixed of size 4, and the
        // records must round-trip through a GenericDatumWriter.
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("nested_flba_test.parquet")));
             AvroRowReader reader = AvroReaders.rowReader(fileReader)) {

            Schema schema = reader.getSchema();
            List<GenericRecord> records = readAll(reader);
            assertThat(records).hasSize(2);

            GenericRecord first = records.get(0);

            GenericRecord struct = (GenericRecord) first.get("s");
            assertThat(struct.get("tag")).isInstanceOf(GenericData.Fixed.class);
            assertThat(((GenericData.Fixed) struct.get("tag")).bytes())
                    .containsExactly(0x00, 0x01, 0x02, 0x03);

            List<?> list = (List<?>) first.get("l");
            assertThat(list).hasSize(2);
            assertThat(list).allMatch(e -> e instanceof GenericData.Fixed fixed
                    && fixed.bytes().length == 4);

            Map<?, ?> map = (Map<?, ?>) first.get("m");
            assertThat(map).hasSize(2);
            assertThat(map.values()).allMatch(v -> v instanceof GenericData.Fixed fixed
                    && fixed.bytes().length == 4);

            assertThatCode(() -> serialize(schema, records)).doesNotThrowAnyException();
        }
    }

    @Test
    void enumValuesMaterializeAsStringsAtEveryNestingPosition() throws Exception {
        // enum_nested_test.parquet: ENUM values occur at the top level, inside a
        // struct, as list elements, and as map values. Avro maps ENUM to string,
        // so every non-null materialized value must be a String.
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("enum_nested_test.parquet")));
             AvroRowReader reader = AvroReaders.rowReader(fileReader)) {

            Schema schema = reader.getSchema();
            assertThat(resolveNullable(schema.getField("status").schema()).getType())
                    .isEqualTo(Schema.Type.STRING);
            assertThat(resolveNullable(resolveNullable(schema.getField("tags").schema()).getElementType()).getType())
                    .isEqualTo(Schema.Type.STRING);
            Schema meta = resolveNullable(schema.getField("meta").schema());
            assertThat(resolveNullable(meta.getField("kind").schema()).getType())
                    .isEqualTo(Schema.Type.STRING);
            assertThat(resolveNullable(resolveNullable(schema.getField("labels").schema()).getValueType()).getType())
                    .isEqualTo(Schema.Type.STRING);

            List<GenericRecord> records = readAll(reader);
            assertThat(records).hasSize(3);

            assertThat(records.get(0).get("status")).isEqualTo("ACTIVE");
            assertThat(((GenericRecord) records.get(0).get("meta")).get("kind")).isEqualTo("PRIMARY");
            List<?> firstTags = (List<?>) records.get(0).get("tags");
            assertThat(firstTags).hasSize(2);
            assertThat(firstTags.get(0)).isEqualTo("RED");
            assertThat(firstTags.get(1)).isEqualTo("GREEN");
            assertThat(((Map<?, ?>) records.get(0).get("labels")).get("a")).isEqualTo("ON");

            assertThat(records.get(1).get("status")).isEqualTo("INACTIVE");
            assertThat(((GenericRecord) records.get(1).get("meta")).get("kind")).isEqualTo("SECONDARY");
            List<?> secondTags = (List<?>) records.get(1).get("tags");
            assertThat(secondTags).hasSize(1);
            assertThat(secondTags.get(0)).isEqualTo("BLUE");
            Map<?, ?> secondLabels = (Map<?, ?>) records.get(1).get("labels");
            assertThat(secondLabels).hasSize(2);
            assertThat(secondLabels.get("b")).isEqualTo("OFF");
            assertThat(secondLabels.get("c")).isEqualTo("ON");

            assertThat(records.get(2).get("status")).isNull();
            GenericRecord thirdMeta = (GenericRecord) records.get(2).get("meta");
            assertThat(thirdMeta.get("kind")).isNull();
            assertThat(((List<?>) records.get(2).get("tags")).get(0)).isNull();
            assertThat(((Map<?, ?>) records.get(2).get("labels")).get("d")).isNull();
        }
    }

    @Test
    void enumListRecordsSerializeWithGeneratedSchema() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("enum_nested_test.parquet")));
             AvroRowReader reader = AvroReaders.rowReader(fileReader)) {

            Schema schema = reader.getSchema();
            List<GenericRecord> records = readAll(reader);

            assertThatCode(() -> serialize(schema, records)).doesNotThrowAnyException();
        }
    }

    @Test
    void listOfDecimalOnFixedMaterializesAsGenericFixed() throws Exception {
        // list_decimal_flba_test.parquet: id INT32, amounts list<decimal(10,2)>
        // stored as FIXED_LEN_BYTE_ARRAY(5). The Avro element type is a `fixed`
        // carrying the decimal logical type. A decimal element decodes to BigDecimal
        // via PqList.get, so the list FIXED path must read the raw physical bytes
        // (PqList.getRaw) to wrap each element as a GenericData.Fixed carrying the
        // on-disk two's-complement payload — and the records must serialize.
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("list_decimal_flba_test.parquet")));
             AvroRowReader reader = AvroReaders.rowReader(fileReader)) {

            Schema schema = reader.getSchema();
            List<GenericRecord> records = readAll(reader);
            assertThat(records).hasSize(2);

            @SuppressWarnings("unchecked")
            List<Object> amounts = (List<Object>) records.get(0).get("amounts");
            assertThat(amounts).hasSize(2);
            assertThat(amounts).allMatch(e -> e instanceof GenericData.Fixed fixed
                    && fixed.bytes().length == 5);
            // The fixed bytes are the two's-complement unscaled value, sign-extended
            // to width 5 — decoding them at scale 2 must recover the original decimals.
            assertThat(decimalFromFixed(amounts.get(0))).isEqualByComparingTo("1.23");
            assertThat(decimalFromFixed(amounts.get(1))).isEqualByComparingTo("45.67");

            @SuppressWarnings("unchecked")
            List<Object> amounts1 = (List<Object>) records.get(1).get("amounts");
            assertThat(decimalFromFixed(amounts1.get(0))).isEqualByComparingTo("-9.99");

            assertThatCode(() -> serialize(schema, records)).doesNotThrowAnyException();
        }
    }

    private static BigDecimal decimalFromFixed(Object fixed) {
        return new BigDecimal(new BigInteger(((GenericData.Fixed) fixed).bytes()), 2);
    }

    private static FileSchema primitiveSchema(String name, PhysicalType type, RepetitionType repetition) {
        SchemaElement root = SchemaElement.root("root", 1);
        SchemaElement value = SchemaElement.primitive(name, type, repetition);
        return FileSchema.fromSchemaElements(List.of(root, value));
    }

    private static FileSchema nestedPrimitiveSchema() {
        SchemaElement root = SchemaElement.root("root", 1);
        SchemaElement struct = SchemaElement.group("record", RepetitionType.REQUIRED, 1);
        SchemaElement value = SchemaElement.primitive("value", PhysicalType.INT32, RepetitionType.REQUIRED);
        return FileSchema.fromSchemaElements(List.of(root, struct, value));
    }

    private static FileSchema listStringSchema() {
        SchemaElement root = SchemaElement.root("root", 1);
        SchemaElement list = SchemaElement.group("items", RepetitionType.REQUIRED, 1, new ListType());
        SchemaElement repeated = SchemaElement.group("list", RepetitionType.REPEATED, 1);
        SchemaElement element = SchemaElement.primitive("element", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                new StringType());
        return FileSchema.fromSchemaElements(List.of(root, list, repeated, element));
    }

    /// `list<null>` — every element is null in a well-formed file, so the `NULL` arm of
    /// the element switch is only reachable when the accessors and the plan disagree.
    private static FileSchema listOfNullSchema() {
        SchemaElement root = SchemaElement.root("root", 1);
        SchemaElement list = SchemaElement.group("nulls", RepetitionType.REQUIRED, 1, new ListType());
        SchemaElement repeated = SchemaElement.group("list", RepetitionType.REPEATED, 1);
        SchemaElement element = SchemaElement.primitive("element", PhysicalType.INT32, RepetitionType.OPTIONAL, new NullType());
        return FileSchema.fromSchemaElements(List.of(root, list, repeated, element));
    }

    /// `map<string, null>` — the map counterpart of [#listOfNullSchema].
    private static FileSchema mapOfNullSchema() {
        SchemaElement root = SchemaElement.root("root", 1);
        SchemaElement map = SchemaElement.group("attributes", RepetitionType.REQUIRED, 1, new MapType());
        SchemaElement keyValue = SchemaElement.group("key_value", RepetitionType.REPEATED, 2);
        SchemaElement key = SchemaElement.primitive("key", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, new StringType());
        SchemaElement value = SchemaElement.primitive("value", PhysicalType.INT32, RepetitionType.OPTIONAL,
                new NullType());
        return FileSchema.fromSchemaElements(List.of(root, map, keyValue, key, value));
    }

    private static FileSchema mapStringSchema() {
        SchemaElement root = SchemaElement.root("root", 1);
        SchemaElement map = SchemaElement.group("attributes", RepetitionType.REQUIRED, 1, new MapType());
        SchemaElement keyValue = SchemaElement.group("key_value", RepetitionType.REPEATED, 2);
        SchemaElement key = SchemaElement.primitive("key", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, new StringType());
        SchemaElement value = SchemaElement.primitive("value", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, new StringType());
        return FileSchema.fromSchemaElements(List.of(root, map, keyValue, key, value));
    }

    private static Map<String, Object> values(Object... entries) {
        Map<String, Object> values = new HashMap<>();
        for (int i = 0; i < entries.length; i += 2) {
            values.put((String) entries[i], entries[i + 1]);
        }
        return values;
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, Map<String, Object> values) {
        return (T) Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[] { type },
                (proxy, method, arguments) -> {
                    if (method.getName().equals("toString")) {
                        return type.getSimpleName() + " proxy";
                    }
                    if (method.getName().equals("hashCode")) {
                        return System.identityHashCode(proxy);
                    }
                    if (method.getName().equals("equals")) {
                        return proxy == arguments[0];
                    }
                    if (values.containsKey(method.getName())) {
                        return values.get(method.getName());
                    }
                    throw new AssertionError("Unexpected accessor: " + method.getName());
                });
    }

    private static void assertEquivalentRecords(List<GenericRecord> expected, List<GenericRecord> actual) {
        assertThat(actual).hasSize(expected.size());
        for (int index = 0; index < expected.size(); index++) {
            assertEquivalentValue(expected.get(index), actual.get(index));
        }
    }

    private static void assertEquivalentValue(Object expected, Object actual) {
        assertThat(actual).as("runtime value class").isInstanceOf(expected.getClass());
        if (expected instanceof GenericRecord expectedRecord) {
            GenericRecord actualRecord = (GenericRecord) actual;
            assertThat(actualRecord.getSchema().getFields()).extracting(Schema.Field::name)
                    .containsExactlyElementsOf(expectedRecord.getSchema().getFields().stream()
                            .map(Schema.Field::name).toList());
            for (Schema.Field field : expectedRecord.getSchema().getFields()) {
                Object expectedValue = expectedRecord.get(field.name());
                Object actualValue = actualRecord.get(field.name());
                if (expectedValue == null) {
                    assertThat(actualValue).isNull();
                }
                else {
                    assertEquivalentValue(expectedValue, actualValue);
                }
            }
            return;
        }
        if (expected instanceof List<?> expectedList) {
            List<?> actualList = (List<?>) actual;
            assertThat(actualList).hasSize(expectedList.size());
            for (int index = 0; index < expectedList.size(); index++) {
                Object expectedValue = expectedList.get(index);
                Object actualValue = actualList.get(index);
                if (expectedValue == null) {
                    assertThat(actualValue).isNull();
                }
                else {
                    assertEquivalentValue(expectedValue, actualValue);
                }
            }
            return;
        }
        if (expected instanceof Map<?, ?> expectedMap) {
            Map<?, ?> actualMap = (Map<?, ?>) actual;
            assertThat(actualMap.keySet().containsAll(expectedMap.keySet())).isTrue();
            assertThat(expectedMap.keySet().containsAll(actualMap.keySet())).isTrue();
            for (Map.Entry<?, ?> entry : expectedMap.entrySet()) {
                Object expectedValue = entry.getValue();
                Object actualValue = actualMap.get(entry.getKey());
                if (expectedValue == null) {
                    assertThat(actualValue).isNull();
                }
                else {
                    assertEquivalentValue(expectedValue, actualValue);
                }
            }
            return;
        }
        assertThat(actual).isEqualTo(expected);
    }

    private static Schema resolveNullable(Schema schema) {
        if (!schema.isUnion()) {
            return schema;
        }
        return schema.getTypes().stream()
                .filter(candidate -> candidate.getType() != Schema.Type.NULL)
                .findFirst()
                .orElseThrow();
    }

    private static void serialize(Schema schema, List<GenericRecord> records) throws Exception {
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        for (GenericRecord record : records) {
            writer.write(record, encoder);
        }
        encoder.flush();
    }

    private static List<GenericRecord> readAll(AvroRowReader reader) {
        List<GenericRecord> records = new ArrayList<>();
        while (reader.hasNext()) {
            records.add(reader.next());
        }
        return records;
    }
}
