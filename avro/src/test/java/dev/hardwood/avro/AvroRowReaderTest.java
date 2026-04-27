/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.avro;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.avro.internal.AvroSchemaConverter;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqVariant;
import dev.hardwood.row.VariantType;

import static org.assertj.core.api.Assertions.assertThat;

class AvroRowReaderTest {

    private static final Path TEST_RESOURCES = Path.of("").toAbsolutePath()
            .resolve("../core/src/test/resources").normalize();

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

            GenericRecord first = records.get(0);
            assertThat(first.get("id")).isEqualTo(1);

            Object addressObj = first.get("address");
            assertThat(addressObj).isInstanceOf(GenericRecord.class);

            GenericRecord address = (GenericRecord) addressObj;
            assertThat(address.get("street").toString()).isEqualTo("123 Main St");
            assertThat(address.get("city").toString()).isEqualTo("New York");
            assertThat(address.get("zip")).isEqualTo(10001);
        }
    }

    @Test
    void readList() throws Exception {
        // list_basic_test.parquet: id INT32, tags list<string>, scores list<int>
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("list_basic_test.parquet")));
             AvroRowReader reader = AvroReaders.rowReader(fileReader)) {

            List<GenericRecord> records = readAll(reader);
            assertThat(records).isNotEmpty();

            GenericRecord first = records.get(0);
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

            GenericRecord first = records.get(0);
            Object attrs = first.get("attributes");
            assertThat(attrs).isInstanceOf(Map.class);

            @SuppressWarnings("unchecked")
            Map<String, Object> attrMap = (Map<String, Object>) attrs;
            assertThat(attrMap).isNotEmpty();
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

            Schema schema = AvroSchemaConverter.convert(fileReader.getFileSchema());
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

    private static List<GenericRecord> readAll(AvroRowReader reader) {
        List<GenericRecord> records = new ArrayList<>();
        while (reader.hasNext()) {
            records.add(reader.next());
        }
        return records;
    }
}
