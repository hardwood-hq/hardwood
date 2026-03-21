/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.avro;

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

import static org.assertj.core.api.Assertions.assertThat;

class AvroRowReaderTest {

    private static final Path TEST_RESOURCES = Path.of("").toAbsolutePath()
            .resolve("../core/src/test/resources").normalize();

    @Test
    void readFlatSchema() throws Exception {
        // plain_uncompressed.parquet: id INT64, value INT64 — 3 rows
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(TEST_RESOURCES.resolve("plain_uncompressed.parquet")));
             AvroRowReader reader = AvroReaders.createRowReader(fileReader)) {

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
             AvroRowReader reader = AvroReaders.createRowReader(fileReader)) {

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
             AvroRowReader reader = AvroReaders.createRowReader(fileReader)) {

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
             AvroRowReader reader = AvroReaders.createRowReader(fileReader)) {

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
             AvroRowReader reader = AvroReaders.createRowReader(fileReader)) {

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
             AvroRowReader reader = AvroReaders.createRowReader(fileReader,
                     FilterPredicate.gt("id", 200L))) {

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

    private static List<GenericRecord> readAll(AvroRowReader reader) {
        List<GenericRecord> records = new ArrayList<>();
        while (reader.hasNext()) {
            records.add(reader.next());
        }
        return records;
    }
}
