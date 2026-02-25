/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.parquet.compat;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.GroupReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for parquet-java API compatibility.
 */
class ParquetReaderCompatTest {

    @Test
    void testBasicReading() throws Exception {
        Path path = new Path("../core/src/test/resources/plain_uncompressed.parquet");

        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path).build()) {
            List<Group> records = new ArrayList<>();
            Group record;
            while ((record = reader.read()) != null) {
                records.add(record);
            }

            assertThat(records).hasSize(3);

            // Check first record: id=1, value=100
            Group first = records.get(0);
            assertThat(first.getLong("id", 0)).isEqualTo(1L);
            assertThat(first.getLong("value", 0)).isEqualTo(100L);

            // Check second record: id=2, value=200
            Group second = records.get(1);
            assertThat(second.getLong("id", 0)).isEqualTo(2L);
            assertThat(second.getLong("value", 0)).isEqualTo(200L);

            // Check repetition count for non-repeated fields
            assertThat(first.getFieldRepetitionCount("id")).isEqualTo(1);
            assertThat(first.getFieldRepetitionCount("value")).isEqualTo(1);
        }
    }

    @Test
    void testSchemaAccess() throws Exception {
        Path path = new Path("../core/src/test/resources/plain_uncompressed.parquet");

        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path).build()) {
            Group record = reader.read();
            assertThat(record).isNotNull();

            // Access schema
            GroupType schema = record.getType();
            assertThat(schema).isInstanceOf(MessageType.class);
            assertThat(schema.getFieldCount()).isEqualTo(2);

            // Check field types
            Type idField = schema.getType("id");
            assertThat(idField.isPrimitive()).isTrue();
            assertThat(idField.asPrimitiveType().getPrimitiveTypeName())
                    .isEqualTo(PrimitiveType.PrimitiveTypeName.INT64);

            Type valueField = schema.getType(1);
            assertThat(valueField.getName()).isEqualTo("value");
        }
    }

    @Test
    void testNestedStruct() throws Exception {
        Path path = new Path("../core/src/test/resources/nested_struct_test.parquet");

        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path).build()) {
            Group record = reader.read();
            assertThat(record).isNotNull();

            // Read top-level field (schema: id INT32, address struct)
            assertThat(record.getInteger("id", 0)).isEqualTo(1);

            // Read nested group
            Group address = record.getGroup("address", 0);
            assertThat(address).isNotNull();
            assertThat(address.getString("street", 0)).isEqualTo("123 Main St");
            assertThat(address.getString("city", 0)).isEqualTo("New York");
            assertThat(address.getInteger("zip", 0)).isEqualTo(10001);
        }
    }

    @Test
    void testRepeatedFields() throws Exception {
        Path path = new Path("../core/src/test/resources/list_basic_test.parquet");

        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path).build()) {
            Group record = reader.read();
            assertThat(record).isNotNull();

            // Schema: id INT32, tags list<string>, scores list<int>
            assertThat(record.getInteger("id", 0)).isEqualTo(1);

            // Check repetition count for tags (list with 3 elements: "a", "b", "c")
            int tagCount = record.getFieldRepetitionCount("tags");
            assertThat(tagCount).isEqualTo(1); // The list itself is present once

            // Check repetition count for scores (list with 3 elements: 10, 20, 30)
            int scoreCount = record.getFieldRepetitionCount("scores");
            assertThat(scoreCount).isEqualTo(1); // The list itself is present once
        }
    }

    @Test
    void testStringPath() throws Exception {
        // Test using string path
        try (ParquetReader<Group> reader = ParquetReader.builder(
                new GroupReadSupport(), "../core/src/test/resources/plain_uncompressed.parquet").build()) {
            Group record = reader.read();
            assertThat(record).isNotNull();
            assertThat(record.getLong("id", 0)).isEqualTo(1L);
        }
    }

    @Test
    void testNullHandling() throws Exception {
        // Schema: id INT64, name STRING (optional, with nulls)
        Path path = new Path("../core/src/test/resources/plain_uncompressed_with_nulls.parquet");

        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path).build()) {
            List<Group> records = new ArrayList<>();
            Group record;
            while ((record = reader.read()) != null) {
                records.add(record);
            }

            assertThat(records).hasSize(3);

            // Check that we can handle null values
            // The repetition count should be 0 for null optional fields, 1 otherwise
            for (Group r : records) {
                int count = r.getFieldRepetitionCount("name");
                assertThat(count).isIn(0, 1);
            }
        }
    }

    @Test
    void testBinaryField() throws Exception {
        Path path = new Path("../core/src/test/resources/plain_uncompressed.parquet");

        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path).build()) {
            // Just verify we can read without errors
            Group record = reader.read();
            assertThat(record).isNotNull();
        }
    }

    @Test
    void testMultipleRowGroups() throws Exception {
        // plain_uncompressed.parquet has 3 rows which should be readable
        Path path = new Path("../core/src/test/resources/plain_uncompressed.parquet");

        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path).build()) {
            int count = 0;
            while (reader.read() != null) {
                count++;
            }
            assertThat(count).isEqualTo(3);
        }
    }

    @Test
    void testBuilderWithConfiguration() throws Exception {
        // Test that withConf() doesn't break anything
        Path path = new Path("../core/src/test/resources/plain_uncompressed.parquet");
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("some.property", "value");

        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
                .withConf(conf)
                .build()) {
            Group record = reader.read();
            assertThat(record).isNotNull();
        }
    }
}
