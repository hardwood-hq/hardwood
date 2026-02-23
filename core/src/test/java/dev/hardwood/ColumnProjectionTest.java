/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for column projection support.
 */
public class ColumnProjectionTest {

    // ==================== ColumnProjection Unit Tests ====================

    @Test
    void testColumnProjectionAll() {
        ColumnProjection projection = ColumnProjection.all();
        assertThat(projection.projectsAll()).isTrue();
        assertThat(projection.getProjectedColumnNames()).isNull();
    }

    @Test
    void testColumnProjectionColumns() {
        ColumnProjection projection = ColumnProjection.columns("id", "name", "address");
        assertThat(projection.projectsAll()).isFalse();
        assertThat(projection.getProjectedColumnNames())
                .containsExactlyInAnyOrder("id", "name", "address");
    }

    @Test
    void testColumnProjectionRejectsEmptyColumns() {
        assertThatThrownBy(ColumnProjection::columns)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("At least one column");
    }

    @Test
    void testColumnProjectionRejectsNullColumnName() {
        assertThatThrownBy(() -> ColumnProjection.columns("id", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("null or empty");
    }

    @Test
    void testColumnProjectionRejectsEmptyColumnName() {
        assertThatThrownBy(() -> ColumnProjection.columns("id", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("null or empty");
    }

    // ==================== ProjectedSchema Unit Tests ====================

    @Test
    void testProjectedSchemaAllColumns() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(parquetFile)) {
            FileSchema schema = reader.getFileSchema();
            ProjectedSchema projected = ProjectedSchema.create(schema, ColumnProjection.all());

            assertThat(projected.projectsAll()).isTrue();
            assertThat(projected.getProjectedColumnCount()).isEqualTo(2);
            assertThat(projected.toOriginalIndex(0)).isEqualTo(0);
            assertThat(projected.toOriginalIndex(1)).isEqualTo(1);
            assertThat(projected.toProjectedIndex(0)).isEqualTo(0);
            assertThat(projected.toProjectedIndex(1)).isEqualTo(1);
        }
    }

    @Test
    void testProjectedSchemaSubset() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/logical_types_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(parquetFile)) {
            FileSchema schema = reader.getFileSchema();
            // Select only id and name from the schema
            ProjectedSchema projected = ProjectedSchema.create(schema, ColumnProjection.columns("id", "name"));

            assertThat(projected.projectsAll()).isFalse();
            assertThat(projected.getProjectedColumnCount()).isEqualTo(2);
            assertThat(projected.getProjectedColumn(0).name()).isEqualTo("id");
            assertThat(projected.getProjectedColumn(1).name()).isEqualTo("name");
        }
    }

    @Test
    void testProjectedSchemaUnknownColumn() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(parquetFile)) {
            FileSchema schema = reader.getFileSchema();

            assertThatThrownBy(() -> ProjectedSchema.create(schema, ColumnProjection.columns("nonexistent")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Column not found");
        }
    }

    // ==================== Flat Schema Projection Tests ====================

    @Test
    void testFlatSchemaReadAllColumns() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(parquetFile);
             RowReader rows = reader.createRowReader(ColumnProjection.all())) {

            assertThat(rows.getFieldCount()).isEqualTo(2);

            rows.next();
            assertThat(rows.getLong("id")).isEqualTo(1L);
            assertThat(rows.getLong("value")).isEqualTo(100L);

            rows.next();
            assertThat(rows.getLong("id")).isEqualTo(2L);
            assertThat(rows.getLong("value")).isEqualTo(200L);

            rows.next();
            assertThat(rows.getLong("id")).isEqualTo(3L);
            assertThat(rows.getLong("value")).isEqualTo(300L);

            assertThat(rows.hasNext()).isFalse();
        }
    }

    @Test
    void testFlatSchemaReadSingleColumn() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(parquetFile);
             RowReader rows = reader.createRowReader(ColumnProjection.columns("id"))) {

            assertThat(rows.getFieldCount()).isEqualTo(1);
            assertThat(rows.getFieldName(0)).isEqualTo("id");

            rows.next();
            assertThat(rows.getLong("id")).isEqualTo(1L);
            assertThat(rows.getLong(0)).isEqualTo(1L);

            rows.next();
            assertThat(rows.getLong("id")).isEqualTo(2L);

            rows.next();
            assertThat(rows.getLong("id")).isEqualTo(3L);

            assertThat(rows.hasNext()).isFalse();
        }
    }

    @Test
    void testFlatSchemaAccessNonProjectedColumnThrows() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(parquetFile);
             RowReader rows = reader.createRowReader(ColumnProjection.columns("id"))) {

            rows.next();

            // Accessing non-projected column should throw
            assertThatThrownBy(() -> rows.getLong("value"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("not in projection");
        }
    }

    @Test
    void testFlatSchemaProjectionWithNulls() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed_with_nulls.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(parquetFile);
             RowReader rows = reader.createRowReader(ColumnProjection.columns("name"))) {

            assertThat(rows.getFieldCount()).isEqualTo(1);

            // Row 0: name="alice"
            rows.next();
            assertThat(rows.isNull("name")).isFalse();
            assertThat(rows.getString("name")).isEqualTo("alice");

            // Row 1: name=null
            rows.next();
            assertThat(rows.isNull("name")).isTrue();
            assertThat(rows.getString("name")).isNull();

            // Row 2: name="charlie"
            rows.next();
            assertThat(rows.isNull("name")).isFalse();
            assertThat(rows.getString("name")).isEqualTo("charlie");

            assertThat(rows.hasNext()).isFalse();
        }
    }

    @Test
    void testFlatSchemaProjectMultipleColumns() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/logical_types_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(parquetFile);
             RowReader rows = reader.createRowReader(ColumnProjection.columns("id", "name", "balance"))) {

            assertThat(rows.getFieldCount()).isEqualTo(3);

            rows.next();
            assertThat(rows.getInt("id")).isEqualTo(1);
            assertThat(rows.getString("name")).isEqualTo("Alice");
            assertThat(rows.getDecimal("balance")).isEqualByComparingTo("1234.56");

            rows.next();
            assertThat(rows.getInt("id")).isEqualTo(2);
            assertThat(rows.getString("name")).isEqualTo("Bob");
            assertThat(rows.getDecimal("balance")).isEqualByComparingTo("9876.54");
        }
    }

    // ==================== Nested Schema Projection Tests ====================

    @Test
    void testNestedSchemaProjectTopLevelField() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/nested_struct_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(parquetFile);
             RowReader rows = reader.createRowReader(ColumnProjection.columns("id"))) {

            assertThat(rows.getFieldCount()).isEqualTo(1);
            assertThat(rows.getFieldName(0)).isEqualTo("id");

            // Row 0
            rows.next();
            assertThat(rows.getInt("id")).isEqualTo(1);

            // Row 1
            rows.next();
            assertThat(rows.getInt("id")).isEqualTo(2);

            // Row 2
            rows.next();
            assertThat(rows.getInt("id")).isEqualTo(3);

            assertThat(rows.hasNext()).isFalse();
        }
    }

    @Test
    void testNestedSchemaProjectStructField() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/nested_struct_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(parquetFile);
             RowReader rows = reader.createRowReader(ColumnProjection.columns("address"))) {

            assertThat(rows.getFieldCount()).isEqualTo(1);
            assertThat(rows.getFieldName(0)).isEqualTo("address");

            // Row 0: address={street="123 Main St", city="New York", zip=10001}
            rows.next();
            PqStruct address0 = rows.getStruct("address");
            assertThat(address0).isNotNull();
            assertThat(address0.getString("street")).isEqualTo("123 Main St");
            assertThat(address0.getString("city")).isEqualTo("New York");
            assertThat(address0.getInt("zip")).isEqualTo(10001);

            // Row 1
            rows.next();
            PqStruct address1 = rows.getStruct("address");
            assertThat(address1).isNotNull();
            assertThat(address1.getString("city")).isEqualTo("Los Angeles");

            // Row 2: address=null
            rows.next();
            assertThat(rows.isNull("address")).isTrue();
            assertThat(rows.getStruct("address")).isNull();

            assertThat(rows.hasNext()).isFalse();
        }
    }

    @Test
    void testNestedSchemaAccessNonProjectedThrows() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/nested_struct_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(parquetFile);
             RowReader rows = reader.createRowReader(ColumnProjection.columns("address"))) {

            rows.next();

            // Accessing non-projected column should throw
            assertThatThrownBy(() -> rows.getInt("id"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("not in projection");
        }
    }

    @Test
    void testNestedSchemaProjectBothFields() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/nested_struct_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(parquetFile);
             RowReader rows = reader.createRowReader(ColumnProjection.columns("id", "address"))) {

            assertThat(rows.getFieldCount()).isEqualTo(2);

            // Row 0
            rows.next();
            assertThat(rows.getInt("id")).isEqualTo(1);
            PqStruct address0 = rows.getStruct("address");
            assertThat(address0).isNotNull();
            assertThat(address0.getString("city")).isEqualTo("New York");

            // Row 2
            rows.next();
            rows.next();
            assertThat(rows.getInt("id")).isEqualTo(3);
            assertThat(rows.getStruct("address")).isNull();
        }
    }

    @Test
    void testListFieldProjection() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/list_basic_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(parquetFile);
             RowReader rows = reader.createRowReader(ColumnProjection.columns("tags"))) {

            assertThat(rows.getFieldCount()).isEqualTo(1);

            // Row 0: tags=["a","b","c"]
            rows.next();
            assertThat(rows.getList("tags")).isNotNull();
            assertThat(rows.getList("tags").size()).isEqualTo(3);

            // Accessing non-projected column should throw
            assertThatThrownBy(() -> rows.getInt("id"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("not in projection");
        }
    }

    @Test
    void testDeepNestedStructProjection() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/deep_nested_struct_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(parquetFile);
             RowReader rows = reader.createRowReader(ColumnProjection.columns("account"))) {

            assertThat(rows.getFieldCount()).isEqualTo(1);

            // Row 0: Alice with full nested structure
            rows.next();
            PqStruct account0 = rows.getStruct("account");
            assertThat(account0).isNotNull();
            assertThat(account0.getString("id")).isEqualTo("ACC-001");

            PqStruct org0 = account0.getStruct("organization");
            assertThat(org0).isNotNull();
            assertThat(org0.getString("name")).isEqualTo("Acme Corp");

            PqStruct addr0 = org0.getStruct("address");
            assertThat(addr0).isNotNull();
            assertThat(addr0.getString("city")).isEqualTo("New York");

            // Accessing non-projected column should throw
            assertThatThrownBy(() -> rows.getInt("customer_id"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("not in projection");
        }
    }

    // ==================== Index-Based Access Tests ====================

    @Test
    void testIndexBasedAccessWithProjection() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(parquetFile);
             RowReader rows = reader.createRowReader(ColumnProjection.columns("value"))) {

            assertThat(rows.getFieldCount()).isEqualTo(1);
            assertThat(rows.getFieldName(0)).isEqualTo("value");

            rows.next();
            // Using projected index 0 to access "value" (originally index 1)
            assertThat(rows.getLong(0)).isEqualTo(100L);

            rows.next();
            assertThat(rows.getLong(0)).isEqualTo(200L);

            rows.next();
            assertThat(rows.getLong(0)).isEqualTo(300L);
        }
    }

    @Test
    void testNestedIndexBasedAccessWithProjection() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/nested_struct_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(parquetFile);
             RowReader rows = reader.createRowReader(ColumnProjection.columns("address"))) {

            assertThat(rows.getFieldCount()).isEqualTo(1);
            assertThat(rows.getFieldName(0)).isEqualTo("address");

            rows.next();
            // Accessing via projected index
            assertThat(rows.isNull(0)).isFalse();
        }
    }
}
