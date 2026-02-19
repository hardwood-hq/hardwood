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

import dev.hardwood.metadata.ConvertedType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.SchemaNode;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for parsing nested schema structures.
 */
public class NestedSchemaTest {

    @Test
    void testNestedStructSchema() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/nested_struct_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            var schema = fileReader.getFileSchema();
            var root = schema.getRootNode();

            // Root should have 2 children: id and address
            assertThat(root.children()).hasSize(2);

            // Check id column
            var idNode = root.children().get(0);
            assertThat(idNode).isInstanceOf(SchemaNode.PrimitiveNode.class);
            assertThat(idNode.name()).isEqualTo("id");
            assertThat(idNode.maxDefinitionLevel()).isEqualTo(0); // REQUIRED
            assertThat(idNode.maxRepetitionLevel()).isEqualTo(0);

            // Check address struct
            var addressNode = root.children().get(1);
            assertThat(addressNode).isInstanceOf(SchemaNode.GroupNode.class);
            assertThat(addressNode.name()).isEqualTo("address");
            assertThat(addressNode.maxDefinitionLevel()).isEqualTo(1); // OPTIONAL
            assertThat(addressNode.maxRepetitionLevel()).isEqualTo(0);

            var addressGroup = (SchemaNode.GroupNode) addressNode;
            assertThat(addressGroup.isStruct()).isTrue();
            assertThat(addressGroup.children()).hasSize(3); // street, city, zip

            // Check nested fields have correct levels
            var streetNode = (SchemaNode.PrimitiveNode) addressGroup.children().get(0);
            assertThat(streetNode.name()).isEqualTo("street");
            assertThat(streetNode.maxDefinitionLevel()).isEqualTo(2); // parent(1) + optional(1)
            assertThat(streetNode.maxRepetitionLevel()).isEqualTo(0);

            // Flat column list should have 4 columns
            assertThat(schema.getColumnCount()).isEqualTo(4);
            assertThat(schema.getColumn(0).name()).isEqualTo("id");
            assertThat(schema.getColumn(1).name()).isEqualTo("street");
            assertThat(schema.getColumn(2).name()).isEqualTo("city");
            assertThat(schema.getColumn(3).name()).isEqualTo("zip");
        }
    }

    @Test
    void testListSchema() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/list_basic_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            var schema = fileReader.getFileSchema();
            var root = schema.getRootNode();

            // Root should have 3 children: id, tags, scores
            assertThat(root.children()).hasSize(3);

            // Check tags LIST
            var tagsNode = root.children().get(1);
            assertThat(tagsNode).isInstanceOf(SchemaNode.GroupNode.class);
            var tagsGroup = (SchemaNode.GroupNode) tagsNode;
            assertThat(tagsGroup.name()).isEqualTo("tags");
            assertThat(tagsGroup.isList()).isTrue();
            assertThat(tagsGroup.convertedType()).isEqualTo(ConvertedType.LIST);
            assertThat(tagsGroup.maxDefinitionLevel()).isEqualTo(1); // OPTIONAL

            // List has 3-level encoding: tags (LIST) -> list (REPEATED) -> element
            assertThat(tagsGroup.children()).hasSize(1);
            var innerGroup = (SchemaNode.GroupNode) tagsGroup.children().get(0);
            assertThat(innerGroup.name()).isEqualTo("list");
            assertThat(innerGroup.repetitionType()).isEqualTo(RepetitionType.REPEATED);
            assertThat(innerGroup.maxDefinitionLevel()).isEqualTo(2); // parent(1) + repeated counts as optional(1)
            assertThat(innerGroup.maxRepetitionLevel()).isEqualTo(1);

            // Element is the actual string column
            var elementNode = (SchemaNode.PrimitiveNode) innerGroup.children().get(0);
            assertThat(elementNode.name()).isEqualTo("element");
            assertThat(elementNode.maxDefinitionLevel()).isEqualTo(3); // list(2) + optional(1)
            assertThat(elementNode.maxRepetitionLevel()).isEqualTo(1);

            // getListElement() helper should work
            var listElement = tagsGroup.getListElement();
            assertThat(listElement).isNotNull();
            assertThat(listElement.name()).isEqualTo("element");
        }
    }

    @Test
    void testListOfStructsSchema() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/list_struct_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            var schema = fileReader.getFileSchema();
            var root = schema.getRootNode();

            // Root should have 2 children: id, items
            assertThat(root.children()).hasSize(2);

            // Check items LIST
            var itemsNode = (SchemaNode.GroupNode) root.children().get(1);
            assertThat(itemsNode.name()).isEqualTo("items");
            assertThat(itemsNode.isList()).isTrue();

            // The list element should be a struct
            var listElement = itemsNode.getListElement();
            assertThat(listElement).isInstanceOf(SchemaNode.GroupNode.class);
            var elementGroup = (SchemaNode.GroupNode) listElement;
            assertThat(elementGroup.name()).isEqualTo("element");
            assertThat(elementGroup.isStruct()).isTrue();
            assertThat(elementGroup.children()).hasSize(2); // name, quantity
        }
    }
}
