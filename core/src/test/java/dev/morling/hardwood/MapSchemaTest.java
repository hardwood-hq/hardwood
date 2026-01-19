/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.morling.hardwood.metadata.ConvertedType;
import dev.morling.hardwood.reader.ParquetFileReader;
import dev.morling.hardwood.reader.RowReader;
import dev.morling.hardwood.row.PqList;
import dev.morling.hardwood.row.PqMap;
import dev.morling.hardwood.row.PqRow;
import dev.morling.hardwood.schema.SchemaNode;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for MAP schema parsing and data reading.
 */
public class MapSchemaTest {

    @Test
    void testSimpleMapSchema() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/simple_map_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            var schema = fileReader.getFileSchema();
            var root = schema.getRootNode();

            // Root should have 3 children: id, name, attributes
            assertThat(root.children()).hasSize(3);

            // Check attributes MAP
            var attributesNode = root.children().get(2);
            assertThat(attributesNode).isInstanceOf(SchemaNode.GroupNode.class);
            var attributesGroup = (SchemaNode.GroupNode) attributesNode;
            assertThat(attributesGroup.name()).isEqualTo("attributes");
            assertThat(attributesGroup.isMap()).isTrue();
            assertThat(attributesGroup.convertedType()).isEqualTo(ConvertedType.MAP);

            // MAP has structure: attributes (MAP) -> key_value (REPEATED) -> key, value
            assertThat(attributesGroup.children()).hasSize(1);
            var keyValueGroup = (SchemaNode.GroupNode) attributesGroup.children().get(0);
            assertThat(keyValueGroup.children()).hasSize(2); // key and value

            var keyNode = (SchemaNode.PrimitiveNode) keyValueGroup.children().get(0);
            assertThat(keyNode.name()).isEqualTo("key");

            var valueNode = (SchemaNode.PrimitiveNode) keyValueGroup.children().get(1);
            assertThat(valueNode.name()).isEqualTo("value");
        }
    }

    @Test
    void testSimpleMapData() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/simple_map_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<PqRow> rows = new ArrayList<>();
                for (PqRow row : rowReader) {
                    rows.add(row);
                }

                assertThat(rows).hasSize(5);

                // Row 0: Alice with 3 attributes
                PqRow row0 = rows.get(0);
                assertThat(row0.getInt("id")).isEqualTo(1);
                assertThat(row0.getString("name")).isEqualTo("Alice");

                PqMap map0 = row0.getMap("attributes");
                assertThat(map0.size()).isEqualTo(3);

                // Check key-value pairs (order preserved from PyArrow)
                List<PqMap.Entry> entries0 = map0.getEntries();
                assertThat(entries0.get(0).getStringKey()).isEqualTo("age");
                assertThat(entries0.get(0).getIntValue()).isEqualTo(30);
                assertThat(entries0.get(1).getStringKey()).isEqualTo("score");
                assertThat(entries0.get(1).getIntValue()).isEqualTo(95);
                assertThat(entries0.get(2).getStringKey()).isEqualTo("level");
                assertThat(entries0.get(2).getIntValue()).isEqualTo(5);

                // Row 2: Charlie with empty map
                PqRow row2 = rows.get(2);
                assertThat(row2.getString("name")).isEqualTo("Charlie");
                PqMap map2 = row2.getMap("attributes");
                assertThat(map2.isEmpty()).isTrue();

                // Row 3: Diana with null map
                PqRow row3 = rows.get(3);
                assertThat(row3.getString("name")).isEqualTo("Diana");
                assertThat(row3.isNull("attributes")).isTrue();
            }
        }
    }

    @Test
    void testMapOfMapsSchema() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/map_of_maps_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            var schema = fileReader.getFileSchema();
            var root = schema.getRootNode();

            // Root should have 3 children: id, name, nested_map
            assertThat(root.children()).hasSize(3);

            // Check nested_map MAP<string, MAP<string, int32>>
            var nestedMapNode = (SchemaNode.GroupNode) root.children().get(2);
            assertThat(nestedMapNode.name()).isEqualTo("nested_map");
            assertThat(nestedMapNode.isMap()).isTrue();

            // Navigate to inner map
            var outerKeyValue = (SchemaNode.GroupNode) nestedMapNode.children().get(0);
            assertThat(outerKeyValue.children()).hasSize(2); // key and value

            var valueNode = outerKeyValue.children().get(1);
            assertThat(valueNode).isInstanceOf(SchemaNode.GroupNode.class);
            var innerMapGroup = (SchemaNode.GroupNode) valueNode;
            assertThat(innerMapGroup.isMap()).isTrue();
        }
    }

    @Test
    void testMapOfMapsData() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/map_of_maps_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<PqRow> rows = new ArrayList<>();
                for (PqRow row : rowReader) {
                    rows.add(row);
                }

                assertThat(rows).hasSize(5);

                // Row 0: Department A with two teams
                PqRow row0 = rows.get(0);
                assertThat(row0.getInt("id")).isEqualTo(1);
                assertThat(row0.getString("name")).isEqualTo("Department A");

                PqMap outerMap0 = row0.getMap("nested_map");
                assertThat(outerMap0.size()).isEqualTo(2);

                // First team: team1 with alice=100, bob=95
                List<PqMap.Entry> outerEntries = outerMap0.getEntries();
                assertThat(outerEntries.get(0).getStringKey()).isEqualTo("team1");

                PqMap team1 = outerEntries.get(0).getMapValue();
                assertThat(team1.size()).isEqualTo(2);
                List<PqMap.Entry> team1Entries = team1.getEntries();
                assertThat(team1Entries.get(0).getStringKey()).isEqualTo("alice");
                assertThat(team1Entries.get(0).getIntValue()).isEqualTo(100);

                // Row 3: Department D with empty outer map
                PqRow row3 = rows.get(3);
                assertThat(row3.getString("name")).isEqualTo("Department D");
                PqMap outerMap3 = row3.getMap("nested_map");
                assertThat(outerMap3.isEmpty()).isTrue();

                // Row 4: Department E with null map
                PqRow row4 = rows.get(4);
                assertThat(row4.getString("name")).isEqualTo("Department E");
                assertThat(row4.isNull("nested_map")).isTrue();
            }
        }
    }

    @Test
    void testListOfMapsData() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/list_of_maps_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<PqRow> rows = new ArrayList<>();
                for (PqRow row : rowReader) {
                    rows.add(row);
                }

                assertThat(rows).hasSize(5);

                // Row 0: List with 3 maps
                PqRow row0 = rows.get(0);
                assertThat(row0.getInt("id")).isEqualTo(1);

                PqList mapList0 = row0.getList("map_list");
                assertThat(mapList0.size()).isEqualTo(3);

                // First map: {a:1, b:2}
                List<PqMap> maps = new ArrayList<>();
                for (PqMap map : mapList0.maps()) {
                    maps.add(map);
                }
                assertThat(maps.get(0).size()).isEqualTo(2);
                assertThat(maps.get(0).getEntries().get(0).getStringKey()).isEqualTo("a");
                assertThat(maps.get(0).getEntries().get(0).getIntValue()).isEqualTo(1);

                // Row 3: Empty list
                PqRow row3 = rows.get(3);
                PqList mapList3 = row3.getList("map_list");
                assertThat(mapList3.isEmpty()).isTrue();

                // Row 4: Null list
                PqRow row4 = rows.get(4);
                assertThat(row4.isNull("map_list")).isTrue();
            }
        }
    }

    @Test
    void testMapWithStructValues() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/map_struct_value_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<PqRow> rows = new ArrayList<>();
                for (PqRow row : rowReader) {
                    rows.add(row);
                }

                assertThat(rows).hasSize(3);

                // Row 0: Two employees
                PqRow row0 = rows.get(0);
                assertThat(row0.getInt("id")).isEqualTo(1);

                PqMap people0 = row0.getMap("people");
                assertThat(people0.size()).isEqualTo(2);

                // First entry: employee1 -> {name: Alice, age: 30}
                List<PqMap.Entry> entries = people0.getEntries();
                assertThat(entries.get(0).getStringKey()).isEqualTo("employee1");

                PqRow person1 = entries.get(0).getRowValue();
                assertThat(person1.getString("name")).isEqualTo("Alice");
                assertThat(person1.getInt("age")).isEqualTo(30);

                // Row 2: Empty map
                PqRow row2 = rows.get(2);
                PqMap people2 = row2.getMap("people");
                assertThat(people2.isEmpty()).isTrue();
            }
        }
    }
}
