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
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.ConvertedType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.SchemaNode;

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
                int rowCount = 0;

                // Row 0: Alice with 3 attributes
                assertThat(rowReader.hasNext()).isTrue();
                rowReader.next();
                rowCount++;
                assertThat(rowReader.getInt("id")).isEqualTo(1);
                assertThat(rowReader.getString("name")).isEqualTo("Alice");

                PqMap map0 = rowReader.getMap("attributes");
                assertThat(map0.size()).isEqualTo(3);

                // Check key-value pairs (order preserved from PyArrow)
                List<PqMap.Entry> entries0 = map0.getEntries();
                assertThat(entries0.get(0).getStringKey()).isEqualTo("age");
                assertThat(entries0.get(0).getIntValue()).isEqualTo(30);
                assertThat(entries0.get(1).getStringKey()).isEqualTo("score");
                assertThat(entries0.get(1).getIntValue()).isEqualTo(95);
                assertThat(entries0.get(2).getStringKey()).isEqualTo("level");
                assertThat(entries0.get(2).getIntValue()).isEqualTo(5);

                // Row 1: Bob (skip)
                assertThat(rowReader.hasNext()).isTrue();
                rowReader.next();
                rowCount++;

                // Row 2: Charlie with empty map
                assertThat(rowReader.hasNext()).isTrue();
                rowReader.next();
                rowCount++;
                assertThat(rowReader.getString("name")).isEqualTo("Charlie");
                PqMap map2 = rowReader.getMap("attributes");
                assertThat(map2.isEmpty()).isTrue();

                // Row 3: Diana with null map
                assertThat(rowReader.hasNext()).isTrue();
                rowReader.next();
                rowCount++;
                assertThat(rowReader.getString("name")).isEqualTo("Diana");
                assertThat(rowReader.isNull("attributes")).isTrue();

                // Row 4: skip and count
                while (rowReader.hasNext()) {
                    rowReader.next();
                    rowCount++;
                }

                assertThat(rowCount).isEqualTo(5);
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
                int rowCount = 0;

                // Row 0: Department A with two teams
                assertThat(rowReader.hasNext()).isTrue();
                rowReader.next();
                rowCount++;
                assertThat(rowReader.getInt("id")).isEqualTo(1);
                assertThat(rowReader.getString("name")).isEqualTo("Department A");

                PqMap outerMap0 = rowReader.getMap("nested_map");
                assertThat(outerMap0.size()).isEqualTo(2);

                // First team: team1 with alice=100, bob=95
                List<PqMap.Entry> outerEntries = outerMap0.getEntries();
                assertThat(outerEntries.get(0).getStringKey()).isEqualTo("team1");

                PqMap team1 = outerEntries.get(0).getMapValue();
                assertThat(team1.size()).isEqualTo(2);
                List<PqMap.Entry> team1Entries = team1.getEntries();
                assertThat(team1Entries.get(0).getStringKey()).isEqualTo("alice");
                assertThat(team1Entries.get(0).getIntValue()).isEqualTo(100);

                // Skip rows 1-2
                rowReader.next();
                rowCount++;
                rowReader.next();
                rowCount++;

                // Row 3: Department D with empty outer map
                assertThat(rowReader.hasNext()).isTrue();
                rowReader.next();
                rowCount++;
                assertThat(rowReader.getString("name")).isEqualTo("Department D");
                PqMap outerMap3 = rowReader.getMap("nested_map");
                assertThat(outerMap3.isEmpty()).isTrue();

                // Row 4: Department E with null map
                assertThat(rowReader.hasNext()).isTrue();
                rowReader.next();
                rowCount++;
                assertThat(rowReader.getString("name")).isEqualTo("Department E");
                assertThat(rowReader.isNull("nested_map")).isTrue();

                assertThat(rowReader.hasNext()).isFalse();
                assertThat(rowCount).isEqualTo(5);
            }
        }
    }

    @Test
    void testListOfMapsData() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/list_of_maps_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                int rowCount = 0;

                // Row 0: List with 3 maps
                assertThat(rowReader.hasNext()).isTrue();
                rowReader.next();
                rowCount++;
                assertThat(rowReader.getInt("id")).isEqualTo(1);

                PqList mapList0 = rowReader.getList("map_list");
                assertThat(mapList0.size()).isEqualTo(3);

                // First map: {a:1, b:2}
                List<PqMap> maps = new ArrayList<>();
                for (PqMap map : mapList0.maps()) {
                    maps.add(map);
                }
                assertThat(maps.get(0).size()).isEqualTo(2);
                assertThat(maps.get(0).getEntries().get(0).getStringKey()).isEqualTo("a");
                assertThat(maps.get(0).getEntries().get(0).getIntValue()).isEqualTo(1);

                // Skip rows 1-2
                rowReader.next();
                rowCount++;
                rowReader.next();
                rowCount++;

                // Row 3: Empty list
                assertThat(rowReader.hasNext()).isTrue();
                rowReader.next();
                rowCount++;
                PqList mapList3 = rowReader.getList("map_list");
                assertThat(mapList3.isEmpty()).isTrue();

                // Row 4: Null list
                assertThat(rowReader.hasNext()).isTrue();
                rowReader.next();
                rowCount++;
                assertThat(rowReader.isNull("map_list")).isTrue();

                assertThat(rowReader.hasNext()).isFalse();
                assertThat(rowCount).isEqualTo(5);
            }
        }
    }

    @Test
    void testMapWithStructValues() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/map_struct_value_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                int rowCount = 0;

                // Row 0: Two employees
                assertThat(rowReader.hasNext()).isTrue();
                rowReader.next();
                rowCount++;
                assertThat(rowReader.getInt("id")).isEqualTo(1);

                PqMap people0 = rowReader.getMap("people");
                assertThat(people0.size()).isEqualTo(2);

                // First entry: employee1 -> {name: Alice, age: 30}
                List<PqMap.Entry> entries = people0.getEntries();
                assertThat(entries.get(0).getStringKey()).isEqualTo("employee1");

                PqStruct person1 = entries.get(0).getStructValue();
                assertThat(person1.getString("name")).isEqualTo("Alice");
                assertThat(person1.getInt("age")).isEqualTo(30);

                // Skip row 1
                rowReader.next();
                rowCount++;

                // Row 2: Empty map
                assertThat(rowReader.hasNext()).isTrue();
                rowReader.next();
                rowCount++;
                PqMap people2 = rowReader.getMap("people");
                assertThat(people2.isEmpty()).isTrue();

                assertThat(rowReader.hasNext()).isFalse();
                assertThat(rowCount).isEqualTo(3);
            }
        }
    }

    @Test
    void testMapByIndex() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/simple_map_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                rowReader.next();

                // Find the attributes column index
                int attributesIdx = -1;
                for (int i = 0; i < rowReader.getFieldCount(); i++) {
                    if ("attributes".equals(rowReader.getFieldName(i))) {
                        attributesIdx = i;
                        break;
                    }
                }

                // Access map by index
                PqMap map = rowReader.getMap(attributesIdx);
                assertThat(map.size()).isEqualTo(3);

                List<PqMap.Entry> entries = map.getEntries();
                assertThat(entries.get(0).getStringKey()).isEqualTo("age");
                assertThat(entries.get(0).getIntValue()).isEqualTo(30);

                // Skip to row 3 (Diana with null map)
                rowReader.next();
                rowReader.next();
                rowReader.next();

                assertThat(rowReader.isNull(attributesIdx)).isTrue();
                assertThat(rowReader.getMap(attributesIdx)).isNull();
            }
        }
    }
}
