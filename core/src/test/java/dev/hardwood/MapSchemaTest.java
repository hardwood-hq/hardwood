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

/// Tests for MAP schema parsing and data reading.
public class MapSchemaTest {

    @Test
    void testSimpleMapSchema() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/simple_map_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(parquetFile))) {
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

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            try (RowReader rowReader = fileReader.rowReader()) {
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

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(parquetFile))) {
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

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            try (RowReader rowReader = fileReader.rowReader()) {
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

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            try (RowReader rowReader = fileReader.rowReader()) {
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

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            try (RowReader rowReader = fileReader.rowReader()) {
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
    void testMapWithListInsideStructValue() throws Exception {
        // Schema: id, entries: map<string, struct<scores: list<int32>>> (hardwood-hq/hardwood#293)
        Path parquetFile = Paths.get("src/test/resources/map_with_list_value_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rowReader = fileReader.rowReader()) {

            // Row 0: entries={a: {scores=[10,20]}}
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(1);
            PqMap entries0 = rowReader.getMap("entries");
            assertThat(entries0.size()).isEqualTo(1);
            List<PqMap.Entry> row0Entries = entries0.getEntries();
            assertThat(row0Entries.get(0).getStringKey()).isEqualTo("a");
            assertThat(row0Entries.get(0).isValueNull()).isFalse();
            PqList scoresA = row0Entries.get(0).getStructValue().getList("scores");
            assertThat(scoresA).isNotNull();
            List<Integer> scoresAValues = new ArrayList<>();
            for (int s : scoresA.ints()) {
                scoresAValues.add(s);
            }
            assertThat(scoresAValues).containsExactly(10, 20);

            // Row 1: entries={b: {scores=[30,40,50]}, c: {scores=[]}, d: null, e: {scores=null}}
            // This is the regression path for #293: the value column emits one record
            // per list element for 'b' and one marker for each of 'c', 'd', 'e', so a
            // key-indexed valueIdx of `start+index` no longer matches the value side.
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(2);
            PqMap entries1 = rowReader.getMap("entries");
            assertThat(entries1.size()).isEqualTo(4);
            List<PqMap.Entry> row1Entries = entries1.getEntries();

            assertThat(row1Entries.get(0).getStringKey()).isEqualTo("b");
            assertThat(row1Entries.get(0).isValueNull()).isFalse();
            PqList scoresB = row1Entries.get(0).getStructValue().getList("scores");
            List<Integer> scoresBValues = new ArrayList<>();
            for (int s : scoresB.ints()) {
                scoresBValues.add(s);
            }
            assertThat(scoresBValues).containsExactly(30, 40, 50);

            // Non-null value struct with an empty inner list
            assertThat(row1Entries.get(1).getStringKey()).isEqualTo("c");
            assertThat(row1Entries.get(1).isValueNull()).isFalse();
            PqList scoresC = row1Entries.get(1).getStructValue().getList("scores");
            assertThat(scoresC).isNotNull();
            assertThat(scoresC.isEmpty()).isTrue();

            // Null value struct
            assertThat(row1Entries.get(2).getStringKey()).isEqualTo("d");
            assertThat(row1Entries.get(2).isValueNull()).isTrue();
            assertThat(row1Entries.get(2).getStructValue()).isNull();

            // Non-null value struct but its inner list is null
            assertThat(row1Entries.get(3).getStringKey()).isEqualTo("e");
            assertThat(row1Entries.get(3).isValueNull()).isFalse();
            assertThat(row1Entries.get(3).getStructValue().getList("scores")).isNull();

            // Row 2: empty map
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(3);
            PqMap entries2 = rowReader.getMap("entries");
            assertThat(entries2).isNotNull();
            assertThat(entries2.isEmpty()).isTrue();

            assertThat(rowReader.hasNext()).isFalse();
        }
    }

    @Test
    void testMapByIndex() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/simple_map_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            try (RowReader rowReader = fileReader.rowReader()) {
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

    // ==================== getMapKey / getMapValue ====================

    @Test
    void testGetMapKeyAndGetMapValueOnStandardEncoding() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/simple_map_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            SchemaNode.GroupNode attributesGroup =
                    (SchemaNode.GroupNode) fileReader.getFileSchema().getField("attributes");
            assertThat(attributesGroup.isMap()).isTrue();

            SchemaNode keyNode = attributesGroup.getMapKey();
            assertThat(keyNode).isNotNull();
            assertThat(keyNode.name()).isEqualTo("key");
            assertThat(keyNode).isInstanceOf(SchemaNode.PrimitiveNode.class);

            SchemaNode valueNode = attributesGroup.getMapValue();
            assertThat(valueNode).isNotNull();
            assertThat(valueNode.name()).isEqualTo("value");
            assertThat(valueNode).isInstanceOf(SchemaNode.PrimitiveNode.class);
        }
    }

    @Test
    void testGetMapKeyValueWithMapOfMaps() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/map_of_maps_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            // Find the outer MAP group; its value is itself a MAP.
            SchemaNode.GroupNode outerMap = null;
            for (SchemaNode child : fileReader.getFileSchema().getRootNode().children()) {
                if (child instanceof SchemaNode.GroupNode g && g.isMap()) {
                    outerMap = g;
                    break;
                }
            }
            assertThat(outerMap).isNotNull();

            // Inner MAP is the outer map's value.
            SchemaNode innerValue = outerMap.getMapValue();
            assertThat(innerValue).isInstanceOf(SchemaNode.GroupNode.class);
            SchemaNode.GroupNode innerMap = (SchemaNode.GroupNode) innerValue;
            assertThat(innerMap.isMap()).isTrue();

            // Inner map's own key/value should be reachable too.
            assertThat(innerMap.getMapKey()).isNotNull();
            assertThat(innerMap.getMapValue()).isNotNull();
        }
    }

    @Test
    void testGetMapKeyValueReturnsNullForNonMapGroup() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/simple_map_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            // The MAP group itself is "attributes"; navigate to its key_value subgroup, which
            // is REPEATED but not MAP-annotated — getMapKey/Value on it must return null.
            SchemaNode.GroupNode attributesGroup =
                    (SchemaNode.GroupNode) fileReader.getFileSchema().getField("attributes");
            SchemaNode.GroupNode keyValueGroup =
                    (SchemaNode.GroupNode) attributesGroup.children().get(0);
            assertThat(keyValueGroup.isMap()).isFalse();
            assertThat(keyValueGroup.getMapKey()).isNull();
            assertThat(keyValueGroup.getMapValue()).isNull();
        }
    }
}
