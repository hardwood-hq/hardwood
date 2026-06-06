/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqMap;
import dev.hardwood.schema.SchemaNode;

import static org.assertj.core.api.Assertions.assertThat;

/// End-to-end recognition of the MAP logical-type annotation, exercised in
/// every form a Parquet writer may emit:
///
/// - `both annotations` — legacy `ConvertedType.MAP` plus modern
///   `LogicalType.MapType` (PyArrow's default output).
/// - `modern-only` — `LogicalType.MapType` only, no `ConvertedType`. This is
///   the regression case for `LogicalTypeReader` field ID 2.
/// - `legacy key_value` — no annotation on the outer group, only
///   `ConvertedType.MAP_KEY_VALUE` on the inner repeated group. Older
///   parquet-mr / Hive / Impala writers emit this form (hardwood-hq/hardwood#596).
///
/// All three fixtures carry the same data: row 0 maps `a→1, b→2`; row 1 is null.
class MapLogicalTypeRecognitionTest {

    static Stream<Arguments> allVariants() {
        return Stream.of(
                Arguments.of(Named.of("both annotations",
                        Paths.get("src/test/resources/map_annotation_both_test.parquet"))),
                Arguments.of(Named.of("modern-only (logicalType only)",
                        Paths.get("src/test/resources/map_annotation_modern_only_test.parquet"))),
                Arguments.of(Named.of("legacy key_value (MAP_KEY_VALUE on inner group only)",
                        Paths.get("src/test/resources/map_annotation_legacy_key_value_test.parquet"))));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allVariants")
    void testOuterGroupIsRecognisedAsMap(Path file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            SchemaNode attrsNode = reader.getFileSchema().getRootNode().children().get(1);
            assertThat(attrsNode).isInstanceOf(SchemaNode.GroupNode.class);
            SchemaNode.GroupNode attrsGroup = (SchemaNode.GroupNode) attrsNode;
            assertThat(attrsGroup.isMap()).isTrue();
            assertThat(attrsGroup.isStruct()).isFalse();
            assertThat(attrsGroup.isList()).isFalse();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allVariants")
    void testLogicalTypeIsMapType(Path file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            SchemaNode.GroupNode attrsGroup =
                    (SchemaNode.GroupNode) reader.getFileSchema().getRootNode().children().get(1);
            assertThat(attrsGroup.logicalType()).isInstanceOf(LogicalType.MapType.class);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allVariants")
    void testKeyValueChildrenReachable(Path file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            SchemaNode.GroupNode attrsGroup =
                    (SchemaNode.GroupNode) reader.getFileSchema().getRootNode().children().get(1);
            assertThat(attrsGroup.children()).hasSize(1);
            SchemaNode.GroupNode keyValue = (SchemaNode.GroupNode) attrsGroup.children().get(0);
            assertThat(keyValue.name()).isEqualTo("key_value");
            assertThat(keyValue.children()).hasSize(2);
            assertThat(keyValue.children().get(0).name()).isEqualTo("key");
            assertThat(keyValue.children().get(1).name()).isEqualTo("value");
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allVariants")
    void testMapEntriesReadCorrectly(Path file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                RowReader rowReader = reader.rowReader()) {
            assertThat(rowReader.hasNext()).isTrue();
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(1);

            PqMap map = rowReader.getMap("attrs");
            assertThat(map).isNotNull();
            assertThat(map.size()).isEqualTo(2);
            assertThat(map.getEntries().get(0).getStringKey()).isEqualTo("a");
            assertThat(map.getEntries().get(0).getIntValue()).isEqualTo(1);
            assertThat(map.getEntries().get(1).getStringKey()).isEqualTo("b");
            assertThat(map.getEntries().get(1).getIntValue()).isEqualTo(2);

            assertThat(rowReader.hasNext()).isTrue();
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(2);
            assertThat(rowReader.isNull("attrs")).isTrue();

            assertThat(rowReader.hasNext()).isFalse();
        }
    }
}
