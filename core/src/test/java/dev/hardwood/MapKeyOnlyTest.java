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
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqMap;
import dev.hardwood.schema.SchemaNode;

import static org.assertj.core.api.Assertions.assertThat;

/// Tests for Parquet MAP fields whose {@code key_value} group has no {@code value} field.
class MapKeyOnlyTest {

    private static final Path FILE = Paths.get("src/test/resources/map_key_only_test.parquet");

    @Test
    void testOuterGroupIsRecognisedAsMap() throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE))) {
            SchemaNode attrsNode = reader.getFileSchema().getRootNode().children().get(1);
            assertThat(attrsNode).isInstanceOf(SchemaNode.GroupNode.class);
            SchemaNode.GroupNode attrsGroup = (SchemaNode.GroupNode) attrsNode;
            assertThat(attrsGroup.isMap()).isTrue();
        }
    }

    @Test
    void testLogicalTypeIsMapType() throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE))) {
            SchemaNode.GroupNode attrsGroup =
                    (SchemaNode.GroupNode) reader.getFileSchema().getRootNode().children().get(1);
            assertThat(attrsGroup.logicalType()).isInstanceOf(LogicalType.MapType.class);
        }
    }

    @Test
    void testKeyValueChildrenReachable() throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE))) {
            SchemaNode.GroupNode attrsGroup =
                    (SchemaNode.GroupNode) reader.getFileSchema().getRootNode().children().get(1);
            assertThat(attrsGroup.children()).hasSize(1);
            SchemaNode.GroupNode keyValue = (SchemaNode.GroupNode) attrsGroup.children().get(0);
            assertThat(keyValue.name()).isEqualTo("key_value");
            assertThat(keyValue.children()).hasSize(1);
            assertThat(keyValue.children().get(0).name()).isEqualTo("key");
        }
    }

    @Test
    void testReadKeyOnlyMap() throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE))) {
            RowReader rowReader = reader.rowReader();
            assertThat(rowReader.hasNext()).isTrue();

            // Row 1: id=1, tags={'a'=null, 'b'=null}
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(1);
            PqMap tags1 = rowReader.getMap("tags");
            assertThat(tags1.size()).isEqualTo(2);

            List<PqMap.Entry> entries1 = tags1.getEntries();
            assertThat(entries1.get(0).getStringKey()).isEqualTo("a");
            assertThat(entries1.get(0).getValue()).isNull();
            assertThat(entries1.get(0).isValueNull()).isTrue();

            assertThat(entries1.get(1).getStringKey()).isEqualTo("b");
            assertThat(entries1.get(1).getValue()).isNull();
            assertThat(entries1.get(1).isValueNull()).isTrue();

            // Row 2: id=2, tags={'c'=null}
            assertThat(rowReader.hasNext()).isTrue();
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(2);
            PqMap tags2 = rowReader.getMap("tags");
            assertThat(tags2.size()).isEqualTo(1);
            assertThat(tags2.getEntries().get(0).getStringKey()).isEqualTo("c");
            assertThat(tags2.getEntries().get(0).getValue()).isNull();
        }
    }
}
