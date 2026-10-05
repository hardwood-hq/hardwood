/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.schema;

import java.nio.file.Path;
import java.util.List;

import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;

import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

/// Covers `field_id` on the schema model: read from a footer, written back to one, and
/// declared through [FileSchema.Builder].
///
/// `field_ids.parquet` is written by PyArrow with an id on every element except the root and
/// the synthetic `list` and `key_value` groups, the way Iceberg stamps them.
class FileSchemaFieldIdTest {

    private static final Path FIXTURE = Path.of("src/test/resources/field_ids.parquet");

    private static final Tuple[] FIXTURE_IDS = {
            tuple("schema", null),
            tuple("id", 1),
            tuple("location", 2),
            tuple("lat", 3),
            tuple("lon", 4),
            tuple("tags", 5),
            tuple("list", null),
            tuple("element", 6),
            tuple("attributes", 7),
            tuple("key_value", null),
            tuple("key", 8),
            tuple("value", 9)
    };

    private static List<SchemaElement> fixtureElements() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            return reader.getFileMetaData().schema();
        }
    }

    @Test
    void fieldIdsSurviveReadThenRewrite() throws Exception {
        List<SchemaElement> elements = fixtureElements();
        assertThat(elements).extracting(SchemaElement::name, SchemaElement::fieldId).containsExactly(FIXTURE_IDS);

        List<SchemaElement> rewritten = FileSchema.fromSchemaElements(elements).toSchemaElements();

        assertThat(rewritten).extracting(SchemaElement::name, SchemaElement::fieldId).containsExactly(FIXTURE_IDS);
    }

    @Test
    void readSchemaExposesFieldIds() throws Exception {
        FileSchema schema = FileSchema.fromSchemaElements(fixtureElements());

        assertThat(schema.getColumns()).extracting(column -> column.fieldPath().toString(), ColumnSchema::fieldId)
                .containsExactly(
                        tuple("id", 1),
                        tuple("location.lat", 3),
                        tuple("location.lon", 4),
                        tuple("tags.list.element", 6),
                        tuple("attributes.key_value.key", 8),
                        tuple("attributes.key_value.value", 9));
        assertThat(schema.getRootNode().fieldId()).isNull();
        assertThat(schema.getField("location").fieldId()).isEqualTo(2);
        SchemaNode.GroupNode tags = (SchemaNode.GroupNode) schema.getField("tags");
        assertThat(tags.fieldId()).isEqualTo(5);
        assertThat(tags.children().get(0).fieldId()).isNull();
        assertThat(tags.getListElement().fieldId()).isEqualTo(6);
        SchemaNode.GroupNode attributes = (SchemaNode.GroupNode) schema.getField("attributes");
        assertThat(attributes.fieldId()).isEqualTo(7);
        assertThat(attributes.getMapKey().fieldId()).isEqualTo(8);
        assertThat(attributes.getMapValue().fieldId()).isEqualTo(9);
    }

    @Test
    void writtenFileCarriesTheFieldIdsOfTheSchemaItWasReadWith() throws Exception {
        FileSchema schema = FileSchema.fromSchemaElements(fixtureElements());

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.rowWriter().writeRow(row -> row.setLong("id", 1L));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            assertThat(reader.getFileMetaData().schema())
                    .extracting(SchemaElement::name, SchemaElement::fieldId)
                    .containsExactly(FIXTURE_IDS);
        }
    }

    @Test
    void builderDeclaresTheFieldIdsOfEveryKindOfField() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED, column -> column.fieldId(1))
                .struct("location", RepetitionType.OPTIONAL, location -> location.fieldId(2)
                        .addColumn("lat", PhysicalType.DOUBLE, RepetitionType.OPTIONAL, column -> column.fieldId(3))
                        .addColumn("lon", PhysicalType.DOUBLE, RepetitionType.OPTIONAL, column -> column.fieldId(4)))
                .list("tags", RepetitionType.OPTIONAL, element -> element.fieldId(5)
                        .primitive(PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                                column -> column.logicalType(LogicalType.string()).fieldId(6)))
                .map("attributes", RepetitionType.OPTIONAL, PhysicalType.BYTE_ARRAY,
                        key -> key.logicalType(LogicalType.string()).fieldId(8),
                        value -> value.fieldId(7)
                                .primitive(PhysicalType.INT64, RepetitionType.OPTIONAL, column -> column.fieldId(9)))
                .build();

        assertThat(schema.toSchemaElements()).isEqualTo(FileSchema.fromSchemaElements(fixtureElements())
                .toSchemaElements());
    }

    @Test
    void builderDeclaresTheFieldIdsOfNestedGroupElements() {
        FileSchema schema = FileSchema.builder("schema")
                .list("matrix", RepetitionType.OPTIONAL, outer -> outer.fieldId(1)
                        .list(RepetitionType.OPTIONAL, inner -> inner.fieldId(2)
                                .primitive(PhysicalType.INT32, RepetitionType.OPTIONAL, column -> column.fieldId(3))))
                .list("points", RepetitionType.OPTIONAL, element -> element.fieldId(4)
                        .struct(RepetitionType.OPTIONAL, point -> point.fieldId(5)
                                .addColumn("x", PhysicalType.INT32, RepetitionType.REQUIRED, column -> column.fieldId(6))))
                .map("lookup", RepetitionType.OPTIONAL, PhysicalType.INT32, value -> value.fieldId(7)
                        .map(RepetitionType.OPTIONAL, PhysicalType.INT32, key -> key.fieldId(8),
                                inner -> inner.fieldId(9)
                                        .primitive(PhysicalType.INT32, RepetitionType.OPTIONAL)))
                .build();

        assertThat(schema.toSchemaElements()).extracting(SchemaElement::name, SchemaElement::fieldId)
                .containsExactly(
                        tuple("schema", null),
                        tuple("matrix", 1),
                        tuple("list", null),
                        tuple("element", 2),
                        tuple("list", null),
                        tuple("element", 3),
                        tuple("points", 4),
                        tuple("list", null),
                        tuple("element", 5),
                        tuple("x", 6),
                        tuple("lookup", 7),
                        tuple("key_value", null),
                        tuple("key", null),
                        tuple("value", 9),
                        tuple("key_value", null),
                        tuple("key", 8),
                        tuple("value", null));
    }

    @Test
    void toStringShowsFieldIds() throws Exception {
        FileSchema schema = FileSchema.fromSchemaElements(fixtureElements());

        assertThat(schema.toString()).isEqualTo("""
                message schema {
                  required int64 id = 1;
                  optional group location = 2 {
                    optional double lat = 3;
                    optional double lon = 4;
                  }
                  optional group tags (LIST) = 5 {
                    repeated group list {
                      optional byte_array element (STRING) = 6;
                    }
                  }
                  optional group attributes (MAP) = 7 {
                    repeated group key_value {
                      required byte_array key (STRING) = 8;
                      optional int64 value = 9;
                    }
                  }
                }""");
        assertThat(schema.getColumn("location.lat").toString()).isEqualTo("optional double lat = 3;");
    }

    @Test
    void columnAttributeDeclaredTwiceIsRejected() {
        assertThatThrownBy(() -> FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED,
                        column -> column.fieldId(1).fieldId(2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The field id of column id is already declared");
        assertThatThrownBy(() -> FileSchema.builder("schema")
                .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                        column -> column.logicalType(LogicalType.string()).logicalType(LogicalType.json())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The logical type of column name is already declared");
        assertThatThrownBy(() -> FileSchema.builder("schema")
                .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                        column -> column.logicalType(null).logicalType(LogicalType.string())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The logical type of column name is already declared");
        assertThatThrownBy(() -> FileSchema.builder("schema")
                .addColumn("digest", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED,
                        column -> column.typeLength(16).typeLength(32)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The type length of column digest is already declared");
    }

    @Test
    void groupFieldIdDeclaredTwiceIsRejected() {
        assertThatThrownBy(() -> FileSchema.builder("schema")
                .struct("location", RepetitionType.OPTIONAL, location -> location.fieldId(1).fieldId(2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The field id of struct location is already declared");
        assertThatThrownBy(() -> FileSchema.builder("schema")
                .list("tags", RepetitionType.OPTIONAL, element -> element.fieldId(1).fieldId(2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The field id of tags is already declared");
        assertThatThrownBy(() -> FileSchema.builder("schema")
                .map("attributes", RepetitionType.OPTIONAL, PhysicalType.INT32,
                        value -> value.fieldId(1).fieldId(2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The field id of attributes is already declared");
    }
}
