/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.schema;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// `SchemaElement.field_id` on the schema model: read from a footer, written back by
/// [FileSchema#toSchemaElements], and declared through [FileSchema.Builder].
class FileSchemaFieldIdTest {

    /// PyArrow writes field ids 1-10 on every declared field of the fixture; the root and the
    /// synthetic `list` and `key_value` groups have none.
    private static final Path FIXTURE = Paths.get("src/test/resources/field_ids_test.parquet");

    /// The fixture's field ids in schema-element order: root, `id`, `name`, `location`, `lat`,
    /// `lon`, `tags`, `list`, `element`, `props`, `key_value`, `key`, `value`.
    private static final List<Integer> FIXTURE_FIELD_IDS =
            Arrays.asList(null, 1, 2, 3, 4, 5, 6, null, 7, 8, null, 9, 10);

    @Test
    void fieldIdsSurviveTheRoundTripThroughTheSchemaModel() throws Exception {
        List<SchemaElement> elements = footerSchema(InputFile.of(FIXTURE));
        assertThat(fieldIds(elements)).isEqualTo(FIXTURE_FIELD_IDS);

        List<SchemaElement> flattened = FileSchema.fromSchemaElements(elements).toSchemaElements();

        assertThat(fieldIds(flattened)).isEqualTo(FIXTURE_FIELD_IDS);
    }

    @Test
    void schemaModelExposesTheFieldIds() throws Exception {
        FileSchema schema;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            schema = reader.getFileSchema();
        }

        assertThat(schema.getRootNode().fieldId()).isNull();
        assertThat(schema.getField("id").fieldId()).isEqualTo(1);
        assertThat(schema.getField("location").fieldId()).isEqualTo(3);
        SchemaNode.GroupNode tags = (SchemaNode.GroupNode) schema.getField("tags");
        assertThat(tags.fieldId()).isEqualTo(6);
        assertThat(tags.children().get(0).fieldId()).isNull();
        assertThat(tags.getListElement().fieldId()).isEqualTo(7);
        SchemaNode.GroupNode props = (SchemaNode.GroupNode) schema.getField("props");
        assertThat(props.fieldId()).isEqualTo(8);
        assertThat(props.getMapKey().fieldId()).isEqualTo(9);
        assertThat(props.getMapValue().fieldId()).isEqualTo(10);

        assertThat(schema.getColumns()).extracting(ColumnSchema::fieldId)
                .containsExactly(1, 2, 4, 5, 7, 9, 10);
    }

    /// A schema read from a file and passed to the writer writes the same field ids.
    @Test
    void readThenRewriteKeepsTheFieldIds() throws Exception {
        FileSchema schema;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            schema = reader.getFileSchema();
        }

        assertThat(fieldIds(footerSchema(write(schema)))).isEqualTo(FIXTURE_FIELD_IDS);
    }

    /// Each declaration scope sets the field id of the field it declares; the builder gives
    /// the same field ids PyArrow wrote to the fixture.
    @Test
    void builderDeclaresAFieldIdInEveryScope() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED, col -> col.fieldId(1))
                .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                        col -> col.logicalType(LogicalType.string()).fieldId(2))
                .struct("location", RepetitionType.OPTIONAL, location -> location
                        .fieldId(3)
                        .addColumn("lat", PhysicalType.DOUBLE, RepetitionType.REQUIRED, col -> col.fieldId(4))
                        .addColumn("lon", PhysicalType.DOUBLE, RepetitionType.REQUIRED, col -> col.fieldId(5)))
                .list("tags", RepetitionType.OPTIONAL, element -> element
                        .groupFieldId(6)
                        .primitive(PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                                col -> col.logicalType(LogicalType.string()).fieldId(7)))
                .map("props", RepetitionType.OPTIONAL, PhysicalType.BYTE_ARRAY,
                        key -> key.logicalType(LogicalType.string()).fieldId(9),
                        value -> value
                                .groupFieldId(8)
                                .primitive(PhysicalType.INT64, RepetitionType.OPTIONAL, col -> col.fieldId(10)))
                .build();

        assertThat(fieldIds(schema.toSchemaElements())).isEqualTo(FIXTURE_FIELD_IDS);
        assertThat(fieldIds(footerSchema(write(schema)))).isEqualTo(FIXTURE_FIELD_IDS);
    }

    /// The field id of a nested group is set in the scope that declares the group's content.
    @Test
    void builderDeclaresFieldIdsOfNestedGroups() {
        FileSchema schema = FileSchema.builder("schema")
                .list("matrix", RepetitionType.OPTIONAL, outer -> outer
                        .groupFieldId(1)
                        .list(RepetitionType.OPTIONAL, inner -> inner
                                .groupFieldId(2)
                                .struct(RepetitionType.OPTIONAL, cell -> cell
                                        .fieldId(3)
                                        .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED,
                                                col -> col.fieldId(4)))))
                .build();

        // matrix, list, element (inner list), list, element (struct), v
        assertThat(fieldIds(schema.toSchemaElements())).isEqualTo(Arrays.asList(null, 1, null, 2, null, 3, 4));
    }

    @Test
    void schemaWithoutFieldIdsHasNone() {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();

        assertThat(schema.getColumn("id").fieldId()).isNull();
        assertThat(schema.getField("id").fieldId()).isNull();
        assertThat(fieldIds(schema.toSchemaElements())).isEqualTo(Arrays.asList(null, null));
    }

    @Test
    void toStringShowsTheFieldIds() {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED, col -> col.fieldId(1))
                .struct("s", RepetitionType.OPTIONAL, s -> s
                        .fieldId(2)
                        .addColumn("x", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                                col -> col.logicalType(LogicalType.string()).fieldId(3)))
                .build();

        assertThat(schema.toString()).isEqualTo("""
                message schema {
                  required int64 id = 1;
                  optional group s = 2 {
                    optional byte_array x (STRING) = 3;
                  }
                }""");
        assertThat(schema.getColumn("s.x").toString()).isEqualTo("optional byte_array x (STRING) = 3;");
    }

    @Test
    void columnAttributeCannotBeSetTwice() {
        assertThatThrownBy(() -> FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED, col -> col.fieldId(1).fieldId(2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The field id of 'id' is already set to 1");
        assertThatThrownBy(() -> FileSchema.builder("schema")
                .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                        col -> col.logicalType(LogicalType.string()).logicalType(LogicalType.json())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The logical type of 'name' is already set to STRING");
        assertThatThrownBy(() -> FileSchema.builder("schema")
                .addColumn("md5", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED,
                        col -> col.typeLength(16).typeLength(8)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The type length of 'md5' is already set to 16");
    }

    /// A `null` annotation counts as set, so a second annotation is refused.
    @Test
    void nullLogicalTypeCannotBeOverwritten() {
        assertThatThrownBy(() -> FileSchema.builder("schema")
                .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                        col -> col.logicalType(null).logicalType(LogicalType.string())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The logical type of 'name' is already set to null");
    }

    @Test
    void groupFieldIdCannotBeSetTwice() {
        assertThatThrownBy(() -> FileSchema.builder("schema")
                .struct("s", RepetitionType.OPTIONAL, s -> s
                        .fieldId(1)
                        .fieldId(2)
                        .addColumn("x", PhysicalType.INT32, RepetitionType.REQUIRED)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The field id of 's' is already set to 1");
        assertThatThrownBy(() -> FileSchema.builder("schema")
                .list("tags", RepetitionType.OPTIONAL, element -> element
                        .groupFieldId(1)
                        .groupFieldId(2)
                        .primitive(PhysicalType.INT32, RepetitionType.REQUIRED)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The field id of 'tags' is already set to 1");
        assertThatThrownBy(() -> FileSchema.builder("schema")
                .map("props", RepetitionType.OPTIONAL, PhysicalType.INT32, value -> value
                        .groupFieldId(1)
                        .groupFieldId(2)
                        .primitive(PhysicalType.INT32, RepetitionType.REQUIRED)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The field id of 'props' is already set to 1");
    }

    /// The nested `map` and `list` verbs of [FileSchema.StructBuilder] and
    /// [FileSchema.ElementBuilder] set the field ids of the group, the key and the value.
    @Test
    void builderDeclaresFieldIdsOfNestedMapsAndLists() {
        FileSchema schema = FileSchema.builder("schema")
                .struct("s", RepetitionType.OPTIONAL, s -> s
                        .fieldId(1)
                        .map("m", RepetitionType.OPTIONAL, PhysicalType.BYTE_ARRAY,
                                key -> key.logicalType(LogicalType.string()).fieldId(3),
                                value -> value
                                        .groupFieldId(2)
                                        .primitive(PhysicalType.INT32, RepetitionType.OPTIONAL,
                                                col -> col.fieldId(4)))
                        .list("l", RepetitionType.OPTIONAL, element -> element
                                .groupFieldId(5)
                                .map(RepetitionType.OPTIONAL, PhysicalType.INT64, key -> key.fieldId(7),
                                        value -> value
                                                .groupFieldId(6)
                                                .primitive(PhysicalType.INT32, RepetitionType.OPTIONAL,
                                                        col -> col.fieldId(8)))))
                .build();

        // root, s, m, key_value, key, value, l, list, element (map), key_value, key, value
        assertThat(fieldIds(schema.toSchemaElements()))
                .isEqualTo(Arrays.asList(null, 1, 2, null, 3, 4, 5, null, 6, null, 7, 8));
    }

    /// A `FILE` group takes a field id; its children have none.
    @Test
    void builderDeclaresTheFieldIdOfAFileGroup() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .file("top", RepetitionType.OPTIONAL, 1)
                .struct("s", RepetitionType.OPTIONAL, s -> s
                        .file("nested", RepetitionType.OPTIONAL, 2))
                .list("files", RepetitionType.OPTIONAL, element -> element
                        .file(RepetitionType.OPTIONAL, 3))
                .build();

        // root; top and its children; s, nested and its children; files, list, element and its children
        List<Integer> expected = Arrays.asList(null,
                1, null, null, null, null, null, null,
                null, 2, null, null, null, null, null, null,
                null, null, 3, null, null, null, null, null, null);

        assertThat(fieldIds(schema.toSchemaElements())).isEqualTo(expected);
        assertThat(fieldIds(footerSchema(write(schema)))).isEqualTo(expected);
        assertThat(schema.getField("top").fieldId()).isEqualTo(1);
    }

    @Test
    void nullLogicalTypeLeavesTheColumnUnannotated() {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED, col -> col.logicalType(null).fieldId(1))
                .build();

        assertThat(schema.getColumn("id").logicalType()).isNull();
        assertThat(schema.getColumn("id").fieldId()).isEqualTo(1);
    }

    /// The constructors without a field id give a `null` field id.
    @Test
    void constructorsWithoutAFieldIdHaveNone() {
        SchemaNode.PrimitiveNode leaf = new SchemaNode.PrimitiveNode("x", PhysicalType.INT32,
                RepetitionType.REQUIRED, null, 0, 0, 0);
        SchemaNode.GroupNode group = new SchemaNode.GroupNode("g", RepetitionType.REQUIRED, null, null,
                List.of(leaf), 0, 0);
        ColumnSchema column = new ColumnSchema(FieldPath.of("x"), PhysicalType.INT32, RepetitionType.REQUIRED,
                null, 0, 0, 0, null);

        assertThat(leaf.fieldId()).isNull();
        assertThat(group.fieldId()).isNull();
        assertThat(column.fieldId()).isNull();
    }

    private static InputFile write(FileSchema schema) throws Exception {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.rowWriter();
        }
        return InputFile.of(out.buffer());
    }

    private static List<SchemaElement> footerSchema(InputFile file) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(file)) {
            return reader.getFileMetaData().schema();
        }
    }

    private static List<Integer> fieldIds(List<SchemaElement> elements) {
        List<Integer> ids = new ArrayList<>(elements.size());
        for (SchemaElement element : elements) {
            ids.add(element.fieldId());
        }
        return ids;
    }
}
