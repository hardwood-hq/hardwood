/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.avro.internal;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

import static dev.hardwood.metadata.SchemaElement.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AvroNamesTest {

    @Test
    void resolvesSanitizingCollisionsDeterministically() {
        FileSchema schema = schemaWithChildren("root", "a_b", "a-b", "a.b", "a_b_2");
        AvroNames names = AvroNames.forSchema(schema);

        assertThat(locals(names, schema.getRootNode().children()))
                .containsExactly("a_b", "a_b_3", "a_b_4", "a_b_2");
    }
    @Test
    void rejectsDuplicateRawNames() {
        FileSchema schema = schemaWithChildren("root", "dup", "dup");

        assertThatThrownBy(() -> AvroNames.forSchema(schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dup");
    }

    @Test
    void preservesQualifiedRootAndBuildsDescendantNamespace() {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElement.root("acme.row", 1),
                SchemaElement.group("acme", RepetitionType.OPTIONAL, 1),
                SchemaElement.group("row", RepetitionType.OPTIONAL, 1),
                SchemaElement.primitive("value", PhysicalType.INT32, RepetitionType.REQUIRED)));
        AvroNames names = AvroNames.forSchema(schema);
        SchemaNode.GroupNode root = schema.getRootNode();
        SchemaNode.GroupNode child = (SchemaNode.GroupNode) root.children().getFirst();
        SchemaNode.GroupNode grandchild = (SchemaNode.GroupNode) child.children().getFirst();

        assertThat(names.typeName(root).fullName()).isEqualTo("acme.row");
        assertThat(names.typeName(child).fullName()).isEqualTo("acme.row.acme");
        assertThat(names.typeName(grandchild).fullName()).isEqualTo("acme.row.acme.row");
    }

    @Test
    void sanitizesQualifiedRootSegments() {
        FileSchema schema = schemaWithChildren("1acme.row", "value");
        AvroNames.TypeName root = AvroNames.forSchema(schema).typeName(schema.getRootNode());

        assertThat(root.name()).isEqualTo("row");
        assertThat(root.namespace()).isEqualTo("_1acme");
        assertThat(root.fullName()).isEqualTo("_1acme.row");
    }

    private static List<String> locals(AvroNames names, List<SchemaNode> nodes) {
        return nodes.stream().map(names::fieldName).toList();
    }

    private static FileSchema schemaWithChildren(String rootName, String... names) {
        SchemaElement rootElement = root(rootName, names.length);
        List<SchemaElement> elements = new ArrayList<>();
        elements.add(rootElement);
        for (String name : names) {
            elements.add(primitive(name, PhysicalType.INT32, RepetitionType.REQUIRED));
        }
        return FileSchema.fromSchemaElements(elements);
    }
}
