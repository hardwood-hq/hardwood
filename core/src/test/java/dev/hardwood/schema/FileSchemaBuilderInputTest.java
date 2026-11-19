/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.schema;

import java.util.List;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The input [FileSchema.Builder] and the builders its lambdas receive refuse where they are
/// called: `null` arguments, empty and repeated sibling names, and `REPEATED` fields, each named
/// by its dotted path.
class FileSchemaBuilderInputTest {

    // ==================== null arguments ====================

    @Test
    void nullSchemaNameIsRejected() {
        assertThatThrownBy(() -> FileSchema.builder(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The schema name must not be null");
    }

    @Test
    void nullFieldNameIsRejected() {
        assertThatThrownBy(() -> FileSchema.builder("s").primitive(null, PhysicalType.INT32, RepetitionType.REQUIRED))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("A field name in schema s must not be null");
    }

    @Test
    void nullPhysicalTypeIsRejected() {
        assertThatThrownBy(() -> FileSchema.builder("s")
                .struct("p", RepetitionType.OPTIONAL, p -> p.primitive("x", null, RepetitionType.REQUIRED)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The physical type of p.x must not be null");
    }

    @Test
    void nullRepetitionIsRejected() {
        assertThatThrownBy(() -> FileSchema.builder("s").primitive("a", PhysicalType.INT32, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The repetition of a must not be null");
    }

    @Test
    void nullMapKeyTypeIsRejected() {
        assertThatThrownBy(() -> FileSchema.builder("s").map("m", RepetitionType.OPTIONAL, null,
                value -> value.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The physical type of m.key_value.key must not be null");
    }

    /// A leaf is unannotated when its lambda does not set a logical type, so `null` is refused.
    @Test
    void nullLogicalTypeIsRejected() {
        assertThatThrownBy(() -> FileSchema.builder("s").struct("p", RepetitionType.OPTIONAL, p -> p
                .primitive("x", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, col -> col.logicalType(null))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The logical type of p.x must not be null");
    }

    @Test
    void nullLambdasAreRejected() {
        assertThatThrownBy(() -> FileSchema.builder("s").struct("p", RepetitionType.OPTIONAL, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The filler of p must not be null");
        assertThatThrownBy(() -> FileSchema.builder("s")
                .list("l", RepetitionType.OPTIONAL, (Consumer<FileSchema.ElementBuilder>) null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The element lambda of l must not be null");
        assertThatThrownBy(() -> FileSchema.builder("s")
                .primitive("a", PhysicalType.INT32, RepetitionType.REQUIRED, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The attribute lambda of a must not be null");
        assertThatThrownBy(() -> FileSchema.builder("s").file("f", RepetitionType.OPTIONAL, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The group lambda of f must not be null");
        assertThatThrownBy(() -> FileSchema.builder("s", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The fields lambda of schema s must not be null");
    }

    // ==================== sibling names ====================

    @Test
    void emptyFieldNameIsRejected() {
        assertThatThrownBy(() -> FileSchema.builder("s").primitive("", PhysicalType.INT32, RepetitionType.REQUIRED))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("A field name in schema s must not be empty");
    }

    @Test
    void repeatedSiblingNameIsRejected() {
        assertThatThrownBy(() -> FileSchema.builder("s")
                .primitive("a", PhysicalType.INT32, RepetitionType.REQUIRED)
                .list("a", RepetitionType.OPTIONAL,
                        element -> element.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field 'a' is already declared in schema s");
        assertThatThrownBy(() -> FileSchema.builder("s")
                .struct("p", RepetitionType.OPTIONAL, p -> p
                        .primitive("x", PhysicalType.INT32, RepetitionType.REQUIRED)
                        .primitive("x", PhysicalType.INT64, RepetitionType.OPTIONAL)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field 'x' is already declared in struct p");
    }

    @Test
    void oneNameInDifferentGroupsIsAccepted() {
        FileSchema schema = FileSchema.builder("s")
                .primitive("x", PhysicalType.INT32, RepetitionType.REQUIRED)
                .struct("p", RepetitionType.OPTIONAL, p -> p.primitive("x", PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();

        assertThat(schema.getColumn("x").columnIndex()).isZero();
        assertThat(schema.getColumn("p.x").columnIndex()).isEqualTo(1);
    }

    // ==================== REPEATED ====================

    @Test
    void repeatedFieldIsRejectedWhereItIsDeclared() {
        assertThatThrownBy(() -> FileSchema.builder("s").primitive("a", PhysicalType.INT32, RepetitionType.REPEATED))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field a is REPEATED; the builder declares a repeated field with list(...) or map(...)");
        assertThatThrownBy(() -> FileSchema.builder("s").list("l", RepetitionType.OPTIONAL,
                element -> element.primitive(PhysicalType.INT32, RepetitionType.REPEATED)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field l.list.element is REPEATED; the builder declares a repeated field with list(...) "
                        + "or map(...)");
        assertThatThrownBy(() -> FileSchema.builder("s").map("m", RepetitionType.OPTIONAL, PhysicalType.INT32,
                value -> value.struct(RepetitionType.REPEATED,
                        v -> v.primitive("x", PhysicalType.INT32, RepetitionType.REQUIRED))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field m.key_value.value is REPEATED; the builder declares a repeated field with "
                        + "list(...) or map(...)");
    }

    // ==================== paths in messages ====================

    @Test
    void nestedFieldsAreNamedByTheirPath() {
        assertThatThrownBy(() -> FileSchema.builder("s").struct("p", RepetitionType.OPTIONAL, p -> p
                .primitive("x", PhysicalType.INT32, RepetitionType.REQUIRED,
                        col -> col.logicalType(LogicalType.string()))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("STRING annotates a BYTE_ARRAY column, not INT32 (column p.x)");
        assertThatThrownBy(() -> FileSchema.builder("s").list("l", RepetitionType.OPTIONAL,
                element -> element.struct(RepetitionType.OPTIONAL, cell -> { })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Struct l.list.element must have at least one field");
        assertThatThrownBy(() -> FileSchema.builder("s").list("l", RepetitionType.OPTIONAL, element -> {
            element.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL);
            element.primitive(PhysicalType.INT64, RepetitionType.OPTIONAL);
        }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The element of l is already declared");
        assertThatThrownBy(() -> FileSchema.builder("s").struct("p", RepetitionType.OPTIONAL, p -> p
                .map("m", RepetitionType.OPTIONAL, PhysicalType.INT32, value -> { })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Must declare the value of p.m");
        assertThatThrownBy(() -> FileSchema.builder("s").struct("p", RepetitionType.OPTIONAL, p -> p
                .primitive("x", PhysicalType.INT32, RepetitionType.REQUIRED, col -> col.fieldId(1).fieldId(2))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The field id of 'p.x' is already set to 1");
    }

    // ==================== root declared as a struct ====================

    /// A source schema model for the converter below: a field is a leaf or a struct of fields.
    private record SourceField(String name, List<SourceField> fields) {
        static SourceField leaf(String name) {
            return new SourceField(name, List.of());
        }
    }

    /// One method declares the root's fields and every nested struct's.
    private static void addFields(FileSchema.StructBuilder target, List<SourceField> fields) {
        for (SourceField field : fields) {
            if (field.fields().isEmpty()) {
                target.primitive(field.name(), PhysicalType.INT64, RepetitionType.OPTIONAL);
            }
            else {
                target.struct(field.name(), RepetitionType.OPTIONAL, nested -> addFields(nested, field.fields()));
            }
        }
    }

    @Test
    void rootFieldsAreDeclaredOnAStructBuilder() {
        List<SourceField> source = List.of(
                SourceField.leaf("id"),
                new SourceField("location", List.of(SourceField.leaf("lat"), SourceField.leaf("lon"))));

        FileSchema schema = FileSchema.builder("event", root -> addFields(root, source))
                .primitive("extra", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();

        assertThat(schema.toString()).isEqualTo("""
                message event {
                  optional int64 id;
                  optional group location {
                    optional int64 lat;
                    optional int64 lon;
                  }
                  required int32 extra;
                }""");
    }

    /// The root has no field id, so the root's `StructBuilder` refuses one rather than drop it.
    @Test
    void rootFieldIdIsRejected() {
        assertThatThrownBy(() -> FileSchema.builder("event", root -> root.fieldId(7)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The root of schema event has no field id");
    }
}
