/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.schema;

import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.metadata.SchemaElements;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Round-trips the `VARIANT` logical-type annotation through schema construction
/// and exercises the Variant group shape validation in [FileSchema].
class VariantSchemaTest {

    private static SchemaElement primChild(String name, PhysicalType type) {
        return SchemaElements.primitive(name, type, RepetitionType.REQUIRED);
    }

    @Test
    void groupNodeRoundTripsVariantAnnotation() {
        SchemaElement root = SchemaElements.root("root", 1);
        SchemaElement variant = SchemaElements.group("v", RepetitionType.OPTIONAL, 2, LogicalType.variant(1));
        SchemaElement metadata = primChild("metadata", PhysicalType.BYTE_ARRAY);
        SchemaElement value = primChild("value", PhysicalType.BYTE_ARRAY);

        FileSchema schema = FileSchema.fromSchemaElements(List.of(root, variant, metadata, value));
        SchemaNode.GroupNode variantNode = (SchemaNode.GroupNode) schema.getRootNode().children().get(0);
        assertThat(variantNode.isVariant()).isTrue();
        assertThat(variantNode.logicalType()).isInstanceOf(LogicalType.VariantType.class);
        assertThat(((LogicalType.VariantType) variantNode.logicalType()).specVersion()).isEqualTo(1);
        assertThat(variantNode.children()).hasSize(2);
    }

    @Test
    void variantGroupWithTypedValueIsAccepted() {
        SchemaElement root = SchemaElements.root("root", 1);
        SchemaElement variant = SchemaElements.group("v", RepetitionType.OPTIONAL, 3, LogicalType.variant(1));
        SchemaElement metadata = primChild("metadata", PhysicalType.BYTE_ARRAY);
        SchemaElement value = primChild("value", PhysicalType.BYTE_ARRAY);
        SchemaElement typedValue = SchemaElements.primitive("typed_value", PhysicalType.INT64, RepetitionType.OPTIONAL);

        FileSchema schema = FileSchema.fromSchemaElements(List.of(root, variant, metadata, value, typedValue));
        SchemaNode.GroupNode variantNode = (SchemaNode.GroupNode) schema.getRootNode().children().get(0);
        assertThat(variantNode.isVariant()).isTrue();
        assertThat(variantNode.children()).hasSize(3);
    }

    @Test
    void variantGroupMissingValueChildIsRejected() {
        SchemaElement root = SchemaElements.root("root", 1);
        SchemaElement variant = SchemaElements.group("v", RepetitionType.OPTIONAL, 1, LogicalType.variant(1));
        SchemaElement metadata = primChild("metadata", PhysicalType.BYTE_ARRAY);

        assertThatThrownBy(() -> FileSchema.fromSchemaElements(List.of(root, variant, metadata)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Variant group 'v' must have 2 or 3 children (metadata, value[, typed_value]), "
                         + "found: 1");
    }

    @Test
    void variantGroupWithWrongChildNameIsRejected() {
        SchemaElement root = SchemaElements.root("root", 1);
        SchemaElement variant = SchemaElements.group("v", RepetitionType.OPTIONAL, 2, LogicalType.variant(1));
        SchemaElement metadata = primChild("metadata", PhysicalType.BYTE_ARRAY);
        SchemaElement misnamed = primChild("payload", PhysicalType.BYTE_ARRAY);

        assertThatThrownBy(() -> FileSchema.fromSchemaElements(List.of(root, variant, metadata, misnamed)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Variant group 'v' expected child 'value', found: payload");
    }

    @Test
    void variantGroupWithWrongPhysicalTypeIsRejected() {
        SchemaElement root = SchemaElements.root("root", 1);
        SchemaElement variant = SchemaElements.group("v", RepetitionType.OPTIONAL, 2, LogicalType.variant(1));
        SchemaElement metadata = primChild("metadata", PhysicalType.BYTE_ARRAY);
        SchemaElement wrongType = primChild("value", PhysicalType.INT32);

        assertThatThrownBy(() -> FileSchema.fromSchemaElements(List.of(root, variant, metadata, wrongType)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Variant group 'v' child 'value' must be a BYTE_ARRAY primitive");
    }

    @Test
    void variantTypeRejectsZeroSpecVersion() {
        assertThatThrownBy(() -> LogicalType.variant(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void plainStructUnchangedByNewAnnotation() {
        SchemaElement root = SchemaElements.root("root", 1);
        SchemaElement addr = SchemaElements.group("addr", RepetitionType.OPTIONAL, 1);
        SchemaElement zip = primChild("zip", PhysicalType.INT32);

        FileSchema schema = FileSchema.fromSchemaElements(List.of(root, addr, zip));
        SchemaNode.GroupNode addrNode = (SchemaNode.GroupNode) schema.getRootNode().children().get(0);
        assertThat(addrNode.isStruct()).isTrue();
        assertThat(addrNode.isVariant()).isFalse();
        assertThat(addrNode.logicalType()).isNull();
    }

    @Test
    void builderExpandsVariantIntoMetadataAndValueChildren() {
        FileSchema schema = FileSchema.builder("schema")
                .variant("payload", RepetitionType.OPTIONAL)
                .build();

        SchemaNode.GroupNode payload = (SchemaNode.GroupNode) schema.getRootNode().children().get(0);
        assertThat(payload.name()).isEqualTo("payload");
        assertThat(payload.isVariant()).isTrue();
        assertThat(payload.repetitionType()).isEqualTo(RepetitionType.OPTIONAL);
        assertThat(payload.children()).hasSize(2);
        assertThat(payload.children().get(0).name()).isEqualTo("metadata");
        assertThat(payload.children().get(1).name()).isEqualTo("value");
    }

    @Test
    void builderDeclaresAVariantInsideAStruct() {
        FileSchema schema = FileSchema.builder("schema")
                .struct("meta", RepetitionType.OPTIONAL, meta -> meta
                        .variant("extra", RepetitionType.OPTIONAL))
                .build();

        SchemaNode.GroupNode meta = (SchemaNode.GroupNode) schema.getRootNode().children().get(0);
        SchemaNode.GroupNode extra = (SchemaNode.GroupNode) meta.children().get(0);
        assertThat(extra.name()).isEqualTo("extra");
        assertThat(extra.isVariant()).isTrue();
        assertThat(extra.children()).hasSize(2);
        assertThat(extra.children().get(0).name()).isEqualTo("metadata");
        assertThat(extra.children().get(1).name()).isEqualTo("value");
    }

    @Test
    void builderDeclaresAVariantAsAListElement() {
        FileSchema schema = FileSchema.builder("schema")
                .list("events", RepetitionType.OPTIONAL, element -> element
                        .variant(RepetitionType.OPTIONAL))
                .build();

        SchemaNode.GroupNode list = (SchemaNode.GroupNode) schema.getRootNode().children().get(0);
        SchemaNode.GroupNode repeated = (SchemaNode.GroupNode) list.children().get(0);
        SchemaNode.GroupNode element = (SchemaNode.GroupNode) repeated.children().get(0);
        assertThat(element.isVariant()).isTrue();
        assertThat(element.children()).hasSize(2);
        assertThat(element.children().get(0).name()).isEqualTo("metadata");
        assertThat(element.children().get(1).name()).isEqualTo("value");
    }

    @Test
    void repeatedVariantIsRejectedWhereItIsDeclared() {
        assertThatThrownBy(() -> FileSchema.builder("schema").variant("v", RepetitionType.REPEATED))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field v is REPEATED; the builder declares a repeated field with list(...) or map(...)");
        assertThatThrownBy(() -> FileSchema.builder("schema").list("l", RepetitionType.OPTIONAL,
                element -> element.variant(RepetitionType.REPEATED)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field l.list.element is REPEATED; the builder declares a repeated field with list(...) "
                        + "or map(...)");
    }
}
