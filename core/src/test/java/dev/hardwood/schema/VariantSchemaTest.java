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
        return new SchemaElement(name, type, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
    }

    @Test
    void groupNodeRoundTripsVariantAnnotation() {
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement variant = new SchemaElement("v", null, null, RepetitionType.OPTIONAL, 2,
                null, null, null, null, new LogicalType.VariantType(1));
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
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement variant = new SchemaElement("v", null, null, RepetitionType.OPTIONAL, 3,
                null, null, null, null, new LogicalType.VariantType(1));
        SchemaElement metadata = primChild("metadata", PhysicalType.BYTE_ARRAY);
        SchemaElement value = primChild("value", PhysicalType.BYTE_ARRAY);
        SchemaElement typedValue = new SchemaElement("typed_value", PhysicalType.INT64, null,
                RepetitionType.OPTIONAL, null, null, null, null, null, null);

        FileSchema schema = FileSchema.fromSchemaElements(List.of(root, variant, metadata, value, typedValue));
        SchemaNode.GroupNode variantNode = (SchemaNode.GroupNode) schema.getRootNode().children().get(0);
        assertThat(variantNode.isVariant()).isTrue();
        assertThat(variantNode.children()).hasSize(3);
    }

    @Test
    void variantGroupMissingValueChildIsRejected() {
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement variant = new SchemaElement("v", null, null, RepetitionType.OPTIONAL, 1,
                null, null, null, null, new LogicalType.VariantType(1));
        SchemaElement metadata = primChild("metadata", PhysicalType.BYTE_ARRAY);

        assertThatThrownBy(() -> FileSchema.fromSchemaElements(List.of(root, variant, metadata)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Variant group");
    }

    @Test
    void variantGroupWithWrongChildNameIsRejected() {
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement variant = new SchemaElement("v", null, null, RepetitionType.OPTIONAL, 2,
                null, null, null, null, new LogicalType.VariantType(1));
        SchemaElement metadata = primChild("metadata", PhysicalType.BYTE_ARRAY);
        SchemaElement misnamed = primChild("payload", PhysicalType.BYTE_ARRAY);

        assertThatThrownBy(() -> FileSchema.fromSchemaElements(List.of(root, variant, metadata, misnamed)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Variant group");
    }

    @Test
    void variantGroupWithWrongPhysicalTypeIsRejected() {
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement variant = new SchemaElement("v", null, null, RepetitionType.OPTIONAL, 2,
                null, null, null, null, new LogicalType.VariantType(1));
        SchemaElement metadata = primChild("metadata", PhysicalType.BYTE_ARRAY);
        SchemaElement wrongType = primChild("value", PhysicalType.INT32);

        assertThatThrownBy(() -> FileSchema.fromSchemaElements(List.of(root, variant, metadata, wrongType)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("BYTE_ARRAY");
    }

    @Test
    void variantTypeRejectsZeroSpecVersion() {
        assertThatThrownBy(() -> new LogicalType.VariantType(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void plainStructUnchangedByNewAnnotation() {
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement addr = new SchemaElement("addr", null, null, RepetitionType.OPTIONAL, 1,
                null, null, null, null, null);
        SchemaElement zip = primChild("zip", PhysicalType.INT32);

        FileSchema schema = FileSchema.fromSchemaElements(List.of(root, addr, zip));
        SchemaNode.GroupNode addrNode = (SchemaNode.GroupNode) schema.getRootNode().children().get(0);
        assertThat(addrNode.isStruct()).isTrue();
        assertThat(addrNode.isVariant()).isFalse();
        assertThat(addrNode.logicalType()).isNull();
    }
}
