/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.variant;

import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.SchemaNode;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A malformed shredding shape in a file's schema is a read failure.
class ShredLevelTest {

    @Test
    void unexpectedNestedShreddedChildIsAReadFailure() {
        SchemaNode.GroupNode field = group("field", List.of(binary("unexpected")));

        assertThatThrownBy(() -> ShredLevel.buildNested(field, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Unexpected shredded-Variant child 'unexpected' in group 'field'");
    }

    @Test
    void nestedGroupWithNeitherValueNorTypedValueIsAReadFailure() {
        SchemaNode.GroupNode field = group("field", List.of());

        assertThatThrownBy(() -> ShredLevel.buildNested(field, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Shredded-Variant group 'field' must contain at least one of 'value' or 'typed_value'");
    }

    @Test
    void valueColumnOfTheWrongPhysicalTypeIsAReadFailure() {
        SchemaNode.GroupNode field = group("field", List.of(
                new SchemaNode.PrimitiveNode("value", PhysicalType.INT32, RepetitionType.OPTIONAL, null, 0, 1, 0)));

        assertThatThrownBy(() -> ShredLevel.buildNested(field, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Shredded-Variant 'value' column must be BYTE_ARRAY, got INT32");
    }

    @Test
    void valueGroupIsAReadFailure() {
        SchemaNode.GroupNode field = group("field", List.of(group("value", List.of(binary("inner")))));

        assertThatThrownBy(() -> ShredLevel.buildNested(field, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Shredded-Variant 'value' child must be a primitive");
    }

    @Test
    void arrayTypedValueWithAPrimitiveElementIsAReadFailure() {
        SchemaNode.PrimitiveNode element =
                new SchemaNode.PrimitiveNode("element", PhysicalType.INT32, RepetitionType.OPTIONAL, null, 0, 3, 1);
        SchemaNode.GroupNode repeated = new SchemaNode.GroupNode(
                "list", RepetitionType.REPEATED, null, null, List.of(element), 2, 1);
        SchemaNode.GroupNode typedValue = new SchemaNode.GroupNode(
                "typed_value", RepetitionType.OPTIONAL, null, LogicalType.list(), List.of(repeated), 1, 0);
        SchemaNode.GroupNode field = group("field", List.of(typedValue));

        assertThatThrownBy(() -> ShredLevel.buildNested(field, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Shredded-Variant array typed_value must have a group element, got " + element);
    }

    @Test
    void objectTypedValueWithAPrimitiveFieldIsAReadFailure() {
        SchemaNode.GroupNode typedValue = group("typed_value", List.of(binary("name")));
        SchemaNode.GroupNode field = group("field", List.of(typedValue));

        assertThatThrownBy(() -> ShredLevel.buildNested(field, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Shredded-Variant object field 'name' must be a group");
    }

    private static SchemaNode.GroupNode group(String name, List<SchemaNode> children) {
        return new SchemaNode.GroupNode(name, RepetitionType.OPTIONAL, null, null, children, 1, 0);
    }

    private static SchemaNode.PrimitiveNode binary(String name) {
        return new SchemaNode.PrimitiveNode(name, PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, null, 0, 1, 0);
    }
}
