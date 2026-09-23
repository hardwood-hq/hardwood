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

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.SchemaIncompatibleException;
import dev.hardwood.schema.SchemaNode;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Malformed nested shredding shapes discovered while planning a file are schema failures.
class ShredLevelTest {

    @Test
    void unexpectedNestedShreddedChildIsSchemaIncompatible() {
        SchemaNode.PrimitiveNode unexpected = new SchemaNode.PrimitiveNode(
                "unexpected", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, null, 0, 1, 0);
        SchemaNode.GroupNode group = new SchemaNode.GroupNode(
                "field", RepetitionType.OPTIONAL, null, null, List.of(unexpected), 1, 0);

        assertThatThrownBy(() -> ShredLevel.buildNested(group, null))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessage("Unexpected shredded-Variant child 'unexpected' in group 'field'");
    }
}
