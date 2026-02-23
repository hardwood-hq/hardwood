/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import dev.hardwood.schema.ProjectedSchema;
import dev.hardwood.schema.SchemaNode;

/**
 * Builds {@link TopLevelFieldMap.FieldDesc} descriptors from schema nodes at runtime.
 * Used by flyweight implementations that need to create descriptors for nested types
 * within lists and maps.
 */
final class DescriptorBuilder {

    private DescriptorBuilder() {
    }

    static TopLevelFieldMap.FieldDesc.Struct buildStructDesc(SchemaNode.GroupNode group,
                                                              ProjectedSchema projectedSchema) {
        return TopLevelFieldMap.buildStructDesc(group, null, projectedSchema);
    }

    static TopLevelFieldMap.FieldDesc.ListOf buildListDesc(SchemaNode.GroupNode listGroup,
                                                            ProjectedSchema projectedSchema) {
        return TopLevelFieldMap.buildListDesc(listGroup, null, projectedSchema);
    }

    static TopLevelFieldMap.FieldDesc.MapOf buildMapDesc(SchemaNode.GroupNode mapGroup,
                                                          ProjectedSchema projectedSchema) {
        return TopLevelFieldMap.buildMapDesc(mapGroup, null, projectedSchema);
    }
}
