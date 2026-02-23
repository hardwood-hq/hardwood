/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.HashMap;
import java.util.Map;

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

    static TopLevelFieldMap.FieldDesc build(SchemaNode node, ProjectedSchema projectedSchema) {
        return switch (node) {
            case SchemaNode.PrimitiveNode prim -> {
                int projCol = projectedSchema.toProjectedIndex(prim.columnIndex());
                yield projCol < 0 ? null : new TopLevelFieldMap.FieldDesc.Primitive(projCol, prim);
            }
            case SchemaNode.GroupNode group -> {
                if (group.isList()) {
                    yield buildListDesc(group, projectedSchema);
                } else if (group.isMap()) {
                    yield buildMapDesc(group, projectedSchema);
                } else {
                    yield buildStructDesc(group, projectedSchema);
                }
            }
        };
    }

    static TopLevelFieldMap.FieldDesc.Struct buildStructDesc(SchemaNode.GroupNode group,
                                                              ProjectedSchema projectedSchema) {
        Map<String, TopLevelFieldMap.FieldDesc> children = new HashMap<>();
        for (SchemaNode child : group.children()) {
            TopLevelFieldMap.FieldDesc childDesc = build(child, projectedSchema);
            if (childDesc != null) {
                children.put(child.name(), childDesc);
            }
        }
        return new TopLevelFieldMap.FieldDesc.Struct(group, children);
    }

    static TopLevelFieldMap.FieldDesc.ListOf buildListDesc(SchemaNode.GroupNode listGroup,
                                                            ProjectedSchema projectedSchema) {
        SchemaNode elementSchema = listGroup.getListElement();
        int nullDefLevel = listGroup.maxDefinitionLevel();
        SchemaNode innerRepeated = listGroup.children().get(0);
        int elementDefLevel = innerRepeated.maxDefinitionLevel();

        int[] range = {Integer.MAX_VALUE, -1};
        collectLeafRange(elementSchema, projectedSchema, range);
        int firstProjCol = range[0];
        int lastProjCol = range[1];
        int leafCount = (firstProjCol <= lastProjCol) ? (lastProjCol - firstProjCol + 1) : 0;

        return new TopLevelFieldMap.FieldDesc.ListOf(listGroup, elementSchema,
                firstProjCol, leafCount, nullDefLevel, elementDefLevel);
    }

    static TopLevelFieldMap.FieldDesc.MapOf buildMapDesc(SchemaNode.GroupNode mapGroup,
                                                          ProjectedSchema projectedSchema) {
        int nullDefLevel = mapGroup.maxDefinitionLevel();
        SchemaNode.GroupNode keyValueGroup = (SchemaNode.GroupNode) mapGroup.children().get(0);
        int entryDefLevel = keyValueGroup.maxDefinitionLevel();

        SchemaNode keyNode = keyValueGroup.children().get(0);
        SchemaNode valueNode = keyValueGroup.children().get(1);

        int keyProjCol = findFirstLeafProjCol(keyNode, projectedSchema);
        int valueProjCol = findFirstLeafProjCol(valueNode, projectedSchema);
        return new TopLevelFieldMap.FieldDesc.MapOf(mapGroup, keyProjCol, valueProjCol,
                nullDefLevel, entryDefLevel);
    }

    private static void collectLeafRange(SchemaNode node, ProjectedSchema projectedSchema, int[] range) {
        switch (node) {
            case SchemaNode.PrimitiveNode prim -> {
                int projCol = projectedSchema.toProjectedIndex(prim.columnIndex());
                if (projCol >= 0) {
                    range[0] = Math.min(range[0], projCol);
                    range[1] = Math.max(range[1], projCol);
                }
            }
            case SchemaNode.GroupNode group -> {
                for (SchemaNode child : group.children()) {
                    collectLeafRange(child, projectedSchema, range);
                }
            }
        }
    }

    private static int findFirstLeafProjCol(SchemaNode node, ProjectedSchema projectedSchema) {
        return switch (node) {
            case SchemaNode.PrimitiveNode prim -> projectedSchema.toProjectedIndex(prim.columnIndex());
            case SchemaNode.GroupNode group -> {
                for (SchemaNode child : group.children()) {
                    int result = findFirstLeafProjCol(child, projectedSchema);
                    if (result >= 0) {
                        yield result;
                    }
                }
                yield -1;
            }
        };
    }
}
