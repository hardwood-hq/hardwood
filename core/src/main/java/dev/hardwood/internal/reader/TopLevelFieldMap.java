/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;
import dev.hardwood.schema.SchemaNode;

/**
 * Maps each top-level field in the root schema to its leaf column(s) within a projection.
 */
final class TopLevelFieldMap {

    sealed interface FieldDesc {

        record Primitive(int projectedCol, SchemaNode.PrimitiveNode schema) implements FieldDesc {}

        record Struct(SchemaNode.GroupNode schema, Map<String, FieldDesc> children) implements FieldDesc {}

        /**
         * @param nullDefLevel     def level below which the list itself is null
         * @param elementDefLevel  def level at or above which an actual element exists
         */
        record ListOf(SchemaNode.GroupNode schema, SchemaNode elementSchema,
                      int firstLeafProjCol, int leafColCount,
                      int nullDefLevel, int elementDefLevel) implements FieldDesc {}

        /**
         * @param nullDefLevel   def level below which the map itself is null
         * @param entryDefLevel  def level at or above which an actual entry exists
         */
        record MapOf(SchemaNode.GroupNode schema, int keyProjCol, int valueProjCol,
                     int nullDefLevel, int entryDefLevel) implements FieldDesc {}
    }

    private final Map<String, FieldDesc> byName;
    private final Map<Integer, FieldDesc> byOriginalIndex;

    private TopLevelFieldMap(Map<String, FieldDesc> byName, Map<Integer, FieldDesc> byOriginalIndex) {
        this.byName = byName;
        this.byOriginalIndex = byOriginalIndex;
    }

    FieldDesc getByName(String name) {
        FieldDesc desc = byName.get(name);
        if (desc == null) {
            throw new IllegalArgumentException("Field '" + name + "' not in projection");
        }
        return desc;
    }

    FieldDesc getByOriginalIndex(int originalFieldIndex) {
        return byOriginalIndex.get(originalFieldIndex);
    }

    static TopLevelFieldMap build(FileSchema schema, ProjectedSchema projectedSchema) {
        List<SchemaNode> rootChildren = schema.getRootNode().children();
        int[] projectedFieldIndices = projectedSchema.getProjectedFieldIndices();

        Map<String, FieldDesc> byName = new HashMap<>();
        Map<Integer, FieldDesc> byOriginalIndex = new HashMap<>();

        for (int projFieldIdx : projectedFieldIndices) {
            SchemaNode topLevelNode = rootChildren.get(projFieldIdx);
            FieldDesc desc = buildDesc(topLevelNode, schema, projectedSchema);
            byName.put(topLevelNode.name(), desc);
            byOriginalIndex.put(projFieldIdx, desc);
        }

        return new TopLevelFieldMap(byName, byOriginalIndex);
    }

    private static FieldDesc buildDesc(SchemaNode node, FileSchema schema, ProjectedSchema projectedSchema) {
        return switch (node) {
            case SchemaNode.PrimitiveNode prim -> {
                int projCol = projectedSchema.toProjectedIndex(prim.columnIndex());
                yield new FieldDesc.Primitive(projCol, prim);
            }
            case SchemaNode.GroupNode group -> {
                if (group.isList()) {
                    yield buildListDesc(group, schema, projectedSchema);
                }
                else if (group.isMap()) {
                    yield buildMapDesc(group, schema, projectedSchema);
                }
                else {
                    yield buildStructDesc(group, schema, projectedSchema);
                }
            }
        };
    }

    private static FieldDesc.Struct buildStructDesc(SchemaNode.GroupNode group,
                                                    FileSchema schema,
                                                    ProjectedSchema projectedSchema) {
        Map<String, FieldDesc> children = new HashMap<>();
        for (int i = 0; i < group.children().size(); i++) {
            SchemaNode child = group.children().get(i);
            FieldDesc childDesc = buildDescForChild(child, schema, projectedSchema);
            if (childDesc != null) {
                children.put(child.name(), childDesc);
            }
        }
        return new FieldDesc.Struct(group, children);
    }

    private static FieldDesc.ListOf buildListDesc(SchemaNode.GroupNode listGroup,
                                                  FileSchema schema,
                                                  ProjectedSchema projectedSchema) {
        SchemaNode elementSchema = listGroup.getListElement();

        // Compute defLevel thresholds
        int nullDefLevel = listGroup.maxDefinitionLevel();
        // The inner repeated group's def level = threshold for element existence
        SchemaNode innerRepeated = listGroup.children().get(0);
        int elementDefLevel = innerRepeated.maxDefinitionLevel();

        int[] range = new int[] { Integer.MAX_VALUE, -1 };
        collectLeafRange(elementSchema, projectedSchema, range);
        int firstProjCol = range[0];
        int lastProjCol = range[1];
        int leafCount = (firstProjCol <= lastProjCol) ? (lastProjCol - firstProjCol + 1) : 0;

        return new FieldDesc.ListOf(listGroup, elementSchema, firstProjCol, leafCount,
                nullDefLevel, elementDefLevel);
    }

    private static FieldDesc.MapOf buildMapDesc(SchemaNode.GroupNode mapGroup,
                                                FileSchema schema,
                                                ProjectedSchema projectedSchema) {
        // Compute defLevel thresholds
        int nullDefLevel = mapGroup.maxDefinitionLevel();
        // The inner repeated key_value group
        SchemaNode.GroupNode keyValueGroup = (SchemaNode.GroupNode) mapGroup.children().get(0);
        int entryDefLevel = keyValueGroup.maxDefinitionLevel();

        SchemaNode keyNode = keyValueGroup.children().get(0);
        SchemaNode valueNode = keyValueGroup.children().get(1);

        int keyProjCol = findFirstLeafProjCol(keyNode, projectedSchema);
        int valueProjCol = findFirstLeafProjCol(valueNode, projectedSchema);
        return new FieldDesc.MapOf(mapGroup, keyProjCol, valueProjCol, nullDefLevel, entryDefLevel);
    }

    private static FieldDesc buildDescForChild(SchemaNode node, FileSchema schema,
                                               ProjectedSchema projectedSchema) {
        return switch (node) {
            case SchemaNode.PrimitiveNode prim -> {
                int projCol = projectedSchema.toProjectedIndex(prim.columnIndex());
                if (projCol < 0) {
                    yield null;
                }
                yield new FieldDesc.Primitive(projCol, prim);
            }
            case SchemaNode.GroupNode group -> {
                if (group.isList()) {
                    yield buildListDesc(group, schema, projectedSchema);
                }
                else if (group.isMap()) {
                    yield buildMapDesc(group, schema, projectedSchema);
                }
                else {
                    yield buildStructDesc(group, schema, projectedSchema);
                }
            }
        };
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
