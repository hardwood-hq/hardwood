/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.List;

import dev.hardwood.internal.util.StringToIntMap;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;
import dev.hardwood.schema.SchemaNode;

/// Maps each top-level field in the root schema to its leaf column(s) within a projection.
final class TopLevelFieldMap {

    sealed interface FieldDesc {

        default String name() {
            return switch (this) {
                case Primitive p -> p.schema().name();
                case Struct s -> s.schema().name();
                case ListOf l -> l.schema().name();
                case MapOf m -> m.schema().name();
                case Variant v -> v.schema().name();
            };
        }

        record Primitive(int projectedCol, SchemaNode.PrimitiveNode schema) implements FieldDesc {}

        /// Variant logical-type group. `metadataCol` and `valueCol` reference the
        /// two required binary child columns that together carry the canonical
        /// Variant bytes.
        ///
        /// @param schema the Variant-annotated GroupNode
        /// @param metadataCol projected column index of the `metadata` binary child
        /// @param valueCol projected column index of the `value` binary child
        /// @param nullDefLevel def level below which the Variant group itself is null
        record Variant(SchemaNode.GroupNode schema,
                       int metadataCol, int valueCol,
                       int nullDefLevel) implements FieldDesc {}

        /// @param nameToIndex       name → child ordinal (boundary lookup)
        /// @param children          ordinal → descriptor (internal lookup)
        /// @param firstPrimitiveCol projected column of first primitive child, or -1 if none
        /// @param firstLeafProjCol  projected column of first leaf at any depth under this
        ///                          struct (same as `firstPrimitiveCol` when the struct has
        ///                          a direct primitive child), or -1 if no leaf is projected
        record Struct(SchemaNode.GroupNode schema,
                      StringToIntMap nameToIndex,
                      FieldDesc[] children,
                      int firstPrimitiveCol,
                      int firstLeafProjCol) implements FieldDesc {

            FieldDesc getChild(String name) {
                int idx = nameToIndex.get(name);
                return idx >= 0 ? children[idx] : null;
            }

            FieldDesc getChild(int ordinal) {
                return children[ordinal];
            }
        }

        /// @param nullDefLevel     def level below which the list itself is null
        /// @param elementDefLevel  def level at or above which an actual element exists
        /// @param elementDesc      pre-built descriptor for struct/list/map elements, null for primitives
        record ListOf(SchemaNode.GroupNode schema, SchemaNode elementSchema,
                      int firstLeafProjCol, int leafColCount,
                      int nullDefLevel, int elementDefLevel,
                      FieldDesc elementDesc) implements FieldDesc {}

        /// @param nullDefLevel   def level below which the map itself is null
        /// @param entryDefLevel  def level at or above which an actual entry exists
        /// @param valueDesc      pre-built descriptor for struct/list/map values, null for primitives
        record MapOf(SchemaNode.GroupNode schema, int keyProjCol, int valueProjCol,
                     int nullDefLevel, int entryDefLevel,
                     FieldDesc valueDesc) implements FieldDesc {}
    }

    private final StringToIntMap nameToIndex;
    private final FieldDesc[] byIndex;
    private final FieldDesc[] byOriginalIndex;

    private TopLevelFieldMap(StringToIntMap nameToIndex, FieldDesc[] byIndex, FieldDesc[] byOriginalIndex) {
        this.nameToIndex = nameToIndex;
        this.byIndex = byIndex;
        this.byOriginalIndex = byOriginalIndex;
    }

    FieldDesc getByName(String name) {
        int idx = nameToIndex.get(name);
        if (idx < 0) {
            throw new IllegalArgumentException("Field '" + name + "' not in projection");
        }
        return byIndex[idx];
    }

    FieldDesc getByOriginalIndex(int originalFieldIndex) {
        if (originalFieldIndex < 0 || originalFieldIndex >= byOriginalIndex.length) {
            return null;
        }
        return byOriginalIndex[originalFieldIndex];
    }

    static TopLevelFieldMap build(FileSchema schema, ProjectedSchema projectedSchema) {
        List<SchemaNode> rootChildren = schema.getRootNode().children();
        int[] projectedFieldIndices = projectedSchema.getProjectedFieldIndices();

        int fieldCount = projectedFieldIndices.length;
        StringToIntMap nameToIndex = new StringToIntMap(fieldCount);
        FieldDesc[] byIndex = new FieldDesc[fieldCount];

        int maxOriginalIndex = rootChildren.size();
        FieldDesc[] byOriginalIndex = new FieldDesc[maxOriginalIndex];

        for (int i = 0; i < fieldCount; i++) {
            int projFieldIdx = projectedFieldIndices[i];
            SchemaNode topLevelNode = rootChildren.get(projFieldIdx);
            FieldDesc desc = buildDesc(topLevelNode, schema, projectedSchema);
            nameToIndex.put(topLevelNode.name(), i);
            byIndex[i] = desc;
            byOriginalIndex[projFieldIdx] = desc;
        }

        return new TopLevelFieldMap(nameToIndex, byIndex, byOriginalIndex);
    }

    private static FieldDesc buildDesc(SchemaNode node, FileSchema schema, ProjectedSchema projectedSchema) {
        return switch (node) {
            case SchemaNode.PrimitiveNode prim -> {
                int projCol = projectedSchema.toProjectedIndex(prim.columnIndex());
                yield new FieldDesc.Primitive(projCol, prim);
            }
            case SchemaNode.GroupNode group -> {
                if (group.isVariant()) {
                    yield buildVariantDesc(group, projectedSchema);
                }
                else if (group.isList()) {
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

    static FieldDesc.Variant buildVariantDesc(SchemaNode.GroupNode group, ProjectedSchema projectedSchema) {
        // FileSchema validation guarantees the first two children are PrimitiveNodes
        // named `metadata` and `value`; any third child (`typed_value`) is ignored
        // in Phase 1 and only consulted once shredded-variant reassembly lands.
        SchemaNode.PrimitiveNode metadataNode = (SchemaNode.PrimitiveNode) group.children().get(0);
        SchemaNode.PrimitiveNode valueNode = (SchemaNode.PrimitiveNode) group.children().get(1);
        int metadataCol = projectedSchema.toProjectedIndex(metadataNode.columnIndex());
        int valueCol = projectedSchema.toProjectedIndex(valueNode.columnIndex());
        return new FieldDesc.Variant(group, metadataCol, valueCol, group.maxDefinitionLevel());
    }

    static FieldDesc.Struct buildStructDesc(SchemaNode.GroupNode group,
                                            FileSchema schema,
                                            ProjectedSchema projectedSchema) {
        List<SchemaNode> schemaChildren = group.children();
        int childCount = schemaChildren.size();
        StringToIntMap nameToIndex = new StringToIntMap(childCount);
        // Count projected children first
        int projected = 0;
        for (int i = 0; i < childCount; i++) {
            SchemaNode child = schemaChildren.get(i);
            if (isChildProjected(child, projectedSchema)) {
                projected++;
            }
        }
        FieldDesc[] children = new FieldDesc[projected];
        int firstPrimitiveCol = -1;
        int idx = 0;
        for (int i = 0; i < childCount; i++) {
            SchemaNode child = schemaChildren.get(i);
            FieldDesc childDesc = buildDescForChild(child, schema, projectedSchema);
            if (childDesc != null) {
                nameToIndex.put(child.name(), idx);
                children[idx] = childDesc;
                if (firstPrimitiveCol < 0 && childDesc instanceof FieldDesc.Primitive p) {
                    firstPrimitiveCol = p.projectedCol();
                }
                idx++;
            }
        }
        int firstLeafProjCol = firstPrimitiveCol >= 0
                ? firstPrimitiveCol
                : findFirstLeafProjCol(group, projectedSchema);
        return new FieldDesc.Struct(group, nameToIndex, children, firstPrimitiveCol, firstLeafProjCol);
    }

    static FieldDesc.ListOf buildListDesc(SchemaNode.GroupNode listGroup,
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

        // Pre-build element descriptor for nested types
        FieldDesc elementDesc = null;
        if (elementSchema instanceof SchemaNode.GroupNode group) {
            elementDesc = buildDescForChild(elementSchema, schema, projectedSchema);
        }

        return new FieldDesc.ListOf(listGroup, elementSchema, firstProjCol, leafCount,
                nullDefLevel, elementDefLevel, elementDesc);
    }

    static FieldDesc.MapOf buildMapDesc(SchemaNode.GroupNode mapGroup,
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

        // Pre-build value descriptor for nested types
        FieldDesc valueDesc = null;
        if (valueNode instanceof SchemaNode.GroupNode) {
            valueDesc = buildDescForChild(valueNode, schema, projectedSchema);
        }

        return new FieldDesc.MapOf(mapGroup, keyProjCol, valueProjCol, nullDefLevel, entryDefLevel, valueDesc);
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
                if (!isChildProjected(group, projectedSchema)) {
                    yield null;
                }
                if (group.isVariant()) {
                    yield buildVariantDesc(group, projectedSchema);
                }
                else if (group.isList()) {
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

    private static boolean isChildProjected(SchemaNode node, ProjectedSchema projectedSchema) {
        return switch (node) {
            case SchemaNode.PrimitiveNode prim -> projectedSchema.toProjectedIndex(prim.columnIndex()) >= 0;
            case SchemaNode.GroupNode group -> {
                for (SchemaNode child : group.children()) {
                    if (isChildProjected(child, projectedSchema)) {
                        yield true;
                    }
                }
                yield false;
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
