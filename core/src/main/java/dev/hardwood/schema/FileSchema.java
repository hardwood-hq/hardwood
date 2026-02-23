/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.schema;

import java.util.ArrayList;
import java.util.List;

import dev.hardwood.internal.util.StringToIntMap;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;

/**
 * Root schema container representing the complete Parquet schema.
 * Supports both flat schemas and nested structures (structs, lists).
 */
public class FileSchema {

    private final String name;
    private final List<ColumnSchema> columns;
    private final StringToIntMap columnNameToIndex;
    private final SchemaNode.GroupNode rootNode;

    private FileSchema(String name, List<ColumnSchema> columns, SchemaNode.GroupNode rootNode) {
        this.name = name;
        this.columns = columns;
        this.rootNode = rootNode;

        // Pre-compute name -> index mapping for O(1) lookup
        this.columnNameToIndex = new StringToIntMap(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            columnNameToIndex.put(columns.get(i).name(), i);
        }
    }

    public String getName() {
        return name;
    }

    public List<ColumnSchema> getColumns() {
        return columns;
    }

    public ColumnSchema getColumn(int index) {
        return columns.get(index);
    }

    public ColumnSchema getColumn(String name) {
        int index = columnNameToIndex.get(name);
        if (index < 0) {
            throw new IllegalArgumentException("Column not found: " + name);
        }
        return columns.get(index);
    }

    public int getColumnCount() {
        return columns.size();
    }

    /**
     * Returns the hierarchical schema tree representation.
     */
    public SchemaNode.GroupNode getRootNode() {
        return rootNode;
    }

    /**
     * Finds a top-level field by name in the schema tree.
     */
    public SchemaNode getField(String name) {
        for (SchemaNode child : rootNode.children()) {
            if (child.name().equals(name)) {
                return child;
            }
        }
        throw new IllegalArgumentException("Field not found: " + name);
    }

    /**
     * Returns true if this schema supports direct columnar access.
     * For such schemas, enabling direct columnar access without record assembly.
     * <p>
     * A schema supports columnar access if all top-level fields are primitives
     * (no nested structs, lists, or maps) and no columns have repetition.
     * </p>
     */
    public boolean isFlatSchema() {
        // Check that all top-level fields are primitives (no nested structs)
        for (SchemaNode child : rootNode.children()) {
            if (child instanceof SchemaNode.GroupNode) {
                return false;
            }
        }
        // Also check repetition levels
        for (ColumnSchema col : columns) {
            if (col.maxRepetitionLevel() > 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Reconstruct schema from Thrift SchemaElement list.
     */
    public static FileSchema fromSchemaElements(List<SchemaElement> elements) {
        if (elements.isEmpty()) {
            throw new IllegalArgumentException("Schema elements list is empty");
        }

        SchemaElement root = elements.get(0);
        if (root.isPrimitive()) {
            throw new IllegalArgumentException("Root schema element must be a group");
        }

        // Build hierarchical tree and flat column list simultaneously
        List<ColumnSchema> columns = new ArrayList<>();
        int[] columnIndex = { 0 }; // Mutable counter for column indexing

        List<SchemaNode> rootChildren = buildChildren(elements, 1, root.numChildren() != null ? root.numChildren() : 0, 0, 0, columns, columnIndex);

        SchemaNode.GroupNode rootNode = new SchemaNode.GroupNode(
                root.name(),
                root.repetitionType() != null ? root.repetitionType() : RepetitionType.REQUIRED,
                root.convertedType(),
                rootChildren,
                0, // Root has def level 0
                0 // Root has rep level 0
        );

        return new FileSchema(root.name(), columns, rootNode);
    }

    /**
     * Build children nodes from schema elements.
     */
    private static List<SchemaNode> buildChildren(
                                                  List<SchemaElement> elements,
                                                  int startIndex,
                                                  int numChildren,
                                                  int parentDefLevel,
                                                  int parentRepLevel,
                                                  List<ColumnSchema> columns,
                                                  int[] columnIndex) {

        List<SchemaNode> children = new ArrayList<>();
        int currentIndex = startIndex;

        for (int i = 0; i < numChildren; i++) {
            SchemaElement element = elements.get(currentIndex);
            RepetitionType repType = element.repetitionType() != null ? element.repetitionType() : RepetitionType.OPTIONAL;

            // Calculate levels for this node
            int defLevel = parentDefLevel + (repType != RepetitionType.REQUIRED ? 1 : 0);
            int repLevel = parentRepLevel + (repType == RepetitionType.REPEATED ? 1 : 0);

            if (element.isPrimitive()) {
                // Primitive node - represents an actual column
                int colIdx = columnIndex[0]++;
                columns.add(new ColumnSchema(
                        element.name(),
                        element.type(),
                        repType,
                        element.typeLength(),
                        colIdx,
                        defLevel,
                        repLevel,
                        element.logicalType()));

                children.add(new SchemaNode.PrimitiveNode(
                        element.name(),
                        element.type(),
                        repType,
                        element.logicalType(),
                        colIdx,
                        defLevel,
                        repLevel));

                currentIndex++;
            }
            else {
                // Group node - recurse into children
                int groupNumChildren = element.numChildren() != null ? element.numChildren() : 0;
                List<SchemaNode> groupChildren = buildChildren(
                        elements,
                        currentIndex + 1,
                        groupNumChildren,
                        defLevel,
                        repLevel,
                        columns,
                        columnIndex);

                children.add(new SchemaNode.GroupNode(
                        element.name(),
                        repType,
                        element.convertedType(),
                        groupChildren,
                        defLevel,
                        repLevel));

                // Skip over this group and all its descendants
                currentIndex = currentIndex + 1 + countDescendants(elements, currentIndex + 1, groupNumChildren);
            }
        }

        return children;
    }

    /**
     * Count total descendants of a group (including nested groups).
     */
    private static int countDescendants(List<SchemaElement> elements, int startIndex, int numChildren) {
        int count = 0;
        int currentIndex = startIndex;

        for (int i = 0; i < numChildren; i++) {
            SchemaElement element = elements.get(currentIndex);
            count++;
            currentIndex++;

            if (element.isGroup()) {
                int groupChildren = element.numChildren() != null ? element.numChildren() : 0;
                int descendantCount = countDescendants(elements, currentIndex, groupChildren);
                count += descendantCount;
                currentIndex += descendantCount;
            }
        }

        return count;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("message ").append(name).append(" {\n");
        for (SchemaNode child : rootNode.children()) {
            appendNode(sb, child, 1);
        }
        sb.append("}");
        return sb.toString();
    }

    private void appendNode(StringBuilder sb, SchemaNode node, int indent) {
        String prefix = "  ".repeat(indent);
        switch (node) {
            case SchemaNode.GroupNode group -> {
                sb.append(prefix);
                sb.append(group.repetitionType().name().toLowerCase());
                sb.append(" group ").append(group.name());
                if (group.convertedType() != null) {
                    sb.append(" (").append(group.convertedType()).append(")");
                }
                sb.append(" {\n");
                for (SchemaNode child : group.children()) {
                    appendNode(sb, child, indent + 1);
                }
                sb.append(prefix).append("}\n");
            }
            case SchemaNode.PrimitiveNode prim -> {
                sb.append(prefix);
                sb.append(prim.repetitionType().name().toLowerCase());
                sb.append(" ").append(prim.type().name().toLowerCase());
                sb.append(" ").append(prim.name());
                if (prim.logicalType() != null) {
                    sb.append(" (").append(prim.logicalType()).append(")");
                }
                sb.append(";\n");
            }
        }
    }
}
