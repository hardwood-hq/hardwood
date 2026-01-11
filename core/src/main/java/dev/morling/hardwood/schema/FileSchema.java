/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.schema;

import java.util.ArrayList;
import java.util.List;

import dev.morling.hardwood.metadata.RepetitionType;
import dev.morling.hardwood.metadata.SchemaElement;

/**
 * Root schema container representing the complete Parquet schema.
 * Supports both flat schemas and nested structures (structs, lists).
 */
public class FileSchema {

    private final String name;
    private final List<ColumnSchema> columns;
    private final SchemaNode.GroupNode rootNode;

    private FileSchema(String name, List<ColumnSchema> columns, SchemaNode.GroupNode rootNode) {
        this.name = name;
        this.columns = columns;
        this.rootNode = rootNode;
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
        for (ColumnSchema column : columns) {
            if (column.name().equals(name)) {
                return column;
            }
        }
        throw new IllegalArgumentException("Column not found: " + name);
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
     * Check if a column (by index) is part of a list that contains struct elements.
     * Such columns need raw mode reading for multi-column assembly.
     */
    public boolean isColumnInListOfStruct(int columnIndex) {
        return isColumnInListOfStruct(rootNode, columnIndex, false);
    }

    private boolean isColumnInListOfStruct(SchemaNode node, int columnIndex, boolean inListWithStructElement) {
        if (node instanceof SchemaNode.PrimitiveNode prim) {
            return prim.columnIndex() == columnIndex && inListWithStructElement;
        }

        SchemaNode.GroupNode group = (SchemaNode.GroupNode) node;

        // Check if this is a list with struct elements
        boolean isListOfStruct = false;
        if (group.isList()) {
            SchemaNode element = group.getListElement();
            if (element instanceof SchemaNode.GroupNode elementGroup && !elementGroup.isList()) {
                // List element is a struct (group that's not a list)
                isListOfStruct = true;
            }
        }

        for (SchemaNode child : group.children()) {
            if (isColumnInListOfStruct(child, columnIndex, inListWithStructElement || isListOfStruct)) {
                return true;
            }
        }
        return false;
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
        appendNode(sb, rootNode, 1);
        sb.append("}");
        return sb.toString();
    }

    private void appendNode(StringBuilder sb, SchemaNode node, int indent) {
        if (node instanceof SchemaNode.GroupNode group) {
            for (SchemaNode child : group.children()) {
                appendNode(sb, child, indent);
            }
        }
        else if (node instanceof SchemaNode.PrimitiveNode prim) {
            sb.append("  ".repeat(indent));
            sb.append(prim.repetitionType().name().toLowerCase());
            sb.append(" ");
            sb.append(prim.type().name().toLowerCase());
            sb.append(" ");
            sb.append(prim.name());
            if (prim.logicalType() != null) {
                sb.append(" (").append(prim.logicalType()).append(")");
            }
            sb.append(";\n");
        }
    }
}
