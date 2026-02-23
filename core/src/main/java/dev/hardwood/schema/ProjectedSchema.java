/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Represents a projected view of a Parquet schema containing only selected columns.
 *
 * <p>This class handles the mapping between projected column indices (dense, 0-based)
 * and original column indices, allowing the reader to skip I/O, decoding, and memory
 * allocation for non-projected columns.</p>
 *
 * <p>For nested schemas, projecting a parent group includes all its child columns.
 * For example, if "address" is a struct containing "city" and "street", projecting
 * "address" includes both child columns.</p>
 */
public final class ProjectedSchema {

    private final FileSchema originalSchema;
    private final int[] projectedToOriginal;   // projected index -> original index
    private final int[] originalToProjected;   // original index -> projected index (-1 if not projected)
    private final List<ColumnSchema> projectedColumns;
    private final int[] projectedFieldIndices; // indices of projected top-level fields in root children

    private ProjectedSchema(FileSchema originalSchema, int[] projectedToOriginal,
                            int[] originalToProjected, List<ColumnSchema> projectedColumns,
                            int[] projectedFieldIndices) {
        this.originalSchema = originalSchema;
        this.projectedToOriginal = projectedToOriginal;
        this.originalToProjected = originalToProjected;
        this.projectedColumns = projectedColumns;
        this.projectedFieldIndices = projectedFieldIndices;
    }

    /**
     * Creates a projected schema from the given full schema and projection.
     *
     * @param schema the original file schema
     * @param projection the column projection specifying which columns to include
     * @return a projected schema containing only the selected columns
     * @throws IllegalArgumentException if a projected column name is not found in the schema
     */
    public static ProjectedSchema create(FileSchema schema, ColumnProjection projection) {
        if (projection.projectsAll()) {
            return createAllColumnsProjection(schema);
        }

        Set<String> projectedNames = projection.getProjectedColumnNames();
        List<ColumnSchema> originalColumns = schema.getColumns();
        int originalCount = originalColumns.size();

        // Build lists of which columns to include
        List<Integer> includedOriginalIndices = new ArrayList<>();
        List<Integer> includedFieldIndices = new ArrayList<>();
        int[] originalToProjected = new int[originalCount];
        Arrays.fill(originalToProjected, -1);

        // Process each requested column name
        for (String name : projectedNames) {
            if (name.contains(".")) {
                // Dot notation for nested field
                resolveNestedColumn(schema, name, includedOriginalIndices, includedFieldIndices, originalToProjected);
            }
            else {
                // Simple name - could be a primitive column or a group
                resolveSimpleColumn(schema, name, includedOriginalIndices, includedFieldIndices, originalToProjected);
            }
        }

        // Sort by original index to maintain column order
        includedOriginalIndices.sort(Integer::compareTo);
        includedFieldIndices.sort(Integer::compareTo);

        // Remove duplicates from field indices
        includedFieldIndices = new ArrayList<>(includedFieldIndices.stream().distinct().toList());

        // Build projected arrays
        int projectedCount = includedOriginalIndices.size();
        int[] projectedToOriginal = new int[projectedCount];
        List<ColumnSchema> projectedColumns = new ArrayList<>(projectedCount);

        for (int i = 0; i < projectedCount; i++) {
            int origIdx = includedOriginalIndices.get(i);
            projectedToOriginal[i] = origIdx;
            originalToProjected[origIdx] = i;
            projectedColumns.add(originalColumns.get(origIdx));
        }

        int[] projectedFieldIndices = includedFieldIndices.stream().mapToInt(Integer::intValue).toArray();

        return new ProjectedSchema(schema, projectedToOriginal, originalToProjected, projectedColumns, projectedFieldIndices);
    }

    /**
     * Creates a projection that includes all columns.
     */
    private static ProjectedSchema createAllColumnsProjection(FileSchema schema) {
        int columnCount = schema.getColumnCount();
        int[] projectedToOriginal = new int[columnCount];
        int[] originalToProjected = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            projectedToOriginal[i] = i;
            originalToProjected[i] = i;
        }

        int fieldCount = schema.getRootNode().children().size();
        int[] projectedFieldIndices = new int[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            projectedFieldIndices[i] = i;
        }

        return new ProjectedSchema(schema, projectedToOriginal, originalToProjected,
                new ArrayList<>(schema.getColumns()), projectedFieldIndices);
    }

    /**
     * Resolves a simple column name (no dot notation).
     */
    private static void resolveSimpleColumn(FileSchema schema, String name,
                                            List<Integer> includedOriginalIndices,
                                            List<Integer> includedFieldIndices,
                                            int[] originalToProjected) {
        // First check if it's a direct column name (flat schema)
        for (ColumnSchema col : schema.getColumns()) {
            if (col.name().equals(name) && originalToProjected[col.columnIndex()] < 0) {
                includedOriginalIndices.add(col.columnIndex());
                // Find the field index
                List<SchemaNode> children = schema.getRootNode().children();
                for (int i = 0; i < children.size(); i++) {
                    if (children.get(i).name().equals(name)) {
                        includedFieldIndices.add(i);
                        break;
                    }
                }
                return;
            }
        }

        // Check top-level fields (could be a group)
        List<SchemaNode> children = schema.getRootNode().children();
        for (int i = 0; i < children.size(); i++) {
            SchemaNode child = children.get(i);
            if (child.name().equals(name)) {
                includedFieldIndices.add(i);
                collectColumnsFromNode(child, includedOriginalIndices, originalToProjected);
                return;
            }
        }

        throw new IllegalArgumentException("Column not found: " + name);
    }

    /**
     * Resolves a nested column name (dot notation).
     */
    private static void resolveNestedColumn(FileSchema schema, String name,
                                            List<Integer> includedOriginalIndices,
                                            List<Integer> includedFieldIndices,
                                            int[] originalToProjected) {
        String[] parts = name.split("\\.");

        // Find the top-level field
        List<SchemaNode> children = schema.getRootNode().children();
        SchemaNode current = null;
        int topLevelFieldIndex = -1;

        for (int i = 0; i < children.size(); i++) {
            if (children.get(i).name().equals(parts[0])) {
                current = children.get(i);
                topLevelFieldIndex = i;
                break;
            }
        }

        if (current == null) {
            throw new IllegalArgumentException("Column not found: " + name);
        }

        includedFieldIndices.add(topLevelFieldIndex);

        // Navigate through the path
        for (int p = 1; p < parts.length; p++) {
            if (!(current instanceof SchemaNode.GroupNode group)) {
                throw new IllegalArgumentException("Cannot navigate into primitive column: " + name);
            }

            SchemaNode found = null;
            for (SchemaNode child : group.children()) {
                if (child.name().equals(parts[p])) {
                    found = child;
                    break;
                }
            }

            if (found == null) {
                throw new IllegalArgumentException("Column not found: " + name);
            }
            current = found;
        }

        // Collect all columns under this node
        collectColumnsFromNode(current, includedOriginalIndices, originalToProjected);
    }

    /**
     * Recursively collects all column indices under a schema node.
     */
    private static void collectColumnsFromNode(SchemaNode node,
                                               List<Integer> includedOriginalIndices,
                                               int[] originalToProjected) {
        switch (node) {
            case SchemaNode.PrimitiveNode prim -> {
                if (originalToProjected[prim.columnIndex()] < 0) {
                    includedOriginalIndices.add(prim.columnIndex());
                }
            }
            case SchemaNode.GroupNode group -> {
                for (SchemaNode child : group.children()) {
                    collectColumnsFromNode(child, includedOriginalIndices, originalToProjected);
                }
            }
        }
    }

    /**
     * Returns the original file schema.
     */
    public FileSchema getOriginalSchema() {
        return originalSchema;
    }

    /**
     * Returns the number of projected columns.
     */
    public int getProjectedColumnCount() {
        return projectedToOriginal.length;
    }

    /**
     * Converts a projected column index to the original column index.
     *
     * @param projectedIndex the index in the projected schema (0-based)
     * @return the corresponding index in the original schema
     * @throws IndexOutOfBoundsException if projectedIndex is out of range
     */
    public int toOriginalIndex(int projectedIndex) {
        return projectedToOriginal[projectedIndex];
    }

    /**
     * Converts an original column index to the projected column index.
     *
     * @param originalIndex the index in the original schema
     * @return the corresponding index in the projected schema, or -1 if not projected
     */
    public int toProjectedIndex(int originalIndex) {
        if (originalIndex < 0 || originalIndex >= originalToProjected.length) {
            return -1;
        }
        return originalToProjected[originalIndex];
    }

    /**
     * Returns the list of projected columns.
     */
    public List<ColumnSchema> getProjectedColumns() {
        return projectedColumns;
    }

    /**
     * Returns the projected column at the given projected index.
     */
    public ColumnSchema getProjectedColumn(int projectedIndex) {
        return projectedColumns.get(projectedIndex);
    }

    /**
     * Returns the indices of projected top-level fields in the root node's children.
     * This is used by NestedBatchDataView to build a sparse record structure.
     */
    public int[] getProjectedFieldIndices() {
        return projectedFieldIndices;
    }

    /**
     * Returns true if all columns are projected.
     */
    public boolean projectsAll() {
        return projectedToOriginal.length == originalSchema.getColumnCount();
    }
}
