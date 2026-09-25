/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.IntPredicate;

import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

/// Represents a projected view of a Parquet schema containing only selected columns.
///
/// This class handles the mapping between projected column indices (dense, 0-based)
/// and original column indices, allowing the reader to skip I/O, decoding, and memory
/// allocation for non-projected columns.
///
/// For nested schemas, projecting a parent group includes all its child columns.
/// For example, if "address" is a struct containing "city" and "street", projecting
/// "address" includes both child columns.
///
/// Projected column indices follow file schema order; they address the decode pipeline.
/// What a reader exposes by index follows the order the projection requests its columns
/// in instead, in two forms:
///
/// - [#requestedColumn] lists the leaf columns each request selects, request after request,
///   so a column selected by several requests appears once for each. The columnar readers
///   expose this list.
/// - [#exposedColumn], [#getProjectedFieldIndices] and [#projectedChildren] give every leaf
///   column, top-level field and struct child one position, ordered by the first request that
///   selects it. The row readers expose these.
///
/// Either way, the columns a single request selects follow schema order among themselves.
public final class ProjectedSchema {

    private final FileSchema originalSchema;
    private final int[] projectedToOriginal;   // projected index -> original index
    private final int[] originalToProjected;   // original index -> projected index (-1 if not projected)
    private final List<ColumnSchema> projectedColumns;
    private final int[] projectedFieldIndices; // projected top-level fields in root children, in request order
    private final int[] requestByOriginal;     // original index -> first request selecting it (-1 if not projected)
    private final int[] exposedToProjected;    // exposed position -> projected index
    private final int[] projectedToExposed;    // projected index -> exposed position
    private final int[] requestedOriginals;    // requested position -> original index, repeats included
    private final int[] requestedToProjected;  // requested position -> projected index

    private ProjectedSchema(FileSchema originalSchema, int[] projectedToOriginal, int[] projectedFieldIndices,
                            int[] requestByOriginal, int[] requestedOriginals) {
        this.originalSchema = originalSchema;
        this.projectedToOriginal = projectedToOriginal;
        this.projectedFieldIndices = projectedFieldIndices;
        this.requestByOriginal = requestByOriginal;
        this.requestedOriginals = requestedOriginals;

        int projectedCount = projectedToOriginal.length;
        List<ColumnSchema> originalColumns = originalSchema.getColumns();
        this.originalToProjected = new int[originalColumns.size()];
        Arrays.fill(originalToProjected, -1);
        this.projectedColumns = new ArrayList<>(projectedCount);
        long[] exposureKeys = new long[projectedCount];
        for (int i = 0; i < projectedCount; i++) {
            int originalIndex = projectedToOriginal[i];
            originalToProjected[originalIndex] = i;
            projectedColumns.add(originalColumns.get(originalIndex));
            exposureKeys[i] = requestOrderKey(requestByOriginal[originalIndex], i);
        }
        this.exposedToProjected = sortedByRequest(exposureKeys);
        this.projectedToExposed = new int[projectedCount];
        for (int position = 0; position < projectedCount; position++) {
            projectedToExposed[exposedToProjected[position]] = position;
        }
        this.requestedToProjected = new int[requestedOriginals.length];
        for (int position = 0; position < requestedOriginals.length; position++) {
            requestedToProjected[position] = originalToProjected[requestedOriginals[position]];
        }
    }

    /// Creates a projected schema from the given full schema and projection.
    ///
    /// @param schema the original file schema
    /// @param projection the column projection specifying which columns to include
    /// @return a projected schema containing only the selected columns
    /// @throws IllegalArgumentException if a projected column name is not found in the schema
    public static ProjectedSchema create(FileSchema schema, ColumnProjection projection) {
        return create(schema, projection, false);
    }

    /// Resolves a [ColumnProjection] against `schema`.
    ///
    /// When `completeContainers` is true, special groups are expanded to the
    /// leaves they cannot be materialized without: a MAP's `key` column and every
    /// leaf of a VARIANT (read atomically). Row assembly requires this; the
    /// columnar [ColumnReader] / [dev.hardwood.reader.ColumnReaders] paths read
    /// individual leaves and pass false to keep the projection literal.
    ///
    /// @param schema the file schema
    /// @param projection the requested columns
    /// @param completeContainers whether to pull in required sibling leaves of
    ///        projected MAP / VARIANT groups
    /// @return the resolved projection
    /// @throws IllegalArgumentException if a requested name is not found in the schema
    public static ProjectedSchema create(FileSchema schema, ColumnProjection projection,
            boolean completeContainers) {
        if (projection.projectsAll()) {
            return createAllColumnsProjection(schema);
        }
        return resolve(schema, projection.getProjectedColumnNames(), completeContainers);
    }

    /// Names may repeat or overlap: a column selected by several of them is projected once, and
    /// takes its [#exposedColumn] position from the first.
    private static ProjectedSchema resolve(FileSchema schema, List<String> names, boolean completeContainers) {
        int originalCount = schema.getColumnCount();
        int fieldCount = schema.getRootNode().children().size();
        int[] requestByOriginal = new int[originalCount];
        Arrays.fill(requestByOriginal, -1);
        int[] requestByField = new int[fieldCount];
        Arrays.fill(requestByField, -1);

        List<Integer> leaves = new ArrayList<>();
        List<Integer> requested = new ArrayList<>();
        for (int request = 0; request < names.size(); request++) {
            String name = names.get(request);
            leaves.clear();
            int field = name.contains(".")
                    ? resolveNestedColumn(schema, name, leaves)
                    : resolveSimpleColumn(schema, name, leaves);
            if (requestByField[field] < 0) {
                requestByField[field] = request;
            }
            for (int leaf : leaves) {
                if (requestByOriginal[leaf] < 0) {
                    requestByOriginal[leaf] = request;
                }
            }
            requested.addAll(leaves);
        }

        // Enforce structural invariants on special groups: a MAP cannot be
        // assembled without its key column, and a VARIANT is read atomically.
        // Pull in the required sibling leaves whenever any part of such a group
        // is projected.
        if (completeContainers) {
            enforceGroupInvariants(schema.getRootNode(), requestByOriginal);
        }

        int projectedCount = 0;
        for (int request : requestByOriginal) {
            if (request >= 0) {
                projectedCount++;
            }
        }
        int[] projectedToOriginal = new int[projectedCount];
        int next = 0;
        for (int i = 0; i < originalCount; i++) {
            if (requestByOriginal[i] >= 0) {
                projectedToOriginal[next++] = i;
            }
        }

        int projectedFieldCount = 0;
        for (int request : requestByField) {
            if (request >= 0) {
                projectedFieldCount++;
            }
        }
        long[] fieldKeys = new long[projectedFieldCount];
        next = 0;
        for (int i = 0; i < fieldCount; i++) {
            if (requestByField[i] >= 0) {
                fieldKeys[next++] = requestOrderKey(requestByField[i], i);
            }
        }

        return new ProjectedSchema(schema, projectedToOriginal, sortedByRequest(fieldKeys), requestByOriginal,
                requested.stream().mapToInt(Integer::intValue).toArray());
    }

    /// Packs a request and a tie-breaking position so that sorting the keys orders by request
    /// first and position second.
    private static long requestOrderKey(int request, int position) {
        return ((long) request << 32) | position;
    }

    /// The positions of `keys` built by [#requestOrderKey], in request order. Sorts `keys`.
    private static int[] sortedByRequest(long[] keys) {
        Arrays.sort(keys);
        int[] positions = new int[keys.length];
        for (int i = 0; i < keys.length; i++) {
            positions[i] = (int) keys[i];
        }
        return positions;
    }

    /// Returns `all` reordered so that the columns and top-level fields of `leading` come
    /// first, at the indices they hold in `leading`, and the rest follow in their original
    /// order. `leading` must be a subset of `all`. Used by [ReadProjection] to place the
    /// payload columns ahead of the predicate-only ones.
    static ProjectedSchema leadingThenRest(ProjectedSchema leading, ProjectedSchema all) {
        int[] projectedToOriginal = partition(all.projectedToOriginal,
                original -> leading.toProjectedIndex(original) >= 0);
        int[] leadingFields = leading.projectedFieldIndices;
        int[] fieldIndices = Arrays.copyOf(leadingFields, all.projectedFieldIndices.length);
        int next = leadingFields.length;
        for (int field : all.projectedFieldIndices) {
            if (!contains(leadingFields, field)) {
                fieldIndices[next++] = field;
            }
        }
        return new ProjectedSchema(all.originalSchema, projectedToOriginal, fieldIndices, all.requestByOriginal,
                all.requestedOriginals);
    }

    /// Returns `values` with every entry `exposed` accepts first, in their original order,
    /// followed by the rest in theirs.
    private static int[] partition(int[] values, IntPredicate exposed) {
        int[] partitioned = new int[values.length];
        int next = 0;
        for (int value : values) {
            if (exposed.test(value)) {
                partitioned[next++] = value;
            }
        }
        for (int value : values) {
            if (!exposed.test(value)) {
                partitioned[next++] = value;
            }
        }
        return partitioned;
    }

    private static boolean contains(int[] values, int value) {
        for (int candidate : values) {
            if (candidate == value) {
                return true;
            }
        }
        return false;
    }

    /// Creates a projection that includes all columns.
    private static ProjectedSchema createAllColumnsProjection(FileSchema schema) {
        int columnCount = schema.getColumnCount();
        int[] projectedToOriginal = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            projectedToOriginal[i] = i;
        }

        int fieldCount = schema.getRootNode().children().size();
        int[] projectedFieldIndices = new int[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            projectedFieldIndices[i] = i;
        }

        // A single request selects every column.
        return new ProjectedSchema(schema, projectedToOriginal, projectedFieldIndices, new int[columnCount],
                projectedToOriginal);
    }

    /// Resolves a simple column name (no dot notation), which names a top-level field, into
    /// `leaves`. A nested field is reached by its full path only, so a name matching a nested
    /// leaf's own name does not select it.
    ///
    /// @return the index of the top-level field the name selects
    private static int resolveSimpleColumn(FileSchema schema, String name, List<Integer> leaves) {
        List<SchemaNode> children = schema.getRootNode().children();
        for (int i = 0; i < children.size(); i++) {
            SchemaNode child = children.get(i);
            if (child.name().equals(name)) {
                collectColumnsFromNode(child, leaves);
                return i;
            }
        }
        throw new IllegalArgumentException("Column not found: " + name);
    }

    /// Resolves a nested column name (dot notation) into `leaves`.
    ///
    /// @return the index of the top-level field the name selects from
    private static int resolveNestedColumn(FileSchema schema, String name, List<Integer> leaves) {
        SchemaPathResolver.Resolution resolution = SchemaPathResolver.resolve(schema, name);
        if (resolution.blockedByPrimitive()) {
            throw new IllegalArgumentException("Cannot navigate into primitive column: " + name);
        }
        if (resolution.node() == null) {
            throw new IllegalArgumentException("Column not found: " + name);
        }

        // Collect all columns under this node
        collectColumnsFromNode(resolution.node(), leaves);
        return resolution.topLevelChildIndex();
    }

    /// Recursively collects all column indices under a schema node.
    private static void collectColumnsFromNode(SchemaNode node, List<Integer> leaves) {
        switch (node) {
            case SchemaNode.PrimitiveNode prim -> leaves.add(prim.columnIndex());
            case SchemaNode.GroupNode group -> {
                for (SchemaNode child : group.children()) {
                    collectColumnsFromNode(child, leaves);
                }
            }
        }
    }

    /// Walks the schema tree and, for any special group that has at least one
    /// projected leaf, pulls in the sibling leaves the group cannot be read
    /// without: a MAP's `key` column, and every leaf of a VARIANT (which is
    /// reassembled atomically). Without this, a sub-field projection such as
    /// `people.key_value.value.age` or `var.typed_value` would leave the reader
    /// unable to assemble the map or the variant.
    /// Pulled-in leaves take the position of the first request that selects anything in the
    /// group, so they sit with the rest of it.
    private static void enforceGroupInvariants(SchemaNode node, int[] requestByOriginal) {
        if (!(node instanceof SchemaNode.GroupNode group)) {
            return;
        }
        for (SchemaNode child : group.children()) {
            enforceGroupInvariants(child, requestByOriginal);
        }
        int request = firstRequest(group, requestByOriginal);
        if (request < 0) {
            return;
        }
        if (group.isVariant()) {
            addAllLeaves(group, request, requestByOriginal);
        }
        else if (group.isMap()) {
            SchemaNode key = group.getMapKey();
            if (key != null) {
                addAllLeaves(key, request, requestByOriginal);
            }
        }
    }

    /// The first request selecting a leaf under `node`, or -1 if none does.
    private static int firstRequest(SchemaNode node, int[] requestByOriginal) {
        return switch (node) {
            case SchemaNode.PrimitiveNode prim -> requestByOriginal[prim.columnIndex()];
            case SchemaNode.GroupNode group -> {
                int first = -1;
                for (SchemaNode child : group.children()) {
                    int request = firstRequest(child, requestByOriginal);
                    if (request >= 0 && (first < 0 || request < first)) {
                        first = request;
                    }
                }
                yield first;
            }
        };
    }

    /// Assigns `request` to every leaf under `node` no request selected yet.
    private static void addAllLeaves(SchemaNode node, int request, int[] requestByOriginal) {
        switch (node) {
            case SchemaNode.PrimitiveNode prim -> {
                if (requestByOriginal[prim.columnIndex()] < 0) {
                    requestByOriginal[prim.columnIndex()] = request;
                }
            }
            case SchemaNode.GroupNode group -> {
                for (SchemaNode child : group.children()) {
                    addAllLeaves(child, request, requestByOriginal);
                }
            }
        }
    }

    /// Returns the original file schema.
    public FileSchema getOriginalSchema() {
        return originalSchema;
    }

    /// Returns the number of projected columns.
    public int getProjectedColumnCount() {
        return projectedToOriginal.length;
    }

    /// Converts a projected column index to the original column index.
    ///
    /// @param projectedIndex the index in the projected schema (0-based)
    /// @return the corresponding index in the original schema
    /// @throws IndexOutOfBoundsException if projectedIndex is out of range
    public int toOriginalIndex(int projectedIndex) {
        return projectedToOriginal[projectedIndex];
    }

    /// Converts an original column index to the projected column index.
    ///
    /// @param originalIndex the index in the original schema
    /// @return the corresponding index in the projected schema, or -1 if not projected
    public int toProjectedIndex(int originalIndex) {
        if (originalIndex < 0 || originalIndex >= originalToProjected.length) {
            return -1;
        }
        return originalToProjected[originalIndex];
    }

    /// Returns the list of projected columns.
    public List<ColumnSchema> getProjectedColumns() {
        return projectedColumns;
    }

    /// Returns the projected column at the given projected index.
    public ColumnSchema getProjectedColumn(int projectedIndex) {
        return projectedColumns.get(projectedIndex);
    }

    /// Returns the indices of projected top-level fields in the root node's children, in
    /// request order: the position a row reader exposes each field at.
    public int[] getProjectedFieldIndices() {
        return projectedFieldIndices;
    }

    /// Returns the projected index of the column a reader exposes at `position`.
    ///
    /// @param position the column's position in request order (0-based)
    /// @return the corresponding index in the projected schema
    public int exposedColumn(int position) {
        return exposedToProjected[position];
    }

    /// Returns the position, in request order, a reader exposes a projected column at.
    ///
    /// @param projectedIndex the index in the projected schema (0-based)
    /// @return the column's position in request order
    public int exposedPosition(int projectedIndex) {
        return projectedToExposed[projectedIndex];
    }

    /// Returns the number of leaf columns the requests select, counting a column once for
    /// every request that selects it.
    public int requestedColumnCount() {
        return requestedToProjected.length;
    }

    /// Returns the projected index of the leaf column at `position` in the requested list:
    /// the leaf columns of each request in turn, each request's in schema order.
    ///
    /// @param position the position in the requested list (0-based)
    /// @return the corresponding index in the projected schema
    public int requestedColumn(int position) {
        return requestedToProjected[position];
    }

    /// Returns the children of `group` holding a projected leaf, in request order.
    public List<SchemaNode> projectedChildren(SchemaNode.GroupNode group) {
        List<SchemaNode> children = new ArrayList<>(group.children().size());
        for (SchemaNode child : group.children()) {
            if (firstRequest(child, requestByOriginal) >= 0) {
                children.add(child);
            }
        }
        children.sort(Comparator.comparingInt(child -> firstRequest(child, requestByOriginal)));
        return children;
    }

    /// Returns a copy of the projected-to-original index mapping.
    /// Each element is the original column index for the corresponding projected index.
    public int[] toOriginalIndices() {
        return projectedToOriginal.clone();
    }

    /// Returns true if all columns are projected.
    public boolean projectsAll() {
        return projectedToOriginal.length == originalSchema.getColumnCount();
    }
}
