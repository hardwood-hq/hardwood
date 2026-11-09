/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

/// Resolves a dot-separated field path (e.g. `"address.city"`) against the [SchemaNode] tree.
///
/// [FileSchema#getColumn(String)] is keyed on leaf paths only, so a name denoting a group
/// resolves to nothing there. This walker descends the node tree instead and can therefore stop
/// on a group, which is what callers need in order to report *why* a name is not a usable leaf.
///
/// A field name may itself contain dots, as in a flat column named `sepal.length`, so a dot in
/// the path does not necessarily separate two levels. The walk follows every field whose name
/// matches the path up to a dot or its end. Should two fields' paths join to the same name, the
/// name is ambiguous and resolving it fails. Sibling fields may also share a name, giving several
/// nodes one identical path: [#resolve] fails for such a path, and [#resolveAll] returns them all.
///
/// Path segments are compared in place, so no array is materialized for the path.
public final class SchemaPathResolver {

    private SchemaPathResolver() {
    }

    /// The nodes from the root's child down to the leaf of the column at `columnIndex`, in
    /// root-to-leaf order. The walk follows the column's index rather than its path's names, since
    /// two siblings may share a name: leaves are numbered in schema order, so at each level the
    /// column sits under the last child whose first leaf is not beyond it.
    ///
    /// @throws IndexOutOfBoundsException if `columnIndex` is not a column of `schema`
    public static List<SchemaNode> nodesOnPath(FileSchema schema, int columnIndex) {
        Objects.checkIndex(columnIndex, schema.getColumnCount());
        List<SchemaNode> nodes = new ArrayList<>();
        SchemaNode current = schema.getRootNode();
        while (current instanceof SchemaNode.GroupNode group) {
            current = childHolding(group, columnIndex);
            nodes.add(current);
        }
        return nodes;
    }

    private static SchemaNode childHolding(SchemaNode.GroupNode group, int columnIndex) {
        SchemaNode holder = null;
        for (SchemaNode child : group.children()) {
            int firstLeaf = firstLeaf(child);
            if (firstLeaf > columnIndex) {
                break;
            }
            if (firstLeaf >= 0) {
                holder = child;
            }
        }
        if (holder == null) {
            throw new IllegalStateException("No child of " + group.name() + " holds column " + columnIndex);
        }
        return holder;
    }

    /// The index of the first leaf column under `node`, or -1 when it holds none.
    private static int firstLeaf(SchemaNode node) {
        if (node instanceof SchemaNode.PrimitiveNode primitive) {
            return primitive.columnIndex();
        }
        for (SchemaNode child : ((SchemaNode.GroupNode) node).children()) {
            int firstLeaf = firstLeaf(child);
            if (firstLeaf >= 0) {
                return firstLeaf;
            }
        }
        return -1;
    }

    /// The outcome of walking a dot-separated path over a schema tree.
    ///
    /// @param node the node at the requested path, or `null` when the path names no node
    /// @param topLevelChildIndex index of the path's first segment among the root's children,
    ///        or `-1` when the root has no child of that name
    /// @param blockedByPrimitive `true` when the walk stopped because a segment would have had to
    ///        descend into a primitive column
    /// @param variantAncestor the path of the innermost `VARIANT` group the walk descended into on
    ///        the way to the node, or `null` when it passed through none. A node below one holds a
    ///        variant's encoded payload rather than a value of its own
    public record Resolution(SchemaNode node, int topLevelChildIndex, boolean blockedByPrimitive,
            String variantAncestor) {
    }

    /// Resolves `path` against the root node of `schema`.
    ///
    /// @throws IllegalArgumentException if `path` names more than one node
    public static Resolution resolve(FileSchema schema, String path) {
        return resolve(schema.getRootNode(), path);
    }

    /// Resolves `path` against `root`, descending one tree level per field name the path holds.
    ///
    /// @throws IllegalArgumentException if `path` names more than one node
    public static Resolution resolve(SchemaNode.GroupNode root, String path) {
        Walk walk = new Walk(path);
        walk.descend(root, 0, -1, null, "");
        if (walk.matches.size() > 1) {
            throw ambiguous(path);
        }
        if (walk.matches.size() == 1) {
            return walk.matches.getFirst();
        }
        return new Resolution(null, walk.topLevelChildIndex, walk.blockedByPrimitive, null);
    }

    /// Resolves `path` against the root node of `schema` to every node it names, in schema order.
    /// Sibling fields may share a name, so one path may name several nodes; a selection takes all
    /// of them. A path that reaches no node yields an empty list, and [#resolve] tells why.
    ///
    /// @throws IllegalArgumentException if `path` names nodes whose paths differ, as field names
    ///         containing dots allow: a flat `a.b` and a `b` nested in `a`
    public static List<Resolution> resolveAll(FileSchema schema, String path) {
        Walk walk = new Walk(path);
        walk.descend(schema.getRootNode(), 0, -1, null, "");
        for (String split : walk.splits) {
            if (!split.equals(walk.splits.getFirst())) {
                throw ambiguous(path);
            }
        }
        return walk.matches;
    }

    private static IllegalArgumentException ambiguous(String path) {
        return new IllegalArgumentException("Column name '" + path
                + "' is ambiguous: it is the dot-separated path of more than one field in the schema");
    }

    /// The state of one walk: the node reached so far, and why the walk failed if it reaches none.
    private static final class Walk {

        private final String path;
        private final List<Resolution> matches = new ArrayList<>();
        /// Per match, the positions in `path` at which the walk took a dot as a level boundary
        private final List<String> splits = new ArrayList<>();
        private int topLevelChildIndex = -1;
        private boolean blockedByPrimitive;

        private Walk(String path) {
            this.path = path;
        }

        /// Follows every child of `group` whose name matches `path` from `start` up to a dot or
        /// the end of the path.
        private void descend(SchemaNode.GroupNode group, int start, int topLevel, String variantAncestor,
                             String split) {
            List<SchemaNode> children = group.children();
            for (int i = 0; i < children.size(); i++) {
                SchemaNode child = children.get(i);
                int end = endOfName(child.name(), start);
                if (end < 0) {
                    continue;
                }
                int childTopLevel = topLevel < 0 ? i : topLevel;
                if (topLevelChildIndex < 0) {
                    topLevelChildIndex = childTopLevel;
                }
                if (end == path.length()) {
                    matches.add(new Resolution(child, childTopLevel, false, variantAncestor));
                    splits.add(split);
                }
                else if (child instanceof SchemaNode.GroupNode childGroup) {
                    descend(childGroup, end + 1, childTopLevel,
                            childGroup.isVariant() ? path.substring(0, end) : variantAncestor, split + end + ",");
                }
                else {
                    blockedByPrimitive = true;
                }
            }
        }

        /// The position after `name` in `path` when `path` holds it at `start` followed by a dot
        /// or the end of the path, or `-1` otherwise.
        private int endOfName(String name, int start) {
            if (!path.startsWith(name, start)) {
                return -1;
            }
            int end = start + name.length();
            return end == path.length() || path.charAt(end) == '.' ? end : -1;
        }
    }
}
