/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

import java.util.List;

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
/// name is ambiguous and resolving it fails.
///
/// Path segments are compared in place, so no array is materialized for the path.
public final class SchemaPathResolver {

    private SchemaPathResolver() {
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
        walk.descend(root, 0, -1, null);
        if (walk.match != null) {
            return walk.match;
        }
        return new Resolution(null, walk.topLevelChildIndex, walk.blockedByPrimitive, null);
    }

    /// The state of one walk: the node reached so far, and why the walk failed if it reaches none.
    private static final class Walk {

        private final String path;
        private Resolution match;
        private int topLevelChildIndex = -1;
        private boolean blockedByPrimitive;

        private Walk(String path) {
            this.path = path;
        }

        /// Follows every child of `group` whose name matches `path` from `start` up to a dot or
        /// the end of the path.
        private void descend(SchemaNode.GroupNode group, int start, int topLevel, String variantAncestor) {
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
                    found(new Resolution(child, childTopLevel, false, variantAncestor));
                }
                else if (child instanceof SchemaNode.GroupNode childGroup) {
                    descend(childGroup, end + 1, childTopLevel,
                            childGroup.isVariant() ? path.substring(0, end) : variantAncestor);
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

        private void found(Resolution resolution) {
            if (match != null) {
                throw new IllegalArgumentException("Column name '" + path
                        + "' is ambiguous: it is the dot-separated path of more than one field in the schema");
            }
            match = resolution;
        }
    }
}
