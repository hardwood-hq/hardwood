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
    public static Resolution resolve(FileSchema schema, String path) {
        return resolve(schema.getRootNode(), path);
    }

    /// Resolves `path` against `root`, one dot-separated segment per tree level.
    public static Resolution resolve(SchemaNode.GroupNode root, String path) {
        SchemaNode current = root;
        int topLevelChildIndex = -1;
        int start = 0;
        String variantAncestor = null;

        while (true) {
            if (!(current instanceof SchemaNode.GroupNode group)) {
                return new Resolution(null, topLevelChildIndex, true, variantAncestor);
            }

            int dot = path.indexOf('.', start);
            int end = dot < 0 ? path.length() : dot;

            int childIndex = indexOfChild(group, path, start, end);
            if (childIndex < 0) {
                return new Resolution(null, topLevelChildIndex, false, variantAncestor);
            }
            if (topLevelChildIndex < 0) {
                topLevelChildIndex = childIndex;
            }
            current = group.children().get(childIndex);

            if (dot < 0) {
                return new Resolution(current, topLevelChildIndex, false, variantAncestor);
            }
            if (current instanceof SchemaNode.GroupNode child && child.isVariant()) {
                variantAncestor = path.substring(0, end);
            }
            start = dot + 1;
        }
    }

    /// Index of the child of `group` named by `path[start, end)`, or `-1` if there is none.
    private static int indexOfChild(SchemaNode.GroupNode group, String path, int start, int end) {
        int length = end - start;
        List<SchemaNode> children = group.children();
        for (int i = 0; i < children.size(); i++) {
            String name = children.get(i).name();
            if (name.length() == length && path.regionMatches(start, name, 0, length)) {
                return i;
            }
        }
        return -1;
    }
}
