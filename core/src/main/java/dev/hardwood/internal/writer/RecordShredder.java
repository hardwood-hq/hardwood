/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import dev.hardwood.Validity;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

/// Computes a column's definition levels from the per-layer validity a batch carries —
/// the write-side inverse of the reader's `NestedLevelComputer`. This increment covers
/// struct nesting (`REQUIRED` / `OPTIONAL` groups); repetition levels and offset-driven
/// list/map input arrive with the later shredding increments.
///
/// Each leaf's definition level at a row is the number of `OPTIONAL` nodes on its path
/// that are present, counting from the root and stopping at the first absent one: an
/// absent ancestor struct hides everything beneath it. A row's value is present exactly
/// when its level reaches the leaf's `maxDefinitionLevel`.
///
/// The shredder is bound to the current batch's validity, then queried per page-sized
/// range so the definition-level buffer stays bounded to one page rather than the whole
/// batch, mirroring the value seam.
public final class RecordShredder {

    /// For each leaf column, the paths of its `OPTIONAL` group ancestors, outermost
    /// first. Empty for a column with no nullable ancestor.
    private final String[][] optionalAncestorPaths;
    private final boolean[] leafOptional;
    private final int[] maxDefLevel;

    // Per-batch binding, resolved in bind().
    private final Validity[][] ancestorValidities;
    private Validity[] leafValidities;

    public RecordShredder(FileSchema schema) {
        int columnCount = schema.getColumnCount();
        this.optionalAncestorPaths = new String[columnCount][];
        this.leafOptional = new boolean[columnCount];
        this.maxDefLevel = new int[columnCount];
        this.ancestorValidities = new Validity[columnCount][];
        walk(schema.getRootNode(), new ArrayList<>(), new ArrayList<>());

        for (int c = 0; c < columnCount; c++) {
            ColumnSchema column = schema.getColumn(c);
            if (column.maxRepetitionLevel() != 0) {
                throw new IllegalStateException(
                        "RecordShredder handles struct nesting only; column " + column.name() + " is repeated");
            }
            maxDefLevel[c] = column.maxDefinitionLevel();
            int expected = optionalAncestorPaths[c].length + (leafOptional[c] ? 1 : 0);
            if (expected != maxDefLevel[c]) {
                throw new IllegalStateException("Definition-level mismatch for column " + column.name()
                        + ": schema says " + maxDefLevel[c] + " but the path yields " + expected);
            }
        }
    }

    /// Records each leaf's optional-ancestor paths by walking the schema tree, tracking
    /// the current group path and the subset of those groups that are `OPTIONAL`.
    private void walk(SchemaNode.GroupNode group, List<String> pathStack, List<String> optionalStack) {
        for (SchemaNode child : group.children()) {
            switch (child) {
                case SchemaNode.PrimitiveNode leaf -> {
                    optionalAncestorPaths[leaf.columnIndex()] = optionalStack.toArray(new String[0]);
                    leafOptional[leaf.columnIndex()] = leaf.repetitionType() == RepetitionType.OPTIONAL;
                }
                case SchemaNode.GroupNode nested -> {
                    pathStack.add(nested.name());
                    boolean optional = nested.repetitionType() == RepetitionType.OPTIONAL;
                    String path = String.join(".", pathStack);
                    if (optional) {
                        optionalStack.add(path);
                    }
                    walk(nested, pathStack, optionalStack);
                    if (optional) {
                        optionalStack.remove(optionalStack.size() - 1);
                    }
                    pathStack.remove(pathStack.size() - 1);
                }
            }
        }
    }

    /// Binds the shredder to one batch's validity. `leafValidities[c]` is the leaf null
    /// bitmap (or `null` when every leaf is present); `structValidities` maps a `STRUCT`
    /// layer's path to its validity, absent entries meaning all instances present.
    public void bind(Validity[] leafValidities, Map<String, Validity> structValidities) {
        this.leafValidities = leafValidities;
        for (int c = 0; c < ancestorValidities.length; c++) {
            String[] paths = optionalAncestorPaths[c];
            Validity[] resolved = ancestorValidities[c];
            if (resolved == null || resolved.length != paths.length) {
                resolved = new Validity[paths.length];
                ancestorValidities[c] = resolved;
            }
            for (int a = 0; a < paths.length; a++) {
                Validity v = structValidities.get(paths[a]);
                resolved[a] = v != null ? v : Validity.NO_NULLS;
            }
        }
    }

    /// Fills `dest[destPos .. destPos+count)` with the definition levels of rows
    /// `[from, from+count)` of the bound batch for leaf column `columnIndex`.
    public void fillDefinitionLevels(int columnIndex, int from, int count, int[] dest, int destPos) {
        int maxDef = maxDefLevel[columnIndex];
        Arrays.fill(dest, destPos, destPos + count, maxDef);
        int end = from + count;

        // An absent ancestor at 0-based position `level` means `level` outer ancestors
        // are present and this one is not, so the definition level caps at `level`.
        // Taking the minimum makes the outer (smaller) cap win regardless of order.
        Validity[] ancestors = ancestorValidities[columnIndex];
        for (int level = 0; level < ancestors.length; level++) {
            capAtNulls(ancestors[level], from, end, dest, destPos, level);
        }

        // A null leaf under fully-present ancestors caps one below the max; where an
        // ancestor already dropped the level lower, the minimum keeps that.
        if (leafOptional[columnIndex]) {
            capAtNulls(leafValidities[columnIndex], from, end, dest, destPos, maxDef - 1);
        }
    }

    private static void capAtNulls(Validity validity, int from, int end, int[] dest, int destPos, int cap) {
        if (validity == null) {
            return;
        }
        for (int i = validity.nextNull(from, end); i != -1; i = validity.nextNull(i + 1, end)) {
            int idx = destPos + (i - from);
            if (cap < dest[idx]) {
                dest[idx] = cap;
            }
        }
    }
}
