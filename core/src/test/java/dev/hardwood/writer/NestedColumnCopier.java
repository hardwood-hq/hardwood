/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import dev.hardwood.Validity;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.LayerKind;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

/// Copies a nested file column by column, reading through [ColumnReader]'s layer model and
/// writing through [ColumnBatch]'s group verbs — the generic bridge between the two columnar
/// APIs, shared by every test that copies a nested file rather than round-tripping through one
/// side of it.
///
/// It bridges the four places the two APIs describe nesting differently, each of which is a
/// silent wrong answer or an exception if a caller assumes otherwise:
///
/// - **Layers do not name their group.** `getLayerKind(k)` reports `STRUCT` or `REPEATED` but
///   not which group layer `k` is, and the writer's only group address is a dot path. So
///   `layerGroupsOf` re-derives the correspondence from the schema, reapplying the rule
///   [LayerKind] documents — including skipping the synthetic `list` / `key_value` segment,
///   which is a path segment but not a layer.
/// - **`LayerKind` does not separate `LIST` from `MAP`**, but the writer has distinct `list`
///   and `map` verbs that reject each other's groups, so the kind comes from the schema too.
/// - **Layers are per leaf, group verbs are per group.** Two leaves under one struct both
///   report it, and setting a group twice is rejected, so shared ancestors need de-duplicating.
/// - **Leaf validity is not the writer's leaf null mask.** `getLeafValidity()` answers "is
///   there a value in this slot", which is false wherever an `OPTIONAL` ancestor is absent, so
///   a `REQUIRED` leaf under an `OPTIONAL` struct reports nulls — and the writer refuses a mask
///   on a `REQUIRED` column.
///
/// Binary leaves are copied in the reader's packed form (`getBinaryValues()` /
/// `getBinaryOffsets()`), which spans an absent slot as an ordinary slice rather than a Java
/// `null`, so the all-present setter takes a `REQUIRED` leaf's slots as they are. A binary
/// `DECIMAL` is the exception: its zero-byte span at an absent slot is no unscaled value, and
/// the setter refuses it.
final class NestedColumnCopier {

    private NestedColumnCopier() {
    }

    /// Copies every batch `reader` hands out through `writer`, and returns how many there were.
    static int copy(ParquetFileReader reader, FileSchema schema, ParquetFileWriter writer) throws Exception {
        int batches = 0;
        try (ColumnReaders readers = reader.columnReaders(ColumnProjection.all())) {
            List<List<GroupRef>> layerGroups = deriveLayerGroups(schema);
            while (readers.nextBatch()) {
                batches++;
                writer.columnWriter().writeBatch(batch -> {
                    Set<String> alreadySet = new HashSet<>();
                    for (int c = 0; c < schema.getColumnCount(); c++) {
                        ColumnReader col = readers.getColumnReader(c);
                        List<GroupRef> ancestors = layerGroups.get(c);
                        for (int l = 0; l < col.getLayerCount(); l++) {
                            GroupRef g = ancestors.get(l);
                            if (!alreadySet.add(g.path)) {
                                continue;
                            }
                            if (col.getLayerKind(l) == LayerKind.STRUCT) {
                                batch.struct(g.path, col.getLayerValidity(l));
                            }
                            else if (!g.optional) {
                                // Same asymmetry as the leaf: a REQUIRED list/map refuses a
                                // mask, so the two-argument form is the only one that fits.
                                if (g.isMap) {
                                    batch.map(g.path, col.getLayerOffsets(l));
                                }
                                else {
                                    batch.list(g.path, col.getLayerOffsets(l));
                                }
                            }
                            else if (g.isMap) {
                                batch.map(g.path, col.getLayerOffsets(l), col.getLayerValidity(l));
                            }
                            else {
                                batch.list(g.path, col.getLayerOffsets(l), col.getLayerValidity(l));
                            }
                        }
                        copyLeaf(batch, col, c);
                    }
                });
            }
        }
        return batches;
    }

    private static void copyLeaf(ColumnBatch batch, ColumnReader col, int index) {
        Validity nulls = col.getLeafValidity();
        // The reader's leaf validity answers "is there a value in this slot", which is false
        // wherever an OPTIONAL ancestor is absent — so a REQUIRED leaf under an OPTIONAL
        // struct reports nulls. The writer's leaf mask answers "is this column's own value
        // null" and is refused outright on a REQUIRED column. Different quantities: the
        // branch has to be on the column's repetition, not on whether the mask has bits set.
        if (col.getColumnSchema().repetitionType() != RepetitionType.OPTIONAL) {
            switch (col.getColumnSchema().type()) {
                case INT32 -> batch.ints(index, col.getInts());
                case INT64 -> batch.longs(index, col.getLongs());
                case FLOAT -> batch.floats(index, col.getFloats());
                case DOUBLE -> batch.doubles(index, col.getDoubles());
                case BOOLEAN -> batch.booleans(index, col.getBooleans());
                case BYTE_ARRAY -> batch.bytes(index, col.getBinaryValues(), col.getBinaryOffsets());
                case FIXED_LEN_BYTE_ARRAY -> batch.fixed(index, col.getBinaryValues(), col.getBinaryOffsets());
                default -> throw new UnsupportedOperationException("INT96 is not writable");
            }
            return;
        }
        switch (col.getColumnSchema().type()) {
            case INT32 -> batch.ints(index, col.getInts(), nulls);
            case INT64 -> batch.longs(index, col.getLongs(), nulls);
            case FLOAT -> batch.floats(index, col.getFloats(), nulls);
            case DOUBLE -> batch.doubles(index, col.getDoubles(), nulls);
            case BOOLEAN -> batch.booleans(index, col.getBooleans(), nulls);
            case BYTE_ARRAY -> batch.bytes(index, col.getBinaryValues(), col.getBinaryOffsets(), nulls);
            case FIXED_LEN_BYTE_ARRAY -> batch.fixed(index, col.getBinaryValues(), col.getBinaryOffsets(), nulls);
            default -> throw new UnsupportedOperationException("INT96 is not writable");
        }
    }

    /// A group the writer must be told about, and which layer of which leaf carries its data.
    private record GroupRef(String path, boolean isMap, boolean optional) {}

    /// Re-derives, per leaf column, the ordered groups its layers correspond to — the rule
    /// `LayerKind` documents: an `OPTIONAL` group or a `LIST`/`MAP` group contributes a layer,
    /// a `REQUIRED` group and the synthetic scaffolding inside a `LIST`/`MAP` do not.
    private static List<List<GroupRef>> deriveLayerGroups(FileSchema schema) {
        List<List<GroupRef>> perColumn = new ArrayList<>();
        for (ColumnSchema column : schema.getColumns()) {
            perColumn.add(layerGroupsOf(schema, column.fieldPath()));
        }
        return perColumn;
    }

    private static List<GroupRef> layerGroupsOf(FileSchema schema, FieldPath leafPath) {
        List<GroupRef> groups = new ArrayList<>();
        SchemaNode node = schema.getRootNode();
        StringBuilder path = new StringBuilder();
        List<String> elements = leafPath.elements();
        // Every element but the leaf itself is a group on the chain.
        for (int i = 0; i < elements.size() - 1; i++) {
            String segment = elements.get(i);
            node = childNamed(node, segment);
            if (!(node instanceof SchemaNode.GroupNode group)) {
                throw new IllegalStateException("Not a group: " + segment);
            }
            if (!path.isEmpty()) {
                path.append('.');
            }
            path.append(segment);
            if (group.isList() || group.isMap()) {
                groups.add(new GroupRef(path.toString(), group.isMap(),
                        group.repetitionType() == RepetitionType.OPTIONAL));
                // Skip the synthetic repeated group: it contributes no layer, but it IS a
                // path segment, so the next iteration must not append it to the group path.
                i++;
                String synthetic = elements.get(i);
                node = childNamed(node, synthetic);
                path.append('.').append(synthetic);
            }
            else if (group.repetitionType() == RepetitionType.OPTIONAL) {
                groups.add(new GroupRef(path.toString(), false, true));
            }
        }
        return groups;
    }

    private static SchemaNode childNamed(SchemaNode node, String name) {
        for (SchemaNode child : ((SchemaNode.GroupNode) node).children()) {
            if (child.name().equals(name)) {
                return child;
            }
        }
        throw new IllegalStateException("No child " + name);
    }
}
