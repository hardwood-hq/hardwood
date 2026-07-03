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

/// Shreds a batch's per-layer validity and offsets into each leaf column's repetition and
/// definition level streams — the write-side inverse of the reader's `NestedLevelComputer`.
///
/// A leaf's path is modelled as an ordered list of layers, outermost first: one per
/// `OPTIONAL` `struct` group (`STRUCT`) and one per `LIST` group (`REPEATED`); `REQUIRED`
/// groups and the synthetic `repeated` scaffolding of a list contribute no layer, only
/// definition/repetition depth. For each top-level record the shredder descends the layers
/// driven by the caller's validity and offsets, emitting one `(rep, def)` pair per leaf
/// slot including the phantom slots that mark a null struct, a null list, or an empty list.
/// A value is present exactly at `def == maxDefinitionLevel`.
///
/// Shredding a whole batch produces, per column, the level streams plus `recordStart` /
/// `valueStart` prefix arrays so the writer can band the batch into row groups by record
/// while cutting pages by level count.
public final class RecordShredder {

    /// A `STRUCT` or `REPEATED` step on a leaf's path, carrying the definition/repetition
    /// contributions and the batch-input key (the group's dotted path).
    private record Layer(Kind kind, String key, boolean nullable, int presentDefInc,
                         int contentDefInc, int repLevel) {
        enum Kind { STRUCT, REPEATED }
    }

    private final Layer[][] layers;
    private final boolean[] leafOptional;
    private final int[] maxDef;
    private final int[] maxRep;

    // Per-batch binding.
    private Validity[] leafValidities;
    private Map<String, Validity> structValidities;
    private Map<String, Validity> listValidities;
    private Map<String, int[]> listOffsets;
    private IntColumnSource[] sources;
    private int recordCount;

    // Reusable per-column shred scratch.
    private final IntList repBuf = new IntList();
    private final IntList defBuf = new IntList();
    private final IntList valueBuf = new IntList();

    public RecordShredder(FileSchema schema) {
        int columnCount = schema.getColumnCount();
        this.layers = new Layer[columnCount][];
        this.leafOptional = new boolean[columnCount];
        this.maxDef = new int[columnCount];
        this.maxRep = new int[columnCount];
        walk(schema.getRootNode(), new ArrayList<>(), new ArrayList<>(), false);
        for (int c = 0; c < columnCount; c++) {
            ColumnSchema column = schema.getColumn(c);
            maxDef[c] = column.maxDefinitionLevel();
            maxRep[c] = column.maxRepetitionLevel();
        }
    }

    /// Classifies each group on the way to a leaf into the layer list the shredder walks.
    private void walk(SchemaNode.GroupNode group, List<String> path, List<Layer> layerStack,
                      boolean insideListScaffolding) {
        for (SchemaNode child : group.children()) {
            switch (child) {
                case SchemaNode.PrimitiveNode leaf -> {
                    layers[leaf.columnIndex()] = layerStack.toArray(new Layer[0]);
                    leafOptional[leaf.columnIndex()] = leaf.repetitionType() == RepetitionType.OPTIONAL;
                }
                case SchemaNode.GroupNode nested -> {
                    path.add(nested.name());
                    Layer added = classify(nested, String.join(".", path), layerStack, insideListScaffolding);
                    if (added != null) {
                        layerStack.add(added);
                    }
                    walk(nested, path, layerStack, nested.isList());
                    if (added != null) {
                        layerStack.remove(layerStack.size() - 1);
                    }
                    path.remove(path.size() - 1);
                }
            }
        }
    }

    private static Layer classify(SchemaNode.GroupNode group, String path, List<Layer> layerStack,
                                  boolean insideListScaffolding) {
        if (group.isList()) {
            boolean nullable = group.repetitionType() == RepetitionType.OPTIONAL;
            int repDepth = 1;
            for (Layer layer : layerStack) {
                if (layer.kind() == Layer.Kind.REPEATED) {
                    repDepth++;
                }
            }
            return new Layer(Layer.Kind.REPEATED, path, nullable, nullable ? 1 : 0, 1, repDepth);
        }
        if (insideListScaffolding && group.repetitionType() == RepetitionType.REPEATED) {
            return null; // synthetic `repeated group list` inside a LIST — no layer
        }
        if (group.repetitionType() == RepetitionType.OPTIONAL) {
            return new Layer(Layer.Kind.STRUCT, path, true, 1, 0, 0);
        }
        if (group.repetitionType() == RepetitionType.REQUIRED) {
            return null; // required struct — no layer, no level contribution
        }
        throw new IllegalStateException("Unsupported repeated group at " + path + " (not LIST/MAP annotated)");
    }

    /// Binds the shredder to one batch's inputs and derives the record count.
    public void bind(IntColumnSource[] sources, Validity[] leafValidities,
                     Map<String, Validity> structValidities, Map<String, Validity> listValidities,
                     Map<String, int[]> listOffsets) {
        this.sources = sources;
        this.leafValidities = leafValidities;
        this.structValidities = structValidities;
        this.listValidities = listValidities;
        this.listOffsets = listOffsets;
        this.recordCount = deriveRecordCount();
    }

    public int recordCount() {
        return recordCount;
    }

    /// The record count implied by column 0, walking its layers from leaf to root: each
    /// `REPEATED` layer replaces the running count with its parent count (`offsets.length -
    /// 1`), each `STRUCT` layer preserves it. A `STRUCT` layer enclosing a repeated field
    /// would break this invariant and is rejected, since its offset scope would not be the
    /// record scope.
    private int deriveRecordCount() {
        Layer[] path = layers[0];
        int count = sources[0].size();
        boolean seenRepeated = false;
        for (int k = path.length - 1; k >= 0; k--) {
            Layer layer = path[k];
            if (layer.kind() == Layer.Kind.REPEATED) {
                count = offsetsFor(layer).length - 1;
                seenRepeated = true;
            }
            else if (seenRepeated && layer.nullable()) {
                throw new IllegalArgumentException("A nullable struct enclosing a repeated field ("
                        + layer.key() + ") is not yet supported by the writer");
            }
        }
        return count;
    }

    /// Shreds column `columnIndex` over the whole bound batch.
    public ColumnLevels shred(int columnIndex) {
        Layer[] path = layers[columnIndex];
        boolean hasRep = maxRep[columnIndex] > 0;
        boolean hasDef = maxDef[columnIndex] > 0;

        repBuf.clear();
        defBuf.clear();
        valueBuf.clear();
        int[] recordStart = new int[recordCount + 1];
        int[] valueStart = new int[recordCount + 1];
        Ctx ctx = new Ctx(columnIndex, path, maxDef[columnIndex], hasRep, hasDef, materialize(columnIndex));

        for (int r = 0; r < recordCount; r++) {
            recordStart[r] = ctx.triples;
            valueStart[r] = ctx.present;
            emit(ctx, 0, r, 0, 0);
        }
        recordStart[recordCount] = ctx.triples;
        valueStart[recordCount] = ctx.present;

        return new ColumnLevels(
                hasRep ? repBuf.toArray() : null,
                hasDef ? defBuf.toArray() : null,
                valueBuf.toArray(), recordStart, valueStart, ctx.triples);
    }

    /// Copies a column's leaf source into a flat array indexed by the leaf's item space
    /// (record index for a struct-only chain, element index under a list), so the shred can
    /// read `source[itemIndex]` at each present leaf.
    private int[] materialize(int columnIndex) {
        IntColumnSource source = sources[columnIndex];
        int[] all = new int[source.size()];
        source.copyInto(0, all, 0, all.length);
        return all;
    }

    /// Emits the `(rep, def)` pairs for the subtree rooted at `layerIndex` for the item at
    /// `itemIndex` in that layer's scope. `parentDef` is the level contributed by the
    /// present ancestors so far; `repToEmit` is the repetition level of the first pair.
    private void emit(Ctx ctx, int layerIndex, int itemIndex, int parentDef, int repToEmit) {
        if (layerIndex == ctx.path.length) {
            boolean present = !(leafOptional[ctx.columnIndex] && isNull(leafValidities[ctx.columnIndex], itemIndex));
            addTriple(ctx, repToEmit, present ? ctx.maxDef : parentDef);
            if (present) {
                valueBuf.add(ctx.sourceAll[itemIndex]);
                ctx.present++;
            }
            return;
        }
        Layer layer = ctx.path[layerIndex];
        if (layer.kind() == Layer.Kind.STRUCT) {
            if (layer.nullable() && isNull(structValidities.get(layer.key()), itemIndex)) {
                addTriple(ctx, repToEmit, parentDef);
            }
            else {
                emit(ctx, layerIndex + 1, itemIndex, parentDef + layer.presentDefInc(), repToEmit);
            }
            return;
        }
        // REPEATED (list).
        if (layer.nullable() && isNull(listValidities.get(layer.key()), itemIndex)) {
            addTriple(ctx, repToEmit, parentDef); // null list — outer group absent
            return;
        }
        int[] offsets = offsetsFor(layer);
        int start = offsets[itemIndex];
        int end = offsets[itemIndex + 1];
        int listDef = parentDef + layer.presentDefInc();
        if (start == end) {
            addTriple(ctx, repToEmit, listDef); // empty list — present but no elements
            return;
        }
        int childDef = listDef + layer.contentDefInc();
        for (int j = start; j < end; j++) {
            emit(ctx, layerIndex + 1, j, childDef, j == start ? repToEmit : layer.repLevel());
        }
    }

    private void addTriple(Ctx ctx, int rep, int def) {
        if (ctx.hasRep) {
            repBuf.add(rep);
        }
        if (ctx.hasDef) {
            defBuf.add(def);
        }
        ctx.triples++;
    }

    private int[] offsetsFor(Layer layer) {
        int[] offsets = listOffsets.get(layer.key());
        if (offsets == null) {
            throw new IllegalArgumentException("Missing offsets for list " + layer.key());
        }
        return offsets;
    }

    private static boolean isNull(Validity validity, int index) {
        return validity != null && validity.isNull(index);
    }

    /// Per-column shred state, threaded through the recursion to avoid re-allocating.
    private static final class Ctx {
        final int columnIndex;
        final Layer[] path;
        final int maxDef;
        final boolean hasRep;
        final boolean hasDef;
        final int[] sourceAll;
        int triples;
        int present;

        Ctx(int columnIndex, Layer[] path, int maxDef, boolean hasRep, boolean hasDef, int[] sourceAll) {
            this.columnIndex = columnIndex;
            this.path = path;
            this.maxDef = maxDef;
            this.hasRep = hasRep;
            this.hasDef = hasDef;
            this.sourceAll = sourceAll;
        }
    }

    /// The shredded level streams for one column over a whole batch.
    ///
    /// @param repetitionLevels one per level triple, or `null` when `maxRepetitionLevel == 0`
    /// @param definitionLevels one per level triple, or `null` when `maxDefinitionLevel == 0`
    /// @param values the present leaf values in order
    /// @param recordStart triple index at which each record begins, length `recordCount + 1`
    /// @param valueStart present-value index at which each record begins, length `recordCount + 1`
    /// @param totalTriples total number of level triples across the batch
    public record ColumnLevels(int[] repetitionLevels, int[] definitionLevels, int[] values,
                               int[] recordStart, int[] valueStart, int totalTriples) {}

    /// A minimal growable `int` buffer, avoiding the boxing of a `List<Integer>`.
    private static final class IntList {
        private int[] values = new int[16];
        private int size;

        void add(int value) {
            if (size == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
            values[size++] = value;
        }

        void clear() {
            size = 0;
        }

        int[] toArray() {
            return Arrays.copyOf(values, size);
        }
    }
}
