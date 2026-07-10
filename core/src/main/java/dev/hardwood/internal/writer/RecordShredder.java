/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.util.ArrayList;
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
/// Shredding is **streaming**: [#shred] walks a range of records and pushes each level entry
/// into a [LevelSink] that seals pages as they fill, so a record is shredded on demand
/// rather than materialized. Leaf values are read through a bounded sliding window over the
/// source, so nothing scales with batch size. Records are processed in order and each
/// column's value cursor advances monotonically, so a batch spanning several row groups
/// shreds continuously.
public final class RecordShredder {

    /// Receives one level entry at a time. `present` is true when the leaf value at this
    /// position exists, in which case `value` is that value; otherwise `value` is ignored.
    public interface LevelSink {
        void accept(int repetitionLevel, int definitionLevel, boolean present, int value);
    }

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
    private final String[] columnNames;
    private final ValueWindow[] windows;

    // Per-batch binding.
    private Validity[] leafValidities;
    private Map<String, Validity> structValidities;
    private Map<String, Validity> listValidities;
    private Map<String, int[]> listOffsets;
    private IntColumnSource[] sources;
    private int recordCount;

    /// @param schema the file schema
    /// @param valueWindowCapacity the size of each column's leaf-value read window, in values
    public RecordShredder(FileSchema schema, int valueWindowCapacity) {
        int columnCount = schema.getColumnCount();
        this.layers = new Layer[columnCount][];
        this.leafOptional = new boolean[columnCount];
        this.maxDef = new int[columnCount];
        this.maxRep = new int[columnCount];
        this.columnNames = new String[columnCount];
        this.windows = new ValueWindow[columnCount];
        walk(schema.getRootNode(), new ArrayList<>(), new ArrayList<>(), false);
        for (int c = 0; c < columnCount; c++) {
            ColumnSchema column = schema.getColumn(c);
            maxDef[c] = column.maxDefinitionLevel();
            maxRep[c] = column.maxRepetitionLevel();
            columnNames[c] = column.fieldPath().toString();
            windows[c] = new ValueWindow(valueWindowCapacity);
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

    /// Binds the shredder to one batch's inputs, validates them, and derives the record
    /// count. Each column's value window is reset to the batch's source.
    public void bind(IntColumnSource[] sources, Validity[] leafValidities,
                     Map<String, Validity> structValidities, Map<String, Validity> listValidities,
                     Map<String, int[]> listOffsets) {
        this.sources = sources;
        this.leafValidities = leafValidities;
        this.structValidities = structValidities;
        this.listValidities = listValidities;
        this.listOffsets = listOffsets;
        this.recordCount = validateAndDeriveRecordCount();
        for (int c = 0; c < windows.length; c++) {
            windows[c].reset(sources[c]);
        }
    }

    public int recordCount() {
        return recordCount;
    }

    /// Shreds records `[from, from + count)` of column `columnIndex`, pushing each level
    /// entry into `sink`. Records must be shredded in order, since the column's value window
    /// advances monotonically across calls.
    public void shred(int columnIndex, int from, int count, LevelSink sink) {
        Ctx ctx = new Ctx(columnIndex, layers[columnIndex], maxDef[columnIndex], sink, windows[columnIndex]);
        int end = from + count;
        for (int r = from; r < end; r++) {
            emit(ctx, 0, r, 0, 0);
        }
    }

    /// Emits the `(rep, def)` pairs for the subtree rooted at `layerIndex` for the item at
    /// `itemIndex` in that layer's scope. `parentDef` is the level contributed by the
    /// present ancestors so far; `repToEmit` is the repetition level of the first pair.
    private void emit(Ctx ctx, int layerIndex, int itemIndex, int parentDef, int repToEmit) {
        if (layerIndex == ctx.path.length) {
            if (leafOptional[ctx.columnIndex] && isNull(leafValidities[ctx.columnIndex], itemIndex)) {
                ctx.sink.accept(repToEmit, parentDef, false, 0);
            }
            else {
                ctx.sink.accept(repToEmit, ctx.maxDef, true, ctx.window.at(itemIndex));
            }
            return;
        }
        Layer layer = ctx.path[layerIndex];
        if (layer.kind() == Layer.Kind.STRUCT) {
            if (layer.nullable() && isNull(structValidities.get(layer.key()), itemIndex)) {
                ctx.sink.accept(repToEmit, parentDef, false, 0);
            }
            else {
                emit(ctx, layerIndex + 1, itemIndex, parentDef + layer.presentDefInc(), repToEmit);
            }
            return;
        }
        // REPEATED (list).
        if (layer.nullable() && isNull(listValidities.get(layer.key()), itemIndex)) {
            ctx.sink.accept(repToEmit, parentDef, false, 0); // null list — outer group absent
            return;
        }
        int[] offsets = offsetsFor(layer);
        int start = offsets[itemIndex];
        int end = offsets[itemIndex + 1];
        int listDef = parentDef + layer.presentDefInc();
        if (start == end) {
            ctx.sink.accept(repToEmit, listDef, false, 0); // empty list — present but no elements
            return;
        }
        int childDef = listDef + layer.contentDefInc();
        for (int j = start; j < end; j++) {
            emit(ctx, layerIndex + 1, j, childDef, j == start ? repToEmit : layer.repLevel());
        }
    }

    /// Validates every column's per-layer inputs and derives the batch's record count,
    /// rejecting a ragged nested batch eagerly. Column 0 sets the record count; every other
    /// column must imply the same, so a short or long column — flat or nested — is caught.
    private int validateAndDeriveRecordCount() {
        int records = impliedRecordCount(0);
        for (int c = 1; c < layers.length; c++) {
            int implied = impliedRecordCount(c);
            if (implied != records) {
                throw new IllegalArgumentException("Column " + columnNames[c] + " implies " + implied
                        + " records but the batch has " + records);
            }
        }
        return records;
    }

    /// The record count implied by one column, walking its layers from leaf to root while
    /// validating the offset chain: each `REPEATED` layer replaces the running count with its
    /// parent count (`offsets.length - 1`), each `STRUCT` layer preserves it. A `STRUCT`
    /// layer enclosing a repeated field would break this invariant and is rejected, since its
    /// offset scope would not be the record scope.
    private int impliedRecordCount(int columnIndex) {
        Layer[] path = layers[columnIndex];
        int count = sources[columnIndex].size();
        boolean seenRepeated = false;
        for (int k = path.length - 1; k >= 0; k--) {
            Layer layer = path[k];
            if (layer.kind() == Layer.Kind.REPEATED) {
                int[] offsets = offsetsFor(layer);
                validateOffsets(offsets, count, layer.key());
                validateNullListsEmpty(offsets, layer.key());
                count = offsets.length - 1;
                seenRepeated = true;
            }
            else if (seenRepeated && layer.nullable()) {
                throw new IllegalArgumentException("A nullable struct enclosing a repeated field ("
                        + layer.key() + ") is not yet supported by the writer");
            }
        }
        return count;
    }

    /// Checks a list's entry offsets: they start at 0, are non-decreasing, and end at the
    /// item count of the layer they index into, so the shred never indexes out of range or
    /// silently drops or duplicates entries.
    private static void validateOffsets(int[] offsets, int innerCount, String key) {
        if (offsets[0] != 0) {
            throw new IllegalArgumentException("List " + key + " offsets must start at 0 but start at " + offsets[0]);
        }
        for (int i = 1; i < offsets.length; i++) {
            if (offsets[i] < offsets[i - 1]) {
                throw new IllegalArgumentException("List " + key + " offsets are not non-decreasing at index " + i);
            }
        }
        int last = offsets[offsets.length - 1];
        if (last != innerCount) {
            throw new IllegalArgumentException("List " + key + " offsets end at " + last
                    + " but its contents have " + innerCount + " items");
        }
    }

    /// Rejects a null list whose offsets span elements: a null list is absent, so its
    /// element delta must be zero. Without this the shredder takes the null branch and
    /// silently drops the stray elements, producing a plausible but wrong file. The
    /// validity's length is not checked — [Validity] is intentionally length-less — so only
    /// the null-positions-within-range are verified.
    private void validateNullListsEmpty(int[] offsets, String key) {
        Validity validity = listValidities.get(key);
        if (validity == null) {
            return;
        }
        int parentCount = offsets.length - 1;
        for (int i = validity.nextNull(0, parentCount); i != -1; i = validity.nextNull(i + 1, parentCount)) {
            if (offsets[i + 1] != offsets[i]) {
                throw new IllegalArgumentException("List " + key + " is null at index " + i
                        + " but its offsets span " + (offsets[i + 1] - offsets[i])
                        + " elements; a null list has none");
            }
        }
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

    /// Per-column shred state, threaded through the recursion.
    private static final class Ctx {
        final int columnIndex;
        final Layer[] path;
        final int maxDef;
        final LevelSink sink;
        final ValueWindow window;

        Ctx(int columnIndex, Layer[] path, int maxDef, LevelSink sink, ValueWindow window) {
            this.columnIndex = columnIndex;
            this.path = path;
            this.maxDef = maxDef;
            this.sink = sink;
            this.window = window;
        }
    }

    /// A bounded, forward-only view over a column's leaf source. `at` is called with
    /// monotonically non-decreasing indices; the window slides forward and refills through a
    /// single bulk `copyInto`, so a foreign columnar source is read in page-sized chunks
    /// rather than one value at a time or copied whole.
    private static final class ValueWindow {
        private final int[] buffer;
        private IntColumnSource source;
        private int size;
        private int base;
        private int length;

        ValueWindow(int capacity) {
            this.buffer = new int[Math.max(1, capacity)];
        }

        void reset(IntColumnSource source) {
            this.source = source;
            this.size = source.size();
            this.base = 0;
            this.length = 0;
        }

        int at(int index) {
            if (index >= base + length) {
                base = index;
                length = Math.min(buffer.length, size - base);
                source.copyInto(base, buffer, 0, length);
            }
            return buffer[index - base];
        }
    }
}
