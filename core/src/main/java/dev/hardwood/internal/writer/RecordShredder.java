/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import dev.hardwood.Validity;
import dev.hardwood.internal.schema.FileReferences;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

/// Shreds a batch's per-layer validity and offsets into each leaf column's repetition and
/// definition level streams — the write-side inverse of the reader's `NestedLevelComputer`.
///
/// A leaf's path is modelled as an ordered list of layers, outermost first: one per
/// `OPTIONAL` `struct` group (`STRUCT`) and one per `LIST` or `MAP` group (`REPEATED`);
/// `REQUIRED` groups and the synthetic `repeated` scaffolding of a list (`list`) or map
/// (`key_value`) contribute no layer, only definition/repetition depth. A `MAP`'s `key` and
/// `value` leaves share the one `REPEATED` layer keyed on the map group, so both are driven
/// by the same offsets. For each top-level record the shredder descends the layers
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

    /// Receives one level entry at a time. `valueIndex` is the position of the present leaf
    /// value in the column's source; a negative `valueIndex` marks an absent slot (a null
    /// leaf, a null or empty list, or a null struct ancestor), which carries no value. The
    /// shredder is value-type-agnostic: it emits source positions, and the sink reads the typed
    /// value itself.
    public interface LevelSink {
        void accept(int repetitionLevel, int definitionLevel, int valueIndex);
    }

    /// Emitted as the `valueIndex` for an absent slot.
    private static final int ABSENT = -1;

    /// A `STRUCT` or `REPEATED` step on a leaf's path, carrying the definition/repetition
    /// contributions and the batch-input key (the group's dotted path).
    ///
    /// One instance is shared by every leaf beneath the group, and its batch inputs are
    /// resolved once per [#bind] rather than looked up by key while shredding: `emit` runs per
    /// record per layer per column, so a map lookup there is paid on every value of every
    /// nested column.
    private static final class Layer {

        enum Kind { STRUCT, REPEATED }

        private final Kind kind;
        private final String key;
        private final boolean nullable;
        private final int presentDefInc;
        private final int contentDefInc;
        private final int repLevel;

        /// This layer's per-instance nulls for the bound batch, or `null` when the group is
        /// `REQUIRED` or the caller supplied none.
        private Validity validity;

        /// This layer's entry offsets for the bound batch; `null` for a `STRUCT` layer.
        private int[] offsets;

        Layer(Kind kind, String key, boolean nullable, int presentDefInc, int contentDefInc, int repLevel) {
            this.kind = kind;
            this.key = key;
            this.nullable = nullable;
            this.presentDefInc = presentDefInc;
            this.contentDefInc = contentDefInc;
            this.repLevel = repLevel;
        }

        Kind kind() {
            return kind;
        }

        String key() {
            return key;
        }

        boolean nullable() {
            return nullable;
        }

        int presentDefInc() {
            return presentDefInc;
        }

        int contentDefInc() {
            return contentDefInc;
        }

        int repLevel() {
            return repLevel;
        }
    }

    private final Layer[][] layers;

    /// Every layer once, in no particular order, so a bind resolves each one's batch inputs a
    /// single time however many leaves sit beneath it.
    private final Layer[] distinctLayers;
    private final boolean[] leafOptional;
    private final int[] maxDef;
    private final int[] maxRep;
    private final String[] columnNames;

    /// Every `FILE` group in the schema, whose values a bound batch must hold valid.
    private final List<FileGroup> fileGroups = new ArrayList<>();

    // Per-batch binding.
    private Validity[] leafValidities;
    private Map<String, Validity> structValidities;
    private Map<String, Validity> listValidities;
    private Map<String, int[]> listOffsets;
    private ColumnSource[] sources;
    private int recordCount;

    /// @param schema the file schema
    public RecordShredder(FileSchema schema) {
        int columnCount = schema.getColumnCount();
        this.layers = new Layer[columnCount][];
        this.leafOptional = new boolean[columnCount];
        this.maxDef = new int[columnCount];
        this.maxRep = new int[columnCount];
        this.columnNames = new String[columnCount];
        List<Layer> collected = new ArrayList<>();
        walk(schema.getRootNode(), new ArrayList<>(), new ArrayList<>(), false, collected);
        this.distinctLayers = collected.toArray(new Layer[0]);
        for (int c = 0; c < columnCount; c++) {
            ColumnSchema column = schema.getColumn(c);
            maxDef[c] = column.maxDefinitionLevel();
            maxRep[c] = column.maxRepetitionLevel();
            columnNames[c] = column.fieldPath().toString();
        }
    }

    /// Classifies each group on the way to a leaf into the layer list the shredder walks.
    private void walk(SchemaNode.GroupNode group, List<String> path, List<Layer> layerStack,
                      boolean insideRepeatedScaffolding, List<Layer> collected) {
        for (SchemaNode child : group.children()) {
            switch (child) {
                case SchemaNode.PrimitiveNode leaf -> {
                    layers[leaf.columnIndex()] = layerStack.toArray(new Layer[0]);
                    leafOptional[leaf.columnIndex()] = leaf.repetitionType() == RepetitionType.OPTIONAL;
                }
                case SchemaNode.GroupNode nested -> {
                    path.add(nested.name());
                    Layer added = classify(nested, String.join(".", path), layerStack, insideRepeatedScaffolding);
                    if (added != null) {
                        layerStack.add(added);
                        collected.add(added);
                    }
                    if (nested.isFile()) {
                        fileGroups.add(FileGroup.of(nested, String.join(".", path), layerStack));
                    }
                    walk(nested, path, layerStack, nested.isList() || nested.isMap(), collected);
                    if (added != null) {
                        layerStack.remove(layerStack.size() - 1);
                    }
                    path.remove(path.size() - 1);
                }
            }
        }
    }

    private static Layer classify(SchemaNode.GroupNode group, String path, List<Layer> layerStack,
                                  boolean insideRepeatedScaffolding) {
        if (group.isList() || group.isMap()) {
            boolean nullable = group.repetitionType() == RepetitionType.OPTIONAL;
            int repDepth = 1;
            for (Layer layer : layerStack) {
                if (layer.kind() == Layer.Kind.REPEATED) {
                    repDepth++;
                }
            }
            return new Layer(Layer.Kind.REPEATED, path, nullable, nullable ? 1 : 0, 1, repDepth);
        }
        if (insideRepeatedScaffolding && group.repetitionType() == RepetitionType.REPEATED) {
            return null; // synthetic `repeated group` inside a LIST (list) or MAP (key_value) — no layer
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
    public void bind(ColumnSource[] sources, Validity[] leafValidities,
                     Map<String, Validity> structValidities, Map<String, Validity> listValidities,
                     Map<String, int[]> listOffsets) {
        this.sources = sources;
        this.leafValidities = leafValidities;
        this.structValidities = structValidities;
        this.listValidities = listValidities;
        this.listOffsets = listOffsets;
        for (Layer layer : distinctLayers) {
            layer.validity = layer.kind == Layer.Kind.STRUCT
                    ? structValidities.get(layer.key)
                    : listValidities.get(layer.key);
            layer.offsets = layer.kind == Layer.Kind.REPEATED ? listOffsets.get(layer.key) : null;
        }
        this.recordCount = validateAndDeriveRecordCount();
        for (FileGroup group : fileGroups) {
            checkFileValues(group);
        }
    }

    /// Rejects the batch if a present value of `group` is not a valid reference. The group's
    /// children are its direct leaves, so value `i` of each child belongs to instance `i` of the
    /// group, which is present when no `STRUCT` layer between it and the nearest `REPEATED`
    /// layer marks `i` null: a null ancestor above that layer leaves it no entries, which
    /// [#validateAndDeriveRecordCount] has checked.
    private void checkFileValues(FileGroup group) {
        int count = sources[group.anyColumn()].size();
        long[] offsets = new long[FILE_CHECK_CHUNK];
        long[] sizes = new long[FILE_CHECK_CHUNK];
        byte[][] checksums = new byte[FILE_CHECK_CHUNK][];
        for (int from = 0; from < count; from += FILE_CHECK_CHUNK) {
            int length = Math.min(FILE_CHECK_CHUNK, count - from);
            copyLongs(group, FileReferences.Child.OFFSET, from, offsets, length);
            copyLongs(group, FileReferences.Child.SIZE, from, sizes, length);
            int checksumColumn = group.columns()[FileReferences.Child.CHECKSUM.ordinal()];
            if (checksumColumn >= 0) {
                ((BinaryColumnSource) sources[checksumColumn]).copyInto(from, checksums, 0, length);
            }
            for (int k = 0; k < length; k++) {
                int i = from + k;
                if (!isFilePresent(group, i)) {
                    continue;
                }
                String violation = FileReferences.violation(
                        isFileFieldSet(group, FileReferences.Child.INLINE, i),
                        isFileFieldSet(group, FileReferences.Child.URI, i),
                        isFileFieldSet(group, FileReferences.Child.OFFSET, i), offsets[k],
                        isFileFieldSet(group, FileReferences.Child.SIZE, i), sizes[k]);
                if (violation == null && isFileFieldSet(group, FileReferences.Child.CHECKSUM, i)) {
                    violation = FileReferences.checksumViolation(new String(checksums[k], StandardCharsets.UTF_8));
                }
                if (violation != null) {
                    throw new IllegalArgumentException("FILE group " + group.path() + " value " + i
                            + " is invalid: " + violation);
                }
            }
        }
    }

    private static boolean isFilePresent(FileGroup group, int index) {
        for (Layer layer : group.presence()) {
            if (isNull(layer.validity, index)) {
                return false;
            }
        }
        return true;
    }

    /// Whether `child` is set at `index`: present in the group and non-null, and for a string
    /// field non-empty.
    private boolean isFileFieldSet(FileGroup group, FileReferences.Child child, int index) {
        int column = group.columns()[child.ordinal()];
        if (column < 0 || isNull(leafValidities[column], index)) {
            return false;
        }
        return !(child.annotation() instanceof LogicalType.StringType)
                || ((BinaryColumnSource) sources[column]).valueBytesAt(index) > 0;
    }

    private void copyLongs(FileGroup group, FileReferences.Child child, int from, long[] dest, int length) {
        int column = group.columns()[child.ordinal()];
        if (column >= 0) {
            ((LongColumnSource) sources[column]).copyInto(from, dest, 0, length);
        }
    }

    public int recordCount() {
        return recordCount;
    }

    /// Shreds records `[from, from + count)` of column `columnIndex`, pushing each level
    /// entry into `sink`. Records must be shredded in order, since the column's value window
    /// advances monotonically across calls.
    /// The leaf slots records `[from, from + count)` reach in one column, packed as
    /// `leafFrom << 32 | leafCount`.
    ///
    /// Offsets are cumulative, so a record range's leaf range is had by composing one array
    /// lookup per repeated layer rather than by walking the records. Packed rather than returned
    /// as a pair because the writer asks this of every column before every slice it appends.
    public long leafRange(int columnIndex, int from, int count) {
        int slotFrom = from;
        int slotTo = from + count;
        for (Layer layer : layers[columnIndex]) {
            if (layer.kind() == Layer.Kind.REPEATED) {
                int[] offsets = layer.offsets;
                slotFrom = offsets[slotFrom];
                slotTo = offsets[slotTo];
            }
        }
        return ((long) slotFrom << Integer.SIZE) | Integer.toUnsignedLong(slotTo - slotFrom);
    }

    /// How many entries beyond its leaf slots one record can add to a column: one for every layer
    /// that can stand in for absent content — a null struct, a null list, an empty list — each of
    /// which emits an entry carrying no value.
    public int phantomLayers(int columnIndex) {
        int phantoms = 0;
        for (Layer layer : layers[columnIndex]) {
            if (layer.kind() == Layer.Kind.REPEATED || layer.nullable()) {
                phantoms++;
            }
        }
        return phantoms;
    }

    public void shred(int columnIndex, int from, int count, LevelSink sink) {
        Ctx ctx = new Ctx(columnIndex, layers[columnIndex], maxDef[columnIndex], sink);
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
                ctx.sink.accept(repToEmit, parentDef, ABSENT);
            }
            else {
                ctx.sink.accept(repToEmit, ctx.maxDef, itemIndex);
            }
            return;
        }
        Layer layer = ctx.path[layerIndex];
        if (layer.kind() == Layer.Kind.STRUCT) {
            if (layer.nullable() && isNull(layer.validity, itemIndex)) {
                ctx.sink.accept(repToEmit, parentDef, ABSENT);
            }
            else {
                emit(ctx, layerIndex + 1, itemIndex, parentDef + layer.presentDefInc(), repToEmit);
            }
            return;
        }
        // REPEATED (list).
        if (layer.nullable() && isNull(layer.validity, itemIndex)) {
            ctx.sink.accept(repToEmit, parentDef, ABSENT); // null list — outer group absent
            return;
        }
        int[] offsets = layer.offsets;
        int start = offsets[itemIndex];
        int end = offsets[itemIndex + 1];
        int listDef = parentDef + layer.presentDefInc();
        if (start == end) {
            ctx.sink.accept(repToEmit, listDef, ABSENT); // empty list — present but no elements
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
    /// parent count (`offsets.length - 1`); a `STRUCT` layer never remaps its scope, nullable
    /// or not, so it always preserves the count unchanged. A nullable `STRUCT` layer still
    /// constrains the offsets beneath it, which [#validateAbsentStructsEmpty] checks.
    private int impliedRecordCount(int columnIndex) {
        Layer[] path = layers[columnIndex];
        int count = sources[columnIndex].size();
        for (int k = path.length - 1; k >= 0; k--) {
            Layer layer = path[k];
            if (layer.kind() == Layer.Kind.REPEATED) {
                int[] offsets = offsetsFor(layer);
                validateOffsets(offsets, count, layer.key());
                validateNullListsEmpty(offsets, layer.key());
                validateAbsentStructsEmpty(path, k, offsets, layer.key());
                count = offsets.length - 1;
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
    /// silently drops the stray elements, producing a plausible but wrong file.
    private void validateNullListsEmpty(int[] offsets, String key) {
        int i = firstAbsentWithEntries(listValidities.get(key), offsets);
        if (i != -1) {
            throw new IllegalArgumentException("List " + key + " is null at index " + i
                    + " but its offsets span " + (offsets[i + 1] - offsets[i])
                    + " elements; a null list has none");
        }
    }

    /// Rejects offsets that span entries at a record where an enclosing struct is absent.
    /// [#emit] stops at the absent struct and never descends into the offsets below it, so
    /// without this the entries they claim are dropped from the file without a word — the
    /// same silent loss [#validateNullListsEmpty] prevents one layer down, arriving through
    /// the struct's `Validity` instead of the list's.
    ///
    /// Only the `STRUCT` layers between this layer and the next `REPEATED` one above it are
    /// consulted: a `STRUCT` layer never remaps its scope, so those index exactly the items
    /// this layer's offsets do, while a struct above the next `REPEATED` layer is checked
    /// against that layer's own offsets in its turn.
    ///
    /// @param path the column's layers, outermost first
    /// @param repeatedIndex the index in `path` of the `REPEATED` layer `offsets` belong to
    /// @param offsets that layer's entry offsets
    /// @param key that layer's dotted path, for the message
    private void validateAbsentStructsEmpty(Layer[] path, int repeatedIndex, int[] offsets, String key) {
        for (int k = repeatedIndex - 1; k >= 0 && path[k].kind() != Layer.Kind.REPEATED; k--) {
            String structKey = path[k].key();
            int i = firstAbsentWithEntries(structValidities.get(structKey), offsets);
            if (i != -1) {
                throw new IllegalArgumentException("Struct " + structKey + " is null at index " + i
                        + " but " + key + "'s offsets span " + (offsets[i + 1] - offsets[i])
                        + " entries there; an absent struct encloses none");
            }
        }
    }

    /// The first index at which `offsets` span entries although `validity` marks that item
    /// absent, or `-1` if there is none. The validity's length is not checked — [Validity] is
    /// intentionally length-less — so only the null positions within the offsets' range are
    /// examined.
    private static int firstAbsentWithEntries(Validity validity, int[] offsets) {
        if (validity == null) {
            return -1;
        }
        int parentCount = offsets.length - 1;
        for (int i = validity.nextNull(0, parentCount); i != -1; i = validity.nextNull(i + 1, parentCount)) {
            if (offsets[i + 1] != offsets[i]) {
                return i;
            }
        }
        return -1;
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

    /// The values of a `FILE` group's `INT64` children read at a time while its batch is checked.
    private static final int FILE_CHECK_CHUNK = 1024;

    /// A `FILE` group as the batch check reads it.
    ///
    /// @param path the group's dotted path
    /// @param presence the nullable `STRUCT` layers from the nearest `REPEATED` layer down to the
    ///        group itself, whose validities decide whether an instance is present
    /// @param columns the leaf column of each [FileReferences.Child] by ordinal, `-1` for a child
    ///        the group does not define
    private record FileGroup(String path, Layer[] presence, int[] columns) {

        static FileGroup of(SchemaNode.GroupNode group, String path, List<Layer> layerStack) {
            int nearestRepeated = -1;
            for (int i = 0; i < layerStack.size(); i++) {
                if (layerStack.get(i).kind() == Layer.Kind.REPEATED) {
                    nearestRepeated = i;
                }
            }
            Layer[] presence = layerStack.subList(nearestRepeated + 1, layerStack.size()).toArray(new Layer[0]);
            int[] columns = new int[FileReferences.Child.values().length];
            Arrays.fill(columns, -1);
            for (SchemaNode child : group.children()) {
                columns[FileReferences.Child.named(child.name()).ordinal()] =
                        ((SchemaNode.PrimitiveNode) child).columnIndex();
            }
            return new FileGroup(path, presence, columns);
        }

        int anyColumn() {
            for (int column : columns) {
                if (column >= 0) {
                    return column;
                }
            }
            throw new IllegalStateException("FILE group " + path + " has no children");
        }
    }

    /// Per-column shred state, threaded through the recursion.
    private static final class Ctx {
        final int columnIndex;
        final Layer[] path;
        final int maxDef;
        final LevelSink sink;

        Ctx(int columnIndex, Layer[] path, int maxDef, LevelSink sink) {
            this.columnIndex = columnIndex;
            this.path = path;
            this.maxDef = maxDef;
            this.sink = sink;
        }
    }
}
