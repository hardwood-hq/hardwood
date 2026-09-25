/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.IntPredicate;

import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.internal.util.StringToIntMap;
import dev.hardwood.row.PqInterval;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.row.StructAccessor;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

/// The [StructAccessor] a compiled [dev.hardwood.internal.predicate.RowMatcher] is tested
/// against: a view over the current batches of the predicate's columns, positioned at one
/// record, and over nothing else.
///
/// Readers expose their payload columns through accessors of their own, which never reach a
/// predicate column outside the projection. The matcher reaches the predicate columns through
/// this view instead, so the one accessor surface a caller sees is bounded by the projection.
/// See `_designs/RECORD_FILTERING.md`.
///
/// Flat predicate columns are served from their typed arrays, nested ones through a
/// [NestedBatchDataView] over a projection of the predicate paths. Only the accessors the
/// compiled matcher calls are implemented; the rest throw.
///
/// Indexed access uses the view's own index space, which [#indexOf] maps a file leaf column
/// into: the flat predicate columns occupy `[0, flatCount)`, and the top-level fields of the
/// nested projection follow.
public final class PredicateView implements StructAccessor {

    /// Decoded column index of each flat predicate column, by slot.
    private final int[] flatProjected;
    private final StringToIntMap flatSlotByName;
    private final Object[] flatValues;
    /// Per-slot validity (set bit = present), or `null` when every value of the batch is present.
    private final long[][] flatValidity;

    /// `null` when every predicate column is flat.
    private final NestedBatchDataView nestedView;
    /// Decoded column index of each nested predicate column, in the nested projection's order.
    private final int[] nestedProjected;
    private final ColumnSchema[] nestedSchemas;
    private final NestedBatch[] nestedBatches;
    /// Whether the nested batches arrive without element validity, so that [#refresh] derives it
    /// from the definition levels.
    private final boolean deriveElementValidity;

    /// View index by file leaf-column index, `-1` for a column indexed access does not reach.
    private final int[] indexByColumn;

    private int record;

    private PredicateView(int[] flatProjected, StringToIntMap flatSlotByName,
                          NestedBatchDataView nestedView, int[] nestedProjected, ColumnSchema[] nestedSchemas,
                          boolean deriveElementValidity, int[] indexByColumn) {
        this.flatProjected = flatProjected;
        this.flatSlotByName = flatSlotByName;
        this.flatValues = new Object[flatProjected.length];
        this.flatValidity = new long[flatProjected.length][];
        this.nestedView = nestedView;
        this.nestedProjected = nestedProjected;
        this.nestedSchemas = nestedSchemas;
        this.nestedBatches = new NestedBatch[nestedProjected.length];
        this.deriveElementValidity = deriveElementValidity;
        this.indexByColumn = indexByColumn;
    }

    /// Builds a view over the columns `resolved` references.
    ///
    /// @param schema the file schema
    /// @param decoded the columns the reader decodes, predicate columns included ([dev.hardwood.internal.schema.ReadProjection#decoded()])
    /// @param resolved the predicate
    /// @param nestedAt whether the column at an decoded column index arrives as a
    ///        [NestedBatch] rather than a flat [BatchExchange.Batch]
    /// @param deriveElementValidity whether the nested batches arrive without
    ///        [NestedBatch#elementValidity], for [#refresh] to derive it
    public static PredicateView create(FileSchema schema, ProjectedSchema decoded,
                                       ResolvedPredicate resolved, IntPredicate nestedAt,
                                       boolean deriveElementValidity) {
        Set<Integer> columns = predicateColumns(resolved);

        List<Integer> flatColumns = new ArrayList<>();
        List<Integer> nestedColumns = new ArrayList<>();
        List<String> nestedPaths = new ArrayList<>();
        for (int columnIndex : columns) {
            if (nestedAt.test(decoded.toProjectedIndex(columnIndex))) {
                nestedColumns.add(columnIndex);
                nestedPaths.add(schema.getColumn(columnIndex).fieldPath().toString());
            }
            else {
                flatColumns.add(columnIndex);
            }
        }

        int[] indexByColumn = new int[schema.getColumnCount()];
        Arrays.fill(indexByColumn, -1);

        int flatCount = flatColumns.size();
        int[] flatProjected = new int[flatCount];
        StringToIntMap flatSlotByName = new StringToIntMap(Math.max(flatCount, 1));
        for (int slot = 0; slot < flatCount; slot++) {
            int columnIndex = flatColumns.get(slot);
            flatProjected[slot] = decoded.toProjectedIndex(columnIndex);
            // A flat column below a struct is reached by index only; under its leaf name it would
            // shadow a top-level column of the same name.
            if (schema.getColumn(columnIndex).fieldPath().elements().size() == 1) {
                flatSlotByName.put(schema.getColumn(columnIndex).name(), slot);
            }
            indexByColumn[columnIndex] = slot;
        }

        if (nestedPaths.isEmpty()) {
            return new PredicateView(flatProjected, flatSlotByName,
                    null, new int[0], new ColumnSchema[0], deriveElementValidity, indexByColumn);
        }
        ProjectedSchema nestedProjection = ProjectedSchema.create(
                schema, ColumnProjection.columns(nestedPaths.toArray(new String[0])));
        int q = nestedProjection.getProjectedColumnCount();
        int[] nestedProjected = new int[q];
        ColumnSchema[] nestedSchemas = new ColumnSchema[q];
        for (int j = 0; j < q; j++) {
            int originalIndex = nestedProjection.toOriginalIndex(j);
            nestedSchemas[j] = schema.getColumn(originalIndex);
            nestedProjected[j] = decoded.toProjectedIndex(originalIndex);
        }
        int[] topLevel = topLevelFieldIndexLookup(schema, nestedProjection);
        for (int columnIndex : nestedColumns) {
            if (topLevel[columnIndex] >= 0) {
                indexByColumn[columnIndex] = flatCount + topLevel[columnIndex];
            }
        }
        return new PredicateView(flatProjected, flatSlotByName,
                new NestedBatchDataView(schema, nestedProjection), nestedProjected, nestedSchemas,
                deriveElementValidity, indexByColumn);
    }

    /// The file leaf columns `resolved` references, in first-seen order.
    public static Set<Integer> predicateColumns(ResolvedPredicate resolved) {
        Set<Integer> columns = new LinkedHashSet<>();
        collectColumnIndices(resolved, columns);
        return columns;
    }

    private static void collectColumnIndices(ResolvedPredicate p, Set<Integer> out) {
        switch (p) {
            case ResolvedPredicate.And a -> a.children().forEach(c -> collectColumnIndices(c, out));
            case ResolvedPredicate.Or o -> o.children().forEach(c -> collectColumnIndices(c, out));
            default -> out.add(leafColumnIndex(p));
        }
    }

    /// The column one leaf reads, as [ResolvedPredicate#leafColumnIndex] resolves it. `And` and
    /// `Or` never reach here — [#collectColumnIndices] peels them off first — so the `-1` that
    /// marks them is a compiler that grew a case this method did not.
    private static int leafColumnIndex(ResolvedPredicate p) {
        int columnIndex = ResolvedPredicate.leafColumnIndex(p);
        if (columnIndex < 0) {
            throw new IllegalStateException(p.getClass().getSimpleName() + " is not a leaf");
        }
        return columnIndex;
    }

    /// Builds a `fileLeafColumnIndex → projectedTopLevelFieldIndex` lookup, the index space of
    /// [NestedBatchDataView]'s indexed accessors. `-1` for any column whose path is not a single
    /// top-level element, or whose top-level field is not in the projection.
    private static int[] topLevelFieldIndexLookup(FileSchema schema, ProjectedSchema projectedSchema) {
        int columnCount = schema.getColumnCount();
        int[] lookup = new int[columnCount];
        Arrays.fill(lookup, -1);

        int[] projectedFieldIndices = projectedSchema.getProjectedFieldIndices();
        List<SchemaNode> children = schema.getRootNode().children();

        for (int col = 0; col < columnCount; col++) {
            if (schema.getColumn(col).fieldPath().elements().size() != 1) {
                continue;
            }
            String topLevelName = schema.getColumn(col).fieldPath().topLevelName();
            for (int i = 0; i < projectedFieldIndices.length; i++) {
                if (children.get(projectedFieldIndices[i]).name().equals(topLevelName)) {
                    lookup[col] = i;
                    break;
                }
            }
        }
        return lookup;
    }

    /// The view index of a file leaf column for indexed access, or `-1` when the matcher must
    /// reach it by name. Passed to [dev.hardwood.internal.predicate.RecordFilterCompiler] as its
    /// `topLevelFieldIndex` callback.
    public int indexOf(int columnIndex) {
        return indexByColumn[columnIndex];
    }

    /// Re-points the view at the batches now current. Called once per batch the matcher
    /// evaluates, before [#setRecord].
    ///
    /// @param flatBatches the flat batches by decoded column index; only the flat
    ///        predicate columns' entries are read
    /// @param nestedBatches the nested batches by decoded column index; only the nested
    ///        predicate columns' entries are read
    /// @param fileName the file the batches came from
    public void refresh(BatchExchange.Batch[] flatBatches, NestedBatch[] nestedBatches, String fileName) {
        for (int slot = 0; slot < flatProjected.length; slot++) {
            BatchExchange.Batch batch = flatBatches[flatProjected[slot]];
            flatValues[slot] = batch.values;
            flatValidity[slot] = batch.validity;
        }
        if (nestedView != null) {
            for (int j = 0; j < nestedProjected.length; j++) {
                NestedBatch batch = nestedBatches[nestedProjected[j]];
                if (deriveElementValidity) {
                    batch.elementValidity = NestedLevelComputer.computeElementValidity(
                            batch.definitionLevels, batch.valueCount, nestedSchemas[j].maxDefinitionLevel());
                }
                this.nestedBatches[j] = batch;
            }
            nestedView.setBatchData(this.nestedBatches, nestedSchemas, fileName);
        }
    }

    /// Positions the view at `record` of the current batches.
    public void setRecord(int record) {
        this.record = record;
        if (nestedView != null) {
            nestedView.setRowIndex(record);
        }
    }

    /// The flat slot `name` is held in, or `-1`. A view whose predicate columns are all nested,
    /// which is every view the nested row reader builds, answers without a lookup.
    private int flatSlot(String name) {
        return flatProjected.length == 0 ? -1 : flatSlotByName.get(name);
    }

    private boolean flatNull(int slot) {
        long[] validity = flatValidity[slot];
        return validity != null && (validity[record >>> 6] & (1L << record)) == 0L;
    }

    private float flatFloat(int slot) {
        Object values = flatValues[slot];
        // A FLOAT16 column is held as two-byte binary values, read as the half they encode.
        return values instanceof float[] floats
                ? floats[record]
                : ((BinaryBatchValues) values).float16At(record);
    }

    private int nestedIndex(int index) {
        return index - flatProjected.length;
    }

    // ==================== Name-keyed ====================

    @Override public boolean isNull(String name) {
        int slot = flatSlot(name);
        return slot >= 0 ? flatNull(slot) : nested(name).isNull(name);
    }

    @Override public int getInt(String name) {
        int slot = flatSlot(name);
        return slot >= 0 ? ((int[]) flatValues[slot])[record] : nested(name).getInt(name);
    }

    @Override public long getLong(String name) {
        int slot = flatSlot(name);
        return slot >= 0 ? ((long[]) flatValues[slot])[record] : nested(name).getLong(name);
    }

    @Override public float getFloat(String name) {
        int slot = flatSlot(name);
        return slot >= 0 ? flatFloat(slot) : nested(name).getFloat(name);
    }

    @Override public double getDouble(String name) {
        int slot = flatSlot(name);
        return slot >= 0 ? ((double[]) flatValues[slot])[record] : nested(name).getDouble(name);
    }

    @Override public boolean getBoolean(String name) {
        int slot = flatSlot(name);
        return slot >= 0 ? ((boolean[]) flatValues[slot])[record] : nested(name).getBoolean(name);
    }

    @Override public byte[] getBinary(String name) {
        int slot = flatSlot(name);
        return slot >= 0 ? ((BinaryBatchValues) flatValues[slot]).byteArrayAt(record) : nested(name).getBinary(name);
    }

    @Override public PqStruct getStruct(String name) {
        return nested(name).getStruct(name);
    }

    /// The nested view, which serves every name the flat slots do not. A leaf below a
    /// struct that decodes as a flat column (every level of its path required) is not
    /// navigated by name: [#indexOf] maps it to its flat slot, and every leaf the matcher
    /// compiles for it reads by index.
    private NestedBatchDataView nested(String name) {
        if (nestedView == null) {
            throw new IllegalStateException("Predicate view has no nested column to serve field '"
                    + name + "'; a flat column below a struct is read by index");
        }
        return nestedView;
    }

    // ==================== Indexed ====================

    @Override public boolean isNull(int index) {
        return index < flatProjected.length ? flatNull(index) : nestedView.isNull(nestedIndex(index));
    }

    @Override public int getInt(int index) {
        return index < flatProjected.length
                ? ((int[]) flatValues[index])[record]
                : nestedView.getInt(nestedIndex(index));
    }

    @Override public long getLong(int index) {
        return index < flatProjected.length
                ? ((long[]) flatValues[index])[record]
                : nestedView.getLong(nestedIndex(index));
    }

    @Override public float getFloat(int index) {
        return index < flatProjected.length ? flatFloat(index) : nestedView.getFloat(nestedIndex(index));
    }

    @Override public double getDouble(int index) {
        return index < flatProjected.length
                ? ((double[]) flatValues[index])[record]
                : nestedView.getDouble(nestedIndex(index));
    }

    @Override public boolean getBoolean(int index) {
        return index < flatProjected.length
                ? ((boolean[]) flatValues[index])[record]
                : nestedView.getBoolean(nestedIndex(index));
    }

    @Override public byte[] getBinary(int index) {
        return index < flatProjected.length
                ? ((BinaryBatchValues) flatValues[index]).byteArrayAt(record)
                : nestedView.getBinary(nestedIndex(index));
    }

    // ---- Never invoked by a compiled RowMatcher ----

    private static UnsupportedOperationException unsupported() {
        return new UnsupportedOperationException("Accessor not supported during predicate evaluation");
    }

    @Override public String getString(String name) { throw unsupported(); }
    @Override public LocalDate getDate(String name) { throw unsupported(); }
    @Override public LocalTime getTime(String name) { throw unsupported(); }
    @Override public Instant getTimestamp(String name) { throw unsupported(); }
    @Override public LocalDateTime getLocalTimestamp(String name) { throw unsupported(); }
    @Override public BigDecimal getDecimal(String name) { throw unsupported(); }
    @Override public UUID getUuid(String name) { throw unsupported(); }
    @Override public PqInterval getInterval(String name) { throw unsupported(); }
    @Override public Object getValue(String name) { throw unsupported(); }
    @Override public Object getRawValue(String name) { throw unsupported(); }
    @Override public PqList getList(String name) { throw unsupported(); }
    @Override public PqMap getMap(String name) { throw unsupported(); }
    @Override public PqVariant getVariant(String name) { throw unsupported(); }
    @Override public int getFieldCount() { throw unsupported(); }
    @Override public String getFieldName(int index) { throw unsupported(); }

    @Override public String getString(int index) { throw unsupported(); }
    @Override public LocalDate getDate(int index) { throw unsupported(); }
    @Override public LocalTime getTime(int index) { throw unsupported(); }
    @Override public Instant getTimestamp(int index) { throw unsupported(); }
    @Override public LocalDateTime getLocalTimestamp(int index) { throw unsupported(); }
    @Override public BigDecimal getDecimal(int index) { throw unsupported(); }
    @Override public UUID getUuid(int index) { throw unsupported(); }
    @Override public PqInterval getInterval(int index) { throw unsupported(); }
    @Override public PqVariant getVariant(int index) { throw unsupported(); }
    @Override public PqStruct getStruct(int index) { throw unsupported(); }
    @Override public PqList getList(int index) { throw unsupported(); }
    @Override public PqMap getMap(int index) { throw unsupported(); }
    @Override public Object getValue(int index) { throw unsupported(); }
    @Override public Object getRawValue(int index) { throw unsupported(); }
}
