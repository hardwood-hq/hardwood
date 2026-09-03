/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import dev.hardwood.Validity;
import dev.hardwood.internal.predicate.BatchFilterCompiler;
import dev.hardwood.internal.predicate.CompiledBatchFilter;
import dev.hardwood.internal.predicate.RecordFilterCompiler;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.predicate.RowMatcher;
import dev.hardwood.internal.reader.BatchExchange;
import dev.hardwood.internal.reader.BatchMatchMerger;
import dev.hardwood.internal.reader.BinaryBatchValues;
import dev.hardwood.internal.reader.NestedBatch;
import dev.hardwood.internal.reader.NestedBatchDataView;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.row.PqInterval;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.row.StructAccessor;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// Computes the per-batch **record selection** that makes a column-reader
/// filter exact (#624). Given the already-decoded batches of an augmented
/// projection (payload columns plus the predicate columns), it produces the
/// ascending indices of the records that satisfy the predicate, which the
/// [FilterCoordinator] then uses to compact each payload column.
///
/// Two backends, chosen once at construction:
///
/// - **Drain-side** — when [BatchFilterCompiler] accepts the predicate (flat,
///   top-level, supported `(type, op)`), a [BatchMatchMerger] runs the
///   per-column matcher fragments over the flat batches and combines them into
///   one survivor bitmap. This is the row reader's drain-side merge, in the
///   mode where the merger runs the matchers itself because there are no
///   worker threads to have run them already.
/// - **Record matcher** — otherwise (nested paths, binary/string, unsupported
///   operators) the compiled [RowMatcher] is evaluated per record over a
///   batch-backed [StructAccessor] view of the predicate columns, giving full
///   parity with the row reader's filtered result.
final class SelectionEngine {

    // Drain-side backend (null when the record-matcher backend is used).
    private final BatchMatchMerger merger;
    /// The batches [#merger] reads, indexed by projected column index. Staged
    /// here rather than passed straight through because the merger takes the
    /// batches, and this engine holds readers. `null` on the record-matcher
    /// backend.
    private final BatchExchange.Batch[] stagedBatches;
    /// The projected indices [#merger] reads — its own set, read once at
    /// construction, so nothing here re-derives which columns the plan touches.
    /// `null` on the record-matcher backend.
    private final int[] stagedColumns;

    private final ColumnReader[] readersByProjectedIndex;

    // Record-matcher backend (null when the drain-side backend is used).
    private final RowMatcher rowMatcher;
    private final PredicateRowView predicateView;

    /// Reusable buffer holding the matching record indices of the current batch
    /// in `[0, count)`. Owned by the engine and overwritten every batch, so the
    /// [FilterCoordinator] must consume it (apply it to all payload readers)
    /// before the next [#computeSelection]. Sized to the batch capacity.
    private final int[] selection;

    private SelectionEngine(BatchMatchMerger merger, int columnCount,
                            ColumnReader[] readersByProjectedIndex,
                            RowMatcher rowMatcher, PredicateRowView predicateView, int[] selection) {
        this.merger = merger;
        this.stagedBatches = merger != null ? new BatchExchange.Batch[columnCount] : null;
        this.stagedColumns = merger != null ? merger.referencedColumns() : null;
        this.readersByProjectedIndex = readersByProjectedIndex;
        this.rowMatcher = rowMatcher;
        this.predicateView = predicateView;
        this.selection = selection;
    }

    /// Builds an engine for `resolved` over the augmented projection, reading
    /// predicate values from `readersByProjectedIndex` (indexed by the
    /// augmented projected column index).
    static SelectionEngine create(FileSchema schema, ProjectedSchema augProjected,
                                  ResolvedPredicate resolved,
                                  ColumnReader[] readersByProjectedIndex, int batchSize) {
        int wordsLen = (batchSize + 63) >>> 6;
        int[] selection = new int[batchSize];
        CompiledBatchFilter compiled = BatchFilterCompiler.tryCompile(
                resolved, schema, augProjected::toProjectedIndex);

        if (compiled != null) {
            // Owning mode: no column workers ran the matchers, so the merger runs
            // them itself into buffers it allocates.
            BatchMatchMerger merger = BatchMatchMerger.owning(
                    compiled, readersByProjectedIndex.length, wordsLen);
            return new SelectionEngine(merger, readersByProjectedIndex.length,
                    readersByProjectedIndex, null, null, selection);
        }

        // Record-matcher backend. Name-keyed compilation (no indexed-leaf
        // callback) so the matcher navigates the batch view purely by field
        // name, which works uniformly for flat and nested predicate columns.
        RowMatcher matcher = RecordFilterCompiler.compile(resolved, schema);
        PredicateRowView view = PredicateRowView.create(
                schema, augProjected, resolved, readersByProjectedIndex);
        return new SelectionEngine(null, readersByProjectedIndex.length,
                readersByProjectedIndex, matcher, view, selection);
    }

    /// Computes the matching records of the current batch into the reusable
    /// [#selection] buffer and returns their count, or `-1` when every record
    /// matches (the no-compaction fast path). The indices live in
    /// `selection()[0, count)` until the next call.
    int computeSelection(int recordCount) {
        return merger != null
                ? computeDrainSide(recordCount)
                : computeRecordMatcher(recordCount);
    }

    /// The reusable selection buffer; valid in `[0, count)` for the count
    /// returned by the most recent [#computeSelection].
    int[] selection() {
        return selection;
    }

    private int computeDrainSide(int recordCount) {
        for (int i = 0; i < stagedColumns.length; i++) {
            int p = stagedColumns[i];
            stagedBatches[p] = readersByProjectedIndex[p].currentFlatBatch();
        }
        return collectSetBits(merger.merge(stagedBatches, recordCount), recordCount);
    }

    private int computeRecordMatcher(int recordCount) {
        predicateView.refresh();
        int count = 0;
        for (int r = 0; r < recordCount; r++) {
            predicateView.setRecord(r);
            if (rowMatcher.test(predicateView)) {
                selection[count++] = r;
            }
        }
        return count == recordCount ? -1 : count;
    }

    /// Collects set bits in `[0, recordCount)` into [#selection] and returns the
    /// count, or `-1` when all are set (the no-compaction fast path).
    private int collectSetBits(long[] words, int recordCount) {
        int count = 0;
        for (int r = 0; r < recordCount; r++) {
            if ((words[r >>> 6] & (1L << r)) != 0L) {
                selection[count++] = r;
            }
        }
        return count == recordCount ? -1 : count;
    }

    // ==================== Predicate column discovery ====================

    /// File leaf-column paths referenced by `resolved`, in first-seen order.
    /// Used to augment the projection so the predicate columns are decoded.
    static List<String> predicateColumnPaths(ResolvedPredicate resolved, FileSchema schema) {
        Set<Integer> indices = new LinkedHashSet<>();
        collectColumnIndices(resolved, indices);
        List<String> paths = new ArrayList<>(indices.size());
        for (int columnIndex : indices) {
            paths.add(schema.getColumn(columnIndex).fieldPath().toString());
        }
        return paths;
    }

    private static void collectColumnIndices(ResolvedPredicate p, Set<Integer> out) {
        switch (p) {
            case ResolvedPredicate.And a -> a.children().forEach(c -> collectColumnIndices(c, out));
            case ResolvedPredicate.Or o -> o.children().forEach(c -> collectColumnIndices(c, out));
            default -> out.add(leafColumnIndex(p));
        }
    }

    private static int leafColumnIndex(ResolvedPredicate p) {
        return switch (p) {
            case ResolvedPredicate.IntPredicate x -> x.columnIndex();
            case ResolvedPredicate.LongPredicate x -> x.columnIndex();
            case ResolvedPredicate.FloatPredicate x -> x.columnIndex();
            case ResolvedPredicate.Float16Predicate x -> x.columnIndex();
            case ResolvedPredicate.DoublePredicate x -> x.columnIndex();
            case ResolvedPredicate.BooleanPredicate x -> x.columnIndex();
            case ResolvedPredicate.BinaryPredicate x -> x.columnIndex();
            case ResolvedPredicate.IntInPredicate x -> x.columnIndex();
            case ResolvedPredicate.LongInPredicate x -> x.columnIndex();
            case ResolvedPredicate.BinaryInPredicate x -> x.columnIndex();
            case ResolvedPredicate.DoubleInPredicate x -> x.columnIndex();
            case ResolvedPredicate.IsNullPredicate x -> x.columnIndex();
            case ResolvedPredicate.IsNotNullPredicate x -> x.columnIndex();
            case ResolvedPredicate.GeospatialPredicate x -> x.columnIndex();
            case ResolvedPredicate.And a -> throw new IllegalStateException("And is not a leaf");
            case ResolvedPredicate.Or o -> throw new IllegalStateException("Or is not a leaf");
        };
    }

    // ==================== Batch-backed predicate accessor ====================

    /// A [StructAccessor] over the current (pre-selection) predicate-column
    /// batches, positioned at one record. Flat predicate columns are served
    /// from their typed arrays; nested predicate columns are served through a
    /// [NestedBatchDataView] so `getStruct(...)` navigation works exactly as in
    /// the nested row reader. Only the accessor methods the compiled
    /// [RowMatcher] actually calls are implemented; the rest throw.
    private static final class PredicateRowView implements StructAccessor {

        private final Map<String, FlatField> flatByName;
        private final NestedBatchDataView nestedView;
        private final ColumnReader[] nestedReaders;
        private final ColumnSchema[] nestedColumnSchemas;
        private final NestedBatch[] nestedBatches;
        private int record;

        /// A flat predicate column with its backing array and validity resolved
        /// once per batch (in [PredicateRowView#refresh()]), so the per-record
        /// accessors are a direct array index rather than a fresh `getXxx()`
        /// dispatch + cast each call.
        private static final class FlatField {
            final ColumnReader reader;
            Object values;
            Validity validity;

            FlatField(ColumnReader reader) {
                this.reader = reader;
            }

            void refresh() {
                values = reader.currentFlatBatch().values;
                validity = reader.getLeafValidity();
            }
        }

        private PredicateRowView(Map<String, FlatField> flatByName,
                                 NestedBatchDataView nestedView,
                                 ColumnReader[] nestedReaders,
                                 ColumnSchema[] nestedColumnSchemas) {
            this.flatByName = flatByName;
            this.nestedView = nestedView;
            this.nestedReaders = nestedReaders;
            this.nestedColumnSchemas = nestedColumnSchemas;
            this.nestedBatches = nestedReaders != null ? new NestedBatch[nestedReaders.length] : null;
        }

        static PredicateRowView create(FileSchema schema, ProjectedSchema augProjected,
                                       ResolvedPredicate resolved,
                                       ColumnReader[] readersByProjectedIndex) {
            Set<Integer> indices = new LinkedHashSet<>();
            collectColumnIndices(resolved, indices);

            Map<String, FlatField> flatByName = new LinkedHashMap<>();
            List<String> nestedPaths = new ArrayList<>();
            for (int columnIndex : indices) {
                ColumnReader reader = readersByProjectedIndex[augProjected.toProjectedIndex(columnIndex)];
                if (reader.isNested()) {
                    nestedPaths.add(schema.getColumn(columnIndex).fieldPath().toString());
                }
                else {
                    flatByName.put(schema.getColumn(columnIndex).name(), new FlatField(reader));
                }
            }

            if (nestedPaths.isEmpty()) {
                return new PredicateRowView(flatByName, null, null, null);
            }
            ProjectedSchema nestedProjected = ProjectedSchema.create(
                    schema, ColumnProjection.columns(nestedPaths.toArray(new String[0])));
            int q = nestedProjected.getProjectedColumnCount();
            ColumnReader[] nestedReaders = new ColumnReader[q];
            ColumnSchema[] nestedColumnSchemas = new ColumnSchema[q];
            for (int j = 0; j < q; j++) {
                int originalIndex = nestedProjected.toOriginalIndex(j);
                nestedColumnSchemas[j] = schema.getColumn(originalIndex);
                nestedReaders[j] = readersByProjectedIndex[augProjected.toProjectedIndex(originalIndex)];
            }
            NestedBatchDataView view = new NestedBatchDataView(schema, nestedProjected);
            return new PredicateRowView(flatByName, view, nestedReaders, nestedColumnSchemas);
        }

        /// Re-points the nested view and resolves each flat field's backing
        /// array + validity for the current batch. Called once per batch before
        /// per-record evaluation, so the per-record accessors avoid repeated
        /// lookups and `getXxx()` dispatch.
        void refresh() {
            for (FlatField field : flatByName.values()) {
                field.refresh();
            }
            if (nestedView != null) {
                for (int j = 0; j < nestedReaders.length; j++) {
                    nestedBatches[j] = nestedReaders[j].currentNestedBatch();
                }
                nestedView.setBatchData(nestedBatches, nestedColumnSchemas, nestedBatches[0].fileName);
            }
        }

        void setRecord(int record) {
            this.record = record;
            if (nestedView != null) {
                nestedView.setRowIndex(record);
            }
        }

        private FlatField flat(String name) {
            FlatField field = flatByName.get(name);
            if (field == null) {
                throw new IllegalStateException("No flat predicate column named '" + name + "'");
            }
            return field;
        }

        @Override public boolean isNull(String name) {
            FlatField field = flatByName.get(name);
            if (field != null) {
                return field.validity.isNull(record);
            }
            return nestedView.isNull(name);
        }

        @Override public int getInt(String name) {
            return ((int[]) flat(name).values)[record];
        }

        @Override public long getLong(String name) {
            return ((long[]) flat(name).values)[record];
        }

        @Override public float getFloat(String name) {
            return ((float[]) flat(name).values)[record];
        }

        @Override public double getDouble(String name) {
            return ((double[]) flat(name).values)[record];
        }

        @Override public boolean getBoolean(String name) {
            return ((boolean[]) flat(name).values)[record];
        }

        @Override public byte[] getBinary(String name) {
            return ((BinaryBatchValues) flat(name).values).byteArrayAt(record);
        }

        @Override public PqStruct getStruct(String name) {
            if (nestedView == null) {
                throw new UnsupportedOperationException(
                        "Nested-path predicate on a non-nullable struct path is not supported"
                                + " for column readers; column '" + name + "' did not decode as nested");
            }
            return nestedView.getStruct(name);
        }

        // ---- Methods never invoked by the compiled RowMatcher for supported predicates ----

        private static UnsupportedOperationException unsupported() {
            return new UnsupportedOperationException(
                    "Accessor not supported during predicate evaluation");
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

        @Override public boolean isNull(int fieldIndex) { throw unsupported(); }
        @Override public int getInt(int fieldIndex) { throw unsupported(); }
        @Override public long getLong(int fieldIndex) { throw unsupported(); }
        @Override public float getFloat(int fieldIndex) { throw unsupported(); }
        @Override public double getDouble(int fieldIndex) { throw unsupported(); }
        @Override public boolean getBoolean(int fieldIndex) { throw unsupported(); }
        @Override public String getString(int fieldIndex) { throw unsupported(); }
        @Override public byte[] getBinary(int fieldIndex) { throw unsupported(); }
        @Override public LocalDate getDate(int fieldIndex) { throw unsupported(); }
        @Override public LocalTime getTime(int fieldIndex) { throw unsupported(); }
        @Override public Instant getTimestamp(int fieldIndex) { throw unsupported(); }
        @Override public LocalDateTime getLocalTimestamp(int fieldIndex) { throw unsupported(); }
        @Override public BigDecimal getDecimal(int fieldIndex) { throw unsupported(); }
        @Override public UUID getUuid(int fieldIndex) { throw unsupported(); }
        @Override public PqInterval getInterval(int fieldIndex) { throw unsupported(); }
        @Override public PqVariant getVariant(int fieldIndex) { throw unsupported(); }
        @Override public PqStruct getStruct(int fieldIndex) { throw unsupported(); }
        @Override public PqList getList(int fieldIndex) { throw unsupported(); }
        @Override public PqMap getMap(int fieldIndex) { throw unsupported(); }
        @Override public Object getValue(int fieldIndex) { throw unsupported(); }
        @Override public Object getRawValue(int fieldIndex) { throw unsupported(); }
    }
}
